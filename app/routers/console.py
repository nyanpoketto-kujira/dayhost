"""Console — RCON-backed command execution with allow-list + rate limit.

Behaviour
---------
* `RCON_PASSWORD` empty  -> Fase 1 stub behaviour preserved: 200, `executed:false`,
  a clear note. Never an error, so an unconfigured box still shows the UI.
* `RCON_PASSWORD` set    -> the command is really sent via `app/services/rcon.py`
  (stdlib Source RCON), response streamed into the SSE log bus.
* Every command is classified (`safe` / `destructive` / `unknown`):
    - `safe`        -> allowed
    - `destructive` -> allowed only when `RCON_ALLOW_DESTRUCTIVE=true`
    - `unknown`     -> rejected (deny-by-default; Minecraft has too many
                       world-mutating verbs to enumerate exhaustively)
* Rejections are structured JSON with HTTP 403, never a traceback.
* Simple in-memory rate limit: one command per `RCON_MIN_INTERVAL` seconds
  (default 1 s) per authenticated user. 429 on violation.
* **Every** attempt is written to the persistent `rcon_audit` table — allowed,
  refused (403) and rate-limited (429) alike. Refusals are the point of an
  audit trail, so they are recorded too. Audit failures are logged but never
  break the command they describe.
"""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse

from .. import db
from ..auth import require_user
from ..config import get_settings
from ..services.logbus import bus

log = logging.getLogger("dayhost.console")

# `rcon` is imported lazily inside `console_send` (see the note there): it is
# only needed once a command is actually executed, and importing it eagerly
# costs RSS at startup for a code path most boxes never hit.

router = APIRouter(tags=["console"])

# Per-user rate-limit state: username -> monotonic timestamp of last command.
# Bounded by the number of admin users (1), so no cleanup machinery is needed;
# entries are dropped only if the map somehow grows past MAX_TRACKED.
_last_command: dict[str, float] = {}
MAX_TRACKED = 32


def _rate_limited(user: str, min_interval: float) -> float:
    """Return seconds to wait, or 0.0 when the command may proceed."""
    now = time.monotonic()
    if len(_last_command) > MAX_TRACKED:
        _last_command.clear()
    last = _last_command.get(user)
    if last is not None and (now - last) < min_interval:
        return round(min_interval - (now - last), 2)
    _last_command[user] = now
    return 0.0


def _reset_rate_limit() -> None:
    """Test hook."""
    _last_command.clear()


def _client_ip(request: Request) -> str:
    """Peer address. uvicorn runs with --proxy-headers from 127.0.0.1 only, so
    `request.client` is already the forwarded client when that applies."""
    client = getattr(request, "client", None)
    return getattr(client, "host", "") or ""


async def _audit(
    request: Request,
    *,
    user: str,
    command: str,
    classification: str,
    allowed: bool,
    executed: bool,
    summary: str | None = None,
) -> None:
    """Best-effort audit write. A failure is logged, never raised."""
    written = await db.rcon_audit_add(
        username=user,
        command=command,
        classification=classification,
        allowed=allowed,
        executed=executed,
        result_summary=summary,
        client_ip=_client_ip(request),
    )
    if not written:
        # Visible on purpose: a silently missing audit trail is worse than a
        # failed command, and the operator must see it in the log.
        log.warning(
            "AUDIT WRITE FAILED for user=%s classification=%s allowed=%s command=%r",
            user, classification, allowed, command[:120],
        )


@router.post("/api/console/send")
async def console_send(
    request: Request,
    command: str = Form(""),
    user: str = Depends(require_user),
) -> JSONResponse:
    # Lazy: `rcon` (Source RCON client + classifier) is only needed when a
    # command is actually sent. Eagerly importing it at module scope added
    # ~1 MB to steady-state RSS for a path that is idle on an unconfigured box.
    from ..services import rcon

    settings = get_settings()
    cmd = command.strip()
    if not cmd:
        await _audit(
            request, user=user, command="", classification="unknown",
            allowed=False, executed=False, summary="rejected: empty command",
        )
        return JSONResponse(
            {"ok": False, "error": "empty command", "executed": False}, status_code=400
        )

    kind = rcon.classify(cmd)
    bus.publish(f"$ {cmd}", level="input", source="console")

    # ---- authorisation -----------------------------------------------------
    if kind == "destructive" and not settings.rcon_allow_destructive:
        bus.publish(
            f"rejected destructive command {cmd!r} "
            "(set RCON_ALLOW_DESTRUCTIVE=true to permit)",
            level="warn",
            source="console",
        )
        await _audit(
            request, user=user, command=cmd, classification=kind,
            allowed=False, executed=False,
            summary="blocked: destructive command, RCON_ALLOW_DESTRUCTIVE=false",
        )
        return JSONResponse(
            {
                "ok": False,
                "executed": False,
                "command": cmd,
                "classification": kind,
                "error": (
                    "Command classified as destructive and blocked. "
                    "Set RCON_ALLOW_DESTRUCTIVE=true in .env to permit it."
                ),
            },
            status_code=403,
        )
    if kind == "unknown":
        bus.publish(f"rejected non-allow-listed command {cmd!r}", level="warn", source="console")
        await _audit(
            request, user=user, command=cmd, classification=kind,
            allowed=False, executed=False,
            summary="blocked: not in SAFE_COMMANDS allow-list",
        )
        return JSONResponse(
            {
                "ok": False,
                "executed": False,
                "command": cmd,
                "classification": kind,
                "error": (
                    "Command is not in the read-only allow-list. "
                    "Add it to SAFE_COMMANDS in app/services/rcon.py if it is genuinely safe."
                ),
            },
            status_code=403,
        )

    # ---- rate limit (checked only for commands we would actually run) ------
    wait = _rate_limited(user, settings.rcon_min_interval)
    if wait > 0:
        bus.publish(f"rate-limited command from {user} ({wait}s)", level="warn", source="console")
        await _audit(
            request, user=user, command=cmd, classification=kind,
            allowed=False, executed=False,
            summary=f"rate limited: retry after {wait}s",
        )
        return JSONResponse(
            {
                "ok": False,
                "executed": False,
                "command": cmd,
                "classification": kind,
                "error": f"Rate limited: wait {wait}s before the next command.",
                "retry_after_s": wait,
            },
            status_code=429,
        )

    # ---- stub path (no RCON configured) ------------------------------------
    if not settings.rcon_configured:
        bus.publish(
            "RCON not configured (RCON_PASSWORD empty). Command accepted but NOT executed.",
            level="warn",
            source="console",
        )
        await _audit(
            request, user=user, command=cmd, classification=kind,
            allowed=True, executed=False,
            summary="RCON not configured: accepted as stub, not executed",
        )
        return JSONResponse(
            {
                "ok": True,
                "executed": False,
                "stub": True,
                "command": cmd,
                "classification": kind,
                "note": "RCON_PASSWORD is empty — set it in .env to execute commands for real.",
            }
        )

    # ---- real execution ----------------------------------------------------
    client = rcon.RconClient(
        host=settings.rcon_host,
        port=settings.rcon_port,
        password=settings.rcon_password,
        timeout=settings.rcon_timeout,
    )
    result = await client.execute(cmd)

    if not result.ok:
        bus.publish(f"RCON error: {result.error}", level="error", source="console")
        await _audit(
            request, user=user, command=cmd, classification=kind,
            allowed=True, executed=False,
            summary=f"RCON error: {result.error}",
        )
        return JSONResponse(
            {
                "ok": False,
                "executed": False,
                "command": cmd,
                "classification": kind,
                "error": result.error or "RCON command failed",
            },
            status_code=502,
        )

    for line in (result.body or "(no output)").splitlines():
        bus.publish(line, level="info", source="rcon")

    await _audit(
        request, user=user, command=cmd, classification=kind,
        allowed=True, executed=True,
        summary=result.body or "(no output)",
    )
    return JSONResponse(
        {
            "ok": True,
            "executed": True,
            "stub": False,
            "command": cmd,
            "classification": kind,
            "response": result.body,
            "packet_count": len(result.packets),
        }
    )


@router.get("/api/console/history")
async def console_history(
    request: Request,
    _user: str = Depends(require_user),
) -> JSONResponse:
    lines = [
        {
            "ts": line.ts,
            "level": line.level,
            "source": line.source,
            "message": line.message,
        }
        for line in bus.snapshot()[-100:]
    ]
    return JSONResponse({"ok": True, "lines": lines})


@router.get("/api/console/audit")
async def console_audit(
    request: Request,
    limit: int = 50,
    _user: str = Depends(require_user),
) -> JSONResponse:
    """Persistent RCON audit trail (allowed, refused and rate-limited attempts)."""
    try:
        limit_value = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        return JSONResponse(
            {"ok": False, "error": "limit must be an integer"}, status_code=400
        )
    try:
        rows = await db.rcon_audit_recent(limit_value)
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "error": f"audit query failed: {type(exc).__name__}"},
            status_code=500,
        )
    return JSONResponse(
        {
            "ok": True,
            "limit": limit_value,
            "count": len(rows),
            "entries": [
                {
                    "id": row["id"],
                    "ts": row["ts"],
                    "username": row["username"],
                    "command": row["command"],
                    "classification": row["classification"],
                    "allowed": bool(row["allowed"]),
                    "executed": bool(row["executed"]),
                    "result_summary": row["result_summary"],
                    "client_ip": row["client_ip"],
                }
                for row in rows
            ],
        }
    )
