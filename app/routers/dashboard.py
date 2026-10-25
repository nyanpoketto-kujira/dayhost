"""Dashboard: HTML shell + JSON/HTMX fragments for status, power, SSE logs.

Fusion Phase D adds the orchestrator panel (host/agent partials, manual backup,
HTMX start/stop) plus a **named** SSE bridge (``event: state``). The log frames
keep their old anonymous ``data:`` shape on purpose: ``app/static/js/console.js``
listens on ``source.onmessage``, and per the SSE spec a named event never fires
``onmessage`` — so the console stays clean JSON-free without being touched.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from ..auth import require_user
from ..config import get_settings
from ..db import recent_events
from ..services import mcping, tunnel
from ..services.daytona_client import BUSY_STATES, LIVE_STATES, get_client
from ..services.logbus import bus
from ..templating import templates

router = APIRouter(tags=["dashboard"])


async def gather_status(tunnel_live: dict | None = None) -> dict:
    """Collect every widget signal concurrently. Never raises.

    ``tunnel_live`` is the *live* ``SSHTunnel`` snapshot (see ``_live_tunnel``),
    passed in by the call sites because this function takes no ``Request``. It
    is optional so the three existing callers keep their old signature.
    """
    import asyncio

    settings = get_settings()
    client = get_client()

    daytona_task = asyncio.create_task(_safe(client.get_status()))
    ping_task = asyncio.create_task(
        _safe(mcping.ping(settings.mc_host, settings.mc_port, float(settings.mc_ping_timeout)))
    )
    tunnel_task = asyncio.create_task(_safe(tunnel.probe(settings)))

    daytona = await daytona_task
    ping = await ping_task
    tun = await tunnel_task

    ping_dict = ping.as_dict() if hasattr(ping, "as_dict") else {"online": False, "error": str(ping)}
    tunnel_dict = tun.as_dict() if hasattr(tun, "as_dict") else {"socket_open": False}

    return {
        "ok": True,
        "daytona": daytona,
        "sandbox_live": daytona.get("state") in LIVE_STATES,
        "sandbox_busy": daytona.get("state") in BUSY_STATES,
        "sandbox_error_reason": daytona.get("error_reason"),
        "sandbox_recoverable": daytona.get("recoverable"),
        "sandbox_desired_state": daytona.get("desired_state"),
        "ping": ping_dict,
        "tunnel": tunnel_dict,
        # Live ssh-tunnel snapshot (None for frp/none and for a failed start).
        "tunnel_live": tunnel_live,
        "mc_endpoint": f"{settings.mc_host}:{settings.mc_port}",
        # frp keeps owning this: it comes from the observer, not from ssh.
        "public_endpoint": tunnel_dict.get("public_endpoint", ""),
        "stub_console": not settings.rcon_configured,
        "sync_enabled": settings.sync_enabled,
    }


async def _safe(coro):
    """Await a coroutine, converting any escape into a normalised error dict."""
    try:
        return await coro
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "ok": False,
            "state": "UNKNOWN",
            "error": f"{type(exc).__name__}: {exc}",
            "online": False,
            "socket_open": False,
        }


def _live_tunnel(request: Request) -> dict | None:
    """Live ``SSHTunnel.status()``, or ``None``.

    ``app.state.tunnel`` is ``None`` unless ``TUNNEL_MODE=ssh`` and the start
    succeeded, so the frp/none paths are untouched. Guarded twice: a missing
    attribute and a raising ``status()`` both degrade to ``None`` rather than
    breaking the dashboard.
    """
    tunnel_obj = getattr(getattr(request, "app", None), "state", None)
    tunnel_obj = getattr(tunnel_obj, "tunnel", None)
    if tunnel_obj is None:
        return None
    try:
        snap = tunnel_obj.status()
    except Exception:  # noqa: BLE001 - the panel must never 500 over this
        return None
    return snap if isinstance(snap, dict) else None


# --------------------------------------------------------------------- orch
def _orch(request: Request):
    """The live orchestrator, or a clean 503 (never a 500 / traceback).

    ``ORCH_ENABLED=false`` (and a failed start) leave ``app.state.orch`` as
    ``None``. Every *mutating* orchestrator route goes through here; the two
    read-only partials render an explicit ``disabled`` card instead, so the
    dashboard is honest rather than broken.
    """
    orch = getattr(request.app.state, "orch", None)
    if orch is None:
        raise HTTPException(
            status_code=503,
            detail="Orchestrator nonaktif (ORCH_ENABLED=false atau gagal start).",
        )
    return orch


async def _host_view(request: Request) -> dict:
    """Panel data for both partials. Never raises, never blocks the loop."""
    orch = getattr(request.app.state, "orch", None)
    if orch is None:
        return {"available": False}
    view: dict = {"available": True}
    try:
        state = orch.state
        view["state"] = state.public_state()
        view["ws"] = orch.ws.public_state() if getattr(orch, "ws", None) is not None else {}
        view["counters"] = dict(getattr(orch, "counters", {}) or {})
        cfg = getattr(orch, "store_cfg", None)
        if cfg is not None:
            view["paths"] = {
                "mirror": str(cfg.mirror_dir),
                "snapshots": str(cfg.snapshots_dir),
            }
    except Exception as exc:  # noqa: BLE001 - a panel must never 500
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
    try:
        view["backups"] = await asyncio.to_thread(orch.storage.backups_payload)
    except Exception as exc:  # noqa: BLE001 - empty listing beats a 500
        view["backups"] = {
            "snapshots": [], "deltas": {}, "mirror": {"has_world": False},
            "error": f"{type(exc).__name__}: {exc}",
        }
    return view


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    status = await gather_status(tunnel_live=_live_tunnel(request))
    events = [dict(row) for row in await recent_events(20)]
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "title": "DayHost",
            "username": user,
            "status": status,
            "events": events,
            # Phase D panel context (same data the two partials render).
            "host": await _host_view(request),
            "tunnel": status["tunnel"],
            "public_endpoint": status["public_endpoint"],
        },
    )


@router.get("/api/status")
async def api_status(request: Request, _user: str = Depends(require_user)) -> JSONResponse:
    return JSONResponse(await gather_status(tunnel_live=_live_tunnel(request)))


@router.get("/partials/status", response_class=HTMLResponse)
async def partial_status(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    """HTMX fragment: refreshed every few seconds + after every power action."""
    status = await gather_status(tunnel_live=_live_tunnel(request))
    return templates.TemplateResponse(
        request, "partials/status.html", {"status": status}
    )


@router.get("/partials/host", response_class=HTMLResponse)
async def partial_host(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    """Orchestrator host card (sandbox + MC process + provisioning)."""
    return templates.TemplateResponse(
        request, "partials/host.html", {"host": await _host_view(request)}
    )


@router.get("/partials/agent", response_class=HTMLResponse)
async def partial_agent(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    """Agent-link card + tunnel + snapshot list."""
    settings = get_settings()
    tun = await _safe(tunnel.probe(settings))
    tunnel_dict = tun.as_dict() if hasattr(tun, "as_dict") else {"socket_open": False}
    return templates.TemplateResponse(
        request,
        "partials/agent.html",
        {
            "host": await _host_view(request),
            "tunnel": tunnel_dict,
            "public_endpoint": tunnel_dict.get("public_endpoint", ""),
        },
    )


@router.post("/backup", response_class=HTMLResponse)
async def backup_now(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    """Manual snapshot. An empty mirror is a *normal* answer, not an error."""
    orch = _orch(request)
    bus.publish(f"backup manual diminta oleh {user}", level="info", source="backup")
    try:
        result = await orch.snapshot_now(reason="manual")
    except ValueError as exc:
        # Storage raises ValueError for "no world yet" and for tar/timeout
        # failures. Both are operator-facing messages, never a 500.
        result = {"ok": False, "kind": "invalid", "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - report, do not traceback
        bus.publish(
            f"backup manual gagal: {type(exc).__name__}: {exc}",
            level="error",
            source="backup",
        )
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    skipped = str(result.get("skipped") or "")
    no_world = skipped.startswith("no world") or result.get("kind") == "invalid"
    if not result.get("ok") and no_world:
        return templates.TemplateResponse(
            request,
            "partials/backup_result.html",
            {
                "ok": False,
                "message": "Belum ada data untuk di-backup "
                           "(mirror masih kosong / world/level.dat tidak ada).",
                "detail": result.get("error") or "",
            },
            status_code=200,
        )
    if not result.get("ok"):
        return templates.TemplateResponse(
            request,
            "partials/backup_result.html",
            {"ok": False, "message": "Backup gagal.", "detail": result.get("error") or ""},
            status_code=200,
        )
    return templates.TemplateResponse(
        request,
        "partials/backup_result.html",
        {
            "ok": True,
            "message": f"Snapshot dibuat: {result.get('name')} "
                       f"({result.get('size_human')}).",
            "detail": "",
        },
        status_code=200,
    )


@router.post("/host/start", response_class=HTMLResponse)
async def host_start(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    orch = _orch(request)
    bus.publish(f"provisioning diminta oleh {user}", level="info", source="orchestrator")
    result = await orch.start_host(reason="manual")
    if not result.get("ok"):
        bus.publish(
            f"provisioning gagal: {result.get('error')}",
            level="warn",
            source="orchestrator",
        )
    return await partial_host(request, user)


@router.post("/host/stop", response_class=HTMLResponse)
async def host_stop(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    orch = _orch(request)
    bus.publish(f"stop host diminta oleh {user}", level="info", source="orchestrator")
    result = await orch.stop_host(reason="manual")
    if not result.get("ok"):
        bus.publish(
            f"stop host gagal: {result.get('error')}",
            level="warn",
            source="orchestrator",
        )
    return await partial_host(request, user)


async def _power(action: str, request: Request, user: str) -> HTMLResponse:
    # Lazy: `sync` (backup/restore plan builder + subprocess executor) is the
    # heaviest Fase 2 module and is only reached from power actions. Keeping it
    # out of the startup import graph removes it from idle RSS entirely.
    from ..services import sync

    settings = get_settings()
    client = get_client()

    bus.publish(f"power action: {action} requested by {user}", level="info", source="power")

    if not client.configured:
        # Requirement (f): clean JSON 503, never a traceback.
        return JSONResponse(
            {
                "ok": False,
                "error": "Daytona is not configured. Set DAYTONA_API_KEY and SANDBOX_ID in .env.",
                "state": "UNKNOWN",
                "action": action,
            },
            status_code=503,
        )

    # ---- pre-stop safety snapshot -----------------------------------------
    # Called on every stop/restart. With SYNC_ENABLED=false (default) this only
    # builds and logs the plan, so production behaviour is unchanged. When it
    # IS enabled, a failed backup aborts the power-off: losing the world is
    # worse than a failed stop.
    if action in {"off", "restart"}:
        backup = await sync.pre_stop_backup(settings)
        if settings.sync_enabled and not backup.get("ok"):
            bus.publish(
                f"power {action} aborted: pre-stop backup failed "
                f"({backup.get('error') or 'see logs'})",
                level="error",
                source="power",
            )
            return JSONResponse(
                {
                    "ok": False,
                    "error": (
                        "Pre-stop backup failed; refusing to stop the sandbox "
                        "while SYNC_ENABLED=true."
                    ),
                    "state": "UNKNOWN",
                    "action": action,
                },
                status_code=502,
            )

    if action == "on":
        result = await client.start()
    elif action == "off":
        result = await client.stop()
    elif action == "restart":
        result = await client.restart()
    else:
        return JSONResponse(
            {"ok": False, "error": f"unknown action {action!r}"}, status_code=400
        )

    bus.publish(
        f"power {action} -> ok={result.get('ok')} state={result.get('state')} "
        f"err={result.get('error')}",
        level="info" if result.get("ok") else "warn",
        source="power",
    )

    if not result.get("ok"):
        return JSONResponse(
            {
                "ok": False,
                "error": result.get("error") or "Daytona call failed",
                "state": result.get("state", "UNKNOWN"),
                "action": action,
            },
            status_code=502,
        )

    # ---- post-start restore ------------------------------------------------
    if action in {"on", "restart"}:
        restore = await sync.post_start_restore(settings)
        if settings.sync_enabled and not restore.get("ok"):
            bus.publish(
                f"post-start restore failed: {restore.get('error') or 'see logs'}",
                level="error",
                source="power",
            )

    return await partial_status(request, user)


@router.post("/power/on", response_class=HTMLResponse)
async def power_on(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    return await _power("on", request, user)


@router.post("/power/off", response_class=HTMLResponse)
async def power_off(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    return await _power("off", request, user)


@router.post("/power/restart", response_class=HTMLResponse)
async def power_restart(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    return await _power("restart", request, user)


@router.post("/power/recover", response_class=HTMLResponse)
async def power_recover(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    """Recover a sandbox stuck in `state=error` (only offered when recoverable)."""
    client = get_client()
    bus.publish(f"recover requested by {user}", level="info", source="power")
    if not client.configured:
        return JSONResponse(
            {
                "ok": False,
                "error": "Daytona is not configured. Set DAYTONA_API_KEY and SANDBOX_ID in .env.",
                "state": "UNKNOWN",
                "action": "recover",
            },
            status_code=503,
        )
    result = await client.recover()
    bus.publish(
        f"recover -> ok={result.get('ok')} state={result.get('state')} err={result.get('error')}",
        level="info" if result.get("ok") else "warn",
        source="power",
    )
    if not result.get("ok"):
        return JSONResponse(
            {
                "ok": False,
                "error": result.get("error") or "Daytona recover failed",
                "state": result.get("state", "UNKNOWN"),
                "action": "recover",
            },
            status_code=502,
        )
    return await partial_status(request, user)


@router.get("/events")
async def sse_events(request: Request, _user: str = Depends(require_user)) -> StreamingResponse:
    """Server-Sent Events log stream. Heartbeats every 15s keep it alive."""
    queue = await bus.subscribe()
    return StreamingResponse(
        bus.stream(queue),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _state_frames(request: Request):
    """Yield ``event: state`` frames (named!), plus comment keepalives.

    Named events are invisible to ``source.onmessage`` (SSE spec), which is
    exactly why the old console keeps working untouched. Payload is one JSON
    object per line, never raw log text.
    """
    last = None
    ticks = 0
    while True:
        if await request.is_disconnected():
            return
        orch = getattr(request.app.state, "orch", None)
        if orch is None:
            payload = {"available": False, "reason": "disabled"}
        else:
            try:
                payload = {"available": True, "state": orch.state.public_state()}
            except Exception as exc:  # noqa: BLE001 - a stream must not die
                payload = {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        if body != last:
            last = body
            yield f"event: state\ndata: {body}\n\n"
            ticks = 0
        else:
            ticks += 1
            if ticks >= 15:      # ~15 s with no change: keep the pipe warm
                ticks = 0
                yield ": keepalive\n\n"
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            raise


@router.get("/events/state")
async def sse_state(request: Request, _user: str = Depends(require_user)) -> StreamingResponse:
    """Orchestrator state stream for the Phase-D panel (``addEventListener``)."""
    return StreamingResponse(
        _state_frames(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
