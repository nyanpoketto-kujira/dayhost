"""World backup/restore — allows building plans AND executing them safely.

Lifecycle:
    pre_stop_backup()    -> snapshot the world BEFORE stopping the sandbox
    post_start_restore() -> push the snapshot back AFTER starting

Two layers on purpose:

1. **Plan builders** (`build_backup_plan` / `build_restore_plan`) — pure, no
   I/O, no side effects. They have been stable since Fase 1 and stay unchanged
   in shape, so the operator can always inspect exactly what would run.

2. **Executor** (`run_plan`) — added in Fase 2:
   * `asyncio.create_subprocess_exec` only, **never** `shell=True`, never a
     string command line.
   * **Binary allow-list**: only `mkdir`, `tar`, `rsync`, `rclone`. Anything
     else is refused before spawning (the plan is data, not code).
   * Per-step timeout (`SYNC_TIMEOUT`, default 180 s) → killed and reported.
   * stdout/stderr captured (bounded) and returned; never raised to the caller.
   * Missing binary → logged as a **warning**, step skipped, no crash. On this
     host `tar` and `rsync` exist; `rclone` and `frpc` do not.

Kill-switch: `SYNC_ENABLED=false` (default). When false, `pre_stop_backup()` /
`post_start_restore()` only build + log the plan, exactly like Fase 1 — so
wiring them into the power flow does not silently change production behaviour.

Fase 3 adds a **transport** layer (`SYNC_TRANSPORT`):

    local           (default) — Fase 2 behaviour, byte-for-byte unchanged.
    daytona-files             — the world is pulled out of the sandbox through
                                the Daytona toolbox file API before the local
                                archive is built, and pushed back after the
                                restore. Streaming both ways (see
                                app/services/daytona_files.py).

The kill-switch binds **every** mode: with `SYNC_ENABLED=false` the
`daytona-files` transport makes no network calls at all, it only reports the
plan it would run.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
from dataclasses import dataclass, field
from typing import Any

from ..config import Settings, get_settings

log = logging.getLogger("dayhost.sync")

# Only these binaries may ever be spawned. The plan is data; this is the gate.
ALLOWED_BINARIES = frozenset({"mkdir", "tar", "rsync", "rclone"})

# Keep captured output small: sync output can be enormous on a big world.
MAX_CAPTURED_BYTES = 64 * 1024

TRANSPORT_LOCAL = "local"
TRANSPORT_DAYTONA_FILES = "daytona-files"
TRANSPORTS = frozenset({TRANSPORT_LOCAL, TRANSPORT_DAYTONA_FILES})


@dataclass
class SyncPlan:
    direction: str  # "backup" | "restore"
    steps: list[list[str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "direction": self.direction,
            "steps": [shlex.join(step) for step in self.steps],
            "notes": self.notes,
            "executed": False,
        }


@dataclass
class StepResult:
    argv: list[str]
    ok: bool
    skipped: bool = False
    returncode: int | None = None
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    duration_ms: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "argv": shlex.join(self.argv),
            "ok": self.ok,
            "skipped": self.skipped,
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


# --------------------------------------------------------------------- planners
def build_backup_plan(
    settings: Settings | None = None,
    *,
    local_only: bool = False,
    world_src: str | None = None,
) -> SyncPlan:
    """Build the tar/rsync/rclone commands needed to snapshot the world."""
    settings = settings or get_settings()
    stage = settings.backup_path
    world_src = world_src or settings.sync_world_src
    world_parent = os.path.dirname(world_src.rstrip("/")) or "/data"
    world_name = os.path.basename(world_src.rstrip("/")) or "world"
    archive = str(stage / "world-latest.tar.gz")
    plan = SyncPlan(direction="backup")

    plan.steps.append(["mkdir", "-p", str(stage)])
    plan.steps.append(["tar", "-czf", archive, "-C", world_parent, world_name])
    plan.notes.append(
        "tar runs locally against a staged copy. Sandbox -> VPS transfer is the "
        "operator's tunnel's job (see TUNNEL_MODE)."
    )
    if settings.rclone_remote:
        plan.steps.append(
            ["rclone", "copy", archive, settings.rclone_remote, "--stats-one-line"]
        )
        plan.notes.append("rclone target taken from RCLONE_REMOTE.")
    else:
        plan.notes.append("RCLONE_REMOTE empty -> local archive only.")

    if local_only:
        plan.notes.append(
            "SYNC_TRANSPORT=daytona-files -> sandbox transfer is done by the "
            "toolbox file API, so the ssh/rsync step is omitted."
        )
    elif settings.tunnel_mode.lower() == "ssh":
        plan.steps.append(
            [
                "rsync", "-az", "--partial",
                f"{settings.public_host}:{world_src}/",
                str(stage / "world/"),
            ]
        )
        plan.notes.append("rsync over the operator-managed SSH tunnel.")
    else:
        plan.notes.append(
            "Tunnel mode is not 'ssh' -> rsync step omitted (operator manages FRP)."
        )
    return plan


def build_restore_plan(settings: Settings | None = None, *, local_only: bool = False) -> SyncPlan:
    """Build the commands needed to push a snapshot back into the sandbox."""
    settings = settings or get_settings()
    stage = settings.backup_path
    archive = str(stage / "world-latest.tar.gz")
    world_dst = settings.sync_world_src
    plan = SyncPlan(direction="restore")

    if settings.rclone_remote:
        plan.steps.append(["rclone", "copy", settings.rclone_remote, str(stage)])
    plan.steps.append(["tar", "-xzf", archive, "-C", settings.sync_world_dst])
    # The staged tree only exists when a previous ssh-mode backup populated it.
    # Adding the rsync step unconditionally makes `rsync` exit 23 on a plain
    # tar-only setup, aborting an otherwise successful restore.
    staged_world = stage / "world"
    if local_only:
        plan.notes.append(
            "SYNC_TRANSPORT=daytona-files -> the staged tree is pushed back by "
            "the toolbox file API, so the rsync overlay is omitted."
        )
    elif staged_world.is_dir():
        plan.steps.append(
            [
                "rsync", "-az", "--partial", "--delete",
                f"{staged_world}/", f"{world_dst.rstrip('/')}/",
            ]
        )
        plan.notes.append(f"rsync overlay from staged tree {staged_world}.")
    else:
        plan.notes.append(
            f"No staged tree at {staged_world} -> rsync step omitted (tar only)."
        )
    plan.notes.append("Order matters: restore BEFORE the server accepts players.")
    return plan


# --------------------------------------------------------------------- executor
def _clip(text: bytes) -> str:
    if len(text) > MAX_CAPTURED_BYTES:
        return text[:MAX_CAPTURED_BYTES].decode("utf-8", "replace") + "\n…[truncated]"
    return text.decode("utf-8", "replace")


async def _run_step(argv: list[str], timeout: float) -> StepResult:
    """Run one argv, honouring the allow-list. Never raises."""
    if not argv:
        return StepResult(argv=argv, ok=False, skipped=True, error="empty step")

    binary = argv[0]
    if binary not in ALLOWED_BINARIES:
        log.warning("sync: refusing non-allow-listed binary %r", binary)
        return StepResult(
            argv=argv,
            ok=False,
            skipped=True,
            error=f"binary {binary!r} is not in the allow-list {sorted(ALLOWED_BINARIES)}",
        )

    resolved = shutil.which(binary)
    if resolved is None:
        # Warning, not crash: a missing optional tool (rclone here) must not
        # take the power flow down.
        log.warning("sync: binary %r not found in PATH; step skipped: %s", binary, shlex.join(argv))
        return StepResult(
            argv=argv, ok=False, skipped=True, error=f"{binary} not found in PATH"
        )

    started = asyncio.get_running_loop().time()
    try:
        proc = await asyncio.create_subprocess_exec(
            resolved,
            *argv[1:],
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (OSError, ValueError) as exc:
        log.warning("sync: failed to spawn %r: %s", binary, exc)
        return StepResult(argv=argv, ok=False, error=f"spawn failed: {exc}")

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except Exception:
            pass
        elapsed = int((asyncio.get_running_loop().time() - started) * 1000)
        log.warning("sync: %r timed out after %ss", binary, timeout)
        return StepResult(
            argv=argv, ok=False, error=f"timeout after {timeout}s", duration_ms=elapsed
        )

    elapsed = int((asyncio.get_running_loop().time() - started) * 1000)
    rc = proc.returncode
    result = StepResult(
        argv=argv,
        ok=(rc == 0),
        returncode=rc,
        stdout=_clip(out or b""),
        stderr=_clip(err or b""),
        error=None if rc == 0 else f"exit code {rc}",
        duration_ms=elapsed,
    )
    if rc != 0:
        log.warning("sync: %s exited %s: %s", shlex.join(argv), rc, result.stderr.strip()[:400])
    return result


async def run_plan(
    plan: SyncPlan,
    settings: Settings | None = None,
    *,
    require_enabled: bool = True,
    transport_stage: Any = None,
) -> dict[str, Any]:
    """Execute every step of a plan. Stops at the first failure.

    Returns a structured report; never raises. `executed` is False when the
    kill-switch is off, so callers can log "planned only" unambiguously.

    `transport_stage` is an optional zero-arg async callable run **before** the
    steps (backup: pull the world out) or **after** them (restore: push it
    back). That is how the `daytona-files` transport hooks in without touching
    the plan builders.
    """
    settings = settings or get_settings()
    payload: dict[str, Any] = {
        **plan.as_dict(),
        "enabled": settings.sync_enabled,
        "transport": settings.sync_transport,
        "steps_total": len(plan.steps),
        "results": [],
        "transport_result": None,
    }

    if require_enabled and not settings.sync_enabled:
        log.info(
            "sync.%s: SYNC_ENABLED=false -> plan only (%d steps, nothing executed)",
            plan.direction,
            len(plan.steps),
        )
        payload["note"] = "SYNC_ENABLED=false — plan built, nothing executed."
        return payload

    # ---- pre-step transport (backup: pull the world out of the sandbox) ---
    if transport_stage is not None and plan.direction == "backup":
        tr = await transport_stage()
        payload["transport_result"] = tr
        if not tr.get("ok"):
            payload["ok"] = False
            payload["executed"] = False
            payload["steps_completed"] = 0
            payload["error"] = tr.get("error") or "transport failed"
            log.warning("sync.backup transport failed: %s", payload["error"])
            return payload

    executed_any = False
    for step in plan.steps:
        result = await _run_step(step, float(settings.sync_timeout))
        payload["results"].append(result.as_dict())
        if result.skipped:
            # A skipped step means a required tool is absent or the step was
            # refused. Logged as a warning, reported as a failure: claiming
            # "backup ok" when tar never ran would be a lie.
            payload["ok"] = False
            payload["error"] = result.error
            payload["executed"] = executed_any
            payload["skipped_step"] = True
            payload["steps_completed"] = len(payload["results"]) - 1
            log.warning(
                "sync.%s incomplete at step %d/%d: %s",
                plan.direction, len(payload["results"]), len(plan.steps), result.error,
            )
            return payload
        executed_any = True
        if not result.ok:
            payload["ok"] = False
            payload["error"] = result.error
            payload["executed"] = executed_any
            payload["steps_completed"] = len(payload["results"]) - 1
            log.warning(
                "sync.%s aborted at step %d/%d: %s",
                plan.direction, len(payload["results"]), len(plan.steps), result.error,
            )
            return payload

    payload["ok"] = True
    payload["executed"] = executed_any

    # ---- post-step transport (restore: push the world back) ---------------
    if transport_stage is not None and plan.direction == "restore":
        tr = await transport_stage()
        payload["transport_result"] = tr
        if not tr.get("ok"):
            payload["ok"] = False
            payload["error"] = tr.get("error") or "transport failed"
            log.warning("sync.restore transport failed: %s", payload["error"])
    return payload


# ------------------------------------------------------------------ entrypoints
TRANSPORT_LOCAL_NOTE = (
    "SYNC_TRANSPORT=%s is not a known transport; falling back to 'local'."
)


def _files_mode(settings: Settings) -> bool:
    """True when the daytona-files transport is selected (else local)."""
    transport = (settings.sync_transport or TRANSPORT_LOCAL).strip().lower()
    if transport not in TRANSPORTS:
        log.warning(TRANSPORT_LOCAL_NOTE, transport)
        return False
    return transport == TRANSPORT_DAYTONA_FILES


async def _transport_ready(settings: Settings) -> dict[str, Any]:
    """Is the daytona-files transport usable? Structured error when not."""
    from . import daytona_files  # lazy: brings httpx in only when needed

    if not settings.sandbox_id:
        return {
            "ok": False,
            "transport": TRANSPORT_DAYTONA_FILES,
            "error": (
                "SYNC_TRANSPORT=daytona-files requires SANDBOX_ID, plus either "
                "DAYTONA_TOOLBOX_URL or a reachable lifecycle API for discovery."
            ),
        }
    client = daytona_files.get_client()
    if client.auth_mode == "invalid":
        return {
            "ok": False,
            "transport": TRANSPORT_DAYTONA_FILES,
            "error": (
                f"DAYTONA_TOOLBOX_AUTH={settings.daytona_toolbox_auth!r} is invalid; "
                "expected 'bearer' or 'none'."
            ),
        }
    base = await client.base_url()
    if base is None:
        return {
            "ok": False,
            "transport": TRANSPORT_DAYTONA_FILES,
            "error": (
                "daytona file transport not configured: set DAYTONA_TOOLBOX_URL, or "
                "make the lifecycle API reachable so /toolbox-proxy-url can resolve."
            ),
        }
    return {"ok": True, "transport": TRANSPORT_DAYTONA_FILES, "base_url": base}


def _remote_root(settings: Settings) -> str:
    return (settings.sync_remote_world or settings.sync_world_src or "/data/world").rstrip("/") or "/"


def _archive_local_path(settings: Settings):
    remote = settings.sync_remote_archive.rstrip("/")
    name = os.path.basename(remote) or "world.tar.gz"
    return settings.backup_path / name


async def pull_world(settings: Settings) -> dict[str, Any]:
    """Download the sandbox world into the staging dir, streaming to disk."""
    from . import daytona_files

    ready = await _transport_ready(settings)
    if not ready.get("ok"):
        return ready
    client = daytona_files.get_client()
    stage = settings.backup_path
    try:
        stage.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {
            "ok": False, "transport": TRANSPORT_DAYTONA_FILES,
            "error": f"cannot create staging dir {stage}: {exc}",
        }

    # Single-archive mode: one streamed download, no directory walk.
    if settings.sync_remote_archive:
        target = _archive_local_path(settings)
        result = await client.download_file(settings.sync_remote_archive, target)
        return {
            **result,
            "transport": TRANSPORT_DAYTONA_FILES,
            "mode": "archive",
            "files": 1 if result.get("ok") else 0,
        }

    root = _remote_root(settings)
    dest_root = stage / (os.path.basename(root) or "world")
    entries: list[dict[str, Any]] = []
    async for entry in client.walk_files(root):
        if "_error" in entry:
            err = entry["_error"]
            return {
                "ok": False,
                "transport": TRANSPORT_DAYTONA_FILES,
                "mode": "tree",
                "error": err.get("error") or "remote listing failed",
                "kind": err.get("kind"),
                "path": entry.get("path"),
            }
        entries.append(entry)

    downloaded = 0
    total_bytes = 0
    errors: list[str] = []
    for entry in entries:
        remote = entry["path"]
        rel = remote[len(root):].lstrip("/") if remote.startswith(root) else remote.lstrip("/")
        if not rel:
            continue
        result = await client.download_file(remote, dest_root / rel)
        if result.get("ok"):
            downloaded += 1
            total_bytes += int(result.get("size") or 0)
        else:
            errors.append(f"{remote}: {result.get('error')}")
            if result.get("kind") == "auth":
                break
    if errors:
        return {
            "ok": False, "transport": TRANSPORT_DAYTONA_FILES, "mode": "tree",
            "files": downloaded, "bytes": total_bytes,
            "error": errors[0], "errors": errors[:10],
        }
    return {
        "ok": True, "transport": TRANSPORT_DAYTONA_FILES, "mode": "tree",
        "files": downloaded, "bytes": total_bytes, "local_root": str(dest_root),
    }


async def push_world(settings: Settings) -> dict[str, Any]:
    """Upload the staged world back into the sandbox, streaming from disk."""
    from . import daytona_files

    ready = await _transport_ready(settings)
    if not ready.get("ok"):
        return ready
    client = daytona_files.get_client()

    if settings.sync_remote_archive:
        local = _archive_local_path(settings)
        if not local.is_file():
            return {
                "ok": False, "transport": TRANSPORT_DAYTONA_FILES,
                "error": f"local archive missing: {local}",
            }
        result = await client.upload_file(local, settings.sync_remote_archive)
        return {**result, "transport": TRANSPORT_DAYTONA_FILES, "mode": "archive"}

    root = _remote_root(settings)
    local_root = settings.backup_path / (os.path.basename(root) or "world")
    if not local_root.is_dir():
        return {
            "ok": False,
            "transport": TRANSPORT_DAYTONA_FILES,
            "error": (
                f"no staged world tree at {local_root}; a daytona-files restore "
                "needs the files pulled during backup, or SYNC_REMOTE_ARCHIVE."
            ),
        }
    uploaded = 0
    total_bytes = 0
    errors: list[str] = []
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        remote = f"{root}/{path.relative_to(local_root).as_posix()}"
        result = await client.upload_file(path, remote)
        if result.get("ok"):
            uploaded += 1
            total_bytes += int(result.get("size") or 0)
        else:
            errors.append(f"{remote}: {result.get('error')}")
            if result.get("kind") == "auth":
                break
    if errors:
        return {
            "ok": False, "transport": TRANSPORT_DAYTONA_FILES, "mode": "tree",
            "files": uploaded, "bytes": total_bytes,
            "error": errors[0], "errors": errors[:10],
        }
    return {
        "ok": True, "transport": TRANSPORT_DAYTONA_FILES, "mode": "tree",
        "files": uploaded, "bytes": total_bytes, "remote_root": root,
    }


async def pre_stop_backup(
    settings: Settings | None = None, *, force: bool = False
) -> dict[str, Any]:
    """Backup before stopping. Plans always; executes only when enabled."""
    settings = settings or get_settings()
    files_mode = _files_mode(settings)
    plan = build_backup_plan(settings, local_only=files_mode)
    stage = (lambda: pull_world(settings)) if files_mode else None
    if force:
        return await run_plan(plan, settings, require_enabled=False, transport_stage=stage)
    return await run_plan(plan, settings, transport_stage=stage)


async def post_start_restore(
    settings: Settings | None = None, *, force: bool = False
) -> dict[str, Any]:
    """Restore after starting. Plans always; executes only when enabled."""
    settings = settings or get_settings()
    files_mode = _files_mode(settings)
    plan = build_restore_plan(settings, local_only=files_mode)
    stage = (lambda: push_world(settings)) if files_mode else None
    if force:
        return await run_plan(plan, settings, require_enabled=False, transport_stage=stage)
    return await run_plan(plan, settings, transport_stage=stage)
