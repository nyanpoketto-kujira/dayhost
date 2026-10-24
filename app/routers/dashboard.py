"""Dashboard: HTML shell + JSON/HTMX fragments for status, power, SSE logs."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from ..auth import require_user
from ..config import get_settings
from ..db import recent_events
from ..services import mcping, tunnel
from ..services.daytona_client import BUSY_STATES, LIVE_STATES, get_client
from ..services.logbus import bus
from ..templating import templates

router = APIRouter(tags=["dashboard"])


async def gather_status() -> dict:
    """Collect every widget signal concurrently. Never raises."""
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
        "mc_endpoint": f"{settings.mc_host}:{settings.mc_port}",
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


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, user: str = Depends(require_user)) -> HTMLResponse:
    status = await gather_status()
    events = [dict(row) for row in await recent_events(20)]
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "title": "DayHost",
            "username": user,
            "status": status,
            "events": events,
        },
    )


@router.get("/api/status")
async def api_status(request: Request, _user: str = Depends(require_user)) -> JSONResponse:
    return JSONResponse(await gather_status())


@router.get("/partials/status", response_class=HTMLResponse)
async def partial_status(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    """HTMX fragment: refreshed every few seconds + after every power action."""
    status = await gather_status()
    return templates.TemplateResponse(
        request, "partials/status.html", {"status": status}
    )


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
