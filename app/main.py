"""DayHost — FastAPI application factory + lifespan wiring.

Run (dev):
    .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

IMPORTANT: production must run **one** uvicorn worker (see deploy/dayhost.service)
because app/services/logbus.py keeps subscribers in process memory.
"""
from __future__ import annotations

import logging
import sys
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__, db
from .config import REPO_ROOT, get_settings
from .logging_setup import setup_logging
from .routers import auth as auth_router
from .routers import console, dashboard, daytona, health
from .services.daytona_client import close_client
from .services.logbus import bus

# Logging is configured once, here: rotating file + stderr, with uvicorn's own
# handlers replaced so no line is written twice. `setup_logging` never raises —
# an unwritable log path degrades to stderr with a warning instead of blocking
# startup (the unit runs with ProtectSystem=strict).
_LOG_INFO = setup_logging()
log = logging.getLogger("dayhost")


def build_orchestrator(settings=None):
    """Wire the real orchestrator, two explicit steps.

    Deliberately *not* ``Orchestrator.from_settings(settings, **hooks)``: the
    hook wiring stays readable at the call site, and the classmethod stays a
    general-purpose API (its hook-kwarg regression is fixed separately).
    A module-level factory also gives the Phase-D harness a seam to inject a
    fake without touching production code paths.
    """
    from .services.agent_ws import AgentWSServer
    from .services.orchestrator import Orchestrator

    settings = settings or get_settings()
    orch = Orchestrator(settings)
    orch.ws = AgentWSServer.from_settings(settings, **orch.hooks())
    return orch


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    await db.init_db(settings.db_file)
    settings.backup_path.mkdir(parents=True, exist_ok=True)

    bus.publish(f"DayHost v{__version__} starting (env={settings.env})", source="boot")
    if not settings.auth_configured:
        bus.publish(
            "ADMIN_PASSWORD_HASH / SESSION_SECRET not set — login is disabled. "
            "Run: python scripts/hash_password.py '<password>'",
            level="warn",
            source="boot",
        )
    if not settings.daytona_configured:
        bus.publish(
            "Daytona not configured (DAYTONA_API_KEY / SANDBOX_ID) — power actions return 503.",
            level="warn",
            source="boot",
        )
    if settings.sync_enabled:
        bus.publish(
            "SYNC_ENABLED=true — power actions will run tar/rsync/rclone plans.",
            level="warn",
            source="boot",
        )
        if settings.sync_transport == "daytona-files":
            bus.publish(
                "SYNC_TRANSPORT=daytona-files — the world is pulled/pushed through "
                "the Daytona toolbox file API (streamed).",
                level="warn",
                source="boot",
            )
        elif settings.sync_transport != "local":
            bus.publish(
                f"Unknown SYNC_TRANSPORT={settings.sync_transport!r} — falling back to 'local'.",
                level="warn",
                source="boot",
            )
    else:
        bus.publish(
            "SYNC_ENABLED=false — backup/restore plans are logged, not executed.",
            source="boot",
        )
    if settings.rcon_configured:
        bus.publish(
            f"RCON console enabled -> {settings.rcon_host}:{settings.rcon_port} "
            f"(destructive={settings.rcon_allow_destructive}).",
            level="warn" if settings.rcon_allow_destructive else "info",
            source="boot",
        )
    else:
        bus.publish(
            "RCON_PASSWORD empty — console is a stub (commands are not executed).",
            source="boot",
        )
    await db.purge_expired_sessions()
    purged = await db.rcon_audit_purge_older_than(settings.rcon_audit_retention_days)
    if purged:
        bus.publish(
            f"rcon audit: purged {purged} row(s) older than "
            f"{settings.rcon_audit_retention_days} day(s).",
            source="boot",
        )
    log.info(
        "startup complete; db=%s log=%s level=%s format=%s",
        settings.db_file,
        _LOG_INFO.get("file") or "stderr",
        _LOG_INFO.get("level"),
        _LOG_INFO.get("format"),
    )

    # ---- orchestrator (Fusion Phase D) ------------------------------------
    # ORCH_ENABLED is the master switch. Off -> the package is never imported,
    # so an idle process pays nothing for it (RSS budget) and the panel shows
    # an explicit "disabled" state instead of pretending to be healthy.
    app.state.orch = None
    if settings.orch_enabled:
        orch = None
        try:
            orch = build_orchestrator(settings)
            started = await orch.start()
        except Exception as exc:  # noqa: BLE001 - boot must survive a bad orch
            log.exception("orchestrator failed to start")
            bus.publish(
                f"Orchestrator gagal start ({type(exc).__name__}: {exc}) — panel "
                "menampilkannya sebagai 'disabled'.",
                level="error",
                source="orchestrator",
            )
            if orch is not None:
                try:
                    await orch.close()
                except Exception:  # noqa: BLE001 - shutdown must not raise
                    log.warning("orchestrator: close() after a failed start raised")
            orch = None
        app.state.orch = orch
        if orch is not None:
            bus.publish(
                "Orchestrator aktif: "
                f"agent_ws={started.get('agent_ws')} "
                f"(enabled={started.get('agent_ws_enabled')}), "
                f"started={','.join(started.get('started') or []) or '-'}, "
                f"autostart={started.get('autostart')}.",
                source="orchestrator",
            )
            provision = started.get("provision")
            if provision is not None:
                bus.publish(
                    f"ORCH_AUTOSTART: provisioning -> ok={provision.get('ok')} "
                    f"err={provision.get('error')}",
                    level="info" if provision.get("ok") else "warn",
                    source="orchestrator",
                )
    else:
        bus.publish(
            "ORCH_ENABLED=false — orchestrator, agent WS and snapshots are off "
            "(power actions use the Daytona API directly).",
            source="boot",
        )

    # ---- ssh tunnel (TUNNEL_MODE=ssh) -------------------------------------
    # Imported *inside* the branch on purpose: the module is ~680 lines and
    # pulls asyncio.subprocess, and frp/none deployments never touch it (RSS
    # budget). Boot must survive a bad tunnel: start() does not raise, and the
    # wrapper below covers everything else — failures land in last_error and
    # the panel renders them instead of the process dying.
    app.state.tunnel = None
    if settings.tunnel_mode.strip().lower() == "ssh":
        ssh_tunnel = None
        try:
            from .services.ssh_tunnel import SSHTunnel

            ssh_tunnel = SSHTunnel(settings)
            await ssh_tunnel.start()
            # Bounded readiness wait. `start()` only spawns the supervisor, so a
            # plain `wait_ready(timeout=...)` would hold the lifespan open for
            # the whole window even when the tunnel can never come up (e.g. no
            # DAYTONA_API_KEY) — the dashboard would not serve /healthz until
            # it expired. So wait in short slices and bail out as soon as the
            # supervisor has recorded an error and is not running: boot stays
            # fast on failure, and a healthy tunnel still gets the full window.
            deadline = time.monotonic() + settings.ssh_connect_timeout + 5
            ok = False
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if await ssh_tunnel.wait_ready(timeout=min(0.25, remaining)):
                    ok = True
                    break
                snap = ssh_tunnel.status() or {}
                if snap.get("last_error") and not snap.get("running"):
                    break
            snap = ssh_tunnel.status() or {}
            endpoint = snap.get("public_endpoint") or (
                f"{settings.public_host}:{settings.public_port}"
                if settings.public_host
                else f"{settings.ssh_bind_mc}:{settings.public_port}"
            )
            pid = snap.get("pid")
            if ok:
                bus.publish(
                    f"SSH tunnel aktif: mode=ssh endpoint={endpoint} pid={pid}.",
                    source="tunnel",
                )
            else:
                bus.publish(
                    f"SSH tunnel belum siap: mode=ssh endpoint={endpoint} pid={pid} "
                    f"err={snap.get('last_error') or '-'}.",
                    level="warn",
                    source="tunnel",
                )
            # Kept even when not ready: the supervisor retries in the
            # background and status() is where last_error is visible.
            app.state.tunnel = ssh_tunnel
        except Exception as exc:  # noqa: BLE001 - boot must survive a bad tunnel
            log.exception("ssh tunnel failed to start")
            # Type only, never str(exc): the token lives in ssh credentials and
            # an exception message is the one place it could surface.
            bus.publish(
                f"SSH tunnel gagal start ({type(exc).__name__}) — mode=ssh.",
                level="error",
                source="tunnel",
            )
            if ssh_tunnel is not None:
                try:
                    await ssh_tunnel.stop()
                except Exception:  # noqa: BLE001 - shutdown must not raise
                    log.warning("ssh tunnel: stop() after a failed start raised")
            app.state.tunnel = None

    try:
        yield
    finally:
        bus.publish("DayHost shutting down", level="warn", source="boot")
        orch = getattr(app.state, "orch", None)
        if orch is not None:
            # Stop the listener/tasks BEFORE the HTTP clients go away: close()
            # may still flush a delta or send an agent command.
            try:
                await orch.close()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                log.warning("orchestrator: close() raised during shutdown")
            app.state.orch = None
        # The ssh child must be reaped and its 0600 temp config unlinked before
        # the process exits; the tunnel owns nothing close_client() needs.
        ssh_tunnel = getattr(app.state, "tunnel", None)
        if ssh_tunnel is not None:
            try:
                await ssh_tunnel.stop()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                log.warning("ssh tunnel: stop() raised during shutdown")
            app.state.tunnel = None
        await close_client()
        # Close the toolbox client too, but only if the transport ever loaded it:
        # importing it here would pull httpx into every shutdown path.
        if "app.services.daytona_files" in sys.modules:
            from .services import daytona_files

            await daytona_files.close_client()
        await db.close_db()
        log.info("shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings()
    # Public API docs are a dev convenience. In prod they are extra attack
    # surface (full schema, every route, every parameter) for zero benefit on a
    # single-operator dashboard, so they are switched off entirely (None).
    docs_url = None if settings.is_prod else "/api/docs"
    openapi_url = None if settings.is_prod else "/api/openapi.json"
    app = FastAPI(
        title="DayHost",
        version=__version__,
        description="Minecraft server lifecycle dashboard (Daytona sandbox + FRP tunnel).",
        lifespan=lifespan,
        docs_url=docs_url,
        openapi_url=openapi_url,
        redoc_url=None if settings.is_prod else "/api/redoc",
    )

    static_dir = REPO_ROOT / "app" / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(health.router)
    app.include_router(auth_router.router)
    app.include_router(dashboard.router)
    app.include_router(daytona.router)
    app.include_router(console.router)

    # ---- error handling: always JSON for API, redirect for HTML -------------
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 303 and "location" in {k.lower() for k in exc.headers or {}}:
            return RedirectResponse(exc.headers["Location"], status_code=303)
        wants_html = "text/html" in (request.headers.get("accept") or "")
        if wants_html and request.url.path in {"/", "/login"}:
            return RedirectResponse("/login", status_code=303)
        return JSONResponse(
            {"ok": False, "error": exc.detail, "status": exc.status_code},
            status_code=exc.status_code,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        log.exception("unhandled error on %s", request.url.path)
        bus.publish(f"unhandled error: {type(exc).__name__}: {exc}", level="error", source="app")
        return JSONResponse(
            {"ok": False, "error": "internal server error"}, status_code=500
        )

    return app


app = create_app()
