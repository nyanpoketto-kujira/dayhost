"""DayHost — FastAPI application factory + lifespan wiring.

Run (dev):
    .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

IMPORTANT: production must run **one** uvicorn worker (see deploy/dayhost.service)
because app/services/logbus.py keeps subscribers in process memory.
"""
from __future__ import annotations

import logging
import sys
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

    try:
        yield
    finally:
        bus.publish("DayHost shutting down", level="warn", source="boot")
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
