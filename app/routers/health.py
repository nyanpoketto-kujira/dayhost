"""Health endpoints — unauthenticated, no external I/O (fast + safe to probe)."""
from __future__ import annotations

import time

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..services.logbus import bus

router = APIRouter(tags=["health"])
_STARTED = time.time()


@router.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "status": "ok",
            "uptime_s": round(time.time() - _STARTED, 1),
            "subscribers": bus.subscriber_count,
        }
    )


@router.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness: does the process have the config it needs to be useful?"""
    settings = get_settings()
    return JSONResponse(
        {
            "ok": True,
            "auth_configured": settings.auth_configured,
            "daytona_configured": settings.daytona_configured,
            "env": settings.env,
        }
    )
