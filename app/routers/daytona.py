"""Daytona lifecycle API (JSON). Every failure path returns structured JSON.

Requirement (f): with an empty `.env` these endpoints must answer a tidy
JSON 503 — never a traceback.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from ..auth import require_user
from ..services.daytona_client import get_client

router = APIRouter(prefix="/api/daytona", tags=["daytona"])


def _not_configured(action: str) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "configured": False,
            "action": action,
            "state": "UNKNOWN",
            "error": "Daytona is not configured. Set DAYTONA_API_KEY and SANDBOX_ID in .env.",
        },
        status_code=503,
    )


def _payload(action: str, result: dict[str, Any]) -> JSONResponse:
    body = {"action": action, **result}
    if not result.get("ok"):
        return JSONResponse(body, status_code=502)
    return JSONResponse(body)


@router.get("/status")
async def daytona_status(_user: str = Depends(require_user)) -> JSONResponse:
    client = get_client()
    if not client.configured:
        return _not_configured("status")
    return _payload("status", await client.get_status())


@router.post("/start")
async def daytona_start(_user: str = Depends(require_user)) -> JSONResponse:
    client = get_client()
    if not client.configured:
        return _not_configured("start")
    return _payload("start", await client.start())


@router.post("/stop")
async def daytona_stop(_user: str = Depends(require_user)) -> JSONResponse:
    client = get_client()
    if not client.configured:
        return _not_configured("stop")
    return _payload("stop", await client.stop())


@router.post("/restart")
async def daytona_restart(_user: str = Depends(require_user)) -> JSONResponse:
    client = get_client()
    if not client.configured:
        return _not_configured("restart")
    return _payload("restart", await client.restart())


@router.post("/recover")
async def daytona_recover(_user: str = Depends(require_user)) -> JSONResponse:
    """POST /sandbox/{id}/recover — only meaningful for `state=error` + `recoverable`."""
    client = get_client()
    if not client.configured:
        return _not_configured("recover")
    return _payload("recover", await client.recover())
