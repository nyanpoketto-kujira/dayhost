"""Auth: password hashing + signed cookie sessions. No passlib, no bcrypt.

Password format (identical to what scripts/hash_password.py emits):
    pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>

Sessions are opaque random tokens stored (hashed) in SQLite; the cookie only
carries the raw token. This keeps revocation trivial and avoids JWT deps.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from typing import Any

from fastapi import Cookie, HTTPException, Request, status

from .config import Settings, get_settings
from . import db

PBKDF2_ALGO = "pbkdf2_sha256"
PBKDF2_ITERATIONS = 240_000


# ------------------------------------------------------------------- password
def hash_password(password: str, *, iterations: int = PBKDF2_ITERATIONS) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"{PBKDF2_ALGO}${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    """Constant-time verification. Returns False on any malformed input."""
    if not encoded:
        return False
    try:
        algo, iters, salt_hex, hash_hex = encoded.split("$", 3)
    except ValueError:
        return False
    if algo != PBKDF2_ALGO:
        return False
    try:
        iterations = int(iters)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except ValueError:
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return hmac.compare_digest(candidate, expected)


# -------------------------------------------------------------------- session
def _token_hash(token: str, secret: str) -> str:
    return hmac.new(secret.encode(), token.encode(), hashlib.sha256).hexdigest()


async def create_session(username: str, settings: Settings | None = None) -> tuple[str, int]:
    """Create a session row, return (raw_token, expires_at_epoch)."""
    settings = settings or get_settings()
    token = secrets.token_urlsafe(32)
    now = int(time.time())
    expires_at = now + settings.session_ttl
    await db.execute(
        "INSERT INTO sessions (token_hash, username, created_at, expires_at) VALUES (?, ?, ?, ?)",
        (_token_hash(token, settings.session_secret), username, now, expires_at),
    )
    await db.log_event(f"session opened for {username}", source="auth")
    return token, expires_at


async def destroy_session(token: str, settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    await db.execute(
        "DELETE FROM sessions WHERE token_hash = ?",
        (_token_hash(token, settings.session_secret),),
    )


async def resolve_session(token: str, settings: Settings | None = None) -> str | None:
    """Return the username for a valid, unexpired token, else None."""
    settings = settings or get_settings()
    if not token:
        return None
    row = await db.fetchone(
        "SELECT username, expires_at FROM sessions WHERE token_hash = ?",
        (_token_hash(token, settings.session_secret),),
    )
    if row is None:
        return None
    if int(row["expires_at"]) < int(time.time()):
        await db.execute(
            "DELETE FROM sessions WHERE token_hash = ?",
            (_token_hash(token, settings.session_secret),),
        )
        return None
    return str(row["username"])


# ------------------------------------------------------------------ dependency
async def require_user(request: Request) -> str:
    """FastAPI dependency: 401 JSON for API, 303 redirect for HTML pages."""
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie, "")
    username = await resolve_session(token, settings)
    if username:
        request.state.username = username
        return username
    wants_html = "text/html" in (request.headers.get("accept") or "")
    if wants_html:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="not authenticated")


def session_cookie_params(settings: Settings, max_age: int) -> dict[str, Any]:
    return {
        "key": settings.session_cookie,
        "httponly": True,
        "samesite": "lax",
        "secure": settings.cookie_secure,
        "max_age": max_age,
        "path": "/",
    }


__all__ = [
    "hash_password",
    "verify_password",
    "create_session",
    "destroy_session",
    "resolve_session",
    "require_user",
    "session_cookie_params",
    "Cookie",
]
