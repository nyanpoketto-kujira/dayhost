"""SQLite access layer — raw SQL via aiosqlite. No ORM, no migrations framework.

Design notes
------------
* One shared connection per process (WAL + busy_timeout) is enough for a
  single-admin dashboard and keeps idle RSS low.
* Schema is created idempotently at startup (`init_db`).
* Fase 1 stores: users (single admin row), sessions, audit/event log.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Iterable

import aiosqlite

log = logging.getLogger("dayhost.db")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token_hash  TEXT PRIMARY KEY,
    username    TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         INTEGER NOT NULL,
    level      TEXT NOT NULL DEFAULT 'info',
    source     TEXT NOT NULL DEFAULT 'app',
    message    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

-- Fase 3: every console command attempt, including the refused ones. The
-- refusals are the point of an audit trail, so `allowed` is recorded for all.
CREATE TABLE IF NOT EXISTS rcon_audit (
    id            INTEGER PRIMARY KEY,
    ts            INTEGER NOT NULL,
    username      TEXT NOT NULL,
    command       TEXT NOT NULL,
    classification TEXT NOT NULL,
    allowed       INTEGER NOT NULL,
    executed      INTEGER NOT NULL,
    result_summary TEXT,
    client_ip     TEXT
);
CREATE INDEX IF NOT EXISTS idx_rcon_audit_ts ON rcon_audit(ts);
"""

# Audit columns are capped so a hostile/huge RCON response can never bloat the
# database (the DB lives next to the world backups on a small VPS disk).
MAX_AUDIT_COMMAND = 512
MAX_AUDIT_SUMMARY = 500

_conn: aiosqlite.Connection | None = None
_lock = asyncio.Lock()


async def init_db(db_file: Path) -> aiosqlite.Connection:
    """Open (once) and migrate the database. Safe to call repeatedly."""
    global _conn
    async with _lock:
        if _conn is not None:
            return _conn
        db_file.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(db_file)
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA)
        await conn.commit()
        _conn = conn
        return conn


async def get_db() -> aiosqlite.Connection:
    if _conn is None:
        raise RuntimeError("database not initialised; call init_db() first")
    return _conn


async def close_db() -> None:
    global _conn
    async with _lock:
        if _conn is not None:
            await _conn.close()
            _conn = None


# --------------------------------------------------------------------- helpers
async def fetchone(sql: str, params: Iterable[Any] = ()) -> aiosqlite.Row | None:
    conn = await get_db()
    async with conn.execute(sql, tuple(params)) as cur:
        return await cur.fetchone()


async def fetchall(sql: str, params: Iterable[Any] = ()) -> list[aiosqlite.Row]:
    conn = await get_db()
    async with conn.execute(sql, tuple(params)) as cur:
        return list(await cur.fetchall())


async def execute(sql: str, params: Iterable[Any] = ()) -> int:
    conn = await get_db()
    cur = await conn.execute(sql, tuple(params))
    await conn.commit()
    return cur.lastrowid or 0


async def log_event(message: str, level: str = "info", source: str = "app") -> None:
    """Best-effort audit/event insert; never raises into the request path."""
    try:
        await execute(
            "INSERT INTO events (ts, level, source, message) VALUES (?, ?, ?, ?)",
            (int(time.time()), level, source, message),
        )
    except Exception:  # pragma: no cover - logging must not break requests
        pass


async def recent_events(limit: int = 50) -> list[aiosqlite.Row]:
    return await fetchall(
        "SELECT ts, level, source, message FROM events ORDER BY id DESC LIMIT ?",
        (limit,),
    )


async def purge_expired_sessions() -> int:
    conn = await get_db()
    cur = await conn.execute("DELETE FROM sessions WHERE expires_at < ?", (int(time.time()),))
    await conn.commit()
    return cur.rowcount or 0


# ------------------------------------------------------------- rcon audit log
def _clip(value: str | None, limit: int) -> str:
    if not value:
        return ""
    text = str(value)
    return text if len(text) <= limit else text[:limit] + "…[truncated]"


async def rcon_audit_add(
    *,
    username: str,
    command: str,
    classification: str,
    allowed: bool,
    executed: bool,
    result_summary: str | None = None,
    client_ip: str | None = None,
) -> bool:
    """Record one console attempt. Never raises: returns False on failure.

    Audit failures must not break the command they describe, but they must not
    be silent either — the caller sees False and the failure is logged here.
    """
    try:
        await execute(
            "INSERT INTO rcon_audit "
            "(ts, username, command, classification, allowed, executed, "
            " result_summary, client_ip) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(time.time()),
                _clip(username, 64),
                _clip(command, MAX_AUDIT_COMMAND),
                _clip(classification, 32),
                1 if allowed else 0,
                1 if executed else 0,
                _clip(result_summary, MAX_AUDIT_SUMMARY),
                _clip(client_ip, 64),
            ),
        )
        return True
    except Exception as exc:  # audit must never break the request path
        log.warning("rcon audit insert failed (%s): %s", type(exc).__name__, exc)
        return False


async def rcon_audit_recent(limit: int = 50) -> list[aiosqlite.Row]:
    limit = max(1, min(int(limit), 500))
    return await fetchall(
        "SELECT id, ts, username, command, classification, allowed, executed, "
        "result_summary, client_ip FROM rcon_audit ORDER BY id DESC LIMIT ?",
        (limit,),
    )


async def rcon_audit_purge_older_than(days: int) -> int:
    """Delete audit rows older than `days`. 0/negative = keep everything."""
    if days <= 0:
        return 0
    cutoff = int(time.time()) - days * 86400
    conn = await get_db()
    cur = await conn.execute("DELETE FROM rcon_audit WHERE ts < ?", (cutoff,))
    await conn.commit()
    return cur.rowcount or 0
