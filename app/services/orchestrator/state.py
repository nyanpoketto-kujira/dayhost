"""In-memory application state and browser broadcast fan-out.

Ported from ``mc-orchestrator/controller/state.py`` (Fusion Phase C) with two
deliberate changes:

  * **Two independent status fields** (Ner's decision #1). ``mc_state`` is the
    PaperMC *process* state and is only ever set from agent messages
    (``hello`` / ``mc_state`` / ``heartbeat``). ``sandbox_state`` is the Daytona
    *lifecycle* state and is only ever set by :mod:`app.services.orchestrator.core`
    from driver results. Neither setter touches the other; a lifecycle stop must
    not silently claim the JVM exited, and an agent ``hello`` must not claim the
    sandbox is running.
  * **Transport-agnostic writes.** The original called aiohttp's
    ``ws.send_str``. Here the agent handle is whatever
    :class:`app.services.agent_ws.AgentWSServer` (or a fake in the harness)
    exposes: ``send_json`` is preferred, ``send_str`` accepted. Browser
    broadcast is identical in spirit but Phase D owns the actual wiring.

No aiohttp, no FastAPI. Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Any

from .util import clean_line, utc_now

# Agent-authoritative PaperMC process states.
MC_STATES = ("unknown", "starting", "running", "stopping", "stopped", "error")

# Daytona-authoritative sandbox lifecycle states. Canonical uppercase values,
# exactly the set produced by ``daytona_client.normalise_state`` (see
# ``scripts/check_state_map.py`` for the full input mapping).
SANDBOX_STATES = (
    "UNKNOWN",
    "STARTING",
    "RUNNING",
    "STOPPING",
    "STOPPED",
    "ARCHIVED",
    "ERROR",
    "BUSY",
)


class AppState:
    """Everything the panel needs, in memory, with a bounded log ring."""

    def __init__(self, log_buffer: int = 1500) -> None:
        # Link state: is the agent WS connected? (unchanged semantics)
        self.status = "disconnected"  # online | disconnected | recovering
        # Agent-authoritative: PaperMC process.
        self.mc_state = "unknown"
        # Daytona-authoritative: sandbox lifecycle. Separate field, on purpose.
        self.sandbox_state = "UNKNOWN"
        self.sandbox_id = ""
        self.agent_info: dict[str, Any] = {}
        self.agent_ws: Any = None
        self.connected = False
        self.last_seen = 0.0  # monotonic
        self.last_seen_wall = 0.0
        self.disconnected_since = time.monotonic()
        self.started_at = utc_now()
        self.started_mono = time.monotonic()
        self.logs: deque[dict[str, str]] = deque(maxlen=max(200, log_buffer))
        self.browsers: set[Any] = set()
        self.agent_tasks: dict[str, Any] = {}
        self.recovery: dict[str, Any] = {
            "active": False,
            "attempt": 0,
            "step": "idle",
            "started_at": None,
            "updated_at": None,
            "last_error": None,
        }
        self.provisioning: dict[str, Any] = {
            "active": False,
            "step": "idle",
            "started_at": None,
            "updated_at": None,
            "last_error": None,
            "steps": [],
        }
        self.backups: dict[str, Any] = {"snapshots": [], "deltas": {}, "mirror": {}}
        self.plugins: list[dict[str, Any]] = []
        self.snapshot_interval = 6 * 3600
        self._broadcast_lock = asyncio.Lock()
        self._agent_send_lock = asyncio.Lock()

    # ------------------------------------------------------------------ agent
    def touch_agent(self) -> None:
        self.last_seen = time.monotonic()
        self.last_seen_wall = time.time()

    def mark_agent_connected(self, ws: Any) -> None:
        self.agent_ws = ws
        self.connected = True
        self.disconnected_since = 0.0
        self.touch_agent()
        self.status = "online"

    def mark_agent_disconnected(self, ws: Any = None) -> None:
        if ws is not None and self.agent_ws is not ws:
            return
        self.agent_ws = None
        self.connected = False
        self.disconnected_since = time.monotonic()
        if self.status != "recovering":
            self.status = "disconnected"

    async def send_agent(self, payload: dict[str, Any]) -> bool:
        """Serialize writes to the agent link (commands, plugin sync, ...).

        Accepts either an ``AgentWSServer``-like object (``send_json``) or a
        connection object exposing ``send_str`` (the aiohttp shape). Either way
        the payload is serialised once, under a lock, and a closed link is
        reported as ``False`` rather than raised.
        """
        ws = self.agent_ws
        if ws is None:
            return False
        closed = bool(getattr(ws, "closed", False))
        if closed:
            return False
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        async with self._agent_send_lock:
            if ws is not self.agent_ws or getattr(ws, "closed", False):
                return False
            try:
                send_json = getattr(ws, "send_json", None)
                if callable(send_json):
                    return bool(await send_json(payload))
                send_str = getattr(ws, "send_str", None)
                if callable(send_str):
                    await send_str(data)
                    return True
                return False
            except Exception:  # noqa: BLE001 - caller decides how to handle loss
                return False

    # ---------------------------------------------------------------- logging
    def make_entry(self, text: str, source: str = "system") -> dict[str, str]:
        return {"ts": utc_now(), "source": source, "text": clean_line(text)}

    async def log(self, text: str, source: str = "system", broadcast: bool = True) -> None:
        entry = self.make_entry(text, source)
        self.logs.append(entry)
        if broadcast:
            await self.broadcast({"type": "log", "entries": [entry]})

    async def log_many(self, entries: list[dict[str, str]]) -> None:
        cleaned = []
        for entry in entries[:500]:
            text = clean_line(str(entry.get("text", "")))
            if not text:
                continue
            cleaned.append(
                {
                    "ts": str(entry.get("ts") or utc_now()),
                    "source": str(entry.get("source") or "mc"),
                    "text": text,
                }
            )
        if not cleaned:
            return
        self.logs.extend(cleaned)
        await self.broadcast({"type": "log", "entries": cleaned})

    # --------------------------------------------------------------- status
    async def set_mc_state(self, mc_state: str) -> None:
        """PaperMC process state. Agent-authored only."""
        if mc_state == self.mc_state:
            return
        self.mc_state = mc_state
        await self.push_state()

    async def set_sandbox_state(self, sandbox_state: str) -> None:
        """Daytona lifecycle state. Driver-authored only. Never touches mc_state."""
        if sandbox_state == self.sandbox_state:
            return
        self.sandbox_state = sandbox_state
        await self.push_state()

    def recovery_update(self, **changes: Any) -> None:
        self.recovery.update(changes)
        self.recovery["updated_at"] = utc_now()

    def provisioning_update(self, *, step: str | None = None, **changes: Any) -> None:
        if step is not None:
            self.provisioning["step"] = step
            self.provisioning["steps"].append({"step": step, "at": utc_now()})  # type: ignore[union-attr]
        self.provisioning.update(changes)
        self.provisioning["updated_at"] = utc_now()

    async def push_recovery(self) -> None:
        await self.broadcast({"type": "recovery", "recovery": dict(self.recovery)})

    async def push_state(self) -> None:
        await self.broadcast({"type": "state", "state": self.public_state()})

    def public_state(self) -> dict[str, Any]:
        last_seen_ago = (
            round(time.monotonic() - self.last_seen, 1) if self.last_seen else None
        )
        return {
            "status": self.status,
            # Two separate, never-merged fields (Ner #1).
            "mc_state": self.mc_state,
            "sandbox_state": self.sandbox_state,
            "sandbox_id": self.sandbox_id,
            "connected": self.connected,
            "agent": self.agent_info,
            "last_seen": self.last_seen_wall or None,
            "last_seen_ago": last_seen_ago,
            "server_time": utc_now(),
            "controller_started_at": self.started_at,
            "controller_uptime": round(time.monotonic() - self.started_mono, 1),
            "snapshot_interval": self.snapshot_interval,
            "recovery": dict(self.recovery),
            "provisioning": {
                **dict(self.provisioning),
                "steps": list(self.provisioning.get("steps") or [])[-20:],
            },
        }

    def snapshot_payload(self, backups: dict[str, Any], plugins: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "state": self.public_state(),
            "logs": list(self.logs)[-300:],
            "backups": backups,
            "plugins": plugins,
        }

    # ------------------------------------------------------------- broadcast
    async def broadcast(self, payload: dict[str, Any]) -> None:
        if not self.browsers:
            return
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        dead = []
        async with self._broadcast_lock:
            for ws in list(self.browsers):
                try:
                    send_json = getattr(ws, "send_json", None)
                    if callable(send_json):
                        await send_json(payload)
                    else:
                        await ws.send_str(data)
                except Exception:  # noqa: BLE001 - connection is gone
                    dead.append(ws)
        for ws in dead:
            self.browsers.discard(ws)

    async def broadcast_backups(self, backups: dict[str, Any]) -> None:
        self.backups = backups
        await self.broadcast({"type": "backups", "backups": backups})

    async def broadcast_plugins(self, plugins: list[dict[str, Any]]) -> None:
        self.plugins = plugins
        await self.broadcast({"type": "plugins", "plugins": plugins})
