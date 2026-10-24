"""Agent WebSocket server — RFC 6455, **stdlib only**.

Fusion Phase B. DayHost's venv has no ``websockets``/``wsproto``, so FastAPI's
``@app.websocket`` answers 404 ("No supported WebSocket library detected").
This module is the server half of the hand-rolled framing that already exists
and is proven in ``mc-orchestrator/agent/agent.py`` (``ws_frame`` / ``WSConn`` /
``ws_connect``). Zero new dependencies, one extra listening socket.

Design notes (deliberate):
  * **Library only.** Importing this module has no side effects: no socket, no
    task, no config read. :meth:`AgentWSServer.start` / :meth:`stop` are
         explicit and nothing wires them from ``app/main.py`` yet (Phase C does).
  * **One active agent.** A newer connection replaces the older one, which is
    closed with a clear reason — same policy as
    ``mc-orchestrator/controller/state.py``.
  * **Own pub/sub.** Outbound: :meth:`AgentWSServer.send_json` (serialised by a
    per-connection ``asyncio.Lock``). Inbound: :meth:`AgentWSServer.subscribe`
    hands out bounded ``asyncio.Queue``s. No broker, no threads.
  * **Ordered ``on_event`` dispatch.** Each connection owns one FIFO queue and
    one worker task; the worker calls ``on_event`` *one event at a time*. A
    per-message ``ensure_future`` would let ``hello`` / ``mc_state`` /
    ``heartbeat`` overtake each other and let a stale ``mc_state`` win.
  * **Never raises into the loop.** Every per-connection failure is logged and
    turns into a close frame; the accept callback has a catch-all.
  * **Auth is mandatory.** A configured token is compared with
    ``hmac.compare_digest``. With no token configured the server still starts
    but rejects *every* connection (503) and says why, loudly, at ``start()``.

Event envelope published to subscribers (all dicts, ``type`` always present):

    {"type": "hello", ...}            agent's own message, parsed from JSON
    {"type": <agent type>, ...}       any other JSON object with a "type"
    {"type": "_agent_message", "payload": {...}}   JSON without a "type"
    {"type": "_agent_text", "text": "..."}         non-JSON text frame
    {"type": "_agent_binary", "size": N}           binary frame (not decoded)
    {"type": "_agent_connected", "session": {...}}
    {"type": "_agent_replaced", "reason": "..."}
    {"type": "_agent_disconnected", "reason": "...", "idle": 1.2}
    {"type": "_agent_stale", "idle": 76.4}

The ``_agent_*`` names are reserved for the transport itself so Phase C can
react (recovery hook) without parsing agent payloads.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import logging
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

__all__ = [
    "WS_GUID",
    "AgentConnection",
    "AgentSession",
    "AgentWSServer",
    "WSProtocolError",
]

# RFC 6455 §1.3. Same constant the agent uses.
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA

DEFAULT_MAX_FRAME = 4 * 1024 * 1024
DEFAULT_PING_INTERVAL = 20.0
DEFAULT_STALE_TIMEOUT = 75.0
EVENT_QUEUE_MAX = 1000
HANDSHAKE_TIMEOUT = 10.0
MAX_HANDSHAKE_BYTES = 16 * 1024

# Close codes we emit.
CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_PROTOCOL_ERROR = 1002
CLOSE_UNSUPPORTED_DATA = 1003
CLOSE_INVALID_PAYLOAD = 1007
CLOSE_TOO_BIG = 1009

log = logging.getLogger("dayhost.agent_ws")


class WSProtocolError(Exception):
    """A peer violated RFC 6455; carries the close code we answer with."""

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


# --------------------------------------------------------------------- helpers


def accept_key(client_key: str) -> str:
    """``base64(sha1(key + GUID))`` — RFC 6455 §4.2.2."""
    digest = hashlib.sha1((client_key + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def _tokens(value: str) -> set[str]:
    return {part.strip().lower() for part in (value or "").split(",") if part.strip()}


def _frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    """Build one **unmasked** frame (servers MUST NOT mask — RFC 6455 §5.1)."""
    header = bytearray()
    header.append((0x80 if fin else 0x00) | (opcode & 0x0F))
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))
    return bytes(header) + payload


def _http_response(status: int, reason: str, body: str,
                   extra: Iterable[tuple[str, str]] = ()) -> bytes:
    payload = body.encode("utf-8")
    lines = [
        f"HTTP/1.1 {status} {reason}",
        "Content-Type: text/plain; charset=utf-8",
        f"Content-Length: {len(payload)}",
        "Connection: close",
    ]
    lines.extend(f"{name}: {value}" for name, value in extra)
    head = "\r\n".join(lines) + "\r\n\r\n"
    return head.encode("latin-1") + payload


@dataclass
class AgentSession:
    """What the rest of the app may know about the connected agent."""

    id: str = ""
    hostname: str = ""
    peer: str = ""
    version: str = ""
    mc_state: str = "unknown"
    connected_at: float = 0.0      # wall clock
    connected_mono: float = 0.0    # monotonic
    hello: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "hostname": self.hostname,
            "peer": self.peer,
            "version": self.version,
            "mc_state": self.mc_state,
            "connected_at": self.connected_at,
            "connected_mono": self.connected_mono,
            "uptime": round(time.monotonic() - self.connected_mono, 1)
            if self.connected_mono else 0.0,
            "hello": dict(self.hello),
        }


# ---------------------------------------------------------------- connection


class AgentConnection:
    """One accepted socket: framing plus a serialised writer."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        peer: str,
        max_frame: int,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.peer = peer
        self.max_frame = max_frame
        self.closed = False
        self.connected_at = time.time()
        self.connected_mono = time.monotonic()
        self.last_rx = self.connected_mono
        self.last_pong = 0.0
        self.close_code: int | None = None
        self.close_reason = ""
        self.frames_in = 0
        self.bytes_in = 0
        self._send_lock = asyncio.Lock()
        # Serialised on_event pipeline (one queue + one worker per connection).
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=EVENT_QUEUE_MAX
        )
        self.event_task: asyncio.Task | None = None
        self.events_dropped = 0

    # ---------------------------------------------------------------- outbound
    async def send_frame(self, opcode: int, payload: bytes) -> bool:
        if self.closed:
            return False
        if len(payload) > self.max_frame:
            log.warning(
                "agent ws: refusing to send %d-byte %#x frame to %s (cap %d)",
                len(payload), opcode, self.peer, self.max_frame,
            )
            return False
        try:
            async with self._send_lock:
                if self.closed:
                    return False
                self.writer.write(_frame(opcode, payload))
                await self.writer.drain()
            return True
        except (ConnectionError, asyncio.IncompleteReadError, RuntimeError) as exc:
            log.info("agent ws: send to %s failed (%s)", self.peer, exc)
            self.closed = True
            return False

    async def send_text(self, text: str) -> bool:
        return await self.send_frame(OP_TEXT, text.encode("utf-8"))

    async def send_json(self, payload: dict[str, Any]) -> bool:
        try:
            data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            log.warning("agent ws: unserialisable payload (%s)", exc)
            return False
        return await self.send_text(data)

    async def ping(self, payload: bytes = b"") -> bool:
        return await self.send_frame(OP_PING, payload[:125])

    async def pong(self, payload: bytes = b"") -> bool:
        return await self.send_frame(OP_PONG, payload[:125])

    async def close(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        """Send a close frame, then tear the socket down. Idempotent."""
        if self.closed:
            return
        self.closed = True
        self.close_code = code
        self.close_reason = reason
        body = struct.pack("!H", code & 0xFFFF) + reason.encode("utf-8")[:120]
        try:
            self.writer.write(_frame(OP_CLOSE, body))
            await self.writer.drain()
        except Exception:  # noqa: BLE001 - the peer may already be gone
            pass
        await self._shutdown_transport()

    async def _shutdown_transport(self) -> None:
        self.closed = True
        try:
            self.writer.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            await self.writer.wait_closed()
        except Exception:  # noqa: BLE001 - already-closed transports raise variously
            pass

    # ----------------------------------------------------------------- inbound
    def touch(self) -> None:
        self.last_rx = time.monotonic()

    # ---------------------------------------------------------- event pipeline
    def enqueue_event(self, event: dict[str, Any]) -> bool:
        """Queue one event for the serial worker. Never blocks the reader."""
        try:
            self.events.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self.events_dropped += 1
            log.warning(
                "agent ws: event queue for %s is full (%d) — dropping %r",
                self.peer, EVENT_QUEUE_MAX, event.get("type"),
            )
            return False

    def start_event_worker(self, handler: Callable[[dict[str, Any]], Any]) -> None:
        """Start the single consumer that calls ``handler`` one event at a time."""
        if self.event_task is not None:
            return
        self.event_task = asyncio.ensure_future(self._event_worker(handler))

    async def _event_worker(self, handler: Callable[[dict[str, Any]], Any]) -> None:
        while True:
            event = await self.events.get()
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad event must not stop the rest
                log.warning(
                    "agent ws: on_event failed for %r from %s (%s: %s)",
                    event.get("type"), self.peer, type(exc).__name__, exc,
                )
            finally:
                self.events.task_done()

    async def drain_events(self, timeout: float = 2.0) -> None:
        """Wait (bounded) for events already queued to be handled."""
        if self.event_task is None:
            return
        try:
            await asyncio.wait_for(self.events.join(), timeout=timeout)
        except (asyncio.TimeoutError, Exception):  # noqa: BLE001
            pass

    async def stop_event_worker(self) -> None:
        task, self.event_task = self.event_task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass

    async def read_frame(self) -> tuple[int, bool, bytes]:
        """Read one raw frame. Returns ``(opcode, fin, payload)``."""
        head = await self.reader.readexactly(2)
        b0, b1 = head[0], head[1]
        fin = bool(b0 & 0x80)
        if b0 & 0x70:
            raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "reserved bits set")
        opcode = b0 & 0x0F
        if not b1 & 0x80:
            raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "client frame not masked")
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack("!H", await self.reader.readexactly(2))[0]
        elif length == 127:
            raw = await self.reader.readexactly(8)
            if raw[0] & 0x80:
                raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "invalid 64-bit length")
            length = struct.unpack("!Q", raw)[0]
        if length > self.max_frame:
            # Checked *before* reading the mask/payload so an absurd claim is
            # rejected without allocating anything.
            raise WSProtocolError(
                CLOSE_TOO_BIG, f"frame too large ({length} > {self.max_frame})"
            )
        if opcode >= 0x8:
            if not fin:
                raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "fragmented control frame")
            if length > 125:
                raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "control payload > 125")
        if opcode not in (OP_CONT, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG):
            raise WSProtocolError(
                CLOSE_PROTOCOL_ERROR, f"unsupported opcode {opcode:#x}"
            )
        mask = await self.reader.readexactly(4)
        payload = await self.reader.readexactly(length) if length else b""
        if length:
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        self.frames_in += 1
        self.bytes_in += length
        return opcode, fin, payload


# ------------------------------------------------------------------- the server


class AgentWSServer:
    """One-listener WebSocket endpoint for exactly one active agent."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        path: str = "/agent-ws",
        token: str = "",
        ping_interval: float = DEFAULT_PING_INTERVAL,
        stale_timeout: float = DEFAULT_STALE_TIMEOUT,
        max_frame: int = DEFAULT_MAX_FRAME,
        handshake_timeout: float = HANDSHAKE_TIMEOUT,
        history: int = 50,
        queue_size: int = 500,
        on_event: Callable[[dict[str, Any]], Any] | None = None,
        on_stale: Callable[[dict[str, Any]], Any] | None = None,
        on_connect: Callable[[AgentSession], Any] | None = None,
        on_disconnect: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.path = "/" + (path or "/agent-ws").lstrip("/")
        self._token = (token or "").strip()
        self._ping_interval = float(ping_interval)
        self._stale_timeout = float(stale_timeout)
        self._max_frame = int(max_frame) if int(max_frame) > 0 else DEFAULT_MAX_FRAME
        self._handshake_timeout = float(handshake_timeout)

        # Hooks (Phase C wires recovery / state push here).
        self._on_event = on_event
        self._on_stale = on_stale
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect

        # Registry: one active agent, plus the live connections to close on stop.
        self.session: AgentSession | None = None
        self.agent_info: dict[str, Any] = {}
        self.last_seen = 0.0            # monotonic, last frame from the agent
        self.last_seen_wall = 0.0
        self.last_pong = 0.0
        self.connected = False
        self.stats: dict[str, int] = {
            "connections": 0,
            "rejected": 0,
            "replaced": 0,
            "messages": 0,
            "stale_closes": 0,
            "protocol_closes": 0,
        }

        self._server: asyncio.AbstractServer | None = None
        self._conn: AgentConnection | None = None
        self._conns: set[AgentConnection] = set()
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._history: deque[dict[str, Any]] = deque(maxlen=max(1, history))
        self._queue_size = max(1, queue_size)
        self._sub_lock = asyncio.Lock()

    # ------------------------------------------------------------------ wiring
    @classmethod
    def from_settings(cls, settings: Any, **kwargs: Any) -> "AgentWSServer":
        """Build from ``app.config.Settings`` without importing it.

        Kept duck-typed so this module stays importable on its own (no dotenv
        load, no side effects).
        """
        get = getattr
        token = get(settings, "agent_ws_effective_token", "") or get(
            settings, "agent_ws_token", ""
        ) or get(settings, "agent_token", "")
        params: dict[str, Any] = {
            "host": get(settings, "agent_ws_host", "127.0.0.1"),
            "port": get(settings, "agent_ws_port", 8765),
            "path": get(settings, "agent_ws_path", "/agent-ws"),
            "token": token,
            "ping_interval": get(
                settings, "agent_ws_ping_interval", DEFAULT_PING_INTERVAL
            ),
            "stale_timeout": get(
                settings, "agent_ws_stale_timeout", DEFAULT_STALE_TIMEOUT
            ),
            "max_frame": get(settings, "agent_ws_max_frame", DEFAULT_MAX_FRAME),
        }
        params.update(kwargs)  # explicit overrides win
        return cls(**params)

    @property
    def running(self) -> bool:
        return self._server is not None and self._server.is_serving()

    @property
    def auth_configured(self) -> bool:
        return bool(self._token)

    @property
    def bound_port(self) -> int:
        """Actual port (resolves ``port=0``)."""
        if self._server is None or not self._server.sockets:
            return self.port
        try:
            return int(self._server.sockets[0].getsockname()[1])
        except (OSError, IndexError):
            return self.port

    # --------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Bind and serve. Idempotent; raises only for a real bind failure."""
        if self.running:
            return
        if not self._token:
            log.error(
                "agent ws: AGENT_WS_TOKEN and AGENT_TOKEN are both empty — the "
                "server will start but REJECT every connection (503). Set one "
                "before Phase C wires this in."
            )
        if self._ping_interval <= 0:
            log.warning("agent ws: ping interval <= 0 — heartbeat disabled")
        elif self._stale_timeout <= self._ping_interval:
            log.warning(
                "agent ws: stale timeout %.1fs <= ping interval %.1fs — "
                "connections may be reaped before a pong can arrive",
                self._stale_timeout, self._ping_interval,
            )
        self._server = await asyncio.start_server(
            self._client_connected, self.host, self.port, limit=65536,
        )
        self.port = self.bound_port
        log.info(
            "agent ws: listening on ws://%s:%d%s (auth=%s, max_frame=%d)",
            self.host, self.port, self.path,
            "token" if self._token else "MISSING", self._max_frame,
        )

    async def stop(self) -> None:
        """Close the listener, then every live connection. Idempotent."""
        server, self._server = self._server, None
        if server is not None:
            server.close()
        # Close live connections BEFORE wait_closed(): since Python 3.12
        # wait_closed() blocks until every connection handler has returned, so
        # waiting first would stall for the full timeout on an active agent.
        conns = list(self._conns)
        self._conns.clear()
        self._conn = None
        self.connected = False
        for conn in conns:
            await conn.close(CLOSE_GOING_AWAY, "server shutting down")
        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=5)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                pass
        self.session = None
        log.info("agent ws: stopped (port %s released)", self.port)

    # ------------------------------------------------------------ outbound API
    async def send_json(self, payload: dict[str, Any]) -> bool:
        """Send one JSON object to the active agent. False when not connected."""
        conn = self._conn
        if conn is None or conn.closed:
            return False
        ok = await conn.send_json(payload)
        if ok and isinstance(payload, dict) and payload.get("type") == "command":
            log.debug("agent ws: command %s sent", payload.get("id"))
        return ok

    async def send_text(self, text: str) -> bool:
        conn = self._conn
        if conn is None or conn.closed:
            return False
        return await conn.send_text(text)

    async def ping(self, payload: bytes = b"") -> bool:
        conn = self._conn
        if conn is None or conn.closed:
            return False
        return await conn.ping(payload)

    async def close_agent(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        conn, self._conn = self._conn, None
        self.connected = False
        if conn is not None:
            await conn.close(code, reason)

    # ------------------------------------------------------------- inbound API
    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        """Register a bounded event queue. Drain it, then unsubscribe."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    def snapshot(self) -> list[dict[str, Any]]:
        """Most recent events (bounded), oldest first."""
        return list(self._history)

    def public_state(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "connected": self.connected,
            "agent": self.session.as_dict() if self.session else {},
            "agent_info": dict(self.agent_info),
            "last_seen": self.last_seen_wall or None,
            "last_seen_ago": round(time.monotonic() - self.last_seen, 1)
            if self.last_seen else None,
            "stats": dict(self.stats),
        }

    # --------------------------------------------------------------- internals
    def _publish(
        self, event: dict[str, Any], conn: AgentConnection | None = None
    ) -> str:
        """Fan out to subscribers, then hand the event to ``on_event``.

        When the event came from a connection, it is appended to that
        connection's FIFO queue and handled by its single worker task, so
        ``hello``/``mc_state``/``heartbeat`` keep wire order. Without an owning
        connection (server-side transport events) the hook is still dispatched
        best-effort via :meth:`_spawn_hook`.

        Returns ``"queued"``, ``"spawned"``, or ``"none"``. Never blocks,
        never raises.
        """
        if "type" not in event:
            event = {"type": "_agent_message", "payload": event}
        self._history.append(event)
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except Exception:  # noqa: BLE001
                    pass
            except Exception:  # noqa: BLE001
                pass
        if self._on_event is None:
            return "none"
        if conn is not None and conn.event_task is not None:
            return "queued" if conn.enqueue_event(event) else "none"
        self._spawn_hook(self._on_event, event)
        return "spawned"

    def _dispatch_event(self, event: dict[str, Any]) -> Any:
        """Serial-worker entry point: resolve the hook, per event."""
        hook = self._on_event
        return None if hook is None else hook(event)

    def _spawn_hook(self, hook: Callable[[Any], Any], arg: Any) -> None:
        """Hooks may be sync or async; a bad hook must not kill the transport."""
        try:
            result = hook(arg)
        except Exception as exc:  # noqa: BLE001
            log.warning("agent ws: hook %r raised %s", getattr(hook, "__name__", hook), exc)
            return
        if asyncio.iscoroutine(result):
            task = asyncio.ensure_future(result)

            def _done(t: asyncio.Future) -> None:
                if t.cancelled():
                    return
                exc = t.exception()
                if exc is not None:
                    log.warning("agent ws: async hook failed (%s)", exc)

            task.add_done_callback(_done)

    # ---------------------------------------------------------- accept callback
    async def _client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """asyncio entry point. Must never let an exception escape."""
        conn: AgentConnection | None = None
        try:
            conn = await self._handshake(reader, writer)
            if conn is not None:
                await self._serve(conn)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport must survive anything
            log.warning("agent ws: connection error (%s: %s)", type(exc).__name__, exc)
            log.debug("agent ws: traceback", exc_info=True)
        finally:
            if conn is not None:
                try:
                    await conn.close(CLOSE_GOING_AWAY, "connection ended")
                except Exception:  # noqa: BLE001
                    pass
                self._conns.discard(conn)
            else:
                try:
                    writer.close()
                except Exception:  # noqa: BLE001
                    pass

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, dict[str, str]]:
        blob = await asyncio.wait_for(
            reader.readuntil(b"\r\n\r\n"), timeout=self._handshake_timeout
        )
        if len(blob) > MAX_HANDSHAKE_BYTES:
            raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "handshake too large")
        text = blob.decode("latin-1", "replace")
        lines = text.split("\r\n")
        request_line = lines[0] if lines else ""
        parts = request_line.split(" ")
        if len(parts) < 2:
            raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "malformed request line")
        method, target = parts[0], parts[1]
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line:
                continue
            if ":" not in line:
                raise WSProtocolError(CLOSE_PROTOCOL_ERROR, "malformed header")
            name, value = line.split(":", 1)
            key = name.strip().lower()
            headers[key] = f"{headers[key]}, {value.strip()}" if key in headers else value.strip()
        return method, target, headers

    async def _reject(
        self, writer: asyncio.StreamWriter, status: int, reason: str, body: str,
        extra: Iterable[tuple[str, str]] = (),
    ) -> None:
        try:
            writer.write(_http_response(status, reason, body, extra))
            await writer.drain()
        except Exception:  # noqa: BLE001
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass

    async def _handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> AgentConnection | None:
        """Return a live connection, or None after answering with an error."""
        peer = ""
        try:
            peer = writer.get_extra_info("peername")
            peer = f"{peer[0]}:{peer[1]}" if isinstance(peer, tuple) else str(peer)
        except Exception:  # noqa: BLE001
            peer = "?"

        try:
            method, target, headers = await self._read_request(reader)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            log.info("agent ws: %s sent no complete handshake", peer)
            await self._reject(writer, 408, "Request Timeout", "handshake timeout\n")
            return None
        except (asyncio.LimitOverrunError, WSProtocolError) as exc:
            log.info("agent ws: bad handshake from %s (%s)", peer, exc)
            await self._reject(writer, 400, "Bad Request", "malformed handshake\n")
            return None

        # --- auth first: never reveal whether a path exists to a stranger.
        supplied = headers.get("x-agent-token", "")
        if not supplied:
            query_token = ""
            if "?" in target:
                for part in target.split("?", 1)[1].split("&"):
                    name, _, value = part.partition("=")
                    if name == "token":
                        query_token = value
                        break
            supplied = query_token
        supplied = supplied.strip()

        if not self._token:
            self.stats["rejected"] += 1
            log.warning(
                "agent ws: rejected %s — no token configured "
                "(AGENT_WS_TOKEN/AGENT_TOKEN empty)", peer,
            )
            await self._reject(
                writer, 503, "Service Unavailable",
                "agent websocket auth not configured\n",
            )
            return None
        if not supplied or not hmac.compare_digest(supplied, self._token):
            self.stats["rejected"] += 1
            log.warning(
                "agent ws: rejected %s — %s token",
                peer, "missing" if not supplied else "invalid",
            )
            await self._reject(writer, 401, "Unauthorized", "invalid agent token\n")
            return None

        # --- protocol upgrade checks.
        async def fail(status: int, reason: str, body: str,
                       extra: Iterable[tuple[str, str]] = ()) -> None:
            log.info("agent ws: handshake rejected for %s — %s", peer, body.strip())
            self.stats["rejected"] += 1
            await self._reject(writer, status, reason, body, extra)

        if method.upper() != "GET":
            await fail(405, "Method Not Allowed", "websocket handshake requires GET\n")
            return None
        if "websocket" not in _tokens(headers.get("upgrade", "")):
            await fail(400, "Bad Request", "missing Upgrade: websocket\n")
            return None
        if "upgrade" not in _tokens(headers.get("connection", "")):
            await fail(400, "Bad Request", "missing Connection: Upgrade\n")
            return None
        if headers.get("sec-websocket-version", "").strip() != "13":
            await fail(
                426, "Upgrade Required", "unsupported websocket version\n",
                extra=(("Sec-WebSocket-Version", "13"),),
            )
            return None
        client_key = headers.get("sec-websocket-key", "").strip()
        if not client_key:
            await fail(400, "Bad Request", "missing Sec-WebSocket-Key\n")
            return None
        try:
            if len(base64.b64decode(client_key, validate=True)) != 16:
                raise ValueError("key is not 16 bytes")
        except (ValueError, TypeError):
            await fail(400, "Bad Request", "invalid Sec-WebSocket-Key\n")
            return None

        wanted = self.path.rstrip("/") or "/"
        request_path = target.split("?", 1)[0].rstrip("/") or "/"
        if request_path != wanted:
            await fail(404, "Not Found", f"unknown websocket path {request_path}\n")
            return None

        try:
            writer.write(
                (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept_key(client_key)}\r\n"
                    "\r\n"
                ).encode("ascii")
            )
            await writer.drain()
        except (ConnectionError, OSError) as exc:
            log.info("agent ws: %s vanished during upgrade (%s)", peer, exc)
            try:
                writer.close()
            except Exception:  # noqa: BLE001
                pass
            return None

        conn = AgentConnection(
            reader, writer, peer=peer, max_frame=self._max_frame
        )
        self.stats["connections"] += 1

        # Replace any previous agent (mc-orchestrator/state.py policy).
        old, self._conn = self._conn, conn
        if old is not None and old is not conn:
            self.stats["replaced"] += 1
            log.warning("agent ws: %s replaces %s", peer, old.peer)
            self._publish(
                {"type": "_agent_replaced", "reason": "new agent connection",
                 "peer": old.peer}
            )
            try:
                await old.close(CLOSE_NORMAL, "replaced by new agent connection")
            except Exception:  # noqa: BLE001
                pass
            self._conns.discard(old)

        self._conns.add(conn)
        self.connected = True
        self.agent_info = {}
        self.session = AgentSession(
            peer=peer,
            connected_at=conn.connected_at,
            connected_mono=conn.connected_mono,
        )
        self.last_seen = conn.connected_mono
        self.last_seen_wall = conn.connected_at
        log.info("agent ws: agent connected from %s", peer)
        self._publish({"type": "_agent_connected", "session": self.session.as_dict()})
        if self._on_connect is not None:
            self._spawn_hook(self._on_connect, self.session)
        return conn

    # ------------------------------------------------------------- read/deliver
    async def _serve(self, conn: AgentConnection) -> None:
        heartbeat = (
            asyncio.ensure_future(self._heartbeat(conn))
            if self._ping_interval > 0 else None
        )
        # One consumer per connection: on_event sees events in wire order.
        conn.start_event_worker(self._dispatch_event)
        reason = "connection closed"
        try:
            reason = await self._read_loop(conn)
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                try:
                    await heartbeat
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            # Flush what the agent already sent before tearing the worker down.
            await conn.drain_events(timeout=2.0)
            await conn.stop_event_worker()
            idle = max(0.0, time.monotonic() - conn.last_rx)
            if self._conn is conn:
                self._conn = None
                self.connected = False
            self._conns.discard(conn)
            log.info("agent ws: agent %s disconnected (%s)", conn.peer, reason)
            self._publish(
                {"type": "_agent_disconnected", "reason": reason,
                 "peer": conn.peer, "idle": round(idle, 1)}
            )
            if self._on_disconnect is not None:
                self._spawn_hook(
                    self._on_disconnect,
                    {"reason": reason, "peer": conn.peer, "idle": round(idle, 1)},
                )

    async def _read_loop(self, conn: AgentConnection) -> str:
        frag_opcode: int | None = None
        frag_buf = bytearray()
        while True:
            try:
                opcode, fin, payload = await conn.read_frame()
            except WSProtocolError as exc:
                self.stats["protocol_closes"] += 1
                log.info(
                    "agent ws: protocol error from %s — %s (close %d)",
                    conn.peer, exc.reason, exc.code,
                )
                await conn.close(exc.code, exc.reason)
                return f"protocol error: {exc.reason}"
            except asyncio.CancelledError:
                raise  # never swallow cancellation: shutdown must stay prompt
            except (asyncio.IncompleteReadError, ConnectionError):
                return "socket closed"
            except OSError as exc:
                return f"socket error: {exc}"

            conn.touch()
            self.last_seen = conn.last_rx
            self.last_seen_wall = time.time()

            if opcode == OP_PING:
                await conn.pong(payload)
                continue
            if opcode == OP_PONG:
                conn.last_pong = time.monotonic()
                self.last_pong = conn.last_pong
                continue
            if opcode == OP_CLOSE:
                code = CLOSE_NORMAL
                text = ""
                if len(payload) >= 2:
                    code = struct.unpack("!H", payload[:2])[0]
                    text = payload[2:].decode("utf-8", "replace")
                await conn.close(code if code != 1005 else CLOSE_NORMAL, "")
                return f"peer close {code} {text}".strip()

            if opcode in (OP_TEXT, OP_BINARY):
                if frag_opcode is not None:
                    await conn.close(CLOSE_PROTOCOL_ERROR, "new message inside fragment")
                    self.stats["protocol_closes"] += 1
                    return "protocol error: interleaved data frame"
                if fin:
                    await self._deliver(conn, opcode, payload)
                else:
                    frag_opcode, frag_buf = opcode, bytearray(payload)
                continue

            if opcode == OP_CONT:
                if frag_opcode is None:
                    await conn.close(CLOSE_PROTOCOL_ERROR, "unexpected continuation")
                    self.stats["protocol_closes"] += 1
                    return "protocol error: orphan continuation"
                frag_buf.extend(payload)
                if len(frag_buf) > self._max_frame:
                    self.stats["protocol_closes"] += 1
                    await conn.close(CLOSE_TOO_BIG, "message too large")
                    return "protocol error: fragmented message too large"
                if fin:
                    data = bytes(frag_buf)
                    kind, frag_opcode, frag_buf = frag_opcode, None, bytearray()
                    await self._deliver(conn, kind, data)
                continue

    async def _deliver(self, conn: AgentConnection, opcode: int, payload: bytes) -> None:
        """Turn one complete message into a pub/sub event."""
        self.stats["messages"] += 1
        if opcode == OP_BINARY:
            self._publish({"type": "_agent_binary", "size": len(payload)}, conn)
            return
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            self.stats["protocol_closes"] += 1
            await conn.close(CLOSE_INVALID_PAYLOAD, "invalid utf-8")
            return
        if not text:
            return
        try:
            message = json.loads(text)
        except (ValueError, TypeError):
            self._publish({"type": "_agent_text", "text": text[:2000]}, conn)
            return
        if not isinstance(message, dict):
            self._publish({"type": "_agent_message", "payload": message}, conn)
            return
        kind = message.get("type")
        if kind == "hello":
            self._absorb_hello(conn, message)
        self._publish(message if isinstance(kind, str) else
                      {"type": "_agent_message", "payload": message}, conn)

    def _absorb_hello(self, conn: AgentConnection, message: dict[str, Any]) -> None:
        """Mirror the fields the panel cares about into the session registry."""
        self.agent_info = dict(message)
        session = self.session
        if session is None:
            return
        session.hello = dict(message)
        session.hostname = str(message.get("hostname") or "")
        session.version = str(message.get("version") or "")
        session.mc_state = str(message.get("mc_state") or "unknown")
        session.id = str(message.get("id") or session.hostname or conn.peer)
        log.info(
            "agent ws: hello from %s (v%s, mc=%s)",
            session.hostname or conn.peer, session.version or "?", session.mc_state,
        )

    # ---------------------------------------------------------------- heartbeat
    async def _heartbeat(self, conn: AgentConnection) -> None:
        try:
            while not conn.closed:
                await asyncio.sleep(self._ping_interval)
                if conn.closed:
                    return
                idle = time.monotonic() - conn.last_rx
                if self._stale_timeout > 0 and idle > self._stale_timeout:
                    self.stats["stale_closes"] += 1
                    log.warning(
                        "agent ws: %s stale (%.1fs > %.1fs) — closing",
                        conn.peer, idle, self._stale_timeout,
                    )
                    info = {
                        "type": "_agent_stale",
                        "peer": conn.peer,
                        "idle": round(idle, 1),
                        "session": self.session.as_dict() if self.session else {},
                    }
                    self._publish(info)
                    if self._on_stale is not None:
                        self._spawn_hook(self._on_stale, info)
                    await conn.close(CLOSE_GOING_AWAY, "stale heartbeat")
                    return
                if not await conn.ping(b""):
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never kill the loop
            log.warning("agent ws: heartbeat for %s failed (%s)", conn.peer, exc)
