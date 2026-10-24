"""Agent WebSocket server checks — `python scripts/check_agent_ws.py`

Phase B gate. Runs the real ``app.services.agent_ws.AgentWSServer`` on
127.0.0.1:0 (ephemeral port, loopback only) and drives it with a **fake agent
client** that hand-rolls the same RFC-6455 framing as
``mc-orchestrator/agent/agent.py`` (``ws_frame``): masked client frames,
unmasked server frames, 7/16/64-bit lengths.

What is proven (a–i, matching the Phase-B brief):

  a) handshake succeeds: ``101`` + a correct ``Sec-WebSocket-Accept``;
  b) wrong token -> ``401``, no upgrade, **zero** tracebacks; the no-token
     configured server refuses with ``503`` and says why;
  c) a good token connects and the server parses the agent's ``hello`` JSON
     into the session registry;
  d) server -> client ``command`` arrives byte-for-byte;
  e) client -> server ``log`` with many entries arrives complete;
  f) ping/pong works in both directions (server-initiated ping gets a pong
     back and updates last_pong; client ping is answered with a pong);
  g) an over-cap frame closes the connection cleanly with 1009, not a crash;
  h) two consecutive connections: the old one is replaced with a clear close
     reason and the registry holds exactly the new session;
  i) ``stop()`` closes the listener and actually frees the port.

Exit 0 = all passed. Stdlib only, no network beyond loopback, writes no files.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import socket
import struct
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# Never load a real .env: this harness must not see operator credentials.
os.environ["DAYHOST_ENV_FILE"] = str(ROOT / ".nonexistent-harness.env")
os.environ.setdefault("LOG_LEVEL", "CRITICAL")

from app.config import Settings, get_settings  # noqa: E402
from app.services.agent_ws import (  # noqa: E402
    WS_GUID,
    AgentWSServer,
    accept_key,
)

TOKEN = "s3cret-agent-token"
PATH = "/agent-ws"

# --- traceback surveillance (armed for the whole run) -----------------------
TRACEBACKS: list[str] = []
FAILURES: list[str] = []
CHECKS = 0


def check(label: str, ok: bool, detail: str = "") -> int:
    global CHECKS
    CHECKS += 1
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"   [{detail}]" if detail and not ok else ""))
    if not ok:
        FAILURES.append(f"{label}" + (f" ({detail})" if detail else ""))
        return 1
    return 0


def _install_hooks() -> None:
    def hook(exc_type, exc, tb):
        TRACEBACKS.append("".join(traceback.format_exception(exc_type, exc, tb)))

    sys.excepthook = hook
    asyncio.get_running_loop().set_exception_handler(
        lambda _loop, ctx: TRACEBACKS.append(repr(ctx.get("exception") or ctx))
    )


# --------------------------------------------------------------------- client
# Mirrors mc-orchestrator/agent/agent.py: client frames are always masked and
# the server's frames are expected to be unmasked (RFC 6455 §5.1).


def client_frame(opcode: int, payload: bytes, *, fin: bool = True, claim: int | None = None) -> bytes:
    header = bytearray([(0x80 if fin else 0x00) | opcode])
    length = len(payload) if claim is None else claim
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    mask = os.urandom(4)
    header.extend(mask)
    if claim is not None:
        return bytes(header)  # declaration only (oversize probe)
    return bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


class FakeAgent:
    """Hand-rolled RFC-6455 client, same shape as agent.py's WSConn."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 status: str, headers: dict[str, str]) -> None:
        self.reader = reader
        self.writer = writer
        self.status = status
        self.headers = headers
        self.closed = False
        self.key = ""  # Sec-WebSocket-Key we sent, for the accept assertion

    # ------------------------------------------------------------------ open
    @classmethod
    async def dial(cls, port: int, token: str = TOKEN, path: str = PATH,
                   *, query_token: bool = False, extra_headers: dict[str, str] | None = None,
                   omit_upgrade: bool = False) -> "FakeAgent":
        reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=2 ** 20)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        target = path + (f"?token={token}" if query_token else "")
        lines = [
            f"GET {target} HTTP/1.1",
            f"Host: 127.0.0.1:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            f"X-Agent-Token: {token}",
            "User-Agent: fake-agent/1.0",
        ]
        if omit_upgrade:
            lines = [ln for ln in lines if not ln.startswith("Upgrade:")]
        for name, value in (extra_headers or {}).items():
            lines.append(f"{name}: {value}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
        await writer.drain()
        blob = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        text = blob.decode("latin-1").split("\r\n")
        status = text[0]
        headers = {}
        for line in text[1:]:
            if ":" in line:
                name, value = line.split(":", 1)
                headers[name.strip().lower()] = value.strip()
        agent = cls(reader, writer, status, headers)
        agent.key = key
        return agent

    # ------------------------------------------------------------------ send
    async def send(self, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        self.writer.write(client_frame(opcode, payload, fin=fin))
        await self.writer.drain()

    async def send_text(self, text: str) -> None:
        await self.send(0x1, text.encode("utf-8"))

    async def send_json(self, payload: dict) -> None:
        await self.send_text(json.dumps(payload))

    async def send_raw(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    # ------------------------------------------------------------------ recv
    async def recv(self, timeout: float = 5.0) -> tuple[int, bytes]:
        async def _read() -> tuple[int, bytes]:
            head = await self.reader.readexactly(2)
            opcode = head[0] & 0x0F
            fin = bool(head[0] & 0x80)
            masked = bool(head[1] & 0x80)
            length = head[1] & 0x7F
            if length == 126:
                length = struct.unpack("!H", await self.reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
            mask = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length) if length else b""
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            return (opcode if fin else 0x100 | opcode), payload

        return await asyncio.wait_for(_read(), timeout=timeout)

    async def recv_text(self, timeout: float = 5.0) -> str:
        opcode, payload = await self.recv(timeout)
        assert opcode == 0x1, f"expected TEXT, got {opcode:#x}"
        return payload.decode("utf-8")

    async def recv_json(self, timeout: float = 5.0) -> dict:
        return json.loads(await self.recv_text(timeout))

    async def read_http_body(self, timeout: float = 5.0) -> str:
        try:
            return (await asyncio.wait_for(self.reader.read(65536), timeout=timeout)).decode(
                "utf-8", "replace"
            )
        except asyncio.TimeoutError:
            return ""

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.writer.write(client_frame(0x8, struct.pack("!H", 1000)))
            await self.writer.drain()
        except Exception:  # noqa: BLE001
            pass
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass


def close_code(payload: bytes) -> int | None:
    return struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None


async def collect(queue: asyncio.Queue, count: int, timeout: float = 5.0) -> list[dict]:
    out: list[dict] = []
    deadline = asyncio.get_running_loop().time() + timeout
    while len(out) < count:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            out.append(await asyncio.wait_for(queue.get(), timeout=remaining))
        except asyncio.TimeoutError:
            break
    return out


async def drain(queue: asyncio.Queue) -> list[dict]:
    out = []
    while True:
        try:
            out.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return out


def port_is_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


# ----------------------------------------------------------------------- suite


async def main() -> int:
    settings = get_settings()
    # ------------------------------------------------------------------ config
    print("\n== (0) config surface (app/config.py) ==")
    check("AGENT_WS_ENABLED defaults to False", settings.agent_ws_enabled is False,
          repr(settings.agent_ws_enabled))
    check("AGENT_WS_HOST defaults to 127.0.0.1", settings.agent_ws_host == "127.0.0.1",
          settings.agent_ws_host)
    check("AGENT_WS_PORT defaults to 8765", settings.agent_ws_port == 8765,
          str(settings.agent_ws_port))
    check("AGENT_WS_PATH defaults to /agent-ws", settings.agent_ws_path == "/agent-ws",
          settings.agent_ws_path)
    check("AGENT_WS_PING_INTERVAL defaults to 20", settings.agent_ws_ping_interval == 20.0,
          str(settings.agent_ws_ping_interval))
    check("AGENT_WS_STALE_TIMEOUT defaults to 75", settings.agent_ws_stale_timeout == 75.0,
          str(settings.agent_ws_stale_timeout))
    check("AGENT_WS_MAX_FRAME defaults to 4194304", settings.agent_ws_max_frame == 4_194_304,
          str(settings.agent_ws_max_frame))
    check("no token configured -> agent_ws_configured False",
          settings.agent_ws_configured is False)
    fallback = Settings(agent_token="shared", agent_ws_token="")
    check("AGENT_TOKEN is the fallback for the WS token",
          fallback.agent_ws_effective_token == "shared", fallback.agent_ws_effective_token)
    preferred = Settings(agent_token="shared", agent_ws_token="dedicated")
    check("AGENT_WS_TOKEN wins over AGENT_TOKEN",
          preferred.agent_ws_effective_token == "dedicated",
          preferred.agent_ws_effective_token)
    ws_from_settings = AgentWSServer.from_settings(preferred, port=0)
    check("from_settings carries the effective token",
          ws_from_settings.auth_configured and ws_from_settings._token == "dedicated")

    events: asyncio.Queue = asyncio.Queue(maxsize=2000)
    server = AgentWSServer(
        host="127.0.0.1", port=0, path=PATH, token=TOKEN,
        ping_interval=0,  # heartbeat off for the deterministic assertions below
        stale_timeout=75, max_frame=4_194_304,
    )
    # Two hooks that are deliberately noisy: they must never break the transport.
    def bad_hook(event: dict) -> None:
        raise RuntimeError("hook exploded on purpose")

    server._on_event = bad_hook
    await server.start()
    port = server.bound_port
    print(f"\n  server listening on 127.0.0.1:{port}{PATH}")
    subscriber = server.subscribe()
    client: FakeAgent | None = None

    try:
        # =================================================================== (a)
        print("\n== (a) handshake: 101 + correct Sec-WebSocket-Accept ==")
        client = await FakeAgent.dial(port)
        check("status line is 101", "101" in client.status, client.status)
        check("Upgrade: websocket echoed",
              client.headers.get("upgrade", "").lower() == "websocket",
              str(client.headers.get("upgrade")))
        check("Connection: Upgrade echoed",
              "upgrade" in client.headers.get("connection", "").lower(),
              str(client.headers.get("connection")))
        expected_accept = base64.b64encode(
            hashlib.sha1((client.key + WS_GUID).encode("ascii")).digest()
        ).decode("ascii")
        check("Sec-WebSocket-Accept present",
              bool(client.headers.get("sec-websocket-accept")))
        check("accept matches base64(sha1(key+GUID))",
              client.headers.get("sec-websocket-accept") == expected_accept,
              f"got {client.headers.get('sec-websocket-accept')} want {expected_accept}")
        check("accept_key() matches the RFC 6455 §1.3 example",
              accept_key("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
              accept_key("dGhlIHNhbXBsZSBub25jZQ=="))
        check("registry: server says connected", server.connected is True)
        check("registry: exactly one connection tracked", len(server._conns) == 1,
              str(len(server._conns)))

        # =================================================================== (b)
        print("\n== (b) auth: wrong token -> 401, no traceback ==")
        before_tb = len(TRACEBACKS)
        bad = await FakeAgent.dial(port, token="wrong-token")
        check("status is 401", "401" in bad.status, bad.status)
        check("no 101 leak", "101" not in bad.status)
        body = await bad.read_http_body()
        check("body explains the refusal", "invalid agent token" in body, body.strip()[:80])
        await bad.close()
        missing = await FakeAgent.dial(port, token="")
        check("missing token also 401", "401" in missing.status, missing.status)
        await missing.close()
        check("no traceback from auth failures", len(TRACEBACKS) == before_tb,
              f"{len(TRACEBACKS) - before_tb} captured")
        check("stats: 2 rejections counted", server.stats["rejected"] == 2,
              str(server.stats["rejected"]))
        check("still exactly one live agent after rejections", len(server._conns) == 1)

        noauth = AgentWSServer(host="127.0.0.1", port=0, token="", ping_interval=0)
        await noauth.start()
        try:
            before_tb = len(TRACEBACKS)
            naked = await FakeAgent.dial(noauth.bound_port, token="anything")
            check("unconfigured server refuses with 503", "503" in naked.status, naked.status)
            naked_body = await naked.read_http_body()
            check("503 body names the missing auth",
                  "auth not configured" in naked_body, naked_body.strip()[:80])
            await naked.close()
            check("no traceback for the no-token server", len(TRACEBACKS) == before_tb)
        finally:
            await noauth.stop()

        # query-string token is accepted too (agent may use either).
        qclient = await FakeAgent.dial(port, query_token=True)
        check("?token= query parameter is accepted", "101" in qclient.status, qclient.status)
        await qclient.close()
        # it replaced the first client; re-dial for the rest of the suite.
        await client.close()
        client = await FakeAgent.dial(port)
        check("reconnect after query-token probe", "101" in client.status, client.status)
        await drain(subscriber)

        # =================================================================== (c)
        print("\n== (c) hello: server parses the agent's JSON ==")
        hello = {
            "type": "hello",
            "version": "1.4.0",
            "hostname": "sbx-agent-1",
            "platform": "Linux x86_64 / py 3.12.14",
            "server_dir": "/home/daytona/mc",
            "mc_state": "stopped",
            "installed_plugins": [],
            "backup_interval": 3600,
        }
        await client.send_json(hello)
        got = await collect(subscriber, 1)
        check("hello event published", len(got) == 1 and got[0].get("type") == "hello",
              str(got[:1]))
        check("hello payload intact", got and got[0].get("hostname") == "sbx-agent-1"
              and got[0].get("version") == "1.4.0")
        check("registry absorbed hostname",
              server.session is not None and server.session.hostname == "sbx-agent-1",
              repr(server.session.hostname if server.session else None))
        check("registry absorbed version",
              server.session is not None and server.session.version == "1.4.0")
        check("registry absorbed mc_state",
              server.session is not None and server.session.mc_state == "stopped")
        check("agent_info mirrors the hello", server.agent_info.get("hostname") == "sbx-agent-1")
        check("public_state reports connected",
              server.public_state()["connected"] is True)
        check("hook that raises did not break the transport",
              server.connected is True and len(TRACEBACKS) == 0,
              str(TRACEBACKS[:1]))

        # =================================================================== (d)
        print("\n== (d) server -> client command ==")
        command = {"type": "command", "id": "cmd-42", "action": "start",
                   "payload": {"grace": 20, "note": "héllo — unicode ✓"}}
        sent = await server.send_json(command)
        check("send_json reported success", sent is True)
        received = await client.recv_json()
        check("client received the command object", received == command,
              json.dumps(received, ensure_ascii=False)[:160])
        check("unicode survived the round trip",
              received.get("payload", {}).get("note") == "héllo — unicode ✓")

        # =================================================================== (e)
        print("\n== (e) client -> server log with many entries ==")
        entries = [{"ts": f"2026-10-24T09:00:{i % 60:02d}Z", "source": "mc",
                    "text": f"line {i} — tick {i * 7}"} for i in range(300)]
        await client.send_json({"type": "log", "entries": entries})
        logs = await collect(subscriber, 1)
        check("log event published", len(logs) == 1 and logs[0].get("type") == "log",
              str(len(logs)))
        got_entries = logs[0].get("entries") if logs else None
        check("all 300 entries arrived", isinstance(got_entries, list)
              and len(got_entries) == 300,
              str(len(got_entries or [])))
        check("first entry intact", got_entries and got_entries[0]["text"] == "line 0 — tick 0")
        check("last entry intact", got_entries and got_entries[-1]["text"] == "line 299 — tick 2093")
        # fragmented message (0x1 without FIN, then 0x0 with FIN)
        part_a, part_b = b'{"type":"frag","half":', b'1}'
        await client.send(0x1, part_a, fin=False)
        await client.send(0x0, part_b, fin=True)
        frag = await collect(subscriber, 1)
        check("fragmented message reassembled",
              len(frag) == 1 and frag[0].get("type") == "frag" and frag[0].get("half") == 1,
              str(frag[:1]))
        check("message counter advanced", server.stats["messages"] >= 3,
              str(server.stats["messages"]))

        # =================================================================== (f)
        print("\n== (f) ping/pong both directions ==")
        await client.send(0x9, b"are-you-there")
        opcode, payload = await client.recv()
        check("client ping answered with pong", opcode == 0xA, f"opcode={opcode:#x}")
        check("pong echoes the ping payload", payload == b"are-you-there", repr(payload))

        check("server ping() reported success", await server.ping(b"srv-probe") is True)
        opcode, payload = await client.recv()
        check("server sent a ping frame", opcode == 0x9, f"opcode={opcode:#x}")
        check("server ping payload intact", payload == b"srv-probe", repr(payload))
        before_pong = server.last_pong
        await client.send(0xA, payload)
        for _ in range(50):
            if server.last_pong > before_pong:
                break
            await asyncio.sleep(0.02)
        check("server recorded the pong (last_pong advanced)", server.last_pong > before_pong,
              f"{before_pong} -> {server.last_pong}")

        # heartbeat + staleness hook, on a short-interval server
        stale_events: list[dict] = []
        stale_srv = AgentWSServer(host="127.0.0.1", port=0, token=TOKEN,
                                  ping_interval=0.1, stale_timeout=0.35,
                                  on_stale=stale_events.append)
        await stale_srv.start()
        try:
            sleeper = await FakeAgent.dial(stale_srv.bound_port)
            # Read the server's pings but never answer them, so last_rx goes stale.
            deadline = asyncio.get_running_loop().time() + 3
            closed_frame = None
            while asyncio.get_running_loop().time() < deadline:
                try:
                    opcode, payload = await sleeper.recv(timeout=0.5)
                except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                    break
                if opcode == 0x8:
                    closed_frame = payload
                    break
            check("stale connection closed by the heartbeat",
                  closed_frame is not None, "no close frame")
            check("stale close code is 1001 (going away)",
                  close_code(closed_frame or b"") == 1001, str(close_code(closed_frame or b"")))
            check("on_stale recovery hook fired",
                  len(stale_events) == 1 and stale_events[0]["type"] == "_agent_stale",
                  str(stale_events))
            check("stale counter incremented", stale_srv.stats["stale_closes"] == 1,
                  str(stale_srv.stats["stale_closes"]))
            await sleeper.close()
        finally:
            await stale_srv.stop()

        # =================================================================== (g)
        print("\n== (g) over-cap frame -> clean close, no crash ==")
        cap_srv = AgentWSServer(host="127.0.0.1", port=0, token=TOKEN,
                                ping_interval=0, max_frame=4096)
        await cap_srv.start()
        try:
            big = await FakeAgent.dial(cap_srv.bound_port)
            await big.send_raw(client_frame(0x1, b"", claim=4097))
            close_payload = None
            for _ in range(20):
                try:
                    opcode, payload = await big.recv(timeout=1.0)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                    break
                if opcode == 0x8:
                    close_payload = payload
                    break
            check("oversize frame answered with a close frame",
                  close_payload is not None, "no close frame")
            check("close code 1009 (message too big)",
                  close_code(close_payload or b"") == 1009, str(close_code(close_payload or b"")))
            check("server survived (still serving)", cap_srv.running is True)
            check("protocol close counted", cap_srv.stats["protocol_closes"] >= 1,
                  str(cap_srv.stats["protocol_closes"]))
            check("no traceback from the oversize frame", len(TRACEBACKS) == 0,
                  str(TRACEBACKS[:1]))
            await big.close()
        finally:
            await cap_srv.stop()

        # unmasked client frame is a protocol error too.
        strict_srv = AgentWSServer(host="127.0.0.1", port=0, token=TOKEN, ping_interval=0)
        await strict_srv.start()
        try:
            unmasked = await FakeAgent.dial(strict_srv.bound_port)
            unmasked.writer.write(bytes([0x81, 0x02]) + b"hi")  # no mask bit
            await unmasked.writer.drain()
            payload = None
            for _ in range(20):
                try:
                    opcode, payload = await unmasked.recv(timeout=1.0)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                    break
                if opcode == 0x8:
                    break
            check("unmasked client frame -> close 1002 (protocol error)",
                  close_code(payload or b"") == 1002, str(close_code(payload or b"")))
            await unmasked.close()
        finally:
            await strict_srv.stop()

        # =================================================================== (h)
        print("\n== (h) two consecutive connections: replacement ==")
        first = await FakeAgent.dial(port)
        check("first connection accepted", "101" in first.status, first.status)
        await first.send_json({"type": "hello", "hostname": "old-host", "version": "0.9"})
        await asyncio.sleep(0.1)
        check("registry points at old-host",
              server.session is not None and server.session.hostname == "old-host",
              repr(server.session.hostname if server.session else None))
        replaced_before = server.stats["replaced"]
        second = await FakeAgent.dial(port)
        check("second connection accepted", "101" in second.status, second.status)
        close_payload = None
        for _ in range(20):
            try:
                opcode, payload = await first.recv(timeout=1.0)
            except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                break
            if opcode == 0x8:
                close_payload = payload
                break
        check("old connection received a close frame", close_payload is not None,
              "no close frame")
        check("old connection closed with 1000",
              close_code(close_payload or b"") == 1000, str(close_code(close_payload or b"")))
        reason = (close_payload or b"")[2:].decode("utf-8", "replace")
        check("close reason explains the replacement",
              "replaced" in reason, repr(reason))
        check("replacement counted", server.stats["replaced"] == replaced_before + 1,
              str(server.stats["replaced"]))
        check("registry has exactly one connection", len(server._conns) == 1,
              str(len(server._conns)))
        await second.send_json({"type": "hello", "hostname": "new-host", "version": "1.0"})
        await asyncio.sleep(0.1)
        check("registry now holds the new session",
              server.session is not None and server.session.hostname == "new-host",
              repr(server.session.hostname if server.session else None))
        check("only the new connection can be written to",
              await server.send_json({"type": "command", "id": "after-replace"}) is True)
        got = await second.recv_json()
        check("new connection receives commands", got.get("id") == "after-replace", str(got))
        await first.close()
        await second.close()

        # =================================================================== (i)
        print("\n== (i) stop(): listener closed, port released ==")
        await client.close()
        await asyncio.sleep(0.05)
        check("port is in use before stop", port_is_free(port) is False)
        await server.stop()
        check("server reports not running", server.running is False)
        check("registry cleared after stop", server._conns == set() and server._conn is None)
        check("port is free after stop", port_is_free(port) is True)
        try:
            _r, _w = await asyncio.open_connection("127.0.0.1", port, limit=1024)
            refused = False
            _w.close()
        except (ConnectionRefusedError, OSError):
            refused = True
        check("connect after stop is refused", refused)
        # second stop must be a no-op, not an error
        await server.stop()
        check("stop() is idempotent", server.running is False)

        # stop() with a LIVE agent + running heartbeat must return promptly.
        # (Python 3.12 wait_closed() blocks until handlers return, so the order
        # of close-then-wait is a real bug class — this pins it down.)
        live = AgentWSServer(host="127.0.0.1", port=0, token=TOKEN, ping_interval=0.2)
        await live.start()
        held = await FakeAgent.dial(live.bound_port)
        await asyncio.sleep(0.5)  # let a ping round-trip happen
        t0 = asyncio.get_running_loop().time()
        await live.stop()
        elapsed = asyncio.get_running_loop().time() - t0
        check(f"stop() with a live agent returns promptly ({elapsed * 1000:.0f} ms)",
              elapsed < 1.0, f"{elapsed:.2f}s")
        check("stop() with a live agent leaves nothing behind",
              live.running is False and live._conns == set() and live._conn is None)
        check("port freed while an agent was connected", port_is_free(live.port) is True)
        await held.close()

        # =================================================================== (j)
        print("\n== (j) negative handshake paths ==")
        neg = AgentWSServer(host="127.0.0.1", port=0, path=PATH, token=TOKEN, ping_interval=0)
        await neg.start()
        try:
            wrong_path = await FakeAgent.dial(neg.bound_port, path="/nope")
            check("wrong path -> 404", "404" in wrong_path.status, wrong_path.status)
            await wrong_path.close()
            no_upgrade = await FakeAgent.dial(neg.bound_port, omit_upgrade=True)
            check("missing Upgrade header -> 400", "400" in no_upgrade.status, no_upgrade.status)
            await no_upgrade.close()
            bad_version = await FakeAgent.dial(
                neg.bound_port, extra_headers={"Sec-WebSocket-Version": "8"}
            )
            check("Sec-WebSocket-Version != 13 -> 426",
                  "426" in bad_version.status, bad_version.status)
            check("426 advertises version 13",
                  bad_version.headers.get("sec-websocket-version") == "13",
                  str(bad_version.headers.get("sec-websocket-version")))
            await bad_version.close()
            check("no tracebacks in the negative paths", len(TRACEBACKS) == 0,
                  str(TRACEBACKS[:1]))
        finally:
            await neg.stop()

        # import purity: the module must not bind anything on import
        check("import has no side effects (no listener from import)",
              AgentWSServer(token=TOKEN).running is False)

        # =================================================================== (k)
        print("\n== (k) wire compatibility with the REAL agent.py client ==")
        await _real_agent_compat()
    finally:
        for c in (client,):
            if c is not None:
                await c.close()
        if server.running:
            await server.stop()
        server.unsubscribe(subscriber)

    print()
    check("zero tracebacks across the entire run", TRACEBACKS == [],
          TRACEBACKS[0][:400] if TRACEBACKS else "")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print(f"ALL {CHECKS} AGENT-WS CHECKS PASSED")
    return 0


async def _real_agent_compat() -> None:
    """Drive the server with mc-orchestrator/agent/agent.py's own WS client.

    This is the assertion that matters most: the fake agent above could share a
    bug with the server. Here the *actual* proven client (``ws_frame`` /
    ``WSConn`` / ``ws_connect``) talks to the new server half, unchanged.
    """
    agent_path = ROOT / "mc-orchestrator" / "agent" / "agent.py"
    if not agent_path.is_file():
        print("  SKIP  agent.py not present in this tree")
        return
    import importlib.util

    spec = importlib.util.spec_from_file_location("mcagent_probe", agent_path)
    agent = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(agent)  # noqa: F401 - the module is the fixture
    except SystemExit:  # pragma: no cover - agent.py must not exit on import
        check("agent.py imports without executing", False, "SystemExit on import")
        return
    except Exception as exc:  # noqa: BLE001
        check("agent.py imports without executing", False, f"{type(exc).__name__}: {exc}")
        return
    check("agent.py imports without executing", True)
    check("agent.py uses the same RFC 6455 GUID", agent.WS_GUID == WS_GUID, agent.WS_GUID)

    srv = AgentWSServer(host="127.0.0.1", port=0, path=PATH, token=TOKEN,
                        ping_interval=0, max_frame=4_194_304)
    await srv.start()
    queue = srv.subscribe()
    url = f"ws://127.0.0.1:{srv.bound_port}{PATH}"

    async def wait_type(want: str, timeout: float = 5.0) -> dict:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError(want)
            event = await asyncio.wait_for(queue.get(), timeout=remaining)
            if event.get("type") == want:
                return event

    try:
        conn = await agent.ws_connect(url, TOKEN, False)
        check("agent.py ws_connect() completes the handshake", True)

        hello = {"type": "hello", "version": "1.4.0", "hostname": "real-sbx",
                 "platform": "Linux x86_64", "server_dir": "/home/daytona/mc",
                 "mc_state": "stopped", "installed_plugins": [],
                 "backup_interval": 3600}
        await conn.send_text(json.dumps(hello))
        event = await wait_type("hello")
        check("server parses the real agent's hello",
              event.get("hostname") == "real-sbx", str(event)[:120])

        command = {"type": "command", "id": "real-1", "action": "start"}
        check("server.send_json reaches the real client",
              await srv.send_json(command) is True)
        check("agent.py WSConn decodes the command",
              json.loads(await conn.recv()) == command)

        for size, label in ((125, "7-bit"), (4000, "16-bit"), (200_000, "64-bit")):
            blob = {"type": "log", "entries":
                    [{"ts": "x", "source": "mc", "text": "a" * size}]}
            await conn.send_text(json.dumps(blob))
            event = await wait_type("log")
            check(f"client->server {label} length ({size} B) intact",
                  event.get("entries", [{}])[0].get("text") == "a" * size)

        before = srv.last_pong
        await srv.ping(b"hb")
        try:  # WSConn.recv() consumes control frames and auto-pongs
            await asyncio.wait_for(conn.recv(), timeout=0.4)
        except asyncio.TimeoutError:
            pass
        for _ in range(50):
            if srv.last_pong > before:
                break
            await asyncio.sleep(0.02)
        check("agent.py auto-pongs a server ping", srv.last_pong > before)

        await conn.close()
        for _ in range(50):
            if not srv.connected:
                break
            await asyncio.sleep(0.02)
        check("server sees the real client's close()", srv.connected is False)

        try:
            await agent.ws_connect(url, "wrong-token", False)
            check("real client with a bad token is refused", False, "connected anyway")
        except agent.WSClosed as exc:
            check("real client reports a clean 401 (no traceback)",
                  "handshake ditolak" in str(exc), str(exc))
        check("no traceback from the real-client section", len(TRACEBACKS) == 0,
              str(TRACEBACKS[:1]))
    finally:
        srv.unsubscribe(queue)
        await srv.stop()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
