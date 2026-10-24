"""RCON end-to-end test against a fake Source RCON server on loopback.

Run standalone: `python scripts/check_rcon.py`

Proves, without any network access:
  1. wire framing (4-byte LE length, id, type, body, 2 nulls) round-trips,
  2. auth handshake works with the right password,
  3. a command is sent and its response is returned,
  4. a wrong password is reported cleanly (no traceback),
  5. the command classifier blocks destructive commands
     and rejects unknown ones (deny-by-default).

Exit 0 = pass.
"""
from __future__ import annotations

import asyncio
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services import rcon  # noqa: E402

PASSWORD = "test-secret"


# ------------------------------------------------------------------ fake server
async def _read_packet(reader: asyncio.StreamReader) -> tuple[int, int, str]:
    (length,) = struct.unpack("<i", await reader.readexactly(4))
    data = await reader.readexactly(length)
    request_id, packet_type = struct.unpack("<ii", data[:8])
    body = data[8:-2].decode("utf-8", "replace")
    return request_id, packet_type, body


def _frame(request_id: int, packet_type: int, body: str) -> bytes:
    payload = struct.pack("<ii", request_id, packet_type) + body.encode() + b"\x00\x00"
    return struct.pack("<i", len(payload)) + payload


class FakeRconServer:
    """Minimal but protocol-correct Source RCON server."""

    def __init__(self, password: str = PASSWORD) -> None:
        self.password = password
        self.authenticated = False
        self.commands: list[str] = []
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            r_id, r_type, body = await _read_packet(reader)
            if r_type == rcon.SERVERDATA_AUTH:
                if body == self.password:
                    self.authenticated = True
                    # Minecraft-style: empty RESPONSE_VALUE, then AUTH_RESPONSE.
                    writer.write(_frame(r_id, rcon.SERVERDATA_RESPONSE_VALUE, ""))
                    writer.write(_frame(r_id, rcon.SERVERDATA_AUTH_RESPONSE, ""))
                else:
                    writer.write(_frame(-1, rcon.SERVERDATA_AUTH_RESPONSE, ""))
                await writer.drain()
                if body != self.password:
                    return
                # keep the connection open for the command
                r_id, r_type, body = await _read_packet(reader)

            if r_type == rcon.SERVERDATA_EXECCOMMAND:
                self.commands.append(body)
                reply = self._respond(body)
                writer.write(_frame(r_id, rcon.SERVERDATA_RESPONSE_VALUE, reply))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    def _respond(self, command: str) -> str:
        if command == "list":
            return "There are 2 of a max of 20 players online: Alice, Bob"
        if command.startswith("say "):
            return f"[Server] {command[4:]}"
        return f"Unknown command: {command}"

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


# ----------------------------------------------------------------------- checks
def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


async def main() -> int:
    failures = 0
    server = FakeRconServer()
    port = await server.start()
    print(f"fake RCON server listening on 127.0.0.1:{port}\n")

    try:
        # --- 1. framing round-trip -----------------------------------------
        frame = rcon.encode_packet(7, rcon.SERVERDATA_EXECCOMMAND, "list")
        (declared,) = struct.unpack("<i", frame[:4])
        body_len = len(frame) - 4
        ok = check(
            "frame length prefix == id+type+body+2 nulls",
            declared == body_len,
            f"declared={declared} actual={body_len}",
        )
        failures += 0 if ok else 1
        ok = check("frame ends with two null bytes", frame.endswith(b"\x00\x00"))
        failures += 0 if ok else 1

        # --- 2. auth + command ---------------------------------------------
        client = rcon.RconClient("127.0.0.1", port, PASSWORD, timeout=3.0)
        result = await client.execute("list")
        ok = check("authenticated command executes", result.ok, result.error or "")
        failures += 0 if ok else 1
        ok = check("server recorded the command", server.commands == ["list"], str(server.commands))
        failures += 0 if ok else 1
        ok = check(
            "response body returned",
            "Alice, Bob" in result.body,
            repr(result.body),
        )
        failures += 0 if ok else 1

        # --- 3. second command on a fresh connection ------------------------
        result2 = await client.execute("say hello world")
        ok = check("second command works", result2.ok and "[Server] hello world" in result2.body,
                   repr(result2.body))
        failures += 0 if ok else 1

        # --- 4. wrong password ---------------------------------------------
        bad = rcon.RconClient("127.0.0.1", port, "wrong-password", timeout=3.0)
        bad_result = await bad.execute("list")
        ok = check(
            "bad password -> clean failure, no exception",
            (not bad_result.ok) and "rejected" in (bad_result.error or ""),
            repr(bad_result.error),
        )
        failures += 0 if ok else 1

        # --- 5. unreachable port -------------------------------------------
        dead = rcon.RconClient("127.0.0.1", 1, PASSWORD, timeout=1.0)
        dead_result = await dead.execute("list")
        ok = check(
            "closed port -> structured error",
            (not dead_result.ok) and bool(dead_result.error),
            repr(dead_result.error),
        )
        failures += 0 if ok else 1

        # --- 6. classifier --------------------------------------------------
        destructive = ["stop", "op Steve", "ban Alice", "kick Bob", "whitelist off",
                       "deop Steve", "gamemode creative Steve", "/stop", "SAVE-ALL"]
        for cmd in destructive:
            got = rcon.classify(cmd)
            ok = check(f"destructive blocked-classified: {cmd!r}", got == "destructive", got)
            failures += 0 if ok else 1

        safe = ["list", "tps", "seed", "say hi", "help", "time query daytime", "whitelist list"]
        for cmd in safe:
            got = rcon.classify(cmd)
            ok = check(f"safe allowed-classified: {cmd!r}", got == "safe", got)
            failures += 0 if ok else 1

        unknown = ["give Steve diamond 64x", "summon zombie", "lolwut"]
        for cmd in unknown:
            got = rcon.classify(cmd)
            ok = check(f"unknown rejected-classified: {cmd!r}", got in {"unknown", "destructive"}, got)
            failures += 0 if ok else 1
    finally:
        await server.stop()

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL RCON CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
