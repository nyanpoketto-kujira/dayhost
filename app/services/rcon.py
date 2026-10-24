"""Source RCON client — stdlib only (`asyncio`, `struct`). No `rcon` package.

Wire format (Source RCON / Minecraft), little-endian throughout:

    int32 length   -- size of (id + type + body + 2 nulls), i.e. not itself
    int32 id       -- request/response correlation id
    int32 type     -- 3 = SERVERDATA_AUTH, 2 = SERVERDATA_EXECCOMMAND,
                      0 = SERVERDATA_RESPONSE_VALUE,
                      2 also used by the server for AUTH_RESPONSE in some
                      implementations; Minecraft instead answers auth with an
                      empty RESPONSE_VALUE (id -1 on failure) then a second
                      packet with the request id.
    bytes body     -- ASCII/UTF-8 payload
    2 null bytes   -- body terminator

Auth handshake: send AUTH(type 3) with the password. Minecraft replies with
two packets: (a) RESPONSE_VALUE, empty body, id == request id, then (b) another
one carrying id == request id for success or id == -1 for failure. Some servers
send only one; we therefore read up to two packets with the socket timeout and
treat `id == -1` as failure.

Hardening:
  * 5 s default timeout, applied to connect + every read.
  * Body size cap (`MAX_BODY`) so a hostile/broken peer cannot make us allocate
    unbounded memory.
  * Never raises from `execute()`; returns `RconResult(ok=False, error=...)`.
  * One connection per command (stateless). Cheap for an admin dashboard, and
    it means a half-dead socket can never poison the next command.

This module has **no import-time side effects** and no third-party imports, so
it costs ~0 RSS until a command is actually sent.
"""
from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("dayhost.rcon")

SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_RESPONSE_VALUE = 0

MAX_BODY = 4096  # Minecraft RCON responses are split across packets well below this
MAX_PACKET = 64 * 1024  # hard ceiling on a single declared length
DEFAULT_TIMEOUT = 5.0


class RconError(RuntimeError):
    """Any transport/protocol failure. Callers normally see it wrapped."""


@dataclass
class RconResult:
    ok: bool
    body: str = ""
    error: str | None = None
    packets: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "body": self.body,
            "error": self.error,
            "packet_count": len(self.packets),
        }


def encode_packet(request_id: int, packet_type: int, body: str) -> bytes:
    """Build one RCON frame (length prefix excluded from its own count)."""
    payload = body.encode("utf-8", errors="replace")
    if len(payload) > MAX_BODY:
        raise RconError(f"command too long ({len(payload)} > {MAX_BODY} bytes)")
    packet = struct.pack("<ii", request_id, packet_type) + payload + b"\x00\x00"
    return struct.pack("<i", len(packet)) + packet


async def _read_packet(reader: asyncio.StreamReader) -> tuple[int, int, str]:
    """Read one frame. Raises RconError on EOF/oversize/malformed data."""
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        raise RconError("connection closed before length header") from exc
    (length,) = struct.unpack("<i", header)
    if length < 10:  # id(4) + type(4) + 2 nulls
        raise RconError(f"invalid packet length {length}")
    if length > MAX_PACKET:
        raise RconError(f"packet too large ({length} > {MAX_PACKET})")
    try:
        data = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise RconError("connection closed mid-packet") from exc
    request_id, packet_type = struct.unpack("<ii", data[:8])
    body = data[8:-2].decode("utf-8", errors="replace")
    return request_id, packet_type, body


class RconClient:
    """Stateless Source RCON client bound to one endpoint."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 25575,
        password: str = "",
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout

    # ---------------------------------------------------------------- internals
    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout
            )
        except asyncio.TimeoutError as exc:
            raise RconError(
                f"connect timeout after {self.timeout}s to {self.host}:{self.port}"
            ) from exc
        except OSError as exc:
            raise RconError(f"connect failed to {self.host}:{self.port}: {exc}") from exc

    async def _authenticate(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if not self.password:
            raise RconError("no RCON password configured")
        writer.write(encode_packet(1, SERVERDATA_AUTH, self.password))
        await writer.drain()

        # Read up to two packets. Failure is signalled by id == -1.
        last_error: str | None = None
        for _ in range(2):
            try:
                request_id, _type, _body = await asyncio.wait_for(
                    _read_packet(reader), timeout=self.timeout
                )
            except asyncio.TimeoutError as exc:
                raise RconError(
                    f"auth timeout after {self.timeout}s (no AUTH_RESPONSE)"
                ) from exc
            except RconError as exc:
                raise RconError(f"auth read failed: {exc}") from exc
            if request_id == -1:
                raise RconError("authentication rejected (bad RCON_PASSWORD)")
            if request_id == 1:
                return
            last_error = f"unexpected auth response id={request_id}"
        raise RconError(last_error or "authentication failed")

    async def _login(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await self._connect()
        try:
            await self._authenticate(reader, writer)
        except BaseException:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            raise
        return reader, writer

    # -------------------------------------------------------------- public API
    async def execute(self, command: str) -> RconResult:
        """Send one command and collect the full (possibly multi-packet) body."""
        command = command.strip()
        if not command:
            return RconResult(ok=False, error="empty command")
        if not self.password:
            return RconResult(ok=False, error="RCON not configured (RCON_PASSWORD empty)")

        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await self._login()
            writer.write(encode_packet(2, SERVERDATA_EXECCOMMAND, command))
            await writer.drain()

            packets: list[str] = []
            # Minecraft sends the response in one or more RESPONSE_VALUE packets
            # and does not always signal "end of response". We stop when:
            #   * the peer closes the connection, or
            #   * a short idle gap follows the first packet (0.25 s), or
            #   * we hit MAX_PACKETS.
            while len(packets) < 64:
                idle = 0.25 if packets else self.timeout
                try:
                    request_id, packet_type, body = await asyncio.wait_for(
                        _read_packet(reader), timeout=idle
                    )
                except asyncio.TimeoutError:
                    if packets:
                        break
                    raise RconError(f"command timeout after {self.timeout}s")
                except RconError:
                    break  # EOF = end of response
                if request_id == -1:
                    return RconResult(ok=False, error="server rejected command", packets=packets)
                if packet_type == SERVERDATA_RESPONSE_VALUE:
                    packets.append(body)
            body = "\n".join(p.strip() for p in packets if p).strip()
            return RconResult(ok=True, body=body, packets=packets)
        except RconError as exc:
            log.info("rcon error: %s", exc)
            return RconResult(ok=False, error=str(exc))
        except Exception as exc:  # pragma: no cover - defensive, never traceback the caller
            log.warning("rcon unexpected %s: %s", type(exc).__name__, exc)
            return RconResult(ok=False, error=f"{type(exc).__name__}: {exc}")
        finally:
            if writer is not None:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
                except Exception:
                    pass


# ------------------------------------------------------------------ allow-list
# Read-only / informational commands. Unknown commands are rejected by default
# (deny-by-default), because Minecraft has far too many world-mutating verbs to
# enumerate as "dangerous" safely.
SAFE_COMMANDS: frozenset[str] = frozenset(
    {
        # pure reads — no world, entity or player state changes
        "list", "help", "?", "seed", "time", "time query", "tps", "mspt",
        "version", "plugins", "pl", "mods", "forge", "neoforge",
        "whitelist list", "banlist", "banlist players", "banlist ips",
        "scoreboard objectives list", "scoreboard players list",
        "data get", "stats", "difficulty", "debug report",
        "say", "me",
    }
)

# Commands that change server state or punish players. Allowed only when
# RCON_ALLOW_DESTRUCTIVE=true. Matching is on the first token (plus a few
# two-token forms like "whitelist off").
DESTRUCTIVE_COMMANDS: frozenset[str] = frozenset(
    {
        "stop", "restart", "op", "deop", "ban", "ban-ip", "pardon", "pardon-ip",
        "kick", "kill", "clear", "give", "effect", "enchant", "gamemode",
        "gamerule", "setworldspawn", "spawnpoint", "tp", "teleport",
        "fill", "clone", "setblock", "summon", "delete", "damage", "advancement",
        "recipe", "reload", "save-off", "save-all", "save-on",
        "whitelist", "dewhitelist", "execute", "function", "datapack",
        "perf", "jfr", "debug", "publish", "transfer",
        "worldborder", "weather", "time set", "forceload", "place", "loot",
    }
)


def classify(command: str) -> str:
    """Return 'safe' | 'destructive' | 'unknown' for a raw command string.

    Deny-by-default: anything we cannot positively classify is 'unknown'.
    """
    text = " ".join(command.strip().split())
    if not text:
        return "unknown"
    lowered = text.lower().lstrip("/")
    first = lowered.split(" ", 1)[0]
    two = " ".join(lowered.split(" ")[:2])

    for candidate in (lowered, two, first):
        if candidate in SAFE_COMMANDS:
            return "safe"
    for candidate in (lowered, two, first):
        if candidate in DESTRUCTIVE_COMMANDS:
            return "destructive"
    return "unknown"
