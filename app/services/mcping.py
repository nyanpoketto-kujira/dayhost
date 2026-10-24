"""Hand-rolled Minecraft Server List Ping (SLP) — no third-party dependency.

Why not `mcstatus`? The RSS budget is a hard requirement (<60 MB for the app
process, measured under load). A ~60-line
raw-socket implementation is enough for "is it up / how many players".

Protocol (modern, 1.7+):
    1. TCP connect
    2. Handshake  : varint len | 0x00 | varint proto | str host | u16 port | varint 1
    3. Status Req : varint len | 0x00
    4. Status Resp: varint len | 0x00 | str json

Everything is defensive: any parse/transport failure returns a normalised
`PingResult` with `online=False` and a short `error` — never an exception.
"""
from __future__ import annotations

import asyncio
import json
import socket
import struct
from dataclasses import dataclass, field
from typing import Any

DEFAULT_PROTOCOL = 765  # 1.20.4 — only used for the handshake echo
MAX_PACKET = 2 * 1024 * 1024


@dataclass
class PingResult:
    online: bool
    host: str
    port: int
    latency_ms: float | None = None
    version: str = ""
    protocol: int | None = None
    players_online: int | None = None
    players_max: int | None = None
    motd: str = ""
    sample: list[str] = field(default_factory=list)
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "online": self.online,
            "host": self.host,
            "port": self.port,
            "latency_ms": self.latency_ms,
            "version": self.version,
            "protocol": self.protocol,
            "players_online": self.players_online,
            "players_max": self.players_max,
            "motd": self.motd,
            "sample": self.sample,
            "error": self.error,
        }


# ------------------------------------------------------------------ varint I/O
def _write_varint(value: int) -> bytes:
    out = bytearray()
    value &= 0xFFFFFFFF
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _write_string(text: str) -> bytes:
    raw = text.encode("utf-8")
    return _write_varint(len(raw)) + raw


async def _read_varint(reader: asyncio.StreamReader) -> int:
    num = 0
    for _ in range(5):
        byte = (await reader.readexactly(1))[0]
        num |= (byte & 0x7F) << (7 * _)
        if not byte & 0x80:
            return num
    raise ValueError("varint too long")


async def _read_packet(reader: asyncio.StreamReader) -> bytes:
    length = await _read_varint(reader)
    if length <= 0 or length > MAX_PACKET:
        raise ValueError(f"bad packet length {length}")
    return await reader.readexactly(length)


async def _read_string(reader: asyncio.StreamReader) -> str:
    size = await _read_varint(reader)
    if size < 0 or size > MAX_PACKET:
        raise ValueError(f"bad string length {size}")
    return (await reader.readexactly(size)).decode("utf-8", errors="replace")


# ---------------------------------------------------------------------- motd
def _flatten_chat(node: Any) -> str:
    """Flatten a Minecraft chat component (or legacy string) into plain text."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_flatten_chat(item) for item in node)
    if isinstance(node, dict):
        parts = [str(node.get("text", ""))]
        parts.extend(_flatten_chat(node.get(k)) for k in ("extra", "with"))
        return "".join(parts)
    return str(node)


# ----------------------------------------------------------------------- ping
async def _modern_ping(host: str, port: int, timeout: float) -> PingResult:
    loop = asyncio.get_running_loop()
    started = loop.time()
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        handshake = (
            _write_varint(0x00)
            + _write_varint(DEFAULT_PROTOCOL)
            + _write_string(host)
            + struct.pack(">H", port)
            + _write_varint(1)
        )
        writer.write(_write_varint(len(handshake)) + handshake)
        writer.write(_write_varint(1) + _write_varint(0x00))
        await asyncio.wait_for(writer.drain(), timeout=timeout)

        payload = await asyncio.wait_for(_read_packet(reader), timeout=timeout)
        # Re-parse the payload as a stream: packet id, then the JSON string.
        stream = asyncio.StreamReader()
        stream.feed_data(payload)
        stream.feed_eof()
        packet_id = await _read_varint(stream)
        if packet_id != 0x00:
            raise ValueError(f"unexpected packet id {packet_id}")
        data = json.loads(await _read_string(stream))

        latency = (loop.time() - started) * 1000.0
        version = data.get("version") or {}
        players = data.get("players") or {}
        sample = [
            str(entry.get("name", ""))
            for entry in (players.get("sample") or [])
            if isinstance(entry, dict)
        ]
        return PingResult(
            online=True,
            host=host,
            port=port,
            latency_ms=round(latency, 1),
            version=str(version.get("name", "")),
            protocol=version.get("protocol"),
            players_online=players.get("online"),
            players_max=players.get("max"),
            motd=_flatten_chat(data.get("description")).strip(),
            sample=sample,
        )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def _legacy_ping(host: str, port: int, timeout: float) -> PingResult:
    """1.6-and-older fallback: send 0xFE 0x01, read the §-delimited string."""
    loop = asyncio.get_running_loop()
    started = loop.time()
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        writer.write(b"\xfe\x01")
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        raw = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        latency = (loop.time() - started) * 1000.0
        text = raw.decode("utf-16-be", errors="replace")
        fields = [f for f in text.split("\x00") if f]
        motd = fields[3] if len(fields) > 3 else text.strip("\x00\ufffd")
        online = fields[4] if len(fields) > 4 else None
        maximum = fields[5] if len(fields) > 5 else None
        return PingResult(
            online=True,
            host=host,
            port=port,
            latency_ms=round(latency, 1),
            version="legacy",
            players_online=int(online) if online and online.isdigit() else None,
            players_max=int(maximum) if maximum and maximum.isdigit() else None,
            motd=motd.strip(),
        )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def ping(host: str, port: int, timeout: float = 4.0) -> PingResult:
    """Ping a Minecraft server. Never raises; returns PingResult(online=False)."""
    try:
        return await _modern_ping(host, port, timeout)
    except (asyncio.TimeoutError, socket.gaierror) as exc:
        return PingResult(False, host, port, error=f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        # Modern handshake failed — try the legacy protocol before giving up.
        try:
            return await _legacy_ping(host, port, timeout)
        except Exception as legacy_exc:
            return PingResult(
                False,
                host,
                port,
                error=f"{type(exc).__name__}: {exc} / legacy: {type(legacy_exc).__name__}",
            )
