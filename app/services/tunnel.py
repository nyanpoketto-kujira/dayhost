"""Tunnel status probes.

SCOPE DECISION (agreed with operator)
-------------------------------------
FRP/SSH tunnel **lifecycle is OUT of scope for DayHost**. The tunnel is managed
separately by the operator (systemd unit, autossh, frpc config, etc.).
DayHost only *observes*:

  1. socket probe  — can we TCP-connect to MC_HOST:MC_PORT from this VPS?
  2. frpc log peek — optional, secondary liveness signal (FRPC_LOG_PATH)

It never starts/stops/restarts a tunnel. See README + TODO fase 2.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import Settings, get_settings


@dataclass
class TunnelStatus:
    mode: str
    socket_open: bool
    socket_error: str = ""
    latency_ms: float | None = None
    frpc_running: bool | None = None
    frpc_log_tail: str = ""
    public_endpoint: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "socket_open": self.socket_open,
            "socket_error": self.socket_error,
            "latency_ms": self.latency_ms,
            "frpc_running": self.frpc_running,
            "frpc_log_tail": self.frpc_log_tail,
            "public_endpoint": self.public_endpoint,
        }


async def _socket_probe(host: str, port: int, timeout: float) -> tuple[bool, str, float | None]:
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        latency = (loop.time() - started) * 1000.0
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, "", round(latency, 1)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}", None


def _frpc_process_running() -> bool | None:
    """Cheap /proc scan — no dependency, no subprocess spawn."""
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/comm", "r", encoding="utf-8") as fh:
                    if fh.read().strip().lower().startswith("frpc"):
                        return True
            except (OSError, PermissionError):
                continue
        return False
    except OSError:
        return None


def _tail_log(path: str, max_bytes: int = 2000) -> str:
    if not path:
        return ""
    try:
        p = Path(path)
        if not p.is_file():
            return ""
        size = p.stat().st_size
        with p.open("rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            return fh.read(max_bytes).decode("utf-8", errors="replace").strip()
    except OSError:
        return ""


async def probe(settings: Settings | None = None) -> TunnelStatus:
    settings = settings or get_settings()
    ok, err, latency = await _socket_probe(
        settings.mc_host, settings.mc_port, float(settings.mc_ping_timeout)
    )
    frpc_running: bool | None = None
    log_tail = ""
    if settings.tunnel_mode.lower() == "frp":
        frpc_running = await asyncio.to_thread(_frpc_process_running)
        log_tail = await asyncio.to_thread(_tail_log, settings.frpc_log_path)

    public_endpoint = ""
    if settings.public_host:
        public_endpoint = f"{settings.public_host}:{settings.public_port}"

    return TunnelStatus(
        mode=settings.tunnel_mode,
        socket_open=ok,
        socket_error=err,
        latency_ms=latency,
        frpc_running=frpc_running,
        frpc_log_tail=log_tail,
        public_endpoint=public_endpoint,
    )
