#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SoloA Minecraft agent.

Runs on the ephemeral PaperMC host with no third-party dependencies:
  * persistent WebSocket client to the controller
  * start / stop / restart PaperMC and stream stdout+stderr
  * pull plugin jars uploaded from the panel
  * every BACKUP_INTERVAL seconds upload only changed server files (delta)
  * bootstrap portable Java 21 (JDK) + PaperMC when the host was wiped
  * optional frpc tunnel (binary fetched from the pinned GitHub release and
    SHA-256 verified) so 25565 is reachable without a public IP

Designed for Python 3.9+ and a freshly reset Linux home directory.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import http.client
import io
import json
import os
import platform
import random
import re
import shlex
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

VERSION = "1.1.0"
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MANIFEST_NAME = "__manifest__.json"
FILL_API = "https://fill.papermc.io/v3/projects/paper"
# Java is pinned to 21: PaperMC 1.21.x refuses to start on a newer major.
ADOPTIUM_API = (
    "https://api.adoptium.net/v3/binary/latest/{version}/ga/linux/{arch}/jdk/hotspot/normal/eclipse"
)
FRP_RELEASE_URL = (
    "https://github.com/fatedier/frp/releases/download/v{version}/frp_{version}_linux_{arch}.tar.gz"
)
# Official checksum list, only used as documentation: the agent never guesses a
# hash. Empty FRP_SHA256 means "do not download frpc" (see Tunnel.prepare).
FRP_CHECKSUMS_URL = (
    "https://github.com/fatedier/frp/releases/download/v{version}/frp_sha256_checksums.txt"
)
EXCLUDE_DIRS = {
    "logs",
    "cache",
    "crash-reports",
    "libraries",
    "versions",
    "runtime",
    ".git",
    "tmp",
}
EXCLUDE_NAMES = {
    "session.lock",
    "paper.jar",
    "server.jar",
    "agent.pid",
    "agent.log",
    "backup-index.json",
}
EXCLUDE_SUFFIXES = (".log", ".tmp", ".part")
PLUGIN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+ -]{0,148}\.jar$")


# --------------------------------------------------------------------------- env


def log(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} {message}", flush=True)


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                values[key] = value
    except OSError:
        pass
    return values


def env_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "y")


class Config:
    def __init__(self, path: Path | None = None) -> None:
        self.agent_dir = Path(__file__).resolve().parent
        self.env_path = path or Path(
            os.environ.get("AGENT_CONFIG", self.agent_dir / "agent.env")
        )
        self.file_values = parse_env_file(self.env_path)

        def get(key: str, default: str = "") -> str:
            if key in os.environ:
                return os.environ[key]
            return self.file_values.get(key, default)

        self.controller_ws = get("CONTROLLER_WS", "").strip()
        self.controller_http = get("CONTROLLER_HTTP", "").strip().rstrip("/")
        if not self.controller_http and self.controller_ws:
            self.controller_http = re.sub(r"^ws", "http", self.controller_ws)
            self.controller_http = self.controller_http.split("/agent-ws")[0].rstrip("/")
        self.agent_token = get("AGENT_TOKEN", "").strip()

        self.server_dir = Path(
            get("SERVER_DIR", str(self.agent_dir.parent / "server"))
        ).expanduser()
        self.runtime_dir = Path(
            get("RUNTIME_DIR", str(self.agent_dir.parent / "runtime"))
        ).expanduser()
        self.tmp_dir = Path(get("TMP_DIR", str(self.agent_dir / "tmp"))).expanduser()
        self.index_path = Path(
            get("BACKUP_INDEX", str(self.agent_dir / "backup-index.json"))
        ).expanduser()
        self.pid_path = Path(
            get("AGENT_PID_FILE", str(self.agent_dir / "agent.pid"))
        ).expanduser()

        self.java_bin = get("JAVA_BIN", "").strip()
        # Pinned major. The orchestrator writes JAVA_VERSION=21 into agent.env.
        try:
            self.java_version = int(get("JAVA_VERSION", "21") or 21)
        except ValueError:
            self.java_version = 21
        if self.java_version < 21:
            self.java_version = 21
        self.java_args = get(
            "JAVA_ARGS", "-Xms2G -Xmx28G -XX:+UseG1GC -XX:+ParallelRefProcEnabled"
        )
        self.paper_jar_name = get("PAPER_JAR", "paper.jar")
        self.paper_version = get("PAPER_VERSION", "latest")
        self.jre_arch = get("JRE_ARCH", "x64")
        self.autostart = env_bool(get("AUTOSTART", None), True)
        self.provision = env_bool(get("PROVISION", None), True)
        self.kill_orphans = env_bool(get("KILL_ORPHANS", None), True)

        self.backup_enabled = env_bool(get("BACKUP_ENABLED", None), True)
        self.backup_interval = max(60, int(get("BACKUP_INTERVAL", "240") or 240))
        self.backup_flush = env_bool(get("BACKUP_FLUSH", None), True)
        self.backup_batch_bytes = max(
            16, int(get("BACKUP_BATCH_MB", "128") or 128)
        ) * 1024 * 1024
        self.backup_on_start_delay = max(5, int(get("BACKUP_START_DELAY", "45") or 45))
        self.max_delta_bytes = max(32, int(get("MAX_DELTA_MB", "512") or 512)) * 1024 * 1024

        self.ws_insecure = env_bool(get("WS_INSECURE", None), False)

        # --- frpc tunnel (optional; a failure here never stops PaperMC) -----
        # FRP_SHA256 empty => frpc is NOT downloaded and the tunnel reports
        # "unconfigured". A missing hash is never filled in with a guess.
        self.frp_enabled = env_bool(get("FRP_ENABLED", None), True)
        self.frp_server_addr = get("FRP_SERVER_ADDR", "").strip()
        try:
            self.frp_server_port = int(get("FRP_SERVER_PORT", "7000") or 7000)
        except ValueError:
            self.frp_server_port = 7000
        self.frp_token = get("FRP_TOKEN", "").strip()
        try:
            self.frp_local_port = int(get("FRP_LOCAL_PORT", "25565") or 25565)
        except ValueError:
            self.frp_local_port = 25565
        try:
            self.frp_remote_port = int(get("FRP_REMOTE_PORT", "25565") or 25565)
        except ValueError:
            self.frp_remote_port = 25565
        self.frp_version = get("FRP_VERSION", "0.71.1").strip() or "0.71.1"
        self.frp_sha256 = get("FRP_SHA256", "").strip().lower()
        self.frp_arch = get("FRP_ARCH", "").strip() or self._frp_arch()
        self.frp_dir = self.runtime_dir / "frpc"
        self.frp_config_path = self.frp_dir / "frpc.toml"
        self.frp_log_path = Path(
            get("FRP_LOG", str(self.runtime_dir / "frpc.log"))
        ).expanduser()

        # Artifacts (plugin jars) are served by the DayHost panel when it has a
        # download endpoint; CONTROLLER_HTTP is the base we know about.
        self.artifact_base_url = (
            get("ARTIFACT_BASE_URL", "").strip().rstrip("/") or self.controller_http
        )
        # Path template used when DayHost serves artifacts itself, e.g.
        # "/api/agent/artifacts/{name}". EMPTY == the panel has no such
        # endpoint yet, so upstream URLs are used directly (see report).
        self.artifact_path_template = get("ARTIFACT_PATH_TEMPLATE", "").strip()

        self.mc_log_lines = max(200, int(get("MC_LOG_LINES", "2000") or 2000))
        self.stop_timeout = max(10, int(get("STOP_TIMEOUT", "45") or 45))

        if not self.controller_ws:
            raise SystemExit("CONTROLLER_WS belum diset")
        if not self.agent_token:
            raise SystemExit("AGENT_TOKEN belum diset")

    def _frp_arch(self) -> str:
        machine = platform.machine().lower()
        if machine in ("aarch64", "arm64"):
            return "arm64"
        if machine in ("armv7l", "armv6l", "arm"):
            return "arm"
        return "amd64"

    def make_dirs(self) -> None:
        self.server_dir.mkdir(parents=True, exist_ok=True)
        (self.server_dir / "plugins").mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.frp_dir.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------- WebSocket


class WSClosed(RuntimeError):
    pass


def ws_frame(opcode: int, payload: bytes) -> bytes:
    header = bytearray([0x80 | opcode])
    length = len(payload)
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
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return bytes(header) + masked


class WSConn:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.send_lock = asyncio.Lock()
        self.closed = False

    async def send_text(self, text: str) -> None:
        await self._send(0x1, text.encode("utf-8"))

    async def _send(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            raise WSClosed("websocket closed")
        async with self.send_lock:
            self.writer.write(ws_frame(opcode, payload))
            await self.writer.drain()

    async def pong(self, payload: bytes) -> None:
        await self._send(0xA, payload)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.writer.write(ws_frame(0x8, b"\x03\xe8"))
            await self.writer.drain()
        except Exception:
            pass
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

    async def recv(self) -> str | None:
        """Return a text message, or None for a control frame that was handled."""
        fragments: list[bytes] = []
        fragment_opcode: int | None = None
        while True:
            try:
                first = await self.reader.readexactly(2)
            except (asyncio.IncompleteReadError, ConnectionError) as exc:
                raise WSClosed("connection closed") from exc
            fin = bool(first[0] & 0x80)
            opcode = first[0] & 0x0F
            masked = bool(first[1] & 0x80)
            length = first[1] & 0x7F
            if length == 126:
                length = struct.unpack("!H", await self.reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
            if length > 4 * 1024 * 1024:
                raise WSClosed("frame too large")
            mask = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length)
            if masked:
                payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))

            if opcode == 0x8:
                raise WSClosed("server closed connection")
            if opcode == 0x9:
                await self.pong(payload)
                continue
            if opcode == 0xA:
                continue
            if opcode in (0x1, 0x2):
                if fin:
                    return payload.decode("utf-8", "replace")
                fragment_opcode = opcode
                fragments = [payload]
                continue
            if opcode == 0x0 and fragment_opcode is not None:
                fragments.append(payload)
                if fin:
                    data = b"".join(fragments)
                    fragments = []
                    opcode = fragment_opcode
                    fragment_opcode = None
                    return data.decode("utf-8", "replace")
                continue
        return None


async def ws_connect(url: str, token: str, insecure: bool) -> WSConn:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("ws", "wss"):
        raise WSClosed(f"skema websocket tidak didukung: {parsed.scheme}")
    secure = parsed.scheme == "wss"
    host = parsed.hostname or ""
    port = parsed.port or (443 if secure else 80)
    if not host:
        raise WSClosed("host websocket kosong")

    ssl_context = None
    if secure:
        ssl_context = ssl.create_default_context()
        if insecure:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl_context, limit=2 * 1024 * 1024),
        timeout=15,
    )
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    default_port = 443 if secure else 80
    host_header = host if port == default_port else f"{host}:{port}"
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host_header}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        f"X-Agent-Token: {token}\r\n"
        f"User-Agent: mc-orchestrator-agent/{VERSION}\r\n"
        "\r\n"
    )
    writer.write(request.encode("ascii"))
    await writer.drain()
    try:
        header_blob = await asyncio.wait_for(
            reader.readuntil(b"\r\n\r\n"), timeout=15
        )
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError) as exc:
        writer.close()
        raise WSClosed("handshake websocket gagal") from exc
    text = header_blob.decode("latin-1", "replace").split("\r\n")
    status = text[0] if text else ""
    headers: dict[str, str] = {}
    for line in text[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
    if "101" not in status:
        writer.close()
        raise WSClosed(f"handshake ditolak: {status}")
    expected = base64.b64encode(
        hashlib.sha1((key + WS_GUID).encode("ascii")).digest()
    ).decode("ascii")
    if headers.get("sec-websocket-accept") != expected:
        writer.close()
        raise WSClosed("validasi Sec-WebSocket-Accept gagal")
    return WSConn(reader, writer)


# --------------------------------------------------------------------- Minecraft


class Minecraft:
    def __init__(self, agent: "Agent") -> None:
        self.agent = agent
        self.cfg = agent.cfg
        self.proc: asyncio.subprocess.Process | None = None
        self.read_task: asyncio.Task | None = None
        self.wait_task: asyncio.Task | None = None
        self.started_at = 0.0
        self.state = "stopped"
        self.exit_code: int | None = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    @property
    def paper_path(self) -> Path:
        return self.cfg.server_dir / self.cfg.paper_jar_name

    async def set_state(self, state: str, exit_code: int | None = None) -> None:
        self.state = state
        if exit_code is not None:
            self.exit_code = exit_code
        await self.agent.send_mc_state(state, exit_code)

    async def start(self) -> tuple[bool, str]:
        if self.running:
            return False, "server sudah berjalan"
        if not await self.agent.ensure_runtime_ready():
            return False, "runtime Java/PaperMC belum siap"
        if self.cfg.kill_orphans:
            await self._kill_orphans()

        java = self.agent.java_bin or shutil.which("java")
        if not java or not Path(java).exists():
            return False, "Java tidak ditemukan"
        args = shlex.split(self.cfg.java_args)
        command = [java, *args, "-jar", str(self.paper_path), "nogui"]
        env = os.environ.copy()
        java_path = Path(java)
        if java_path.parent.name == "bin":
            env["JAVA_HOME"] = str(java_path.parent.parent)
            env["PATH"] = str(java_path.parent) + os.pathsep + env.get("PATH", "")
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.cfg.server_dir),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
                env=env,
                limit=1024 * 1024,
            )
        except OSError as exc:
            await self.agent.event(f"Gagal menjalankan PaperMC: {exc}", source="agent")
            await self.set_state("error")
            return False, f"gagal menjalankan Java: {exc}"

        self.proc = proc
        self.started_at = time.time()
        self.exit_code = None
        await self.set_state("starting")
        await self.agent.event(
            f"PaperMC dimulai (pid {proc.pid}, java {java}).", source="agent"
        )
        self.read_task = asyncio.create_task(self._read_output(proc), name="mc-stdout")
        self.wait_task = asyncio.create_task(self._wait_exit(proc), name="mc-wait")
        return True, f"server dimulai (pid {proc.pid})"

    async def _read_output(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        while True:
            try:
                raw = await proc.stdout.readline()
            except (asyncio.LimitOverrunError, ValueError):
                raw = b"[agent] baris log terlalu panjang\n"
            except Exception:
                break
            if not raw:
                break
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if not line:
                continue
            self.agent.emit_log("mc", line)
            if self.state == "starting" and "Done (" in line:
                await self.set_state("running")

    async def _wait_exit(self, proc: asyncio.subprocess.Process) -> None:
        code = await proc.wait()
        if self.proc is not proc:
            return
        self.proc = None
        self.exit_code = code
        await self.agent.event(
            f"PaperMC berhenti (exit code {code}).", source="agent"
        )
        await self.set_state("stopped", exit_code=code)
        self.agent.emit_log(
            "mc", f"[agent] Server process exited with code {code}"
        )

    async def stop(self) -> tuple[bool, str]:
        proc = self.proc
        if proc is None or proc.returncode is not None:
            return False, "server tidak berjalan"
        await self.set_state("stopping")
        try:
            if proc.stdin is not None:
                proc.stdin.write(b"stop\n")
                await proc.stdin.drain()
        except Exception:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=self.cfg.stop_timeout)
            return True, "server dihentikan"
        except asyncio.TimeoutError:
            pass
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=15)
            return True, "server dihentikan (SIGTERM)"
        except asyncio.TimeoutError:
            pass
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
        return True, "server dihentikan paksa (SIGKILL)"

    async def restart(self) -> tuple[bool, str]:
        if self.running:
            ok, message = await self.stop()
            if not ok:
                return False, message
            await asyncio.sleep(1.5)
        return await self.start()

    async def send_command(self, command: str) -> bool:
        proc = self.proc
        if proc is None or proc.returncode is not None or proc.stdin is None:
            return False
        try:
            proc.stdin.write((command + "\n").encode())
            await proc.stdin.drain()
            return True
        except Exception:
            return False

    async def _kill_orphans(self) -> None:
        if not Path("/proc").is_dir():
            return
        marker = str(self.cfg.server_dir)
        killed: list[int] = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid == os.getpid():
                continue
            try:
                raw = (entry / "cmdline").read_bytes()
            except OSError:
                continue
            if not raw:
                continue
            cmdline = raw.replace(b"\x00", b" ").decode("utf-8", "replace")
            if "paper.jar" not in cmdline or marker not in cmdline:
                continue
            if "java" not in cmdline:
                continue
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                killed.append(pid)
            except (ProcessLookupError, PermissionError):
                pass
        if not killed:
            return
        await self.agent.event(
            f"Menghentikan proses PaperMC yatim: {', '.join(map(str, killed))}",
            source="agent",
        )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if not any(Path(f"/proc/{pid}").exists() for pid in killed):
                return
            await asyncio.sleep(0.5)
        for pid in killed:
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass


# ------------------------------------------------------------------- backup sync


class BackupSync:
    def __init__(self, agent: "Agent") -> None:
        self.agent = agent
        self.cfg = agent.cfg
        self.lock = asyncio.Lock()
        self.last_result: dict[str, Any] | None = None

    # ------------------------------------------------------------------ index
    def _load_index(self) -> dict[str, list[int]]:
        try:
            with open(self.cfg.index_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            files = data.get("files") if isinstance(data, dict) else None
            if isinstance(files, dict):
                return {
                    str(key): [int(value[0]), int(value[1])]
                    for key, value in files.items()
                    if isinstance(value, list) and len(value) == 2
                }
        except (OSError, ValueError, TypeError):
            pass
        return {}

    def _save_index(self, files: dict[str, list[int]]) -> None:
        tmp = self.cfg.index_path.with_name(self.cfg.index_path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump({"version": 1, "files": files}, handle, separators=(",", ":"))
        os.replace(tmp, self.cfg.index_path)

    def _excluded(self, rel: str) -> bool:
        parts = rel.split("/")
        if any(part in EXCLUDE_DIRS for part in parts[:-1]):
            return True
        name = parts[-1]
        if name in EXCLUDE_NAMES:
            return True
        if name.endswith(EXCLUDE_SUFFIXES):
            return True
        if rel.startswith("plugins/.paper-remapped/"):
            return True
        return False

    def _scan(self) -> dict[str, list[int]]:
        current: dict[str, list[int]] = {}
        root = self.cfg.server_dir
        if not root.is_dir():
            return current
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(d for d in dirnames if d not in EXCLUDE_DIRS)
            for filename in filenames:
                if filename in EXCLUDE_NAMES or filename.endswith(EXCLUDE_SUFFIXES):
                    continue
                full = Path(dirpath) / filename
                try:
                    if not full.is_file() or full.is_symlink():
                        continue
                    rel = full.relative_to(root).as_posix()
                    if self._excluded(rel):
                        continue
                    stat = full.stat()
                    current[rel] = [stat.st_size, stat.st_mtime_ns]
                except OSError:
                    continue
        return current

    # ------------------------------------------------------------------- sync
    async def loop(self) -> None:
        await asyncio.sleep(self.cfg.backup_on_start_delay)
        while True:
            try:
                if self.cfg.backup_enabled:
                    await self.sync_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self.agent.event(f"Backup gagal: {exc}", source="backup")
            jitter = random.uniform(-10, 10)
            await asyncio.sleep(max(30, self.cfg.backup_interval + jitter))

    async def sync_once(self, force: bool = False) -> dict[str, Any]:
        if not self.cfg.backup_enabled and not force:
            return {"skipped": True}
        if self.lock.locked():
            return {"skipped": True, "reason": "sync sedang berjalan"}
        async with self.lock:
            disable_save = False
            try:
                if self.cfg.backup_flush and self.agent.mc.running:
                    if await self.agent.mc.send_command("save-off"):
                        disable_save = True
                        await self.agent.mc.send_command("save-all flush")
                        await asyncio.sleep(2.5)
                result = await asyncio.to_thread(self._sync_blocking)
            finally:
                if disable_save:
                    await self.agent.mc.send_command("save-on")
            self.last_result = result
            if result.get("files", 0) or result.get("deleted", 0):
                await self.agent.event(
                    "Delta backup selesai: "
                    f"{result.get('files', 0)} file, {result.get('deleted', 0)} dihapus, "
                    f"{human_bytes(result.get('bytes', 0))}, {result.get('batches', 0)} batch.",
                    source="backup",
                )
                await self.agent.send_json(
                    {
                        "type": "backup_done",
                        "files": result.get("files", 0),
                        "deleted": result.get("deleted", 0),
                        "bytes": result.get("bytes", 0),
                        "bytes_human": human_bytes(result.get("bytes", 0)),
                        "batches": result.get("batches", 0),
                    }
                )
            return result

    def _sync_blocking(self) -> dict[str, Any]:
        current = self._scan()
        old = self._load_index()
        world_gone = "world/level.dat" in old and "world/level.dat" not in current
        if (not current and old) or (world_gone and not self.agent.mc.running):
            # A reset host can leave agent metadata behind but wipe the server
            # directory. Never let that empty state delete the master mirror;
            # recovery restores it from a snapshot instead.
            return {
                "skipped": True,
                "reason": "server directory/world kosong; mirror tidak dihapus",
                "files": 0,
                "deleted": 0,
                "bytes": 0,
                "batches": 0,
            }
        changed = [
            rel for rel, meta in current.items() if old.get(rel) != meta
        ]
        deleted = sorted(set(old) - set(current))
        if not changed and not deleted:
            return {"files": 0, "deleted": 0, "bytes": 0, "batches": 0}

        batches: list[list[str]] = []
        batch: list[str] = []
        batch_size = 0
        for rel in changed:
            size = current[rel][0]
            if batch and batch_size + size > self.cfg.backup_batch_bytes:
                batches.append(batch)
                batch = []
                batch_size = 0
            batch.append(rel)
            batch_size += size
        if batch:
            batches.append(batch)
        if not batches and deleted:
            # A deletion-only delta still has to reach the controller.
            batches.append([])

        total_bytes = 0
        uploaded = 0
        for index, files in enumerate(batches, start=1):
            archive, built = self._build_archive(files, deleted)
            for rel in files:
                if rel not in built:
                    # File vanished between scan and archive creation: forget
                    # it so a later sync uploads it if it reappears.
                    current.pop(rel, None)
            try:
                size = archive.stat().st_size
                total_bytes += size
                if size > self.cfg.max_delta_bytes:
                    raise RuntimeError(
                        f"batch {index} ({human_bytes(size)}) melebihi batas {human_bytes(self.cfg.max_delta_bytes)}"
                    )
                self._upload_archive(archive)
                uploaded += len(files)
            finally:
                try:
                    archive.unlink()
                except OSError:
                    pass
        self._save_index(current)
        return {
            "files": uploaded,
            "deleted": len(deleted),
            "bytes": total_bytes,
            "batches": len(batches),
        }

    def _build_archive(self, files: list[str], deleted: list[str]) -> tuple[Path, set[str]]:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        tmp = self.cfg.tmp_dir / f".delta-{stamp}-{os.getpid()}-{random.randrange(1 << 20):x}.tar.gz.part"
        built: set[str] = set()
        server = self.cfg.server_dir
        manifest = {
            "ts": int(time.time()),
            "host": socket.gethostname(),
            "files": files,
            "deleted": deleted,
        }
        payload = json.dumps(manifest, separators=(",", ":")).encode("utf-8")
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(payload)
        info.mtime = int(time.time())
        info.mode = 0o600
        with tarfile.open(tmp, "w:gz", compresslevel=3) as tar:
            tar.addfile(info, io.BytesIO(payload))
            for rel in files:
                full = server / rel
                try:
                    stat = full.stat()
                    if not full.is_file() or full.is_symlink():
                        continue
                    member = tar.gettarinfo(str(full), arcname=rel)
                    if not member.isfile():
                        continue
                    with open(full, "rb") as handle:
                        tar.addfile(member, handle)
                    built.add(rel)
                except OSError:
                    continue
        return tmp, built

    def _upload_archive(self, archive: Path) -> None:
        parsed = urllib.parse.urlsplit(self.cfg.controller_http)
        secure = parsed.scheme == "https"
        host = parsed.hostname or ""
        if not host:
            raise RuntimeError("CONTROLLER_HTTP tidak valid")
        port = parsed.port or (443 if secure else 80)
        path = (parsed.path.rstrip("/") or "") + "/api/agent/delta"
        context = ssl.create_default_context()
        if self.cfg.ws_insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        connection_cls = http.client.HTTPSConnection if secure else http.client.HTTPConnection
        kwargs = {"timeout": 1800}
        if secure:
            kwargs["context"] = context
        connection = connection_cls(host, port, **kwargs)
        size = archive.stat().st_size
        try:
            connection.putrequest(
                "POST", path, skip_host=True, skip_accept_encoding=True
            )
            connection.putheader("Host", parsed.netloc)
            connection.putheader("X-Agent-Token", self.cfg.agent_token)
            connection.putheader("X-Agent-Host", socket.gethostname())
            connection.putheader("Content-Type", "application/gzip")
            connection.putheader("Content-Length", str(size))
            connection.putheader("User-Agent", f"mc-orchestrator-agent/{VERSION}")
            connection.putheader("Connection", "close")
            connection.endheaders()
            with open(archive, "rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read(65536).decode("utf-8", "replace")
            if response.status != 200:
                raise RuntimeError(f"controller HTTP {response.status}: {body[:300]}")
        finally:
            try:
                connection.close()
            except Exception:
                pass

    # --------------------------------------------------------------- plugins
    def plugin_manifest(self) -> list[dict[str, Any]]:
        result = []
        plugins_dir = self.cfg.server_dir / "plugins"
        if not plugins_dir.is_dir():
            return result
        for path in sorted(plugins_dir.glob("*.jar")):
            try:
                if not path.is_file():
                    continue
                result.append(
                    {
                        "name": path.name,
                        "size": path.stat().st_size,
                        "sha256": sha256_file(path),
                    }
                )
            except OSError:
                continue
        return result


# ------------------------------------------------------------------------ Agent


# ------------------------------------------------------------------- frp tunnel


class TunnelError(RuntimeError):
    """Structured, non-fatal tunnel failure.

    ``code`` is stable so the panel can branch on it without parsing prose:
    ``unconfigured`` / ``checksum`` / ``download`` / ``extract`` / ``spawn``.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Tunnel:
    """frpc sidecar: fetched binary + generated TOML + one managed process.

    Deliberately isolated from PaperMC. Every public method swallows its own
    failures and only reports them as a ``tunnel`` event, so a broken frp
    config can never keep the Minecraft server from starting or surviving.
    """

    def __init__(self, agent: "Agent") -> None:
        self.agent = agent
        self.cfg = agent.cfg
        self.proc: asyncio.subprocess.Process | None = None
        self.state = "idle"
        self.code = ""
        self.detail = ""
        # Injectable for the offline harness; production uses the real HTTP
        # downloader, unchanged from the JDK/Paper path.
        self.downloader = download_file
        self.extractor = safe_extract_tar

    # ------------------------------------------------------------------ state
    @property
    def binary(self) -> Path:
        return self.cfg.frp_dir / "frpc"

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    def public_state(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "code": self.code,
            "detail": self.detail,
            "server": self.cfg.frp_server_addr,
            "server_port": self.cfg.frp_server_port,
            "remote_port": self.cfg.frp_remote_port,
            "version": self.cfg.frp_version,
            "pid": self.proc.pid if self.running and self.proc else None,
        }

    async def report(self) -> None:
        payload = {"type": "tunnel", **self.public_state()}
        await self.agent.send_json(payload)

    def _set(self, state: str, detail: str = "", code: str = "") -> None:
        self.state = state
        self.detail = detail
        self.code = code or (state if state in (
            "unconfigured", "checksum", "download", "extract", "disabled"
        ) else "")

    # ---------------------------------------------------------------- prepare
    def _validate(self) -> None:
        cfg = self.cfg
        if not cfg.frp_enabled:
            raise TunnelError("disabled", "FRP_ENABLED=0")
        missing = []
        if not cfg.frp_server_addr:
            missing.append("FRP_SERVER_ADDR")
        if not cfg.frp_token:
            missing.append("FRP_TOKEN")
        if not cfg.frp_sha256:
            # Ner's decision (Phase E): an empty hash means "do not download".
            # Never substitute a guessed or remembered digest here.
            missing.append("FRP_SHA256")
        if missing:
            raise TunnelError("unconfigured", "belum diset: " + ", ".join(missing))
        if not re.fullmatch(r"[0-9a-f]{64}", cfg.frp_sha256):
            raise TunnelError("unconfigured", "FRP_SHA256 bukan 64 karakter hex")
        if not re.fullmatch(r"\d+\.\d+\.\d+", cfg.frp_version):
            raise TunnelError("download", f"FRP_VERSION tidak valid: {cfg.frp_version}")

    def _ensure_binary(self) -> None:
        if self.binary.is_file() and os.access(self.binary, os.X_OK):
            return
        cfg = self.cfg
        url = FRP_RELEASE_URL.format(version=cfg.frp_version, arch=cfg.frp_arch)
        archive = cfg.runtime_dir / f"frp_{cfg.frp_version}_linux_{cfg.frp_arch}.tar.gz"
        part = archive.with_name(archive.name + ".part")
        try:
            self.downloader(url, part, label=f"frpc {cfg.frp_version}")
        except Exception as exc:  # noqa: BLE001
            part.unlink(missing_ok=True)
            raise TunnelError("download", f"unduh frpc gagal: {exc}") from exc
        digest = sha256_file(part)
        if digest != cfg.frp_sha256:
            part.unlink(missing_ok=True)
            raise TunnelError(
                "checksum",
                f"SHA-256 frpc tidak cocok (dapat {digest[:12]}..., "
                f"harap {cfg.frp_sha256[:12]}...)",
            )
        os.replace(part, archive)
        extract_dir = cfg.frp_dir / ".extract"
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        extract_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.extractor(archive, extract_dir)
            found = next((p for p in extract_dir.rglob("frpc") if p.is_file()), None)
            if found is None:
                raise TunnelError("extract", "arsip frp tidak berisi frpc")
            staged = cfg.frp_dir / ".frpc.new"
            shutil.copy2(found, staged)
            os.chmod(staged, 0o755)
            os.replace(staged, self.binary)
        except TunnelError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise TunnelError("extract", f"ekstrak frpc gagal: {exc}") from exc
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)
            archive.unlink(missing_ok=True)

    def _write_config(self) -> None:
        cfg = self.cfg
        # frp >= 0.52 uses TOML. Nested tables must precede the [[proxies]]
        # array-of-tables or the file is invalid.
        text = (
            "# Generated by the DayHost mc-agent — do not edit.\n"
            f'serverAddr = "{_toml(cfg.frp_server_addr)}"\n'
            f"serverPort = {int(cfg.frp_server_port)}\n"
            "\n"
            'auth.method = "token"\n'
            f'auth.token = "{_toml(cfg.frp_token)}"\n'
            "\n"
            "[[proxies]]\n"
            'name = "minecraft"\n'
            'type = "tcp"\n'
            'localIP = "127.0.0.1"\n'
            f"localPort = {int(cfg.frp_local_port)}\n"
            f"remotePort = {int(cfg.frp_remote_port)}\n"
        )
        cfg.frp_config_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cfg.frp_config_path.with_name(cfg.frp_config_path.name + ".part")
        tmp.write_text(text, encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, cfg.frp_config_path)

    async def start(self) -> bool:
        """Prepare and launch frpc. Never raises; False == tunnel is not up."""
        try:
            self._validate()
        except TunnelError as exc:
            self._set(exc.code, exc.message)
            await self.agent.event(
                f"Tunnel frp tidak aktif ({exc.code}): {exc.message}", source="tunnel"
            )
            await self.report()
            return False
        if self.running:
            return True
        self._set("preparing", f"frpc {self.cfg.frp_version}")
        try:
            await asyncio.to_thread(self._ensure_binary)
            await asyncio.to_thread(self._write_config)
        except TunnelError as exc:
            self._set(exc.code, exc.message)
            await self.agent.event(
                f"Tunnel frp gagal disiapkan ({exc.code}): {exc.message}", source="tunnel"
            )
            await self.report()
            return False
        except Exception as exc:  # noqa: BLE001
            self._set("error", str(exc))
            await self.agent.event(f"Tunnel frp gagal disiapkan: {exc}", source="tunnel")
            await self.report()
            return False

        try:
            self.cfg.frp_log_path.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(self.cfg.frp_log_path, "ab")
        except OSError:
            log_handle = subprocess.DEVNULL  # type: ignore[assignment]
        try:
            # start_new_session == setsid; no shell, so no orphaned sh parent.
            proc = await asyncio.create_subprocess_exec(
                str(self.binary),
                "-c",
                str(self.cfg.frp_config_path),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log_handle,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as exc:  # noqa: BLE001
            self._set("error", f"gagal menjalankan frpc: {exc}")
            await self.agent.event(f"Tunnel frp gagal dijalankan: {exc}", source="tunnel")
            await self.report()
            return False
        finally:
            if log_handle is not subprocess.DEVNULL:
                log_handle.close()  # the child holds its own descriptor

        self.proc = proc
        self._watch_task = asyncio.create_task(self._watch(proc), name="frpc-watch")
        await asyncio.sleep(1.0)
        if proc.returncode is not None:
            tail = self._log_tail()
            self._set("failed", f"frpc keluar (exit {proc.returncode}): {tail}")
            await self.agent.event(
                f"Tunnel frp gagal: frpc keluar dengan kode {proc.returncode}.", source="tunnel"
            )
            await self.report()
            return False
        self._set(
            "running",
            f"{self.cfg.frp_server_addr}:{self.cfg.frp_server_port} "
            f"-> 127.0.0.1:{self.cfg.frp_local_port} (remote {self.cfg.frp_remote_port})",
        )
        await self.agent.event(
            f"Tunnel frp aktif (pid {proc.pid}): port {self.cfg.frp_remote_port} di "
            f"{self.cfg.frp_server_addr}.",
            source="tunnel",
        )
        await self.report()
        return True

    async def _watch(self, proc: asyncio.subprocess.Process) -> None:
        """Report a post-start frpc death; never touches the MC process."""
        try:
            code = await proc.wait()
        except asyncio.CancelledError:
            raise
        if self.proc is not proc:
            return
        self.proc = None
        self._set("failed", f"frpc keluar (exit {code})")
        try:
            await self.agent.event(
                f"Tunnel frp terputus (exit {code}); server Minecraft tidak terpengaruh.",
                source="tunnel",
            )
            await self.report()
        except Exception:  # noqa: BLE001 - a dying tunnel must stay quiet
            pass

    def _log_tail(self, limit: int = 400) -> str:
        try:
            data = self.cfg.frp_log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
        tail = data.strip().splitlines()[-3:]
        return " | ".join(tail)[:limit]

    async def stop(self) -> None:
        proc, self.proc = self.proc, None
        task, self._watch_task = getattr(self, "_watch_task", None), None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if proc is None or proc.returncode is not None:
            if self.state == "running":
                self._set("stopped")
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=8)
        except asyncio.TimeoutError:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
        self._set("stopped")


def _toml(value: str) -> str:
    """Escape for a TOML basic string; control chars are not legal raw."""
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


class Agent:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ws: WSConn | None = None
        self.connected = False
        self.running = True
        self.java_bin = cfg.java_bin or ""
        self.mc = Minecraft(self)
        self.backup = BackupSync(self)
        self.tunnel = Tunnel(self)
        self.log_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=5000)
        self.pending: deque[dict[str, str]] = deque(maxlen=cfg.mc_log_lines)
        self.plugin_lock = asyncio.Lock()
        self.log_task: asyncio.Task | None = None
        self.ws_task: asyncio.Task | None = None
        self.backup_task: asyncio.Task | None = None
        self.boot_task: asyncio.Task | None = None
        self.shutdown_event = asyncio.Event()
        self.exit_code: int | None = None

    # ------------------------------------------------------------------ utils
    def emit_log(self, source: str, text: str) -> None:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": source,
            "text": text,
        }
        try:
            self.log_queue.put_nowait(entry)
        except asyncio.QueueFull:
            try:
                self.log_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self.log_queue.put_nowait(entry)
            except asyncio.QueueFull:
                pass

    async def event(self, text: str, source: str = "agent") -> None:
        log(text)
        self.emit_log(source, text)

    async def send_json(self, payload: dict[str, Any]) -> bool:
        ws = self.ws
        if ws is None or not self.connected:
            return False
        try:
            await ws.send_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            return True
        except Exception:
            self.connected = False
            try:
                await ws.close()
            except Exception:
                pass
            return False

    async def send_mc_state(self, state: str, exit_code: int | None = None) -> None:
        payload = {"type": "mc_state", "state": state, "pid": self.mc.proc.pid if self.mc.proc else None}
        if exit_code is not None:
            payload["exit_code"] = exit_code
        await self.send_json(payload)

    def artifact_url(self, url: str, name: str = "") -> str:
        """Resolve a plugin artifact URL.

        A relative URL is always the panel's. An absolute upstream URL is only
        rewritten when the operator configured ARTIFACT_PATH_TEMPLATE — no
        endpoint is invented here, so an unconfigured panel keeps the direct
        URL (recorded as an open question in the Phase-E report).
        """
        base = self.cfg.artifact_base_url
        if url.startswith("/"):
            return (base + url) if base else url
        template = self.cfg.artifact_path_template
        if template and base and name:
            return base + template.replace("{name}", urllib.parse.quote(name, safe=""))
        return url

    # -------------------------------------------------------------- lifecycle
    async def run(self) -> None:
        self.cfg.make_dirs()
        self._write_pid()
        self._install_signals()
        self.log_task = asyncio.create_task(self._log_pump(), name="log-pump")
        self.backup_task = asyncio.create_task(self.backup.loop(), name="backup-loop")
        self.ws_task = asyncio.create_task(self._ws_supervisor(), name="ws-supervisor")
        self.boot_task = asyncio.create_task(self._boot(), name="boot")
        await self.shutdown_event.wait()
        await self._shutdown()

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._request_stop()))
            except (NotImplementedError, RuntimeError):
                pass

    async def _request_stop(self) -> None:
        log("Menerima sinyal berhenti; agent keluar.")
        self.running = False
        self.shutdown_event.set()

    def _write_pid(self) -> None:
        try:
            self.cfg.pid_path.write_text(str(os.getpid()), encoding="utf-8")
        except OSError:
            pass

    def _remove_pid(self) -> None:
        try:
            if self.cfg.pid_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                self.cfg.pid_path.unlink()
        except OSError:
            pass

    async def _boot(self) -> None:
        await asyncio.sleep(0.2)
        await self.event("Agent dimulai (v%s)." % VERSION)
        if self.cfg.provision:
            try:
                await self.ensure_runtime_ready()
            except Exception as exc:  # noqa: BLE001
                await self.event(f"Provisioning runtime gagal: {exc}", source="agent")
        if self.cfg.autostart:
            ok, message = await self.mc.start()
            await self.event(("Startup: " if ok else "Startup gagal: ") + message)
        else:
            await self.send_mc_state(self.mc.state)
        # Tunnel last, and never allowed to break the server: a failed frpc is
        # a feature that is missing, not a dead Minecraft host.
        try:
            await self.tunnel.start()
        except Exception as exc:  # noqa: BLE001 - belt and braces
            await self.event(f"Tunnel frp gagal (diabaikan): {exc}", source="tunnel")

    async def _shutdown(self) -> None:
        self.running = False
        try:
            await self.tunnel.stop()
        except Exception:  # noqa: BLE001
            pass
        ws = self.ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        for task in (self.log_task, self.backup_task, self.ws_task, self.boot_task):
            if task and not task.done():
                task.cancel()
        for task in (self.log_task, self.backup_task, self.ws_task, self.boot_task):
            if task:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._remove_pid()
        log("Agent berhenti.")

    # ------------------------------------------------------------------- logs
    async def _log_pump(self) -> None:
        while True:
            try:
                entry = await self.log_queue.get()
            except asyncio.CancelledError:
                raise
            batch = [entry]
            deadline = asyncio.get_running_loop().time() + 0.15
            while len(batch) < 100:
                timeout = deadline - asyncio.get_running_loop().time()
                if timeout <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self.log_queue.get(), timeout))
                except asyncio.TimeoutError:
                    break
            if self.connected and await self.send_json({"type": "log", "entries": batch}):
                continue
            for item in batch:
                self.pending.append(item)

    async def _flush_pending_logs(self) -> None:
        while self.pending and self.connected:
            batch = [self.pending.popleft() for _ in range(min(100, len(self.pending)))]
            if not await self.send_json({"type": "log", "entries": batch}):
                for item in reversed(batch):
                    self.pending.appendleft(item)
                return

    # ---------------------------------------------------------------- websocket
    async def _ws_supervisor(self) -> None:
        backoff = 1.0
        while self.running:
            try:
                await self._ws_session()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log(f"WebSocket: {exc}")
            self.connected = False
            self.ws = None
            if not self.running:
                break
            await asyncio.sleep(backoff + random.uniform(0, 0.7))
            backoff = min(15.0, backoff * 1.8)

    async def _ws_session(self) -> None:
        conn = await ws_connect(self.cfg.controller_ws, self.cfg.agent_token, self.cfg.ws_insecure)
        self.ws = conn
        self.connected = True
        log("WebSocket terhubung ke controller.")
        if not await self._send_hello():
            await conn.close()
            raise WSClosed("gagal mengirim hello")
        await self._flush_pending_logs()
        heartbeat = asyncio.create_task(self._heartbeat(conn), name="ws-heartbeat")
        try:
            while self.running:
                message = await asyncio.wait_for(conn.recv(), timeout=75)
                if message is None:
                    continue
                try:
                    payload = json.loads(message)
                except ValueError:
                    continue
                if isinstance(payload, dict):
                    await self._handle_message(payload)
        except asyncio.TimeoutError as exc:
            raise WSClosed("tidak ada frame dari controller") from exc
        except Exception as exc:
            if self.running:
                log(f"WebSocket terputus: {exc}")
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except (asyncio.CancelledError, Exception):
                pass
            await conn.close()
            if self.ws is conn:
                self.ws = None
                self.connected = False
            log("WebSocket terputus.")

    async def _send_hello(self) -> bool:
        plugins = await asyncio.to_thread(self.backup.plugin_manifest)
        return await self.send_json(
            {
                "type": "hello",
                "version": VERSION,
                "hostname": socket.gethostname(),
                "platform": f"{platform.system()} {platform.machine()} / py {platform.python_version()}",
                "server_dir": str(self.cfg.server_dir),
                "mc_state": self.mc.state,
                "installed_plugins": plugins,
                "backup_interval": self.cfg.backup_interval,
                "tunnel": self.tunnel.public_state(),
            }
        )

    async def _heartbeat(self, conn: WSConn) -> None:
        while self.running:
            await asyncio.sleep(10)
            if not await self.send_json(
                {
                    "type": "heartbeat",
                    "mc_state": self.mc.state,
                    "backup_enabled": self.cfg.backup_enabled,
                }
            ):
                try:
                    await conn.close()
                except Exception:
                    pass
                return

    async def _handle_message(self, message: dict[str, Any]) -> None:
        kind = message.get("type")
        if kind == "command":
            asyncio.create_task(self._run_command(message))
        elif kind == "plugin":
            asyncio.create_task(self._install_plugin(message))
        elif kind == "plugin_sync":
            for plugin in message.get("plugins") or []:
                if isinstance(plugin, dict):
                    asyncio.create_task(self._install_plugin(plugin))
        elif kind == "ping":
            await self.send_json({"type": "pong"})

    async def _run_command(self, message: dict[str, Any]) -> None:
        command_id = str(message.get("id", ""))
        action = str(message.get("action", "")).lower()
        ok = False
        detail = ""
        try:
            if action == "start":
                ok, detail = await self.mc.start()
            elif action == "stop":
                ok, detail = await self.mc.stop()
            elif action == "restart":
                ok, detail = await self.mc.restart()
            elif action == "backup":
                ok, detail = True, "delta sync dijadwalkan"
                asyncio.create_task(self.backup.sync_once(force=True))
            else:
                detail = f"aksi tidak dikenal: {action}"
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            await self.event(f"Perintah {action} gagal: {exc}", source="agent")
        await self.send_json(
            {"type": "ack", "id": command_id, "action": action, "ok": ok, "message": detail}
        )

    # ---------------------------------------------------------------- runtime
    async def ensure_runtime_ready(self) -> bool:
        self.cfg.make_dirs()
        eula = self.cfg.server_dir / "eula.txt"
        if not eula.is_file():
            eula.write_text("eula=true\n", encoding="utf-8")
        if not self.cfg.provision:
            self.java_bin = self._find_java()
            return bool(self.java_bin) and self.mc.paper_path.is_file()

        if not self._find_java():
            await self.event(
                f"Java belum tersedia; mengunduh JDK {self.cfg.java_version} portable...",
                source="agent",
            )
            try:
                await asyncio.to_thread(self._download_jre)
            except Exception as exc:  # noqa: BLE001
                await self.event(f"Unduh Java gagal: {exc}", source="agent")
        self.java_bin = self._find_java()
        if not self.java_bin:
            await self.event(
                f"Java {self.cfg.java_version}+ tidak ditemukan setelah provisioning.",
                source="agent",
            )
            return False

        if not self.mc.paper_path.is_file() or self.mc.paper_path.stat().st_size < 1_000_000:
            await self.event("PaperMC jar belum ada; mengunduh...", source="agent")
            try:
                await asyncio.to_thread(self._download_paper)
            except Exception as exc:  # noqa: BLE001
                await self.event(f"Unduh PaperMC gagal: {exc}", source="agent")
                return False
        return self.mc.paper_path.is_file()

    def _find_java(self) -> str:
        # Order matters: an operator override, then the portable JDK of the
        # pinned major, then whatever the system provides. A stale jdk25
        # directory is deliberately NOT a candidate.
        portable = self.cfg.runtime_dir / f"jdk{self.cfg.java_version}" / "bin" / "java"
        if self.cfg.java_bin:
            path = Path(self.cfg.java_bin)
            if path.is_file() and os.access(path, os.X_OK) and java_version_ok(
                str(path), self.cfg.java_version
            ):
                return str(path)
        if portable.is_file() and os.access(portable, os.X_OK) and java_version_ok(
            str(portable), self.cfg.java_version
        ):
            return str(portable)
        system = shutil.which("java")
        if system and java_version_ok(system, self.cfg.java_version):
            return system
        return ""

    def _download_jre(self) -> None:
        arch = self.cfg.jre_arch if self.cfg.jre_arch in ("x64", "aarch64") else "x64"
        version = self.cfg.java_version
        url = ADOPTIUM_API.format(version=version, arch=arch)
        archive = self.cfg.runtime_dir / f"jdk{version}.tar.gz.part"
        final = self.cfg.runtime_dir / f"jdk{version}.tar.gz"
        self._download_file(url, archive, label=f"JDK {version}")
        os.replace(archive, final)
        extract_dir = self.cfg.runtime_dir / ".jdk-extract"
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        extract_dir.mkdir(parents=True, exist_ok=True)
        safe_extract_tar(final, extract_dir)
        candidates = [p for p in extract_dir.iterdir() if p.is_dir()]
        if not candidates:
            raise RuntimeError("arsip JDK tidak berisi direktori")
        target = self.cfg.runtime_dir / f"jdk{version}"
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        os.replace(candidates[0], target)
        shutil.rmtree(extract_dir, ignore_errors=True)
        try:
            final.unlink()
        except OSError:
            pass
        if not (target / "bin" / "java").is_file():
            raise RuntimeError("struktur JRE tidak valid")

    def _download_paper(self) -> None:
        version = self.cfg.paper_version or "latest"
        if version == "latest":
            meta = self._fetch_json(FILL_API)
            versions = meta.get("versions", {}) if isinstance(meta, dict) else {}
            version = next((key for key in versions if "-" not in key), "26.1.2")
        build = self._fetch_json(f"{FILL_API}/versions/{version}/builds/latest")
        downloads = build.get("downloads") or {}
        selected = downloads.get("server:default") or next(iter(downloads.values()), None)
        if not selected:
            raise RuntimeError("PaperMC tidak menyediakan build server")
        url = selected["url"]
        sha = (selected.get("checksums") or {}).get("sha256")
        tmp = self.cfg.server_dir / (self.cfg.paper_jar_name + ".part")
        self._download_file(url, tmp, label=f"PaperMC {version}")
        if sha and sha256_file(tmp) != sha:
            tmp.unlink(missing_ok=True)
            raise RuntimeError("checksum PaperMC tidak cocok")
        os.replace(tmp, self.mc.paper_path)
        try:
            (self.cfg.server_dir / "paper-version.json").write_text(
                json.dumps(
                    {
                        "version": version,
                        "build": build.get("id"),
                        "channel": build.get("channel"),
                        "sha256": sha,
                        "installed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except OSError:
            pass

    @staticmethod
    def _fetch_json(url: str) -> dict[str, Any]:
        request = urllib.request.Request(
            url, headers={"User-Agent": f"mc-orchestrator-agent/{VERSION}"}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))

    @staticmethod
    def _download_file(url: str, target: Path, label: str) -> None:
        download_file(url, target, label)

    # ---------------------------------------------------------------- plugins
    async def _install_plugin(self, message: dict[str, Any]) -> None:
        async with self.plugin_lock:
            await self._install_plugin_locked(message)

    async def _install_plugin_locked(self, message: dict[str, Any]) -> None:
        name = str(message.get("name", ""))
        if not PLUGIN_RE.fullmatch(name):
            await self.send_json(
                {"type": "plugin_ack", "name": name, "ok": False, "message": "nama tidak valid"}
            )
            return
        expected = str(message.get("sha256") or "")
        url = str(message.get("url") or "")
        if url.startswith("/"):
            if not self.cfg.artifact_base_url:
                await self.send_json(
                    {"type": "plugin_ack", "name": name, "ok": False,
                     "message": "URL relatif tanpa CONTROLLER_HTTP"}
                )
                return
            url = self.cfg.artifact_base_url + url
        dest = self.cfg.server_dir / "plugins" / name
        try:
            if dest.is_file() and expected and sha256_file(dest) == expected:
                ok, note = True, "sudah terpasang"
            else:
                await self.event(f"Mengunduh plugin {name}...", source="agent")
                await asyncio.to_thread(
                    self._download_plugin, self.artifact_url(url, name), dest, expected
                )
                ok, note = True, "terpasang"
            await self.send_json(
                {"type": "plugin_ack", "name": name, "ok": ok, "message": note}
            )
            manifest = await asyncio.to_thread(self.backup.plugin_manifest)
            await self.send_json({"type": "plugin_state", "installed_plugins": manifest})
            await self.event(
                f"Plugin {name} {note}."
                + (" Restart server untuk memuatnya." if self.mc.running else ""),
                source="agent",
            )
        except Exception as exc:  # noqa: BLE001
            await self.send_json(
                {"type": "plugin_ack", "name": name, "ok": False, "message": str(exc)}
            )
            await self.event(f"Plugin {name} gagal dipasang: {exc}", source="agent")

    def _download_plugin(self, url: str, dest: Path, expected_sha: str) -> None:
        if not url.startswith(("http://", "https://")):
            raise RuntimeError("URL plugin tidak valid")
        tmp = dest.with_name(dest.name + ".part")
        request = urllib.request.Request(
            url,
            headers={
                "X-Agent-Token": self.cfg.agent_token,
                "User-Agent": f"mc-orchestrator-agent/{VERSION}",
            },
        )
        context = None
        if url.startswith("https://") and self.cfg.ws_insecure:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(request, timeout=300, context=context) as response, open(tmp, "wb") as out:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            shutil.copyfileobj(response, out, length=512 * 1024)
        if expected_sha and sha256_file(tmp) != expected_sha:
            tmp.unlink(missing_ok=True)
            raise RuntimeError("checksum plugin tidak cocok")
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, dest)


# ------------------------------------------------------------------------ utils


def download_file(url: str, target: Path, label: str) -> None:
    """Stream one URL to ``target``. Module-level so tests can inject a fake."""
    target.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        url, headers={"User-Agent": f"mc-orchestrator-agent/{VERSION}"}
    )
    log(f"Mengunduh {label} dari {url.split('?')[0]}")
    with urllib.request.urlopen(request, timeout=900) as response, open(target, "wb") as out:
        total = int(response.headers.get("Content-Length") or 0)
        done = 0
        report = 0.2
        while True:
            chunk = response.read(512 * 1024)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            if total and done / total >= report:
                log(f"  {label}: {done * 100 // total}% ({human_bytes(done)} / {human_bytes(total)})")
                report += 0.2
    if target.stat().st_size < 1024:
        raise RuntimeError(f"unduhan {label} terlalu kecil")


def human_bytes(num: int | float) -> str:
    num = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024.0 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def java_version_ok(java: str, min_major: int = 21) -> bool:
    try:
        result = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=20
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    output = (result.stderr or "") + (result.stdout or "")
    match = re.search(r'version "(\d+)(?:[.\-_]|")', output)
    if not match:
        return False
    try:
        return int(match.group(1)) >= int(min_major)
    except ValueError:
        return False


def safe_extract_tar(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            name = member.name.lstrip("./") if member.name.startswith("./") else member.name
            if not name or name.startswith("/") or "\x00" in name:
                continue
            parts = [part for part in re.split(r"[\\/]+", name) if part not in ("", ".")]
            if any(part == ".." for part in parts):
                continue
            target = (destination / Path(*parts)).resolve()
            try:
                if os.path.commonpath([str(destination), str(target)]) != str(destination):
                    continue
            except ValueError:
                continue
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    continue
                with open(target, "wb") as out:
                    shutil.copyfileobj(source, out, length=1024 * 1024)
                # Permission bits matter: without this a downloaded JDK's
                # bin/java is not executable and provisioning silently fails.
                try:
                    os.chmod(target, member.mode & 0o777)
                except OSError:
                    pass
                if member.mtime:
                    os.utime(target, (member.mtime, member.mtime))
            elif member.issym():
                link = os.path.normpath(
                    os.path.join(os.path.dirname(name), member.linkname)
                )
                if link.startswith("..") or link.startswith("/"):
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    if target.exists() or target.is_symlink():
                        target.unlink()
                    os.symlink(member.linkname, target)
                except OSError:
                    pass


# ------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SoloA Minecraft agent")
    parser.add_argument("--config", help="path ke agent.env")
    parser.add_argument("--version", action="version", version=VERSION)
    args = parser.parse_args(argv)
    config_path = Path(args.config) if args.config else None
    cfg = Config(config_path)
    cfg.make_dirs()
    log(f"mc-agent {VERSION} | server={cfg.server_dir} | controller={cfg.controller_ws}")
    agent = Agent(cfg)
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
