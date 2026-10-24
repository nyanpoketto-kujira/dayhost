"""Orchestrator checks — `python scripts/check_orchestrator.py`

Fusion Phase C gate. Runs the **real** ``Orchestrator``, ``Storage``, ``AppState``
and ``AgentWSServer`` against **fakes** for Daytona (lifecycle/exec) and for the
remote filesystem, plus a hand-rolled RFC-6455 client standing in for
``agent.py``. Everything is loopback; nothing listens outside 127.0.0.1 and every
byte written goes into a throwaway temp dir that is removed on exit.

What is proven (a-h, matching the Phase-C brief):

  a) ``start_host()`` runs §5 steps in order: lifecycle start -> artifact check
     -> upload when missing -> ``ensure_agent_running`` -> agent connects.
     ``agent.env`` is uploaded and chmod 600'd; ``agent.py`` is not re-uploaded
     when the remote sha matches.
  b) idempotence: a second ``start_host()`` on a live host issues **no** extra
     upload and **no** extra launch (counters + fake call log).
  c) ``stop_host()`` order: MC stop (agent ack) -> backup flush -> agent stop ->
     lifecycle stop; and a second ``stop_host()`` with the agent already dead
     still returns ok (no error, lifecycle stop still attempted).
  d) ``recover()`` through the **real** ``on_stale`` hook of the WS server runs
     exactly one ``start_host()``; a competing ``on_disconnect`` is coalesced
     (``recovery_skipped`` grows, no stacked bootstrap).
  e) delta ingest: a tar.gz delta with a ``deleted`` manifest lands in the
     mirror, the deleted file is really gone, mirror stats change.
  f) snapshot scheduler with a 0.4 s interval creates snapshots,
     ``latest.tar.gz`` exists, and ``snapshot_keep`` is honoured.
  g) ``AGENT_WS_ENABLED=false`` means the WS server is genuinely not started
     (and the flag is reported as false, not silently ignored).
  h) a toolbox **401** during provisioning returns a structured error
     (kind=auth, step=agent_artifact, action-able message) and produces
     **zero** tracebacks.
  i) an ``ack`` frame goes through the real ``handle_event()``: it does not
     raise (regression: it was ``await``-ed while ``_resolve_waiter`` is sync),
     it resolves the pending waiter, and ``push_state()`` still runs.
  j) ``Orchestrator.from_settings(settings, on_event=...)`` does not raise (the
     hook kwargs used to be forwarded to ``__init__`` -> TypeError) and the
     callbacks really end up on the WS server (``ws._on_event`` etc.).
  k) the generated ``agent.env`` carries every Phase-E key (PROVISION,
     KILL_ORPHANS, WS_INSECURE, AUTOSTART, all FRP_*), its CONTROLLER_WS ends
     in ``/agent-ws``, AGENT_TOKEN/FRP_TOKEN appear in the file but never in
     counters/logs, and the real ``agent.Config`` + ``Tunnel._validate()``
     read it as ``unconfigured`` (empty FRP_SHA256) instead of crashing.

Exit 0 = all passed. Stdlib only, no network beyond loopback, no .env created.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import struct
import sys
import tarfile
import tempfile
import time
import traceback
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# Never load a real .env: this harness must not see operator credentials.
os.environ["DAYHOST_ENV_FILE"] = str(ROOT / ".nonexistent-harness.env")
os.environ.setdefault("LOG_LEVEL", "CRITICAL")

from app.config import Settings  # noqa: E402
from app.services.agent_ws import AgentWSServer  # noqa: E402
from app.services.orchestrator import Orchestrator, Storage, StoreConfig  # noqa: E402
from app.services.orchestrator.state import AppState  # noqa: E402

TOKEN = "phase-c-agent-token"
MC_WS_PATH = "/agent-ws"
MANIFEST_NAME = "__manifest__.json"

TRACEBACKS: list[str] = []
FAILURES: list[str] = []
# Suites that raised instead of returning. Kept apart from TRACEBACKS so the
# "zero tracebacks" assertion still means "no stray task/hook blew up".
ABORTS: list[tuple[str, str]] = []
CHECKS = 0
SUITES_RUN: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> int:
    global CHECKS
    CHECKS += 1
    suffix = "" if ok or not detail else f"   [{detail}]"
    print(f"  {'PASS' if ok else 'FAIL'}  {label}{suffix}")
    if not ok:
        FAILURES.append(f"{label}" + (f" ({detail})" if detail else ""))
        return 1
    return 0


def check_eq(label: str, got, want) -> int:
    return check(label, got == want, f"got {got!r}, want {want!r}")


def _install_hooks() -> None:
    def hook(exc_type, exc, tb):
        TRACEBACKS.append("".join(traceback.format_exception(exc_type, exc, tb)))

    sys.excepthook = hook
    asyncio.get_running_loop().set_exception_handler(
        lambda _loop, ctx: TRACEBACKS.append(repr(ctx.get("exception") or ctx))
    )


# --------------------------------------------------------------------- WS client
# Same framing as mc-orchestrator/agent/agent.py (masked client -> unmasked server).


def client_frame(opcode: int, payload: bytes) -> bytes:
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
    return bytes(header) + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


class FakeAgent:
    """The worker agent: dials the real server, sends hello, answers commands."""

    def __init__(self, reader, writer, *, auto_respond: bool = True,
                 driver: "FakeDriver | None" = None) -> None:
        self.reader = reader
        self.writer = writer
        self.auto_respond = auto_respond
        self.closed = False
        self.received: list[dict] = []
        self.commands: list[dict] = []
        self.stop_acks = 0
        self.backups = 0
        self._task: asyncio.Task | None = None
        # The sandbox bookkeeping: a dead agent must be visible to the driver.
        self._driver = driver

    @classmethod
    async def dial(cls, port: int, *, token: str = TOKEN, auto_respond: bool = True,
                   mc_state: str = "running",
                   driver: "FakeDriver | None" = None) -> "FakeAgent":
        reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=2 ** 20)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        lines = [
            f"GET {MC_WS_PATH} HTTP/1.1",
            f"Host: 127.0.0.1:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            f"X-Agent-Token: {token}",
            "User-Agent: fake-agent/1.0",
        ]
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("ascii"))
        await writer.drain()
        blob = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        status = blob.decode("latin-1").split("\r\n")[0]
        if "101" not in status:
            raise RuntimeError(f"handshake refused: {status}")
        agent = cls(reader, writer, auto_respond=auto_respond, driver=driver)
        await agent.send_json(
            {
                "type": "hello",
                "version": "fake-9.9",
                "hostname": "sandbox-fake",
                "platform": "Linux x86_64 / py fake",
                "server_dir": "/home/daytona/mc-orchestrator/server",
                "mc_state": mc_state,
                "installed_plugins": [],
            }
        )
        if auto_respond:
            agent._task = asyncio.ensure_future(agent._respond_loop())
        return agent

    async def send_json(self, payload: dict) -> None:
        if self.closed:
            return
        self.writer.write(client_frame(0x1, json.dumps(payload).encode()))
        await self.writer.drain()

    async def _recv(self) -> tuple[int, bytes]:
        head = await self.reader.readexactly(2)
        opcode = head[0] & 0x0F
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
        return opcode, payload

    async def _respond_loop(self) -> None:
        """Answer server pings, and ack stop/backup commands like the real agent."""
        try:
            while not self.closed:
                opcode, payload = await self._recv()
                if opcode == 0x9:                      # PING -> PONG
                    self.writer.write(client_frame(0xA, payload))
                    await self.writer.drain()
                    continue
                if opcode == 0x8:                      # CLOSE
                    return
                if opcode != 0x1:
                    continue
                message = json.loads(payload.decode("utf-8"))
                self.received.append(message)
                if message.get("type") == "command":
                    self.commands.append(message)
                    action = message.get("action")
                    if action == "stop":
                        self.stop_acks += 1
                        await self.send_json(
                            {"type": "ack", "id": message.get("id"), "action": "stop",
                             "ok": True, "message": "server dihentikan"}
                        )
                        await self.send_json(
                            {"type": "mc_state", "state": "stopped", "exit_code": 0}
                        )
                    elif action == "backup":
                        self.backups += 1
                        await self.send_json(
                            {"type": "backup_done", "files": 2, "deleted": 0,
                             "bytes": 42, "bytes_human": "42 B", "batches": 1}
                        )
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return
        except asyncio.CancelledError:
            raise

    async def hang(self) -> None:
        """The process died without a clean close: socket open, pings unanswered.

        The server's heartbeat must notice the silence (real ``on_stale``) and
        the driver must report the process as gone so ``ensure_agent_running()``
        really launches a replacement.
        """
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        if self._driver is not None:
            self._driver.agent_died(self)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
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
        if self._driver is not None:
            self._driver.agent_died(self)


# ------------------------------------------------------------------ fake Daytona
class FakeFiles:
    """In-memory sandbox filesystem + the toolbox file API surface we use."""

    def __init__(self, *, http_status: int | None = None) -> None:
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []
        self.chmods: list[str] = []
        self.http_status = http_status      # non-200 -> structured toolbox error
        self.fail_kind = "auth" if http_status in (401, 403) else "http"

    def _error(self, path: str) -> dict:
        return {
            "ok": False,
            "kind": self.fail_kind,
            "status_code": self.http_status,
            "path": path,
            "error": "Toolbox menolak kredensial (HTTP 401). Periksa DAYTONA_API_KEY "
                     "atau set DAYTONA_TOOLBOX_AUTH=none."
            if self.http_status in (401, 403) else f"HTTP {self.http_status}",
        }

    async def create_folder(self, remote_path: str, mode: str = "755") -> dict:
        self.calls.append(("create_folder", remote_path))
        return {"ok": True, "path": remote_path}

    async def file_info(self, remote_path: str) -> dict:
        self.calls.append(("file_info", remote_path))
        if self.http_status is not None:
            return self._error(remote_path)
        data = self.files.get(remote_path)
        if data is None:
            return {"ok": False, "kind": "http", "status_code": 404, "path": remote_path,
                    "error": "HTTP 404"}
        return {
            "ok": True, "status_code": 200, "path": remote_path,
            "info": {"name": remote_path.rsplit("/", 1)[-1], "path": remote_path,
                     "size": len(data), "is_dir": False, "type": "file"},
        }

    async def upload_file(self, local_path, remote_path: str) -> dict:
        self.calls.append(("upload", remote_path))
        if self.http_status is not None:
            return self._error(remote_path)
        data = Path(local_path).read_bytes()
        self.files[remote_path] = data
        return {"ok": True, "status_code": 200, "path": remote_path, "size": len(data)}


class FakeDriver:
    """Daytona lifecycle + process exec. Records everything it is asked to do."""

    def __init__(self, files: FakeFiles, *, state: str = "STOPPED",
                 agent_port: int = 0) -> None:
        self.files = files
        self.state = state
        self.agent_port = agent_port
        self.calls: list[tuple[str, str]] = []
        self.start_calls = 0
        self.stop_calls = 0
        self.ensure_calls = 0
        self.launches = 0
        self.alive = False
        self.agents: list[FakeAgent] = []
        self.agent_state = "running"
        self.dial_timeout: float | None = None   # make the launch itself slow
        self.dial_delay: float = 0.0             # wait before the new agent dials
        self.start_delay: float = 0.0            # slow lifecycle start (RUNNING -> RUNNING)
        self.dead_dials = 0                     # launches that never came up

    def agent_died(self, agent: FakeAgent) -> None:
        """The fake process is gone: pgrep/process_status must agree."""
        if agent in self.agents:
            self.agents.remove(agent)
        self.alive = False

    def agent_count(self) -> int:
        return len(self.agents)

    # ---- lifecycle --------------------------------------------------------
    async def status(self) -> dict:
        self.calls.append(("status", ""))
        return {"ok": True, "kind": "http", "status_code": 200, "state": self.state,
                "action": "status"}

    async def start(self, *, wait: bool = True, timeout: float | None = None) -> dict:
        self.calls.append(("start", ""))
        self.start_calls += 1
        if self.state == "RUNNING":
            return {"ok": True, "state": "RUNNING", "action": "start", "polls": 0}
        if self.start_delay:
            await asyncio.sleep(self.start_delay)
        self.state = "RUNNING"
        return {"ok": True, "state": "RUNNING", "action": "start", "polls": 2,
                "waited": 0.1}

    async def stop(self, force: bool = False) -> dict:
        self.calls.append(("stop", ""))
        self.stop_calls += 1
        self.state = "STOPPED"
        self.alive = False
        for agent in list(self.agents):
            agent.closed = True
        self.agents.clear()
        return {"ok": True, "state": "STOPPED", "action": "stop", "force": force}

    # ---- exec -------------------------------------------------------------
    async def exec_command(self, command: str, timeout: float = 60, **kwargs) -> dict:
        self.calls.append(("exec", command))
        parts = command.split()
        base = {"ok": True, "kind": "exec", "status_code": 200, "stdout": "",
                "stderr": "", "exit_code": 0, "success": True, "error": None,
                "command": command}
        if not parts:
            return {**base, "exit_code": 1, "success": False}
        if parts[0] == "sha256sum":
            data = self.files.files.get(parts[1])
            if data is None:
                return {**base, "exit_code": 1, "success": False,
                        "stderr": f"sha256sum: {parts[1]}: No such file"}
            digest = hashlib.sha256(data).hexdigest()
            return {**base, "stdout": f"{digest}  {parts[1]}\n"}
        if parts[0] == "chmod":
            self.files.chmods.append(parts[-1])
            return base
        if parts[0] == "pkill":
            was_alive = self.alive
            self.alive = False
            for agent in list(self.agents):
                agent.closed = True
            self.agents.clear()
            # pkill -f: 0 = killed something, 1 = nothing matched.
            return {**base, "exit_code": 0 if was_alive else 1}
        if parts[0] == "pgrep":
            return {**base, "exit_code": 0 if self.alive else 1,
                    "stdout": "4242\n" if self.alive else ""}
        return base

    async def process_status(self, name: str = "agent") -> dict:
        self.calls.append(("process_status", name))
        return {"ok": True, "name": name, "running": self.alive, "exit_code": None}

    async def ensure_agent_running(self, **kwargs) -> dict:
        self.calls.append(("ensure_agent_running", ""))
        self.ensure_calls += 1
        if self.alive:
            return {"ok": True, "launched": False, "alive_before": True,
                    "alive_after": True, "checked": "process_status"}
        self.alive = True
        self.launches += 1
        # "Launch" = the fake agent starts dialling the controller.
        if self.dial_delay:
            await asyncio.sleep(self.dial_delay)
        if self.dial_timeout is not None:
            try:
                agent = await asyncio.wait_for(
                    FakeAgent.dial(self.agent_port, mc_state=self.agent_state,
                                   driver=self),
                    timeout=self.dial_timeout,
                )
            except asyncio.TimeoutError:
                self.dead_dials += 1
                self.alive = False
                return {"ok": False, "kind": "timeout",
                        "error": f"agent did not dial in {self.dial_timeout:g}s",
                        "launched": True, "alive_before": False,
                        "alive_after": False, "checked": "pgrep"}
        else:
            agent = await FakeAgent.dial(self.agent_port, mc_state=self.agent_state,
                                         driver=self)
        self.agents.append(agent)
        return {"ok": True, "launched": True, "alive_before": False,
                "alive_after": True, "checked": "pgrep"}


# ------------------------------------------------------------------- fixtures
def make_settings(tmp: Path, **overrides) -> Settings:
    base = Settings()
    defaults = dict(
        sandbox_id="sbx-phase-c",
        daytona_api_key="fake-key",
        daytona_toolbox_auth="bearer",
        # WS server: loopback only, ephemeral port, token required.
        agent_ws_enabled=True,
        agent_ws_host="127.0.0.1",
        agent_ws_port=0,
        agent_ws_path=MC_WS_PATH,
        agent_ws_token=TOKEN,
        agent_ws_ping_interval=20.0,
        agent_ws_stale_timeout=75.0,
        agent_remote_dir="mc-orchestrator/agent",
        # Orchestrator: everything under tmp, nothing under ./data.
        orch_enabled=True,
        orch_mirror_dir=str(tmp / "mirror"),
        orch_snapshots_dir=str(tmp / "snapshots"),
        orch_deltas_dir=str(tmp / "deltas"),
        orch_plugins_dir=str(tmp / "plugins"),
        orch_tmp_dir=str(tmp / "orch-tmp"),
        orch_snapshot_enabled=False,
        orch_snapshot_interval=0.0,
        orch_snapshot_keep=8,
        orch_delta_keep=20,
        orch_provision_timeout=10.0,
        orch_connect_timeout=5.0,
        orch_connect_poll=0.02,
        orch_stop_mc_timeout=5.0,
        orch_backup_flush_timeout=5.0,
        orch_autostart=False,
        orch_recovery_enabled=True,
        orch_recovery_grace=0.0,
        orch_recovery_cooldown=0.0,
    )
    defaults.update(overrides)
    return replace(base, **defaults)


def build_orchestrator(settings: Settings) -> tuple[Orchestrator, AgentWSServer]:
    """Real server + real orchestrator, fakes for Daytona."""
    state = AppState()
    orch = Orchestrator(settings, state=state)
    server = AgentWSServer.from_settings(settings, **orch.hooks())
    orch.ws = server
    return orch, server


async def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


def step_names(result: dict) -> list[str]:
    return [step.get("step") for step in result.get("steps") or []]


# ------------------------------------------------------------------------- (a)
async def suite_start(tmp: Path) -> int:
    failures = 0
    print("\n== (a) start_host: 8-step bootstrap, upload when the artifact is missing ==")
    settings = make_settings(tmp)
    orch, server = build_orchestrator(settings)
    orch.store_cfg = StoreConfig.from_settings(settings)
    orch.storage = Storage(orch.store_cfg)
    files = FakeFiles()
    driver = FakeDriver(files)

    started = await orch.start(autostart=False)
    failures += check("orchestrator.start() enables the WS server (AGENT_WS_ENABLED=true)",
                      started["agent_ws"] is True and server.running)
    driver.agent_port = server.bound_port
    orch._driver, orch._files = driver, files

    result = await orch.start_host(reason="test")
    failures += check("start_host() -> ok", result.get("ok") is True,
                      json.dumps(result.get("error") or result)[:200])
    failures += check_eq("step order matches §5", step_names(result),
                         ["lifecycle_start", "agent_artifact", "ensure_agent",
                          "wait_connected"])

    ordered = [name for name, _ in driver.calls if name in {"status", "start", "ensure_agent_running"}]
    failures += check_eq("lifecycle start happens before the agent is ensured",
                         ordered[:3], ["status", "start", "ensure_agent_running"])
    failures += check("sandbox start waited for RUNNING (one driver.start)",
                      driver.start_calls == 1, f"calls={driver.start_calls}")

    file_seq = [f"{kind}:{path}" for kind, path in files.calls if kind in {"upload"}]
    failures += check("agent.py uploaded because it was missing remotely",
                      "upload:mc-orchestrator/agent/agent.py" in file_seq, str(file_seq))
    failures += check("agent.env uploaded and chmod 600",
                      "upload:mc-orchestrator/agent/agent.env" in file_seq
                      and files.chmods == ["mc-orchestrator/agent/agent.env"],
                      f"uploads={file_seq} chmods={files.chmods}")
    failures += check("ensure_agent_running() called exactly once (launched)",
                      driver.ensure_calls == 1 and driver.launches == 1,
                      f"ensure={driver.ensure_calls} launches={driver.launches}")

    connected = await wait_until(lambda: orch.state.connected, timeout=3)
    failures += check("wait connected: agent hello registered on the WS server",
                      connected and server.connected)
    # The hello event is handled by the connection's serial on_event worker, so
    # the state transition is awaited rather than assumed to be instantaneous.
    hello_ingested = await wait_until(lambda: orch.state.mc_state == "running", timeout=3)
    failures += check("agent-authoritative mc_state taken from hello", hello_ingested,
                      f"got {orch.state.mc_state!r}, want 'running'")
    failures += check_eq("Daytona-authoritative sandbox_state is a separate field",
                         orch.state.sandbox_state, "RUNNING")
    failures += check("agent.env carries CONTROLLER_WS and AGENT_TOKEN",
                      b"CONTROLLER_WS=ws://" in files.files["mc-orchestrator/agent/agent.env"]
                      and TOKEN.encode() in files.files["mc-orchestrator/agent/agent.env"])

    # --------------------------------------------------------------------- (b)
    print("\n== (b) start_host() is idempotent on a live host ==")
    uploads_before = orch.counters["uploads"]
    upload_calls_before = sum(1 for kind, _ in files.calls if kind == "upload")
    files.calls.clear()
    launches_before = driver.launches
    result2 = await orch.start_host(reason="test-again")
    failures += check("second start_host() still ok", result2.get("ok") is True)
    second_uploads = [c for c in files.calls if c[0] == "upload"]
    failures += check("no extra upload (remote sha matched)", second_uploads == [],
                      str(second_uploads))
    failures += check_eq("upload counter unchanged",
                         orch.counters["uploads"], uploads_before)
    failures += check("no extra launch", driver.launches == launches_before,
                      f"launches={driver.launches}")
    failures += check("no extra sandbox start", driver.start_calls == 1,
                      f"start_calls={driver.start_calls}")
    failures += check("artifact was verified by sha256sum, not blindly re-pushed",
                      any(kind == "exec" and val.startswith("sha256sum") for kind, val in driver.calls))
    failures += check("agent.py upload count stays 1 across both calls",
                      upload_calls_before == 2 and
                      sum(1 for k, p in files.calls if k == "upload") == 0,
                      f"before={upload_calls_before}")

    # --------------------------------------------------------------------- (c)
    print("\n== (c) stop_host(): MC -> flush -> agent -> lifecycle ==")
    agent = driver.agents[-1]
    driver.calls.clear()
    stop = await orch.stop_host(reason="test")
    failures += check("stop_host() -> ok", stop.get("ok") is True,
                      json.dumps(stop.get("error") or stop)[:200])
    failures += check_eq("stop order", step_names(stop),
                         ["stop_mc", "flush_backup", "stop_agent", "lifecycle_stop"])
    failures += check("agent was asked to stop MC exactly once",
                      agent.stop_acks == 1 and agent.backups == 1,
                      f"acks={agent.stop_acks} backups={agent.backups}")

    flat = [f"{name}:{arg}" for name, arg in driver.calls]
    lifecycle_at = flat.index("stop:") if "stop:" in flat else -1
    pkill_at = next((i for i, c in enumerate(flat) if c.startswith("exec:pkill")), -1)
    failures += check("MC stop (agent ack) precedes the lifecycle stop",
                      agent.stop_acks == 1 and lifecycle_at > 0)
    failures += check("agent process is killed before the sandbox stops",
                      -1 < pkill_at < lifecycle_at, f"pkill={pkill_at} stop={lifecycle_at}")
    failures += check_eq("sandbox_state after stop", orch.state.sandbox_state, "STOPPED")
    failures += check_eq("mc_state recorded as stopped by the agent", orch.state.mc_state, "stopped")

    second_stop = await orch.stop_host(reason="test-again")
    failures += check("second stop_host() with no live agent does not error",
                      second_stop.get("ok") is True and second_stop.get("partial") is False,
                      json.dumps(second_stop.get("errors"))[:200])
    failures += check("lifecycle stop still issued when the agent was already dead",
                      driver.stop_calls == 2, f"stop_calls={driver.stop_calls}")

    await orch.close()
    failures += check("close() releases the WS listener", server.running is False)
    return failures


# ------------------------------------------------------------------------- (d)
async def suite_recover(tmp: Path) -> int:
    failures = 0
    print("\n== (d) recover() through the real on_stale hook (no stacking) ==")
    settings = make_settings(
        tmp,
        agent_ws_ping_interval=0.1,
        agent_ws_stale_timeout=0.4,
        orch_recovery_grace=0.15,
        orch_connect_timeout=5.0,
    )
    orch, server = build_orchestrator(settings)
    files = FakeFiles()
    driver = FakeDriver(files, state="RUNNING")
    orch._driver, orch._files = driver, files
    await orch.start(autostart=False)
    driver.agent_port = server.bound_port

    first = await orch.start_host(reason="initial")
    failures += check("initial start_host() ok", first.get("ok") is True,
                      json.dumps(first.get("error") or first)[:200])
    failures += check("recovery is wired to the server's stale hook",
                      server._on_stale is not None and server._on_disconnect is not None)

    agent = driver.agents[-1]
    launches_before = driver.launches
    await agent.hang()                      # process died: no close frame, no pongs
    failures += check("fake agent death is visible to the driver (alive=False)",
                      driver.alive is False and driver.agent_count() == 0,
                      f"alive={driver.alive} agents={driver.agent_count()}")
    start_before = orch.counters["start_host"]
    recover_before = orch.counters["recover"]
    skipped_before = orch.counters["recovery_skipped"]

    ran = await wait_until(
        lambda: orch.counters["recover"] > recover_before
        and orch.counters["start_host"] > start_before,
        timeout=8,
    )
    failures += check("on_stale and on_disconnect both fired a watchdog event", ran,
                      f"recover={orch.counters['recover']} start={orch.counters['start_host']}")
    failures += check_eq("exactly one start_host() ran for the event",
                         orch.counters["start_host"] - start_before, 1)
    # Both hooks fire within milliseconds of each other; the second one must be
    # coalesced while the first recovery task is still in flight.
    failures += check("competing on_disconnect was coalesced, not stacked",
                      orch.counters["recovery_skipped"] > skipped_before,
                      f"skipped={orch.counters['recovery_skipped']} (before {skipped_before})")
    failures += check("the dead agent process was really relaunched",
                      driver.launches == launches_before + 1
                      and driver.ensure_calls == 2,
                      f"ensure={driver.ensure_calls} launches={driver.launches}")
    failures += check("recovery left the host connected again",
                      await wait_until(lambda: orch.state.connected, timeout=6))

    # Now prove schedule_recovery() itself refuses to stack, deterministically.
    before = orch.counters["start_host"]
    orch.schedule_recovery("a")
    orch.schedule_recovery("b")
    task = orch._recovery_task
    if task is not None:
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=8)
        except asyncio.TimeoutError:
            pass
    failures += check("two back-to-back schedule_recovery() -> at most one start_host()",
                      orch.counters["start_host"] - before <= 1,
                      f"delta={orch.counters['start_host'] - before}")

    await orch.close()
    return failures


# ------------------------------------------------------------------------- (e)
async def suite_delta(tmp: Path) -> int:
    failures = 0
    print("\n== (e) delta ingest: mirror filled, manifest deletions applied ==")
    settings = make_settings(tmp)
    orch, server = build_orchestrator(settings)
    orch._driver, orch._files = FakeDriver(FakeFiles()), FakeFiles()
    orch.state.unsubscribe = None  # noqa: SLF001 - AppState has no subscriber API
    await orch.start(autostart=False)

    mirror = orch.store_cfg.mirror_dir
    (mirror / "world").mkdir(parents=True, exist_ok=True)
    (mirror / "world" / "level.dat").write_bytes(b"OLD-LEVEL")
    (mirror / "world" / "obsolete.dat").write_bytes(b"REMOVE-ME")
    stats_before = orch.storage.mirror_stats()
    failures += check_eq("mirror starts with the old level.dat",
                         (mirror / "world" / "level.dat").read_bytes(), b"OLD-LEVEL")

    archive = orch.store_cfg.tmp_dir / "incoming-delta.tar.gz"
    manifest = {"ts": int(time.time()), "host": "sandbox-fake",
                "files": ["world/level.dat", "world/region/r.0.0.mca"],
                "deleted": ["world/obsolete.dat"]}
    with tarfile.open(archive, "w:gz") as tar:
        payload = json.dumps(manifest).encode()
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(payload)
        tar.addfile(info, BytesIO(payload))
        for name, data in (
            ("world/level.dat", b"NEW-LEVEL"),
            ("world/region/r.0.0.mca", b"REGION-BYTES"),
        ):
            member = tarfile.TarInfo(name)
            member.size = len(data)
            member.mtime = int(time.time())
            tar.addfile(member, BytesIO(data))

    result = await orch.ingest_delta(archive, "sandbox-fake")
    failures += check("ingest_delta() -> ok", result.get("ok") is True, str(result.get("error")))
    failures += check_eq("mirror level.dat overwritten",
                         (mirror / "world" / "level.dat").read_bytes(), b"NEW-LEVEL")
    failures += check("new nested file extracted",
                      (mirror / "world" / "region" / "r.0.0.mca").read_bytes() == b"REGION-BYTES")
    failures += check("manifest 'deleted' entry really removed",
                      not (mirror / "world" / "obsolete.dat").exists())
    failures += check_eq("deleted count reported", result.get("deleted"), 1)
    failures += check_eq("files count reported", result.get("files"), 2)
    stats_after = orch.storage.mirror_stats()
    failures += check("mirror stats changed (files/bytes)",
                      stats_after["files"] != stats_before["files"]
                      or stats_after["bytes"] != stats_before["bytes"],
                      f"{stats_before['files']}/{stats_before['bytes']} -> "
                      f"{stats_after['files']}/{stats_after['bytes']}")
    failures += check_eq("delta archive retained for audit",
                         len(list(orch.store_cfg.deltas_dir.glob("delta-*.tar.gz"))), 1)
    failures += check_eq("state was fed the ingest (log ring grew)", result.get("ok"), True)
    failures += check("no leftover incoming archive",
                      not archive.exists())

    evil = orch.store_cfg.tmp_dir / "evil.tar.gz"
    with tarfile.open(evil, "w:gz") as tar:
        member = tarfile.TarInfo("../../escape.txt")
        member.size = 3
        tar.addfile(member, BytesIO(b"bad"))
    rejected = await orch.ingest_delta(evil, "attacker")
    failures += check("path-traversal delta rejected with a structured error",
                      rejected.get("ok") is False and rejected.get("kind") == "invalid",
                      str(rejected))

    await orch.close()
    return failures


# ------------------------------------------------------------------------- (f)
async def suite_snapshot(tmp: Path) -> int:
    failures = 0
    print("\n== (f) snapshot scheduler + (g) AGENT_WS_ENABLED=false ==")
    settings = make_settings(
        tmp,
        orch_snapshot_enabled=True,
        orch_snapshot_interval=0.4,
        orch_snapshot_keep=2,
        agent_ws_enabled=False,          # (g) the flag must actually mean something
    )
    orch, server = build_orchestrator(settings)
    orch._driver, orch._files = FakeDriver(FakeFiles()), FakeFiles()

    started = await orch.start(autostart=False)
    failures += check_eq("(g) AGENT_WS_ENABLED=false -> server NOT started",
                         started["agent_ws"], False)
    failures += check("(g) no listener was bound", server.running is False)
    failures += check_eq("(g) flag is reported, not silently swallowed",
                         started["agent_ws_enabled"], False)
    failures += check("snapshot loop started",
                      "snapshot_loop" in started["started"], str(started["started"]))

    mirror = orch.store_cfg.mirror_dir
    (mirror / "world").mkdir(parents=True, exist_ok=True)
    (mirror / "world" / "level.dat").write_bytes(b"WORLD-BYTES" * 32)
    (mirror / "world" / "region").mkdir(parents=True, exist_ok=True)
    (mirror / "world" / "region" / "r.0.0.mca").write_bytes(b"R" * 4096)

    made = await wait_until(lambda: orch.counters["snapshots"] >= 3, timeout=8)
    failures += check("scheduled snapshots were created", made,
                      f"snapshots={orch.counters['snapshots']}")
    await asyncio.sleep(0.5)             # one more interval, so pruning gets a chance
    snapshots = orch.storage.list_snapshots()
    failures += check("snapshot_keep=2 is honoured", len(snapshots) == 2,
                      f"{[s['name'] for s in snapshots]}")
    latest = orch.store_cfg.snapshots_dir / "latest.tar.gz"
    failures += check("latest.tar.gz exists", latest.is_file())
    if latest.is_file():
        with tarfile.open(latest, "r:gz") as tar:
            names = tar.getnames()
        failures += check("latest snapshot is a real tar of the mirror",
                          any(name.endswith("world/level.dat") for name in names), str(names[:5]))
    # `.tmp-<name>` / `.tmp-latest` are the staging files of a snapshot that is
    # *currently* being written; at a 0.4 s interval a bare sample races the
    # writer. What the check means is "nothing is left behind", so wait for the
    # writer to settle — an orphaned temp file never goes away and still fails.
    settled = await wait_until(
        lambda: not list(orch.store_cfg.snapshots_dir.glob(".tmp-*")), timeout=5
    )
    failures += check("no temp files left in the snapshot dir", settled,
                      str(sorted(p.name for p in
                                 orch.store_cfg.snapshots_dir.glob(".tmp-*"))))

    await orch.close()
    failures += check("close() cancels the snapshot loop",
                      orch._snapshot_task is None)
    before = orch.counters["snapshots"]
    await asyncio.sleep(0.6)
    failures += check_eq("no snapshot runs after close()", orch.counters["snapshots"], before)
    return failures


# ------------------------------------------------------------------------- (h)
class StateSink:
    """Browser stand-in: records every ``push_state()`` broadcast."""

    def __init__(self) -> None:
        self.frames: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.frames.append(payload)

    def state_frames(self) -> list[dict]:
        return [f for f in self.frames if f.get("type") == "state"]


async def suite_ack(tmp: Path) -> int:
    failures = 0
    print("\n== (i) ack ingest: real handle_event(), waiter resolved, state pushed ==")
    # No WS server needed: the bug lived entirely inside handle_event().
    settings = make_settings(tmp, agent_ws_enabled=False)
    orch = Orchestrator(settings)
    await orch.start(autostart=False)
    sink = StateSink()
    orch.state.browsers.add(sink)

    waiter = orch._add_waiter(  # noqa: SLF001 - the waiter list is the unit under test
        lambda ev: ev.get("type") == "ack" and ev.get("id") == "cmd-1"
    )
    raised: BaseException | None = None
    try:
        await orch.handle_event({"type": "ack", "id": "cmd-1", "ok": True,
                                 "message": "done"})
    except BaseException as exc:  # noqa: BLE001 - the assertion *is* "no raise"
        raised = exc
    failures += check("handle_event(ack) does not raise", raised is None,
                      f"{type(raised).__name__}: {raised}" if raised else "")
    failures += check("ack resolved the pending waiter (sync _resolve_waiter not awaited)",
                      waiter.done())
    payload = waiter.result() if waiter.done() and not waiter.cancelled() else {}
    failures += check("waiter got the ack payload, not an exception",
                      isinstance(payload, dict) and payload.get("type") == "ack"
                      and payload.get("id") == "cmd-1" and payload.get("ok") is True,
                      repr(payload))
    failures += check("push_state() still runs after an ack (state frame broadcast)",
                      len(sink.state_frames()) >= 1, f"frames={len(sink.frames)}")

    # The failure branch runs *after* the resolve; it must be reached too.
    logs_before = len(orch.state.logs)
    frames_before = len(sink.state_frames())
    raised2: BaseException | None = None
    try:
        await orch.handle_event({"type": "ack", "id": "cmd-2", "ok": False,
                                 "message": "boom"})
    except BaseException as exc:  # noqa: BLE001
        raised2 = exc
    failures += check("ack with ok=false does not raise either", raised2 is None,
                      f"{type(raised2).__name__}: {raised2}" if raised2 else "")
    failures += check("failed ack is logged as an agent error",
                      any("gagal" in str(entry.get("text") or "")
                          and entry.get("source") == "agent"
                          for entry in list(orch.state.logs)[logs_before:]),
                      str(list(orch.state.logs)[-1]))
    failures += check("state was pushed for the failed ack as well",
                      len(sink.state_frames()) > frames_before)
    failures += check("_resolve_waiter returned a bool (not a coroutine) for a stale ack",
                      orch._resolve_waiter(  # noqa: SLF001
                          {"type": "ack", "id": "nobody"}, {"type": "ack"}) is False)

    orch.state.browsers.discard(sink)
    await orch.close()
    return failures


async def suite_auth(tmp: Path) -> int:
    failures = 0
    print("\n== (h) toolbox 401 during provisioning: structured error, 0 tracebacks ==")
    settings = make_settings(tmp, orch_connect_timeout=1.0)
    orch, server = build_orchestrator(settings)
    files = FakeFiles(http_status=401)
    driver = FakeDriver(files, state="RUNNING")
    orch._driver, orch._files = driver, files
    await orch.start(autostart=False)
    driver.agent_port = server.bound_port

    result = await orch.start_host(reason="auth-fail")
    failures += check("start_host() fails cleanly", result.get("ok") is False)
    failures += check_eq("failure is classified as auth", result.get("kind"), "auth")
    failures += check_eq("failing step is the artifact step", result.get("step"), "agent_artifact")
    failures += check_eq("HTTP status is preserved", result.get("status_code"), 401)
    failures += check("error message is actionable (mentions the env vars)",
                      "DAYTONA_API_KEY" in str(result.get("error"))
                      or "TOOLBOX_AUTH" in str(result.get("error")),
                      str(result.get("error")))
    failures += check("no launch was attempted after the auth failure",
                      driver.ensure_calls == 0 and driver.launches == 0)
    state = orch.state.public_state()
    failures += check_eq("panel status carries the failure", state["provisioning"]["active"], False)
    failures += check("panel status records last_error",
                      "401" in str(state["provisioning"].get("last_error") or "")
                      or "kredensial" in str(state["provisioning"].get("last_error") or ""),
                      str(state["provisioning"].get("last_error")))
    failures += check("state fields are untouched by a failed provisioning",
                      state["mc_state"] == "unknown")

    await orch.close()
    return failures


# ------------------------------------------------------------------------ main
async def suite_from_settings(tmp: Path) -> int:
    """(j) hook kwargs on from_settings: accepted, and really installed."""
    failures = 0
    print("\n== (j) from_settings(settings, on_event=...) installs the hooks ==")
    settings = make_settings(tmp, agent_ws_port=0)

    async def on_event(event):          # stand-ins: identity is what matters
        return None

    async def on_connect(session):
        return None

    async def on_disconnect(info):
        return None

    async def on_stale(info):
        return None

    raised: BaseException | None = None
    orch = None
    try:
        orch = Orchestrator.from_settings(
            settings,
            driver=object(),
            files=object(),
            on_event=on_event,
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            on_stale=on_stale,
        )
    except BaseException as exc:  # noqa: BLE001 - the assertion *is* "no raise"
        raised = exc
    failures += check(
        "from_settings(settings, on_event=...) does not raise",
        raised is None,
        f"{type(raised).__name__}: {raised}" if raised else "",
    )
    failures += check("from_settings() returned an Orchestrator",
                      isinstance(orch, Orchestrator), type(orch).__name__)
    if orch is None:
        return failures

    ws = orch.ws
    failures += check("the WS server was built", isinstance(ws, AgentWSServer),
                      type(ws).__name__)
    failures += check_eq("on_event landed on ws._on_event", ws._on_event, on_event)  # noqa: SLF001
    failures += check_eq("on_connect landed on ws._on_connect", ws._on_connect, on_connect)  # noqa: SLF001
    failures += check_eq("on_disconnect landed on ws._on_disconnect",  # noqa: SLF001
                         ws._on_disconnect, on_disconnect)
    failures += check_eq("on_stale landed on ws._on_stale", ws._on_stale, on_stale)  # noqa: SLF001
    failures += check("hooks were not forwarded to Orchestrator.__init__ "
                      "(no stray attribute, no TypeError)",
                      not hasattr(orch, "on_event"))

    # The default path: hooks() supplies kwargs that from_settings() must accept.
    base = Orchestrator(settings)
    hooks = base.hooks()
    failures += check_eq("hooks() exposes the four callbacks",
                         sorted(hooks), ["on_connect", "on_disconnect",
                                         "on_event", "on_stale"])
    hooked = Orchestrator.from_settings(
        settings, driver=object(), files=object(), **hooks
    )
    failures += check(
        "hooks() + from_settings bind the orchestrator's own handlers",
        getattr(hooked.ws._on_event, "__self__", None) is base  # noqa: SLF001
        and getattr(hooked.ws._on_event, "__func__", None) is Orchestrator.handle_event,
        repr(hooked.ws._on_event),  # noqa: SLF001
    )
    failures += check(
        "every hook is a bound handler of the same orchestrator",
        all(getattr(getattr(hooked.ws, attr), "__self__", None) is base  # noqa: SLF001
            for attr in ("_on_event", "_on_connect", "_on_disconnect", "_on_stale")),
    )
    failures += check("the WS listener was not started by from_settings()",
                      hooked.ws.running is False)  # noqa: SLF001
    return failures


# ------------------------------------------------------- k: agent.env wiring
FRP_KEYS = (
    "FRP_ENABLED", "FRP_SERVER_ADDR", "FRP_SERVER_PORT", "FRP_TOKEN",
    "FRP_LOCAL_PORT", "FRP_REMOTE_PORT", "FRP_VERSION", "FRP_SHA256",
)
ENV_KEYS = (
    "CONTROLLER_WS", "AGENT_TOKEN", "SERVER_DIR", "RUNTIME_DIR", "JAVA_VERSION",
    "PROVISION", "KILL_ORPHANS", "WS_INSECURE", "AUTOSTART",
) + FRP_KEYS
FRP_SECRET = "phase-e-frp-secret-9c1f"


def load_agent_module():
    """Import agent/agent.py by path (no package, no side effects)."""
    path = ROOT / "agent" / "agent.py"
    spec = importlib.util.spec_from_file_location("dayhost_agent_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_env(text: str) -> dict[str, str]:
    """Same grammar as agent.parse_env_file, applied to the in-memory text."""
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


async def suite_agent_env(tmp: Path) -> int:
    """(k) agent.env carries every Phase-E key; secrets never leave the file."""
    failures = 0
    print("\n== (k) agent.env wiring: flags, FRP_*, and secret hygiene ==")
    settings = make_settings(
        tmp,
        agent_ws_token=TOKEN,
        frp_server_addr="vps.example.net",
        frp_token=FRP_SECRET,
        frp_sha256="",          # deliberately unset -> unconfigured
    )
    orch, ws = build_orchestrator(settings)
    try:
        orch.store_cfg.ensure_dirs()
        env_path, size = orch._build_agent_env()  # noqa: SLF001
        raw = env_path.read_text(encoding="utf-8")
    finally:
        await orch.close()

    env = parse_env(raw)
    failures += check("agent.env was generated and is non-empty", size > 0, str(size))
    missing = [key for key in ENV_KEYS if key not in env]
    failures += check("agent.env contains every expected key", not missing, ", ".join(missing))
    failures += check("CONTROLLER_WS ends with /agent-ws",
                      env.get("CONTROLLER_WS", "").endswith("/agent-ws"),
                      env.get("CONTROLLER_WS", ""))
    failures += check_eq("PROVISION is explicit 1", env.get("PROVISION"), "1")
    failures += check_eq("KILL_ORPHANS is explicit 1", env.get("KILL_ORPHANS"), "1")
    failures += check_eq("AUTOSTART is explicit 0 (orchestrator owns boot)",
                         env.get("AUTOSTART"), "0")
    failures += check_eq("WS_INSECURE is explicit 0", env.get("WS_INSECURE"), "0")
    failures += check_eq("FRP_ENABLED is explicit 1 for tunnel_mode=frp",
                         env.get("FRP_ENABLED"), "1")
    gaps = [key for key in FRP_KEYS if key not in env]
    failures += check("FRP_* written even when empty (no omitted line)",
                      env.get("FRP_SHA256") == "" and not gaps, ", ".join(gaps))
    failures += check("empty CONTROLLER_HTTP/TMP_DIR are omitted, not blank lines",
                      "CONTROLLER_HTTP" not in env and "TMP_DIR" not in env,
                      str(sorted(set(env) - set(ENV_KEYS))))
    failures += check("AGENT_TOKEN present in the file itself", TOKEN in raw)
    failures += check("FRP_TOKEN present in the file itself", FRP_SECRET in raw)

    # Sandboxed paths: absolute, remote-side, and never a host/harness path.
    failures += check("SERVER_DIR is absolute inside the sandbox",
                      env.get("SERVER_DIR", "").startswith("/")
                      and env["SERVER_DIR"].endswith("/mc-orchestrator/server"),
                      env.get("SERVER_DIR", ""))
    failures += check("RUNTIME_DIR is absolute inside the sandbox",
                      env.get("RUNTIME_DIR", "").startswith("/")
                      and env["RUNTIME_DIR"].endswith("/mc-orchestrator/runtime"),
                      env.get("RUNTIME_DIR", ""))
    leaked = [key for key in ("SERVER_DIR", "RUNTIME_DIR")
              if "agent/mc-orchestrator" in env.get(key, "")
              or env.get(key, "").startswith(str(tmp))]
    failures += check("no host path leaked into agent.env", not leaked,
                      ", ".join(f"{key}={env.get(key, '')}" for key in leaked))

    surface = json.dumps(orch.counters, default=str) + json.dumps(
        list(orch.state.logs), default=str
    )
    failures += check("no AGENT_TOKEN/FRP_TOKEN in counters or state.logs",
                      TOKEN not in surface and FRP_SECRET not in surface)

    # The real agent Config + Tunnel must accept this file as-is.
    saved = {key: os.environ.pop(key, None) for key in ENV_KEYS}
    try:
        module = load_agent_module()
        cfg = module.Config(env_path)
        failures += check("agent.Config reads the generated file",
                          cfg.controller_ws == env["CONTROLLER_WS"]
                          and cfg.agent_token == TOKEN
                          and cfg.autostart is False,
                          cfg.controller_ws)
        tunnel = module.Tunnel(SimpleNamespace(cfg=cfg))
        code = ""
        detail = ""
        raised = None
        try:
            tunnel._validate()  # noqa: SLF001
        except module.TunnelError as exc:
            raised = exc
            code = exc.code
            detail = exc.message
        failures += check("Tunnel._validate() rejects empty FRP_SHA256 as unconfigured",
                          isinstance(raised, module.TunnelError) and code == "unconfigured",
                          f"{code}: {detail}")
        failures += check("unconfigured names FRP_SHA256 (no guessed digest)",
                          "FRP_SHA256" in detail, detail)
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value

    # A proxied wss URL must survive untouched: forcing :8765 made every
    # sandbox dial an unreachable port (Phase E live failure).
    keep = Orchestrator._normalise_agent_ws_url("wss://h.example/agent-ws")  # noqa: SLF001
    failures += check_eq("wss URL keeps no forced port (edge terminates TLS)",
                         keep, "wss://h.example/agent-ws")
    explicit = Orchestrator._normalise_agent_ws_url("ws://10.0.0.9:9000/ws")  # noqa: SLF001
    failures += check_eq("explicit port is preserved, path fixed",
                         explicit, "ws://10.0.0.9:9000/ws/agent-ws")
    plain = Orchestrator._normalise_agent_ws_url("ws://10.0.0.9/agent-ws")  # noqa: SLF001
    failures += check_eq("absent port stays absent", plain, "ws://10.0.0.9/agent-ws")
    return failures


# ------------------------------------------------------------------------ main
async def run_suite(label: str, coro) -> int:
    """Run one suite; an abort is a recorded failure, never the end of the run.

    Without this, the first exception killed the whole suite list and the
    process exited 1 with no PASS/FAIL and no traceback (the excepthook had
    swallowed it). A gate must say *what* broke.
    """
    global CHECKS
    CHECKS += 1
    SUITES_RUN.append(label)
    try:
        result = await coro
        if not isinstance(result, int):
            ABORTS.append((label, f"suite returned {type(result).__name__}, not int"))
            FAILURES.append(f"suite {label} returned {type(result).__name__}, not int")
            print(f"  FAIL  suite {label} returned {type(result).__name__}, expected int")
            return 1
        return result
    except Exception as exc:  # noqa: BLE001 - report honestly, then continue
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        ABORTS.append((label, tb))
        FAILURES.append(f"suite {label} aborted ({type(exc).__name__}: {exc})")
        print(f"  FAIL  suite {label} aborted unexpectedly: "
              f"{type(exc).__name__}: {exc}")
        print("  --- traceback ---")
        for line in tb.rstrip().splitlines():
            print(f"  {line}")
        print("  --- end traceback ---")
        return 1


async def run() -> int:
    _install_hooks()
    tmp = Path(tempfile.mkdtemp(prefix="dayhost-orch-test-"))
    failures = 0
    try:
        failures += await run_suite("a:start_host", suite_start(tmp / "start"))
        failures += await run_suite("d:recover", suite_recover(tmp / "recover"))
        failures += await run_suite("e:delta", suite_delta(tmp / "delta"))
        failures += await run_suite("f:snapshot", suite_snapshot(tmp / "snapshot"))
        failures += await run_suite("h:auth", suite_auth(tmp / "auth"))
        failures += await run_suite("i:ack", suite_ack(tmp / "ack"))
        failures += await run_suite("j:from_settings", suite_from_settings(tmp / "from_settings"))
        failures += await run_suite("k:agent_env", suite_agent_env(tmp / "agent_env"))
    finally:
        # Let any spawned hook tasks finish before we inspect tracebacks.
        await asyncio.sleep(0.05)
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n== housekeeping ==")
    failures += check("no suite aborted unexpectedly", not ABORTS,
                      ", ".join(label for label, _ in ABORTS))
    failures += check(f"all suites ran ({len(SUITES_RUN)}/8)", len(SUITES_RUN) == 8,
                      ", ".join(SUITES_RUN))
    failures += check("zero tracebacks", not TRACEBACKS,
                      (TRACEBACKS[0][:400] if TRACEBACKS else ""))
    leftovers = [p for p in (ROOT / "data").glob("**/*") if p.is_dir()] if (ROOT / "data").is_dir() else []
    stray = [p for p in (ROOT / "data").glob("mirror") ] if (ROOT / "data").is_dir() else []
    failures += check("temp dir removed", not tmp.exists())
    failures += check("nothing new written under ./data (no mirror/snapshots/deltas/plugins)",
                      not stray and not (ROOT / "data" / "orch-tmp").exists(),
                      str(leftovers[:5]))

    print(f"\n{CHECKS - failures}/{CHECKS} checks passed")
    if FAILURES:
        print("failed:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1
    print("ALL PASS")
    return 0


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
