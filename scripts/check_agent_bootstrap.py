"""Agent bootstrap checks — `python scripts/check_agent_bootstrap.py`

Phase E gate for the **root** agent (`agent/agent.py`). Fully offline: no JDK
download (190 MB), no frpc download (14 MB), no DNS, no GitHub. Every network
call is replaced by an injected fake, so this harness is safe on a laptop.

What is proven (a–h, matching the Phase-E brief):

  a) ``java_version_ok``: 21 passes, 20 fails, 25 passes (25 >= 21);
  b) ``_find_java`` picks the portable jdk21 and ignores a stale jdk25;
  c) ``frpc.toml`` parses as TOML with serverAddr/serverPort/[[proxies]]/
     type=tcp/localPort/remotePort, is mode 600, and the frp token never
     appears in the agent log;
  d) ``FRP_SHA256`` empty -> zero downloads, state "unconfigured", no traceback;
  e) wrong SHA-256 -> download rejected, nothing extracted, structured error;
  f) ``ensure_runtime_ready`` True with a fake-complete runtime; False (no
     crash) for PROVISION=0 with no java at all;
  g) an frpc failure leaves PaperMC state and the jar untouched;
  h) a real handshake against the Phase-B server (loopback, ephemeral port)
     delivers the agent's ``hello`` into the server's session registry.

Exit 0 = all passed. Stdlib only; every file lives in one temp dir that is
removed before exit, and every spawned frpc is killed.
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import signal
import socket
import sys
import tarfile
import tempfile
import time
import tomllib
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AGENT_SRC = ROOT / "agent" / "agent.py"
sys.path.insert(0, str(ROOT))
# Never load the operator's .env: this harness must not see real credentials.
os.environ["DAYHOST_ENV_FILE"] = str(ROOT / ".nonexistent-harness.env")
os.environ.setdefault("LOG_LEVEL", "CRITICAL")

CHECKS = 0
FAILURES: list[str] = []
TMP = Path(tempfile.mkdtemp(prefix="check-agent-bootstrap-"))
SPAWNED: list[int] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    global CHECKS
    CHECKS += 1
    suffix = "" if ok or not detail else f"   [{detail}]"
    print(f"{'PASS' if ok else 'FAIL'}  {label}{suffix}")
    if not ok:
        FAILURES.append(label + (f" ({detail})" if detail else ""))
    return ok


def load_agent():
    spec = importlib.util.spec_from_file_location("agent_under_test", AGENT_SRC)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["agent_under_test"] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------- fake helpers
def fake_java(path: Path, major: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/sh\n"
        f'echo \'openjdk version "{major}.0.1" 2026-01-20 LTS\' >&2\n'
        'echo \'OpenJDK Runtime Environment (build fake)\' >&2\n',
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def fake_frpc_tar(pattern: str) -> bytes:
    """Build a tiny frp-style tarball whose frpc is a long-lived shell script."""
    buf = io.BytesIO()
    script = b"#!/bin/sh\nexec sleep 30\n"
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo("frp_0.71.1_linux_amd64/frpc")
        info.size = len(script)
        info.mode = 0o755
        info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(script))
    return buf.getvalue()


def write_env(root: Path, extra: dict[str, str]) -> Path:
    """Write an agent.env (600) with the minimum the Config validates."""
    base = {
        "CONTROLLER_WS": "ws://127.0.0.1:1/agent-ws",
        "CONTROLLER_HTTP": "http://127.0.0.1:1",
        "AGENT_TOKEN": "harness-token",
        "SERVER_DIR": str(root / "server"),
        "RUNTIME_DIR": str(root / "runtime"),
        "TMP_DIR": str(root / "tmp"),
        "BACKUP_INDEX": str(root / "backup-index.json"),
        "AUTOSTART": "0",
        "PROVISION": "1",
    }
    base.update(extra)
    path = root / "agent.env"
    path.write_text(
        "\n".join(f"{k}={v}" for k, v in base.items()) + "\n", encoding="utf-8"
    )
    path.chmod(0o600)
    return path


ENV_KEYS = [
    "CONTROLLER_WS", "CONTROLLER_HTTP", "AGENT_TOKEN", "SERVER_DIR",
    "RUNTIME_DIR", "TMP_DIR", "BACKUP_INDEX", "AGENT_PID_FILE", "AUTOSTART",
    "PROVISION", "KILL_ORPHANS", "WS_INSECURE", "JAVA_BIN", "JAVA_VERSION",
    "FRP_ENABLED", "FRP_SERVER_ADDR", "FRP_SERVER_PORT", "FRP_TOKEN",
    "FRP_LOCAL_PORT", "FRP_REMOTE_PORT", "FRP_VERSION", "FRP_SHA256",
    "FRP_ARCH", "FRP_LOG", "ARTIFACT_BASE_URL", "ARTIFACT_PATH_TEMPLATE",
]


def clear_env() -> None:
    for key in ENV_KEYS:
        os.environ.pop(key, None)


# ------------------------------------------------------------------- scenarios
def scenario_java(agent) -> None:
    print("\n== (a) java_version_ok: 21 pass, 20 fail, 25 pass ==")
    base = TMP / "java"
    j21 = fake_java(base / "j21" / "java", 21)
    j20 = fake_java(base / "j20" / "java", 20)
    j25 = fake_java(base / "j25" / "java", 25)
    check("21 accepted", agent.java_version_ok(str(j21)) is True)
    check("20 rejected", agent.java_version_ok(str(j20)) is False)
    check("25 accepted (25 >= 21)", agent.java_version_ok(str(j25)) is True)
    check("missing binary rejected", agent.java_version_ok(str(base / "nope")) is False)
    check(
        "ADOPTIUM_API pins 21",
        "/latest/{version}/ga/" in agent.ADOPTIUM_API
        and agent.ADOPTIUM_API.format(version=21, arch="x64").endswith("/eclipse"),
    )


def scenario_find_java(agent) -> None:
    print("\n== (b) _find_java prefers portable jdk21 over a stale jdk25 ==")
    root = TMP / "findjava"
    root.mkdir(parents=True, exist_ok=True)
    old25 = fake_java(root / "runtime" / "jdk25" / "bin" / "java", 25)
    new21 = fake_java(root / "runtime" / "jdk21" / "bin" / "java", 21)
    cfg = agent.Config(write_env(root, {"RUNTIME_DIR": str(root / "runtime")}))
    agent_obj = agent.Agent(cfg)
    found = agent_obj._find_java()
    check("returns the jdk21 portable", found == str(new21), found)
    check("does not return jdk25", found != str(old25), found)
    check("cfg.java_version default is 21", cfg.java_version == 21, str(cfg.java_version))

    # A stale jdk25 alone must NOT satisfy the runtime.
    shutil.rmtree(root / "runtime" / "jdk21")
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(root / "empty-path")
    try:
        cfg2 = agent.Config(write_env(root, {"RUNTIME_DIR": str(root / "runtime")}))
        found2 = agent.Agent(cfg2)._find_java()
    finally:
        os.environ["PATH"] = old_path
    check("jdk25 alone is not accepted", found2 == "", found2)


def scenario_toml_and_token(agent) -> str:
    print("\n== (c) frpc.toml valid TOML, mode 600, token absent from logs ==")
    root = TMP / "toml"
    root.mkdir(parents=True, exist_ok=True)
    payload = fake_frpc_tar("linux_amd64")
    digest = hashlib.sha256(payload).hexdigest()
    token = "frp-tok-SHOULD-NOT-LEAK-8fa1"
    env = write_env(root, {
        "RUNTIME_DIR": str(root / "runtime"),
        "FRP_SERVER_ADDR": "vps.example.com",
        "FRP_SERVER_PORT": "7000",
        "FRP_TOKEN": token,
        "FRP_LOCAL_PORT": "25565",
        "FRP_REMOTE_PORT": "25599",
        "FRP_VERSION": "0.71.1",
        "FRP_SHA256": digest,
        "FRP_ARCH": "amd64",
        "FRP_LOG": str(root / "runtime" / "frpc.log"),
    })
    cfg = agent.Config(env)
    cfg.make_dirs()
    ag = agent.Agent(cfg)
    url_seen: list[str] = []

    def fake_download(url: str, target: Path, label: str) -> None:
        url_seen.append(url)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    ag.tunnel.downloader = fake_download
    captured = io.StringIO()
    started = False
    state: dict[str, object] = {}

    async def run_tunnel() -> None:
        nonlocal started
        started = await ag.tunnel.start()
        state["state"] = ag.tunnel.state
        state["detail"] = ag.tunnel.detail
        state["running"] = ag.tunnel.running
        state["pid"] = ag.tunnel.proc.pid if ag.tunnel.proc else None
        state["events"] = captured.getvalue()
        state["public"] = json.dumps(ag.tunnel.public_state())
        await ag.tunnel.stop()

    try:
        with contextlib.redirect_stdout(captured):
            asyncio.run(run_tunnel())
        check("tunnel.start() succeeded", started, str(state.get("detail")))
        check(
            "release URL is the pinned GitHub asset",
            url_seen
            == ["https://github.com/fatedier/frp/releases/download/v0.71.1/"
                "frp_0.71.1_linux_amd64.tar.gz"],
            str(url_seen),
        )
        conf = cfg.frp_config_path
        check("frpc.toml exists", conf.is_file(), str(conf))
        check("frpc.toml mode 600", (conf.stat().st_mode & 0o777) == 0o600,
              oct(conf.stat().st_mode & 0o777))
        data = tomllib.loads(conf.read_text(encoding="utf-8"))
        check("serverAddr", data.get("serverAddr") == "vps.example.com", str(data.get("serverAddr")))
        check("serverPort is int", data.get("serverPort") == 7000, repr(data.get("serverPort")))
        proxies = data.get("proxies") or []
        proxy = proxies[0] if proxies else {}
        check("[[proxies]] present", len(proxies) == 1, str(proxies))
        check("proxy type=tcp", proxy.get("type") == "tcp", str(proxy.get("type")))
        check("localPort", proxy.get("localPort") == 25565, repr(proxy.get("localPort")))
        check("remotePort", proxy.get("remotePort") == 25599, repr(proxy.get("remotePort")))
        check("auth token present in config", (data.get("auth") or {}).get("token") == token)
        pid = state.get("pid")
        if isinstance(pid, int):
            SPAWNED.append(pid)
        check("frpc was running", bool(state.get("running")) and state.get("state") == "running",
              str(state.get("state")))
        events = str(state.get("events")) + str(state.get("public"))
        check("token not in logs/state", token not in events)
        log_text = ""
        if cfg.frp_log_path.is_file():
            log_text = cfg.frp_log_path.read_text(errors="replace")
        check("token not in frpc log", token not in log_text)
        check("public_state has no token key",
              "token" not in str(state.get("public")).lower())
        check("frpc stopped cleanly", ag.tunnel.state == "stopped", ag.tunnel.state)
    finally:
        if SPAWNED:
            _reap(SPAWNED.pop())
    return token


def scenario_unconfigured(agent) -> None:
    print("\n== (d) FRP_SHA256 empty -> no download, state unconfigured ==")
    root = TMP / "unconfigured"
    root.mkdir(parents=True, exist_ok=True)
    calls: list[str] = []

    def boom(url: str, target: Path, label: str) -> None:
        calls.append(url)
        raise AssertionError("downloader must not be called when SHA-256 is empty")

    extracted: list[str] = []

    def spy_extract(archive: Path, dest: Path) -> None:
        extracted.append(str(archive))

    cfg = agent.Config(write_env(root, {
        "RUNTIME_DIR": str(root / "runtime"),
        "FRP_SERVER_ADDR": "vps.example.com",
        "FRP_SERVER_PORT": "7000",
        "FRP_TOKEN": "some-token",
        "FRP_SHA256": "",
    }))
    cfg.make_dirs()
    ag = agent.Agent(cfg)
    ag.tunnel.downloader = boom
    ag.tunnel.extractor = spy_extract
    out, err = io.StringIO(), io.StringIO()
    ok = None
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            ok = asyncio.run(ag.tunnel.start())
    except Exception:  # noqa: BLE001 - any raise here is a failure
        FAILURES.append("(d) tunnel.start raised")
        traceback.print_exc()
    check("start() returned False (no raise)", ok is False, repr(ok))
    check("zero downloads", calls == [], str(calls))
    check("zero extractions", extracted == [], str(extracted))
    check("state unconfigured", ag.tunnel.state == "unconfigured", ag.tunnel.state)
    check("detail names FRP_SHA256", "FRP_SHA256" in ag.tunnel.detail, ag.tunnel.detail)
    blob = out.getvalue() + err.getvalue()
    check("log is explicit", "unconfigured" in blob and "FRP_SHA256" in blob)
    check("no traceback in output", "Traceback (most recent call last)" not in blob)
    check("no binary written", not (cfg.frp_dir / "frpc").exists())


def scenario_bad_sha(agent) -> None:
    print("\n== (e) wrong SHA-256 -> download rejected, nothing extracted ==")
    root = TMP / "badsha"
    root.mkdir(parents=True, exist_ok=True)
    payload = fake_frpc_tar("linux_amd64")
    extracted: list[str] = []

    def fake_download(url: str, target: Path, label: str) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    def spy_extract(archive: Path, dest: Path) -> None:
        extracted.append(str(archive))

    cfg = agent.Config(write_env(root, {
        "RUNTIME_DIR": str(root / "runtime"),
        "FRP_SERVER_ADDR": "vps.example.com",
        "FRP_TOKEN": "some-token",
        "FRP_SHA256": "0" * 64,
        "FRP_ARCH": "amd64",
    }))
    cfg.make_dirs()
    ag = agent.Agent(cfg)
    ag.tunnel.downloader = fake_download
    ag.tunnel.extractor = spy_extract
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        ok = asyncio.run(ag.tunnel.start())
    check("start() returned False", ok is False, repr(ok))
    check("state checksum", ag.tunnel.state == "checksum", ag.tunnel.state)
    check("structured error code", ag.tunnel.code == "checksum"
          and "SHA-256" in ag.tunnel.detail, f"{ag.tunnel.code}: {ag.tunnel.detail}")
    check("extractor never called", extracted == [], str(extracted))
    check("no frpc binary", not (cfg.frp_dir / "frpc").exists())
    check("partial download cleaned up",
          not list(cfg.runtime_dir.glob("frp_*.tar.gz.part")),
          str(list(cfg.runtime_dir.glob("*.part"))))
    check("no traceback", "Traceback (most recent call last)" not in out.getvalue())


def scenario_runtime(agent) -> None:
    print("\n== (f) ensure_runtime_ready: fake-complete True, PROVISION=0 no java False ==")
    root = TMP / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    fake_java(root / "runtime" / "jdk21" / "bin" / "java", 21)
    cfg = agent.Config(write_env(root, {"RUNTIME_DIR": str(root / "runtime")}))
    cfg.make_dirs()
    with open(cfg.server_dir / "paper.jar", "wb") as handle:
        handle.truncate(1_100_000)          # sparse, > 1 MB, no real download
    ag = agent.Agent(cfg)

    def no_download(url: str, target: Path, label: str) -> None:
        raise AssertionError(f"unexpected download: {label}")

    ag._download_file = staticmethod(no_download)  # type: ignore[assignment]
    ok = asyncio.run(ag.ensure_runtime_ready())
    check("ensure_runtime_ready() -> True", ok is True, repr(ok))
    check("java_bin is the portable jdk21",
          ag.java_bin == str(root / "runtime" / "jdk21" / "bin" / "java"), ag.java_bin)
    check("eula.txt written", (cfg.server_dir / "eula.txt").is_file())
    check("paper.jar untouched", (cfg.server_dir / "paper.jar").stat().st_size == 1_100_000)

    root2 = TMP / "runtime-noprovision"
    root2.mkdir(parents=True, exist_ok=True)
    cfg2 = agent.Config(write_env(root2, {
        "RUNTIME_DIR": str(root2 / "runtime"),
        "PROVISION": "0",
    }))
    cfg2.make_dirs()
    empty_path = root2 / "no-java-here"
    empty_path.mkdir()
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(empty_path)
    try:
        ag2 = agent.Agent(cfg2)
        ok2 = asyncio.run(ag2.ensure_runtime_ready())
    finally:
        os.environ["PATH"] = old_path
    check("PROVISION=0 without java -> False", ok2 is False, repr(ok2))
    check("no crash, no java_bin", ag2.java_bin == "", repr(ag2.java_bin))


def scenario_frpc_isolation(agent) -> None:
    print("\n== (g) frpc failure never changes PaperMC state ==")
    root = TMP / "isolation"
    root.mkdir(parents=True, exist_ok=True)
    payload = fake_frpc_tar("linux_amd64")
    digest = hashlib.sha256(payload).hexdigest()
    cfg = agent.Config(write_env(root, {
        "RUNTIME_DIR": str(root / "runtime"),
        "FRP_SERVER_ADDR": "vps.example.com",
        "FRP_TOKEN": "some-token",
        "FRP_SHA256": digest,
        "FRP_ARCH": "amd64",
        "FRP_LOG": str(root / "runtime" / "frpc.log"),
    }))
    cfg.make_dirs()
    with open(cfg.server_dir / "paper.jar", "wb") as handle:
        handle.truncate(1_100_000)
    ag = agent.Agent(cfg)
    ag.mc.state = "running"
    before = (ag.mc.state, cfg.server_dir.joinpath("paper.jar").stat().st_size)

    # (g1) bootstrap failure path (bad hash) must not touch MC state.
    ag.tunnel.downloader = lambda url, target, label: target.write_bytes(payload)
    cfg.frp_sha256 = "0" * 64
    with contextlib.redirect_stdout(io.StringIO()):
        asyncio.run(ag.tunnel.start())
    check("tunnel failed", ag.tunnel.state == "checksum", ag.tunnel.state)
    check("mc state unchanged after failed bootstrap", ag.mc.state == before[0], ag.mc.state)
    check("paper.jar unchanged", cfg.server_dir.joinpath("paper.jar").stat().st_size == before[1])

    # (g2) frpc dying AFTER a good start must also leave the server alone.
    cfg.frp_sha256 = digest
    ag.tunnel.downloader = lambda url, target, label: target.write_bytes(payload)
    result: dict[str, object] = {}

    async def start_then_kill() -> None:
        ok = await ag.tunnel.start()
        result["ok"] = ok
        result["detail"] = ag.tunnel.detail
        pid = ag.tunnel.proc.pid if ag.tunnel.proc else 0
        result["pid"] = pid
        if not pid:
            return
        os.killpg(os.getpgid(pid), signal.SIGKILL)
        # The watch task lives on THIS loop; poll inside it.
        for _ in range(60):
            if ag.tunnel.state == "failed":
                break
            await asyncio.sleep(0.05)
        result["state"] = ag.tunnel.state

    with contextlib.redirect_stdout(io.StringIO()):
        asyncio.run(start_then_kill())
    pid = result.get("pid")
    check("tunnel restarted with good hash",
          bool(result.get("ok")) and bool(pid), str(result.get("detail")))
    check("frpc death detected", result.get("state") == "failed", str(result.get("state")))
    check("mc state still untouched", ag.mc.state == before[0], ag.mc.state)
    check("paper.jar still intact",
          cfg.server_dir.joinpath("paper.jar").stat().st_size == before[1])
    if isinstance(pid, int) and pid:
        _reap(pid)
    check("mc object never replaced by the tunnel",
          ag.tunnel is not ag.mc and ag.mc.state == "running")


async def _handshake(agent) -> None:
    import logging

    from app.services.agent_ws import AgentWSServer

    # ping_interval=0 disables the heartbeat; the server logs a warning for it.
    logging.getLogger("dayhost.agent_ws").setLevel(logging.ERROR)
    token = "phase-b-token-xyz"
    server = AgentWSServer(host="127.0.0.1", port=0, path="/agent-ws", token=token,
                           ping_interval=0)
    await server.start()
    port = server.bound_port
    root = TMP / "ws"
    root.mkdir(parents=True, exist_ok=True)
    cfg = agent.Config(write_env(root, {
        "CONTROLLER_WS": f"ws://127.0.0.1:{port}/agent-ws",
        "CONTROLLER_HTTP": f"http://127.0.0.1:{port}",
        "AGENT_TOKEN": token,
        "RUNTIME_DIR": str(root / "runtime"),
    }))
    cfg.make_dirs()
    ag = agent.Agent(cfg)
    conn = None
    try:
        conn = await agent.ws_connect(cfg.controller_ws, cfg.agent_token, False)
        check("handshake 101 + accept key", conn is not None)
        ag.ws, ag.connected = conn, True
        sent = await ag.send_json({"type": "hello", "version": "test", "hostname": "harness",
                                   "mc_state": "stopped"})
        check("hello frame sent (masked client frame)", sent is True)
        for _ in range(50):
            if server.session is not None and server.session.hello:
                break
            await asyncio.sleep(0.05)
        hello = server.session.hello if server.session else {}
        check("server received hello", hello.get("type") == "hello", str(hello)[:120])
        check("server recorded hostname", server.session.hostname == "harness")
        check("server recorded mc_state", server.session.mc_state == "stopped")
        await conn.close()
        conn = None
        await asyncio.sleep(0.2)
        check("server saw the disconnect", server.connected is False or server._conn is None,
              str(server.connected))
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()
        await server.stop()


def scenario_handshake(agent) -> None:
    print("\n== (h) real handshake against Phase-B server (loopback, ephemeral) ==")
    try:
        asyncio.run(_handshake(agent))
    except Exception:  # noqa: BLE001
        FAILURES.append("(h) handshake raised")
        traceback.print_exc()
        check("handshake completed", False)


# --------------------------------------------------------------------- cleanup
def _reap(pid: int) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(os.getpgid(pid), sig)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                return
        for _ in range(40):
            if not Path(f"/proc/{pid}").exists():
                return
            time.sleep(0.05)


def cleanup() -> int:
    leftovers = 0
    for pid in SPAWNED:
        _reap(pid)
    for pid in SPAWNED:
        if Path(f"/proc/{pid}").exists():
            leftovers += 1
    shutil.rmtree(TMP, ignore_errors=True)
    return leftovers


def main() -> int:
    global CHECKS
    clear_env()
    print(f"agent source : {AGENT_SRC}")
    agent = load_agent()
    try:
        scenario_java(agent)
        scenario_find_java(agent)
        scenario_toml_and_token(agent)
        scenario_unconfigured(agent)
        scenario_bad_sha(agent)
        scenario_runtime(agent)
        scenario_frpc_isolation(agent)
        scenario_handshake(agent)
    except Exception:  # noqa: BLE001 - report, never lose the summary
        traceback.print_exc()
        FAILURES.append("unhandled exception")
    leftovers = cleanup()
    print()
    if leftovers:
        print(f"FAIL  cleanup: {leftovers} frpc process(es) still alive")
        FAILURES.append("leftover frpc processes")
    else:
        print("PASS  cleanup: temp dir removed, zero frpc processes left")
        CHECKS += 1
    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print(f"ALL {CHECKS} AGENT-BOOTSTRAP CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
