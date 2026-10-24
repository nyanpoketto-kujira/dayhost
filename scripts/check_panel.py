"""Phase-D panel checks — `python scripts/check_panel.py`

Runs the **real** FastAPI app under a real uvicorn process (loopback, ephemeral
port) and drives it over plain HTTP, exactly like a browser would. Stdlib only.

What is proven (a-g, plus the prod /api/docs regression):

  a) ``/`` -> 303 to /login without a cookie, 401 for a JSON client, 200 after
     login.
  b) the dashboard renders every state with an **explicit, non-empty** label:
     ``RUNNING``, ``STOPPED``, ``unknown`` (agent-authoritative MC state) and
     ``disabled`` (orchestrator off).
  c) ``POST /host/start`` and ``POST /host/stop`` reach the orchestrator, return
     the HTMX partial and produce **zero** tracebacks.
  d) ``POST /backup`` calls ``snapshot_now``; an empty mirror is a **200** with
     "belum ada data", never a 500. The success path is checked too.
  e) with no orchestrator the mutating routes answer **503**, not 500, and no
     traceback is logged.
  f) ``GET /events`` streams anonymous ``data:`` frames while
     ``GET /events/state`` sends named ``event: state`` frames. Both streams are
     parsed with an EventSource-semantics dispatcher to prove that
     ``console.js`` (``source.onmessage``) receives log text and **never** the
     JSON state payload.
  g) ``AGENT_WS_ENABLED=false`` leaves the agent WS port genuinely free (and a
     positive control with the flag on really does bind it, so the check cannot
     pass vacuously).
  bonus) ``/api/docs`` and ``/api/openapi.json`` are 404 when ``DAYHOST_ENV=prod``.

How the fake orchestrator is injected
-------------------------------------
No production code is touched. ``app/main.py`` resolves ``build_orchestrator``
from its module globals inside ``lifespan``, so the launcher written into the
temp dir imports ``app.main``, rebinds that one name to a fake factory, and then
hands the already-built ``app`` object to ``uvicorn.run``. ``app.state.orch`` is
therefore the fake for the whole process, which is exactly the seam the
factory exists for. The alternative (poking ``app.state.orch`` after startup)
would need a second ASGI bootstrap and would not exercise the lifespan wiring
that Phase D added.

The fake reads its state from a JSON file on every call, so one process can be
driven through RUNNING -> STOPPED -> empty-mirror -> backup-ok without any
control endpoint in the product.

Every server, port, database and log file lives in one temp dir that is removed
before exit; the repo's ``./data`` is only inspected, never written.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
PASSWORD = "panel-test-password-123"

CHECKS = 0
FAILURES: list[str] = []
PORTS: list[int] = []


# --------------------------------------------------------------------- output
def check(label: str, ok: bool, detail: str = "") -> int:
    global CHECKS
    CHECKS += 1
    suffix = "" if ok or not detail else f"   [{detail}]"
    print(f"{'PASS' if ok else 'FAIL'}  {label}{suffix}")
    if not ok:
        FAILURES.append(label + (f" ({detail})" if detail else ""))
        return 1
    return 0


def section(title: str) -> None:
    print(f"\n== {title} ==")


# ------------------------------------------------------------------- plumbing
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """urllib follows 303 by default, which would hide the status we assert."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_is_free(port: int) -> bool:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def request(
    url: str,
    *,
    data: dict | None = None,
    cookie: str = "",
    accept: str = "application/json",
) -> tuple[int, dict, str]:
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body)
    if cookie:
        req.add_header("Cookie", cookie)
    if accept:
        req.add_header("Accept", accept)
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read().decode()


def sse(url: str, cookie: str, seconds: float = 2.5) -> tuple[int, str]:
    """Read an SSE stream for a bounded time and return the raw bytes.

    ``read1`` blocks until *something* arrives; the socket timeout turns an idle
    stream into a retry loop so the deadline is honoured without closing early.
    """
    parsed = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=10)
    path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    conn.request(
        "GET", path, headers={"Cookie": cookie, "Accept": "text/event-stream"}
    )
    resp = conn.getresponse()
    status = resp.status
    if conn.sock is not None:
        conn.sock.settimeout(0.4)
    buf = b""
    deadline = time.time() + seconds
    try:
        while time.time() < deadline:
            try:
                data = resp.read1(8192)
            except (TimeoutError, socket.timeout):
                continue
            except OSError:
                break
            if not data:
                break
            buf += data
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    return status, buf.decode("utf-8", "replace")


def event_source_dispatch(raw: str) -> dict:
    """Parse SSE frames and dispatch them the way EventSource does.

    Anonymous frames (no ``event:`` field) go to ``onmessage``. Named frames go
    to ``addEventListener(name)`` and are *invisible* to ``onmessage`` — this is
    the spec behaviour that keeps ``console.js`` free of JSON state.
    """
    anonymous: list[str] = []
    named: dict[str, list[str]] = {}
    for block in raw.replace("\r\n", "\n").split("\n\n"):
        event: str | None = None
        data: list[str] = []
        for line in block.split("\n"):
            if not line or line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                event = value
            elif field == "data":
                data.append(value)
        if not data:
            continue
        payload = "\n".join(data)
        if event is None:
            anonymous.append(payload)
        else:
            named.setdefault(event, []).append(payload)
    return {"anonymous": anonymous, "named": named}


def wait_ready(base: str, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = request(base + "/healthz")
            if status == 200:
                return True
        except Exception:  # noqa: BLE001 - not up yet
            pass
        time.sleep(0.25)
    return False


def make_password_hash(password: str) -> str:
    """pbkdf2_sha256$iter$salt$hash — same format as scripts/hash_password.py."""
    iterations = 240_000
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def base_env(tmp: Path, *, env: str = "dev") -> dict:
    env_map = dict(os.environ)
    env_map.update(
        {
            "DAYHOST_ENV": env,
            # Never read a real .env: point at a file that does not exist.
            "DAYHOST_ENV_FILE": str(tmp / "nonexistent.env"),
            "ADMIN_PASSWORD_HASH": make_password_hash(PASSWORD),
            "SESSION_SECRET": "panel-test-session-secret-" + "y" * 32,
            "DB_PATH": str(tmp / "dayhost.db"),
            "BACKUP_DIR": str(tmp / "backups"),
            "LOG_FILE": str(tmp / "dayhost.log"),
            "LOG_LEVEL": "INFO",
            "MC_HOST": "127.0.0.1",
            "MC_PORT": "1",              # nothing listening -> fails fast
            "MC_PING_TIMEOUT": "1",
            "TUNNEL_MODE": "ssh",        # skip the /proc frpc scan
            "RCON_PASSWORD": "",         # stub console
            "SYNC_ENABLED": "false",
            "ALLOWED_HOSTS": "127.0.0.1,localhost",
            "ORCH_DATA_DIR": str(tmp / "orch"),
            "ORCH_SNAPSHOT_ENABLED": "false",
            "ORCH_AUTOSTART": "false",
            "ORCH_RECOVERY_ENABLED": "false",
            "AGENT_WS_ENABLED": "false",
            "AGENT_TOKEN": "",
            "AGENT_WS_TOKEN": "",
            "DAYTONA_API_KEY": "",
            "SANDBOX_ID": "",
        }
    )
    return env_map


class Server:
    """One uvicorn process with its own port, log file and env."""

    def __init__(self, label: str, tmp: Path, env: dict, *, launcher: Path | None = None):
        self.label = label
        self.port = free_port()
        PORTS.append(self.port)
        self.log_path = tmp / f"{label}.log"
        self.log = self.log_path.open("wb")
        if launcher is None:
            cmd = [
                PY, "-m", "uvicorn", "app.main:app",
                "--host", "127.0.0.1", "--port", str(self.port),
                "--loop", "asyncio", "--http", "h11", "--log-level", "warning",
                "--no-access-log",
            ]
        else:
            cmd = [PY, str(launcher)]
        env = dict(env, PANEL_PORT=str(self.port))
        self.proc = subprocess.Popen(
            cmd, cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT
        )
        self.base = f"http://127.0.0.1:{self.port}"

    def ready(self, timeout: float = 30.0) -> bool:
        return wait_ready(self.base, timeout)

    def output(self) -> str:
        try:
            self.log.flush()
        except Exception:  # noqa: BLE001
            pass
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""

    def stop(self) -> None:
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        finally:
            try:
                self.log.close()
            except Exception:  # noqa: BLE001
                pass


def login(base: str, *, env: str = "dev") -> str:
    status, headers, _ = request(
        base + "/login",
        data={"username": "admin", "password": PASSWORD},
        accept="text/html",
    )
    cookie = (headers.get("set-cookie") or "").split(";", 1)[0]
    if status != 303 or "dayhost_session=" not in cookie:
        print(f"   (login -> {status} {cookie[:40]!r})")
    return cookie


def cell(html: str, element_id: str) -> str:
    match = re.search(
        r'id="%s"[^>]*>\s*([^<]*?)\s*<' % re.escape(element_id), html
    )
    return match.group(1).strip() if match else ""


def write_state(path: Path, **values) -> None:
    doc = {
        "sandbox_state": "UNKNOWN",
        "mc_state": "unknown",
        "status": "disconnected",
        "connected": False,
        "ws_running": False,
        "world": False,
        "snapshots": False,
    }
    doc.update(values)
    path.write_text(json.dumps(doc))


def calls(path: Path) -> list[str]:
    try:
        return [line for line in path.read_text().splitlines() if line]
    except OSError:
        return []


# ------------------------------------------------------ fake-orch launcher
LAUNCHER_SRC = r'''
"""Throwaway ASGI launcher: app.main with a fake orchestrator factory."""
import json
import os
import sys
from pathlib import Path

ROOT = os.environ["PANEL_ROOT"]
sys.path.insert(0, ROOT)

STATE_FILE = Path(os.environ["PANEL_STATE_FILE"])
CALLS_FILE = Path(os.environ["PANEL_CALLS_FILE"])


def _doc():
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def _record(name):
    with CALLS_FILE.open("a", encoding="utf-8") as fh:
        fh.write(name + "\n")


class FakeState:
    def public_state(self):
        doc = _doc()
        return {
            "status": doc.get("status", "disconnected"),
            "mc_state": doc.get("mc_state", "unknown"),
            "sandbox_state": doc.get("sandbox_state", "UNKNOWN"),
            "sandbox_id": "sbx-panel-test",
            "connected": bool(doc.get("connected", False)),
            "agent": {},
            "last_seen": None,
            "last_seen_ago": None,
            "server_time": "2026-10-24T00:00:00Z",
            "controller_started_at": "2026-10-24T00:00:00Z",
            "controller_uptime": 12.5,
            "snapshot_interval": 21600,
            "recovery": {"active": False, "attempt": 0, "step": "idle"},
            "provisioning": {
                "active": False, "step": "idle", "last_error": None, "steps": []
            },
        }


class FakeWS:
    def public_state(self):
        doc = _doc()
        return {
            "running": bool(doc.get("ws_running", False)),
            "stats": {"connections": 1, "messages": 3},
        }


class FakeStorage:
    def backups_payload(self):
        doc = _doc()
        has_world = bool(doc.get("world", False))
        snaps = []
        if doc.get("snapshots"):
            snaps = [{
                "name": "snapshot-20260101-000000.tar.gz",
                "size": 2048,
                "size_human": "2.0 KB",
                "mtime": 0.0,
                "created_at": "2026-01-01T00:00:00",
            }]
        return {
            "snapshots": snaps,
            "deltas": {},
            "mirror": {
                "files": 3 if has_world else 0,
                "bytes": 100 if has_world else 0,
                "bytes_human": "100 B" if has_world else "0 B",
                "has_world": has_world,
            },
            "keep": {"snapshots": 8, "deltas": 20},
            "interval": 21600,
        }


class FakeCfg:
    def __init__(self, root):
        self.mirror_dir = root / "mirror"
        self.snapshots_dir = root / "snapshots"


class FakeOrch:
    def __init__(self):
        self.state = FakeState()
        self.ws = FakeWS()
        self.storage = FakeStorage()
        self.store_cfg = FakeCfg(Path(os.environ["PANEL_ORCH_DIR"]))
        self.counters = {"start_host": 0, "stop_host": 0, "snapshots": 0}

    async def start(self):
        _record("orch.start")
        return {
            "ok": True,
            "started": [],
            "agent_ws": False,
            "agent_ws_enabled": False,
            "autostart": False,
        }

    async def close(self):
        _record("orch.close")

    async def start_host(self, *, reason="manual"):
        self.counters["start_host"] += 1
        _record("start_host")
        return {"ok": True, "reason": reason, "steps": []}

    async def stop_host(self, *, reason="manual"):
        self.counters["stop_host"] += 1
        _record("stop_host")
        return {"ok": True, "reason": reason, "steps": []}

    async def snapshot_now(self, reason="manual"):
        self.counters["snapshots"] += 1
        if not _doc().get("world", False):
            _record("snapshot:skipped")
            return {"ok": False, "skipped": "no world in mirror"}
        _record("snapshot:ok")
        return {
            "ok": True,
            "name": "snapshot-20260101-000000.tar.gz",
            "size": 2048,
            "size_human": "2.0 KB",
        }


def build_orchestrator(settings=None):
    return FakeOrch()


import app.main as appmain  # noqa: E402

appmain.build_orchestrator = build_orchestrator

import uvicorn  # noqa: E402

uvicorn.run(
    appmain.app,
    host="127.0.0.1",
    port=int(os.environ["PANEL_PORT"]),
    loop="asyncio",
    http="h11",
    log_level="warning",
    access_log=False,
)
'''


# ------------------------------------------------------------------------ main
def main() -> int:
    failures = 0
    tmp = Path(tempfile.mkdtemp(prefix="dayhost-panel-test-"))
    state_file = tmp / "state.json"
    calls_file = tmp / "calls.log"
    calls_file.write_text("")
    write_state(state_file)

    launcher = tmp / "launch_fake.py"
    launcher.write_text(LAUNCHER_SRC)

    repo_data = ROOT / "data"
    data_before = sorted(p.name for p in repo_data.iterdir()) if repo_data.is_dir() else []

    env_fake = base_env(tmp, env="dev")
    env_fake.update(
        {
            "ORCH_ENABLED": "true",
            "PANEL_ROOT": str(ROOT),
            "PANEL_STATE_FILE": str(state_file),
            "PANEL_CALLS_FILE": str(calls_file),
            "PANEL_ORCH_DIR": str(tmp / "orch"),
        }
    )
    env_prod = base_env(tmp, env="prod")
    # A distinct state dir: the dev process must not share a SQLite file with it.
    for key in ("DB_PATH", "LOG_FILE"):
        env_prod[key] = str(tmp / "prod" / Path(env_prod[key]).name)
    env_prod["BACKUP_DIR"] = str(tmp / "prod" / "backups")
    env_prod["ORCH_DATA_DIR"] = str(tmp / "prod" / "orch")
    (tmp / "prod").mkdir(parents=True, exist_ok=True)
    env_prod.update({"ORCH_ENABLED": "false"})

    servers: list[Server] = []
    print(f"temp dir: {tmp}")

    try:
        # =============================================================== (a-f)
        section("(a-f) fake orchestrator, DAYHOST_ENV=dev")
        fake = Server("fake", tmp, env_fake, launcher=launcher)
        servers.append(fake)
        if not fake.ready():
            print("server did not start; log tail:")
            print(fake.output()[-3000:])
            return 1
        base = fake.base
        print(f"uvicorn on {base}")

        # ---- (a) auth ---------------------------------------------------
        status, headers, _ = request(base + "/", accept="text/html")
        failures += check(
            "(a) / without cookie (html) -> 303 /login",
            status == 303 and headers.get("location") == "/login",
            f"{status} {headers.get('location')}",
        )
        status, _, body = request(base + "/", accept="application/json")
        failures += check("(a) / without cookie (json) -> 401", status == 401, str(status))
        cookie = login(base)
        status, _, body = request(base + "/", cookie=cookie, accept="text/html")
        failures += check("(a) / after login -> 200", status == 200, str(status))

        # ---- (b) dashboard states ---------------------------------------
        write_state(
            state_file,
            sandbox_state="RUNNING", mc_state="running", status="online",
            connected=True, ws_running=True,
        )
        status, _, body = request(base + "/", cookie=cookie, accept="text/html")
        sandbox_label = cell(body, "host-sandbox-state")
        mc_label = cell(body, "host-mc-state")
        link_label = cell(body, "host-link-status")
        failures += check("(b) dashboard 200 while sandbox RUNNING", status == 200, str(status))
        failures += check("(b) RUNNING rendered as the sandbox label",
                          "RUNNING" in body and sandbox_label == "RUNNING",
                          f"label={sandbox_label!r}")
        failures += check("(b) mc_state 'running' rendered (agent field, not merged)",
                          mc_label == "running", f"label={mc_label!r}")
        failures += check("(b) agent link 'online' rendered", link_label == "online",
                          f"label={link_label!r}")
        failures += check("(b) WS card says 'listening' when the listener is up",
                          "listening" in body)

        write_state(
            state_file,
            sandbox_state="STOPPED", mc_state="unknown", status="disconnected",
            connected=False, ws_running=False,
        )
        status, _, body = request(base + "/", cookie=cookie, accept="text/html")
        sandbox_label = cell(body, "host-sandbox-state")
        mc_label = cell(body, "host-mc-state")
        failures += check("(b) STOPPED rendered explicitly",
                          "STOPPED" in body and sandbox_label == "STOPPED",
                          f"label={sandbox_label!r}")
        failures += check("(b) 'unknown' rendered explicitly (never blank)",
                          "unknown" in body and mc_label == "unknown",
                          f"label={mc_label!r}")
        failures += check("(b) no empty label cells",
                          all(cell(body, i) for i in
                              ("host-sandbox-state", "host-mc-state", "host-link-status")))

        # ---- (c) host start/stop ----------------------------------------
        failures += check("(c) lifespan built the fake orchestrator (orch.start recorded)",
                          calls(calls_file) == ["orch.start"], str(calls(calls_file)))
        status, _, body = request(base + "/host/start", data={}, cookie=cookie,
                                  accept="text/html")
        failures += check("(c) POST /host/start -> 200 partial", status == 200, str(status))
        failures += check("(c) start_host() reached the orchestrator",
                          "start_host" in calls(calls_file), str(calls(calls_file)))
        failures += check("(c) response is the host card partial",
                          'id="host-card"' in body and "<html" not in body.lower())
        failures += check("(c) start response has no traceback",
                          "Traceback" not in body)

        status, _, body = request(base + "/host/stop", data={}, cookie=cookie,
                                  accept="text/html")
        failures += check("(c) POST /host/stop -> 200 partial", status == 200, str(status))
        failures += check("(c) stop_host() reached the orchestrator",
                          "stop_host" in calls(calls_file), str(calls(calls_file)))
        failures += check("(c) stop response is the host card partial",
                          'id="host-card"' in body and "<html" not in body.lower())
        failures += check("(c) stop response has no traceback",
                          "Traceback" not in body)

        # ---- (d) backup --------------------------------------------------
        write_state(state_file, world=False, sandbox_state="STOPPED",
                    mc_state="unknown")
        before = calls(calls_file).count("snapshot:skipped")
        status, _, body = request(base + "/backup", data={}, cookie=cookie,
                                  accept="text/html")
        failures += check("(d) empty mirror: POST /backup -> 200 (not 500)",
                          status == 200, str(status))
        failures += check("(d) snapshot_now() was called (skipped branch)",
                          calls(calls_file).count("snapshot:skipped") == before + 1,
                          str(calls(calls_file)[-4:]))
        failures += check("(d) body says 'Belum ada data'",
                          "Belum ada data" in body, body.strip()[:120])
        failures += check("(d) empty-mirror body has no traceback",
                          "Traceback" not in body)

        write_state(state_file, world=True, snapshots=True,
                    sandbox_state="RUNNING", mc_state="running")
        status, _, body = request(base + "/backup", data={}, cookie=cookie,
                                  accept="text/html")
        failures += check("(d) with a world: /backup -> 200 and reports the snapshot",
                          status == 200 and "Snapshot dibuat" in body,
                          f"{status} {body.strip()[:120]}")
        failures += check("(d) success path also has no traceback",
                          "Traceback" not in body)
        status, _, body = request(base + "/partials/agent", cookie=cookie,
                                  accept="text/html")
        failures += check("(d) partial lists the new snapshot",
                          status == 200 and "snapshot-20260101-000000.tar.gz" in body,
                          str(status))

        # ---- (f) SSE: two independent streams ---------------------------
        status, raw_logs = sse(base + "/events", cookie, seconds=2.5)
        logs = event_source_dispatch(raw_logs)
        failures += check("(f) GET /events -> 200 text/event-stream", status == 200,
                          str(status))
        failures += check("(f) /events carries >=1 anonymous data: frame",
                          len(logs["anonymous"]) >= 1,
                          f"{len(logs['anonymous'])} frame(s)")
        failures += check("(f) /events never emits a named 'state' event",
                          "state" not in logs["named"], str(list(logs["named"])))

        status, raw_state = sse(base + "/events/state", cookie, seconds=2.5)
        states = event_source_dispatch(raw_state)
        failures += check("(f) GET /events/state -> 200", status == 200, str(status))
        failures += check("(f) /events/state sends 'event: state'",
                          len(states["named"].get("state") or []) >= 1,
                          f"named={list(states['named'])}")
        payload = None
        try:
            payload = json.loads((states["named"].get("state") or ["{}"])[0])
        except ValueError:
            payload = None
        failures += check("(f) state frame is the JSON panel payload",
                          isinstance(payload, dict) and "available" in payload
                          and isinstance(payload.get("state"), dict),
                          str(payload)[:120])
        failures += check("(f) /events/state sends no anonymous frames "
                          "(onmessage stays silent)",
                          states["anonymous"] == [], str(states["anonymous"])[:120])
        failures += check("(f) console.js still listens on onmessage only, "
                          "so it cannot receive the JSON state",
                          'source.onmessage' in (ROOT / "app/static/js/console.js").read_text()
                          and 'addEventListener("state"' not in
                          (ROOT / "app/static/js/console.js").read_text())

        # ---- dev docs present (contrast for the prod check) -------------
        status, _, _ = request(base + "/api/docs")
        failures += check("(bonus) dev: /api/docs -> 200", status == 200, str(status))

        fake_output = fake.output()
        failures += check("(c/d) uvicorn log has zero tracebacks",
                          "Traceback (most recent call last)" not in fake_output,
                          fake_output[-300:].replace("\n", " | "))

        fake.stop()
        servers.remove(fake)

        # ================================================== (b) disabled + (e)
        section("(b) disabled card + (e) 503 without an orchestrator (prod)")
        off = Server("prod", tmp, env_prod)
        servers.append(off)
        if not off.ready():
            print("prod server did not start; log tail:")
            print(off.output()[-3000:])
            return 1
        obase = off.base
        print(f"uvicorn on {obase} (DAYHOST_ENV=prod, ORCH_ENABLED=false)")
        ocookie = login(obase, env="prod")
        status, _, body = request(obase + "/", cookie=ocookie, accept="text/html")
        failures += check("(b) disabled: dashboard still 200", status == 200, str(status))
        failures += check("(b) orchestrator card renders 'disabled' with an explanation",
                          "disabled" in body and "Orchestrator disabled." in body,
                          repr(body[:120]))
        failures += check("(b) agent card says the WS is not managed",
                          "tidak dikelola" in body)

        for route in ("/host/start", "/host/stop", "/backup"):
            status, _, body = request(obase + route, data={}, cookie=ocookie,
                                      accept="text/html")
            failures += check(f"(e) POST {route} -> 503 (not 500)", status == 503,
                              str(status))
            failures += check(f"(e) {route} error is structured + traceback-free",
                              "Orchestrator nonaktif" in body
                              and "Traceback" not in body,
                              body.strip()[:120])

        status, headers, body = request(obase + "/api/docs")
        failures += check("(bonus) prod: /api/docs -> 404", status == 404, str(status))
        status, _, _ = request(obase + "/api/openapi.json")
        failures += check("(bonus) prod: /api/openapi.json -> 404", status == 404,
                          str(status))
        status, _, _ = request(obase + "/api/redoc")
        failures += check("(bonus) prod: /api/redoc -> 404", status == 404, str(status))

        off_output = off.output()
        failures += check("(e) prod uvicorn log has zero tracebacks",
                          "Traceback (most recent call last)" not in off_output,
                          off_output[-300:].replace("\n", " | "))
        off.stop()
        servers.remove(off)

        # ------------------------------------------------- (g) AGENT_WS_ENABLED
        section("(g) AGENT_WS_ENABLED=false really leaves the WS port free")
        ws_port = free_port()
        PORTS.append(ws_port)
        env_ws_off = base_env(tmp, env="dev")
        env_ws_off.update(
            {
                "ORCH_ENABLED": "true",
                "AGENT_WS_ENABLED": "false",
                "AGENT_WS_PORT": str(ws_port),
                "AGENT_WS_HOST": "127.0.0.1",
                "ORCH_SNAPSHOT_ENABLED": "false",
                "ORCH_AUTOSTART": "false",
            }
        )
        real = Server("orch-off", tmp, env_ws_off)
        servers.append(real)
        if not real.ready():
            print("orchestrator-enabled server did not start; log tail:")
            print(real.output()[-3000:])
            return 1
        rbase = real.base
        print(f"real orchestrator on {rbase}; AGENT_WS_PORT={ws_port}")
        rcookie = login(rbase)
        status, _, body = request(rbase + "/partials/host", cookie=rcookie,
                                  accept="text/html")
        failures += check("(g) real orchestrator is live (panel is not 'disabled')",
                          status == 200 and "Orchestrator disabled." not in body,
                          str(status))
        failures += check("(g) panel reports the WS listener as 'not listening'",
                          "not listening" in body, repr(body[:200]))
        failures += check("(g) AGENT_WS_PORT is genuinely free while the app runs",
                          port_is_free(ws_port))
        status, _, body = request(rbase + "/", cookie=rcookie, accept="text/html")
        failures += check("(g) dashboard 200 with the real orchestrator wired",
                          status == 200, str(status))
        failures += check("(g) sandbox state comes from the real state object (UNKNOWN)",
                          cell(body, "host-sandbox-state") == "UNKNOWN",
                          f"label={cell(body, 'host-sandbox-state')!r}")
        real_output = real.output()
        failures += check("(g) real-orchestrator boot logged no traceback",
                          "Traceback (most recent call last)" not in real_output,
                          real_output[-300:].replace("\n", " | "))
        real.stop()
        servers.remove(real)
        failures += check("(g) WS port is free again after shutdown",
                          port_is_free(ws_port))

        # positive control: the flag is actually read (not vacuous)
        env_ws_on = dict(env_ws_off, AGENT_WS_ENABLED="true",
                         AGENT_WS_TOKEN="panel-test-agent-token")
        ws_on_port = free_port()
        PORTS.append(ws_on_port)
        env_ws_on["AGENT_WS_PORT"] = str(ws_on_port)
        on = Server("orch-on", tmp, env_ws_on)
        servers.append(on)
        if not on.ready():
            print("positive-control server did not start; log tail:")
            print(on.output()[-3000:])
            return 1
        ocookie2 = login(on.base)
        _, _, body = request(on.base + "/partials/host", cookie=ocookie2,
                             accept="text/html")
        failures += check("(g) positive control: AGENT_WS_ENABLED=true binds the port",
                          not port_is_free(ws_on_port)
                          and "listening" in body,
                          f"free={port_is_free(ws_on_port)}")
        on.stop()
        servers.remove(on)
        failures += check("(g) positive control: port released on close",
                          port_is_free(ws_on_port))

        # ---------------------------------------------------- housekeeping
        section("housekeeping")
        failures += check("no server process left running",
                          all(s.proc.poll() is not None for s in servers) if servers else True)
        failures += check("every port this harness used is free",
                          all(port_is_free(p) for p in PORTS),
                          str([p for p in PORTS if not port_is_free(p)]))
        data_after = sorted(p.name for p in repo_data.iterdir()) if repo_data.is_dir() else []
        failures += check("repo ./data was not written to by this harness",
                          data_after == data_before,
                          f"before={data_before} after={data_after}")

    finally:
        for server in list(servers):
            try:
                server.stop()
            except Exception:  # noqa: BLE001
                pass
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    print(f"{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("failed:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1
    print("ALL PANEL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
