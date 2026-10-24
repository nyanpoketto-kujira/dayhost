"""Daytona driver checks — `python scripts/check_daytona_driver.py`

Runs against a fake Daytona server on loopback (stdlib only, no network, no
real credentials). The fake serves **both** surfaces the driver touches:

    lifecycle : /sandbox/{id}, /start, /stop, /recover, /toolbox-proxy-url
    toolbox   : /toolbox/{id}/process/...   (session exec, stateless exec,
                session create/get, command poll, process status, process logs)

What is proven (a–g, matching the Phase-A brief):

  a) start/stop/status hit the correct lifecycle paths, bearer auth sent,
     `stop(force=True)` sets `?force=true`, and start() polls to RUNNING;
  b) exec_command dispatches the command and returns the right
     stdout/stderr/exit_code — including the `cmdId` + poll variant;
  c) the stateless `/process/exec` fallback works when the env points there;
  d) the toolbox base is resolved from `/toolbox-proxy-url` **exactly once**
     and then cached (request counter);
  e) a 401 from the toolbox becomes one actionable sentence, with **zero**
     tracebacks (unhandled-exception hooks are armed for the whole run);
  f) ensure_agent_running is idempotent — 3 calls while alive = 0 launches,
     dead = exactly 1 launch — on both the process-status and pgrep paths;
  g) unconfigured -> structured error naming the missing setting, no traceback.

Exit 0 = all passed. Writes nothing outside its own temp dir (removed at end).
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import traceback
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["DAYHOST_ENV_FILE"] = str(ROOT / ".nonexistent-harness.env")
os.environ.setdefault("LOG_LEVEL", "CRITICAL")

from app.config import get_settings  # noqa: E402
from app.services import daytona_driver  # noqa: E402
from app.services.daytona_driver import DaytonaDriver  # noqa: E402

SANDBOX = "sbx-drv-1"
API_KEY = "test-api-key"
SESSION = "dayhost"

# --- traceback surveillance (armed for the whole run) -----------------------
TRACEBACKS: list[str] = []


def _record_exception(where: str, exc: BaseException) -> None:
    TRACEBACKS.append(f"{where}: {''.join(traceback.format_exception(exc))}")


def _install_hooks() -> None:
    def hook(exc_type, exc, tb):
        TRACEBACKS.append("".join(traceback.format_exception(exc_type, exc, tb)))

    sys.excepthook = hook
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _l, ctx: TRACEBACKS.append(repr(ctx)))


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


class FakeDaytona:
    """Fake lifecycle + toolbox proxy that records every request it received."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.state = "stopped"
        self.reject_toolbox = False
        self.require_auth = False
        self.status_404 = False
        self.session_get_404 = False
        self.agent_alive = False
        self.state_flips_to_started = True
        self.httpd: ThreadingHTTPServer | None = None
        self.port = 0

    # ---------------------------------------------------------------- counters
    def calls(self, needle: str) -> list[dict]:
        return [r for r in self.requests if needle in r["path"]]

    def exact(self, method: str, path: str) -> list[dict]:
        return [r for r in self.requests if r["method"] == method and r["path"] == path]

    def launches(self) -> list[dict]:
        out = []
        for req in self.requests:
            if req["method"] != "POST":
                continue
            body = req.get("json") or {}
            cmd = str(body.get("command") or "")
            if "setsid nohup" in cmd:
                out.append(req)
        return out

    def reset(self) -> None:
        self.requests.clear()

    def execs(self, kind: str = "session") -> list[dict]:
        """Exec calls only (not session create/get, not command polls)."""
        if kind == "stateless":
            return [r for r in self.requests
                    if r["method"] == "POST" and r["path"].endswith("/process/exec")]
        return [r for r in self.requests
                if r["method"] == "POST"
                and r["path"].endswith(f"/process/session/{SESSION}/exec")]

    def session_creates(self) -> list[dict]:
        return [r for r in self.requests
                if r["method"] == "POST"
                and r["path"].endswith("/process/session")]

    # ------------------------------------------------------------ fake sandbox
    def _exec(self, payload: dict) -> dict:
        """Answer an exec call the way the docs describe."""
        cmd = str(payload.get("command") or "")
        if "setsid nohup" in cmd:
            self.agent_alive = True
            return {"exitCode": 0, "stdout": "", "stderr": ""}
        if "pgrep" in cmd:
            if self.agent_alive:
                return {"exitCode": 0, "stdout": "4242\n", "stderr": ""}
            return {"exitCode": 1, "stdout": "", "stderr": ""}
        if "poll-me" in cmd:
            # Session variant: dispatch ack only, result must be polled.
            return {"cmdId": "cmd-9"}
        if "boom" in cmd:
            return {"exitCode": 2, "stdout": "partial\n", "stderr": "boom failed\n"}
        return {"exitCode": 0, "stdout": "hello from sandbox\n", "stderr": ""}

    # ------------------------------------------------------------------ server
    def start(self) -> int:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _body(self) -> bytes:
                length = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(length) if length else b""

            def _json(self, code: int, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _text(self, code: int, text: str) -> None:
                body = text.encode()
                self.send_response(code)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _record(self, method: str) -> tuple[str, dict, dict]:
                parsed = urlparse(self.path)
                raw = self._body()
                payload: dict = {}
                if raw:
                    try:
                        payload = json.loads(raw.decode() or "{}")
                    except Exception:
                        payload = {"_raw": raw[:200].decode("utf-8", "replace")}
                outer.requests.append(
                    {
                        "method": method,
                        "path": parsed.path,
                        "query": parse_qs(parsed.query),
                        # HTTP header names are case-insensitive; BaseHTTPRequestHandler
                        # keeps the client's casing, so use the case-insensitive lookup.
                        "auth": self.headers.get("Authorization"),
                        "json": payload,
                    }
                )
                return parsed.path, parse_qs(parsed.query), payload

            # ------------------------------------------------------ endpoints
            def _handle(self, method: str) -> None:
                path, query, payload = self._record(method)
                auth = self.headers.get("Authorization")

                # ---- lifecycle API ------------------------------------
                if path.endswith("/toolbox-proxy-url"):
                    if outer.require_auth and auth != f"Bearer {API_KEY}":
                        self._json(401, {"error": "bad key"})
                        return
                    self._json(200, {"url": f"http://127.0.0.1:{outer.port}/toolbox"})
                    return

                if path.startswith("/sandbox/"):
                    if outer.require_auth and auth != f"Bearer {API_KEY}":
                        self._json(401, {"error": "bad key"})
                        return
                    if path.endswith("/start"):
                        outer.state = "starting" if outer.state_flips_to_started else "error"
                        self._json(200, {"id": SANDBOX, "state": outer.state})
                        return
                    if path.endswith("/stop"):
                        outer.state = "stopping"
                        self._json(200, {"id": SANDBOX, "state": outer.state})
                        return
                    if path.endswith("/recover"):
                        outer.state = "started"
                        self._json(200, {"id": SANDBOX, "state": outer.state})
                        return
                    # GET status: flip "starting" -> "started" on the next poll
                    if outer.state == "starting":
                        outer.state = "started"
                    self._json(
                        200,
                        {
                            "id": SANDBOX,
                            "name": "mc-test",
                            "state": outer.state,
                            "desiredState": "started",
                            "errorReason": None,
                            "recoverable": False,
                        },
                    )
                    return

                # ---- toolbox proxy ------------------------------------
                prefix = f"/toolbox/{SANDBOX}"
                if not path.startswith(prefix):
                    self._json(404, {"error": "unknown path"})
                    return
                rest = path[len(prefix):] or "/"

                if outer.reject_toolbox:
                    self._json(401, {"error": "unauthorized: no sandbox token"})
                    return
                if outer.require_auth and auth != f"Bearer {API_KEY}":
                    self._json(401, {"error": "unauthorized"})
                    return

                if rest == "/process/session" and method == "POST":
                    self._json(200, {"sessionId": SESSION})
                    return

                if rest == f"/process/session/{SESSION}" and method == "GET":
                    if outer.session_get_404:
                        self._json(404, {"error": "session not found"})
                        return
                    self._json(200, {"sessionId": SESSION, "state": "running"})
                    return

                if rest == f"/process/session/{SESSION}/exec" and method == "POST":
                    self._json(200, outer._exec(payload))
                    return

                if rest.startswith(f"/process/session/{SESSION}/command/"):
                    self._json(200, {"exitCode": 0, "stdout": "polled output\n", "stderr": ""})
                    return

                if rest == "/process/exec" and method == "POST":
                    self._json(200, outer._exec(payload))
                    return

                if rest.startswith("/process/") and rest.endswith("/status"):
                    if outer.status_404:
                        self._json(404, {"error": "process api unavailable"})
                        return
                    self._json(
                        200,
                        {
                            "name": rest.split("/")[2],
                            "running": outer.agent_alive,
                            "pid": 4242 if outer.agent_alive else None,
                        },
                    )
                    return

                if rest.startswith("/process/") and rest.endswith("/logs"):
                    self._json(200, {"logs": "agent log line 1\nagent log line 2\n"})
                    return

                self._json(404, {"error": f"no route for {rest}"})

            def do_GET(self):  # noqa: N802
                self._handle("GET")

            def do_POST(self):  # noqa: N802
                self._handle("POST")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self.port

    def stop(self) -> None:
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_settings(port: int, **overrides):
    base = dict(
        daytona_api_key=API_KEY,
        sandbox_id=SANDBOX,
        daytona_api_base=f"http://127.0.0.1:{port}",
        daytona_toolbox_url=f"http://127.0.0.1:{port}/toolbox",
        daytona_toolbox_auth="bearer",
        daytona_timeout=10,
        daytona_max_retries=0,          # deterministic, fast failures
        daytona_start_timeout=5,
        daytona_poll_interval=0.05,
        daytona_toolbox_session_id=SESSION,
        agent_remote_dir="mc-orchestrator/agent",
        agent_launch_cmd="cd {dir} && setsid nohup python3 agent.py >> agent.log 2>&1 &",
    )
    base.update(overrides)
    return replace(get_settings(), **base)


def fresh_driver(settings, **overrides) -> DaytonaDriver:
    daytona_driver._driver = None
    return DaytonaDriver(replace(settings, **overrides) if overrides else settings)


async def main() -> int:
    failures = 0
    _install_hooks()

    tmp = Path(tempfile.mkdtemp(prefix="dayhost-driver-test-"))
    server = FakeDaytona()
    port = server.start()
    settings = make_settings(port)
    print(f"fake daytona on 127.0.0.1:{port} (sandbox {SANDBOX})\n")

    driver = None
    try:
        # =====================================================================
        print("== (a) lifecycle: paths, bearer auth, force-stop, polling ==")
        # =====================================================================
        server.require_auth = True
        driver = fresh_driver(settings)

        status = await driver.status()
        failures += 0 if check("status ok", status.get("ok") is True, str(status.get("error"))) else 1
        failures += 0 if check(
            "GET /sandbox/{id} used",
            any(r["method"] == "GET" and r["path"] == f"/sandbox/{SANDBOX}"
                for r in server.requests),
            str([r["path"] for r in server.requests][:3]),
        ) else 1
        failures += 0 if check(
            "state normalised to STOPPED (sender-side)",
            status.get("state") == "STOPPED", str(status.get("state")),
        ) else 1
        failures += 0 if check(
            "lifecycle requests carried Bearer auth",
            all(r["auth"] == f"Bearer {API_KEY}" for r in server.calls("/sandbox/")),
            str({r["auth"] for r in server.calls("/sandbox/")}),
        ) else 1

        server.reset()
        started = await driver.start()
        start_calls = [r for r in server.calls("/start")]
        failures += 0 if check(
            "POST /sandbox/{id}/start used", len(start_calls) == 1, str(len(start_calls))
        ) else 1
        failures += 0 if check(
            "start waited for the sandbox to become RUNNING",
            started.get("ok") is True and started.get("state") == "RUNNING",
            f"ok={started.get('ok')} state={started.get('state')} polls={started.get('polls')}",
        ) else 1
        failures += 0 if check(
            "start polled GET /sandbox/{id} at least once",
            (started.get("polls") or 0) >= 1
            and any(r["method"] == "GET" and r["path"] == f"/sandbox/{SANDBOX}"
                    for r in server.requests),
            f"polls={started.get('polls')}",
        ) else 1

        server.reset()
        stopped = await driver.stop(force=True)
        stop_call = (server.calls("/stop") or [{}])[0]
        failures += 0 if check(
            "POST /stop with force=true",
            stop_call.get("query", {}).get("force") == ["true"],
            str(stop_call.get("query")),
        ) else 1
        failures += 0 if check("stop ok", stopped.get("ok") is True, str(stopped.get("error"))) else 1

        server.reset()
        soft = await driver.stop()
        failures += 0 if check(
            "stop(force=False) sends no force query",
            (server.calls("/stop") or [{}])[0].get("query") == {},
            str((server.calls('/stop') or [{}])[0].get('query')),
        ) else 1
        failures += 0 if check("soft stop ok", soft.get("ok") is True, str(soft.get("error"))) else 1

        server.reset()
        recovered = await driver.recover()
        failures += 0 if check(
            "POST /recover used",
            recovered.get("ok") is True and len(server.calls("/recover")) == 1,
            str(recovered.get("error")),
        ) else 1
        server.require_auth = False

        # =====================================================================
        print("\n== (b) exec_command: command out, stdout/stderr/exit_code ==")
        # =====================================================================
        server.reset()
        out = await driver.exec_command("echo hi")
        session_execs = server.execs("session")
        failures += 0 if check(
            "POST /process/session/{id}/exec used (preferred path)",
            len(session_execs) == 1, str(len(session_execs)),
        ) else 1
        failures += 0 if check(
            "command body dispatched verbatim",
            (session_execs[0]["json"] or {}).get("command") == "echo hi",
            str((session_execs[0]["json"] or {}).get("command")),
        ) else 1
        failures += 0 if check("ok True", out.get("ok") is True, str(out.get("error"))) else 1
        failures += 0 if check(
            "stdout captured", out.get("stdout") == "hello from sandbox\n", repr(out.get("stdout"))
        ) else 1
        failures += 0 if check(
            "exit_code 0 and success True",
            out.get("exit_code") == 0 and out.get("success") is True,
            f"exit={out.get('exit_code')} success={out.get('success')}",
        ) else 1
        failures += 0 if check(
            "session probed once, then cached (no create, no re-probe per exec)",
            len(server.session_creates()) == 0
            and len(server.exact("GET", f"/toolbox/{SANDBOX}/process/session/{SESSION}")) == 1,
            f"creates={len(server.session_creates())} "
            f"gets={len(server.exact('GET', f'/toolbox/{SANDBOX}/process/session/{SESSION}'))}",
        ) else 1

        failed_cmd = await driver.exec_command("boom")
        failures += 0 if check(
            "non-zero exit_code is reported, request still ok",
            failed_cmd.get("ok") is True and failed_cmd.get("exit_code") == 2,
            f"ok={failed_cmd.get('ok')} exit={failed_cmd.get('exit_code')}",
        ) else 1
        failures += 0 if check(
            "stderr captured on failure",
            failed_cmd.get("stderr") == "boom failed\n", repr(failed_cmd.get("stderr")),
        ) else 1
        failures += 0 if check(
            "success False when exit_code != 0",
            failed_cmd.get("success") is False,
        ) else 1

        polled = await driver.exec_command("poll-me")
        failures += 0 if check(
            "cmdId dispatch is polled for its result",
            polled.get("ok") is True and polled.get("stdout") == "polled output\n",
            f"ok={polled.get('ok')} stdout={polled.get('stdout')!r}",
        ) else 1
        failures += 0 if check(
            "poll hit /command/{id}",
            len(server.calls(f"/process/session/{SESSION}/command/")) >= 1,
            str(len(server.calls('/command/'))),
        ) else 1

        # Missing session -> CreateSession, then exec (never a hard failure).
        server.reset()
        server.session_get_404 = True
        recreated = fresh_driver(settings)
        created = await recreated.exec_command("echo after-create")
        failures += 0 if check(
            "404 on session GET -> CreateSession then exec",
            created.get("ok") is True
            and len(server.session_creates()) == 1
            and len(server.execs("session")) == 1,
            f"creates={len(server.session_creates())} execs={len(server.execs('session'))}",
        ) else 1
        failures += 0 if check(
            "recreated session returns the command output",
            created.get("stdout") == "hello from sandbox\n", repr(created.get("stdout")),
        ) else 1
        await recreated.aclose()
        server.session_get_404 = False

        # =====================================================================
        print("\n== (c) stateless fallback: /process/exec when env points there ==")
        # =====================================================================
        server.reset()
        stateless = fresh_driver(settings, daytona_toolbox_exec_mode="stateless")
        out2 = await stateless.exec_command("echo fallback")
        failures += 0 if check(
            "POST /process/exec used",
            len(server.execs("stateless")) == 1 and len(server.execs("session")) == 0,
            str([r["path"] for r in server.requests]),
        ) else 1
        failures += 0 if check(
            "stateless stdout correct",
            out2.get("ok") is True and out2.get("stdout") == "hello from sandbox\n",
            f"ok={out2.get('ok')} stdout={out2.get('stdout')!r}",
        ) else 1
        failures += 0 if check(
            "mode reported as stateless", out2.get("mode") == "stateless", str(out2.get("mode"))
        ) else 1
        await stateless.aclose()

        # The documented one-setting escape hatch: empty the session path.
        server.reset()
        emptied = fresh_driver(settings, daytona_toolbox_path_exec="")
        out3 = await emptied.exec_command("echo empty")
        failures += 0 if check(
            "empty DAYTONA_TOOLBOX_PATH_EXEC forces the fallback",
            out3.get("mode") == "stateless"
            and len(server.execs("stateless")) == 1,
            f"mode={out3.get('mode')} calls={len(server.execs('stateless'))}",
        ) else 1
        await emptied.aclose()

        # =====================================================================
        print("\n== (d) toolbox base resolved once, then cached ==")
        # =====================================================================
        server.reset()
        discovering = fresh_driver(settings, daytona_toolbox_url="")
        b1 = await discovering.toolbox_base()
        b2 = await discovering.toolbox_base()
        for _ in range(3):
            await discovering.exec_command("echo cached")
        discovery = server.calls("/toolbox-proxy-url")
        failures += 0 if check(
            "discovered base includes the sandbox segment",
            b1 == f"http://127.0.0.1:{port}/toolbox/{SANDBOX}", str(b1),
        ) else 1
        failures += 0 if check("second resolve is the cached value", b1 == b2) else 1
        failures += 0 if check(
            "exactly ONE discovery request for 2 resolves + 3 execs",
            len(discovery) == 1, f"{len(discovery)} discovery call(s)",
        ) else 1
        await discovering.aclose()

        # =====================================================================
        print("\n== (e) 401 -> actionable error, zero traceback ==")
        # =====================================================================
        before = len(TRACEBACKS)
        server.reject_toolbox = True
        denied = await driver.exec_command("echo denied")
        failures += 0 if check("ok False", denied.get("ok") is False, str(denied.get("ok"))) else 1
        failures += 0 if check("kind == auth", denied.get("kind") == "auth", str(denied.get("kind"))) else 1
        failures += 0 if check(
            "message is the actionable sentence",
            "toolbox proxy rejected credentials" in (denied.get("error") or ""),
            str(denied.get("error")),
        ) else 1
        failures += 0 if check(
            "status_code 401", denied.get("status_code") == 401, str(denied.get("status_code"))
        ) else 1
        failures += 0 if check(
            "no exception escaped (handlers saw nothing)",
            len(TRACEBACKS) == before, f"{len(TRACEBACKS) - before} captured",
        ) else 1
        denied_status = await driver.process_status("agent")
        failures += 0 if check(
            "process_status also degrades cleanly on 401",
            denied_status.get("ok") is False and denied_status.get("kind") == "auth"
            and denied_status.get("running") is None,
            str(denied_status.get("kind")),
        ) else 1
        denied_logs = await driver.process_logs("agent")
        failures += 0 if check(
            "process_logs also degrades cleanly on 401",
            denied_logs.get("ok") is False and denied_logs.get("output") == "",
            str(denied_logs.get("kind")),
        ) else 1
        server.reject_toolbox = False

        # =====================================================================
        print("\n== (f) ensure_agent_running is idempotent ==")
        # =====================================================================
        server.reset()
        server.agent_alive = False
        first = await driver.ensure_agent_running()
        failures += 0 if check(
            "dead agent -> exactly 1 launch",
            first.get("launched") is True and len(server.launches()) == 1,
            f"launched={first.get('launched')} launches={len(server.launches())}",
        ) else 1
        failures += 0 if check(
            "launch command is the setsid/nohup form with {dir} substituted",
            "setsid nohup python3 agent.py" in (first.get("command") or "")
            and "mc-orchestrator/agent" in (first.get("command") or ""),
            str(first.get("command")),
        ) else 1
        failures += 0 if check(
            "alive_after verified by a probe",
            first.get("alive_after") is True, str(first.get("alive_after")),
        ) else 1

        server.reset()
        results = [await driver.ensure_agent_running() for _ in range(3)]
        failures += 0 if check(
            "3 calls while alive -> 0 additional launches",
            len(server.launches()) == 0, str(len(server.launches())),
        ) else 1
        failures += 0 if check(
            "all 3 calls report ok, launched False, alive True",
            all(r.get("ok") is True and r.get("launched") is False
                and r.get("alive_before") is True for r in results),
            str([(r.get("ok"), r.get("launched")) for r in results]),
        ) else 1

        # pgrep path: process-status endpoint missing -> liveness still decided
        server.reset()
        server.status_404 = True
        alive_via_pgrep = await driver.ensure_agent_running()
        failures += 0 if check(
            "without /process/status, pgrep sees the live agent -> 0 launches",
            alive_via_pgrep.get("ok") is True
            and alive_via_pgrep.get("launched") is False
            and len(server.launches()) == 0,
            f"launched={alive_via_pgrep.get('launched')} launches={len(server.launches())}",
        ) else 1

        server.reset()
        server.agent_alive = False
        dead_via_pgrep = await driver.ensure_agent_running()
        failures += 0 if check(
            "without /process/status, dead agent -> exactly 1 launch",
            dead_via_pgrep.get("launched") is True and len(server.launches()) == 1,
            f"launches={len(server.launches())}",
        ) else 1

        across = await driver.ensure_agent_running()
        failures += 0 if check(
            "a 4th call right after does not launch again",
            across.get("launched") is False and len(server.launches()) == 1,
            f"launches={len(server.launches())}",
        ) else 1
        server.status_404 = False

        # =====================================================================
        print("\n== (g) unconfigured -> structured error, no traceback ==")
        # =====================================================================
        before = len(TRACEBACKS)
        bare = fresh_driver(settings, sandbox_id="", daytona_toolbox_url="", daytona_api_key="")
        missing = await bare.exec_command("echo nope")
        failures += 0 if check("ok False", missing.get("ok") is False) else 1
        failures += 0 if check(
            "kind == config", missing.get("kind") == "config", str(missing.get("kind"))
        ) else 1
        failures += 0 if check(
            "error names the missing setting",
            "DAYTONA_API_KEY" in (missing.get("error") or ""), str(missing.get("error")),
        ) else 1
        failures += 0 if check(
            "all six documented keys present in the result",
            all(k in missing for k in
                ("ok", "status_code", "stdout", "stderr", "exit_code", "error")),
            str(sorted(missing)),
        ) else 1
        no_sandbox = await fresh_driver(settings, sandbox_id="").ensure_agent_running()
        failures += 0 if check(
            "ensure_agent_running unconfigured -> structured, names SANDBOX_ID",
            no_sandbox.get("ok") is False
            and "SANDBOX_ID" in (no_sandbox.get("error") or ""),
            str(no_sandbox.get("error")),
        ) else 1
        failures += 0 if check(
            "no exception escaped", len(TRACEBACKS) == before,
            f"{len(TRACEBACKS) - before} captured",
        ) else 1
        await bare.aclose()
    finally:
        if driver is not None:
            await driver.aclose()
        server.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    failures += 0 if check(
        "zero tracebacks across the entire run", TRACEBACKS == [],
        TRACEBACKS[0][:300] if TRACEBACKS else "",
    ) else 1
    leftovers = list(Path(tempfile.gettempdir()).glob("dayhost-driver-test-*"))
    failures += 0 if check("temp dir cleaned up", leftovers == [], str(leftovers)) else 1

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL DAYTONA DRIVER CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
