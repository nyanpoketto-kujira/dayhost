"""HTTP smoke test against a real uvicorn process on a throwaway port.

Run: `python scripts/check_http.py`

Verifies (Fase 2):
  * /healthz 200, unauthenticated
  * /  -> 303 to /login without a cookie
  * POST /login -> 303 + session cookie
  * GET / -> 200 with the cookie
  * /api/status 200, includes the new sandbox_* fields
  * /api/console/send with RCON unconfigured -> 200 + executed:false (stub)
  * /api/console/send "stop" -> 403 (destructive, blocked)
  * /api/console/send garbage -> 403 (deny-by-default)
  * docs present in dev
Then re-imports the app with DAYHOST_ENV=prod in a subprocess to prove
/api/docs and /api/openapi.json are 404 there.

Everything runs on 127.0.0.1 on a random free port; no external traffic.
Exits 0 on success.
"""
from __future__ import annotations

import json
import os
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
PASSWORD = "test-password-123"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """urllib follows 303 by default, which hides the status we want to assert."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerThread:
    """Run an asyncio server in its own thread + loop (sync test script)."""

    def __init__(self, server) -> None:
        import asyncio
        import threading

        self.server = server
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.port = 0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        import asyncio

        asyncio.set_event_loop(self.loop)
        self.port = self.loop.run_until_complete(self.server.start())
        self.ready.set()
        self.loop.run_forever()

    def start(self) -> int:
        self.thread.start()
        self.ready.wait(timeout=10)
        return self.port

    def stop(self) -> None:
        import asyncio

        asyncio.run_coroutine_threadsafe(self.server.stop(), self.loop).result(timeout=10)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)


class FakeDaytona:
    """Minimal Daytona REST stub: error state -> recover succeeds."""

    def __init__(self) -> None:
        import http.server
        import threading

        self.requests: list[tuple[str, str]] = []
        self.state = "error"
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence
                pass

            def _send(self, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802
                outer.requests.append(("GET", self.path))
                self._send(
                    {
                        "id": "sbx-1",
                        "name": "mc-test",
                        "state": outer.state,
                        "desiredState": "started",
                        "errorReason": "image pull failed: 429 too many requests",
                        "recoverable": True,
                    }
                )

            def do_POST(self):  # noqa: N802
                outer.requests.append(("POST", self.path))
                if self.path.endswith("/recover"):
                    outer.state = "started"
                self._send({"id": "sbx-1", "state": outer.state})

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> int:
        self.thread.start()
        return self.port

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


def request(
    url: str, *, data: dict | None = None, cookie: str = "", accept: str = "application/json"
) -> tuple[int, dict, str]:
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body)
    if cookie:
        req.add_header("Cookie", cookie)
    if accept:
        req.add_header("Accept", accept)
    try:
        with _OPENER.open(req, timeout=20) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read().decode()


def wait_ready(base: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = request(base + "/healthz")
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def make_env(tmp: Path) -> dict:
    env = dict(os.environ)
    hash_out = subprocess.run(
        [PY, "scripts/hash_password.py", PASSWORD],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.strip().splitlines()[0].strip()   # first line only; the rest is help text
    # hash_password.py prints either a bare hash or VAR=hash — handle both.
    pw_hash = hash_out.split("=", 1)[1] if hash_out.startswith("ADMIN_PASSWORD_HASH=") else hash_out
    env.update(
        {
            "DAYHOST_ENV": "dev",
            "DAYHOST_ENV_FILE": str(tmp / "nonexistent.env"),  # ignore any real .env
            "ADMIN_PASSWORD_HASH": pw_hash,
            "SESSION_SECRET": "test-session-secret-" + "x" * 32,
            "DB_PATH": str(tmp / "dayhost.db"),
            "BACKUP_DIR": str(tmp / "backups"),
            "MC_HOST": "127.0.0.1",
            "MC_PORT": "1",          # nothing listening -> ping fails fast
            "MC_PING_TIMEOUT": "1",
            "RCON_PASSWORD": "",     # stub mode
            "SYNC_ENABLED": "false",
            "ALLOWED_HOSTS": "127.0.0.1,localhost",
        }
    )
    return env


def main() -> int:
    failures = 0
    tmp = Path(tempfile.mkdtemp(prefix="dayhost-http-test-"))
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = make_env(tmp)

    print(f"starting uvicorn on {base} (tmp={tmp})")
    proc = subprocess.Popen(
        [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port),
         "--loop", "asyncio", "--http", "h11", "--log-level", "warning"],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        if not wait_ready(base):
            print("server did not become ready")
            out = proc.stdout.read() if proc.stdout else ""
            print(out[-3000:])
            return 1

        # --- healthz -------------------------------------------------------
        status, _, body = request(base + "/healthz")
        failures += 0 if check("/healthz 200", status == 200, str(status)) else 1
        failures += 0 if check("/healthz ok:true", json.loads(body).get("ok") is True) else 1

        # --- unauthenticated / -> 303 --------------------------------------
        # A browser sends Accept: text/html; that is what triggers the redirect.
        status, headers, _ = request(base + "/", accept="text/html")
        failures += 0 if check(
            "/ without cookie -> 303 /login",
            status == 303 and headers.get("location") == "/login",
            f"{status} {headers.get('location')}",
        ) else 1

        # --- login ---------------------------------------------------------
        status, headers, _ = request(
            base + "/login", data={"username": "admin", "password": PASSWORD}
        )
        set_cookie = headers.get("set-cookie") or ""
        cookie = set_cookie.split(";", 1)[0]
        failures += 0 if check(
            "POST /login -> 303", status == 303, str(status)
        ) else 1
        failures += 0 if check("session cookie issued", "dayhost_session=" in cookie, cookie[:40]) else 1

        # --- dashboard -----------------------------------------------------
        status, _, body = request(base + "/", cookie=cookie, accept="text/html")
        failures += 0 if check("GET / with cookie -> 200", status == 200, str(status)) else 1
        failures += 0 if check("dashboard renders", "Sandbox" in body and "Console log" in body) else 1

        # --- status json ---------------------------------------------------
        status, _, body = request(base + "/api/status", cookie=cookie)
        payload = json.loads(body)
        failures += 0 if check("/api/status 200", status == 200, str(status)) else 1
        for key in ("sandbox_live", "sandbox_busy", "sandbox_error_reason",
                    "sandbox_recoverable", "sync_enabled"):
            failures += 0 if check(f"status has {key}", key in payload) else 1
        print(f"   daytona.state={payload['daytona'].get('state')} "
              f"recoverable={payload.get('sandbox_recoverable')} "
              f"sync_enabled={payload.get('sync_enabled')}")

        # --- console stub --------------------------------------------------
        status, _, body = request(
            base + "/api/console/send", data={"command": "list"}, cookie=cookie
        )
        payload = json.loads(body)
        failures += 0 if check("console stub -> 200", status == 200, str(status)) else 1
        failures += 0 if check("stub executed:false", payload.get("executed") is False) else 1
        failures += 0 if check("stub note explains why", "RCON_PASSWORD" in (payload.get("note") or ""),
                               str(payload.get("note"))) else 1

        # --- destructive refused ------------------------------------------
        status, _, body = request(
            base + "/api/console/send", data={"command": "stop"}, cookie=cookie
        )
        payload = json.loads(body)
        failures += 0 if check("destructive 'stop' -> 403", status == 403, str(status)) else 1
        failures += 0 if check("no traceback in body", "Traceback" not in body) else 1

        # --- unknown refused ----------------------------------------------
        status, _, body = request(
            base + "/api/console/send", data={"command": "lolwut nope"}, cookie=cookie
        )
        failures += 0 if check("unknown command -> 403", status == 403, str(status)) else 1

        # --- rate limit (needs 2 allowed cmds in quick succession) ---------
        time.sleep(1.2)  # clear any pending interval from the checks above
        s1, _, _ = request(base + "/api/console/send", data={"command": "list"}, cookie=cookie)
        s2, _, b2 = request(base + "/api/console/send", data={"command": "tps"}, cookie=cookie)
        failures += 0 if check(
            "first command allowed (200), immediate second rate-limited (429)",
            s1 == 200 and s2 == 429, f"{s1}/{s2} {b2[:80]}"
        ) else 1

        # --- dev docs on ---------------------------------------------------
        status, _, _ = request(base + "/api/docs")
        failures += 0 if check("dev: /api/docs 200", status == 200, str(status)) else 1

        # --- prod docs off (subprocess, ASGI probe) -------------------------
        prod_env = dict(env, DAYHOST_ENV="prod")
        probe = (
            "from app.main import app\n"
            "print('docs_url', app.docs_url)\n"
            "print('openapi_url', app.openapi_url)\n"
            "print('redoc_url', app.redoc_url)\n"
        )
        out = subprocess.run(
            [PY, "-c", probe], cwd=ROOT, env=prod_env, capture_output=True, text=True
        )
        text = out.stdout + out.stderr
        failures += 0 if check(
            "prod: docs_url/openapi_url/redoc_url all None",
            "docs_url None" in text and "openapi_url None" in text and "redoc_url None" in text,
            text.strip().replace("\n", " | "),
        ) else 1

        # --- prod live server returns 404 for docs ------------------------
        prod_port = free_port()
        prod_base = f"http://127.0.0.1:{prod_port}"
        prod_proc = subprocess.Popen(
            [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(prod_port),
             "--loop", "asyncio", "--http", "h11", "--log-level", "warning"],
            cwd=ROOT, env=prod_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            if wait_ready(prod_base):
                s_docs, h_docs, _ = request(prod_base + "/api/docs")
                failures += 0 if check(
                    "prod: /api/docs -> 404", s_docs == 404, str(s_docs)
                ) else 1
                s_open, _, _ = request(prod_base + "/api/openapi.json")
                failures += 0 if check(
                    "prod: /api/openapi.json -> 404", s_open == 404, str(s_open)
                ) else 1
                # secure cookie flag in prod
                s_login, h_login, _ = request(
                    prod_base + "/login", data={"username": "admin", "password": PASSWORD}
                )
                flag = (h_login.get("set-cookie") or "").lower()
                failures += 0 if check(
                    "prod: cookie is Secure + HttpOnly",
                    "secure" in flag and "httponly" in flag,
                    flag[:120],
                ) else 1
            else:
                print("FAIL prod server did not start")
                failures += 1
        finally:
            prod_proc.terminate()
            try:
                prod_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                prod_proc.kill()

        # --- RCON configured: real command through the router ---------------
        # Uses the fake Source RCON server from scripts/check_rcon.py on loopback.
        sys.path.insert(0, str(ROOT / "scripts"))
        from check_rcon import FakeRconServer  # noqa: E402

        fake = FakeRconServer(password="rcon-test-pw")
        fake_thread = ServerThread(fake)
        fake_port = fake_thread.start()
        rcon_port = free_port()
        rcon_env = dict(
            env,
            RCON_PASSWORD="rcon-test-pw",
            RCON_HOST="127.0.0.1",
            RCON_PORT=str(fake_port),
            RCON_MIN_INTERVAL="0",   # isolate the RCON path from rate limiting
        )
        print(f"\nfake RCON on 127.0.0.1:{fake_port}; DayHost on 127.0.0.1:{rcon_port}")
        rcon_proc = subprocess.Popen(
            [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(rcon_port),
             "--loop", "asyncio", "--http", "h11", "--log-level", "warning"],
            cwd=ROOT, env=rcon_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        rbase = f"http://127.0.0.1:{rcon_port}"
        try:
            if not wait_ready(rbase):
                print("FAIL rcon-enabled server did not start")
                failures += 1
            else:
                _, headers, _ = request(
                    rbase + "/login", data={"username": "admin", "password": PASSWORD}
                )
                rcookie = (headers.get("set-cookie") or "").split(";", 1)[0]

                status, _, body = request(
                    rbase + "/api/console/send", data={"command": "list"}, cookie=rcookie
                )
                payload = json.loads(body)
                failures += 0 if check("RCON command -> 200", status == 200, str(status)) else 1
                failures += 0 if check("executed == true", payload.get("executed") is True) else 1
                failures += 0 if check(
                    "real RCON response returned",
                    "Alice, Bob" in (payload.get("response") or ""),
                    repr(payload.get("response")),
                ) else 1
                failures += 0 if check(
                    "fake server received the command", fake.commands == ["list"], str(fake.commands)
                ) else 1

                status, _, body = request(
                    rbase + "/api/console/send", data={"command": "op Steve"}, cookie=rcookie
                )
                failures += 0 if check(
                    "destructive 'op Steve' -> 403 even with RCON live", status == 403, str(status)
                ) else 1
                failures += 0 if check(
                    "blocked command never reached RCON",
                    fake.commands == ["list"], str(fake.commands)
                ) else 1
        finally:
            rcon_proc.terminate()
            try:
                rcon_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                rcon_proc.kill()
            fake_thread.stop()

        # --- errorReason + recover (fake Daytona API) ------------------------
        daytona = FakeDaytona()
        d_port = daytona.start()
        dh_port = free_port()
        d_env = dict(
            env,
            DAYTONA_API_KEY="test-key",
            SANDBOX_ID="sbx-1",
            DAYTONA_API_BASE=f"http://127.0.0.1:{d_port}",
            DAYTONA_MAX_RETRIES="0",
            DAYTONA_TIMEOUT="5",
        )
        print(f"\nfake Daytona on 127.0.0.1:{d_port}; DayHost on 127.0.0.1:{dh_port}")
        d_proc = subprocess.Popen(
            [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(dh_port),
             "--loop", "asyncio", "--http", "h11", "--log-level", "warning"],
            cwd=ROOT, env=d_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        dbase = f"http://127.0.0.1:{dh_port}"
        try:
            if not wait_ready(dbase):
                print("FAIL daytona-enabled server did not start")
                failures += 1
            else:
                _, headers, _ = request(
                    dbase + "/login", data={"username": "admin", "password": PASSWORD}
                )
                dcookie = (headers.get("set-cookie") or "").split(";", 1)[0]

                status, _, body = request(dbase + "/api/status", cookie=dcookie)
                payload = json.loads(body)
                failures += 0 if check(
                    "state mapped to ERROR", payload["daytona"]["state"] == "ERROR",
                    payload["daytona"]["state"],
                ) else 1
                failures += 0 if check(
                    "errorReason surfaced in /api/status",
                    payload.get("sandbox_error_reason") == "image pull failed: 429 too many requests",
                    repr(payload.get("sandbox_error_reason")),
                ) else 1
                failures += 0 if check("recoverable surfaced", payload.get("sandbox_recoverable") is True) else 1
                failures += 0 if check(
                    "desiredState surfaced", payload.get("sandbox_desired_state") == "started",
                    repr(payload.get("sandbox_desired_state")),
                ) else 1
                failures += 0 if check("not live while faulted", payload.get("sandbox_live") is False) else 1

                status, _, html = request(dbase + "/partials/status", cookie=dcookie, accept="text/html")
                failures += 0 if check("partial shows the real reason",
                                       "image pull failed: 429" in html) else 1
                failures += 0 if check("partial offers the Recover button",
                                       'hx-post="/power/recover"' in html) else 1

                status, _, body = request(
                    dbase + "/power/recover", data={}, cookie=dcookie, accept="text/html"
                )
                failures += 0 if check("POST /power/recover -> 200", status == 200, str(status)) else 1
                failures += 0 if check(
                    "recover hit the right endpoint",
                    ("POST", "/sandbox/sbx-1/recover") in daytona.requests,
                    str(daytona.requests),
                ) else 1
                status, _, html = request(dbase + "/partials/status", cookie=dcookie, accept="text/html")
                failures += 0 if check(
                    "after recover the sandbox is Online", "Online" in html and "Faulted" not in html
                ) else 1

                status, _, body = request(dbase + "/api/daytona/recover", data={}, cookie=dcookie)
                failures += 0 if check("POST /api/daytona/recover 200", status == 200, str(status)) else 1

                # --- power OFF with SYNC_ENABLED=false: plan only, no exec ------
                before = len(daytona.requests)
                status, _, html = request(
                    dbase + "/power/off", data={}, cookie=dcookie, accept="text/html"
                )
                failures += 0 if check("power/off 200 with sync disabled", status == 200, str(status)) else 1
                failures += 0 if check(
                    "power/off still called Daytona stop",
                    any(
                        m == "POST" and p.endswith("/stop")
                        for m, p in daytona.requests[before:]
                    ),
                    str(daytona.requests[before:]),
                ) else 1
        finally:
            d_proc.terminate()
            try:
                d_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                d_proc.kill()
            daytona.stop()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL HTTP CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
