"""Unified Daytona driver — lifecycle **and** in-sandbox process execution (Fase 4 / A).

Two Daytona surfaces are glued together here, and they must never be mixed:

    lifecycle : {DAYTONA_API_BASE}                        daytona_client.py
                GET  /sandbox/{id}                       (status)
                POST /sandbox/{id}/start|stop|recover
    toolbox   : {DAYTONA_TOOLBOX_URL or discovered}       this module
                POST /process/session/{sid}/exec         ExecuteCommand (preferred)
                POST /process/exec                       stateless fallback
                POST /process/session                    CreateSession
                GET  /process/session/{sid}              GetSession
                GET  /process/session/{sid}/command/{cid}
                GET  /process/{name}/status
                GET  /process/{name}/logs

Ground truth: docs/daytona-api.md and docs/daytona-file-api.md (API v0.225).
Every path is env-overridable (`DAYTONA_TOOLBOX_PATH_*`, `DAYTONA_PROCESS_EXEC_PATH`)
so a wrong assumption can be corrected without a redeploy.

Design rules
------------
* **Nothing raises into the request path.** Every public coroutine returns a
  normalised dict; httpx failures, bad JSON, timeouts and 401/403 all become
  `{"ok": False, ...}` with an actionable `error`.
* **No duplicated state map.** `normalise_state`, `LIVE_STATES` and the
  22-value `_STATE_MAP` live in `daytona_client.py`; this module imports the
  lifecycle client and subclasses it rather than copying any of that.
* **No duplicated toolbox plumbing.** The base-URL join and the 401 message
  come from `daytona_files.py` (single source of truth).
* `httpx` is imported **lazily** — an idle process pays nothing for it.
* The toolbox base is discovered from `/sandbox/{id}/toolbox-proxy-url`
  **once** and cached for the life of the client.

`ok` vs `exit_code`
-------------------
`ok` means *the request itself worked* (HTTP 2xx, command dispatched). It does
**not** mean the command succeeded — that is what `exit_code` says. A probe
such as `pgrep` legitimately returns exit code 1 and `ok == True`.
`success` is provided as the convenience `exit_code == 0` flag.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from ..config import Settings, get_settings
from .daytona_client import (
    STATE_ERROR,
    STATE_RUNNING,
    DaytonaClient,
    normalise_state,
)
from .daytona_files import AUTH_REJECTED, DaytonaFilesClient

if TYPE_CHECKING:  # pragma: no cover - typing only
    import httpx

log = logging.getLogger("dayhost.daytona.driver")

MAX_BODY = 2048
# Used only when DAYTONA_TOOLBOX_SESSION_ID is empty.
DEFAULT_SESSION_ID = "dayhost"
# Probe that decides whether an agent is already alive. `pgrep` exits 1 when
# nothing matches, which is a perfectly valid (non-error) answer here.
DEFAULT_PROBE_CMD = "pgrep -f agent.py"

_EXIT_CODE_KEYS = ("exitCode", "exit_code", "exitcode", "code")
_STDOUT_KEYS = ("stdout", "output", "result")
_STDERR_KEYS = ("stderr", "err")


# --------------------------------------------------------------------------- #
# small pure helpers
# --------------------------------------------------------------------------- #
def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _payload_nodes(payload: Any) -> list[dict[str, Any]]:
    """Return the payload and its likely nested `result`/`data` objects."""
    if not isinstance(payload, dict):
        return []
    nodes = [payload]
    for key in ("result", "data"):
        inner = payload.get(key)
        if isinstance(inner, dict):
            nodes.append(inner)
    return nodes


def _extract_exec(payload: Any, text: str) -> tuple[str, str, int | None]:
    """Best-effort (stdout, stderr, exit_code) from an unknown exec shape.

    Documented shapes differ between `/process/exec` and the session variant,
    and the session variant may answer with a `cmdId` instead of output, so
    every plausible spelling is accepted. Never raises.
    """
    if isinstance(payload, str):
        return payload, "", None
    nodes = _payload_nodes(payload)
    if not nodes:
        return (text or ""), "", None

    stdout = ""
    stderr = ""
    exit_code: int | None = None

    for node in nodes:
        if not stdout:
            for key in _STDOUT_KEYS:
                value = node.get(key)
                if isinstance(value, str) and value:
                    stdout = value
                    break
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    stdout = str(value)
                    break
        if not stderr:
            for key in _STDERR_KEYS:
                value = node.get(key)
                if isinstance(value, str) and value:
                    stderr = value
                    break
        if exit_code is None:
            for key in _EXIT_CODE_KEYS:
                if key in node:
                    exit_code = _as_int(node[key])
                    if exit_code is not None:
                        break
        # Nested `error` is only stderr-ish when it is a plain string and we
        # have no stderr yet; an error dict is a transport failure, handled by
        # the caller.
        if not stderr:
            outer_error = node.get("error")
            if isinstance(outer_error, str) and outer_error:
                stderr = outer_error

    if not stdout and text and not nodes[0].get("cmdId"):
        stdout = text
    return stdout, stderr, exit_code


def _looks_running(payload: Any) -> bool | None:
    """Interpret a /process/{name}/status body. None = cannot tell."""
    nodes = _payload_nodes(payload)
    if not nodes:
        return None
    for node in nodes:
        for key in ("running", "alive", "isRunning", "is_running"):
            value = node.get(key)
            if isinstance(value, bool):
                return value
        for key in ("state", "status", "phase"):
            value = node.get(key)
            if isinstance(value, str):
                text = value.strip().lower()
                if text in {"running", "started", "active", "alive"}:
                    return True
                if text in {"exited", "stopped", "dead", "finished", "failed", "error"}:
                    return False
        if "pid" in node and _as_int(node.get("pid")):
            return True
    return None


def _result(
    *,
    ok: bool,
    error: str | None = None,
    kind: str = "http",
    status_code: int | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": ok,
        "kind": kind,
        "status_code": status_code,
        "error": error,
    }
    payload.update(extra)
    return payload


# --------------------------------------------------------------------------- #
# lifecycle: reuse DaytonaClient, extend stop() with `force`
# --------------------------------------------------------------------------- #
class _LifecycleClient(DaytonaClient):
    """`DaytonaClient` plus `stop(force=True)` (query `force=true`).

    Subclassing rather than copying keeps the 22-value state map, the retry
    policy and the detail extraction in exactly one place.
    """

    async def stop(self, force: bool = False) -> dict[str, Any]:  # type: ignore[override]
        if not force:
            return await super().stop()
        return await self._request(
            "POST",
            self.settings.daytona_path_stop,
            params={"force": "true"},
        )


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #
class DaytonaDriver:
    """One driver per process. Async, concurrency-safe, never raises."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._lifecycle = _LifecycleClient(self.settings)
        self._client: "httpx.AsyncClient | None" = None
        self._toolbox_base: str | None = None
        self._session_ready = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------- config
    @property
    def configured(self) -> bool:
        return bool(self.settings.daytona_api_key and self.settings.sandbox_id)

    @property
    def auth_mode(self) -> str:
        mode = (self.settings.daytona_toolbox_auth or "bearer").strip().lower()
        return mode if mode in {"bearer", "none"} else "invalid"

    @property
    def exec_mode(self) -> str:
        """`session` (preferred) or `stateless` (`/process/exec`)."""
        mode = (self.settings.daytona_toolbox_exec_mode or "session").strip().lower()
        if mode not in {"session", "stateless"}:
            log.debug("invalid DAYTONA_TOOLBOX_EXEC_MODE=%r, falling back", mode)
            mode = "session"
        if not (self.settings.daytona_toolbox_path_exec or "").strip():
            # An empty template is an explicit instruction to use the
            # stateless endpoint (cheap escape hatch, no redeploy).
            mode = "stateless"
        return mode

    def _unconfigured(self, action: str = "exec") -> dict[str, Any]:
        if not self.settings.daytona_api_key:
            why = "DAYTONA_API_KEY is empty"
        elif not self.settings.sandbox_id:
            why = "SANDBOX_ID is empty"
        else:
            why = "unknown configuration error"
        return _result(
            ok=False,
            kind="config",
            error=f"daytona driver not configured: {why}",
            action=action,
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.auth_mode == "bearer":
            headers["Authorization"] = f"Bearer {self.settings.daytona_api_key}"
        elif self.auth_mode == "invalid":
            log.debug(
                "DAYTONA_TOOLBOX_AUTH=%r is invalid; no Authorization header will be sent",
                self.settings.daytona_toolbox_auth,
            )
        return headers

    # ------------------------------------------------------------- plumbing
    async def _http(self) -> "httpx.AsyncClient":
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(float(self.settings.daytona_timeout)),
                headers=self._headers(),
                follow_redirects=False,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        await self._lifecycle.aclose()

    async def _discover_base(self) -> str | None:
        """One GET against the lifecycle API; result is cached by the caller."""
        import httpx

        api_base = self.settings.daytona_api_base.rstrip("/")
        url = f"{api_base}/sandbox/{self.settings.sandbox_id}/toolbox-proxy-url"
        headers = {"Accept": "application/json"}
        if self.settings.daytona_api_key:
            headers["Authorization"] = f"Bearer {self.settings.daytona_api_key}"
        try:
            async with httpx.AsyncClient(timeout=self.settings.daytona_timeout) as client:
                resp = await client.get(url, headers=headers)
            log.debug(
                "driver toolbox-proxy-url GET %s -> %s body=%r",
                url, resp.status_code, resp.text[:MAX_BODY],
            )
            if resp.status_code != 200:
                return None
            payload = resp.json()
            base = payload.get("url") if isinstance(payload, dict) else None
            if not base:
                return None
            joined = DaytonaFilesClient._join_base(str(base), self.settings.sandbox_id)
            log.info("driver toolbox base resolved once and cached: %s", joined)
            return joined
        except Exception as exc:  # never raise into the request path
            log.debug(
                "driver toolbox-proxy-url discovery failed: %s: %s", type(exc).__name__, exc
            )
            return None

    async def toolbox_base(self) -> str | None:
        """Per-sandbox toolbox base. Env override wins; else discovered once."""
        if self._toolbox_base is not None:
            return self._toolbox_base
        if not self.settings.sandbox_id or self.auth_mode == "invalid":
            return None
        async with self._lock:
            if self._toolbox_base is not None:  # another coroutine won the race
                return self._toolbox_base
            if self.settings.daytona_toolbox_url:
                self._toolbox_base = DaytonaFilesClient._join_base(
                    self.settings.daytona_toolbox_url, self.settings.sandbox_id
                )
            else:
                self._toolbox_base = await self._discover_base()
            return self._toolbox_base

    def _classify(self, status: int, error_body: str) -> dict[str, Any]:
        if status in (401, 403):
            return _result(
                ok=False,
                kind="auth",
                status_code=status,
                error=AUTH_REJECTED,
                detail=error_body[:MAX_BODY],
            )
        return _result(
            ok=False,
            kind="http",
            status_code=status,
            error=f"HTTP {status}",
            detail=error_body[:MAX_BODY],
        )

    def _attempts(self) -> int:
        return max(1, int(self.settings.daytona_max_retries) + 1)

    @staticmethod
    async def _backoff(attempt: int) -> None:
        await asyncio.sleep(min(2 ** attempt * 0.25, 2.0))

    @staticmethod
    def _retryable(status: int | None) -> bool:
        return status is None or status == 429 or status >= 500

    async def _toolbox_call(
        self,
        method: str,
        endpoint: str,
        *,
        label: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """One toolbox call with light retries. Returns a normalised dict."""
        base = await self.toolbox_base()
        if base is None:
            return _result(
                ok=False,
                kind="config",
                error=(
                    "daytona toolbox base unavailable: SANDBOX_ID/DAYTONA_TOOLBOX_URL "
                    "missing or /toolbox-proxy-url discovery failed"
                ),
                path=label,
            )

        import httpx

        attempts = self._attempts()
        request_timeout = httpx.Timeout(
            max(float(timeout or self.settings.daytona_timeout), float(self.settings.daytona_timeout))
        )
        last = _result(ok=False, kind="transport", error="no attempt made", path=label)

        for attempt in range(1, attempts + 1):
            try:
                client = await self._http()
                resp = await client.request(
                    method,
                    base + endpoint,
                    params=params,
                    json=json_body,
                    timeout=request_timeout,
                )
                body = resp.text or ""
                log.debug(
                    "driver toolbox %s %s %s -> %s body=%r",
                    method, endpoint, params or json_body, resp.status_code, body[:MAX_BODY],
                )
                if resp.status_code >= 400:
                    last = self._classify(resp.status_code, body)
                    last["path"] = label
                    if self._retryable(resp.status_code) and attempt < attempts:
                        await self._backoff(attempt)
                        continue
                    return last
                payload: Any
                try:
                    payload = resp.json()
                except Exception:
                    payload = None
                return _result(
                    ok=True,
                    status_code=resp.status_code,
                    path=label,
                    data=payload,
                    text=body,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = _result(
                    ok=False,
                    kind="transport",
                    error=f"{type(exc).__name__}: {exc}",
                    path=label,
                )
                log.debug("driver toolbox %s %s transport error: %s", method, endpoint, last["error"])
                if attempt < attempts:
                    await self._backoff(attempt)
                    continue
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("driver toolbox %s %s unexpected: %s", method, endpoint, exc)
                return _result(
                    ok=False,
                    kind="transport",
                    error=f"{type(exc).__name__}: {exc}",
                    path=label,
                )
        return last

    # ------------------------------------------------------------- lifecycle
    async def status(self) -> dict[str, Any]:
        result = await self._lifecycle.get_status()
        result["action"] = "status"
        return result

    async def start(
        self, *, wait: bool = True, timeout: float | None = None
    ) -> dict[str, Any]:
        """POST start, then poll `GET /sandbox/{id}` until it is live.

        `DAYTONA_START_TIMEOUT` / `DAYTONA_POLL_INTERVAL` bound the wait, so a
        sandbox that never boots returns `ok=False` with the last known state
        instead of hanging the request.
        """
        result = dict(await self._lifecycle.start())
        result["action"] = "start"
        if not result.get("ok") or not wait:
            return result

        limit = float(timeout if timeout is not None else self.settings.daytona_start_timeout)
        interval = max(0.05, float(self.settings.daytona_poll_interval))
        deadline = time.monotonic() + limit
        state = normalise_state(result.get("state"))
        polls = 0

        # Wait for RUNNING specifically, not for the LIVE set: STARTING is
        # "live" for the dashboard (no flapping mid-boot) but a start() call
        # has not finished until the sandbox is actually running.
        while state not in (STATE_RUNNING, STATE_ERROR):
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(interval)
            polls += 1
            probe = await self._lifecycle.get_status()
            state = normalise_state(probe.get("state"))
            for key in ("error_reason", "recoverable", "desired_state", "sandbox_name"):
                if probe.get(key) is not None:
                    result[key] = probe.get(key)
            if not probe.get("ok"):
                result["error"] = probe.get("error") or result.get("error")
                break

        result["state"] = state
        result["polls"] = polls
        result["waited"] = round(limit - max(0.0, deadline - time.monotonic()), 2)
        if state == STATE_ERROR:
            result["ok"] = False
            result["error"] = result.get("error_reason") or "sandbox entered the error state"
        elif state != STATE_RUNNING:
            result["ok"] = False
            result["error"] = (
                f"sandbox did not become live within {limit:g}s "
                f"(last state={state})"
            )
        return result

    async def stop(self, force: bool = False) -> dict[str, Any]:
        result = await self._lifecycle.stop(force=force)
        result["action"] = "stop"
        result["force"] = bool(force)
        return result

    async def recover(self) -> dict[str, Any]:
        result = await self._lifecycle.recover()
        result["action"] = "recover"
        return result

    # ------------------------------------------------------------- sessions
    def _session_id(self, session_id: str | None = None) -> str:
        return (
            session_id
            or (self.settings.daytona_toolbox_session_id or "").strip()
            or DEFAULT_SESSION_ID
        )

    def _command_path(self, session_id: str, command_id: str) -> str:
        template = (self.settings.daytona_toolbox_path_command or "").strip()
        if template:
            return template.format(session_id=session_id, command_id=command_id)
        base = (self.settings.daytona_toolbox_path_session or "/process/session").rstrip("/")
        return f"{base}/{session_id}/command/{command_id}"

    async def _ensure_session(self, session_id: str) -> dict[str, Any]:
        """GET the session, create it on 404. Cached after the first success."""
        if self._session_ready:
            return _result(ok=True, status_code=None, session_id=session_id, cached=True)

        get_path = (self.settings.daytona_toolbox_path_session_get or "").strip()
        if get_path:
            probe = await self._toolbox_call(
                "GET",
                get_path.format(session_id=session_id),
                label=f"session:{session_id}",
            )
            if probe.get("ok"):
                self._session_ready = True
                return probe
            if probe.get("kind") in {"auth", "config"}:
                return probe
            log.debug(
                "driver session %s not usable (%s); trying create",
                session_id, probe.get("error"),
            )

        create = await self._toolbox_call(
            "POST",
            (self.settings.daytona_toolbox_path_session or "/process/session"),
            label=f"session-create:{session_id}",
            json_body={"sessionId": session_id},
        )
        if create.get("ok") or create.get("status_code") in (409,):
            self._session_ready = True
            create["ok"] = True
            create["session_id"] = session_id
            return create
        # Do not block the exec attempt: some deployments auto-create the
        # session on first use. The exec call will report a real error if not.
        log.debug("driver session create failed: %s", create.get("error"))
        return create

    # ------------------------------------------------------------- exec
    async def _poll_command(self, session_id: str, command_id: str, timeout: float) -> dict[str, Any]:
        """Poll `/process/session/{sid}/command/{cid}` until it reports output."""
        interval = max(0.05, float(self.settings.daytona_poll_interval))
        deadline = time.monotonic() + max(0.1, float(timeout))
        last: dict[str, Any] = _result(
            ok=False, kind="timeout", error="command did not report a result"
        )
        while time.monotonic() < deadline:
            await asyncio.sleep(interval)
            polled = await self._toolbox_call(
                "GET",
                self._command_path(session_id, command_id),
                label=f"command:{command_id}",
                timeout=max(5.0, interval * 4),
            )
            if polled.get("ok"):
                payload = polled.get("data")
                stdout, stderr, code = _extract_exec(payload, polled.get("text") or "")
                if code is not None or stdout or stderr:
                    polled.update(
                        stdout=stdout, stderr=stderr, exit_code=code,
                        success=(code == 0), command_id=command_id,
                    )
                    return polled
                last = polled
            elif polled.get("kind") in {"auth", "config"}:
                return polled
            last = polled
            if polled.get("status_code") == 404:
                # Endpoint not implemented by this deployment: keep the
                # dispatch ack rather than waiting for a result that will
                # never come.
                return _result(
                    ok=False,
                    kind="unsupported",
                    status_code=404,
                    error="command polling endpoint returned 404",
                    command_id=command_id,
                )
        last["error"] = "command did not finish within the timeout"
        last["command_id"] = command_id
        return last

    async def exec_command(
        self,
        command: str,
        timeout: float = 60,
        *,
        session_id: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """Run `command` inside the sandbox. Never raises.

        Returns `{ok, status_code, stdout, stderr, exit_code, error}` plus
        `success` (exit_code == 0), `mode`, `session_id`, `command`, `data`.
        """
        empty = {
            "ok": False,
            "status_code": None,
            "stdout": "",
            "stderr": "",
            "exit_code": None,
            "error": None,
            "success": False,
            "command": command,
            "mode": None,
            "session_id": None,
            "data": None,
        }
        if not self.configured:
            return {**empty, **self._unconfigured("exec")}
        if not isinstance(command, str) or not command.strip():
            return {
                **empty,
                "kind": "invalid",
                "error": "exec_command requires a non-empty command string",
            }

        try:
            chosen = (mode or self.exec_mode).strip().lower()
            if chosen not in {"session", "stateless"}:
                chosen = self.exec_mode
            if chosen == "session" and not (self.settings.daytona_toolbox_path_exec or "").strip():
                chosen = "stateless"
            empty["mode"] = chosen
            empty["kind"] = "http"

            if chosen == "stateless":
                endpoint = self.settings.daytona_process_exec_path or "/process/exec"
                label = f"exec:{endpoint}"
                raw = await self._toolbox_call(
                    "POST", endpoint, label=label,
                    json_body={"command": command}, timeout=timeout,
                )
            else:
                sid = self._session_id(session_id)
                empty["session_id"] = sid
                session = await self._ensure_session(sid)
                if not session.get("ok") and session.get("kind") in {"auth", "config"}:
                    return {**empty, **{k: session.get(k) for k in ("kind", "status_code", "error", "detail")}}
                endpoint = (self.settings.daytona_toolbox_path_exec).format(session_id=sid)
                label = f"exec-session:{sid}"
                raw = await self._toolbox_call(
                    "POST", endpoint, label=label,
                    json_body={"command": command}, timeout=timeout,
                )

            if not raw.get("ok"):
                return {
                    **empty,
                    "kind": raw.get("kind", "http"),
                    "status_code": raw.get("status_code"),
                    "error": raw.get("error"),
                    "detail": raw.get("detail"),
                    "data": raw.get("data"),
                }

            payload = raw.get("data")
            stdout, stderr, exit_code = _extract_exec(payload, raw.get("text") or "")

            command_id = None
            if isinstance(payload, dict):
                command_id = payload.get("cmdId") or payload.get("commandId") or payload.get("id")
            if command_id and exit_code is None and not stdout and not stderr:
                polled = await self._poll_command(
                    self._session_id(session_id), str(command_id), timeout
                )
                if polled.get("ok"):
                    stdout = polled.get("stdout", "")
                    stderr = polled.get("stderr", "")
                    exit_code = polled.get("exit_code")
                    payload = polled.get("data")
                else:
                    # Dispatch worked but no result yet. That is not a
                    # transport failure — the caller decides what to do.
                    return {
                        **empty,
                        "ok": True,
                        "kind": "async",
                        "status_code": raw.get("status_code"),
                        "stdout": "",
                        "stderr": "",
                        "exit_code": None,
                        "success": False,
                        "error": None,
                        "command_id": str(command_id),
                        "data": payload,
                        "note": polled.get("error") or "command dispatched; result not available yet",
                    }

            result = {
                **empty,
                "ok": True,
                "kind": "exec",
                "status_code": raw.get("status_code"),
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": exit_code,
                "success": exit_code == 0,
                "data": payload,
            }
            if command_id:
                result["command_id"] = str(command_id)
            return result
        except Exception as exc:  # pragma: no cover - last-resort guard
            log.debug("exec_command unexpected failure: %s", exc)
            return {**empty, "kind": "internal", "error": f"{type(exc).__name__}: {exc}"}

    # ------------------------------------------------------------- process
    async def process_status(self, name: str = "agent") -> dict[str, Any]:
        """GET /process/{name}/status — is a launched process still alive?"""
        if not self.configured:
            return {**self._unconfigured("process_status"), "name": name, "running": None}
        template = self.settings.daytona_toolbox_path_process_status or "/process/{name}/status"
        raw = await self._toolbox_call(
            "GET", template.format(name=name), label=f"process-status:{name}"
        )
        if not raw.get("ok"):
            return {
                "ok": False,
                "kind": raw.get("kind", "http"),
                "status_code": raw.get("status_code"),
                "name": name,
                "running": None,
                "exit_code": None,
                "error": raw.get("error"),
                "data": raw.get("data"),
            }
        payload = raw.get("data")
        nodes = _payload_nodes(payload)
        exit_code = None
        for node in nodes:
            for key in _EXIT_CODE_KEYS:
                if key in node:
                    exit_code = _as_int(node.get(key))
                    if exit_code is not None:
                        break
            if exit_code is not None:
                break
        return {
            "ok": True,
            "kind": "process",
            "status_code": raw.get("status_code"),
            "name": name,
            "running": _looks_running(payload),
            "exit_code": exit_code,
            "error": None,
            "data": payload,
        }

    async def process_logs(self, name: str = "agent") -> dict[str, Any]:
        """GET /process/{name}/logs — bounded by the caller, not accumulated here."""
        if not self.configured:
            return {**self._unconfigured("process_logs"), "name": name, "output": ""}
        template = self.settings.daytona_toolbox_path_process_logs or "/process/{name}/logs"
        raw = await self._toolbox_call(
            "GET", template.format(name=name), label=f"process-logs:{name}"
        )
        if not raw.get("ok"):
            return {
                "ok": False,
                "kind": raw.get("kind", "http"),
                "status_code": raw.get("status_code"),
                "name": name,
                "output": "",
                "error": raw.get("error"),
            }
        payload = raw.get("data")
        output = ""
        if isinstance(payload, dict):
            for key in ("logs", "output", "result", "stdout"):
                value = payload.get(key)
                if isinstance(value, str):
                    output = value
                    break
        elif isinstance(payload, str):
            output = payload
        if not output:
            output = raw.get("text") or ""
        return {
            "ok": True,
            "kind": "process",
            "status_code": raw.get("status_code"),
            "name": name,
            "output": output,
            "error": None,
            "data": payload,
        }

    # ------------------------------------------------------------- agent
    async def ensure_agent_running(
        self,
        *,
        remote_dir: str | None = None,
        launch_cmd: str | None = None,
        name: str = "agent",
        timeout: float = 30,
        probe: str | None = None,
    ) -> dict[str, Any]:
        """Start the agent if (and only if) it is not already running.

        Idempotent by construction: a process-status hit or a `pgrep` hit
        short-circuits before anything is launched, so calling this on every
        dashboard poll cannot spawn a second agent.

        Liveness is decided in this order:
          1. `GET /process/{name}/status` (cheap, authoritative when present)
          2. `pgrep -f agent.py` through exec (works without the process API)
          3. only then launch.
        """
        directory = remote_dir if remote_dir is not None else self.settings.agent_remote_dir
        template = launch_cmd if launch_cmd is not None else self.settings.agent_launch_cmd
        probe_cmd = probe or DEFAULT_PROBE_CMD

        base = {
            "ok": False,
            "launched": False,
            "alive_before": None,
            "alive_after": None,
            "remote_dir": directory,
            "command": None,
            "probe": probe_cmd,
            "mode": self.exec_mode,
            "error": None,
        }

        if not self.configured:
            return {**base, **self._unconfigured("ensure_agent")}

        try:
            command = template.format(dir=directory)
        except (KeyError, IndexError):
            command = template
        base["command"] = command

        try:
            # 1. process-level status -------------------------------------
            status = await self.process_status(name)
            if status.get("ok") and status.get("running") is True:
                return {
                    **base,
                    "ok": True,
                    "alive_before": True,
                    "alive_after": True,
                    "checked": "process_status",
                }

            # 2. pgrep probe ---------------------------------------------
            probed = await self.exec_command(probe_cmd, timeout=min(float(timeout), 30))
            if probed.get("kind") in {"auth", "config"}:
                return {**base, **probed, "launched": False}
            alive = bool(
                probed.get("ok")
                and probed.get("exit_code") == 0
                and (probed.get("stdout") or "").strip()
            )
            if alive:
                return {
                    **base,
                    "ok": True,
                    "alive_before": True,
                    "alive_after": True,
                    "checked": "pgrep",
                }

            # 3. launch ---------------------------------------------------
            launch = await self.exec_command(command, timeout=float(timeout))
            base["launch"] = {
                "ok": launch.get("ok"),
                "status_code": launch.get("status_code"),
                "stdout": launch.get("stdout"),
                "stderr": launch.get("stderr"),
                "exit_code": launch.get("exit_code"),
                "error": launch.get("error"),
            }
            if not launch.get("ok"):
                return {
                    **base,
                    "alive_before": False,
                    "error": launch.get("error") or "launch command failed",
                    "kind": launch.get("kind", "http"),
                    "status_code": launch.get("status_code"),
                }

            # 4. one verification probe (never a second launch) -----------
            after = await self.exec_command(probe_cmd, timeout=min(float(timeout), 15))
            alive_after = bool(
                after.get("ok")
                and after.get("exit_code") == 0
                and (after.get("stdout") or "").strip()
            )
            return {
                **base,
                "ok": True,
                "launched": True,
                "alive_before": False,
                "alive_after": alive_after,
                "checked": "pgrep",
            }
        except Exception as exc:  # pragma: no cover - last-resort guard
            log.debug("ensure_agent_running unexpected failure: %s", exc)
            return {**base, "kind": "internal", "error": f"{type(exc).__name__}: {exc}"}


_driver: DaytonaDriver | None = None


def get_driver() -> DaytonaDriver:
    global _driver
    if _driver is None:
        _driver = DaytonaDriver()
    return _driver


async def close_driver() -> None:
    global _driver
    if _driver is not None:
        await _driver.aclose()
        _driver = None


__all__ = [
    "DEFAULT_PROBE_CMD",
    "DEFAULT_SESSION_ID",
    "DaytonaDriver",
    "close_driver",
    "get_driver",
]
