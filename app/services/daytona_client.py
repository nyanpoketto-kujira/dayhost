"""Daytona REST client — async, defensive, dependency-light (httpx only).

VERIFIED ENDPOINTS (Fase 2 — docs/daytona-api.md, official API v0.225)
----------------------------------------------------------------------
    GET  {DAYTONA_API_BASE}/sandbox/{sandbox_idOrName}          -> status/info
    POST {DAYTONA_API_BASE}/sandbox/{sandbox_idOrName}/start    -> start/resume/restore
    POST {DAYTONA_API_BASE}/sandbox/{sandbox_idOrName}/stop     -> stop (?force=true = SIGKILL)
    POST {DAYTONA_API_BASE}/sandbox/{sandbox_idOrName}/recover  -> recover from `error`

There is **no restart endpoint**; restart is stop-then-start (kept as is).
Paths are still env-overridable (`DAYTONA_PATH_*`) as a cheap escape hatch.
Auth: `Authorization: Bearer <DAYTONA_API_KEY>` (verified).

DESIGN RULE: a wrong endpoint or an unexpected payload must NEVER raise into
the request path. Raw status + body are logged at DEBUG; callers receive a
normalised dict with `state` mapped through a fallback of `UNKNOWN`.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from ..config import Settings, get_settings

if TYPE_CHECKING:  # httpx is imported lazily at runtime to keep idle RSS low
    import httpx

log = logging.getLogger("dayhost.daytona")

# --- state normalisation ------------------------------------------------------
STATE_UNKNOWN = "UNKNOWN"
STATE_RUNNING = "RUNNING"
STATE_STARTING = "STARTING"
STATE_STOPPED = "STOPPED"
STATE_STOPPING = "STOPPING"
STATE_ARCHIVED = "ARCHIVED"
STATE_ERROR = "ERROR"
STATE_BUSY = "BUSY"

# Daytona `SandboxState` enum -> DayHost canonical state.
# Ground truth: 22 values, see docs/daytona-api.md ("Recommended DayHost state
# mapping"). Every one of the 22 is listed explicitly so the unit test in
# scripts/check_state_map.py can assert none falls through by accident.
_STATE_MAP: dict[str, str] = {
    # --- the 22 official SandboxState values --------------------------------
    "creating": STATE_STARTING,
    "restoring": STATE_STARTING,
    "destroyed": STATE_UNKNOWN,          # gone, not a transition (doc allows BUSY/UNKNOWN)
    "destroying": STATE_STOPPING,
    "started": STATE_RUNNING,
    "stopped": STATE_STOPPED,
    "starting": STATE_STARTING,
    "stopping": STATE_STOPPING,
    "error": STATE_ERROR,
    "build_failed": STATE_ERROR,
    "pending_build": STATE_BUSY,
    "building_snapshot": STATE_BUSY,
    "unknown": STATE_UNKNOWN,
    "pulling_snapshot": STATE_STARTING,
    "archived": STATE_ARCHIVED,
    "archiving": STATE_ARCHIVED,
    "resizing": STATE_BUSY,
    "snapshotting": STATE_BUSY,
    "forking": STATE_BUSY,
    "pausing": STATE_STOPPING,           # doc: pausing/paused -> shutting down
    "paused": STATE_STOPPING,
    "resuming": STATE_STARTING,
    # --- legacy / non-enum aliases kept from Fase 1 (harmless) --------------
    "running": STATE_RUNNING,
    "active": STATE_RUNNING,
    "ready": STATE_RUNNING,
    "off": STATE_STOPPED,
    "pending": STATE_STARTING,
    "provisioning": STATE_STARTING,
    "restarting": STATE_STARTING,
    "failed": STATE_ERROR,
    "unhealthy": STATE_ERROR,
    "deleted": STATE_UNKNOWN,
}

# "Powered on" for UI purposes. STARTING-family counts as live so the dashboard
# does not flap to Offline mid-boot. BUSY is deliberately neither live nor
# offline (see BUSY_STATES); ARCHIVED / STOPPED / STOPPING are not live.
LIVE_STATES = frozenset({STATE_RUNNING, STATE_STARTING})
BUSY_STATES = frozenset({STATE_BUSY})


class DaytonaError(RuntimeError):
    """Raised only for *caller-visible* failures we want surfaced as 503/502."""


def normalise_state(raw: Any) -> str:
    """Map an arbitrary raw state value to a canonical state (never raises)."""
    if raw is None:
        return STATE_UNKNOWN
    if isinstance(raw, dict):
        inner = raw.get("state") or raw.get("status") or raw.get("phase")
        if inner is None:
            # One level of nesting, e.g. {"sandbox": {"state": "error"}}.
            for value in raw.values():
                if isinstance(value, dict):
                    inner = value.get("state") or value.get("status") or value.get("phase")
                    if inner is not None:
                        break
        if inner is None:
            return STATE_UNKNOWN
        raw = inner
    text = str(raw).strip().lower()
    if not text:
        return STATE_UNKNOWN
    return _STATE_MAP.get(text, STATE_UNKNOWN)


def _extract_state(payload: Any) -> str:
    """Best-effort dig for a state field in an unknown JSON shape."""
    if not isinstance(payload, dict):
        return STATE_UNKNOWN
    for key in ("state", "status", "phase"):
        if key in payload:
            mapped = normalise_state(payload[key])
            if mapped != STATE_UNKNOWN:
                return mapped
    # one level of nesting, e.g. {"sandbox": {"state": ...}}
    for value in payload.values():
        if isinstance(value, dict):
            for key in ("state", "status", "phase"):
                if key in value:
                    mapped = normalise_state(value[key])
                    if mapped != STATE_UNKNOWN:
                        return mapped
    return STATE_UNKNOWN


def _extract_detail(payload: Any) -> dict[str, Any]:
    """Pull the diagnostic fields worth showing in the UI. Never raises.

    `errorReason` is the only useful signal when `state == error`; `recoverable`
    says whether offering POST .../recover is worth it. `desiredState` explains
    a sandbox that sits in a transition state.
    """
    detail: dict[str, Any] = {
        "error_reason": None,
        "recoverable": None,
        "desired_state": None,
        "sandbox_name": None,
    }
    node = payload if isinstance(payload, dict) else None
    if node is None:
        return detail
    # one level of nesting, same tolerance as _extract_state
    inner = next(
        (v for v in node.values() if isinstance(v, dict) and "state" in v),
        None,
    )
    for src in filter(None, (node, inner)):
        for key, out in (
            ("errorReason", "error_reason"),
            ("recoverable", "recoverable"),
            ("desiredState", "desired_state"),
            ("name", "sandbox_name"),
        ):
            if detail[out] is None and key in src:
                value = src[key]
                if out == "recoverable":
                    detail[out] = bool(value) if value is not None else None
                else:
                    detail[out] = None if value is None else str(value)
    return detail


class DaytonaClient:
    """Thin async wrapper. One instance per process; safe for concurrent use."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client: "httpx.AsyncClient | None" = None

    # ------------------------------------------------------------------ plumbing
    @property
    def configured(self) -> bool:
        return self.settings.daytona_configured

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.settings.daytona_api_key}",
            "Accept": "application/json",
        }

    async def _http(self) -> "httpx.AsyncClient":
        if self._client is None:
            # Lazy import: httpx (~2.5 MB RSS) is only needed once a real
            # Daytona call is about to be made.
            import httpx

            self._client = httpx.AsyncClient(
                base_url=self.settings.daytona_api_base.rstrip("/"),
                timeout=httpx.Timeout(self.settings.daytona_timeout),
                headers=self._headers(),
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _path(self, template: str) -> str:
        return template.format(sandbox_id=self.settings.sandbox_id)

    async def _request(
        self, method: str, template: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Perform a call with light retries. Never raises for HTTP errors."""
        if not self.configured:
            return {
                "ok": False,
                "configured": False,
                "state": STATE_UNKNOWN,
                **_extract_detail(None),
                "status_code": None,
                "error": "daytona not configured (DAYTONA_API_KEY / SANDBOX_ID missing)",
                "data": None,
            }

        path = self._path(template)
        attempts = max(1, self.settings.daytona_max_retries + 1)
        last_error = "unknown error"

        import httpx  # local alias for the exception types below

        for attempt in range(1, attempts + 1):
            try:
                client = await self._http()
                resp = await client.request(method, path, params=params)
                # Raw evidence at DEBUG — this is how we validate assumptions.
                log.debug(
                    "daytona %s %s -> %s body=%r",
                    method, path, resp.status_code, resp.text[:2000],
                )
                if resp.status_code >= 500 and attempt < attempts:
                    last_error = f"HTTP {resp.status_code}"
                    await asyncio.sleep(min(2 ** attempt * 0.25, 2.0))
                    continue

                payload: Any
                try:
                    payload = resp.json()
                except Exception:
                    payload = None

                if resp.status_code >= 400:
                    return {
                        "ok": False,
                        "configured": True,
                        "state": STATE_UNKNOWN,
                        **_extract_detail(payload),
                        "status_code": resp.status_code,
                        "error": f"HTTP {resp.status_code}",
                        "data": payload,
                    }

                state = _extract_state(payload)
                if state == STATE_UNKNOWN and method == "POST":
                    # A 2xx on a lifecycle POST is itself meaningful; do not
                    # pretend we know the new state.
                    state = STATE_UNKNOWN
                return {
                    "ok": True,
                    "configured": True,
                    "state": state,
                    **_extract_detail(payload),
                    "status_code": resp.status_code,
                    "error": None,
                    "data": payload,
                }
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log.debug("daytona %s %s transport error: %s", method, path, last_error)
                if attempt < attempts:
                    await asyncio.sleep(min(2 ** attempt * 0.25, 2.0))
                    continue
            except Exception as exc:  # pragma: no cover - defensive
                last_error = f"{type(exc).__name__}: {exc}"
                log.debug("daytona %s %s unexpected error: %s", method, path, last_error)
                break

        return {
            "ok": False,
            "configured": True,
            "state": STATE_UNKNOWN,
            **_extract_detail(None),
            "status_code": None,
            "error": last_error,
            "data": None,
        }

    # --------------------------------------------------------------- public API
    async def get_status(self) -> dict[str, Any]:
        return await self._request("GET", self.settings.daytona_path_sandbox)

    async def start(self) -> dict[str, Any]:
        return await self._request("POST", self.settings.daytona_path_start)

    async def stop(self) -> dict[str, Any]:
        return await self._request("POST", self.settings.daytona_path_stop)

    async def recover(self) -> dict[str, Any]:
        """POST /sandbox/{id}/recover — only meaningful when `recoverable` is true."""
        return await self._request("POST", self.settings.daytona_path_recover)

    async def create_ssh_access(self, expires_minutes: int) -> dict[str, Any]:
        """POST /sandbox/{id}/ssh-access?expiresInMinutes=N -> SSH token.

        The expiry MUST ride in the QUERY STRING. In the JSON body it is
        silently ignored and every token comes back with +60 minutes
        (docs/daytona-ssh-tunnel.md). The body is therefore `{}`.

        Never raises and never logs the token: the response body contains the
        credential itself, so only the status code is recorded.
        """
        result: dict[str, Any] = {
            "ok": False,
            "token": "",
            "expires_at": "",
            "error": "",
            "status_code": None,
        }

        if not self.configured:
            result["error"] = "daytona not configured"
            return result

        try:
            minutes = int(expires_minutes)
        except (TypeError, ValueError):
            result["error"] = "invalid expires_minutes"
            return result
        if minutes <= 0:
            result["error"] = "invalid expires_minutes"
            return result

        path = self._path("/sandbox/{sandbox_id}/ssh-access")
        params = {"expiresInMinutes": minutes}

        import httpx  # local alias; httpx is an existing dependency

        try:
            client = await self._http()
            # No resp.text in the log line: it carries the bearer token.
            resp = await client.post(path, json={}, params=params)
            result["status_code"] = resp.status_code

            payload: Any = None
            try:
                payload = resp.json()
            except Exception:
                payload = None

            log.debug("daytona POST %s -> %s", path, resp.status_code)

            if resp.status_code >= 400:
                detail = ""
                if isinstance(payload, dict):
                    detail = str(payload.get("message") or payload.get("error") or "")
                result["error"] = (
                    f"HTTP {resp.status_code}" + (f": {detail}" if detail else "")
                )
                return result

            if not isinstance(payload, dict):
                result["error"] = "unexpected response (not JSON)"
                return result

            token = payload.get("token")
            if not token:
                result["error"] = "response missing token"
                return result

            result["ok"] = True
            result["token"] = str(token)
            result["expires_at"] = str(payload.get("expiresAt") or "")
            result["error"] = ""
            return result
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            return result
        except Exception as exc:  # pragma: no cover - defensive
            result["error"] = f"{type(exc).__name__}: {exc}"
            return result

    async def restart(self) -> dict[str, Any]:
        """Stop then start. Best effort; reports both legs."""
        stop_result = await self.stop()
        await asyncio.sleep(1.0)
        start_result = await self.start()
        return {
            "ok": bool(start_result.get("ok")),
            "configured": self.configured,
            "state": start_result.get("state", STATE_UNKNOWN),
            "error_reason": start_result.get("error_reason"),
            "recoverable": start_result.get("recoverable"),
            "desired_state": start_result.get("desired_state"),
            "sandbox_name": start_result.get("sandbox_name"),
            "status_code": start_result.get("status_code"),
            "error": start_result.get("error") or stop_result.get("error"),
            "data": {"stop": stop_result, "start": start_result},
        }


_client: DaytonaClient | None = None


def get_client() -> DaytonaClient:
    global _client
    if _client is None:
        _client = DaytonaClient()
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
