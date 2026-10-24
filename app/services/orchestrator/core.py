"""Provisioning state machine, delta ingest and snapshot scheduling.

Fusion Phase C. This is the orchestration layer that replaces
``mc-orchestrator/controller/recovery.py`` (SSH bootstrap) and the aiohttp
``app.py`` handlers. Every external action goes through an injected object, so
the whole machine runs against fakes with no network at all:

    driver   DaytonaDriver  — lifecycle start/stop + exec + ensure_agent_running
    files    DaytonaFilesClient — file_info / upload_file / create_folder
    ws       AgentWSServer  — the Phase-B server (hooks + send_json)
    storage  Storage        — mirror, deltas, snapshots, plugins
    state    AppState       — logs, link status, both state fields

The 8-step bootstrap of ``docs/fusion-analysis.md`` §5, as methods (idempotent,
no manual terminal step):

    start_host()
      1. lifecycle start, wait for RUNNING specifically   (driver.start)
      2. is the agent artifact there?                     (files.file_info + sha)
      3. push agent.py / agent.env (mode 600) when missing or drifted
      4. ensure the agent process is running              (driver.ensure_agent_running)
      5. wait until the agent is connected on the WS      (ws.connected)
      -> status: agent link + sandbox lifecycle recorded in two fields

    stop_host()
      stop MC (agent command) -> flush delta/backup -> stop agent -> lifecycle stop

    recover()
      watchdog hook (on_stale / on_disconnect) -> start_host() again, coalesced

Two authority rules (Ner's decision #1), enforced structurally:

  * ``mc_state`` is written **only** from agent messages (``handle_event``).
  * ``sandbox_state`` is written **only** here, from driver results.
  Neither method writes the other's field, so a lifecycle stop can never claim
  the JVM exited and an agent ``hello`` can never claim the sandbox is running.

Nothing here is imported by ``app/main.py`` yet — Phase D owns that wiring.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..daytona_client import STATE_RUNNING, normalise_state
from .state import AppState
from .storage import Storage, StoreConfig
from .util import atomic_write_json, sha256_file, utc_now

log = logging.getLogger("dayhost.orchestrator")

AGENT_MAIN = "agent.py"
AGENT_ENV = "agent.env"

# Agent link states that mean "an MC process may still be writing files".
MC_BUSY = frozenset({"starting", "running", "stopping"})


class OrchestrationError(Exception):
    """A step failed with a classified, structured error. Never a traceback."""

    def __init__(
        self,
        step: str,
        kind: str,
        error: str,
        *,
        status_code: int | None = None,
        detail: str | None = None,
        fatal: bool = True,
    ) -> None:
        super().__init__(f"{step}: {error}")
        self.step = step
        self.kind = kind
        self.error = error
        self.status_code = status_code
        self.detail = detail
        self.fatal = fatal

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "kind": self.kind,
            "error": self.error,
            "status_code": self.status_code,
            "detail": self.detail,
            "fatal": self.fatal,
        }


class Orchestrator:
    """One instance per process. Async, single-flight for start/stop/recover."""

    def __init__(
        self,
        settings: Any,
        *,
        driver: Any = None,
        files: Any = None,
        ws: Any = None,
        storage: Storage | None = None,
        state: AppState | None = None,
    ) -> None:
        self.settings = settings
        self.store_cfg = StoreConfig.from_settings(settings)
        self.storage = storage or Storage(self.store_cfg)
        self.state = state or AppState()
        self.state.snapshot_interval = int(self.store_cfg.snapshot_interval or 0)
        self.state.sandbox_id = str(getattr(settings, "sandbox_id", "") or "")
        self.ws = ws
        self._driver = driver
        self._files = files

        self._lock = asyncio.Lock()          # start/stop are mutually exclusive
        self._recovery_active = False
        self._last_recovery = 0.0
        self._stopping = False
        self._closing = False
        self._snapshot_task: asyncio.Task | None = None
        self._recovery_task: asyncio.Task | None = None
        self._waiters: list[asyncio.Future] = []   # ack / backup_done waiters

        # Call/step counters. Deliberately public: the harness asserts on them
        # (idempotence means "these numbers do not grow on a second call").
        self.counters: dict[str, int] = {
            "start_host": 0,
            "stop_host": 0,
            "recover": 0,
            "recovery_skipped": 0,
            "uploads": 0,
            "launches": 0,
            "lifecycle_start": 0,
            "lifecycle_stop": 0,
            "deltas": 0,
            "snapshots": 0,
        }
        self.last_result: dict[str, Any] = {}

    # ------------------------------------------------------------- wiring
    @classmethod
    def from_settings(cls, settings: Any = None, **kwargs: Any) -> "Orchestrator":
        """Build the real objects. Imports are local so this package stays
        import-clean (no dotenv read, no httpx, no socket)."""
        if settings is None:
            from ...config import get_settings

            settings = get_settings()
        from ..agent_ws import AgentWSServer
        from ..daytona_driver import get_driver
        from ..daytona_files import get_client

        # Hook kwargs belong to AgentWSServer, not to Orchestrator.__init__.
        # They are popped *before* ``cls(settings, **kwargs)`` — passing them
        # through raised ``TypeError: unexpected keyword argument 'on_event'``
        # (regression found in Phase D).
        hook_keys = ("on_event", "on_connect", "on_disconnect", "on_stale")
        hooks = {key: kwargs.pop(key) for key in hook_keys if key in kwargs}

        kwargs.setdefault("driver", get_driver())
        kwargs.setdefault("files", get_client())
        if "ws" in kwargs:
            if hooks:
                log.warning(
                    "orchestrator: from_settings() got an explicit ws= and hook "
                    "kwargs %s — the hooks are ignored (wire them yourself).",
                    sorted(hooks),
                )
        else:
            kwargs["ws"] = AgentWSServer.from_settings(settings, **hooks)
        return cls(settings, **kwargs)

    def hooks(self) -> dict[str, Any]:
        """Callback kwargs for ``AgentWSServer.from_settings(settings, **hooks())``.

        Kept separate from :meth:`from_settings` on purpose: that classmethod
        also builds the Daytona driver/client singletons, so wiring the WS
        server would drag global state into tests. This way a harness can build
        the real server with fakes and no ``get_driver()``/``get_client()``.

        The handlers are ``async def``; ``AgentWSServer._spawn_hook`` schedules
        coroutines with ``ensure_future`` and logs (never re-raises) hook
        failures, so a bad hook cannot kill the transport loop.
        """
        return {
            "on_event": self.handle_event,
            "on_connect": self.handle_connect,
            "on_disconnect": self.handle_disconnect,
            "on_stale": self.handle_stale,
        }

    # ------------------------------------------------------------ accessors
    @property
    def driver(self) -> Any:
        if self._driver is None:
            from ..daytona_driver import get_driver

            self._driver = get_driver()
        return self._driver

    @property
    def files(self) -> Any:
        if self._files is None:
            from ..daytona_files import get_client

            self._files = get_client()
        return self._files

    @property
    def running(self) -> bool:
        return bool(self.ws is not None and getattr(self.ws, "running", False))

    # -------------------------------------------------------------- lifecycle
    async def start(self, *, autostart: bool | None = None) -> dict[str, Any]:
        """Bring up the parts Phase C owns. Idempotent.

        ``AGENT_WS_ENABLED=false`` means the WS server is genuinely not started
        — the flag is read here, not decorative. The snapshot loop follows
        ``ORCH_SNAPSHOT_ENABLED`` *and* a positive interval.
        """
        self.store_cfg.ensure_dirs()
        await asyncio.to_thread(self.storage.ensure)
        started: list[str] = []

        if bool(getattr(self.settings, "agent_ws_enabled", False)):
            if self.ws is None:
                raise OrchestrationError(
                    "ws", "config", "AGENT_WS_ENABLED=true but no AgentWSServer was provided"
                )
            if not getattr(self.ws, "running", False):
                await self.ws.start()
                started.append("agent_ws")
            if not getattr(self.ws, "auth_configured", True):
                await self.state.log(
                    "AGENT_WS_ENABLED aktif tetapi AGENT_WS_TOKEN/AGENT_TOKEN kosong: "
                    "server menerima koneksi lalu menolaknya (503).",
                    source="orchestrator",
                )
        else:
            log.info("orchestrator: AGENT_WS_ENABLED=false — agent WS server not started")
            await self.state.log(
                "Agent WS nonaktif (AGENT_WS_ENABLED=false).", source="orchestrator"
            )

        if self._snapshot_enabled() and self._snapshot_task is None:
            self._snapshot_task = asyncio.ensure_future(self._snapshot_loop())
            started.append("snapshot_loop")

        should_autostart = (
            bool(getattr(self.settings, "orch_autostart", False)) if autostart is None else autostart
        )
        result: dict[str, Any] = {
            "ok": True,
            "started": started,
            "agent_ws": self.running,
            "agent_ws_enabled": bool(getattr(self.settings, "agent_ws_enabled", False)),
            "autostart": should_autostart,
        }
        if should_autostart:
            result["provision"] = await self.start_host(reason="autostart")
        return result

    async def close(self) -> None:
        """Stop tasks, then the listener. Never raises."""
        self._closing = True
        for attr in ("_snapshot_task", "_recovery_task"):
            task = getattr(self, attr)
            setattr(self, attr, None)
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        if self.ws is not None and getattr(self.ws, "running", False):
            try:
                await self.ws.stop()
            except Exception as exc:  # noqa: BLE001 - shutdown must not raise
                log.warning("orchestrator: ws stop failed (%s)", exc)
        self._resolve_waiters({"type": "_closed"})

    # ================================================================ start
    async def start_host(self, *, reason: str = "manual") -> dict[str, Any]:
        """Run §5 steps 1-5. Idempotent: safe to call on a live host.

        Returns ``{ok, reason, steps, error?, kind?, sandbox_state, mc_state}``.
        A failed step yields a structured error; no exception escapes.
        """
        async with self._lock:
            self.counters["start_host"] += 1
            started_here = time.monotonic()
            steps: list[dict[str, Any]] = []
            self._stopping = False
            self.state.provisioning_update(
                active=True, step="begin", started_at=utc_now(),
                last_error=None, steps=[],
            )
            try:
                await self._check_startable()
                await self._run(steps, "lifecycle_start", self._step_lifecycle_start)
                await self._run(steps, "agent_artifact", self._step_agent_artifact)
                await self._run(steps, "ensure_agent", self._step_ensure_agent)
                ok, detail = await self._step_wait_connected(steps)
                if not ok:
                    raise OrchestrationError(
                        "wait_connected", "timeout", detail, fatal=False
                    )
            except OrchestrationError as exc:
                self.state.provisioning_update(
                    active=False, step=f"failed:{exc.step}", last_error=exc.error
                )
                await self.state.log(
                    f"Provisioning gagal di langkah {exc.step}: {exc.error}",
                    source="orchestrator",
                )
                result = {
                    "ok": False,
                    "reason": reason,
                    "error": exc.error,
                    "kind": exc.kind,
                    "status_code": exc.status_code,
                    "detail": exc.detail,
                    "step": exc.step,
                    "steps": steps,
                    "sandbox_state": self.state.sandbox_state,
                    "mc_state": self.state.mc_state,
                    "elapsed": round(time.monotonic() - started_here, 2),
                }
                self.last_result = result
                return result

            self.state.provisioning_update(active=False, step="ready")
            await self.state.push_state()
            result = {
                "ok": True,
                "reason": reason,
                "steps": steps,
                "sandbox_state": self.state.sandbox_state,
                "mc_state": self.state.mc_state,
                "connected": self.state.connected,
                "elapsed": round(time.monotonic() - started_here, 2),
            }
            self.last_result = result
            await self.state.log(
                f"Host siap ({reason}): sandbox={self.state.sandbox_state}, "
                f"mc={self.state.mc_state}, agent={'online' if self.state.connected else 'offline'}.",
                source="orchestrator",
            )
            return result

    async def _check_startable(self) -> None:
        if self._closing:
            raise OrchestrationError("start", "config", "orchestrator sedang shutdown")
        missing = [
            name
            for name in ("daytona_api_key", "sandbox_id")
            if not str(getattr(self.settings, name, "") or "")
        ]
        if missing:
            raise OrchestrationError(
                "start", "config",
                "konfigurasi kurang: " + ", ".join(name.upper() for name in missing),
            )

    async def _run(self, steps: list[dict[str, Any]], name: str, func: Any) -> Any:
        """Run one step, recording it. Fatality lives in the step itself."""
        self.state.provisioning_update(step=name)
        at = time.monotonic()
        try:
            value = await func()
        except OrchestrationError as exc:
            steps.append(
                {"step": name, "ok": False, "kind": exc.kind, "error": exc.error,
                 "ms": round((time.monotonic() - at) * 1000)}
            )
            raise
        steps.append({"step": name, "ok": True, "ms": round((time.monotonic() - at) * 1000)})
        return value

    # ---- step 1 -----------------------------------------------------------
    async def _step_lifecycle_start(self) -> dict[str, Any]:
        self.counters["lifecycle_start"] += 1
        current = await self.driver.status()
        state = normalise_state(current.get("state"))
        if current.get("ok") and state == STATE_RUNNING:
            # Already running: do NOT issue a second start (idempotence).
            await self.state.set_sandbox_state(state)
            return {"already_running": True, "state": state}

        timeout = float(getattr(self.settings, "orch_provision_timeout", 180) or 180)
        result = await self.driver.start(wait=True, timeout=timeout)
        if not result.get("ok"):
            raise OrchestrationError(
                "lifecycle_start",
                str(result.get("kind") or "http"),
                str(result.get("error") or "sandbox start gagal"),
                status_code=result.get("status_code"),
                detail=result.get("detail"),
            )
        state = normalise_state(result.get("state"))
        await self.state.set_sandbox_state(state)
        await self.state.log(f"Sandbox {state}.", source="orchestrator")
        return {"state": state, "polls": result.get("polls"), "waited": result.get("waited")}

    # ---- steps 2-3 --------------------------------------------------------
    def _remote(self, name: str) -> str:
        directory = str(getattr(self.settings, "agent_remote_dir", "mc-orchestrator/agent") or "")
        return f"{directory.rstrip('/')}/{name}"

    def _agent_source(self) -> Path:
        raw = str(getattr(self.settings, "orch_agent_source", "") or "")
        if not raw:
            raw = "agent/agent.py"
        path = Path(raw)
        if not path.is_absolute():
            from ...config import REPO_ROOT

            path = REPO_ROOT / path
        return path

    async def _remote_file_info(self, remote_path: str) -> dict[str, Any]:
        """`file_info`, translating 404 into "missing" and real errors into raises."""
        info = await self.files.file_info(remote_path)
        if info.get("ok"):
            return {"exists": True, "size": int((info.get("info") or {}).get("size") or 0)}
        if info.get("status_code") == 404:
            return {"exists": False, "size": 0}
        kind = str(info.get("kind") or "http")
        # A toolbox that cannot answer is NOT "file missing": uploading blind
        # would hide a 401/500 behind a working-looking bootstrap.
        raise OrchestrationError(
            "agent_artifact", kind, str(info.get("error") or "file_info gagal"),
            status_code=info.get("status_code"), detail=info.get("body"),
        )

    async def _remote_sha(self, remote_path: str) -> str | None:
        """Best-effort sha256 of a remote file through exec. None = unknown."""
        try:
            probe = await self.driver.exec_command(
                f"sha256sum {remote_path}", timeout=min(
                    float(getattr(self.settings, "orch_provision_timeout", 180) or 180), 30
                )
            )
        except Exception as exc:  # noqa: BLE001 - never a traceback
            log.debug("orchestrator: remote sha probe failed (%s)", exc)
            return None
        if not probe.get("ok") or probe.get("exit_code") not in (0, None):
            return None
        text = str(probe.get("stdout") or "").strip()
        token = text.split()[0] if text else ""
        return token if len(token) == 64 and all(c in "0123456789abcdef" for c in token) else None

    async def _step_agent_artifact(self) -> dict[str, Any]:
        source = self._agent_source()
        if not source.is_file():
            raise OrchestrationError(
                "agent_artifact", "config",
                f"artefak agent tidak ditemukan: {source}",
            )
        local_sha = await asyncio.to_thread(sha256_file, source)
        local_size = source.stat().st_size

        remote = self._remote(AGENT_MAIN)
        info = await self._remote_file_info(remote)
        need = not info["exists"]
        if info["exists"]:
            if info["size"] != local_size:
                need = True
            else:
                remote_sha = await self._remote_sha(remote)
                # Unknown remote hash (exec unavailable) + equal size = trust it:
                # uploading on every boot would defeat idempotence.
                need = bool(remote_sha and remote_sha != local_sha)

        uploaded: list[str] = []
        if need:
            await self._upload(source, remote)
            uploaded.append(AGENT_MAIN)

        env_remote = self._remote(AGENT_ENV)
        env_info = await self._remote_file_info(env_remote)
        env_path, env_bytes = self._build_agent_env()
        if not env_info["exists"] or env_info["size"] != env_bytes:
            await self._upload(env_path, env_remote)
            uploaded.append(AGENT_ENV)
            await self._chmod_600(env_remote)
        return {
            "uploaded": uploaded,
            "agent_present": True,
            "sha256": local_sha,
            "env_remote": env_remote,
        }

    async def _upload(self, local: Path, remote: str) -> None:
        created = getattr(self.files, "create_folder", None)
        if callable(created):
            try:
                await created(str(Path(remote).parent))
            except Exception as exc:  # noqa: BLE001 - upload auto-creates parents
                log.debug("orchestrator: create_folder(%s) skipped (%s)", remote, exc)
        result = await self.files.upload_file(local, remote)
        if not result.get("ok"):
            raise OrchestrationError(
                "agent_artifact", str(result.get("kind") or "http"),
                f"upload {local.name} gagal: {result.get('error')}",
                status_code=result.get("status_code"), detail=result.get("body"),
            )
        self.counters["uploads"] += 1

    async def _chmod_600(self, remote: str) -> None:
        """`agent.env` holds the agent token: 600 or nothing. Non-fatal."""
        probe = await self.driver.exec_command(f"chmod 600 {remote}", timeout=15)
        if not probe.get("ok") or (probe.get("exit_code") not in (0, None)):
            await self.state.log(
                f"Peringatan: chmod 600 {remote} gagal "
                f"({probe.get('error') or probe.get('stderr') or probe.get('exit_code')}).",
                source="orchestrator",
            )

    def _build_agent_env(self) -> tuple[Path, int]:
        """Generate agent.env locally (600) from settings, then return its path/size.

        No manual step: the values come from the control plane, never from a
        hand-edited file inside the sandbox. Java is pinned to 21 for this
        phase (Ner's decision) — Phase E's agent reads JAVA_VERSION.
        """
        def flag(name: str, default: bool = False) -> str:
            return "1" if bool(getattr(self.settings, name, default)) else "0"

        def text(name: str, default: str = "") -> str:
            value = getattr(self.settings, name, default)
            return "" if value is None else str(value).strip()

        url = str(getattr(self.settings, "orch_agent_ws_url", "") or "").strip()
        if not url:
            scheme = "wss" if str(getattr(self.settings, "orch_agent_ws_tls", "")).lower() in (
                "1", "true", "yes", "on",
            ) else "ws"
            host = str(getattr(self.settings, "agent_ws_public_host", "") or "").strip()
            if not host:
                host = str(getattr(self.settings, "agent_ws_host", "127.0.0.1"))
            port = int(getattr(self.settings, "agent_ws_port", 8765))
            path = str(getattr(self.settings, "agent_ws_path", "/agent-ws"))
            url = f"{scheme}://{host}:{port}/{path.lstrip('/')}"
        else:
            # Phase-B contract: the agent dials ``:8765/agent-ws``. An explicit
            # override that drifts from it is normalised (not silently kept),
            # otherwise the sandbox connects to nothing and every step times out.
            url = self._normalise_agent_ws_url(url)

        frp_enabled = str(getattr(self.settings, "tunnel_mode", "frp") or "frp").lower() == "frp"
        controller_http = text("orch_controller_http")
        home = str(getattr(self.settings, "orch_sandbox_home", "/home/daytona") or "/home/daytona").rstrip("/")

        def sandbox_path(value: str) -> str:
            raw = str(value or "").strip()
            if not raw:
                return ""
            return raw if raw.startswith("/") else f"{home}/{raw.lstrip('/')}"

        lines = [
            "# Generated by DayHost orchestrator — do not edit in the sandbox.",
            f"CONTROLLER_WS={url}",
            f"AGENT_TOKEN={getattr(self.settings, 'agent_ws_effective_token', '')}",
            f"SERVER_DIR={sandbox_path(getattr(self.settings, 'orch_server_dir', 'mc-orchestrator/server'))}",
            f"RUNTIME_DIR={sandbox_path(getattr(self.settings, 'orch_runtime_dir', 'mc-orchestrator/runtime'))}",
            f"JAVA_VERSION={int(getattr(self.settings, 'orch_java_version', 21) or 21)}",
            # Explicit, never omitted: the agent's own defaults must not decide
            # who provisions or who boots MC (the orchestrator does).
            f"PROVISION={flag('orch_provision', True)}",
            f"KILL_ORPHANS={flag('orch_kill_orphans', True)}",
            f"WS_INSECURE={flag('orch_ws_insecure', False)}",
            f"AUTOSTART={flag('orch_autostart', False)}",
            # FRP_* is always written, even when empty: the agent then reports
            # "unconfigured" instead of guessing a default endpoint.
            f"FRP_ENABLED={'1' if frp_enabled else '0'}",
            f"FRP_SERVER_ADDR={text('frp_server_addr')}",
            f"FRP_SERVER_PORT={int(getattr(self.settings, 'frp_server_port', 7000) or 7000)}",
            f"FRP_TOKEN={text('frp_token')}",
            f"FRP_LOCAL_PORT={int(getattr(self.settings, 'frp_local_port', 25565) or 25565)}",
            f"FRP_REMOTE_PORT={int(getattr(self.settings, 'frp_remote_port', 25565) or 25565)}",
            f"FRP_VERSION={text('frp_version', '0.71.1')}",
            f"FRP_SHA256={text('frp_sha256').lower()}",
        ]
        # Optional: an empty value is omitted rather than written as '' so the
        # agent falls back to its documented default instead of an empty path.
        if controller_http:
            lines.append(f"CONTROLLER_HTTP={controller_http}")
        lines += [
            "",
        ]
        spec = url.replace("ws://", "").replace("wss://", "")
        target = self.store_cfg.tmp_dir / f"{AGENT_ENV}.{abs(hash(spec)) % 10**8:08d}"
        target.write_text("\n".join(lines), encoding="utf-8")
        try:
            os.chmod(target, 0o600)
        except OSError:
            pass
        return target, target.stat().st_size

    @staticmethod
    def _normalise_agent_ws_url(url: str) -> str:
        """Force ``<scheme>://<host>:8765/.../agent-ws`` (Phase-B contract)."""
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        if scheme not in ("ws", "wss"):
            scheme = "ws"
        host = parsed.hostname or "127.0.0.1"
        if parsed.port not in (None, 8765):
            log.warning(
                "orch_agent_ws_url: port %s -> 8765 (kontrak Phase B)", parsed.port
            )
        port = 8765
        path = parsed.path or ""
        if not path.rstrip("/").endswith("/agent-ws"):
            original = path or "/"
            new_path = (path.rstrip("/") or "") + "/agent-ws"
            log.warning(
                "orch_agent_ws_url: path %r -> %r (kontrak Phase B)",
                original,
                new_path,
            )
            path = new_path
        netloc = f"[{host}]" if ":" in host else host
        return urlunsplit((scheme, f"{netloc}:{port}", path, "", ""))

    # ---- step 4 -----------------------------------------------------------
    async def _step_ensure_agent(self) -> dict[str, Any]:
        result = await self.driver.ensure_agent_running()
        if not result.get("ok"):
            raise OrchestrationError(
                "ensure_agent", str(result.get("kind") or "http"),
                str(result.get("error") or "agent gagal dijalankan"),
                status_code=result.get("status_code"),
            )
        if result.get("launched"):
            self.counters["launches"] += 1
            await self.state.log("Agent dijalankan di sandbox.", source="orchestrator")
        return {
            "launched": bool(result.get("launched")),
            "alive_before": result.get("alive_before"),
            "checked": result.get("checked"),
        }

    # ---- step 5 -----------------------------------------------------------
    async def _step_wait_connected(self, steps: list[dict[str, Any]]) -> tuple[bool, str]:
        timeout = float(getattr(self.settings, "orch_connect_timeout", 60) or 60)
        poll = max(0.05, float(getattr(self.settings, "orch_connect_poll", 0.2) or 0.2))
        at = time.monotonic()
        deadline = at + timeout
        while True:
            if self._agent_connected():
                steps.append(
                    {"step": "wait_connected", "ok": True,
                     "ms": round((time.monotonic() - at) * 1000)}
                )
                return True, "connected"
            if time.monotonic() >= deadline:
                steps.append(
                    {"step": "wait_connected", "ok": False, "kind": "timeout",
                     "ms": round((time.monotonic() - at) * 1000)}
                )
                return False, f"agent belum terhubung dalam {timeout:g}s"
            await asyncio.sleep(poll)

    # ================================================================= stop
    async def stop_host(self, *, reason: str = "manual") -> dict[str, Any]:
        """Stop in the safe order: MC -> flush -> agent -> lifecycle.

        Failure policy (explicit, because it is a judgement call):
          * MC stop **rejected by the agent** (ack ok=false) is **fatal**: the
            sandbox is not quiesced, and a lifecycle stop now would kill the JVM
            mid-write. We stop there and leave the sandbox running.
          * Everything else — MC stop timeout, backup flush failure, pkill
            finding nothing, agent already dead — is recorded and the sequence
            continues. The lifecycle stop is the only way to reclaim the host,
            so it must not be blocked by a hung agent.
        """
        async with self._lock:
            self.counters["stop_host"] += 1
            self._stopping = True
            started_here = time.monotonic()
            steps: list[dict[str, Any]] = []
            errors: list[dict[str, Any]] = []
            self.state.provisioning_update(
                active=True, step="stopping", started_at=utc_now(), last_error=None, steps=[]
            )
            fatal: OrchestrationError | None = None
            try:
                for name, func in (
                    ("stop_mc", self._step_stop_mc),
                    ("flush_backup", self._step_flush_backup),
                    ("stop_agent", self._step_stop_agent),
                    ("lifecycle_stop", self._step_lifecycle_stop),
                ):
                    self.state.provisioning_update(step=name)
                    at = time.monotonic()
                    try:
                        detail = await func()
                        steps.append({"step": name, "ok": True, "detail": detail,
                                      "ms": round((time.monotonic() - at) * 1000)})
                    except OrchestrationError as exc:
                        errors.append(exc.as_dict())
                        steps.append({"step": name, "ok": False, "kind": exc.kind,
                                      "error": exc.error,
                                      "ms": round((time.monotonic() - at) * 1000)})
                        if exc.fatal and name == "stop_mc":
                            fatal = exc
                            break
                        await self.state.log(
                            f"Stop: langkah {name} gagal ({exc.error}); lanjut.", 
                            source="orchestrator",
                        )
            finally:
                self._stopping = False

            if fatal is not None:
                self.state.provisioning_update(
                    active=False, step="failed:stop_mc", last_error=fatal.error
                )
                result = {
                    "ok": False, "reason": reason, "error": fatal.error,
                    "kind": fatal.kind, "step": fatal.step, "steps": steps,
                    "sandbox_state": self.state.sandbox_state,
                    "mc_state": self.state.mc_state,
                    "elapsed": round(time.monotonic() - started_here, 2),
                }
            else:
                self.state.provisioning_update(active=False, step="stopped")
                result = {
                    "ok": True, "reason": reason,
                    "partial": bool(errors), "errors": errors, "steps": steps,
                    "sandbox_state": self.state.sandbox_state,
                    "mc_state": self.state.mc_state,
                    "elapsed": round(time.monotonic() - started_here, 2),
                }
            self.last_result = result
            return result

    async def _step_stop_mc(self) -> dict[str, Any]:
        if not self._agent_connected():
            return {"skipped": "agent offline"}
        command_id = f"stop-{int(time.time() * 1000)}"
        waiter = self._add_waiter(lambda ev: ev.get("type") == "ack" and ev.get("id") == command_id)
        sent = await self.state.send_agent(
            {"type": "command", "action": "stop", "id": command_id}
        )
        if not sent:
            self._drop_waiter(waiter)
            return {"skipped": "agent link closed"}
        try:
            ack = await asyncio.wait_for(
                waiter, timeout=float(getattr(self.settings, "orch_stop_mc_timeout", 90) or 90)
            )
        except asyncio.TimeoutError:
            raise OrchestrationError(
                "stop_mc", "timeout",
                "agent tidak meng-ack perintah stop; lanjut ke lifecycle stop",
                fatal=False,
            )
        finally:
            self._drop_waiter(waiter)
        if isinstance(ack, dict) and ack.get("ok") is False:
            raise OrchestrationError(
                "stop_mc", "rejected",
                str(ack.get("message") or "agent menolak perintah stop"),
                fatal=True,
            )
        await self.state.set_mc_state("stopped")
        return {"ack": True}

    async def _step_flush_backup(self) -> dict[str, Any]:
        if not self._agent_connected():
            return {"skipped": "agent offline"}
        command_id = f"backup-{int(time.time() * 1000)}"
        waiter = self._add_waiter(lambda ev: ev.get("type") == "backup_done")
        sent = await self.state.send_agent(
            {"type": "command", "action": "backup", "id": command_id}
        )
        if not sent:
            self._drop_waiter(waiter)
            return {"skipped": "agent link closed"}
        timeout = float(getattr(self.settings, "orch_backup_flush_timeout", 120) or 120)
        try:
            info = await asyncio.wait_for(waiter, timeout=timeout)
            return {"files": info.get("files"), "bytes": info.get("bytes")}
        except asyncio.TimeoutError:
            raise OrchestrationError(
                "flush_backup", "timeout",
                f"tidak ada backup_done dalam {timeout:g}s; delta terakhir sudah aman di mirror",
                fatal=False,
            )
        finally:
            self._drop_waiter(waiter)

    async def _step_stop_agent(self) -> dict[str, Any]:
        if self.ws is not None and getattr(self.ws, "running", False):
            try:
                await self.ws.close_agent(1001, "host stopping")
            except Exception as exc:  # noqa: BLE001
                log.debug("orchestrator: close_agent failed (%s)", exc)
        command = str(
            getattr(self.settings, "orch_agent_stop_cmd", "") or "pkill -f agent.py"
        )
        probe = await self.driver.exec_command(command, timeout=20)
        code = probe.get("exit_code")
        # pkill: 0 = killed, 1 = nothing matched (fine). A transport failure is
        # recorded but never blocks the lifecycle stop.
        if not probe.get("ok"):
            raise OrchestrationError(
                "stop_agent", str(probe.get("kind") or "http"),
                f"perintah stop agent gagal: {probe.get('error')}",
                status_code=probe.get("status_code"), fatal=False,
            )
        return {"exit_code": code, "already_dead": code == 1}

    async def _step_lifecycle_stop(self) -> dict[str, Any]:
        self.counters["lifecycle_stop"] += 1
        result = await self.driver.stop()
        if not result.get("ok"):
            raise OrchestrationError(
                "lifecycle_stop", str(result.get("kind") or "http"),
                str(result.get("error") or "sandbox stop gagal"),
                status_code=result.get("status_code"), fatal=False,
            )
        state = normalise_state(result.get("state")) if result.get("state") else "STOPPED"
        await self.state.set_sandbox_state(state)
        return {"state": state}

    # ============================================================== recovery
    async def recover(self, reason: str = "recovery") -> dict[str, Any]:
        """Watchdog entry point. Coalesced: at most one start_host at a time."""
        self.counters["recover"] += 1
        if self._closing or self._stopping:
            self.counters["recovery_skipped"] += 1
            return {"ok": True, "skipped": "stopping" if self._stopping else "closing"}
        if self._recovery_active:
            self.counters["recovery_skipped"] += 1
            return {"ok": True, "skipped": "recovery already in flight"}
        cooldown = float(getattr(self.settings, "orch_recovery_cooldown", 0) or 0)
        since = time.monotonic() - self._last_recovery
        if cooldown > 0 and since < cooldown:
            self.counters["recovery_skipped"] += 1
            return {"ok": True, "skipped": f"cooldown {round(cooldown - since, 1)}s"}
        if self._agent_connected():
            self.counters["recovery_skipped"] += 1
            return {"ok": True, "skipped": "agent connected"}

        self._recovery_active = True
        self.state.recovery_update(active=True, step="starting", last_error=None,
                                   started_at=utc_now())
        await self.state.push_recovery()
        try:
            result = await self.start_host(reason=reason)
            self.state.recovery_update(
                active=False,
                step="ok" if result.get("ok") else "failed",
                attempt=int(self.state.recovery.get("attempt") or 0) + 1,
                last_error=result.get("error"),
            )
            await self.state.push_recovery()
            return result
        finally:
            self._recovery_active = False
            self._last_recovery = time.monotonic()

    def schedule_recovery(self, reason: str = "watchdog") -> None:
        """Fire-and-forget recovery, guarded against stacking."""
        if self._closing or self._stopping:
            return
        if self._recovery_task is not None and not self._recovery_task.done():
            self.counters["recovery_skipped"] += 1
            return
        grace = max(0.0, float(getattr(self.settings, "orch_recovery_grace", 0) or 0))

        async def _run() -> None:
            if grace:
                await asyncio.sleep(grace)
            try:
                await self.recover(reason)
            except Exception as exc:  # noqa: BLE001 - watchdog must not die
                log.warning("orchestrator: recovery failed (%s)", exc)

        self._recovery_task = asyncio.ensure_future(_run())

    # ------------------------------------------------------------ ws hooks
    async def handle_connect(self, session: Any) -> None:
        self.state.mark_agent_connected(self.ws)
        self.state.status = "online"
        await self.state.log(
            f"Agent terhubung: {getattr(session, 'hostname', '?')} "
            f"(v{getattr(session, 'version', '?')})",
            source="agent",
        )
        await self.state.push_state()

    async def handle_disconnect(self, info: dict[str, Any]) -> None:
        self.state.mark_agent_disconnected()
        await self.state.push_state()
        await self.state.log(
            f"Agent terputus ({info.get('reason') or 'unknown'}).", source="agent"
        )
        if bool(getattr(self.settings, "orch_recovery_enabled", True)):
            self.schedule_recovery("agent_disconnected")

    async def handle_stale(self, info: dict[str, Any]) -> None:
        self.state.touch_agent()
        await self.state.log(
            f"Agent tidak merespons {info.get('idle')}s — watchdog.", source="agent"
        )
        if bool(getattr(self.settings, "orch_recovery_enabled", True)):
            self.schedule_recovery("agent_stale")

    async def handle_event(self, event: dict[str, Any]) -> None:
        """Every frame from the agent. ``mc_state`` is written here and nowhere else."""
        kind = event.get("type")
        if kind == "hello":
            info = getattr(self.ws, "agent_info", None) or {}
            self.state.agent_info = {
                "hostname": event.get("hostname"),
                "version": event.get("version"),
                "server_dir": event.get("server_dir"),
                "platform": event.get("platform"),
                "connected_at": utc_now(),
            }
            mc_state = event.get("mc_state")
            if isinstance(mc_state, str):
                await self.state.set_mc_state(mc_state)
            if isinstance(info.get("installed_plugins"), list):
                self.state.agent_info["installed_plugins"] = info["installed_plugins"]
            await self.state.log(
                f"Agent online: {event.get('hostname')} ({event.get('platform')}).",
                source="agent",
            )
        elif kind == "log":
            entries = event.get("entries")
            if isinstance(entries, list):
                await self.state.log_many(entries)
        elif kind == "mc_state":
            mc_state = str(event.get("state") or "unknown")
            if mc_state == "stopped" and event.get("exit_code") is not None:
                await self.state.log(
                    f"PaperMC berhenti (exit code {event.get('exit_code')}).", source="agent"
                )
            await self.state.set_mc_state(mc_state)
        elif kind == "heartbeat":
            mc_state = event.get("mc_state")
            if isinstance(mc_state, str):
                await self.state.set_mc_state(mc_state)
        elif kind == "ack":
            # ``_resolve_waiter`` is synchronous; awaiting it raised TypeError and
            # aborted handle_event before push_state() on every agent ack.
            self._resolve_waiter(event, {"type": "ack", "id": event.get("id"),
                                        "ok": event.get("ok", True),
                                        "message": event.get("message")})
            if event.get("ok") is False:
                await self.state.log(
                    f"Perintah agent gagal: {event.get('message') or '?'}", source="agent"
                )
        elif kind == "backup_done":
            if not self._resolve_waiter(event, event):
                await self.state.log(
                    f"Delta terkirim: {event.get('files', 0)} file, "
                    f"{event.get('bytes_human', '0 B')}.", source="backup"
                )
            await self.refresh_backups()
        elif kind in {"_agent_connected", "_agent_disconnected", "_agent_stale"}:
            return  # transport events are routed via the dedicated hooks
        await self.state.push_state()

    def _agent_connected(self) -> bool:
        if self.ws is not None and getattr(self.ws, "connected", False):
            self.state.mark_agent_connected(self.ws)
            return True
        return bool(self.state.connected and self.state.agent_ws is not None)

    # ------------------------------------------------------------- waiters
    def _add_waiter(self, predicate: Any) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        future._orch_predicate = predicate  # type: ignore[attr-defined]
        self._waiters.append(future)
        return future

    def _drop_waiter(self, future: asyncio.Future) -> None:
        try:
            self._waiters.remove(future)
        except ValueError:
            pass

    def _resolve_waiter(self, event: dict[str, Any], payload: dict[str, Any]) -> bool:
        for future in list(self._waiters):
            if future.done():
                self._drop_waiter(future)
                continue
            predicate = getattr(future, "_orch_predicate", None)
            try:
                hit = bool(predicate(event)) if callable(predicate) else False
            except Exception:  # noqa: BLE001 - a bad predicate must not break ingest
                hit = False
            if hit:
                self._drop_waiter(future)
                future.set_result(payload)
                return True
        return False

    def _resolve_waiters(self, event: dict[str, Any]) -> None:
        for future in list(self._waiters):
            self._drop_waiter(future)
            if not future.done():
                future.cancel()

    # ---------------------------------------------------------- delta ingest
    async def ingest_delta(self, archive: Path | str, agent_host: str = "agent") -> dict[str, Any]:
        """Apply one delta archive to the mirror (Phase D's HTTP route calls this)."""
        path = Path(archive)
        try:
            result = await self.storage.apply_delta(path, agent_host)
        except (ValueError, OSError) as exc:
            # Path-unsafe member / corrupt tar / unreadable file: structured.
            await self.state.log(f"Delta ditolak: {exc}", source="backup")
            return {"ok": False, "kind": "invalid", "error": str(exc)}
        except Exception as exc:  # noqa: BLE001 - never a traceback into a request
            await self.state.log(f"Delta gagal diproses: {type(exc).__name__}: {exc}",
                                 source="backup")
            return {"ok": False, "kind": "internal",
                    "error": f"{type(exc).__name__}: {exc}"}

        self.counters["deltas"] += 1
        await self.state.log(
            f"Delta dari {agent_host}: {result['files']} file, {result['deleted']} dihapus, "
            f"{result['bytes_human']}.",
            source="backup",
        )
        await self.refresh_backups()
        return {"ok": True, **result}

    async def refresh_backups(self) -> dict[str, Any]:
        payload = await asyncio.to_thread(self.storage.backups_payload)
        await self.state.broadcast_backups(payload)
        return payload

    # ------------------------------------------------------- snapshot loop
    def _snapshot_enabled(self) -> bool:
        if not bool(getattr(self.settings, "orch_snapshot_enabled", True)):
            return False
        return float(self.store_cfg.snapshot_interval or 0) > 0

    async def _snapshot_loop(self) -> None:
        interval = max(0.5, float(self.store_cfg.snapshot_interval or 0))
        while not self._closing:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            if self._closing:
                return
            try:
                await self.snapshot_now(reason="scheduled")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the loop must survive
                log.warning("orchestrator: scheduled snapshot failed (%s)", exc)

    async def snapshot_now(self, reason: str = "manual") -> dict[str, Any]:
        if not self.storage.has_world():
            log.debug("orchestrator: snapshot skipped — mirror has no world/level.dat")
            return {"ok": False, "skipped": "no world in mirror"}
        try:
            info = await self.storage.create_snapshot(reason)
        except ValueError as exc:
            await self.state.log(f"Snapshot gagal: {exc}", source="backup")
            return {"ok": False, "kind": "invalid", "error": str(exc)}
        self.counters["snapshots"] += 1
        await self.state.log(f"Snapshot dibuat: {info['name']} ({info['size_human']}).",
                             source="backup")
        await self.refresh_backups()
        return {"ok": True, **info}

    async def start_snapshot_loop(self) -> None:
        if self._snapshot_task is None:
            self._snapshot_task = asyncio.ensure_future(self._snapshot_loop())

    # ---------------------------------------------------------------- misc
    def public_state(self) -> dict[str, Any]:
        return {
            "state": self.state.public_state(),
            "counters": dict(self.counters),
            "ws": self.ws.public_state() if self.ws is not None else {},
            "paths": {
                "mirror": str(self.store_cfg.mirror_dir),
                "snapshots": str(self.store_cfg.snapshots_dir),
                "deltas": str(self.store_cfg.deltas_dir),
                "plugins": str(self.store_cfg.plugins_dir),
            },
        }

    def write_status(self, path: Path) -> None:
        """Optional debug dump (never called by the app; used by operators)."""
        atomic_write_json(path, self.public_state())
