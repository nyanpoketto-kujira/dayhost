"""SSH tunnel daemon to the Daytona sandbox (TUNNEL_MODE=ssh).

Ground truth: ``docs/daytona-ssh-tunnel.md`` — the sandbox cannot dial out
(org-level egress block), so this side dials ``ssh.app.daytona.io`` and keeps
one ``ssh -N`` child alive carrying three local forwards:

    <SSH_BIND_MC>:<PUBLIC_PORT> -> 127.0.0.1:<MC_PORT>       (players)
    127.0.0.1:<RCON_PORT>       -> 127.0.0.1:<RCON_PORT>    (console)
    127.0.0.1:<AGENT_WS_PORT>   -> 127.0.0.1:<AGENT_WS_PORT> (agent link)

CREDENTIAL RULE (the reason this file exists in this shape)
-----------------------------------------------------------
The SSH token *is* the ssh username, and ``ps aux`` shows the full command
line. So the token never reaches argv: it is written into a ``0600`` ssh
config file that is passed with ``-F``, and the file is deleted as soon as the
forwards are up. The token is also never logged and never put in an exception
message or in :meth:`SSHTunnel.status`.

The only in-memory copy lives in ``self._secret``, used for log redaction and
cleared on stop. ``grep -n token`` in this file shows every reference.

Dependencies: stdlib only (asyncio, tempfile, os, logging, random, time,
datetime). No paramiko, no asyncssh — ``ssh`` the binary does the
cryptography.

Scope: this module *owns* the ssh process. It does not touch
``app/services/tunnel.py`` (the read-only frp/socket observer).
"""
from __future__ import annotations

import asyncio
import atexit
import logging
import os
import random
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from ..config import Settings, get_settings
from .daytona_client import DaytonaClient

log = logging.getLogger("dayhost.ssh_tunnel")

# Alias used inside the generated config; argv only ever carries this name.
SSH_ALIAS = "dayhost-sandbox"

_BACKOFF_CAP = 60.0          # seconds, exponential ceiling
_BACKOFF_JITTER = 0.15       # +/- 15%
_RENEW_RETRY = 60.0          # retry a failed renewal this long after it failed
_TERM_GRACE = 5.0            # SIGTERM -> SIGKILL grace
_FORWARD_POLL = 0.25         # how often to re-probe the loopback forwards
_PROBE_TIMEOUT = 2.0
_MAX_STDERR = 400            # kept only for last_error, already redacted
_EXIT_STREAK_REFRESH = 3     # N dead sessions in a row -> drop the token
_MIN_CONNECT_TIMEOUT = 1.0

# Safety net: a crashed interpreter still leaves no 0600 credential file behind.
_TEMP_CONFIGS: set[str] = set()


def _emergency_cleanup() -> None:
    for path in list(_TEMP_CONFIGS):
        try:
            os.unlink(path)
        except OSError:
            pass
        _TEMP_CONFIGS.discard(path)


atexit.register(_emergency_cleanup)


def _parse_iso(value: str) -> float | None:
    """Parse Daytona's ``2026-10-26T00:14:58.589Z`` into epoch seconds."""
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


async def _default_probe(host: str, port: int) -> bool:
    """True if something accepts TCP on host:port right now."""
    writer = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)), timeout=_PROBE_TIMEOUT
        )
        del reader
        return True
    except (OSError, asyncio.TimeoutError, ValueError):
        return False
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def _default_spawn(argv: list[str]) -> Any:
    """Spawn ssh with no stdin, no stdout, and a pipe for stderr."""
    return await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


class SSHTunnel:
    """Owns exactly one ``ssh -N`` child and keeps it up.

    Test seams (all optional, production passes none):
      * ``client``  — a DaytonaClient-shaped object (``create_ssh_access``)
      * ``spawn``   — ``async (argv) -> proc-like``
      * ``probe``   — ``async (host, port) -> bool``
      * ``clock``   — wall-clock source, used for expiry math only
      * ``backoff_base`` — first restart delay (1.0s in production)
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: DaytonaClient | None = None,
        spawn: Callable[[list[str]], Awaitable[Any]] | None = None,
        probe: Callable[[str, int], Awaitable[bool]] | None = None,
        backoff_base: float = 1.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings or get_settings()
        self._client = client
        self._owns_client = client is None
        self._spawn = spawn or _default_spawn
        self._probe = probe or _default_probe
        self._backoff_base = max(0.0, float(backoff_base))
        self._clock = clock

        self._mode = (self.settings.tunnel_mode or "none").strip().lower()

        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._up_event = asyncio.Event()
        self._stopping = False

        self._proc: Any = None
        self._stderr_task: asyncio.Task | None = None
        self._config_path: str | None = None

        self._running = False
        self._pid: int | None = None
        self._connected_since: float | None = None
        self._last_error = ""
        self._last_stderr = ""
        self._restarts = 0

        self._expires_epoch: float | None = None
        self._token_expires_at = ""
        self._renew_retry_at: float | None = None
        # In-memory credential, used to build the 0600 config and to redact
        # stderr. Never logged, never returned by status(). Cleared on stop().
        self._secret = ""

    # ------------------------------------------------------------- public API
    async def start(self) -> None:
        """Idempotent: spawns the supervisor task and returns immediately."""
        try:
            if self._task is not None and not self._task.done():
                return
            if self._mode != "ssh":
                self._last_error = (
                    f"tunnel_mode is {self._mode!r}; ssh tunnel not started"
                )
                log.info("ssh tunnel not started: %s", self._last_error)
                return
            self._stopping = False
            self._stop_event = asyncio.Event()
            self._up_event.clear()
            self._task = asyncio.create_task(self._supervise(), name="dayhost-ssh-tunnel")
            log.info(
                "ssh tunnel supervisor started (host=%s forwards=%d)",
                self.settings.ssh_host,
                len(self._forwards()),
            )
        except Exception as exc:  # never raise out of start()
            self._last_error = f"ssh tunnel start failed: {type(exc).__name__}: {exc}"
            log.warning("%s", self._last_error)

    async def stop(self) -> None:
        """Cancel the supervisor, terminate the child, remove the config."""
        self._stopping = True
        self._stop_event.set()
        try:
            task = self._task
            if task is not None and not task.done():
                task.cancel()
                # asyncio.wait does not propagate cancellation to us, and the
                # timeout bounds a wedged child so stop() always returns.
                await asyncio.wait({task}, timeout=_TERM_GRACE * 3)
            self._task = None

            proc = self._proc
            if proc is not None:
                await self._terminate(proc)
            await self._stop_stderr()
            self._proc = None

            self._unlink_config()
            self._running = False
            self._pid = None
            self._connected_since = None
            self._up_event.clear()

            if self._owns_client and self._client is not None:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
                self._client = None
            self._secret = ""
            log.info("ssh tunnel stopped")
        except Exception as exc:  # never raise out of stop()
            self._last_error = f"ssh tunnel stop failed: {type(exc).__name__}: {exc}"
            log.warning("%s", self._last_error)
            self._unlink_config()
        finally:
            self._running = False
            self._pid = None
            self._connected_since = None
            self._up_event.clear()

    async def wait_ready(self, timeout: float | None = None) -> bool:
        """Await the first successful connection (handy for wiring/tests)."""
        try:
            if timeout is None:
                await self._up_event.wait()
                return True
            await asyncio.wait_for(self._up_event.wait(), timeout=timeout)
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return False

    def status(self) -> dict[str, Any]:
        """Snapshot for the panel. Never contains the token."""
        s = self.settings
        endpoint = (
            f"{s.public_host}:{s.public_port}"
            if s.public_host
            else f"{s.ssh_bind_mc}:{s.public_port}"
        )
        return {
            "mode": self._mode,
            "running": bool(self._running),
            "pid": self._pid,
            "forwards": list(self._forwards()),
            "public_endpoint": endpoint,
            "token_expires_at": self._token_expires_at,
            "last_error": self._last_error,
            "restarts": self._restarts,
            "connected_since": self._connected_since,
        }

    # --------------------------------------------------------------- forwards
    def _forwards(self) -> list[str]:
        """``-L`` specs, derived from settings, de-duplicated in order.

        The public Minecraft port comes from ``PUBLIC_PORT`` (default 25565).
        For a Daytona sandbox set ``PUBLIC_PORT=34212`` — that is the port the
        tunnel was verified on; the value is configuration, not code, so frp
        and ssh deployments keep their own numbers.
        """
        s = self.settings
        specs = [
            f"{s.ssh_bind_mc}:{s.public_port}:127.0.0.1:{s.mc_port}",
            f"127.0.0.1:{s.rcon_port}:127.0.0.1:{s.rcon_port}",
            f"127.0.0.1:{s.agent_ws_port}:127.0.0.1:{s.agent_ws_port}",
        ]
        unique: list[str] = []
        for spec in specs:
            if spec not in unique:
                unique.append(spec)
        return unique

    def _config_text(self, secret: str) -> str:
        """The 0600 file ssh reads via ``-F``. The credential lives here only.

        ``LocalForward`` in a config file takes TWO arguments
        (``[bind_address:]port host:hostport``); the colon-joined single token
        that ``-L`` accepts on the command line is rejected by ssh with
        ``Missing target argument`` before any network I/O. The specs from
        :meth:`_forwards` are therefore split here, not in ``_forwards``: its
        colon-joined form stays correct for ``-L`` argv and for status().
        """
        s = self.settings
        connect_timeout = max(_MIN_CONNECT_TIMEOUT, float(s.ssh_connect_timeout))
        lines = [
            "# generated by dayhost ssh_tunnel — deleted once the forwards are up",
            f"Host {SSH_ALIAS}",
            f"    HostName {s.ssh_host}",
            # Daytona's SSH endpoint listens on 22 (docs/daytona-ssh-tunnel.md).
            "    Port 22",
            f"    User {secret}",
            # Host key is ephemeral per connect; never accumulate it.
            "    StrictHostKeyChecking no",
            "    UserKnownHostsFile /dev/null",
            # A failed bind must be a hard error, not a half-up tunnel.
            "    ExitOnForwardFailure yes",
            f"    ConnectTimeout {int(connect_timeout)}",
            "    BatchMode yes",
            "    ServerAliveInterval 15",
            "    ServerAliveCountMax 3",
            "    LogLevel ERROR",
        ]
        def forward_line(spec: str) -> str:
            """``bind:port:host:hostport`` -> the two-argument config form."""
            parts = spec.split(":", 3)
            if len(parts) != 4 or ":" in parts[3]:
                raise ValueError(
                    f"cannot render LocalForward for spec {spec!r}: expected "
                    "'bind:port:host:hostport' (4 parts, split at the 3rd colon)"
                )
            bind, port, host, hostport = (part.strip() for part in parts)
            if not bind or not host:
                raise ValueError(
                    f"cannot render LocalForward for spec {spec!r}: "
                    "bind address and host must be non-empty"
                )
            for label, value in (("port", port), ("hostport", hostport)):
                if not value.isdigit() or not 0 < int(value) < 65536:
                    raise ValueError(
                        f"cannot render LocalForward for spec {spec!r}: "
                        f"{label} {value!r} is not a valid port"
                    )
            return f"    LocalForward {bind}:{port} {host}:{hostport}"

        lines += [forward_line(spec) for spec in self._forwards()]
        return "\n".join(lines) + "\n"

    def _write_config(self, secret: str) -> str:
        """mkstemp (already 0600) + explicit fchmod; returns the path."""
        fd, path = tempfile.mkstemp(prefix="dayhost-ssh-", suffix=".conf")
        try:
            os.fchmod(fd, 0o600)
            data = self._config_text(secret).encode("utf-8")
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short write on ssh config")
                view = view[written:]
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
        os.close(fd)
        _TEMP_CONFIGS.add(path)
        return path

    def _unlink_config(self) -> None:
        path = self._config_path
        self._config_path = None
        if not path:
            return
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning("could not remove ssh config: %s", type(exc).__name__)
        _TEMP_CONFIGS.discard(path)

    # ---------------------------------------------------------------- secrets
    def _redact(self, text: str) -> str:
        secret = self._secret
        if not text:
            return text
        if secret and secret in text:
            return text.replace(secret, "***")
        return text

    async def _acquire_token(self) -> bool:
        """Fetch a fresh token into ``self._secret``. Never raises."""
        try:
            client = await self._get_client()
            result = await client.create_ssh_access(self.settings.ssh_token_minutes)
        except Exception as exc:
            self._last_error = f"ssh token request raised {type(exc).__name__}"
            return False

        if not result or not result.get("ok"):
            error = str((result or {}).get("error") or "unknown error")
            self._secret = ""
            self._last_error = self._redact(f"ssh token request failed: {error}")[:300]
            return False

        secret = str(result.get("token") or "")
        if not secret:
            self._secret = ""
            self._last_error = "ssh token request returned an empty token"
            return False

        self._secret = secret
        self._token_expires_at = str(result.get("expires_at") or "")
        self._expires_epoch = _parse_iso(self._token_expires_at)
        self._renew_retry_at = None
        log.info(
            "ssh access token acquired (expires_at=%s, requested=%sm)",
            self._token_expires_at or "unknown",
            self.settings.ssh_token_minutes,
        )
        return True

    async def _get_client(self) -> DaytonaClient:
        if self._client is None:
            self._client = DaytonaClient(self.settings)
        return self._client

    # -------------------------------------------------------------- supervisor
    async def _supervise(self) -> None:
        backoff = self._backoff_base
        exit_streak = 0
        try:
            while not self._stopping:
                if not self._secret:
                    if not await self._acquire_token():
                        log.warning("ssh token unavailable: %s", self._last_error)
                        if await self._sleep_or_stop(self._jitter(backoff)):
                            return
                        backoff = self._next_backoff(backoff)
                        continue

                reason = await self._session()
                if reason == "stop" or self._stopping:
                    return

                if reason == "renew":
                    # _session already put a fresh token in self._secret.
                    backoff = self._backoff_base
                    exit_streak = 0
                    continue

                # "exit" (child died) or "failed" (forwards never came up).
                self._restarts += 1
                exit_streak += 1
                if reason == "failed":
                    # A rejected token looks exactly like this: drop and refetch.
                    self._secret = ""
                if exit_streak >= _EXIT_STREAK_REFRESH:
                    self._secret = ""
                    exit_streak = 0

                delay = self._jitter(backoff)
                log.warning(
                    "ssh tunnel down (%s), restart in %.1fs (restarts=%d)",
                    reason,
                    delay,
                    self._restarts,
                )
                if await self._sleep_or_stop(delay):
                    return
                backoff = self._next_backoff(backoff)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            self._last_error = (
                f"ssh supervisor crashed: {type(exc).__name__}: {self._redact(str(exc))}"
            )[:400]
            log.warning("%s", self._last_error)

    def _next_backoff(self, previous: float) -> float:
        base = self._backoff_base if self._backoff_base > 0 else 0.1
        return min(max(base, previous * 2.0), _BACKOFF_CAP)

    @staticmethod
    def _jitter(delay: float) -> float:
        if delay <= 0:
            return 0.0
        return delay * random.uniform(1.0 - _BACKOFF_JITTER, 1.0 + _BACKOFF_JITTER)

    async def _sleep_or_stop(self, delay: float) -> bool:
        """Sleep, but wake immediately on stop. True means stop was requested."""
        if self._stopping:
            return True
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(0.0, delay))
            return True
        except asyncio.TimeoutError:
            return False

    # ---------------------------------------------------------------- session
    async def _session(self) -> str:
        """One ssh child, from spawn to death. Returns stop|exit|renew|failed.

        Guarantees no live child and no config file survive the return.
        """
        proc: Any = None
        self._renew_retry_at = None
        self._last_stderr = ""
        try:
            config_path = self._write_config(self._secret)
            self._config_path = config_path
            log.info("ssh config written (mode 0600); starting ssh -N")

            # argv holds the alias, never the credential.
            proc = await self._spawn(["ssh", "-N", "-F", config_path, SSH_ALIAS])
            self._proc = proc
            self._stderr_task = asyncio.create_task(self._drain_stderr(proc))

            if not await self._wait_until_listening(proc):
                return "failed"

            # ssh has parsed the config by now: the credential file can go.
            self._unlink_config()

            self._running = True
            self._pid = getattr(proc, "pid", None)
            self._connected_since = self._clock()
            self._up_event.set()
            log.info(
                "ssh tunnel up (pid=%s, forwards=%d, expires_at=%s)",
                self._pid,
                len(self._forwards()),
                self._token_expires_at or "unknown",
            )

            while not self._stopping:
                outcome = await self._wait_outcome(proc, self._renew_deadline())
                if outcome == "stop":
                    return "stop"
                if outcome == "exit":
                    code = getattr(proc, "returncode", None)
                    detail = self._redact(self._last_stderr)
                    self._last_error = (
                        f"ssh exited rc={code}" + (f": {detail}" if detail else "")
                    )[:400]
                    log.warning("ssh exited rc=%s", code)
                    return "exit"

                # Renewal deadline reached: new token, then respawn with it.
                if await self._acquire_token():
                    log.info("ssh token renewed; reconnecting")
                    return "renew"

                # Renewal failed: keep the working tunnel, retry later.
                self._renew_retry_at = self._clock() + _RENEW_RETRY
                log.warning(
                    "ssh token renewal failed, keeping tunnel: %s", self._last_error
                )
            return "stop"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = self._redact(
                f"ssh session error: {type(exc).__name__}: {exc}"
            )[:400]
            log.warning("%s", self._last_error)
            return "failed"
        finally:
            self._running = False
            self._pid = None
            self._connected_since = None
            self._unlink_config()
            if proc is not None:
                await self._terminate(proc)
            await self._stop_stderr()
            self._proc = None

    def _renew_deadline(self) -> float | None:
        """When to fetch a new token, or None if we just let the link die."""
        margin = max(0.0, float(self.settings.ssh_renew_margin))
        if self._renew_retry_at is not None:
            if self._expires_epoch is not None and self._renew_retry_at >= self._expires_epoch:
                # Token is spent; the child will die and the restart refetches.
                return None
            return self._renew_retry_at
        if self._expires_epoch is None:
            return None
        return self._expires_epoch - margin

    async def _wait_outcome(self, proc: Any, deadline: float | None) -> str:
        """First of: process exit, stop request, renewal deadline."""
        stop_task = asyncio.create_task(self._stop_event.wait())
        exit_task = asyncio.create_task(proc.wait())
        tasks: list[asyncio.Task] = [stop_task, exit_task]
        if deadline is not None:
            tasks.append(
                asyncio.create_task(
                    asyncio.sleep(max(0.0, deadline - self._clock()))
                )
            )
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            # return_exceptions keeps the outer cancellation visible while
            # still collecting the cancelled siblings.
            await asyncio.gather(*tasks, return_exceptions=True)

        if stop_task in done:
            return "stop"
        if exit_task in done:
            return "exit"
        return "renew"

    # ---------------------------------------------------------------- startup
    async def _forwards_up(self) -> bool:
        for port in (self.settings.rcon_port, self.settings.agent_ws_port):
            try:
                ok = await self._probe("127.0.0.1", int(port))
            except Exception:
                ok = False
            if not ok:
                return False
        return True

    async def _wait_until_listening(self, proc: Any) -> bool:
        """Bounded probe loop; fails fast if ssh dies first."""
        loop = asyncio.get_running_loop()
        budget = max(_MIN_CONNECT_TIMEOUT, float(self.settings.ssh_connect_timeout))
        deadline = loop.time() + budget
        while loop.time() < deadline:
            if getattr(proc, "returncode", None) is not None:
                detail = self._redact(self._last_stderr)
                self._last_error = (
                    f"ssh exited during startup (rc={proc.returncode})"
                    + (f": {detail}" if detail else "")
                )[:400]
                return False
            if await self._forwards_up():
                return True
            if await self._sleep_or_stop(_FORWARD_POLL):
                return False
        self._last_error = (
            f"ssh forwards not listening after {budget:.0f}s "
            f"(rcon={self.settings.rcon_port}, agent_ws={self.settings.agent_ws_port})"
        )
        return False

    # -------------------------------------------------------------- lifecycle
    async def _drain_stderr(self, proc: Any) -> None:
        """Consume ssh stderr so the pipe never blocks; keep a redacted tail."""
        stream = getattr(proc, "stderr", None)
        if stream is None:
            return
        try:
            while True:
                line = await stream.readline()
                if not line:
                    return
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    self._last_stderr = self._redact(text)[-_MAX_STDERR:]
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def _stop_stderr(self) -> None:
        task = self._stderr_task
        self._stderr_task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _terminate(self, proc: Any) -> None:
        """SIGTERM, then SIGKILL after the grace window, then reap."""
        try:
            if getattr(proc, "returncode", None) is None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), _TERM_GRACE)
                except Exception:
                    pass
            if getattr(proc, "returncode", None) is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), _TERM_GRACE)
                except Exception:
                    pass
        except Exception as exc:  # pragma: no cover - defensive
            self._last_error = f"ssh terminate failed: {type(exc).__name__}"
