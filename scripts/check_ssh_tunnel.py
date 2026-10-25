"""Offline checks for app/services/ssh_tunnel.py — no network, no real ssh.

Run standalone: `python scripts/check_ssh_tunnel.py`

Fakes: a DaytonaClient-shaped token source, a fake ssh process, a fake forward
probe, and a fake argv capturer. Nothing touches the network or spawns a
binary, so this is safe to run anywhere.

Proves:
  1. the 0600 ssh config holds the credential (`User <token>`) and the three
     LocalForward lines, and is deleted once ssh is up,
  2. the token NEVER appears in the argv handed to ssh (the security invariant:
     `ps aux` shows argv),
  3. status() exposes the documented keys and leaks no token in its JSON dump,
  4. a child exit increments `restarts` and a new ssh is spawned,
  5. renewal refetches a token without tearing down a working tunnel on failure,
  6. stop() leaves no child, no supervisor task and no temp config file.

Exit 0 = pass.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings  # noqa: E402
from app.services import ssh_tunnel  # noqa: E402
from app.services.ssh_tunnel import SSHTunnel  # noqa: E402

TOKEN = "tok-abcdef0123456789abcdef0123456789"
RENEWED_TOKEN = "tok-99999988888877777766666655555555"
PUBLIC_PORT = 34212
RCON_PORT = 25575
AGENT_PORT = 8765


# --------------------------------------------------------------------- fakes
class FakeClient:
    """DaytonaClient-shaped token source.

    ``tokens`` is consumed per call (the last entry repeats); ``fail_from``
    makes every call from that 1-based index fail with a 401;
    ``short_expiry_first`` gives the first token a real, near-future expiry so
    renewal can be exercised without waiting for 24h.
    """

    def __init__(
        self,
        tokens: tuple[str, ...] = (TOKEN,),
        fail_from: int | None = None,
        short_expiry_first: float | None = None,
    ) -> None:
        self.tokens = list(tokens)
        self.fail_from = fail_from
        self.short_expiry_first = short_expiry_first
        self.calls = 0
        self.minutes: list[int] = []

    async def create_ssh_access(self, expires_minutes: int) -> dict:
        self.calls += 1
        self.minutes.append(expires_minutes)
        if self.fail_from is not None and self.calls >= self.fail_from:
            return {"ok": False, "token": "", "expires_at": "", "error": "HTTP 401", "status_code": 401}
        token = self.tokens[min(self.calls - 1, len(self.tokens) - 1)]
        expires_at = "2099-01-01T00:00:00.000Z"
        if self.calls == 1 and self.short_expiry_first is not None:
            expires_at = _iso_in(self.short_expiry_first)
        return {
            "ok": True,
            "token": token,
            "expires_at": expires_at,
            "error": "",
            "status_code": 200,
        }

    async def aclose(self) -> None:
        return None


def _iso_in(seconds: float) -> str:
    """ISO-8601 UTC timestamp `seconds` from now, in Daytona's exact shape."""
    stamp = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class FakeStderr:
    """Empty, already-closed stderr pipe."""

    async def readline(self) -> bytes:
        return b""


class FakeProc:
    """A stand-in for asyncio.subprocess.Process."""

    def __init__(self, pid: int, argv: list[str]) -> None:
        self.pid = pid
        self.argv = argv
        self.returncode: int | None = None
        self.stderr = FakeStderr()
        self.terminated = False
        self.killed = False
        self._done = asyncio.Event()

    async def wait(self) -> int:
        await self._done.wait()
        return self.returncode if self.returncode is not None else 0

    def terminate(self) -> None:
        self.terminated = True
        self._exit(-15)

    def kill(self) -> None:
        self.killed = True
        self._exit(-9)

    def _exit(self, code: int) -> None:
        if self.returncode is None:
            self.returncode = code
        self._done.set()

    def exit_naturally(self, code: int = 1) -> None:
        self.returncode = code
        self._done.set()


class Harness:
    """Records every spawn; probes report forwarded ports as up."""

    def __init__(self) -> None:
        self.spawned: list[FakeProc] = []
        self.argvs: list[list[str]] = []
        self.config_snapshots: list[tuple[str, int, str]] = []  # path, mode, text
        self._paths_seen: set[str] = set()

    async def spawn(self, argv: list[str]) -> FakeProc:
        self.argvs.append(list(argv))
        cfg = argv[argv.index("-F") + 1]
        if cfg not in self._paths_seen:
            self._paths_seen.add(cfg)
            mode = os.stat(cfg).st_mode & 0o777
            self.config_snapshots.append((cfg, mode, Path(cfg).read_text(encoding="utf-8")))
        proc = FakeProc(pid=1000 + len(self.spawned), argv=list(argv))
        self.spawned.append(proc)
        return proc

    async def probe(self, host: str, port: int) -> bool:
        return port in {RCON_PORT, AGENT_PORT}

    def config_existed_at_spawn(self, index: int) -> bool:
        cfg = self.argvs[index][self.argvs[index].index("-F") + 1]
        return any(path == cfg for path, _mode, _text in self.config_snapshots)


def make_settings(**overrides) -> Settings:
    base = dict(
        tunnel_mode="ssh",
        ssh_host="ssh.app.daytona.io",
        ssh_token_minutes=1440,
        ssh_connect_timeout=2.0,
        ssh_renew_margin=600.0,
        ssh_bind_mc="0.0.0.0",
        public_host="",
        public_port=PUBLIC_PORT,
        mc_port=25565,
        rcon_port=RCON_PORT,
        agent_ws_port=AGENT_PORT,
    )
    base.update(overrides)
    return Settings(**base)


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


async def wait_for(predicate, timeout: float = 3.0, interval: float = 0.02) -> bool:
    """Bounded await of a condition (no busy-wait: yields to the loop)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


# -------------------------------------------------------------------- scenario
async def main() -> int:
    failures = 0
    harness = Harness()
    client = FakeClient()
    saved = (ssh_tunnel._default_spawn, ssh_tunnel._default_probe)

    tunnel = SSHTunnel(
        make_settings(),
        client=client,
        spawn=harness.spawn,
        probe=harness.probe,
        backoff_base=0.05,
    )

    try:
        # --- 1. start + forwards come up -----------------------------------
        await tunnel.start()
        await tunnel.start()  # idempotent: must not spawn a second child
        up = await tunnel.wait_ready(timeout=3.0)
        ok = check("start() brings one ssh child up", up and len(harness.spawned) == 1,
                   f"spawned={len(harness.spawned)}")
        failures += 0 if ok else 1

        ok = check("start() is idempotent (no extra spawn)", len(harness.spawned) == 1,
                   f"spawned={len(harness.spawned)}")
        failures += 0 if ok else 1

        ok = check("token requested with SSH_TOKEN_MINUTES", client.minutes == [1440],
                   str(client.minutes))
        failures += 0 if ok else 1

        # --- 2. argv carries no credential ---------------------------------
        argv = harness.argvs[0]
        ok = check("argv starts with ssh -N -F <config> <alias>",
                   argv[:2] == ["ssh", "-N"] and argv[2] == "-F" and argv[4] == ssh_tunnel.SSH_ALIAS,
                   repr([a if i != 3 else "<config>" for i, a in enumerate(argv)]))
        failures += 0 if ok else 1

        ok = check("token absent from ssh argv", not any(TOKEN in part for part in argv),
                   "argv leaked the token" if any(TOKEN in p for p in argv) else "")
        failures += 0 if ok else 1

        ok = check("token absent from argv join", TOKEN not in " ".join(argv))
        failures += 0 if ok else 1

        # --- 3. config file was 0600 and correct ---------------------------
        path, mode, text = harness.config_snapshots[0]
        ok = check("ssh config created with mode 0600", mode == 0o600, oct(mode))
        failures += 0 if ok else 1

        ok = check("ssh config contains 'User <token>'", f"User {TOKEN}" in text)
        failures += 0 if ok else 1

        expected_forwards = [
            f"LocalForward 0.0.0.0:{PUBLIC_PORT}:127.0.0.1:25565",
            f"LocalForward 127.0.0.1:{RCON_PORT}:127.0.0.1:{RCON_PORT}",
            f"LocalForward 127.0.0.1:{AGENT_PORT}:127.0.0.1:{AGENT_PORT}",
        ]
        missing = [line for line in expected_forwards if line not in text]
        ok = check("ssh config has the three LocalForward lines", not missing,
                   f"missing={missing}")
        failures += 0 if ok else 1

        ok = check("ssh config sets ExitOnForwardFailure + ConnectTimeout",
                   "ExitOnForwardFailure yes" in text and "ConnectTimeout" in text)
        failures += 0 if ok else 1

        ok = check("ssh config pins StrictHostKeyChecking/UserKnownHostsFile",
                   "StrictHostKeyChecking no" in text and "UserKnownHostsFile /dev/null" in text)
        failures += 0 if ok else 1

        ok = check("ssh config deleted once ssh is up", not Path(path).exists(), path)
        failures += 0 if ok else 1

        # --- 4. status() shape + no leak ------------------------------------
        snapshot = tunnel.status()
        required = {
            "mode", "running", "pid", "forwards", "public_endpoint",
            "token_expires_at", "last_error", "restarts", "connected_since",
        }
        missing_keys = sorted(required - set(snapshot))
        ok = check("status() exposes the documented keys", not missing_keys,
                   f"missing={missing_keys}")
        failures += 0 if ok else 1

        ok = check("status() reports running with a pid and forwards",
                   snapshot["running"] is True and isinstance(snapshot["pid"], int)
                   and len(snapshot["forwards"]) == 3,
                   f"pid={snapshot['pid']} forwards={snapshot['forwards']}")
        failures += 0 if ok else 1

        ok = check("status() public_endpoint reflects bind/public port",
                   snapshot["public_endpoint"].endswith(f":{PUBLIC_PORT}"),
                   snapshot["public_endpoint"])
        failures += 0 if ok else 1

        dump = json.dumps(snapshot)
        ok = check("status() JSON dump contains no token", TOKEN not in dump and "token " not in dump)
        failures += 0 if ok else 1

        # --- 5. child exit -> restart counter -------------------------------
        harness.spawned[0].exit_naturally(255)
        restarted = await wait_for(
            lambda: len(harness.spawned) >= 2 and tunnel.status()["restarts"] >= 1,
            timeout=5.0,
        )
        snap2 = tunnel.status()
        ok = check("child exit triggers a restart", restarted and len(harness.spawned) == 2,
                   f"spawned={len(harness.spawned)}")
        failures += 0 if ok else 1

        ok = check("restarts counter incremented", snap2["restarts"] == 1,
                   f"restarts={snap2['restarts']}")
        failures += 0 if ok else 1

        ok = check("last_error records the exit code", "255" in snap2["last_error"],
                   repr(snap2["last_error"]))
        failures += 0 if ok else 1

        ok = check("new ssh config is also deleted",
                   not any(Path(p).exists() for p, _m, _t in harness.config_snapshots))
        failures += 0 if ok else 1

        ok = check("replacement config still 0600 and no token in argv",
                   harness.config_snapshots[-1][1] == 0o600
                   and not any(TOKEN in p for p in harness.argvs[-1]))
        failures += 0 if ok else 1

        # --- 6. successful renewal refreshes the token ----------------------
        # Fresh tunnel (the main one stays up for the stop checks): the first
        # token expires inside the renew margin, so the supervisor must fetch a
        # new one and respawn without counting a restart.
        renew_harness = Harness()
        renew_client = FakeClient(
            tokens=(TOKEN, RENEWED_TOKEN), short_expiry_first=0.5
        )
        renew_tunnel = SSHTunnel(
            make_settings(),
            client=renew_client,
            spawn=renew_harness.spawn,
            probe=renew_harness.probe,
            backoff_base=0.05,
        )
        await renew_tunnel.start()
        renewed = await wait_for(
            lambda: len(renew_harness.spawned) >= 2, timeout=5.0
        )
        ok = check("token inside the renew margin triggers a renew + respawn",
                   renewed and renew_client.calls >= 2,
                   f"spawned={len(renew_harness.spawned)} token_calls={renew_client.calls}")
        failures += 0 if ok else 1

        up_again = await wait_for(lambda: renew_tunnel.status()["running"], timeout=5.0)
        snap_renew = renew_tunnel.status()
        ok = check("renewal is not counted as a restart",
                   up_again and snap_renew["restarts"] == 0,
                   f"restarts={snap_renew['restarts']} running={snap_renew['running']}")
        failures += 0 if ok else 1

        newest = renew_harness.config_snapshots[-1][2]
        ok = check("renewed config carries the new token, not the old one",
                   f"User {RENEWED_TOKEN}" in newest and f"User {TOKEN}\n" not in newest)
        failures += 0 if ok else 1

        ok = check("new token never reaches argv",
                   not any(RENEWED_TOKEN in part for part in renew_harness.argvs[-1]))
        failures += 0 if ok else 1

        ok = check("old ssh child was terminated on renewal",
                   renew_harness.spawned[0].returncode is not None)
        failures += 0 if ok else 1

        ok = check("renewed config is 0600 and already deleted",
                   renew_harness.config_snapshots[-1][1] == 0o600
                   and not Path(renew_harness.config_snapshots[-1][0]).exists())
        failures += 0 if ok else 1

        await renew_tunnel.stop()

        # --- 7. failed renewal never kills a working tunnel ------------------
        fail_harness = Harness()
        fail_client = FakeClient(fail_from=2, short_expiry_first=0.5)
        fail_tunnel = SSHTunnel(
            make_settings(),
            client=fail_client,
            spawn=fail_harness.spawn,
            probe=fail_harness.probe,
            backoff_base=0.05,
        )
        await fail_tunnel.start()
        await fail_tunnel.wait_ready(timeout=3.0)
        retried = await wait_for(
            lambda: fail_client.calls >= 2, timeout=5.0
        )
        await asyncio.sleep(0.2)
        alive = fail_harness.spawned and fail_harness.spawned[0].returncode is None
        ok = check("failed renewal keeps the working tunnel alive",
                   retried and bool(alive) and len(fail_harness.spawned) == 1,
                   f"token_calls={fail_client.calls} spawned={len(fail_harness.spawned)} "
                   f"rc={fail_harness.spawned[0].returncode}")
        failures += 0 if ok else 1

        ok = check("failed renewal is not retried immediately",
                   fail_tunnel._renew_retry_at is not None
                   and fail_tunnel._renew_retry_at > time.time() + 30,
                   f"retry_at={fail_tunnel._renew_retry_at}")
        failures += 0 if ok else 1

        fail_status = fail_tunnel.status()
        ok = check("failed renewal surfaces a redacted error",
                   "401" in fail_status["last_error"]
                   and TOKEN not in json.dumps(fail_status),
                   repr(fail_status["last_error"]))
        failures += 0 if ok else 1

        ok = check("tunnel still reports running after a failed renewal",
                   fail_status["running"] is True)
        failures += 0 if ok else 1

        await fail_tunnel.stop()

        # --- 8. clean stop ---------------------------------------------------
        # Rebuild the main tunnel's last child reference for the stop checks.
        proc = harness.spawned[-1]
        await tunnel.stop()
        stopped = tunnel.status()
        ok = check("stop() terminates the child", proc.terminated or proc.returncode is not None,
                   f"terminated={proc.terminated} rc={proc.returncode}")
        failures += 0 if ok else 1

        ok = check("stop() leaves status not running and no pid",
                   stopped["running"] is False and stopped["pid"] is None,
                   f"running={stopped['running']} pid={stopped['pid']}")
        failures += 0 if ok else 1

        ok = check("stop() reaps the supervisor task",
                   tunnel._task is None and tunnel._proc is None)
        failures += 0 if ok else 1

        ok = check("stop() clears the in-memory token", tunnel._secret == "")
        failures += 0 if ok else 1

        ok = check("stop() leaves no temp ssh config behind",
                   tunnel._config_path is None
                   and not any(Path(p).exists() for p, _m, _t in harness.config_snapshots))
        failures += 0 if ok else 1

        stray = [p for p in ssh_tunnel._TEMP_CONFIGS if Path(p).exists()]
        ok = check("no registered temp config survives", not stray, str(stray))
        failures += 0 if ok else 1
    finally:
        ssh_tunnel._default_spawn, ssh_tunnel._default_probe = saved
        try:
            await tunnel.stop()
        except Exception:
            pass

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL SSH TUNNEL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
