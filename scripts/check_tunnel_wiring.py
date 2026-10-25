"""Offline checks for the SSHTunnel <-> app lifecycle wiring.

Run standalone: `python scripts/check_tunnel_wiring.py`

No network, no ssh, no uvicorn, no real DB. Everything is proven by
construction against the *source text* or against in-memory fakes:

  a) `app/main.py` imports SSHTunnel lazily, inside the TUNNEL_MODE branch
     (an indented import), never at module scope,
  b) the non-ssh path explicitly assigns `app.state.tunnel = None`,
  c) shutdown stops the tunnel before `await close_client()`,
  d) `_live_tunnel` returns the status dict, and degrades to None when
     `app.state.tunnel` is missing or when `.status()` raises,
  e) `gather_status` still returns every pre-existing key (plus `tunnel_live`).

Exit 0 = pass.
"""
from __future__ import annotations

import asyncio
import inspect
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.routers import dashboard  # noqa: E402

MAIN_PY = REPO_ROOT / "app" / "main.py"
DASH_PY = REPO_ROOT / "app" / "routers" / "dashboard.py"

LAZY_IMPORT = "from .services.ssh_tunnel import SSHTunnel"
CLOSE_CLIENT = "await close_client()"
STOP_CALL = "await ssh_tunnel.stop()"


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


def _find_all(haystack: str, needle: str) -> list[int]:
    """Every start index of ``needle`` in ``haystack`` (non-overlapping)."""
    hits: list[int] = []
    start = 0
    while True:
        idx = haystack.find(needle, start)
        if idx == -1:
            return hits
        hits.append(idx)
        start = idx + len(needle)


# --------------------------------------------------------------------- fakes
class FakeTunnel:
    """Minimal SSHTunnel-shaped object for the dashboard helper."""

    def __init__(self, snap: dict | None = None, raises: bool = False) -> None:
        self._snap = snap if snap is not None else {"running": True, "pid": 4242}
        self._raises = raises
        self.calls = 0

    def status(self) -> dict:
        self.calls += 1
        if self._raises:
            raise RuntimeError("status() exploded")
        return self._snap


class FakeState:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class FakeApp:
    def __init__(self, state: FakeState) -> None:
        self.state = state


class FakeRequest:
    """Just enough of a Starlette Request for `_live_tunnel`."""

    def __init__(self, app=None, state=None) -> None:
        if app is not None:
            self.app = app
        elif state is not None:
            self.app = FakeApp(state)


class FakeTunnelProbe:
    """Stand-in for `tunnel.probe(settings)` — an object with as_dict()."""

    def __init__(self) -> None:
        self.mode = "frp"
        self.socket_open = True
        self.public_endpoint = "203.0.113.9:34212"

    def as_dict(self) -> dict:
        return {
            "mode": self.mode,
            "socket_open": self.socket_open,
            "socket_error": "",
            "latency_ms": 12.5,
            "frpc_running": True,
            "frpc_log_tail": "",
            "public_endpoint": self.public_endpoint,
        }


class FakeDaytona:
    def get_status(self) -> dict:
        return {
            "ok": True,
            "state": "running",
            "error_reason": None,
            "recoverable": True,
            "desired_state": "started",
        }


EXPECTED_STATUS_KEYS = {
    "ok",
    "daytona",
    "sandbox_live",
    "sandbox_busy",
    "sandbox_error_reason",
    "sandbox_recoverable",
    "sandbox_desired_state",
    "ping",
    "tunnel",
    "tunnel_live",
    "mc_endpoint",
    "public_endpoint",
    "stub_console",
    "sync_enabled",
}


def source_checks(failures: int) -> int:
    """(a), (b), (c): prove the wiring by reading app/main.py."""
    main_src = MAIN_PY.read_text(encoding="utf-8")
    lines = main_src.splitlines()

    # (a) lazy, indented import inside a branch — and no module-scope import.
    indented = [
        n for n, line in enumerate(lines, 1)
        if line.strip() == LAZY_IMPORT and line[:1].isspace()
    ]
    module_scope = [
        n for n, line in enumerate(lines, 1)
        if line.strip() == LAZY_IMPORT and not line[:1].isspace()
    ]
    ok = check(
        "(a) SSHTunnel is imported lazily with leading whitespace",
        len(indented) == 1,
        f"indented at line(s) {indented}, count={len(indented)}",
    )
    failures += 0 if ok else 1

    ok = check(
        "(a) SSHTunnel is NOT imported at module scope",
        not module_scope,
        f"module-scope line(s) {module_scope or '-'}",
    )
    failures += 0 if ok else 1

    # The import must sit inside the sh/ mode guard, not before it.
    guard_idx = next(
        (n for n, line in enumerate(lines, 1)
         if 'settings.tunnel_mode.strip().lower() == "ssh"' in line),
        None,
    )
    ok = check(
        '(a) the lazy import sits inside the `tunnel_mode == "ssh"` branch',
        guard_idx is not None and bool(indented) and indented[0] > guard_idx,
        f"guard line={guard_idx} import line(s)={indented or '-'}",
    )
    failures += 0 if ok else 1

    # (b) the non-ssh path assigns app.state.tunnel = None.
    none_assigns = [
        n for n, line in enumerate(lines, 1)
        if line.strip() == "app.state.tunnel = None"
    ]
    before_guard = [n for n in none_assigns if guard_idx is not None and n < guard_idx]
    ok = check(
        "(b) app.state.tunnel = None is assigned on the non-ssh path",
        bool(before_guard) and bool(guard_idx),
        f"assignment(s) at line(s) {none_assigns or '-'}, guard line={guard_idx}",
    )
    failures += 0 if ok else 1

    # (c) shutdown ordering: the *shutdown* stop() must precede close_client().
    # There is a second stop() in the start() failure path, above `yield`, so
    # both occurrences are located relative to the yield that splits the two
    # halves of the lifespan.
    yield_idx = main_src.find("\n        yield")
    stop_sites = [
        n for n in _find_all(main_src, STOP_CALL)
        if yield_idx != -1 and n > yield_idx
    ]
    close_sites = [n for n in _find_all(main_src, CLOSE_CLIENT) if yield_idx != -1 and n > yield_idx]
    stop_idx = stop_sites[0] if stop_sites else -1
    close_idx = close_sites[0] if close_sites else -1
    ok = check(
        "(c) shutdown stops the tunnel before `await close_client()`",
        stop_idx != -1 and close_idx != -1 and stop_idx < close_idx,
        f"yield@{yield_idx} stop@{stop_idx} close_client@{close_idx}",
    )
    failures += 0 if ok else 1

    ok = check(
        "(c) exactly one shutdown-half stop call (startup path is separate)",
        len(stop_sites) == 1 and len(_find_all(main_src, STOP_CALL)) == 2,
        f"shutdown-half={len(stop_sites)} total={len(_find_all(main_src, STOP_CALL))}",
    )
    failures += 0 if ok else 1

    ok = check(
        "(c) the stop call lives after `yield` (shutdown half of lifespan)",
        stop_idx != -1 and yield_idx != -1 and stop_idx > yield_idx,
        f"yield@{yield_idx} stop@{stop_idx}",
    )
    failures += 0 if ok else 1

    return failures


def helper_checks(failures: int) -> int:
    """(d): `_live_tunnel` behaviour against in-memory fakes."""
    live = dashboard._live_tunnel

    # d1 — live tunnel present: its status dict comes back verbatim.
    snap = {"mode": "ssh", "running": True, "pid": 4242, "last_error": ""}
    fake = FakeTunnel(snap)
    request = FakeRequest(state=FakeState(tunnel=fake))
    got = live(request)
    failures += 0 if check(
        "(d) _live_tunnel returns the live status dict",
        got == snap and fake.calls == 1,
        f"got={got} calls={fake.calls}",
    ) else 1

    # d2 — app.state.tunnel missing entirely.
    got = live(FakeRequest(state=FakeState()))
    failures += 0 if check(
        "(d) _live_tunnel returns None when app.state.tunnel is missing",
        got is None,
        f"got={got}",
    ) else 1

    # d3 — attribute present but None (frp/none path).
    got = live(FakeRequest(state=FakeState(tunnel=None)))
    failures += 0 if check(
        "(d) _live_tunnel returns None when app.state.tunnel is None",
        got is None,
        f"got={got}",
    ) else 1

    # d4 — status() raises: swallowed, not propagated.
    raising = FakeTunnel(raises=True)
    request = FakeRequest(state=FakeState(tunnel=raising))
    try:
        got = live(request)
    except Exception as exc:  # noqa: BLE001 - the point is that it must not
        got = f"RAISED {type(exc).__name__}"
    failures += 0 if check(
        "(d) _live_tunnel swallows a raising status() and returns None",
        got is None,
        f"got={got}",
    ) else 1

    # d5 — no app/state at all (defensive getattr chain).
    class Bare:
        pass

    try:
        got = live(Bare())
    except Exception as exc:  # noqa: BLE001
        got = f"RAISED {type(exc).__name__}"
    failures += 0 if check(
        "(d) _live_tunnel returns None for a request without app.state",
        got is None,
        f"got={got}",
    ) else 1

    return failures


def status_checks(failures: int) -> int:
    """(e): gather_status keeps every legacy key and threads tunnel_live."""
    sig = inspect.signature(dashboard.gather_status)
    ok = check(
        "(e) gather_status takes tunnel_live as an optional keyword",
        "tunnel_live" in sig.parameters,
        f"params={list(sig.parameters)}",
    )
    failures += 0 if ok else 1

    saved_probe = dashboard.tunnel.probe
    saved_client = dashboard.get_client
    live_snap = {"running": True, "pid": 4242}

    async def fake_probe(_settings):
        return FakeTunnelProbe()

    try:
        dashboard.tunnel.probe = fake_probe
        dashboard.get_client = lambda: FakeDaytona()
        status = asyncio.run(dashboard.gather_status(tunnel_live=live_snap))
    finally:
        dashboard.tunnel.probe = saved_probe
        dashboard.get_client = saved_client

    missing = sorted(EXPECTED_STATUS_KEYS - set(status))
    failures += 0 if check(
        "(e) gather_status returns every pre-existing key",
        not missing,
        f"missing={missing or '-'} keys={len(status)}",
    ) else 1

    extra = sorted(set(status) - EXPECTED_STATUS_KEYS)
    failures += 0 if check(
        "(e) gather_status has no unexpected extra key",
        not extra,
        f"extra={extra or '-'}",
    ) else 1

    failures += 0 if check(
        "(e) tunnel_live is threaded through unchanged",
        status.get("tunnel_live") == live_snap,
        f"tunnel_live={status.get('tunnel_live')}",
    ) else 1

    failures += 0 if check(
        "(e) public_endpoint still comes from tunnel.probe (frp untouched)",
        status.get("public_endpoint") == "203.0.113.9:34212",
        f"public_endpoint={status.get('public_endpoint')!r}",
    ) else 1

    failures += 0 if check(
        "(e) tunnel dict is still the observer's as_dict()",
        isinstance(status.get("tunnel"), dict)
        and status["tunnel"].get("mode") == "frp"
        and status["tunnel"].get("socket_open") is True,
        f"tunnel={status.get('tunnel')}",
    ) else 1

    failures += 0 if check(
        "(e) legacy mc_endpoint / sync_enabled / stub_console survive",
        isinstance(status.get("mc_endpoint"), str)
        and ":" in status.get("mc_endpoint", "")
        and isinstance(status.get("sync_enabled"), bool)
        and isinstance(status.get("stub_console"), bool),
        f"mc_endpoint={status.get('mc_endpoint')!r} "
        f"sync_enabled={status.get('sync_enabled')} "
        f"stub_console={status.get('stub_console')}",
    ) else 1

    return failures


def main() -> int:
    failures = 0
    failures = source_checks(failures)
    failures = helper_checks(failures)
    failures = status_checks(failures)

    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL TUNNEL WIRING CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
