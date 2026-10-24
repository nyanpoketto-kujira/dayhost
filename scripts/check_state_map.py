"""State-map regression check — run standalone: `python scripts/check_state_map.py`.

Asserts that all 22 official Daytona `SandboxState` values map to a deliberate
DayHost canonical state, that no real value silently degrades to UNKNOWN, and
that unknown garbage still degrades to UNKNOWN without raising (Fase 1 rule).

Exit code 0 = all good, 1 = failure. Prints a full table either way.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.daytona_client import (  # noqa: E402
    BUSY_STATES,
    LIVE_STATES,
    STATE_UNKNOWN,
    _STATE_MAP,
    normalise_state,
)

# Verbatim enum from docs/daytona-api.md (official API v0.225), 22 values.
OFFICIAL_STATES: tuple[str, ...] = (
    "creating", "restoring", "destroyed", "destroying", "started", "stopped",
    "starting", "stopping", "error", "build_failed", "pending_build",
    "building_snapshot", "unknown", "pulling_snapshot", "archived", "archiving",
    "resizing", "snapshotting", "forking", "pausing", "paused", "resuming",
)

# Values that are *supposed* to land on UNKNOWN. Anything else mapping to
# UNKNOWN is a missed case (the Fase 1 bug we are fixing).
EXPECTED_UNKNOWN = {"unknown", "destroyed"}

EXPECTED = {
    "creating": "STARTING",
    "restoring": "STARTING",
    "destroyed": "UNKNOWN",
    "destroying": "STOPPING",
    "started": "RUNNING",
    "stopped": "STOPPED",
    "starting": "STARTING",
    "stopping": "STOPPING",
    "error": "ERROR",
    "build_failed": "ERROR",
    "pending_build": "BUSY",
    "building_snapshot": "BUSY",
    "unknown": "UNKNOWN",
    "pulling_snapshot": "STARTING",
    "archived": "ARCHIVED",
    "archiving": "ARCHIVED",
    "resizing": "BUSY",
    "snapshotting": "BUSY",
    "forking": "BUSY",
    "pausing": "STOPPING",
    "paused": "STOPPING",
    "resuming": "STARTING",
}


def main() -> int:
    failures: list[str] = []

    print(f"{'daytona state':<20} {'mapped':<10} {'expected':<10} result")
    print("-" * 56)
    for raw in OFFICIAL_STATES:
        got = normalise_state(raw)
        want = EXPECTED[raw]
        ok = got == want
        print(f"{raw:<20} {got:<10} {want:<10} {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures.append(f"{raw}: got {got}, expected {want}")

    print("-" * 56)
    print(f"checked {len(OFFICIAL_STATES)} official states, "
          f"{len(OFFICIAL_STATES) - len(failures)} passed, {len(failures)} failed")

    # No real state may fall through to UNKNOWN by accident.
    accidental = [s for s in OFFICIAL_STATES
                  if normalise_state(s) == STATE_UNKNOWN and s not in EXPECTED_UNKNOWN]
    if accidental:
        failures.append(f"unintended UNKNOWN: {accidental}")
        print(f"FAIL unintended UNKNOWN: {accidental}")
    else:
        print("PASS no official state degrades to UNKNOWN unintentionally")

    # Every official value must be an explicit _STATE_MAP key (no luck via alias).
    missing_keys = [s for s in OFFICIAL_STATES if s not in _STATE_MAP]
    if missing_keys:
        failures.append(f"missing explicit map keys: {missing_keys}")
        print(f"FAIL missing explicit map keys: {missing_keys}")
    else:
        print("PASS all 22 values are explicit _STATE_MAP keys")

    # Defensive rule from Fase 1 must still hold.
    for garbage in ("", None, "exploded", 123, {"state": "wat"}, {}, [], object()):
        try:
            mapped = normalise_state(garbage)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"normalise_state({garbage!r}) raised {exc!r}")
            print(f"FAIL normalise_state({garbage!r}) raised {exc!r}")
            continue
        if mapped != STATE_UNKNOWN:
            failures.append(f"normalise_state({garbage!r}) = {mapped}, expected UNKNOWN")
    print("PASS unknown/garbage inputs degrade to UNKNOWN without raising")

    # LIVE_STATES sanity: STARTING-family live, ARCHIVED/STOPPED not.
    if "RUNNING" not in LIVE_STATES or "STARTING" not in LIVE_STATES:
        failures.append(f"LIVE_STATES missing RUNNING/STARTING: {LIVE_STATES}")
    if LIVE_STATES & {"STOPPED", "ARCHIVED", "STOPPING", "ERROR"}:
        failures.append(f"LIVE_STATES wrongly includes offline states: {LIVE_STATES}")
    if "BUSY" in LIVE_STATES:
        failures.append("LIVE_STATES must not include BUSY")
    if "BUSY" not in BUSY_STATES:
        failures.append("BUSY_STATES must include BUSY")
    print(f"PASS LIVE_STATES={sorted(LIVE_STATES)} BUSY_STATES={sorted(BUSY_STATES)}")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
