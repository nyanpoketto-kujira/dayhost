"""sync.py execution checks — run standalone: `python scripts/check_sync.py`

Proves:
  a) SYNC_ENABLED=false  -> plan is built, executed=false, zero subprocesses.
  b) SYNC_ENABLED=true + a nonexistent binary -> clean per-step error, no
     traceback, no crash.
  c) allow-list refuses a non-allow-listed binary even when it exists.
  d) SYNC_ENABLED=true with real `tar` -> actually runs (uses a temp dir).

Exit 0 = pass. No network, no writes outside a temp directory.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402
from app.services import sync  # noqa: E402


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


async def main() -> int:
    failures = 0
    base = get_settings()

    tmp = Path(tempfile.mkdtemp(prefix="dayhost-sync-test-"))
    settings = replace(
        base,
        backup_dir=str(tmp / "backups"),
        sync_world_src=str(tmp / "data/world"),
        sync_world_dst=str(tmp / "data"),
        sync_enabled=False,
        sync_timeout=20,
        rclone_remote="",
        tunnel_mode="frp",
    )
    (tmp / "data" / "world").mkdir(parents=True)
    (tmp / "data" / "world" / "level.dat").write_bytes(b"fake-level-data")

    print("== (a) SYNC_ENABLED=false: plan only ==")
    report = await sync.pre_stop_backup(settings)
    failures += 0 if check("executed == False", report["executed"] is False) else 1
    failures += 0 if check("results list empty (nothing spawned)", report["results"] == []) else 1
    failures += 0 if check("plan still built", len(report["steps"]) >= 2, str(report["steps"])) else 1
    print(f"   direction={report['direction']} steps_total={report['steps_total']}")
    for step in report["steps"]:
        print(f"     plan: {step}")

    print("\n== (b) SYNC_ENABLED=true, binary missing ==")
    enabled = replace(settings, sync_enabled=True)
    original_which = sync.shutil.which
    try:
        # Simulate `tar` absent from PATH.
        sync.shutil.which = lambda name, *a, **k: None if name == "tar" else original_which(name, *a, **k)
        report = await sync.pre_stop_backup(enabled)
    finally:
        sync.shutil.which = original_which
    failures += 0 if check("no crash", isinstance(report, dict)) else 1
    failures += 0 if check("ok == False", report.get("ok") is False, str(report.get("error"))) else 1
    failures += 0 if check(
        "skipped with a clear message",
        any("not found in PATH" in (r.get("error") or "") for r in report["results"]),
        str([r.get("error") for r in report["results"]]),
    ) else 1
    # mkdir is allow-listed and present, so it does run; tar is what fails.
    failures += 0 if check("reported as incomplete", report.get("skipped_step") is True) else 1

    print("\n== (c) allow-list refuses a non-listed binary ==")
    plan = sync.SyncPlan(direction="backup", steps=[["sh", "-c", "echo pwned"]])
    report = await sync.run_plan(plan, enabled)
    failures += 0 if check("sh refused (ok False)", report.get("ok") is False) else 1
    failures += 0 if check(
        "allow-list message present",
        "not in the allow-list" in (report["results"][0].get("error") or ""),
        str(report["results"][0].get("error")),
    ) else 1
    failures += 0 if check("nothing executed", report["executed"] is False) else 1

    print("\n== (d) real tar run (temp dir) ==")
    report = await sync.run_plan(sync.build_backup_plan(enabled), enabled)
    failures += 0 if check("ok == True", report.get("ok") is True, str(report.get("error"))) else 1
    failures += 0 if check("executed == True", report.get("executed") is True) else 1
    archive = tmp / "backups" / "world-latest.tar.gz"
    print(f"   archive: {archive}")
    failures += 0 if check("archive created", archive.exists() and archive.stat().st_size > 0) else 1
    for r in report["results"]:
        print(f"   [{r['returncode']}] {r['argv']} ({r['duration_ms']} ms)")

    print("\n== (e) restore plan executes onto a temp target ==")
    restore = await sync.run_plan(sync.build_restore_plan(enabled), enabled)
    failures += 0 if check(
        "restore ok", restore.get("ok") is True, str(restore.get("error"))
    ) else 1
    restored = tmp / "data" / "world" / "level.dat"
    failures += 0 if check(
        "world extracted back", restored.exists() and restored.read_bytes() == b"fake-level-data"
    ) else 1

    print(f"\ntemp dir: {tmp}")
    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL SYNC CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
