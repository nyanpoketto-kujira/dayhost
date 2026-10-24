"""Logging checks — run standalone: `python scripts/check_logging.py`

Proves, automatically, the four promises `app/logging_setup.py` makes:

  1. rotation   — with a small `LOG_MAX_BYTES`, `.log.1` appears, and
                  `LOG_BACKUP_COUNT` is honoured (no `.log.N+1` ever piles up).
  2. no doubles — every event is written to the file **exactly once**. This is
                  the classic "root handler + uvicorn handler" bug: root,
                  `uvicorn`, `uvicorn.error` and `uvicorn.access` must each
                  contribute one line, not two.
  3. no losses  — every event written inside the retention window is readable
                  back from the rotated files, byte-for-byte.
  4. fallback   — an unwritable `LOG_FILE` (directory uncreatable, or the file
                  unopenable) returns `fallback_stderr=True`, emits one tidy
                  warning, and never raises; logging still works afterwards.
  5. json       — with `LOG_FORMAT=json` every line is valid JSON and carries
                  the documented keys (`ts`/`level`/`logger`/`msg`).

Exit 0 = pass. Stdlib only, no network, and every file this script creates lives
in one temp directory that is removed before exit.
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import re
import shutil
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import logging_setup  # noqa: E402
from app.config import get_settings  # noqa: E402


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


# --- helpers ---------------------------------------------------------------


def setup(settings, force: bool = True, stream: io.StringIO | None = None) -> dict:
    """Run setup_logging with the console handler pointed at `stream`.

    It is built from `sys.stderr` inside setup_logging, so we swap that
    temporarily: the script's own output stays readable and the caller can
    still inspect what the stderr sink received.
    """
    logging_setup.close_logging()
    sink = stream if stream is not None else io.StringIO()
    previous, sys.stderr = sys.stderr, sink
    try:
        return logging_setup.setup_logging(settings, force=force)
    finally:
        sys.stderr = previous


def flush() -> None:
    """Push buffered records to disk so the files can be read back."""
    targets = [logging.getLogger()] + [
        logging.getLogger(name) for name in logging_setup.UVICORN_LOGGERS
    ]
    for logger_ in targets:
        for handler in list(logger_.handlers):
            try:
                handler.flush()
            except Exception:  # noqa: BLE001 - a closed handler is fine here
                pass


def log_files(path: Path) -> list[Path]:
    """The live file plus every numbered backup, in the order they exist."""
    found: list[Path] = [path] if path.exists() else []
    index = 1
    while True:
        rotated = path.with_name(f"{path.name}.{index}")
        if not rotated.exists():
            break
        found.append(rotated)
        index += 1
    return found


def read_lines(path: Path) -> list[str]:
    lines: list[str] = []
    for file in log_files(path):
        lines.extend(
            line
            for line in file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return lines


def emit(tag: str, count: int, *, logger_name: str = "dayhost", pad: int = 60) -> list[str]:
    """Write `count` uniquely identifiable events; return their messages."""
    log = logging.getLogger(logger_name)
    messages = [f"{tag}-{i:03d}-" + "x" * pad for i in range(count)]
    for message in messages:
        log.info(message)
    return messages


def ids(lines: list[str], tag: str) -> list[int]:
    pattern = re.compile(rf"{re.escape(tag)}-(\d+)-")
    found: list[int] = []
    for line in lines:
        match = pattern.search(line)
        if match:
            found.append(int(match.group(1)))
    return found


def main() -> int:
    failures = 0
    base = get_settings()
    root_tmp = Path(tempfile.mkdtemp(prefix="dayhost-logging-check-"))
    log_path = root_tmp / "data" / "dayhost.log"

    try:
        # ---------------------------------------------------------------- (1)
        print("== (1) rotation + no duplicate + no lost lines (plain) ==")
        settings = replace(
            base,
            log_file=str(log_path),
            log_max_bytes=1000,
            log_backup_count=50,
            log_level="INFO",
            log_format="plain",
        )
        result = setup(settings)
        failures += 0 if check(
            "setup_logging reports a file sink",
            result["file"] == str(log_path) and result["fallback_stderr"] is False,
            str(result),
        ) else 1
        written = emit("plain", 30)
        flush()

        files = log_files(log_path)
        lines = read_lines(log_path)
        found = ids(lines, "plain")
        print(f"   log files: {[f.name for f in files]}")
        print(f"   lines readable: {len(lines)}  unique events: {len(set(found))}")

        failures += 0 if check(
            "rotation happened (dayhost.log.1 exists)",
            log_path.with_name(log_path.name + ".1").exists(),
            f"{len(files)} file(s)",
        ) else 1
        failures += 0 if check(
            "retention respected (<= LOG_BACKUP_COUNT + 1 files)",
            len(files) <= settings.log_backup_count + 1,
            f"{len(files)} <= {settings.log_backup_count + 1}",
        ) else 1
        failures += 0 if check(
            "no duplicate lines (unique events == total lines)",
            len(found) == len(set(found)) == len(lines),
            f"{len(lines)} lines, {len(set(found))} unique",
        ) else 1
        failures += 0 if check(
            "no lost lines (every event readable back)",
            set(found) == set(range(len(written))),
            f"{len(set(found))}/{len(written)} events recovered",
        ) else 1

        # ---------------------------------------------------------------- (2)
        print("\n== (2) one line per event across root + uvicorn loggers ==")
        path2 = root_tmp / "data" / "double.log"
        settings2 = replace(settings, log_file=str(path2))
        err2 = io.StringIO()
        setup(settings2, stream=err2)

        sinks = [
            ("", "root-event"),                       # root logger itself
            ("dayhost", "app-event"),                 # app logger -> root
            ("uvicorn.access", "access-event"),       # uvicorn access
            ("uvicorn.error", "error-event"),         # uvicorn error -> uvicorn
            ("uvicorn", "uvicorn-event"),             # uvicorn parent
        ]
        for logger_name, message in sinks:
            logging.getLogger(logger_name or None).info(message)
        flush()

        lines2 = read_lines(path2)
        lines_err = [ln for ln in err2.getvalue().splitlines() if ln.strip()]
        print(f"   lines in file: {len(lines2)}   lines on stderr: {len(lines_err)}")
        for _, message in sinks:
            in_file = sum(1 for line in lines2 if message in line)
            in_err = sum(1 for line in lines_err if message in line)
            print(f"     {message:<16} file x{in_file}  stderr x{in_err}")
        failures += 0 if check(
            "each logger wrote its event exactly once to the file",
            all(sum(1 for line in lines2 if m in line) == 1 for _, m in sinks),
            f"{len(lines2)} lines for {len(sinks)} events",
        ) else 1
        failures += 0 if check(
            "total line count == event count (no hidden double sink)",
            len(lines2) == len(sinks),
            f"{len(lines2)} == {len(sinks)}",
        ) else 1
        failures += 0 if check(
            "each logger wrote its event exactly once to stderr",
            all(sum(1 for line in lines_err if m in line) == 1 for _, m in sinks)
            and len(lines_err) == len(sinks),
            f"{len(lines_err)} stderr lines for {len(sinks)} events",
        ) else 1

        # ---------------------------------------------------------------- (3)
        print("\n== (3) LOG_BACKUP_COUNT is honoured under heavy rotation ==")
        path3 = root_tmp / "data" / "prune.log"
        settings3 = replace(settings, log_file=str(path3), log_max_bytes=500, log_backup_count=2)
        setup(settings3)
        emit("prune", 400)
        flush()

        files3 = log_files(path3)
        print(f"   log files: {[f.name for f in files3]}")
        failures += 0 if check(
            "file count == backup_count + 1",
            len(files3) == settings3.log_backup_count + 1,
            f"{len(files3)} == {settings3.log_backup_count + 1}",
        ) else 1
        failures += 0 if check(
            "no dayhost.log.3 (index beyond backup_count)",
            not path3.with_name(path3.name + ".3").exists(),
        ) else 1
        kept = ids(read_lines(path3), "prune")
        failures += 0 if check(
            "pruning still kept the newest event (399 survived)",
            kept and max(kept) == 399,
            f"kept {len(kept)} event(s), newest={max(kept) if kept else None}",
        ) else 1

        # ---------------------------------------------------------------- (4)
        print("\n== (4) JSON format: valid object per line, documented keys ==")
        path4 = root_tmp / "data" / "json.log"
        settings4 = replace(settings, log_file=str(path4), log_format="json")
        setup(settings4)
        written4 = emit("json", 12)
        flush()

        lines4 = read_lines(path4)
        parsed: list[dict] = []
        bad: list[str] = []
        for line in lines4:
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError as exc:
                bad.append(f"{exc}: {line[:60]}")
        required = ("ts", "level", "logger", "msg")
        missing = [
            f"{obj.get('msg', '?')}:{[k for k in required if k not in obj]}"
            for obj in parsed
            if any(k not in obj for k in required)
        ]
        print(f"   lines={len(lines4)} parsed={len(parsed)}")
        if parsed:
            print(f"   sample: {json.dumps(parsed[0], ensure_ascii=False)[:160]}")
        failures += 0 if check(
            "every line is valid JSON",
            not bad and len(parsed) == len(written4),
            bad[0] if bad else f"{len(parsed)}/{len(written4)}",
        ) else 1
        failures += 0 if check(
            "every line has ts/level/logger/msg",
            not missing,
            missing[0] if missing else "",
        ) else 1
        failures += 0 if check(
            "no duplicate json lines",
            len({obj.get("msg") for obj in parsed}) == len(parsed),
            f"{len(parsed)} lines",
        ) else 1
        failures += 0 if check(
            "json level/logger values are sane",
            all(obj.get("level") == "INFO" and obj.get("logger") == "dayhost" for obj in parsed),
            str({obj.get("level") for obj in parsed}),
        ) else 1

        # ---------------------------------------------------------------- (5)
        print("\n== (5) unwritable LOG_FILE -> stderr fallback, no crash ==")

        # (5a) the parent directory cannot be created (a file sits in the way,
        #      which fails even for root — unlike chmod-based tests).
        blocker = root_tmp / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        settings5 = replace(settings, log_file=str(blocker / "nested" / "dayhost.log"))

        buffer = io.StringIO()
        result5 = None
        crashed: str | None = None
        with contextlib.redirect_stderr(buffer):
            try:
                result5 = logging_setup.setup_logging(settings5, force=True)
                logging.getLogger("dayhost").warning("still alive after fallback")
            except Exception as exc:  # noqa: BLE001 - the whole point is no raise
                crashed = f"{type(exc).__name__}: {exc}"
        stderr_a = buffer.getvalue()

        print(f"   warning: {stderr_a.strip()[:160]}")
        failures += 0 if check("no exception raised", crashed is None, crashed or "") else 1
        failures += 0 if check(
            "fallback_stderr == True and file is None",
            isinstance(result5, dict)
            and result5.get("fallback_stderr") is True
            and result5.get("file") is None,
            str(result5),
        ) else 1
        failures += 0 if check(
            "reason is reported, not swallowed",
            bool(result5 and result5.get("warning")),
            str((result5 or {}).get("warning"))[:120],
        ) else 1
        failures += 0 if check(
            "exactly one tidy 'falling back to stderr' warning",
            stderr_a.count("falling back to stderr") == 1
            and not any("Traceback" in ln for ln in stderr_a.splitlines()),
            f"{len([ln for ln in stderr_a.splitlines() if ln.strip()])} line(s)",
        ) else 1
        failures += 0 if check(
            "logging still works after the fallback",
            stderr_a.count("still alive after fallback") == 1,
        ) else 1
        failures += 0 if check(
            "uvicorn loggers are not wired when there is no file sink",
            (result5 or {}).get("uvicorn_handlers") == [],
        ) else 1
        logging_setup.close_logging()

        # (5b) the parent exists but the target itself cannot be opened as a file.
        as_dir = root_tmp / "asdir" / "dayhost.log"
        as_dir.mkdir(parents=True)
        settings6 = replace(settings, log_file=str(as_dir))
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            try:
                result6 = logging_setup.setup_logging(settings6, force=True)
                crashed = None
            except Exception as exc:  # noqa: BLE001
                crashed = f"{type(exc).__name__}: {exc}"
                result6 = None
        stderr_b = buffer.getvalue()
        print(f"   warning: {stderr_b.strip()[:160]}")
        failures += 0 if check(
            "unopenable file also falls back cleanly",
            crashed is None
            and isinstance(result6, dict)
            and result6.get("fallback_stderr") is True
            and "falling back to stderr" in stderr_b,
            crashed or str((result6 or {}).get("warning"))[:120],
        ) else 1
        logging_setup.close_logging()

    finally:
        logging_setup.close_logging()
        shutil.rmtree(root_tmp, ignore_errors=True)

    print(f"\ntemp dir removed: {not root_tmp.exists()} ({root_tmp})")
    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL LOGGING CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
