"""Structured logging to a rotating file + stderr. Stdlib only.

Why this exists
---------------
Production runs under `ProtectSystem=strict` with exactly one writable path
(`/opt/dayhost/data`). Logs must land in a file that survives a restart, must
rotate on their own (the VPS has a small disk), and must never take the process
down if the path turns out to be unwritable — an unwritable log file is an
operational annoyance, not a reason to refuse to serve the dashboard.

Settings
--------
    LOG_FILE          ./data/dayhost.log   (relative to the repo root)
    LOG_MAX_BYTES     5242880              (5 MB per file)
    LOG_BACKUP_COUNT  5                    (dayhost.log.1 … .5)
    LOG_LEVEL         INFO
    LOG_FORMAT        plain | json

Format
------
plain:
    2026-10-23T22:50:01+0700 INFO    dayhost.console: message
json (one object per line, `json.dumps`, no dependency):
    {"ts": "...", "level": "INFO", "logger": "dayhost.console", "msg": "...",
     "extra": {...}}

No double logging
-----------------
uvicorn installs its own handlers on the `uvicorn*` loggers with
`propagate=False`, and `logging.basicConfig` would add a second stderr handler
to the root logger. Both are duplication sources. This module therefore:

  * **replaces** the root handlers (never appends a second stderr handler),
  * attaches the *same* file handler to the `uvicorn` / `uvicorn.access`
    loggers, whose `propagate=False` means those records are written to the
    file exactly once and never reach the root handler afterwards.

`scripts/check_logging.py` asserts one line per event in both sinks, plus
rotation/retention, no lost lines, JSON validity, and the stderr fallback.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Any, Iterable

from .config import Settings, get_settings

LOGGER_NAME = "dayhost"
UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")

# Markers so `setup_logging()` is idempotent across repeated calls (uvicorn
# --reload re-imports the app in the same process).
_FILE_ATTR = "_dayhost_file_handler"
_CONSOLE_ATTR = "_dayhost_console_handler"


class PlainFormatter(logging.Formatter):
    """timestamp LEVEL logger: message — with millisecond precision."""

    def format(self, record: logging.LogRecord) -> str:
        stamp = datetime.fromtimestamp(record.created).astimezone().isoformat(
            timespec="milliseconds"
        )
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        return f"{stamp} {record.levelname:<7} {record.name}: {message}"


class JsonFormatter(logging.Formatter):
    """One JSON object per line. `extra=` keys survive under "extra"."""

    _RESERVED = frozenset(
        {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "message", "asctime", "taskName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created).astimezone().isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._RESERVED and not key.startswith("_")
        }
        if extra:
            payload["extra"] = extra
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def build_formatter(fmt: str) -> logging.Formatter:
    return JsonFormatter() if (fmt or "").strip().lower() == "json" else PlainFormatter()


def _resolve_level(name: str) -> int:
    level = logging.getLevelName((name or "INFO").strip().upper())
    return level if isinstance(level, int) else logging.INFO


def _open_file_handler(
    settings: Settings, formatter: logging.Formatter
) -> tuple[logging.Handler | None, str | None]:
    """Create the rotating handler, or return (None, reason) — never raises."""
    path = settings.log_path
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return None, f"cannot create log directory {path.parent}: {exc}"
    try:
        handler = logging.handlers.RotatingFileHandler(
            filename=str(path),
            maxBytes=max(0, int(settings.log_max_bytes)),
            backupCount=max(0, int(settings.log_backup_count)),
            encoding="utf-8",
            delay=False,
        )
    except (OSError, ValueError) as exc:
        return None, f"cannot open log file {path}: {exc}"
    handler.setFormatter(formatter)
    setattr(handler, _FILE_ATTR, True)
    return handler, None


def _wire_uvicorn(handler: logging.Handler, console: logging.Handler) -> None:
    """Make uvicorn's loggers write to *our* sinks, exactly once each.

    uvicorn pre-installs a stderr handler on `uvicorn` and an access handler on
    `uvicorn.access` (both `propagate=False`). Left alone they would write a
    second, differently formatted copy of every uvicorn line next to ours. We
    therefore replace their handler lists with `[file, console]` and pin
    `propagate=False`, so:

        uvicorn.access  -> [file, console]              (stops, no root)
        uvicorn.error   -> (none) -> uvicorn            (file+console once)
        uvicorn         -> [file, console]              (stops, no root)
        root (app)      -> [console, file]

    Every record reaches each sink exactly once, whatever uvicorn's own
    defaults were at import time.
    """
    for name in ("uvicorn", "uvicorn.access"):
        logger = logging.getLogger(name)
        for existing in list(logger.handlers):
            logger.removeHandler(existing)
        logger.addHandler(handler)
        logger.addHandler(console)
        logger.propagate = False

    # `uvicorn.error` has never had its own handler: it inherits from `uvicorn`.
    error_logger = logging.getLogger("uvicorn.error")
    for existing in list(error_logger.handlers):
        error_logger.removeHandler(existing)
    error_logger.propagate = True


def setup_logging(
    settings: Settings | None = None, *, force: bool = False
) -> dict[str, Any]:
    """Install root + uvicorn handlers. Idempotent, never raises.

    Returns a small dict describing what happened — useful in tests and in the
    startup log line ("log file at X, or warning: falling back to stderr").
    """
    settings = settings or get_settings()
    level = _resolve_level(settings.log_level)
    formatter = build_formatter(settings.log_format)

    root = logging.getLogger()
    already = any(getattr(h, _FILE_ATTR, False) for h in root.handlers)
    quiet = not force and already

    handler, problem = (None, None)
    if not quiet:
        # Replace rather than append: basicConfig/uvicorn may already have put a
        # stderr handler on root, and appending would print every line twice.
        for existing in list(root.handlers):
            root.removeHandler(existing)
        handler, problem = _open_file_handler(settings, formatter)

    if quiet:
        handler = next(
            (h for h in root.handlers if getattr(h, _FILE_ATTR, False)), None
        )
        console = next(
            (h for h in root.handlers if getattr(h, _CONSOLE_ATTR, False)), None
        )
    else:
        console = logging.StreamHandler(stream=sys.stderr)
        console.setFormatter(formatter)
        setattr(console, _CONSOLE_ATTR, True)
        root.addHandler(console)

    if handler is not None:
        root.addHandler(handler)
        _wire_uvicorn(handler, console)
    elif not quiet:
        root.warning(
            "log file unavailable (%s) — falling back to stderr only", problem
        )

    root.setLevel(level)
    logging.getLogger(LOGGER_NAME).setLevel(level)

    # uvicorn's loggers carry their own level (set from --log-level). Only
    # make them stricter when LOG_LEVEL is stricter — never silence an
    # operator's --debug, never make --warning chatty.
    for name in UVICORN_LOGGERS:
        logger = logging.getLogger(name)
        if logger.level == logging.NOTSET or level > logger.level:
            logger.setLevel(level)

    return {
        "level": logging.getLevelName(level),
        "format": "json" if isinstance(formatter, JsonFormatter) else "plain",
        "file": str(settings.log_path) if handler is not None else None,
        "fallback_stderr": handler is None,
        "warning": problem,
        "uvicorn_handlers": list(UVICORN_LOGGERS) if handler is not None else [],
    }


def close_logging() -> None:
    """Flush + detach our handlers (tests, and a clean shutdown)."""
    root = logging.getLogger()
    for logger_ in [root] + [logging.getLogger(n) for n in UVICORN_LOGGERS]:
        for handler in list(logger_.handlers):
            if getattr(handler, _FILE_ATTR, False):
                logger_.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
    sys.stderr.flush()


def log_files(settings: Settings | None = None) -> Iterable[str]:
    """Existing files for this logger, newest first (test/diagnostic helper)."""
    settings = settings or get_settings()
    path = settings.log_path
    if path.exists():
        yield str(path)
    index = 1
    while True:
        rotated = path.with_name(f"{path.name}.{index}")
        if not rotated.exists():
            break
        yield str(rotated)
        index += 1


__all__ = [
    "JsonFormatter",
    "PlainFormatter",
    "build_formatter",
    "close_logging",
    "log_files",
    "setup_logging",
]
