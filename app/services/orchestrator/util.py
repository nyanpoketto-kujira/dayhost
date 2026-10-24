"""Small shared helpers for the fusion core (port of controller/util.py).

Ported from ``mc-orchestrator/controller/util.py`` in Fusion Phase C. Stdlib
only — no aiohttp, no httpx. The public surface is unchanged on purpose so the
ported ``storage.py`` / ``state.py`` stay diff-able against their originals.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ANSI_RE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\)|[@-_])")
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
PLUGIN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+ -]{0,148}\.jar$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def now_ts() -> float:
    return time.time()


def clean_line(text: str, max_len: int = 8000) -> str:
    text = ANSI_RE.sub("", text)
    text = CONTROL_RE.sub("", text)
    text = text.replace("\r", "")
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return text


def human_size(num: int | float) -> str:
    num = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024.0 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"


def sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            data = handle.read(chunk)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def is_safe_relpath(name: str) -> bool:
    if not name or "\x00" in name:
        return False
    if name.startswith(("/", "\\")):
        return False
    parts = re.split(r"[\\/]+", name)
    return all(part not in ("", ".", "..") for part in parts)


def resolve_under(root: Path, name: str) -> Path | None:
    """Resolve a tar member name, refusing anything outside *root*."""
    if not is_safe_relpath(name):
        return None
    root = root.resolve()
    target = (root / name).resolve()
    try:
        if os.path.commonpath([str(root), str(target)]) != str(root):
            return None
    except ValueError:
        return None
    return target


def sanitize_plugin_name(name: str) -> str | None:
    name = (name or "").strip().replace("\\", "/").split("/")[-1]
    if not PLUGIN_RE.fullmatch(name) or name in (".jar",):
        return None
    return name


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass


def read_json(path: Path, default: object = None) -> object:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return default


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on", "y")


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a small KEY=VALUE env file without external dependencies."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            values[key] = value
    return values
