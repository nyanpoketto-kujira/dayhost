"""Backup mirror, delta archives, snapshots and plugin storage.

Ported from ``mc-orchestrator/controller/storage.py`` (Fusion Phase C) with two
changes and nothing else:

  1. ``Config`` (the old controller's env-file class) is replaced by
     :class:`StoreConfig`, a frozen dataclass built from
     ``app.config.Settings`` OR constructed directly by the harness. The
     attribute names ``Storage`` reads are identical, so the ported code below
     is otherwise verbatim.
  2. No aiohttp import existed; none was added. ``tar`` is still the external
     program for snapshots (one ``create_subprocess_exec``, same as before).

Layout (Ner's decision #4): ``data/mirror`, ``data/snapshots``,
``data/deltas``, ``data/plugins`` and a private tmp dir. ``data/dayhost.db`` is
never touched from here.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tarfile
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import (
    atomic_write_json,
    human_size,
    read_json,
    resolve_under,
    sanitize_plugin_name,
    sha256_file,
    utc_now,
)

MANIFEST_NAME = "__manifest__.json"


@dataclass(frozen=True)
class StoreConfig:
    """The subset of configuration ``Storage`` needs.

    Build with :meth:`from_settings` in the app, or pass explicit paths in a
    harness. Every path is already resolved; ``Storage`` never guesses.
    """

    mirror_dir: Path
    snapshots_dir: Path
    deltas_dir: Path
    plugins_dir: Path
    tmp_dir: Path
    snapshot_keep: int = 8
    delta_keep: int = 20
    snapshot_interval: float = 6 * 3600
    max_plugin_bytes: int = 100 * 1024 * 1024

    @classmethod
    def from_settings(cls, settings: Any) -> "StoreConfig":
        """Duck-typed so this module stays importable without dotenv.

        ``Settings`` resolves the paths in ``*_path`` properties (absolute,
        repos-root anchored, subdirs under ``ORCH_DATA_DIR``); the raw
        ``orch_*_dir`` strings are never used here.
        """
        return cls(
            mirror_dir=Path(settings.orch_mirror_path),
            snapshots_dir=Path(settings.orch_snapshots_path),
            deltas_dir=Path(settings.orch_deltas_path),
            plugins_dir=Path(settings.orch_plugins_path),
            tmp_dir=Path(settings.orch_tmp_path),
            snapshot_keep=int(getattr(settings, "orch_snapshot_keep", 8)),
            delta_keep=int(getattr(settings, "orch_delta_keep", 20)),
            snapshot_interval=float(getattr(settings, "orch_snapshot_interval", 6 * 3600)),
            max_plugin_bytes=int(getattr(settings, "orch_max_plugin_bytes", 100 * 1024 * 1024)),
        )

    def ensure_dirs(self) -> None:
        for path in (
            self.mirror_dir,
            self.snapshots_dir,
            self.deltas_dir,
            self.plugins_dir,
            self.tmp_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


class Storage:
    def __init__(self, config: StoreConfig) -> None:
        self.cfg = config
        self.lock = asyncio.Lock()
        self.plugins_index_path = config.plugins_dir / "index.json"
        self._mirror_cache: tuple[float, dict[str, Any]] | None = None

    # ------------------------------------------------------------------ setup
    def ensure(self) -> None:
        self.cfg.ensure_dirs()
        # Old temporary files must never be mistaken for valid data.
        for directory in (self.cfg.tmp_dir, self.cfg.snapshots_dir):
            for path in directory.glob(".tmp-*"):
                try:
                    path.unlink()
                except OSError:
                    pass

    # ----------------------------------------------------------------- mirror
    def has_world(self) -> bool:
        world = self.cfg.mirror_dir / "world" / "level.dat"
        return world.is_file()

    def mirror_stats(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._mirror_cache and now - self._mirror_cache[0] < 5.0:
            return self._mirror_cache[1]
        files = 0
        size = 0
        newest = 0.0
        root = self.cfg.mirror_dir
        if root.is_dir():
            for dirpath, dirnames, filenames in os.walk(root):
                dirnames[:] = [d for d in dirnames if d not in (".git", "cache", "logs")]
                for filename in filenames:
                    try:
                        stat = os.stat(os.path.join(dirpath, filename))
                    except OSError:
                        continue
                    files += 1
                    size += stat.st_size
                    newest = max(newest, stat.st_mtime)
        payload = {
            "files": files,
            "bytes": size,
            "bytes_human": human_size(size),
            "has_world": self.has_world(),
            "updated_at": utc_now() if newest else None,
            "updated_epoch": newest or None,
        }
        self._mirror_cache = (now, payload)
        return payload

    # ---------------------------------------------------------------- plugins
    def _read_plugin_index(self) -> dict[str, Any]:
        data = read_json(self.plugins_index_path, {})
        return data if isinstance(data, dict) else {}

    def list_plugins(self) -> list[dict[str, Any]]:
        index = self._read_plugin_index()
        result: list[dict[str, Any]] = []
        changed = False
        for path in sorted(self.cfg.plugins_dir.glob("*.jar")):
            if not path.is_file():
                continue
            stat = path.stat()
            meta = index.get(path.name)
            if (
                not isinstance(meta, dict)
                or meta.get("size") != stat.st_size
                or meta.get("mtime") != int(stat.st_mtime)
            ):
                meta = {
                    "name": path.name,
                    "size": stat.st_size,
                    "mtime": int(stat.st_mtime),
                    "sha256": sha256_file(path),
                    "uploaded_at": utc_now(),
                }
                index[path.name] = meta
                changed = True
            result.append(
                {
                    "name": path.name,
                    "size": stat.st_size,
                    "size_human": human_size(stat.st_size),
                    "sha256": meta.get("sha256"),
                    "uploaded_at": meta.get("uploaded_at"),
                }
            )
        for name in list(index):
            if not (self.cfg.plugins_dir / name).is_file():
                del index[name]
                changed = True
        if changed:
            atomic_write_json(self.plugins_index_path, index)
        return result

    def save_plugin(self, tmp_path: Path, original_name: str) -> dict[str, Any]:
        name = sanitize_plugin_name(original_name)
        if not name:
            raise ValueError("nama file harus .jar dan hanya berisi karakter aman")
        size = tmp_path.stat().st_size
        if size < 4 or size > self.cfg.max_plugin_bytes:
            raise ValueError(
                f"ukuran plugin tidak valid (maks {self.cfg.max_plugin_bytes // 1024 // 1024} MB)"
            )
        with open(tmp_path, "rb") as handle:
            if handle.read(4) != b"PK\x03\x04":
                raise ValueError("file bukan .jar/zip yang valid")
        dest = self.cfg.plugins_dir / name
        os.replace(tmp_path, dest)
        sha = sha256_file(dest)
        index = self._read_plugin_index()
        index[name] = {
            "name": name,
            "size": size,
            "mtime": int(dest.stat().st_mtime),
            "sha256": sha,
            "uploaded_at": utc_now(),
        }
        atomic_write_json(self.plugins_index_path, index)
        return {
            "name": name,
            "size": size,
            "size_human": human_size(size),
            "sha256": sha,
            "uploaded_at": index[name]["uploaded_at"],
        }

    def plugin_path(self, name: str) -> Path | None:
        safe = sanitize_plugin_name(name)
        if not safe:
            return None
        path = self.cfg.plugins_dir / safe
        return path if path.is_file() else None

    # ----------------------------------------------------------------- deltas
    async def apply_delta(self, archive: Path, agent_host: str) -> dict[str, Any]:
        async with self.lock:
            result = await asyncio.to_thread(self._apply_delta_sync, archive, agent_host)
            self._mirror_cache = None
        return result

    def apply_delta_sync(self, archive: Path, agent_host: str) -> dict[str, Any]:
        """Synchronous entry point (the delta-ingest hook is already in a task)."""
        result = self._apply_delta_sync(archive, agent_host)
        self._mirror_cache = None
        return result

    def _apply_delta_sync(self, archive: Path, agent_host: str) -> dict[str, Any]:
        files = 0
        deleted = 0
        total = 0
        mirror = self.cfg.mirror_dir
        manifest: dict[str, Any] = {}
        with tarfile.open(archive, "r:gz") as tar:
            members = tar.getmembers()
            for member in members:
                if not member.isfile() and not member.isdir():
                    raise ValueError(f"tipe tar tidak diizinkan: {member.name}")
                if not self._safe_member(mirror, member.name):
                    raise ValueError(f"path tar tidak aman: {member.name}")
            for member in members:
                target = self._safe_member(mirror, member.name)
                if target is None:
                    continue
                if member.name == MANIFEST_NAME:
                    source = tar.extractfile(member)
                    if source is not None:
                        try:
                            manifest = json.loads(source.read().decode("utf-8"))
                        except ValueError:
                            manifest = {}
                    continue
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    continue
                fd, tmp_name = tempfile.mkstemp(
                    prefix=target.name + ".", dir=str(target.parent)
                )
                try:
                    with os.fdopen(fd, "wb") as out:
                        shutil.copyfileobj(source, out, length=1024 * 1024)
                    if member.mtime:
                        os.utime(tmp_name, (member.mtime, member.mtime))
                    os.replace(tmp_name, target)
                finally:
                    try:
                        os.unlink(tmp_name)
                    except FileNotFoundError:
                        pass
                files += 1
                total += member.size

        delete_list = manifest.get("deleted") if isinstance(manifest, dict) else None
        if isinstance(delete_list, list):
            for name in delete_list[:20000]:
                if not isinstance(name, str):
                    continue
                target = self._safe_member(mirror, name)
                if target is None:
                    continue
                try:
                    if target.is_dir() and not target.is_symlink():
                        target.rmdir()
                    else:
                        target.unlink()
                    deleted += 1
                except OSError:
                    pass
        self._mirror_cache = None

        # Keep a bounded archive of raw deltas for auditing.
        stamp = time.strftime("%Y%m%d-%H%M%S")
        host = "".join(c if c.isalnum() or c in "-_" else "_" for c in agent_host)[:40]
        dest = (
            self.cfg.deltas_dir
            / f"delta-{stamp}-{host}-{os.getpid()}-{uuid.uuid4().hex[:8]}.tar.gz"
        )
        try:
            os.replace(archive, dest)
            self._prune_deltas()
        except OSError:
            try:
                archive.unlink()
            except OSError:
                pass
        return {
            "files": files,
            "deleted": deleted,
            "bytes": total,
            "bytes_human": human_size(total),
            "agent_host": agent_host,
            "delta_ts": manifest.get("ts") if isinstance(manifest, dict) else None,
            "manifest_files": len(manifest.get("files", [])) if isinstance(manifest, dict) else None,
        }

    @staticmethod
    def _safe_member(root: Path, name: str) -> Path | None:
        return resolve_under(root, name)

    def _prune_deltas(self) -> None:
        archives = sorted(
            self.cfg.deltas_dir.glob("delta-*.tar.gz"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        keep = max(0, self.cfg.delta_keep)
        for old in archives[keep:]:
            try:
                old.unlink()
            except OSError:
                pass

    def deltas_stats(self) -> dict[str, Any]:
        archives = sorted(
            self.cfg.deltas_dir.glob("delta-*.tar.gz"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        size = sum(p.stat().st_size for p in archives if p.is_file())
        latest = archives[0].stat().st_mtime if archives else 0
        return {
            "count": len(archives),
            "bytes": size,
            "bytes_human": human_size(size),
            "latest_at": utc_now() if latest else None,
            "latest_epoch": latest or None,
        }

    # -------------------------------------------------------------- snapshots
    def list_snapshots(self) -> list[dict[str, Any]]:
        records = []
        for path in self.cfg.snapshots_dir.glob("snapshot-*.tar.gz"):
            try:
                stat = path.stat()
            except OSError:
                continue
            records.append(
                {
                    "name": path.name,
                    "size": stat.st_size,
                    "size_human": human_size(stat.st_size),
                    "mtime": stat.st_mtime,
                    "created_at": time.strftime(
                        "%Y-%m-%dT%H:%M:%S%z", time.localtime(stat.st_mtime)
                    ),
                }
            )
        records.sort(key=lambda item: item["mtime"], reverse=True)
        return records

    def latest_snapshot(self) -> Path | None:
        latest = self.cfg.snapshots_dir / "latest.tar.gz"
        if latest.is_file():
            return latest
        records = self.list_snapshots()
        if records:
            return self.cfg.snapshots_dir / records[0]["name"]
        return None

    async def create_snapshot(self, reason: str = "manual") -> dict[str, Any]:
        async with self.lock:
            if not self.has_world():
                raise ValueError("mirror belum berisi world/level.dat; belum ada data untuk di-backup")
            stamp = time.strftime("%Y%m%d-%H%M%S")
            name = f"snapshot-{stamp}.tar.gz"
            final = self.cfg.snapshots_dir / name
            if final.exists():
                name = f"snapshot-{stamp}-{uuid.uuid4().hex[:6]}.tar.gz"
                final = self.cfg.snapshots_dir / name
            tmp = self.cfg.snapshots_dir / f".tmp-{name}"
            process = await asyncio.create_subprocess_exec(
                "tar",
                "-czf",
                str(tmp),
                "-C",
                str(self.cfg.mirror_dir),
                ".",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(process.communicate(), timeout=900)
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
                raise ValueError("pembuatan snapshot timeout")
            if process.returncode != 0:
                try:
                    tmp.unlink()
                except OSError:
                    pass
                raise ValueError(
                    "tar gagal: " + stderr.decode("utf-8", "replace")[-500:]
                )
            os.replace(tmp, final)
            self._write_latest_link(final)
            self._prune_snapshots()
            stat = final.stat()
            self._mirror_cache = None
            return {
                "name": final.name,
                "size": stat.st_size,
                "size_human": human_size(stat.st_size),
                "mtime": stat.st_mtime,
                "created_at": utc_now(),
                "reason": reason,
            }

    def _write_latest_link(self, target: Path) -> None:
        link = self.cfg.snapshots_dir / "latest.tar.gz"
        tmp = self.cfg.snapshots_dir / ".tmp-latest"
        try:
            tmp.unlink()
        except OSError:
            pass
        try:
            os.symlink(target.name, tmp)
            os.replace(tmp, link)
        except OSError:
            # Filesystem without symlinks: hardlink/copy fallback.
            try:
                os.link(target, tmp)
                os.replace(tmp, link)
            except OSError:
                pass

    def _prune_snapshots(self) -> None:
        records = self.list_snapshots()
        for record in records[self.cfg.snapshot_keep :]:
            path = self.cfg.snapshots_dir / record["name"]
            try:
                path.unlink()
            except OSError:
                pass

    def backups_payload(self) -> dict[str, Any]:
        return {
            "snapshots": self.list_snapshots(),
            "deltas": self.deltas_stats(),
            "mirror": self.mirror_stats(),
            "keep": {
                "snapshots": self.cfg.snapshot_keep,
                "deltas": self.cfg.delta_keep,
            },
            "interval": self.cfg.snapshot_interval,
        }
