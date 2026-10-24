"""Daytona toolbox **file** transport checks — `python scripts/check_daytona_files.py`

Runs against a fake toolbox server on loopback (stdlib only, no network):

  a) `POST /files/upload-v2?path=...` accepts a **raw octet-stream body** and
     the bytes arrive intact;
  b) `GET /files/download` streams to disk, and a 401 is reported cleanly —
     never parsed as JSON, never a traceback;
  c) `GET /files` and `GET /files/info` return normalised FileInfo (using
     `modifiedAt`, not the deprecated `modTime`);
  d) the base URL is resolved from `/toolbox-proxy-url` **once** and cached;
  e) `DAYTONA_TOOLBOX_AUTH=none` sends no Authorization header;
  f) **streaming really streams**: an 8 MB upload/download does not grow the
     process RSS by anything close to 8 MB (measured with /proc/self/status).

Exit 0 = pass. Writes only to a temp directory.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.services import daytona_files  # noqa: E402

SANDBOX = "sbx-test-1"
API_KEY = "test-api-key"
BIG_MB = 8


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f" — {detail}" if detail else ""))
    return ok


def rss_kb() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def hwm_kb() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


class FakeToolbox:
    """A fake Daytona toolbox proxy that records exactly what it received."""

    def __init__(self) -> None:
        # Remote path -> local temp file. Stored on disk on purpose: keeping an
        # 8 MB upload in a dict would make the fake server, not the client,
        # pay the memory cost the streaming check is trying to measure.
        self.files: dict[str, Path] = {}
        self.store_dir: Path | None = None
        self.uploads: list[dict] = []
        self.downloads: list[str] = []
        self.list_requests: list[dict] = []
        self.info_requests: list[str] = []
        self.auth_headers: list[str | None] = []
        self.reject = False          # simulate 401 from the proxy
        self.require_auth = False
        self.max_body_seen = 0
        self.httpd: ThreadingHTTPServer | None = None
        self.port = 0

    # ------------------------------------------------------------------ server
    def _store_path(self, remote: str) -> Path:
        """One temp file per remote path (mirrors the sandbox filesystem)."""
        import hashlib

        assert self.store_dir is not None
        digest = hashlib.sha1(remote.encode()).hexdigest()[:16]
        return self.store_dir / digest

    def start(self) -> int:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # silence
                pass

            def _close_header(self) -> None:
                """Announce a closing connection.

                `_drain_body` sets `close_connection = True`, but
                BaseHTTPRequestHandler does not emit the matching header. A
                real HTTP/1.1 server does, and a client that is not told will
                happily keep the socket in its pool and then fail the *next*
                request with RemoteDisconnected. That made this harness flaky
                (~2 in 12 runs) for a reason that has nothing to do with the
                code under test.
                """
                if self.close_connection:
                    self.send_header("Connection", "close")

            def _json(self, code: int, payload: dict) -> None:
                body = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self._close_header()
                self.end_headers()
                self.wfile.write(body)

            def _octet(self, code: int, body: bytes) -> None:
                self.send_response(code)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self._close_header()
                self.end_headers()
                self.wfile.write(body)

            def _drain_body(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                remaining = length
                while remaining > 0:
                    data = self.rfile.read(min(262144, remaining))
                    if not data:
                        break
                    remaining -= len(data)
                self.close_connection = True

            def _guard(self) -> bool:
                """Return True when the request may proceed."""
                auth = self.headers.get("Authorization")
                outer.auth_headers.append(auth)
                if outer.reject:
                    # Drain any request body first: replying 401 while an
                    # upload is still in flight would reset the connection and
                    # hide the status code behind a transport error.
                    self._drain_body()
                    # Errors are octet-stream too — a client that json-parses
                    # this must fail loudly, which is the point of the check.
                    self._octet(401, b"unauthorized: no sandbox token")
                    return False
                if outer.require_auth and auth != f"Bearer {API_KEY}":
                    self._octet(401, b"unauthorized")
                    return False
                return True

            def do_POST(self):  # noqa: N802
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                if parsed.path.endswith("/files/upload-v2"):
                    if not self._guard():
                        return
                    target = (query.get("path") or [""])[0]
                    length = int(self.headers.get("Content-Length") or 0)
                    transfer = (self.headers.get("Transfer-Encoding") or "").lower()
                    stored = outer._store_path(target)
                    seen = 0
                    largest = 0
                    with stored.open("wb") as out:
                        if transfer == "chunked":
                            while True:
                                line = self.rfile.readline().strip()
                                if not line:
                                    break
                                size = int(line, 16)
                                if size == 0:
                                    self.rfile.readline()
                                    break
                                data = self.rfile.read(size)
                                largest = max(largest, len(data))
                                out.write(data)
                                seen += len(data)
                                self.rfile.read(2)  # CRLF
                        else:
                            remaining = length
                            while remaining > 0:
                                data = self.rfile.read(min(262144, remaining))
                                if not data:
                                    break
                                largest = max(largest, len(data))
                                out.write(data)
                                seen += len(data)
                                remaining -= len(data)
                    outer.max_body_seen = max(outer.max_body_seen, largest)
                    outer.files[target] = stored
                    outer.uploads.append(
                        {
                            "path": target,
                            "size": seen,
                            "content_type": self.headers.get("Content-Type"),
                            "transfer": transfer or "content-length",
                        }
                    )
                    name = target.rsplit("/", 1)[-1]
                    self._json(200, {"name": name, "path": target, "type": "file"})
                    return
                if parsed.path.endswith("/files/folder"):
                    self._json(200, {"ok": True})
                    return
                self._json(404, {"error": "not found"})

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                path = parsed.path

                if path.endswith("/toolbox-proxy-url"):
                    outer.auth_headers.append(self.headers.get("Authorization"))
                    outer.list_requests.append({"discovery": True})
                    self._json(200, {"url": f"http://127.0.0.1:{outer.port}/toolbox"})
                    return

                if path.endswith("/files/download"):
                    if not self._guard():
                        return
                    target = (query.get("path") or [""])[0]
                    outer.downloads.append(target)
                    if target not in outer.files:
                        self._octet(404, b"no such file")
                        return
                    stored = outer.files[target]
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(stored.stat().st_size))
                    self.end_headers()
                    with stored.open("rb") as fh:
                        while True:
                            block = fh.read(65536)
                            if not block:
                                break
                            self.wfile.write(block)
                    return

                if path.endswith("/files/info"):
                    if not self._guard():
                        return
                    target = (query.get("path") or [""])[0]
                    outer.info_requests.append(target)
                    if target not in outer.files:
                        self._octet(404, b"no such file")
                        return
                    self._json(
                        200,
                        {
                            "name": target.rsplit("/", 1)[-1],
                            "path": target,
                            "size": outer.files[target].stat().st_size,
                            "isDir": False,
                            "permissions": "0644",
                            "owner": "root",
                            "group": "root",
                            # Deprecated Go layout: must be ignored by the client.
                            "modTime": "2026-10-23 10:00:00.123456789 +0000 UTC",
                            "modifiedAt": "2026-10-23T10:00:00Z",
                        },
                    )
                    return

                if path.rstrip("/").endswith("/files") or path.endswith("/files/"):
                    if not self._guard():
                        return
                    target = (query.get("path") or ["/"])[0].rstrip("/")
                    depth = int((query.get("depth") or ["1"])[0])
                    outer.list_requests.append({"path": target, "depth": depth})
                    entries = []
                    prefix = target + "/"
                    for known in sorted(outer.files):
                        if not known.startswith(prefix):
                            continue
                        rest = known[len(prefix):]
                        if "/" in rest:
                            continue  # flat fake: only direct children
                        entries.append(
                            {
                                "name": rest,
                                "path": known,
                                "size": outer.files[known].stat().st_size,
                                "isDir": False,
                                "permissions": "0644",
                                "owner": "root",
                                "group": "root",
                                "modifiedAt": "2026-10-23T10:00:00Z",
                            }
                        )
                    self._json(200, {"files": entries})
                    return

                self._json(404, {"error": "not found"})

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self.port

    def stop(self) -> None:
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()


def read_file_chunked(path: Path, chunk_size: int = 65536):
    """Yield a file in chunks (used to prove chunked framing is handled too)."""
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            yield block


def fresh_client(settings, **overrides):
    """A client with no cached base (mirrors a cold process)."""
    daytona_files._client = None
    client = daytona_files.DaytonaFilesClient(replace(settings, **overrides))
    return client


async def main() -> int:
    failures = 0
    tmp = Path(tempfile.mkdtemp(prefix="dayhost-files-test-"))
    server = FakeToolbox()
    server.store_dir = tmp / "fake-sandbox"
    server.store_dir.mkdir(parents=True, exist_ok=True)
    port = server.start()
    base = f"http://127.0.0.1:{port}/toolbox"
    print(f"fake toolbox on 127.0.0.1:{port} (base {base})\n")

    settings = replace(
        get_settings(),
        daytona_api_key=API_KEY,
        sandbox_id=SANDBOX,
        daytona_api_base=f"http://127.0.0.1:{port}",
        daytona_toolbox_url=base,
        daytona_toolbox_auth="bearer",
        daytona_timeout=10,
        daytona_max_retries=0,   # keep the failure checks fast and deterministic
    )

    try:
        # ---- (a) upload: raw body, bytes intact ---------------------------
        print("== (a) upload-v2 with a raw octet-stream body ==")
        src = tmp / "level.dat"
        payload = bytes(range(256)) * 40
        src.write_bytes(payload)
        client = fresh_client(settings)
        try:
            result = await client.upload_file(src, "/data/world/level.dat")
            failures += 0 if check("upload ok", result.get("ok") is True, str(result)) else 1
            upload = server.uploads[-1] if server.uploads else {}
            failures += 0 if check(
                "path query parameter used",
                upload.get("path") == "/data/world/level.dat", str(upload.get("path")),
            ) else 1
            failures += 0 if check(
                "raw body (not multipart)",
                upload.get("content_type") == "application/octet-stream"
                and "multipart" not in str(upload.get("content_type")),
                str(upload.get("content_type")),
            ) else 1
            stored_path = server.files.get("/data/world/level.dat")
            failures += 0 if check(
                "bytes arrived intact",
                stored_path is not None and stored_path.read_bytes() == payload,
                f"{stored_path.stat().st_size if stored_path else 0} bytes",
            ) else 1
            failures += 0 if check(
                "Content-Length known -> not chunked",
                upload.get("transfer") == "content-length", str(upload.get("transfer")),
            ) else 1
            failures += 0 if check(
                "UploadedFile parsed",
                (result.get("uploaded") or {}).get("type") == "file", str(result.get("uploaded")),
            ) else 1

            # ---- base URL discovery, once ---------------------------------
            print("\n== (d) base URL discovered from /toolbox-proxy-url, cached ==")
            undiscovered = fresh_client(settings, daytona_toolbox_url="")
            first = await undiscovered.base_url()
            second = await undiscovered.base_url()
            discovery_calls = sum(1 for r in server.list_requests if r.get("discovery"))
            failures += 0 if check(
                "discovered base includes the sandbox segment",
                first == f"http://127.0.0.1:{port}/toolbox/{SANDBOX}", str(first),
            ) else 1
            failures += 0 if check("second call is cached", first == second) else 1
            failures += 0 if check(
                "only one discovery request was made", discovery_calls == 1, str(discovery_calls)
            ) else 1
            await undiscovered.aclose()

            # ---- (b) download streams to disk -----------------------------
            print("\n== (b) download streams to disk ==")
            dest = tmp / "restored" / "level.dat"
            result = await client.download_file("/data/world/level.dat", dest)
            failures += 0 if check("download ok", result.get("ok") is True, str(result)) else 1
            failures += 0 if check(
                "file on disk matches the source",
                dest.is_file() and dest.read_bytes() == payload,
                f"{dest.stat().st_size if dest.exists() else 0} bytes",
            ) else 1
            failures += 0 if check(
                "parent directories created on download",
                dest.parent.is_dir(),
            ) else 1
            leftovers = list(dest.parent.glob("*.part"))
            failures += 0 if check("no .part temp file left behind", leftovers == [], str(leftovers)) else 1

            # ---- 401 -> clean, actionable error --------------------------
            print("\n== (b2) 401 from the proxy -> actionable error, no traceback ==")
            server.reject = True
            denied = await client.download_file("/data/world/level.dat", tmp / "denied.bin")
            failures += 0 if check(
                "ok == False", denied.get("ok") is False, str(denied.get("ok"))
            ) else 1
            failures += 0 if check(
                "kind == auth", denied.get("kind") == "auth", str(denied.get("kind"))
            ) else 1
            failures += 0 if check(
                "message is actionable",
                "toolbox proxy rejected credentials" in (denied.get("error") or ""),
                str(denied.get("error")),
            ) else 1
            failures += 0 if check(
                "status_code 401", denied.get("status_code") == 401, str(denied.get("status_code"))
            ) else 1
            failures += 0 if check(
                "no JSON parse of the octet-stream error body",
                "JSONDecodeError" not in str(denied),
                str(denied.get("detail"))[:80],
            ) else 1
            failures += 0 if check(
                "no partial file written", not (tmp / "denied.bin").exists()
            ) else 1
            up_denied = await client.upload_file(src, "/data/world/denied.dat")
            failures += 0 if check(
                "upload 401 also actionable",
                up_denied.get("kind") == "auth"
                and "rejected credentials" in (up_denied.get("error") or ""),
                str(up_denied.get("error")),
            ) else 1
            server.reject = False

            # ---- (c) list + info -----------------------------------------
            print("\n== (c) list and info ==")
            listing = await client.list_dir("/data/world")
            failures += 0 if check("list ok", listing.get("ok") is True, str(listing)) else 1
            names = [e["name"] for e in listing.get("entries", [])]
            failures += 0 if check(
                "listing contains the uploaded file", "level.dat" in names, str(names)
            ) else 1
            entry = next((e for e in listing.get("entries", []) if e["name"] == "level.dat"), {})
            failures += 0 if check(
                "FileInfo normalised (size/is_dir/type)",
                entry.get("size") == len(payload) and entry.get("is_dir") is False
                and entry.get("type") == "file",
                str(entry),
            ) else 1

            info = await client.file_info("/data/world/level.dat")
            failures += 0 if check("info ok", info.get("ok") is True, str(info)) else 1
            failures += 0 if check(
                "info uses modifiedAt, not the deprecated modTime",
                info["info"].get("modified_at") == "2026-10-23T10:00:00Z"
                and "mod_time" not in info["info"],
                str(info["info"].get("modified_at")),
            ) else 1
            failures += 0 if check(
                "permissions/owner/group surfaced",
                info["info"].get("permissions") == "0644"
                and info["info"].get("owner") == "root",
                str(info["info"]),
            ) else 1

            # ---- unconfigured transport ----------------------------------
            print("\n== (g) unconfigured transport -> structured error ==")
            bare = fresh_client(
                settings, sandbox_id="", daytona_toolbox_url="", daytona_api_key=""
            )
            missing = await bare.list_dir("/data/world")
            failures += 0 if check(
                "kind == config", missing.get("kind") == "config", str(missing.get("kind"))
            ) else 1
            failures += 0 if check(
                "message names the missing setting",
                "SANDBOX_ID" in (missing.get("error") or ""), str(missing.get("error"))
            ) else 1
            await bare.aclose()

            # ---- (e) auth modes ------------------------------------------
            print("\n== (e) auth modes ==")
            server.auth_headers.clear()
            server.require_auth = True
            bearer = fresh_client(settings, daytona_toolbox_auth="bearer")
            ok = await bearer.list_dir("/data/world")
            failures += 0 if check(
                "auth=bearer sends the API key",
                any(h == f"Bearer {API_KEY}" for h in server.auth_headers),
                str(server.auth_headers[:3]),
            ) else 1
            failures += 0 if check("auth=bearer request succeeds", ok.get("ok") is True, str(ok)) else 1
            await bearer.aclose()

            server.auth_headers.clear()
            none_client = fresh_client(settings, daytona_toolbox_auth="none")
            await none_client.list_dir("/data/world")
            failures += 0 if check(
                "auth=none sends no Authorization header",
                all(h is None for h in server.auth_headers),
                str(server.auth_headers[:3]),
            ) else 1
            await none_client.aclose()
            server.require_auth = False

            bad = fresh_client(settings, daytona_toolbox_auth="basic")
            invalid = await bad.list_dir("/data/world")
            failures += 0 if check(
                "invalid auth mode -> config error, no crash",
                invalid.get("kind") == "config" and "invalid" in (invalid.get("error") or ""),
                str(invalid.get("error")),
            ) else 1
            await bad.aclose()

            # ---- (f) streaming: RSS must not grow by ~the file size -------
            print(f"\n== (f) {BIG_MB} MB streaming: peak RSS must not grow by ~{BIG_MB} MB ==")
            big = tmp / "big.bin"
            chunk = os.urandom(1024 * 1024)
            with big.open("wb") as fh:
                for _ in range(BIG_MB):
                    fh.write(chunk)
            big_size = big.stat().st_size
            del chunk

            stream_client = fresh_client(settings)
            # One warm-up request so per-client imports/socket pools are already
            # allocated before the baseline is taken.
            await stream_client.list_dir("/data/world")

            rss_before = rss_kb()
            hwm_before = hwm_kb()
            up = await stream_client.upload_file(big, "/data/world/big.bin")
            rss_after_upload = rss_kb()
            big_dest = tmp / "big-back.bin"
            down = await stream_client.download_file("/data/world/big.bin", big_dest)
            rss_after_download = rss_kb()
            hwm_after = hwm_kb()

            failures += 0 if check(
                f"upload of {big_size / 1048576:.1f} MB ok", up.get("ok") is True, str(up.get("error"))
            ) else 1
            failures += 0 if check(
                "server received all bytes",
                server.uploads[-1]["size"] == big_size,
                f"{server.uploads[-1]['size']} vs {big_size}",
            ) else 1
            failures += 0 if check(
                "download of the same size ok and identical",
                down.get("ok") is True and big_dest.stat().st_size == big_size,
                f"{big_dest.stat().st_size if big_dest.exists() else 0} bytes",
            ) else 1

            growth_upload = rss_after_upload - rss_before
            growth_total = max(rss_after_upload, rss_after_download) - rss_before
            hwm_growth = hwm_after - hwm_before
            file_mb = big_size / 1048576
            print(f"   file                : {big_size} bytes ({file_mb:.1f} MB)")
            print(f"   RSS before          : {rss_before / 1024:.2f} MB")
            print(f"   RSS after upload    : {rss_after_upload / 1024:.2f} MB (+{growth_upload / 1024:.2f} MB)")
            print(f"   RSS after download  : {rss_after_download / 1024:.2f} MB")
            print(f"   VmHWM growth        : {hwm_growth / 1024:.2f} MB")
            print(f"   largest single body seen by the server: {server.max_body_seen / 1024:.0f} KB (chunk, not file)")
            # Budget: buffering the file would cost >= 8 MB. Allow 4 MB for
            # allocator noise + httpx/httpcore buffers, which is far below it.
            budget_kb = 4 * 1024
            failures += 0 if check(
                f"upload RSS growth {growth_upload / 1024:.2f} MB < 4 MB (not {file_mb:.0f} MB)",
                growth_upload < budget_kb,
            ) else 1
            failures += 0 if check(
                f"peak RSS growth {hwm_growth / 1024:.2f} MB < 4 MB", hwm_growth < budget_kb
            ) else 1
            failures += 0 if check(
                "server saw chunks far smaller than the file",
                server.max_body_seen < 1024 * 1024,
                f"{server.max_body_seen} bytes",
            ) else 1
            await stream_client.aclose()
        finally:
            await client.aclose()
    finally:
        server.stop()

    print(f"\ntemp dir: {tmp}")
    print()
    if failures:
        print(f"{failures} CHECK(S) FAILED")
        return 1
    print("ALL DAYTONA FILE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
