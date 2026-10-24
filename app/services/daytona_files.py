"""Daytona toolbox **file** API client — streaming upload/download (Fase 3).

Two different base URLs exist in Daytona and they must never be mixed:

    lifecycle : https://app.daytona.io/api                     (daytona_client.py)
    files     : https://proxy.app.daytona.io/toolbox/{sandbox} (this module)

Endpoints (docs/daytona-file-api.md, Daytona v0.225):

    POST   /files/upload-v2?path=...      raw body OR multipart, parents auto-created
    GET    /files/download?path=...       application/octet-stream (errors too!)
    GET    /files?path=...&depth=N        directory listing
    GET    /files/info?path=...           FileInfo (use `modifiedAt`, not `modTime`)
    POST   /files/folder?path=...&mode=... create a directory
    DELETE /files?path=...&recursive=...  remove

Design rules
------------
* **Streaming, always.** Uploads pass an open file object to httpx (which wraps
  it in a 64 KB chunk iterator); downloads are written chunk-by-chunk to a
  temporary file and atomically renamed. A multi-hundred-MB world must never
  land in RAM — the process has a 60 MB RSS ceiling.
* **Never parse a download body as JSON.** Failures are `octet-stream` too, so
  the status code is checked before anything is written.
* **Auth is configurable, because it is not documented.** The toolbox spec
  carries no `securitySchemes`; `DAYTONA_TOOLBOX_AUTH=bearer|none` decides.
  401/403 produce one actionable sentence, never a traceback.
* `httpx` is imported **lazily** (same pattern as `daytona_client.py`) so a
  cold, idle process pays nothing for a transport it may never use.
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator

from ..config import Settings, get_settings

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps httpx out of startup
    import httpx

log = logging.getLogger("dayhost.daytona.files")

CHUNK_SIZE = 64 * 1024
MAX_ERROR_BODY = 2048


class FileStream:
    """Async byte stream over an open file, read in 64 KB chunks off-loop.

    `httpx.AsyncClient` refuses a plain file object (it yields a *sync* stream),
    and reading in the event loop would block every other request for the
    duration of a multi-hundred-MB upload. Reads therefore run in a worker
    thread, one chunk at a time: the file is never materialised in memory.

    The stream is single-use, which is why `upload_file` re-opens the file for
    every retry attempt.
    """

    def __init__(self, handle: Any, chunk_size: int = CHUNK_SIZE) -> None:
        self._handle = handle
        self._chunk_size = chunk_size

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await asyncio.to_thread(self._handle.read, self._chunk_size)
            if not chunk:
                break
            yield chunk

AUTH_REJECTED = (
    "toolbox proxy rejected credentials — verify auth scheme against Daytona docs "
    "(DAYTONA_TOOLBOX_AUTH=bearer|none)"
)


def _error(
    message: str,
    *,
    kind: str = "http",
    status_code: int | None = None,
    path: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "kind": kind,
        "status_code": status_code,
        "path": path,
        "error": message,
    }
    payload.update(extra)
    return payload


class DaytonaFilesClient:
    """Async client for the toolbox file endpoints of one sandbox."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client: "httpx.AsyncClient | None" = None
        self._base: str | None = None

    # ------------------------------------------------------------------ config
    @property
    def auth_mode(self) -> str:
        mode = (self.settings.daytona_toolbox_auth or "bearer").strip().lower()
        return mode if mode in {"bearer", "none"} else "invalid"

    @property
    def configured(self) -> bool:
        """True when we can name a toolbox base without a discovery call."""
        return bool(self.settings.daytona_toolbox_url and self.settings.sandbox_id)

    def _unconfigured(self, path: str | None = None) -> dict[str, Any]:
        if not self.settings.sandbox_id:
            why = "SANDBOX_ID is empty"
        elif self.auth_mode == "invalid":
            why = (
                f"DAYTONA_TOOLBOX_AUTH={self.settings.daytona_toolbox_auth!r} is invalid "
                "(expected 'bearer' or 'none')"
            )
        else:
            why = "DAYTONA_TOOLBOX_URL is empty and sandbox discovery failed"
        return _error(
            f"daytona file transport not configured: {why}",
            kind="config",
            path=path,
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.auth_mode == "bearer":
            if not self.settings.daytona_api_key:
                # Deliberately no exception: the request will fail with our
                # normalised error instead of a stack trace.
                log.debug("toolbox auth=bearer but DAYTONA_API_KEY is empty")
            headers["Authorization"] = f"Bearer {self.settings.daytona_api_key}"
        return headers

    async def _http(self) -> "httpx.AsyncClient":
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    self.settings.daytona_timeout,
                    connect=min(10.0, float(self.settings.daytona_timeout)),
                ),
                headers=self._headers(),
                follow_redirects=False,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -------------------------------------------------------------- base URL
    @staticmethod
    def _join_base(base: str, sandbox_id: str) -> str:
        base = base.rstrip("/")
        if "{sandbox_id}" in base:
            return base.replace("{sandbox_id}", sandbox_id)
        if not sandbox_id:
            return base
        # A base that already ends with the sandbox id is used verbatim.
        if base.rsplit("/", 1)[-1] == sandbox_id:
            return base
        return f"{base}/{sandbox_id}"

    async def _discover_base(self) -> str | None:
        """Ask the lifecycle API for the toolbox proxy URL. Cached thereafter."""
        import httpx

        api_base = self.settings.daytona_api_base.rstrip("/")
        url = f"{api_base}/sandbox/{self.settings.sandbox_id}/toolbox-proxy-url"
        headers = {"Accept": "application/json"}
        if self.settings.daytona_api_key:
            headers["Authorization"] = f"Bearer {self.settings.daytona_api_key}"
        try:
            async with httpx.AsyncClient(timeout=self.settings.daytona_timeout) as client:
                resp = await client.get(url, headers=headers)
            log.debug(
                "toolbox-proxy-url GET %s -> %s body=%r",
                url, resp.status_code, resp.text[:MAX_ERROR_BODY],
            )
            if resp.status_code != 200:
                return None
            payload = resp.json()
            base = payload.get("url") if isinstance(payload, dict) else None
            if not base:
                return None
            return self._join_base(str(base), self.settings.sandbox_id)
        except Exception as exc:  # never raise into the request path
            log.debug("toolbox-proxy-url discovery failed: %s: %s", type(exc).__name__, exc)
            return None

    async def base_url(self) -> str | None:
        """Resolve (and cache) the per-sandbox toolbox base."""
        if self._base is not None:
            return self._base
        if self.auth_mode == "invalid" or not self.settings.sandbox_id:
            return None
        if self.settings.daytona_toolbox_url:
            self._base = self._join_base(
                self.settings.daytona_toolbox_url, self.settings.sandbox_id
            )
            return self._base
        self._base = await self._discover_base()
        if self._base:
            log.info("toolbox base resolved once and cached: %s", self._base)
        return self._base

    # ----------------------------------------------------------------- plumbing
    def _classify_status(
        self, status: int, path: str, body: str, *, method: str
    ) -> dict[str, Any]:
        if status in (401, 403):
            return _error(
                AUTH_REJECTED,
                kind="auth",
                status_code=status,
                path=path,
                detail=body[:MAX_ERROR_BODY],
            )
        return _error(
            f"HTTP {status}",
            kind="http",
            status_code=status,
            path=path,
            detail=body[:MAX_ERROR_BODY],
            method=method,
        )

    @staticmethod
    def _retryable(status: int | None) -> bool:
        return status is None or status in (429,) or status >= 500

    def _attempts(self) -> int:
        return max(1, int(self.settings.daytona_max_retries) + 1)

    @staticmethod
    async def _backoff(attempt: int) -> None:
        await asyncio.sleep(min(2 ** attempt * 0.25, 2.0))

    async def _request_json(
        self, method: str, endpoint: str, params: dict[str, Any], path: str
    ) -> dict[str, Any]:
        import httpx

        base = await self.base_url()
        if base is None:
            return self._unconfigured(path)
        attempts = self._attempts()
        last: dict[str, Any] = _error("no attempt made", kind="transport", path=path)

        for attempt in range(1, attempts + 1):
            try:
                client = await self._http()
                resp = await client.request(method, base + endpoint, params=params)
                log.debug(
                    "toolbox %s %s %s -> %s body=%r",
                    method, endpoint, params, resp.status_code,
                    resp.text[:MAX_ERROR_BODY],
                )
                if resp.status_code >= 400:
                    last = self._classify_status(
                        resp.status_code, path, resp.text[:MAX_ERROR_BODY], method=method
                    )
                    if self._retryable(resp.status_code) and attempt < attempts:
                        await self._backoff(attempt)
                        continue
                    return last
                try:
                    data = resp.json()
                except Exception:
                    return _error(
                        "toolbox returned a non-JSON success body",
                        kind="protocol",
                        status_code=resp.status_code,
                        path=path,
                    )
                return {"ok": True, "status_code": resp.status_code, "path": path, "data": data}
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = _error(
                    f"{type(exc).__name__}: {exc}", kind="transport", path=path, method=method
                )
                log.debug("toolbox %s %s transport error: %s", method, path, last["error"])
                if attempt < attempts:
                    await self._backoff(attempt)
                    continue
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("toolbox %s %s unexpected: %s", method, path, exc)
                return _error(
                    f"{type(exc).__name__}: {exc}", kind="transport", path=path, method=method
                )
        return last

    # --------------------------------------------------------------- public API
    async def upload_file(self, local_path: str | Path, remote_path: str) -> dict[str, Any]:
        """Stream a local file to the sandbox. Parent dirs are auto-created."""
        import httpx

        src = Path(local_path)
        try:
            size = src.stat().st_size
        except OSError as exc:
            return _error(f"local file unreadable: {exc}", kind="local", path=remote_path)
        if not src.is_file():
            return _error("local path is not a regular file", kind="local", path=remote_path)

        base = await self.base_url()
        if base is None:
            return self._unconfigured(remote_path)

        attempts = self._attempts()
        last: dict[str, Any] = _error("no attempt made", kind="transport", path=remote_path)

        for attempt in range(1, attempts + 1):
            # Re-open per attempt: a partially consumed file object cannot be
            # retried without rewinding, and rewinding a live socket is not a
            # thing. Opening is cheap, buffering the world is not.
            handle = src.open("rb")
            try:
                client = await self._http()
                # Content-Length is stated explicitly: httpx cannot know the
                # length of an async iterator, so without this header the body
                # goes out as `Transfer-Encoding: chunked`. The size is known
                # from stat() and does not change between attempts, so every
                # retry advertises the same (correct) length. Stating it does
                # not buffer anything: the body is still streamed from the
                # file 64 KB at a time by FileStream.
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(size),
                }
                resp = await client.post(
                    base + "/files/upload-v2",
                    params={"path": remote_path},
                    content=FileStream(handle),   # streamed, 64 KB at a time
                    headers=headers,
                )
                log.debug(
                    "toolbox POST /files/upload-v2 path=%s size=%s -> %s body=%r",
                    remote_path, size, resp.status_code, resp.text[:MAX_ERROR_BODY],
                )
                if resp.status_code >= 400:
                    last = self._classify_status(
                        resp.status_code, remote_path, resp.text[:MAX_ERROR_BODY],
                        method="POST",
                    )
                    if self._retryable(resp.status_code) and attempt < attempts:
                        await self._backoff(attempt)
                        continue
                    return last
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                return {
                    "ok": True,
                    "status_code": resp.status_code,
                    "path": remote_path,
                    "local_path": str(src),
                    "size": size,
                    "uploaded": data if isinstance(data, dict) else {},
                }
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = _error(
                    f"{type(exc).__name__}: {exc}",
                    kind="transport", path=remote_path, method="POST",
                )
                log.debug("toolbox upload transport error: %s", last["error"])
                if attempt < attempts:
                    await self._backoff(attempt)
                    continue
            except OSError as exc:
                return _error(f"local read failed: {exc}", kind="local", path=remote_path)
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("toolbox upload unexpected: %s", exc)
                return _error(
                    f"{type(exc).__name__}: {exc}", kind="transport", path=remote_path
                )
            finally:
                handle.close()
        return last

    async def download_file(self, remote_path: str, local_path: str | Path) -> dict[str, Any]:
        """Stream a sandbox file to disk (temp file + atomic rename)."""
        import httpx

        dest = Path(local_path)
        base = await self.base_url()
        if base is None:
            return self._unconfigured(remote_path)

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return _error(f"cannot create {dest.parent}: {exc}", kind="local", path=remote_path)

        attempts = self._attempts()
        last: dict[str, Any] = _error("no attempt made", kind="transport", path=remote_path)

        for attempt in range(1, attempts + 1):
            tmp_name: str | None = None
            try:
                client = await self._http()
                async with client.stream(
                    "GET", base + "/files/download", params={"path": remote_path}
                ) as resp:
                    if resp.status_code >= 400:
                        # The body is octet-stream even on error: read a bounded
                        # slice for the log, never json-parse it.
                        raw = await resp.aread()
                        last = self._classify_status(
                            resp.status_code, remote_path,
                            raw[:MAX_ERROR_BODY].decode("utf-8", "replace"),
                            method="GET",
                        )
                        log.debug(
                            "toolbox download %s -> %s body=%r",
                            remote_path, resp.status_code,
                            raw[:MAX_ERROR_BODY].decode("utf-8", "replace"),
                        )
                        if self._retryable(resp.status_code) and attempt < attempts:
                            await self._backoff(attempt)
                            continue
                        return last

                    fd, tmp_name = tempfile.mkstemp(
                        prefix=f".{dest.name}.", suffix=".part", dir=str(dest.parent)
                    )
                    written = 0
                    with os.fdopen(fd, "wb") as out:
                        async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                            out.write(chunk)   # never accumulates in memory
                            written += len(chunk)
                    os.replace(tmp_name, dest)   # atomic: no half-written world
                    tmp_name = None
                    log.debug(
                        "toolbox GET /files/download path=%s -> %s bytes=%s",
                        remote_path, resp.status_code, written,
                    )
                    return {
                        "ok": True,
                        "status_code": resp.status_code,
                        "path": remote_path,
                        "local_path": str(dest),
                        "size": written,
                    }
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = _error(
                    f"{type(exc).__name__}: {exc}",
                    kind="transport", path=remote_path, method="GET",
                )
                log.debug("toolbox download transport error: %s", last["error"])
                if attempt < attempts:
                    await self._backoff(attempt)
                    continue
            except OSError as exc:
                return _error(f"local write failed: {exc}", kind="local", path=remote_path)
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("toolbox download unexpected: %s", exc)
                return _error(
                    f"{type(exc).__name__}: {exc}", kind="transport", path=remote_path
                )
            finally:
                if tmp_name:
                    try:
                        os.unlink(tmp_name)
                    except OSError:
                        pass
        return last

    async def create_folder(self, remote_path: str, mode: str = "755") -> dict[str, Any]:
        """POST /files/folder — both `path` and `mode` are required by the API."""
        return await self._request_json(
            "POST", "/files/folder",
            {"path": remote_path, "mode": mode}, remote_path,
        )

    async def list_dir(self, remote_path: str, depth: int = 1) -> dict[str, Any]:
        result = await self._request_json(
            "GET", "/files", {"path": remote_path, "depth": max(1, int(depth))}, remote_path
        )
        if not result.get("ok"):
            return result
        raw = result.get("data")
        entries = raw.get("files") if isinstance(raw, dict) else raw
        if not isinstance(entries, list):
            return _error(
                "toolbox returned an unexpected listing shape",
                kind="protocol", status_code=result.get("status_code"), path=remote_path,
            )
        return {
            "ok": True,
            "status_code": result.get("status_code"),
            "path": remote_path,
            "entries": [normalise_file_info(e) for e in entries if isinstance(e, dict)],
        }

    async def file_info(self, remote_path: str) -> dict[str, Any]:
        result = await self._request_json("GET", "/files/info", {"path": remote_path}, remote_path)
        if not result.get("ok"):
            return result
        data = result.get("data")
        if not isinstance(data, dict):
            return _error(
                "toolbox returned an unexpected info shape",
                kind="protocol", status_code=result.get("status_code"), path=remote_path,
            )
        return {
            "ok": True,
            "status_code": result.get("status_code"),
            "path": remote_path,
            "info": normalise_file_info(data),
        }

    async def walk_files(
        self, remote_root: str, *, max_files: int = 20000, max_depth: int = 12
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield file infos under `remote_root` (directories are recursed, not yielded)."""
        queue: list[tuple[str, int]] = [(remote_root, 1)]
        seen = 0
        while queue:
            current, depth = queue.pop(0)
            listing = await self.list_dir(current, depth=1)
            if not listing.get("ok"):
                yield {"_error": listing, "path": current}
                continue
            for entry in listing["entries"]:
                seen += 1
                if seen > max_files:
                    yield {
                        "_error": _error(
                            f"too many files under {remote_root} (>{max_files}); "
                            "set SYNC_REMOTE_ARCHIVE to pull a single archive instead",
                            kind="limit", path=current,
                        ),
                        "path": current,
                    }
                    return
                if entry.get("is_dir"):
                    if depth < max_depth:
                        queue.append((entry["path"], depth + 1))
                    continue
                if entry.get("type") == "dir":
                    if depth < max_depth:
                        queue.append((entry["path"], depth + 1))
                    continue
                yield entry


def normalise_file_info(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise a FileInfo. `modTime` is deprecated (Go layout) — never used."""
    path = str(raw.get("path") or "")
    name = str(raw.get("name") or (path.rsplit("/", 1)[-1] if path else ""))
    is_dir = bool(raw.get("isDir", raw.get("is_dir", False)))
    entry_type = str(raw.get("type") or ("dir" if is_dir else "file"))
    if entry_type == "dir":
        is_dir = True
    size = raw.get("size")
    try:
        size_int = int(size) if size is not None else 0
    except (TypeError, ValueError):
        size_int = 0
    return {
        "name": name,
        "path": path,
        "size": size_int,
        "is_dir": is_dir,
        "type": entry_type,
        "permissions": raw.get("permissions"),
        "owner": raw.get("owner"),
        "group": raw.get("group"),
        # modifiedAt is the supported field; modTime is deprecated and dropped.
        "modified_at": raw.get("modifiedAt") or raw.get("modified_at"),
    }


_client: DaytonaFilesClient | None = None


def get_client() -> DaytonaFilesClient:
    global _client
    if _client is None:
        _client = DaytonaFilesClient()
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


__all__ = [
    "AUTH_REJECTED",
    "CHUNK_SIZE",
    "DaytonaFilesClient",
    "close_client",
    "get_client",
    "normalise_file_info",
]
