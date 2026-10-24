# Daytona File / Toolbox API — verified reference (Fase 3)

Extracted **2026-10-23** from Daytona's official API reference
(<https://www.daytona.io/docs/en/tools/api/>, page version v0.225).

> ⚠️ **This is a DIFFERENT base URL from the sandbox lifecycle API.**
> Lifecycle (`/sandbox/{id}`, `/start`, `/stop`) lives on
> `https://app.daytona.io/api`. The file endpoints live on the **toolbox proxy**.
> Getting this wrong is the single most likely mistake in this phase.

## Two base URLs — do not mix them

| Surface | Base URL | Auth |
|---|---|---|
| Sandbox lifecycle | `https://app.daytona.io/api` | `Authorization: Bearer <API key>` ✅ verified |
| File / toolbox | `https://proxy.app.daytona.io/toolbox/{sandboxId}` | ⚠️ **NOT verified — see open question** |

The per-sandbox toolbox URL is also discoverable at runtime:

```
GET https://app.daytona.io/api/sandbox/{sandboxId}/toolbox-proxy-url
Authorization: Bearer <API key>
→ 200 {"url": "https://proxy.app.daytona.io/toolbox"}
```

`ToolboxProxyUrl` schema: `{ "url": string }` (required). Note the returned
`url` is the **base**, without the `{sandboxId}` segment — the per-sandbox
form appends it.

> ⚠️ **OPEN QUESTION (evidence-based, not a guess):** the embedded toolbox
> OpenAPI section declares `servers: [{url: "https://proxy.app.daytona.io/toolbox/{sandboxId}"}]`
> but carries **no `securitySchemes` of its own**. The documented API-key
> `bearer` scheme is declared on the lifecycle spec. Whether the toolbox proxy
> accepts the same `Authorization: Bearer <API key>`, or requires a
> sandbox-scoped auth token, is **not stated in the public reference**.
> Implementation must therefore treat auth as configurable and fail with a
> clean, explicit error — never a traceback — if the proxy rejects the call.

## File endpoints (all on the toolbox base)

| Purpose | Method | Path | operationId | Params |
|---|---|---|---|---|
| Upload one file | `POST` | `/files/upload-v2` | `UploadFile` | query `path` (**required**) |
| Download one file | `GET` | `/files/download` | `DownloadFile` | query `path` (**required**) |
| List directory | `GET` | `/files` | `ListFiles` | query `path`, `depth` (int ≥1, default 1) |
| File info | `GET` | `/files/info` | `GetFileInfo` | query `path` (**required**) |
| Create folder | `POST` | `/files/folder` | `CreateFolder` | query `path`, `mode` (**both required**) |
| Delete | `DELETE` | `/files` | `DeleteFile` | query `path`, `recursive` (bool) |
| Bulk upload | `POST` | `/files/bulk-upload` | `UploadFiles` | — |
| Bulk download | `POST` | `/files/bulk-download` | `DownloadFiles` | JSON body: paths |

### Upload — `/files/upload-v2`

> "Upload a file to the specified path. Accepts either multipart/form-data
> (field `"file"`) or a raw request body (e.g. `application/octet-stream`).
> **Parent directories are created if missing; an existing file is overwritten.**"

Returns `UploadedFile`: `{ "name": string, "path": string, "type": string }` (all required).

The **raw-body** form is the one that matters for us: it lets a world archive
be streamed without buffering it into a multipart envelope. Parent-directory
auto-creation means no separate `CreateFolder` round trip is needed before an
upload.

### Download — `/files/download`

`GET` with query `path`, returns `application/octet-stream`. Errors are also
`application/octet-stream` (not JSON), so a failed call must **not** be parsed
as JSON — check the status code first.

### FileInfo schema

```
{
  "name": string, "path": string, "size": number, "isDir": bool,
  "permissions": string, "owner": string, "group": string,
  "modTime": string,      // Deprecated: Go time.String() layout — do not parse
  "modifiedAt": string    // use this instead
}
```

`modTime` is explicitly **deprecated** by the reference because it uses Go's
non-standard `time.String()` layout. Use `modifiedAt`.

## Consequences for DayHost's sync transport

1. **Two clients, two base URLs.** The existing `daytona_client.py` is
   lifecycle-only. The file transport needs its own base URL
   (`TOOLBOX`), derived at runtime from `/toolbox-proxy-url` when
   `SANDBOX_ID` is known, else from an env override.
2. **Configurable auth, clean failure.** Because the toolbox auth scheme is
   unverified, the transport must:
   - read an optional `DAYTONA_TOOLBOX_AUTH` mode (`bearer` default, `none`),
   - surface an explicit, actionable error on 401/403 ("toolbox proxy rejected
     credentials — verify auth scheme against Daytona docs"),
   - never raise a traceback into the request path.
3. **Streaming, not buffering.** Use `httpx` streaming for both directions so a
   multi-hundred-MB world never lands in RAM. This is essential given the
   60 MB RSS ceiling.
4. **Never trust a download body as JSON.** It is `octet-stream` even on error.
5. `httpx` is already a dependency and is already **lazy-imported** — the file
   transport must keep it lazy so cold-idle RSS is unaffected.
