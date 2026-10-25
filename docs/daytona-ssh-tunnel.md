# Daytona SSH Access — verified ground truth

Retrieved and **verified by execution** on **2026-10-25** (Asia/Jakarta) against a live
sandbox. Every claim below was reproduced with `curl` / `ssh`, not read from a doc page.
Treat this file as the authority; do not replace it with assumptions.

Sandbox used for verification: `fd15c3c2-d4c8-4a46-981e-3e1c244b57af` (public, Daytona).

## Why this exists

The sandbox has **organisation-level egress restrictions** (Daytona Tier 1/2). Outbound
connections from inside the sandbox are blocked by an Envoy proxy:

- blocked: `mc.paus.qzz.io`, `vps.paus.qzz.io`, `fill.papermc.io`, `api.papermc.io`,
  `example.com`, `1.1.1.1` (all `Errno 104`; `http://example.com` returns `403 server: envoy`)
- allowed: `github.com`, `raw.githubusercontent.com`, `api.github.com`, `pypi.org`,
  `registry.npmjs.org` (Daytona's "essential services")

`POST /api/sandbox/{id}/network-settings` answers **400**:
`"Network access is restricted and cannot be overridden at the sandbox level."`

**Therefore the sandbox can never dial the controller.** The direction must be inverted:
the **VPS dials the sandbox** over Daytona's SSH endpoint. The VPS has unrestricted egress.

## Endpoint

```
POST {DAYTONA_API_BASE}/sandbox/{sandboxId}/ssh-access?expiresInMinutes=1440
```

- `DAYTONA_API_BASE` = `https://app.daytona.io/api`
- Auth header: `Authorization: Bearer <DAYTONA_API_KEY>`
- Request body: `{}` (JSON, `Content-Type: application/json`)

### CRITICAL — the expiry parameter must be a QUERY STRING

| Location | Result |
|---|---|
| **query `?expiresInMinutes=1440`** | **works — `expiresAt` = now + 1440 min** |
| JSON body `{"expiresInMinutes":1440}` | **silently ignored** → defaults to +60 min |
| JSON body `expiresIn`, `expiresInSeconds`, `expires_in`, `ttl`, `duration`, `expiresAt`, `ttlMinutes`, `minutes` | all silently ignored → +60 min |
| header `X-Expires-In-Minutes: 1440` | silently ignored → +60 min |
| string value `"1440"` in body | silently ignored → +60 min |

There is **no error** for the wrong location — you just get 60 minutes. If a token looks
like it expires in an hour despite asking for a day, the parameter is in the wrong place.
This is the single most expensive trap in this API; it cost a full debugging cycle.

### Response (200)

```json
{
  "id": "d845c9bf-...",
  "sandboxId": "fd15c3c2-...",
  "token": "<32 chars>",
  "expiresAt": "2026-10-25T23:59:02.172Z",
  "createdAt": "...",
  "updatedAt": "...",
  "sshCommand": "ssh <token>@ssh.app.daytona.io"
}
```

Verified: asking for `1440` produced `expiresAt` exactly 1440 minutes ahead (computed from
epoch seconds, not eyeballed).

### Auth failures

A wrong/absent key gives **401** `{"statusCode":401,"error":"Unauthorized","message":"Invalid credentials"}`.
This matters because a mangled shell quoting bug silently produced `***` as the bearer
token and every request 401'd — indistinguishable from a missing feature until the raw
body was printed. **Always print the raw response body on failure.**

### Endpoints that do NOT exist

- `GET /sandbox/{id}/ssh-access` → **404** (no list/read endpoint)
- `GET /sandbox/{id}/ports`, `/preview`, `/preview-url` → 403/404
- `POST /sandbox/{id}/ssh`, `/ssh-token`, `/ssh-access-token` → 404

Open question (untested): is there a `DELETE` to revoke a token? Not needed for our flow —
tokens expire on their own — so leave it un-configurable rather than guessing.

## Connecting

- Host: `ssh.app.daytona.io` (resolves to `13.223.35.70`, `3.214.160.170`)
- Username: **the token itself**
- A fresh token is issued per call; several may be valid at once.

### Raw TCP forwarding is CONFIRMED working

```
ssh -N -L 0.0.0.0:34212:localhost:25565 <token>@ssh.app.daytona.io
```

- `-L 127.0.0.1:13412:localhost:25565` → echo server saw `HELLO-VIA-DAYTONA-SSH`,
  replied `TUNNEL-OK:HELLO-VIA-DAYTONA-SSH`
- `-L 0.0.0.0:34212:localhost:25565` → from a machine **outside** the VPS,
  `151.243.222.93:34212` replied `PUBLIC-TUNNEL-OK:OUTSIDE-TEST-1`

So the tunnel carries **raw TCP**, unlike Daytona preview URLs, which are HTTP-only:
`https://{port}-{sandboxId}.proxy.daytona.works` serves HTTP fine (verified HTTP/2 200)
but a Minecraft handshake through it is answered `HTTP/1.1 400 Bad Request / Server: awselb`.
**Do not use preview URLs for Minecraft.**

### ssh options that matter

- `-N` no command, `-f` background after auth, `-o ExitOnForwardFailure=yes` so a failed
  bind is an error instead of a silent half-up tunnel
- `-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null` — host key is ephemeral
  per connect; leaving known_hosts alone would accumulate junk and eventually block
- Token is a credential: never log it, never put it on a command line visible via `ps`.
  Prefer `-o ProxyCommand`-free config file with mode 0600, or an `ssh -F` config file.

## Consequences for DayHost's architecture

| Need | Old (fails) | New (verified) |
|---|---|---|
| MC port 34212 | frpc → frps | `ssh -L 0.0.0.0:34212:localhost:25565` from the VPS |
| Agent control | agent dials wss:// controller | agent **listens locally**, VPS forwards to it |
| RCON 25575 | VPS dials sandbox (fails) | same SSH tunnel |
| PaperMC jar | agent curls `fill.papermc.io` (blocked) | VPS downloads, pushes into sandbox |

The 1440-minute expiry means **no per-hour renewal loop is needed** — one daily token
refresh is enough. A daemon should still renew before expiry and reconnect if the
connection drops (a sandbox restart invalidates the session).

## PaperMC jar via the VPS (verified 2026-10-25)

The sandbox cannot reach `fill.papermc.io`, so the VPS fetches the jar and pushes it in.
Both halves are verified end to end with a real 54,846,016-byte jar.

### Resolving the jar (PaperMC fill v3)

`GET https://fill.papermc.io/v3/projects/paper` returns `{"project":..., "versions":{...}}`.
**`versions` is a dict of version *groups* → arrays of versions**, not a flat object with a
`latest` key:

```
"versions": {"26.3": [...], "26.2": [...], "26.1": [...], "1.21": ["1.21.11", "1.21.11-rc3", ...], ...}
```

So the newest stable pick needs explicit selection logic, not `versions["latest"]`.

`GET https://fill.papermc.io/v3/projects/paper/versions/{version}/builds` returns a **list**
(92 entries for 1.21.11), newest first. Each entry:
`{id, time, channel, commits, downloads}` where `downloads["server:default"]` holds:

- `url` → `https://fill-data.papermc.io/v1/objects/<sha256>/paper-<ver>-<build>.jar`
- the `<sha256>` path segment **equals** `checksums.sha256`, so the download URL is self-verifying

Verified: build `132`, channel `STABLE`, sha256 `5ffef465...ccfba`, size 54846016.

### Uploading into the sandbox (toolbox)

Base: `https://proxy.app-eu.daytona.io/toolbox/{sandboxId}` (per-sandbox; the lifecycle base
`https://app.daytona.io/api` is a different surface — do not mix them).

```
POST {toolbox}/files/upload-v2?path=/tmp/paper.jar
Authorization: Bearer <DAYTONA_API_KEY>
Content-Type: application/octet-stream
<body = raw bytes>
→ 200 {"name":"paper.jar","path":"/tmp/paper.jar","type":"file"}
```

- **Raw body works** (`application/octet-stream`); multipart field `file` also returns 200.
- Parent directories are created; an existing file is overwritten.
- 54 MB uploaded in **25 s**; sha256 recomputed **inside the sandbox** matched exactly.
- The same `Authorization: Bearer <API key>` that works for `exec` also works for uploads —
  no separate sandbox-scoped token is needed (this closes the "OPEN QUESTION" in
  `docs/daytona-file-api.md`).

### Trap: a masked token in a script looks exactly like an auth failure

A shell script whose header line had been redacted to `Authorization: Bearer ***` produced
**401 `Bearer token is invalid`** from upload while `exec` in the same script succeeded.
Before concluding "this endpoint needs different auth", print the token **length** and confirm
the header is built at runtime (`A="Authorization: Bearer ${KEY}"`), never pasted literally.
This is the second time in this project that a masking artifact was mistaken for an API bug.

## B1 landed: config surface + `create_ssh_access()` (verified 2026-10-25)

Implemented and **independently verified against the live API**, not just unit-mocked.

`app/services/daytona_client.py` → `async def create_ssh_access(expires_minutes: int)`.
It deliberately does **not** reuse `_request()`, because that helper logs `resp.text` at DEBUG
and the SSH-access response body **is the credential**. Only method/path/status are logged.

Verified from the patched tree, using the project venv:

```
tunnel_mode=ssh token_min=1440 host=ssh.app.daytona.io
ok=True status=200 err=''
panjang token=32
expires_at=2026-10-26T00:14:58.589Z
umur=1440 menit -> PASS (1440)
```

The age was computed from wall-clock epoch seconds, so a silent fallback to 60 minutes would
have failed the check. Config keys: `SSH_HOST`, `SSH_TOKEN_MINUTES`, `SSH_CONNECT_TIMEOUT`,
`SSH_RENEW_MARGIN`, `SSH_BIND_MC`; `TUNNEL_MODE` accepts `frp | ssh | none`.

Regression gates re-run by hand after the patch, all green with their summary lines:
`check_rcon` → `ALL RCON CHECKS PASSED` · `check_orchestrator` → `125/125` ·
`check_sync` → `ALL SYNC CHECKS PASSED` · `check_agent_ws` → `ALL 104 AGENT-WS CHECKS PASSED` ·
`check_panel` → `62/62`. `requirements.txt` unchanged: md5 `95dfd238...fcae2`, 7 packages.
