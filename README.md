# DayHost — Fase 3

Minecraft server lifecycle dashboard. FastAPI + Uvicorn, fully async, SQLite,
Jinja2 + HTMX + Tailwind + Lucide from CDN (**zero Node/npm/build step**).

DayHost **does not** run the Minecraft server itself. The server lives in a
[Daytona](https://app.daytona.io) sandbox; DayHost starts/stops that sandbox and
reports health. Player traffic arrives through a tunnel that **the operator
manages separately** (see [Tunnel scope](#tunnel-scope--out-of-scope)).

---

## What works in Fase 2

| Area | Status |
|---|---|
| Login / logout (pbkdf2_sha256, cookie sessions in SQLite) | ✅ working |
| Dashboard (dark theme, status/ping/players cards) | ✅ working |
| Power ON / OFF / Restart (HTMX, 45s timeout, spinner) | ✅ wired to Daytona REST |
| SSE log stream (`/events`) + in-memory log bus | ✅ working |
| `GET /healthz`, `GET /readyz` | ✅ working |
| Minecraft SLP ping (hand-rolled, no `mcstatus`) | ✅ working |
| Tunnel socket probe + frpc log peek | ✅ working (read-only) |
| Console command execution (RCON) | ✅ real Source RCON, stdlib only (stub when `RCON_PASSWORD` empty) |
| World backup/restore | ✅ plans + executor, gated by `SYNC_ENABLED=false` |
| Daytona state map (all 22 `SandboxState` values) | ✅ complete, `scripts/check_state_map.py` |
| `errorReason` / `recoverable` surfacing + `POST /power/recover` | ✅ working |

---

## What's new in Fase 3

Fase 3 adds three things and **changes no default behaviour**: with an
unchanged `.env`, the app behaves exactly like Fase 2.

| Feature | What it does | Default |
|---|---|---|
| **Toolbox file transport** (`app/services/daytona_files.py`) | Streaming upload/download/list/info through the per-sandbox toolbox proxy. 64 KB chunks, file never loaded into RAM; downloads land in a temp file and are atomically renamed. Wired into sync as `SYNC_TRANSPORT=daytona-files`. | `SYNC_TRANSPORT=local` (Fase 2 path) |
| **Persistent RCON audit log** (`rcon_audit` table, `GET /api/console/audit`) | Every console attempt is written to SQLite: allowed, **refused by the safe-list**, and **rate-limited (429)**. Purged at startup by age. | `RCON_AUDIT_RETENTION_DAYS=30`, `0` = keep forever |
| **Structured logging to a rotating file** (`app/logging_setup.py`) | Plain or JSON lines to `LOG_FILE`, rotating at `LOG_MAX_BYTES` with `LOG_BACKUP_COUNT` backups, mirrored to stderr/journald. Unwritable path → warning + stderr only, never a crash. | `plain`, 5 MiB, 5 backups, `INFO` |

Harnesses: `scripts/check_daytona_files.py` (**38 checks**, fake toolbox on
loopback), plus the existing `check_http` / `check_rcon` / `check_sync` /
`check_state_map` and the new `check_logging`.

### Sync transport — `SYNC_TRANSPORT=local|daytona-files`

```dotenv
SYNC_TRANSPORT=local        # default — Fase 2 behaviour, byte-for-byte unchanged
```

* `local` — the world is assumed reachable from the DayHost host (operator
tunnel), exactly as in Fase 2. **Nothing about it changed.**
* `daytona-files` — the world is pulled out of / pushed back into the sandbox
  through the toolbox file API, before/after the local tar/rsync steps. The
  ssh/rsync step is omitted (it would be redundant).

`SYNC_ENABLED=false` still binds **every** transport: with the kill-switch off,
`daytona-files` makes **no network calls at all** — it only reports the plan it
would run. `SYNC_REMOTE_WORLD` (default `/data/world`) names the sandbox-side
directory; `SYNC_REMOTE_ARCHIVE` optionally points at one sandbox-side archive
file instead of walking the tree.

### Two different Daytona base URLs — do not mix them

| Surface | Base URL | Used by |
|---|---|---|
| Sandbox lifecycle | `https://app.daytona.io/api` | `daytona_client.py` (start/stop/status/recover) |
| File / toolbox | `https://proxy.app.daytona.io/toolbox/{sandboxId}` | `daytona_files.py` (upload/download/list/info) |

The per-sandbox toolbox base can also be discovered once at runtime from
`GET /sandbox/{id}/toolbox-proxy-url` (then cached); that is what happens when
`DAYTONA_TOOLBOX_URL` is empty. Full endpoint table, request/response shapes and
the evidence trail: **`docs/daytona-file-api.md`**.

> ⚠️ **Open question — toolbox auth is not documented.** The toolbox OpenAPI
> section declares `servers: [{url: ".../toolbox/{sandboxId}"}]` but carries
> **no `securitySchemes` of its own**; the documented `bearer` scheme belongs to
> the *lifecycle* spec. Whether the proxy accepts the same `Authorization:
> Bearer <API key>` or wants a sandbox-scoped token is **not stated publicly**.
> The mitigation is the switch **`DAYTONA_TOOLBOX_AUTH=bearer|none`** (default
> `bearer`): flip it without a code change, and a 401/403 is reported as one
> actionable sentence rather than a traceback. This is still unverified against
> a real sandbox.

### Upload framing: `Content-Length`, not chunked

`upload_file()` passes an async iterator to httpx, which cannot infer a length
and would therefore send `Transfer-Encoding: chunked`. Because the size is
already known from `stat()` — and never changes between retries, since the file
is re-opened per attempt — the header is set explicitly:

```python
headers = {"Content-Type": "application/octet-stream", "Content-Length": str(size)}
```

This states a length; it does **not** buffer anything. The body is still read
from disk 64 KB at a time by `FileStream` (verified: an 8 MB upload grows RSS by
~0.6 MB, and the receiving end sees 64–256 KB chunks).

### Audit log — what is and is not recorded

Written on **every** attempt, including the ones that never reached the server:

* timestamp, username, client IP;
* the command text (**truncated** — `MAX_AUDIT_COMMAND`, not a full transcript);
* `classification` (`allowed` / `destructive` / `unknown` / …), `allowed`,
  `executed`, and a short result summary.

**Not recorded:** RCON passwords, session cookies, or the full RCON response
body — `result_summary` is clipped. The audit table lives in the same SQLite
file as everything else (`DB_PATH`); rows older than
`RCON_AUDIT_RETENTION_DAYS` (default **30**, `0` disables purging) are deleted at
startup. A failed audit write is logged as a **warning** and never breaks the
command — but it is never silent either.

---

## Requirements

* **Python 3.11 or 3.12** (3.12 recommended). The deploy target is **not**
  guaranteed to ship either, so install one first:

  ```bash
  # Option A — uv (recommended; installs a private interpreter)
  uv venv --python 3.12 .venv

  # Option B — distro packages
  sudo apt install python3.12 python3.12-venv        # Debian/Ubuntu
  python3.12 -m venv .venv
  ```

* RAM: the RSS ceiling is **< 60 MB** for the app process, measured **under
  load** (not just at idle). See [RAM budget](#ram-budget--measured-numbers).

---

## Install

Both paths produce the same environment; `requirements.txt` is plain pip format.

### Path A — uv

```bash
cd /opt/dayhost
uv venv --python 3.12 .venv
UV_HTTP_TIMEOUT=30 uv pip install --python .venv/bin/python -r requirements.txt
```

### Path B — stdlib venv + pip

```bash
cd /opt/dayhost
python3.12 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

---

## Configure

```bash
cp .env.example .env
chmod 600 .env

# 1) admin password hash
.venv/bin/python scripts/hash_password.py 'your-password'   # -> ADMIN_PASSWORD_HASH=...
# 2) session secret
.venv/bin/python -c "import secrets;print(secrets.token_urlsafe(48))"  # -> SESSION_SECRET=...
```

Then edit `.env`:

* `DAYTONA_API_KEY`, `SANDBOX_ID` — needed for power actions. Without them the
  power endpoints answer **JSON 503** (by design) instead of erroring out.
* `DAYTONA_PATH_SANDBOX` / `DAYTONA_PATH_START` / `DAYTONA_PATH_STOP` —
  **optional** overrides for the assumed REST paths (`{sandbox_id}` is
  substituted at request time). Leave them commented out unless the real
  Daytona API differs from `/sandbox/{sandbox_id}[/*]`; this is the main
  mitigation for the unverified-endpoint risk below.
* `MC_HOST` / `MC_PORT` — where the Minecraft server is reachable *from this VPS*
  (normally `127.0.0.1` + your tunnel port).
* `PUBLIC_HOST` / `PUBLIC_PORT` — what players actually connect to (display only).
* `TUNNEL_MODE`, `FRPC_LOG_PATH` — see below.
* `ADMIN_USER` (default `admin`).
* `RCON_HOST` / `RCON_PORT` / `RCON_PASSWORD` — console. **Leave the password
  empty to keep the stub** (commands are accepted, logged, and answered with
  `executed:false`; HTTP 200, never an error). Set it and commands really run.
  `RCON_ALLOW_DESTRUCTIVE` (default `false`) gates `stop`/`op`/`ban`/`kick`/
  `whitelist off`/`gamemode`/… — see [Console & RCON](#console--rcon).
* `SYNC_ENABLED` (default `false`) — **kill-switch** for world backup/restore.
  With `false` the plans are built and logged but nothing is executed, so power
  actions behave exactly like Fase 1. Set `true` only after reading
  [Backup / restore](#backup--restore).
* `SYNC_TIMEOUT` (default 180 s per step), `SYNC_WORLD_SRC`,
  `SYNC_WORLD_DST` — world paths used by the `tar`/`rsync` steps.
* `DAYHOST_ENV=prod` also disables `/api/docs`, `/api/redoc` and
  `/api/openapi.json` (404) and marks the session cookie `Secure`.

`.env` is git-ignored; only `.env.example` is committed.

---

## Run

### Development

```bash
scripts/run.sh                 # 127.0.0.1:8000, --reload
HOST=0.0.0.0 PORT=8080 RELOAD=0 scripts/run.sh
```

`--reload` works on plain `uvicorn`: without the `standard` extras it falls
back to the stdlib `StatReload` watcher (polling, slightly slower to notice a
change, zero extra dependencies). Production `requirements.txt` deliberately
stays on plain `uvicorn`; only if you want the faster `watchfiles` watcher do
you run `uv pip install uvicorn[standard]` **by hand in your dev venv** — never
add it to `requirements.txt`, it drags in uvloop/httptools/websockets/pyyaml
that would count against the RSS budget.

### Production (systemd)

```bash
sudo useradd --system --home /opt/dayhost --shell /usr/sbin/nologin dayhost
sudo chown -R dayhost:dayhost /opt/dayhost
sudo cp deploy/dayhost.service /etc/systemd/system/dayhost.service
sudo systemctl daemon-reload
sudo systemctl enable --now dayhost
```

Expose it behind a reverse proxy (nginx/Caddy) with TLS; bind to `127.0.0.1`.

> **Why exactly one uvicorn worker?**
> `app/services/logbus.py` fans out SSE messages using in-process asyncio
> queues. With `--workers 2+`, uvicorn spreads connections across processes: a
> browser may subscribe on worker A while a power action publishes on worker B,
> and the console log would appear frozen. Multi-worker requires an external
> broker (Redis/NATS) — out of scope for Fase 1.

---

## RAM budget — measured numbers

The RSS ceiling is a **hard requirement (< 60 MB)**, and it applies to the
**loaded** process, not just a freshly started one. It was raised from 50 MB on
2026-10-23 after measurement showed the 50 MB line was being crossed under a
realistic load (dashboard render + live SSE) by ~0.1 MB. Choices made for it:

* ❌ dropped `mcstatus` → hand-rolled SLP ping (`app/services/mcping.py`, ~60 lines, stdlib only)
* ❌ dropped `pydantic-settings` → `python-dotenv` + `dataclass(frozen=True)` (`app/config.py`)
* ❌ dropped `passlib`/`bcrypt` → `hashlib.pbkdf2_hmac` (`app/auth.py`, `scripts/hash_password.py`)
* ❌ no ORM / alembic / celery / redis → `aiosqlite` raw SQL + in-process asyncio
* `MALLOC_ARENA_MAX=2` + `PYTHONMALLOC` behaviour via the systemd unit

**Known fixed cost:** `pydantic` still arrives as a transitive dependency of
FastAPI; it cannot be avoided while using FastAPI.

### Measured RSS — three conditions (this machine, CPython 3.12.14, Linux 7.2.6-zen2)

All rows: one worker, `MALLOC_ARENA_MAX=2`, `--loop asyncio --http h11`, plain
`uvicorn` (no `[standard]`), RSS read with `ps -o rss= -p <pid>`. Each condition
was measured on a **fresh process** to its own plateau (6 samples over 60 s for
condition c); the values below are steady plateaus, not transients.

| Condition | Before lazy-import | After lazy-import |
|---|---|---|
| **(a)** cold idle — 40 s, **zero** HTTP requests | 48.15 MB (49304 kB) | **47.61–47.67 MB** (48752 / 48812 kB, 2 runs) |
| **(b)** after login + one dashboard render + 30 s settle | 49.74 MB (50936 kB) | **49.59–49.64 MB** (50780 / 50836 kB, 2 runs) |
| **(c)** 20 requests + **1 live SSE consumer**, 60 s plateau | 50.13 MB (51336 kB) | **50.07 MB** (51276 kB) |

`VmHWM == VmRSS` in every run, so these are true resident plateaus. The
independent reviewer measured 48.14 / 49.67 / 50.20 MB on the pre-fix code;
my before-fix column reproduces that within allocator noise, which is the
apples-to-apples check that the harness is sound.

Condition (c) also re-measured with the **exact** `ExecStart` from
`deploy/dayhost.service` (`--workers 1 --proxy-headers --forwarded-allow-ips
--no-server-header`): **50.13 MB** dev, **50.12 MB** prod. Same plateau.

**Fase 3 re-measurement** (same three conditions, same flags, fresh process
each, `MALLOC_ARENA_MAX=2`, `LOG_LEVEL=INFO`; taken after the
`Content-Length` upload fix):

| Condition | Fase 2 (after lazy-import) | **Fase 3** | Delta |
|---|---|---|---|
| **(a)** cold idle — 40 s, zero requests | 47.61–47.67 MB | **47.54 MB** (48676 kB) | ≈ −0.1 MB |
| **(b)** after login + one render + 30 s settle | 49.59–49.64 MB | **49.07 MB** (50248 kB) | ≈ −0.5 MB |
| **(c)** 20 requests + 1 live SSE consumer, 60 s plateau | 50.07 MB | **49.58 MB** (50772 kB) | ≈ −0.5 MB |

`VmHWM == VmRSS` in all three runs; condition (c) was flat across seven
10-second samples. The three Fase 3 numbers land **below** the Fase 2 plateaus,
not above: the toolbox client is imported lazily and the rotating-file handler
replaces the existing handler rather than adding a second one, so an idle
process pays nothing for either. An independent audit of the same code measured
47.45 / 49.12 / 49.56 MB; the two sets agree within ~0.1 MB, which is the
apples-to-apples cross-check.

**The 60 MB ceiling is unchanged** — the worst case here (49.58 MB under load)
leaves **~10.4 MB** of headroom.

Historic Fase 1 reference rows, same machine:

| Configuration | RSS |
|---|---|
| **before** the 1c fix — `uvicorn[standard]` deps installed (uvloop/httptools/watchfiles/websockets/pyyaml) | **50.9 MB** idle / **51.8 MB** under load (52128 / 53064 kB) |
| plain `uvicorn==0.32.*` — Fase 1 baseline cold idle | **47.6 MB** (48696 / 48748 kB, two runs) |
| same — Fase 1, after one login + 30 s settle | **48.6 MB** (49784 kB) |
| same — Fase 1, after login + 20 dashboard loads, then 30 s settle | **48.9 MB** (50096–50104 kB) |
| same — Fase 1, sustained load (login + 250 requests) **+ 1 live SSE consumer** | **49.6–49.9 MB** (50784–51108 kB) |

> **Fase 2 RSS note (corrected).** An earlier revision of this section claimed
the Fase 2 work was free — "idle 48.1–48.3 MB, delta ≈0–0.2 MB" and "importing
`rcon` + `sync` changed `ru_maxrss` by 0 MB". **Both claims were wrong.** They
were taken from cold-idle only, which is the lightest possible condition, and
the import test was measured against a process that had already imported the
same stdlib modules. Re-measured properly:
>
> * `app.services.rcon` costs **+128 kB** `ru_maxrss` and `app.services.sync`
>   costs **+128 kB** (3 runs each, exact deltas, not rounded to MB);
> * `app.services.mcping` and `app.services.tunnel` cost **0 kB** — they only
>   add `asyncio`/`json`/`socket`/`struct`/`dataclasses`, all already resident,
>   so they stay as module-level imports and are **not** lazy;
> * the regression appeared *after login and under load*, never at cold idle,
>   which is exactly why a cold-idle-only measurement is not a valid conclusion.
>
> The lazy imports are now in place (`rcon` inside `console_send`, `sync`
> inside `_power`) and they recover ~0.5 MB at cold idle and ~0.15 MB at login.
> They do **not** recover the whole gap: at condition (c) the saving is ~60 kB,
> because the person driving the dashboard is by definition also going to open
> the console or hit a power action, which re-imports both modules on demand.
> The remaining ~1.9 MB above cold idle is **not** import weight — see the
> attribution below.

> The ~3 MB between the two `[standard]` rows is exactly the `standard` extras:
> uvloop/httptools are dead weight because the unit pins `--loop asyncio
> --http h11`, SSE is not websocket, and `--reload` falls back to the stdlib
> `StatReload` watcher. They are no longer in `requirements.txt`.
>
> **Why the ceiling moved from 50 MB to 60 MB.** The old 50 MB line sat below
> the measured steady state of a realistic loaded process (50.07–50.20 MB), so
> it was passable only by measuring the lightest condition. Rather than tune the
> app into the last 200 kB, the requirement was re-set to **< 60 MB**, which
> leaves ~9.9 MB of headroom over the worst measured case (50.13 MB) instead of
> ~0.1 MB. That is a deliberate, documented change of the target — not a
> regression being papered over. The 50 MB figures in older revisions are
> superseded.

**Where the loaded-process RSS actually goes.** Step attribution on one fresh
process (condition c path, dev env, exact unit flags):

| Step | RSS | Step cost |
|---|---|---|
| after startup, no requests | 47.66 MB | — |
| + `GET /healthz` (bare ASGI round trip) | 47.74 MB | +0.08 MB |
| + `POST /login` | 47.86 MB | +0.12 MB |
| + `GET /api/status` (`gather_status`) | 49.65 MB | **+0.69 MB** |
| + `GET /` (Jinja dashboard render) | 50.13 MB | **+0.40 MB** |
| + 19 more requests | 50.13 MB | 0 |
| + 1 live SSE consumer | 50.13 MB | ~0 (within plateau) |

Two findings that shaped the fix:

1. **`gather_status` is the single biggest step (+0.69 MB), and it is not an
   import.** It is the first call that runs the frp liveness scan
   (`asyncio.to_thread(_frpc_process_running)`, which lists `/proc/*/comm`).
   Starting that worker thread costs ~0.65 MB of allocator arenas. Measured
   directly: `TUNNEL_MODE=frp` → +956 kB and threads 2→3; `TUNNEL_MODE=ssh`
   (no `/proc` scan) → +308 kB and threads stays 2. This is the only remaining
   structural cost worth attacking, and it is a one-off per-process cost, not a
   leak.
2. **The Jinja render costs ~0.40 MB** but only **77 kB of it is Python heap**
   (tracemalloc, grouped by line: `jinja2/environment.py:709` 21.9 kB,
   `jinja2/visitor.py:33` 11.6 kB, `encodings/unicode_escape` ~8 kB, the rest
   regex compile + frame overhead). The rest is glibc arena growth from
   compiling/rendering the two templates for the first time. It does not repeat
   on subsequent requests (a second and third render added 0 kB).
3. **SSE itself is nearly free once warm**: the first `/events` subscription
   costs ~0.43 MB, each additional open/close cycle ~13 kB, and holding one
   stream open for 20 s adds nothing further. A bare TCP connection with no
   request costs 0 kB, so the cost is the first flush of the stream, not the
   socket.

Per-module `ru_maxrss` when imported into a warm `app.main` (3 runs each, exact):
`rcon` **+128 kB**, `sync` **+128 kB**, `rcon`+`sync` **+128–256 kB**,
`mcping`+`tunnel` **0 kB**, `dataclasses` **0 kB**, `jinja2` **0 kB**.

Import-cost attribution (clean venv, idle): bare Python **14.0 MB**, +Starlette
**14.0 MB**, +Jinja2 **21.8 MB**, +FastAPI/pydantic **42.2 MB**, full app (incl.
uvicorn) **43.3 MB**. In other words **pydantic/FastAPI is ~20 MB of the total**
and remains the single blocker to a comfortable margin.

Because of this, three imports are deliberately **lazy**:

* `httpx` inside `app/services/daytona_client.py` (only needed once a real
  Daytona call is made),
* `rcon` inside `console_send` in `app/routers/console.py` (**+128 kB** if
  eager, and only needed when a command is actually sent),
* `sync` inside `_power` in `app/routers/dashboard.py` (**+128 kB** if eager,
  and only reached from a power action).

`mcping` and `tunnel` stay as module-level imports on purpose: measured at
**0 kB** each when imported into a warm `app.main`, so lazy-importing them
would add complexity for no RSS benefit. The systemd unit sets
`MALLOC_ARENA_MAX=2` and pins `--loop asyncio --http h11`.

### Re-measuring RSS (do this after any change)

Measure **all three conditions** on a fresh process each time. A cold-idle-only
number is not a valid result — the loaded process is ~2.5 MB heavier:

```bash
# (a) cold idle: start, wait 40 s, DO NOT send any request
ps -o rss= -p <uvicorn pid>          # kB

# (b) login, GET / once, wait 30 s, measure again
# (c) login (verify GET / == 200!), 20 requests, 1 live /events stream,
#     then sample every 10 s for 60 s and check the plateau is flat
```

If `GET /` returns 401 the login failed and condition (c) is invalid — the
numbers would describe unauthenticated 401s, not real load.

Numbers above were taken with `MALLOC_ARENA_MAX=2`, `--loop asyncio --http h11`,
plain `uvicorn` (no `[standard]`), one worker, on CPython 3.12.14 /
Linux 7.2.6-zen2. **The worst measured case is 50.13 MB, leaving ~9.9 MB of
headroom under the 60 MB ceiling.** Any regression must be explained before
merge; do not "fix" it by raising the ceiling again. (Fase 3 re-measured lower
across all three conditions — worst case 49.58 MB, ~10.4 MB of headroom.)

If RSS ever approaches 60 MB again, options in order of effort:

1. `MALLOC_ARENA_MAX=2` (already set in the unit). **Do not** add
   `PYTHONMALLOC=malloc` — measured *worse* in earlier runs.
2. Keep `--loop asyncio --http h11` (already in the unit) — saves ~3 MB vs the
   `uvloop`/`httptools` defaults.
3. Lazify the remaining heavy paths if they ever land in the hot path:
   `dataclasses` in `rcon.py` is measurably **free** (0 kB, already resident via
   FastAPI), so removing it is not worth the churn. The real target is the
   `gather_status` frp probe: `TUNNEL_MODE=ssh` costs 0.65 MB less than `frp`
   because it skips the `/proc` scan thread. Caching the frpc probe for a few
   seconds would remove that thread start entirely if it ever matters.
4. Replace FastAPI with **Starlette-only** + small routing helpers. Measured
   live Starlette-only server: **~33 MB** idle (~17 MB saved, mostly pydantic),
   at the cost of losing automatic validation/OpenAPI.

---

## Tunnel scope — OUT OF SCOPE

DayHost **never starts, stops, or restarts the tunnel**. The FRP (or SSH)
tunnel is the operator's responsibility — a separate systemd unit, `autossh`,
or an frps/frpc setup outside this project.

DayHost only *observes*:

1. **Socket probe** — TCP-connect to `MC_HOST:MC_PORT` (`app/services/tunnel.py`).
2. **frpc log peek** — tail of `FRPC_LOG_PATH`, plus a `/proc` scan for a
   running `frpc` process. Read-only, no subprocess.

This is stated in the dashboard footer and in `app/services/tunnel.py`.
**TODO (fase 2):** optional convenience wrapper *if* the operator explicitly
wants it — never enabled by default.

---

## Console & RCON

`app/services/rcon.py` is a **dependency-free Source RCON client** (stdlib
`asyncio` + `struct` only — no `rcon` package, nothing added to
`requirements.txt`). Wire format: 4-byte little-endian length, 4-byte request
id, 4-byte type, body, 2 null bytes; auth is type 3, commands type 2,
responses type 0. Default timeout **5 s** (`RCON_TIMEOUT`), applied to connect,
auth and each read; bodies are capped at 4 KiB and packets at 64 KiB so a
hostile peer cannot make the process allocate unbounded memory. One connection
per command, so a half-dead socket can never poison the next command.

### Two modes

| `RCON_PASSWORD` | Behaviour |
|---|---|
| empty (default) | **Stub preserved** from Fase 1: `200`, `executed:false`, `stub:true`, plus a note naming `RCON_PASSWORD`. Never an error. |
| set | Command is really sent; the response is published to the SSE log bus and returned as `response`. |

### Allow-list (deny-by-default)

`classify()` in `app/services/rcon.py` returns `safe`, `destructive` or
`unknown`:

* **safe** — read-only verbs (`list`, `tps`, `seed`, `help`, `time query`,
  `say`, `whitelist list`, …). Allowed.
* **destructive** — `stop`, `restart`, `op`, `deop`, `ban`, `ban-ip`, `kick`,
  `kill`, `clear`, `give`, `gamemode`, `difficulty`, `gamerule`, `tp`,
  `setblock`, `fill`, `summon`, `whitelist`, `execute`, `reload`, `save-all`, …
  **Refused with 403 unless `RCON_ALLOW_DESTRUCTIVE=true`.**
* **unknown** — anything else. **Refused with 403.** Minecraft simply has too
  many world-mutating verbs to enumerate as safe, so a positive allow-list is
  the only defensible default. Add genuinely-safe commands to `SAFE_COMMANDS`.

Rejections are structured JSON (`{"ok":false,"error":…,"classification":…}`),
never a traceback, and are echoed into the log bus.

### Rate limit

One command per `RCON_MIN_INTERVAL` seconds (default **1 s**) per logged-in
user, tracked in a small in-memory dict (monotonic clock). Violations get
**429** with `retry_after_s`. The limit is checked only for commands that would
actually run, so a rejected destructive command does not consume the budget.
State is process-local — correct, because the deployment is single-worker.

---

## Backup / restore

`app/services/sync.py` keeps the Fase 1 **plan builders** (`build_backup_plan`,
`build_restore_plan` — pure, side-effect free, inspectable) and adds an
**executor** (`run_plan`):

* `asyncio.create_subprocess_exec` only — **never** `shell=True`, never a
  command string. The plan is data, and argv is passed as a list.
* **Binary allow-list**: `mkdir`, `tar`, `rsync`, `rclone`. Anything else is
  refused *before* spawning.
* Per-step timeout (`SYNC_TIMEOUT`, default 180 s) → process killed, reported.
* stdout/stderr captured (truncated at 64 KiB), returned in the result.
* Missing binary → **warning in the log, step marked skipped, no crash.** On
  this host `tar` and `rsync` exist; `rclone` and `frpc` do not.
* A skipped step makes the plan report `ok:false` — "backup finished" is never
  claimed when `tar` did not actually run.

### Kill-switch

`SYNC_ENABLED=false` (**default**). `pre_stop_backup()` / `post_start_restore()`
are wired into the power flow (`/power/off`, `/power/on`, `/power/restart`,
`/power/recover`) but with the switch off they only build and log the plan
(`executed:false`, `results:[]`). Production behaviour is therefore unchanged
from Fase 1 until an operator opts in.

With `SYNC_ENABLED=true`:

* **off / restart** → backup runs *before* `stop`. If it fails, the power-off is
  **aborted** (502) — losing the world is worse than a failed stop.
* **on / restart** → restore runs *after* `start`; a failure is logged loudly
  but does not undo the start.

**systemd caveat:** `deploy/dayhost.service` runs with
`ProtectSystem=strict` and `ReadWritePaths=/opt/dayhost/data`. A backup writing
into `BACKUP_DIR` is fine, but a *restore* that writes to `SYNC_WORLD_DST`
(e.g. `/data/world`) will be blocked by the sandbox. Add the target to
`ReadWritePaths=` **only** if DayHost really owns that directory on the host.

The restore plan only adds the `rsync` overlay step when a staged tree exists
at `BACKUP_DIR/world/` (populated by an ssh-mode backup); otherwise `rsync`
would exit 23 and abort an otherwise successful restore.

---

## Project layout

```
app/
  config.py            # dotenv + frozen dataclass settings
  db.py                # aiosqlite connection, schema, event log
  auth.py              # pbkdf2 hashing + cookie sessions + require_user
  main.py              # app factory, lifespan, error handlers
  templating.py        # shared Jinja2 env
  routers/
    auth.py            # /login, /logout
    dashboard.py       # /, /api/status, /partials/status, /power/*, /events (SSE)
    daytona.py         # /api/daytona/{status,start,stop,restart}
    console.py         # /api/console/* — RCON (allow-list + rate limit)
    health.py          # /healthz, /readyz
  services/
    daytona_client.py  # async Daytona REST client (verified endpoints, 22-state map)
    mcping.py          # hand-rolled Minecraft SLP ping (no mcstatus)
    tunnel.py          # read-only tunnel probes
    rcon.py            # Source RCON client, stdlib only
    sync.py            # backup/restore planners + allow-listed executor
    logbus.py          # in-process SSE fan-out (single worker!)
  templates/           # base, dashboard, login, partials/status
  static/js/console.js # SSE client + command sender
  static/css/app.css   # small local styles
scripts/
  run.sh               # dev runner
  hash_password.py     # generate ADMIN_PASSWORD_HASH
  check_state_map.py   # assert all 22 SandboxState values map correctly
  check_rcon.py        # fake RCON server: framing, auth, commands
  check_sync.py        # kill-switch, allow-list, missing binary, real tar
  check_http.py        # live uvicorn: healthz/login/dashboard/console/recover
  check_logging.py     # rotation/retention, one line per event, JSON, fallback
deploy/
  dayhost.service      # systemd unit (1 worker, hardened)
```

---

## API

| Method | Path | Auth | Notes |
|---|---|---|---|
| GET | `/healthz` | none | liveness; no external I/O |
| GET | `/readyz` | none | shows which config is present |
| GET | `/login` | none | login form |
| POST | `/login` | none | sets session cookie |
| POST | `/logout` | none | clears cookie + DB row |
| GET | `/` | session | dashboard |
| GET | `/api/status` | session | combined JSON status |
| GET | `/partials/status` | session | HTMX status fragment |
| POST | `/power/on` \| `/power/off` \| `/power/restart` | session | HTMX; **503 JSON** if Daytona unconfigured |
| GET | `/api/daytona/status` | session | 503 JSON when unconfigured |
| POST | `/api/daytona/start` \| `/stop` \| `/restart` | session | 502 JSON on upstream failure |
| POST | `/api/daytona/recover` | session | recover from `state=error` when `recoverable` |
| GET | `/events` | session | SSE log stream |
| POST | `/power/recover` | session | HTMX fragment; same as `/api/daytona/recover` |
| POST | `/api/console/send` | session | RCON; 403 destructive/unknown, 429 rate limit, stub when unconfigured |
| GET | `/api/console/history` | session | recent ring-buffer lines |

Interactive docs: `/api/docs` — **dev only**. With `DAYHOST_ENV=prod`,
`docs_url`, `redoc_url` and `openapi_url` are all `None`, so `/api/docs`,
`/api/redoc` and `/api/openapi.json` return **404**.

---

## Fase 2 status / remaining TODO

Done in Fase 2:

1. ✅ **RCON console** — `app/services/rcon.py`, command allow-list, rate limit,
   output streamed into the log bus.
2. ✅ **Backup/restore** — executor with `asyncio.create_subprocess_exec`,
   timeouts, binary allow-list; wired into the power actions behind
   `SYNC_ENABLED`.
3. ✅ **Daytona endpoints verified** against the official API (v0.225); complete
   22-value state map; `errorReason` / `recoverable` surfaced; `recover` wired.

Still open:

4. **Tunnel supervision (opt-in)** — only if the operator asks.
5. **Rate-limit persistence** — in-memory by design (single worker); revisit
   only if multi-worker ever lands.

## Fase 3 status / remaining TODO

Done in Fase 3:

1. ✅ **Toolbox file transport** — `app/services/daytona_files.py` streams
   upload/download (64 KB chunks, no file in RAM), plus list/info/folder/walk.
   Uploads now state `Content-Length` explicitly, so the body is *not* chunked.
2. ✅ **`SYNC_TRANSPORT=local|daytona-files`** — default `local`; Fase 2 behaviour
   is unchanged and still gated by `SYNC_ENABLED`.
3. ✅ **Persistent RCON audit log** — SQLite `rcon_audit`, retention via
   `RCON_AUDIT_RETENTION_DAYS`, read back through `GET /api/console/audit`.
   Refused and rate-limited attempts are recorded too.
4. ✅ **Structured logging** — `app/logging_setup.py`, plain/JSON, rotating file
   with `LOG_MAX_BYTES`/`LOG_BACKUP_COUNT`, stderr fallback when unwritable.
5. ✅ **Logging regression harness** — `scripts/check_logging.py` proves rotation
   and retention, one line per event in **both** sinks (the root vs. uvicorn
   double-handler bug), no lost lines, valid JSON keys, and the unwritable-path
   fallback. Stdlib only, temp dir cleaned up, exit 0 = pass.

Still open in Fase 3:

6. **Toolbox auth scheme is unverified.** No `securitySchemes` in the toolbox
   spec → whether `Bearer <API key>` is accepted is unknown until it is tried
   against a real sandbox. Mitigation shipped: `DAYTONA_TOOLBOX_AUTH=bearer|none`.
7. **`daytona-files` sync transport has not been run against a live sandbox** —
   only against the fake toolbox on loopback (`scripts/check_daytona_files.py`).
   The kill-switch keeps it inert by default.
8. **`walk_files` budget** — capped at 20 000 files / depth 12. Exceeding it
   yields an explicit `kind="limit"` error telling the operator to set
   `SYNC_REMOTE_ARCHIVE`; it does not truncate silently. Revisit only if a real
   world trips it.

---

## Daytona API notes (resolved)

The Fase 1 "open question" is closed — see **`docs/daytona-api.md`** for the
verified table extracted from the official API reference (v0.225, 2026-10-23):

* base `https://app.daytona.io/api`, `Authorization: Bearer <key>` — the Fase 1
  assumptions were correct;
* `GET /sandbox/{idOrName}`, `POST .../start`, `POST .../stop` — correct;
* **no restart endpoint** -> restart stays stop-then-start;
* `SandboxState` has **22 values**, all now mapped (`scripts/check_state_map.py`);
* `POST .../recover` exists and is offered only when `recoverable` is true.

Paths remain env-overridable (`DAYTONA_PATH_*`) as a cheap escape hatch, raw
responses are logged at **DEBUG**, and any *unrecognised* state still degrades
to `UNKNOWN` without raising.

Which process/port the FRP tunnel exposes locally may differ per host; the
socket probe simply reports what it sees.
