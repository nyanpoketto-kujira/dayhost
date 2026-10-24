# DayHost × mc-orchestrator — Fusion Analysis (LANGKAH 1 deliverable)

Audit performed **2026-10-24** by Ner. Every claim below was verified by reading the
source on disk or by running a probe — nothing is assumed.

Scope audited: `mc-orchestrator/` (22 files, 4081 Python lines) + DayHost Fase 1–3.

---

## 0. TL;DR — and four corrections to the brief

The brief is directionally right: DayHost v1 **is** only a power switch. But four of its
premises do not match what is actually on disk. Stating them up front because two of them
change the plan:

| # | Brief says | Reality on disk | Impact |
|---|---|---|---|
| 1 | "`agent.py`… yang WAJIB menyertakan binary `frpc`" | **`grep -ri frp mc-orchestrator/` → 0 hits.** There is no FRP anywhere in the project. | FRP is **new work**, not integration. |
| 2 | "men-download portable **JRE 21**" | Agent downloads **JDK 25** (`adoptium.net/.../25/ga/...`, dir `runtime/jdk25`). | Version decision required (§4). |
| 3 | "Daytona Exec/Toolbox API" | **Exists** — verified: `/process/exec`, `/process/session/{id}/exec`, `/process/session`, opIds `ExecuteCommand`/`CreateSession`/`GetSession`/`ListSessions`. | Brief is correct here. Good. |
| 4 | "controller berbasis **aiohttp**… WebSocket" | True, and it matters: DayHost's 7-package venv has **no `websockets`/`wsproto`**. FastAPI `@app.websocket` returns **404** ("No supported WebSocket library detected" — verified by running it). | **Architecture fork — §3.** |

---

## 1. What mc-orchestrator actually is

A two-process orchestrator, both halves **stdlib-only on the agent side** and **aiohttp-only
on the controller side** (`requirements.txt` = `aiohttp>=3.9,<4` — a single dependency).

```
controller/            aiohttp panel + WebSocket hub
  app.py      668 l    routes, auth (hmac cookie/basic), agent-ws, browser-ws, delta upload
  recovery.py 470 l    watchdog → SSH bootstrap of the worker host
  storage.py  430 l    mirror, delta apply, snapshots, plugin store
  config.py   230 l    env parsing + validation
  state.py    171 l    in-memory state + browser fan-out
  ssh.py      155 l    sshpass-driven remote exec/upload
  util.py             safe tar paths, atomic json, env parsing
  static/index.html  633 l panel UI
agent/agent.py 1250 l  stdlib-only worker agent (WS client, MC lifecycle, delta, plugins)
scripts/install_agent.sh  manual bootstrap
scripts/rdp_password.py   rotates the SoloA SSH password
install.sh                Debian/Ubuntu installer + systemd unit
```

### The genuinely good engineering (keep this)

* **`agent/agent.py` is a small marvel**: hand-rolled RFC-6455 WS client (masking, ping/pong,
  fragmentation, frame-size cap), PaperMC lifecycle with graceful `stop` → SIGTERM → SIGKILL,
  `/proc` orphan reaping, delta-by-hash/mtime, tar with a manifest, path-traversal-safe tar
  extraction, plugin checksum verification, JDK+Paper auto-download. **Zero third-party imports.**
* **Delta-over-HTTP, not rsync-over-SSH** — deliberate and documented (README §"Catatan desain").
  It survives a NAT'd/ephemeral worker far better than rsync would.
* **`storage.py` mirror+snapshot design**: the mirror is the source of truth for restore;
  snapshots are `tar` of the mirror; `latest.tar.gz` symlink; bounded pruning of both. Solid.
* **Safety hygiene**: `resolve_under()` for every tar member, plugin name allow-list, atomic
  JSON writes (fsync + replace), password passed via `SSHPASS` env (never argv).

### The SoloA-specific parts (remove these)

* `ssh.py` + `rdp_password.py` + every `SSH_*` / `RDP_*` setting.
* The assumption that the worker has a routable host and a daily rotating password.
* `recovery.py`'s bootstrap chain (`sshpass` → `scp` JDK/Paper/agent → `setsid nohup python3`).

---

## 2. Comparison table — keep / replace / add

| Component | Verdict | Reason |
|---|---|---|
| `agent/agent.py` | **KEEP ~95 %** | Portable, dependency-free, already does JDK+Paper+eula+delta+plugins. Only the *control channel* and *artifact URLs* need touching. |
| `controller/storage.py` | **KEEP as-is** | Host-agnostic; nothing about it is SoloA-specific. |
| `controller/state.py` | **KEEP as-is** | Pure in-memory fan-out. |
| `controller/util.py` | **KEEP as-is** | Pure helpers. |
| `controller/static/index.html` | **KEEP, extend** | Already has console/backups/plugins; needs Daytona host status + port-forward status. |
| `controller/config.py` | **KEEP shape, replace the SSH block** | Swap `SSH_*`/`RDP_*` for `DAYTONA_*` + `FRP_*`. |
| `controller/recovery.py` | **REWRITE the `_attempt` leg** | Keep the watchdog/retry/state-machine skeleton; replace `_daily_password` + `_ensure_remote` with a Daytona driver. |
| `controller/ssh.py` | **DELETE** | Replaced by `daytona_driver.py`. |
| `scripts/rdp_password.py` | **DELETE** | SoloA-only. |
| `install.sh` `sshpass` dep | **DELETE** | No more SSH. |
| DayHost `app/services/daytona_client.py` | **KEEP, reuse** | Verified endpoints + complete 22-value state map. |
| DayHost FastAPI panel (SSE, auth, RCON, audit, logging) | **KEEP** (see §3) | Works, audited, 150 assertions green. |
| **FRP client in the agent** | **ADD (new)** | Does not exist today. |
| **Daytona driver** (`start`/`stop` + `/process/exec` bootstrap) | **ADD (new)** | This is the whole point of the fusion. |
| **Toolbox file transport** | **REUSE** | DayHost Fase 3 already has streaming `upload`/`download` + `list`/`info`. |
| **FRP server (`frps`) on the VPS** | **ADD (new)** | Also does not exist yet. |

---

## 3. ⚠️ Architecture decision required — the WebSocket fork

**Verified fact.** DayHost's `requirements.txt` is 7 packages and contains neither
`websockets` nor `wsproto`. Running a FastAPI `@app.websocket` endpoint in that venv returns:

```
WARNING:  Unsupported upgrade request.
WARNING:  No supported WebSocket library detected. Please use "pip install 'uvicorn[standard]'",
          or install 'websockets' or 'wsproto' manually.
HTTP/1.1 404 Not Found
```

mc-orchestrator's whole control loop is WebSocket: agent ↔ controller. So the fusion
**must** answer "where do the WS frames terminate?" Three options, costed:

| Option | New deps | Effort | RSS | Verdict |
|---|---|---|---|---|
| **A. Dedicated stdlib WS server** — a small `asyncio.start_server` on its own port (e.g. `AGENT_WS_PORT=8765`) that accepts the agent using the **hand-rolled RFC-6455 framing already proven in `agent.py`**. FastAPI keeps the panel. The two talk over an in-process pub/sub. | **0** | Medium (write the server half + handshake) | +~1 MB | ✅ **Recommended** |
| **B. Add `websockets`** (8th package) and use FastAPI `@app.websocket`. | 1 (`websockets`) | Low | +~2–3 MB | Acceptable, but breaks the 7-package rule we have held for three phases. |
| **C. Replace WS with SSE + POST + long-poll** | 0 | High (rewrite the agent's transport, which is the most battle-tested code we have) | ~0 | ❌ Don't touch working code. |

**Ner's recommendation: Option A.** It preserves the zero-dependency discipline that has held
since Fase 1, keeps the audited FastAPI panel, and reuses framing code that is *already in the
repo and already proven*. The agent needs **no change at all** to its WS client — only the URL.

> **This is the one item I want Gyoju to ratify before we build on it.** Everything else in
> §5 can proceed regardless.

---

## 4. Decisions taken (with reasons)

| Question | Decision | Reason |
|---|---|---|
| Java version | **21**, not 25 | Brief specifies 21; PaperMC 1.21.x requires 21+; 25 is a heavier download and buys nothing. `agent.py`'s `java_version_ok()` must change `>= 25` → `>= 21`, and the Adoptium URL `/25/` → `/21/`. |
| FRP client binary | **Fetch at bootstrap** from the pinned GitHub release, verify SHA-256, extract `frpc` | Not committed to the repo (binary bloat, version drift). Same pattern the agent already uses for JDK/Paper. |
| FRP version | **Pinned** in config (`FRP_VERSION`), never floating | Reproducible provisioning. |
| FRP transport | `frpc` **inside** the sandbox → `frps` **on the VPS**, TCP tunnel for 25565 | Only way to expose a NAT'd sandbox without a public IP. |
| FRP secret | `FRP_TOKEN`, per-deployment, injected at provision time | Never in the repo; `frpc.toml` written with mode 600. |
| Where does the agent live inside Daytona? | `~/mc-orchestrator/agent/agent.py`, launched via `/process/exec` with `setsid nohup` | Mirrors the existing layout; survives the exec call returning. |
| Bootstrap entry point | Daytona `/process/session/{id}/exec` (fallback `/process/exec`) on the **toolbox** base URL | Verified to exist (§0 #3). Toolbox base ≠ lifecycle base — see `docs/daytona-file-api.md`. |
| Panel | **Keep FastAPI** (Option A) | Audited, 150 assertions, RCON/audit/logging already done. |

---

## 5. The bootstrap flow (what happens when the operator clicks **Start**)

Target: **one button, zero manual terminal steps.** Sequence, with the exact call at each step:

```
[operator] ── click "Start" ──▶ DayHost controller (VPS)

 1. Daytona lifecycle    POST {DAYTONA_API_BASE}/sandbox/{id}/start        ← already built
                         poll GET  /sandbox/{id} until state == started

 2. Is the agent already there?
                         Toolbox GET /files/info?path=~/mc-orchestrator/agent/agent.py
                         ├─ present + hash matches  → skip to (5)
                         └─ missing/drifted         → (3)

 3. Push artifacts       Toolbox POST /files/upload-v2?path=...           ← already built (Fase 3)
                         • agent/agent.py
                         • agent/agent.env  (mode 600, contains token + FRP config)
                         • runtime/frpc      (if absent)                  ← NEW

 4. First-run provision   Toolbox POST /process/session/{id}/exec         ← NEW (driver)
                         runs: ensure python3 → fetch JDK 21 → fetch PaperMC
                               → eula.txt=true → write frpc.toml
                         …or simply launch the agent and let agent.py do it
                         (agent already self-provisions JDK+Paper — prefer this)

 5. Launch agent          Toolbox POST /process/session/{id}/exec
                         cd agent && setsid nohup python3 agent.py >> agent.log 2>&1 &

 6. Agent connects back   agent ──ws──▶ controller :AGENT_WS_PORT   (Option A server)
                         hello → controller learns hostname/plugins/mc_state

 7. Agent starts MC+FRP   agent.py starts PaperMC, then frpc with the injected
                         frpc.toml → 25565 becomes reachable on the VPS port

 8. Panel goes live       SSE/WS to browser: host status, agent status, console,
                         player count, port-forward status, backup deltas.
```

**Stop** is the mirror image: `stop` MC → flush backup → kill agent → `POST /sandbox/{id}/stop`.

**Recovery** keeps mc-orchestrator's watchdog skeleton, but `_ensure_remote` becomes
"re-run steps 2–6", which is idempotent by construction (hash-checked uploads).

---

## 6. Environment constraints (LANGKAH 3) and what they force

| Given | Consequence |
|---|---|
| OpenVZ 7 **NAT** VPS, 1 GB RAM, ~6 GB disk | Controller must stay lean (<60 MB). Cache JDK/Paper **once** on the VPS and reuse for every sandbox; never keep more than ~2 snapshots by default (`SNAPSHOT_KEEP`). |
| Reverse proxy **Caddy** → `https://mc.paus.qzz.io` | Panel binds `127.0.0.1`. Agent WS must be reachable **by the sandbox over the public internet** — so either Caddy proxies the WS path too, or the WS server listens on a public port guarded by `AGENT_TOKEN`. Prefer Caddy path proxy (`/agent-ws`) so only 443 is exposed. |
| Public MC port is NAT'd (`play.paus.qzz.io:<PORT_NAT>`) | `frps` binds the **internal** port on the VPS; the hoster's NAT forwards `<PORT_NAT>` → that internal port. DayHost must display both so the operator can give players the right address. |
| `frps` itself lives on the VPS | It is a **separate systemd unit**, not part of the controller — exactly like the tunnel stance we already took in Fase 1/2. DayHost *observes* it, only optionally supervises it. |
| Zero new deps + <60 MB | Reinforces **Option A** in §3. |

---

## 7. Roadmap, file by file (LANGKAH 2 deliverable)

Ordered so that each step is independently verifiable and nothing depends on a later step.
Phases A–D are all inside the DayHost tree; E is the agent; F retires the SoloA code.

| Phase | Files | Deliverable | Gate (must pass) |
|---|---|---|---|
| **A. Driver** | `app/services/daytona_driver.py` (new), `app/config.py` (+`DAYTONA_TOOLBOX_*`, `FRP_*`) | Lifecycle **and** process-exec in one client | Unit test against a fake Daytona server: start/stop/exec/inspect, 0 traceback, auth failure → actionable error |
| **B. WS server** | `app/services/agent_ws.py` (new) | Stdlib RFC-6455 **server** (mirror of the agent's client), token auth, per-agent session registry, in-process pub/sub | Fake agent connects, handshakes, exchanges hello/command/log; framing matches RFC; bad token → 401; 0 new deps (assert requirements still 7) |
| **C. Fusion core** | `app/services/orchestrator.py` (new), port `storage.py`/`state.py`/`util.py` into `app/services/` | Provisioning state machine (§5), delta ingest, snapshot scheduler | End-to-end with a fake agent + fake Daytona: click Start → agent "runs" → snapshot created |
| **D. Panel** | `app/routers/dashboard.py`, `app/templates/*` | Host status, agent status, live console, port-forward status, Start/Stop, Backup | Dashboard renders all states; SSE streams agent logs |
| **E. Agent** | `agent/agent.py` | Java 21, `frpc` bootstrap, `FRP_*` config, artifact URLs pointing at the DayHost panel | Agent runs on a plain VM; provisions JDK21+Paper+frpc; connects to Phase-B WS server |
| **F. Retirement** | delete `controller/ssh.py`, `scripts/rdp_password.py`, `SSH_*`/`RDP_*`; keep the rest of `mc-orchestrator/` as reference or remove | No SSH/sshpass anywhere | `grep -ri sshpass` → 0; `grep -ri rdp_password` → 0 |

**Not in scope for any phase above** (explicitly deferred): `frps` unit authoring, Caddy vhost
authoring, DNS. Those are operator-side and belong to a separate run once the code path works.

---

## 8. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Daytona toolbox **auth scheme unverified** (still open from Fase 3) | **High** | Already mitigated structurally: `DAYTONA_TOOLBOX_AUTH=bearer\|none`, explicit 401/403 error, never a traceback. First real sandbox run will settle it. |
| `/process/exec` semantics (does it detach? stdout capture?) | Medium | Fallback to `/process/session/{id}/exec`; the agent writes `agent.log` itself, so we do not depend on catching stdout. |
| FRP binary download from GitHub inside the sandbox | Medium | Pin version + SHA-256 verify; multiple mirrors; fail loudly, never silently continue without a tunnel. |
| Sandbox has a **short idle timeout** and will be auto-stopped | Medium | Keepalive from the agent; surface `autoStopInterval` in the panel; recovery watchdog already exists. |
| 1 GB VPS running controller + `frps` + Caddy + caching JDK (~190 MB) and Paper (~50 MB) | Medium | Cache-once, prune aggressively, document the disk math, keep `SNAPSHOT_KEEP` low by default. |
| Two WS hubs (browser + agent) on one small box | Low | Both are `asyncio`; no threads; measure RSS in the gate. |

---

## 9. Blocking question for Gyoju

**Only one:** ratify **§3 Option A** (dedicated stdlib WS server, 0 new deps) versus
Option B (add `websockets` as an 8th package). My recommendation is A.

Everything else is decided above, with reasons, and Phase A can start immediately either way.
