# Daytona REST API — verified reference (Fase 2)

Everything below was extracted on **2026-10-23** from Daytona's **official API reference**
(<https://www.daytona.io/docs/en/tools/api/>, page version v0.225).
It replaces the earlier *assumed* endpoint table that Fase 1 shipped with.

## Ground truth

| Item | Value |
|---|---|
| Base URL | `https://app.daytona.io/api` |
| Auth scheme | HTTP **bearer** — header `Authorization: Bearer <API key>` |
| Optional header | `X-Daytona-Organization-ID` (only needed with JWT / org-scoped keys) |

### Endpoints

| Purpose | Method | Path | operationId | Notes |
|---|---|---|---|---|
| Get status | `GET` | `/sandbox/{sandboxIdOrName}` | `getSandbox` | optional query `verbose=bool`; returns `Sandbox` |
| Start | `POST` | `/sandbox/{sandboxIdOrName}/start` | `startSandbox` | also **resumes** from paused and **restores** from archived |
| Stop | `POST` | `/sandbox/{sandboxIdOrName}/stop` | `stopSandbox` | optional query `force=bool` → SIGKILL instead of SIGTERM |
| Pause | `POST` | `/sandbox/{sandboxIdOrName}/pause` | `pauseSandbox` | |
| Archive | `POST` | `/sandbox/{sandboxIdOrName}/archive` | `archiveSandbox` | |
| Recover | `POST` | `/sandbox/{sandboxIdOrName}/recover` | `recoverSandbox` | recover from `error` state |
| Last activity | `GET` | `/sandbox/{sandboxId}/last-activity` | | |

**There is no dedicated restart endpoint.** Restart = `stop` then `start`.

### Verdict on the Fase 1 assumptions

| Fase 1 assumption | Official API | Verdict |
|---|---|---|
| `GET /sandbox/{id}` | `GET /sandbox/{sandboxIdOrName}` | ✅ correct |
| `POST /sandbox/{id}/start` | `POST /sandbox/{sandboxIdOrName}/start` | ✅ correct |
| `POST /sandbox/{id}/stop` | `POST /sandbox/{sandboxIdOrName}/stop` | ✅ correct |
| `Authorization: Bearer <key>` | bearer, `Authorization: Bearer <API key>` | ✅ correct |
| base `https://app.daytona.io/api` | server url `https://app.daytona.io/api` | ✅ correct |
| state values | full enum below | ⚠️ **incomplete** — many real states were unmapped to `UNKNOWN` |

So the paths/auth were right, but the state map was materially incomplete: 22 real states
exist and Fase 1 only mapped a handful. That is the concrete bug this document fixes.

## `SandboxState` enum (22 values, verbatim)

```
creating, restoring, destroyed, destroying, started, stopped, starting, stopping,
error, build_failed, pending_build, building_snapshot, unknown, pulling_snapshot,
archived, archiving, resizing, snapshotting, forking, pausing, paused, resuming
```

## `Sandbox` object fields worth surfacing in the UI

`state`, `desiredState`, `errorReason`, `recoverable`, `snapshot`, `user`, `createdAt`, `id`, `name`

- When `state == "error"`, surface **`errorReason`** — it is far more useful than a generic failure.
- `recoverable` tells you whether the `POST .../recover` call is worth offering.
- `desiredState` explains *why* a sandbox is mid-transition (e.g. `destroyed` while `state` is `stopping`).

## Recommended DayHost state mapping

| Daytona state | DayHost canonical | UI meaning |
|---|---|---|
| `started` | `RUNNING` | Online |
| `starting`, `restoring`, `resuming`, `pulling_snapshot`, `creating` | `STARTING` | Booting |
| `stopped` | `STOPPED` | Offline |
| `stopping`, `destroying`, `pausing`, `paused` | `STOPPING` | Shutting down |
| `archived`, `archiving` | `ARCHIVED` | Archived |
| `error`, `build_failed` | `ERROR` | Faulted (show `errorReason`) |
| `resizing`, `snapshotting`, `forking`, `pending_build`, `building_snapshot`, `destroyed`, `unknown` | `BUSY` / `UNKNOWN` | Transient / unrecognised |

Any value not in this table must still degrade to `UNKNOWN` **without raising** — that
defensive rule from Fase 1 stays.
