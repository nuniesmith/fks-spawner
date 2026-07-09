# spawner — Claude Code Project Instructions

> **Repo:** `github.com/nuniesmith/fks-spawner` — the bot-factory runtime
> (this lifecycle service + the `crypto-bot-core` SDK + the `bots/*` crates).
> **Path:** `fks-spawner/crates/spawner/` (moved here from `fks/crates/spawner/`).

## What this is

Rust HTTP service that creates, manages, and deletes Docker containers
on the fly. Designed for "spawn a bot from the WebUI, watch its logs
stream, see its run history in Postgres, let Prometheus discover it
automatically." Hybrid lib + bin crate so the supervisor logic is
testable.

## Stack

| | |
|--|--|
| Edition | Rust 2024 |
| HTTP | axum 0.8 |
| Docker SDK | bollard 0.19 |
| Async | Tokio |
| Persistence | sqlx + Postgres (optional `db` feature, default on) |
| Auth | `X-Internal-Token` middleware validated against `NGINX_INTERNAL_TOKEN` |
| Metrics | prometheus crate + file_sd_configs writer |

## Build & test

```bash
# Default (db) build
cargo check -p spawner
cargo build -p spawner

# Stateless mode (no Postgres)
cargo check -p spawner --no-default-features

# Unit + HTTP integration tests
cargo test -p spawner            # unit (incl. stats math) + HTTP integration tests
```

## API surface

| Method | Path | Auth | Notes |
|--------|------|:----:|-------|
| `GET` | `/health` | none | Docker healthcheck friendly |
| `GET` | `/metrics` | none | Prometheus scrapes here |
| `GET` | `/containers` | yes | Live list of `fks.bot=true` containers |
| `GET` | `/container/{id}` | yes | Inspect one |
| `POST` | `/spawn` | yes | Create + start a new bot |
| `DELETE` | `/container/{id}` | yes | Force-remove |
| `POST` | `/container/{id}/stop` | yes | 30s graceful stop |
| `POST` | `/container/{id}/restart` | yes | 10s graceful stop + start |
| `GET` | `/container/{id}/logs` | yes | SSE stream |
| `GET` | `/runs` | yes (db only) | Recent `bot_runs` history |
| `GET` | `/net-worth` | yes (db only) | Recent `net_worth_snapshots` (`?bot_id=` filter, `?limit=` default 500 / cap 5000); `[{bot_id, ts, net_worth, currency, venue}]` oldest→newest |
| `POST` | `/secrets` | yes (db only) | Store exchange API credentials (never read back) |
| `GET` | `/secrets/status` | yes (db only) | Which exchanges have keys configured |
| `DELETE` | `/secrets/{exchange}` | yes (db only) | Remove one exchange's stored credentials (hard delete) |
| `POST` | `/notifications` | yes (db only) | Store/UPSERT a notification channel (Discord webhook — URL encrypted, never read back) |
| `GET` | `/notifications` | yes (db only) | List channels (name/kind/events — never the URL) |
| `DELETE` | `/notifications/{name}` | yes (db only) | Remove one notification channel (hard delete) |
| `POST` | `/notifications/{name}/test` | yes (db only) | Send a one-off "connected" probe to one channel; reports whether the webhook accepted it |
| `GET` `POST` | `/configs` | yes (db only) | List / save (UPSERT) reusable spawn configs |
| `DELETE` | `/configs/{name}` | yes (db only) | Soft-delete a saved config |
| `GET` `POST` | `/ui/layouts` | yes (db only) | List (names + updated_at) / save (UPSERT) named WebUI dock layouts |
| `GET` `DELETE` | `/ui/layouts/{name}` | yes (db only) | Fetch one full layout envelope / hard-delete it |

Auth = `X-Internal-Token: ${NGINX_INTERNAL_TOKEN}` set by nginx.
Empty token = dev passthrough.

## Code conventions

- **`DockerOps` trait** abstracts the Docker daemon. Handlers depend on `Arc<dyn DockerOps>`; production wires `DockerClient`, integration tests wire `MockDockerClient`.
- **Hybrid lib + bin crate.** `src/lib.rs` declares `pub mod` for everything; `src/main.rs` uses `spawner::*`. Lets `tests/integration.rs` exercise the real `axum::Router` via `tower::ServiceExt::oneshot`.
- **DB writes never block the response.** Every record is fired via `tokio::spawn` after the Docker call returns. Failures `warn!` and move on.
- **Constant-time token compare** in `src/auth.rs` so a byte mismatch doesn't leak via timing.
- **Routes use axum 0.8 `{id}` syntax**, not the old `:id`. The old syntax panics at startup.

## Safety guards on `/spawn`

- Image must start with `ALLOWED_IMAGE_PREFIX` (default `fks-bot-`).
- Max concurrent containers capped by `MAX_CONCURRENT_BOTS` (default 20).
- Every spawned container is forced onto `ALLOWED_NETWORK` (default `fks_network`).
- `cap_drop: ALL` + `security_opt: no-new-privileges:true` are unconditional.
- Every container gets `fks.bot=true`, `fks.bot_id=<uuid>`, `fks.mode=...` labels.
- **Request input is validated** before any Docker call: `bot_id`/`mode` must
  match the Docker name charset (`[A-Za-z0-9._-]`, ≤64/32 chars); `cpu_limit`
  and `memory_limit_mb` are bounded by `MAX_CPU_LIMIT` (default 8 cores) and
  `MAX_MEMORY_LIMIT_MB` (default 16384); `env`/`labels` are capped (100/50).
  Anything out of range → `400 Bad Request`. (`cmd`/`entrypoint` overrides are
  still accepted — restricting those is a separate, behaviour-changing decision.)

## Common workflows

### Spawn a bot from curl
```bash
curl -X POST http://localhost:8090/spawn \
  -H 'X-Internal-Token: <token>' \
  -H 'Content-Type: application/json' \
  -d '{"image":"fks-bot-example:latest","mode":"paper"}'
```

### Tail logs over SSE
```bash
curl -N http://localhost:8090/container/<id>/logs?tail=100 \
  -H 'X-Internal-Token: <token>'
```

### Add a new Docker daemon operation
1. Add the method to the `DockerOps` trait in `src/docker_client.rs`.
2. Implement on `DockerClient` (delegating to bollard).
3. Implement on `MockDockerClient` in `tests/integration.rs`.
4. Add an HTTP handler in `src/api.rs` (or extend an existing one).
5. Cover it with an integration test.

## Pre-split / pre-publish gotchas

- **Currently a binary crate.** Going to crates.io, decide whether to publish as `spawner-bin` (just a binary) or refactor so most of `lib.rs` is reusable (`spawner` library + thin `spawner-bin` for the binary).
- **Docker image tag `nuniesmith/fks:spawner`.** Will eventually move to `nuniesmith/spawner:latest` on Docker Hub.
- **bollard 0.19 migration is complete** — `src/docker_client.rs` uses the
  `bollard::query_parameters::*Options` API throughout; there is **no**
  `#![allow(deprecated)]` shim. Verified by the blocking `clippy -D warnings`
  gate (which denies the `deprecated` lint), so a regression would fail CI.
- **Postgres schema** lives in the **fks repo** at `src/sql/spawner/` ([github.com/nuniesmith/fks](https://github.com/nuniesmith/fks)) — the DB bootstrap is baked into the postgres image there, so the schema travels with the fks stack, not with this crate. Don't duplicate it here.

## Status

Hardened (auth + HTTP integration tests) and DB-backed in `ruby_db`:
- `bot_runs` history (`/runs`), `bot_configs` saved spawn templates
  (`GET`/`POST /configs`, `DELETE /configs/{name}`), and `exchange_secrets`
  credential storage (`POST /secrets`, `GET /secrets/status`) — all db-gated.
- `/containers` enriches running bots with live CPU% + memory from the Docker
  stats API (pure CPU%/mem math is unit-tested).
- **Notification sender** (`src/notifications.rs`): lifecycle events
  (`bot_spawned` / `bot_stopped` / `bot_removed` / `bot_error`) are dispatched
  to configured Discord webhook channels (URL decrypted via the `SecretsCipher`).
  Best-effort + off the critical path (each dispatch is `tokio::spawn`ed; webhook
  failures are logged + counted, never propagated), gated on `NOTIFY_ENABLED`
  (default true — opt-out). Channel `events=[]` is catch-all; a non-empty list
  filters by kind. `POST /notifications/{name}/test` sends a one-off probe.
  Webhook URLs are NEVER logged (channel name only).
- Wired into the WebUI `/bots` route; `fks-bot-example` / `crypto-demo` demo the
  spawn contract end-to-end.
