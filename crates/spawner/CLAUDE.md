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
| `GET` `POST` | `/net-worth` | yes (db only) | GET: recent `net_worth_snapshots` (`?bot_id=` filter, `?limit=` default 500 / cap 5000 — applied PER account via a `PARTITION BY bot_id` window, so one busy sampler can never evict another account from the /treasury roll-up); `[{bot_id, ts, net_worth, currency, venue}]` oldest→newest. POST: record ONE hand-entered snapshot `{account_id, net_worth, currency?='USD', venue?}` with `source='manual'` (validates finite value + non-empty account_id; awaited write, 201 on success, honest 503 without a DB) — how prop-payout / bank balances get entered until their own node exists |
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
| `GET` `POST` | `/transfers` | yes (db only) | Treasury cash-flow ledger: list (`?account_id=` filter, `?limit=` default 500 / cap 5000; oldest→newest like /net-worth) / append one signed row (positive = deposit in, negative = withdrawal out; kind: deposit / withdrawal / payout / sweep; source: manual / bot_detected; optional backfill `ts`) |
| `GET` `POST` | `/accounts` | yes (db only) | Account registry: list (active first) / save (UPSERT by `account_id`; tier 0–3, role + compliance_flag allowlists; carries NO credentials — keys stay in /secrets) |
| `DELETE` | `/accounts/{id}` | yes (db only) | Soft-delete an account (`active=false`; its transfers/net-worth history is preserved) |
| `GET` | `/profit` | yes (db only) | Decompose one account's net-worth drift into deposits vs trading profit (`?account_id=` required, `?since=` RFC3339): first/last snapshot in range bound the window; `profit = (end − start net worth) − net inflows` from net_worth_snapshots + transfers |
| `GET` `POST` | `/edges` | yes (db only) | Edge registry (the edge portfolio's source of truth): list (active first) / save (UPSERT by `edge_id`; edge_type `adaptive`\|`rule` + status `research`\|`paper`\|`live`\|`retired` allowlists; `asset_scope` JSON symbol array, `[]` = all assets; `backtest_image` = the fks-bot-* image that runs the edge's backtest, NULL = not containerized) |
| `DELETE` | `/edges/{id}` | yes (db only) | Soft-delete an edge (`active=false`; its backtest_runs history is preserved) |
| `GET` | `/edges/{id}/backtests` | yes (db only) | Recent backtest runs (newest first, `?limit=` default 50 / cap 500) with their container-written `results` JSON |
| `POST` | `/edges/{id}/backtest` | yes (db only) | Invoke one backtest: body `{params?: object}`; pre-checks the concurrency cap (429 BEFORE any ledger write), opens a `backtest_runs` row (status `running`), then spawns the edge's `backtest_image` through the SAME spawn path as `/spawn` (prefix guard, forced network, caps) with env `BACKTEST_RUN_ID`/`BACKTEST_EDGE_ID`/`BACKTEST_PARAMS`/`BACKTEST_DB_URL` (the scoped low-privilege `BACKTEST_DB_URL` env var when set; falls back to the spawner's own full-privilege URL with a loud warning) — the one-shot container writes its own results row and exits. 202 `{run_id, container_id}`; 400 on unknown edge / NULL image; stale runs (>2h unreported) are swept to `failed` by the net-worth sampler tick |

Auth = `X-Internal-Token: ${NGINX_INTERNAL_TOKEN}` set by nginx.
Empty token = dev passthrough, announced LOUDLY at boot
(`auth::check_internal_auth_posture`); set `REQUIRE_INTERNAL_TOKEN=true` to
fail closed instead (refuse to boot with an empty token).

## Code conventions

- **`DockerOps` trait** abstracts the Docker daemon. Handlers depend on `Arc<dyn DockerOps>`; production wires `DockerClient`, integration tests wire `MockDockerClient`.
- **Hybrid lib + bin crate.** `src/lib.rs` declares `pub mod` for everything; `src/main.rs` uses `spawner::*`. Lets `tests/integration.rs` exercise the real `axum::Router` via `tower::ServiceExt::oneshot`.
- **DB writes never block the response.** Every record is fired via `tokio::spawn` after the Docker call returns. Failures `warn!` and move on.
- **Constant-time token compare** in `src/auth.rs` so a byte mismatch doesn't leak via timing.
- **Routes use axum 0.8 `{id}` syntax**, not the old `:id`. The old syntax panics at startup.

## Safety guards on `/spawn`

- Image must start with `ALLOWED_IMAGE_PREFIX` (default `fks-bot-`).
- Max concurrent containers capped by `MAX_CONCURRENT_BOTS` (default 20) —
  only RUNNING containers occupy slots; exited/dead one-shots awaiting
  auto-prune (finished backtests) don't count.
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
- **Treasury layer** (`src/treasury.rs`; schema `007_treasury.sql` in the fks
  repo): the `transfers` signed cash-flow ledger + `accounts` topology registry
  (tiers: 0 cold-BTC backbone / 1 personal-crypto / 2 rithmic-main /
  3 prop-copy-target) + the `GET /profit` decomposition, so net-worth drift
  splits into deposits vs trading profit instead of later deposits showing up
  as PnL. Pure validation/arithmetic in `treasury.rs` is unit-tested; the
  handlers are db-gated with graceful no-DB degradation.
- **Read-only treasury nodes** (P0.6) — three DB-gated background/endpoint
  writers that all APPEND `net_worth_snapshots` rows (distinguished by the
  `source` column) and can NEVER move money by construction:
  - **Cold-BTC watcher** (`src/btc_watch.rs`, `source='onchain'`): derives
    BIP84 p2wpkh receive+change addresses from a public account xpub
    (`BTC_WATCH_XPUB`, gap `BTC_WATCH_GAP` default 20 — raise for a deep wallet)
    and/or reads `BTC_WATCH_ADDRESSES` (comma-separated). Sums confirmed balance
    via a public Esplora API (`ESPLORA_API_BASE`, default blockstream.info),
    prices BTC→USD off Kraken's public ticker, and writes ONE row per tick
    (`BTC_WATCH_INTERVAL_SECS` default 3600; account_id `BTC_WATCH_ACCOUNT_ID`
    default `btc-cold`, venue `cold-btc`). OFF unless an xpub/addresses are set.
    An xpub is public-key material — it can derive addresses but never sign. Any
    fetch/price failure skips the whole tick (never a partial/zero row).
  - **Rithmic balance sampler** (`src/rithmic_sampler.rs`, `source='rithmic'`):
    polls the read-only `rithmic-connector` `GET /positions`
    (`RITHMIC_SAMPLER_URL`, e.g. http://fks_rithmic_connector:9091;
    `RITHMIC_SAMPLE_INTERVAL_SECS` default 300) for
    `account_summary.account_balance`, writing rows account_id
    `rithmic:<id>`, venue `rithmic`. OFF unless the URL is set; the connector is
    usually down (gated on creds) → silent debug skip.
  - **Manual snapshot** (`POST /net-worth`, `source='manual'`): a hand-entered
    balance for accounts without a watcher yet.
  Pure parse/derive/validate logic in each module is unit-tested (incl. a BIP84
  xpub derivation test vector); the writers are best-effort and never fatal. The
  one new dep is `bitcoin` (bip32/address derivation only — no wallet/signing
  features).
- **Edge factory v1** (`src/edges.rs` pure validation/request-shaping; schema
  `008_edge_factory.sql` in the fks repo): the `edges` registry (the
  edge-portfolio's source of truth — janus-adaptive + operator rule-edges,
  every edge facing the same validation bar) + the `backtest_runs` ledger.
  `POST /edges/{id}/backtest` pre-checks the concurrency cap (429 before any
  ledger write), opens a run row and spawns the edge's registered
  `backtest_image` as a one-shot container via the SAME `DockerOps::spawn`
  path as `/spawn` (all guards apply); the container is handed
  `BACKTEST_RUN_ID`/`BACKTEST_EDGE_ID`/`BACKTEST_PARAMS`/`BACKTEST_DB_URL`
  and UPDATEs its own row (status + results + finished_at) before exiting.
  The `BACKTEST_DB_URL` it receives is the spawner's `BACKTEST_DB_URL` env
  var — a scoped, low-privilege `fks_backtest` role — falling back to the
  spawner's own full-privilege `database_url` with a loud warning (boot +
  per run) when unset, so a compromised backtest image can't read
  `exchange_secrets` or rewrite the treasury ledger once the var is set. No
  dedicated reaper in v1 — a 2h staleness sweep piggybacked on the net-worth
  sampler tick marks silently-dead runs failed.
- **Weekly edge-decay scheduler** (`src/edge_decay.rs`): on a weekly cadence,
  re-fires every ACTIVE edge with a `backtest_image` through the SAME internal
  trigger path as `POST /edges/{id}/backtest` (all spawn guards + the
  concurrency cap apply; a cap-reached fire stops the sweep — the rest retry
  next week), so the advisor's Sunday report always has a fresh run to compare
  against last week's (the drift comparison itself lives in fks-state's
  advisor). OFF unless `EDGE_DECAY_ENABLED=true`; fire time
  `EDGE_DECAY_WEEKDAY`/`EDGE_DECAY_HOUR_UTC`/`EDGE_DECAY_MINUTE_UTC` (default
  Sun 16:00 UTC — ~6h before the advisor's Sun 18:00 ET report in either DST
  phase); `EDGE_DECAY_INTERVAL_SECS` switches to a fixed-interval loop for
  testing. Schedule math + edge selection are pure and unit-tested; the loop is
  `db`-gated like the rest of the persistence layer.
- Wired into the WebUI `/bots` route; `fks-bot-example` / `crypto-demo` demo the
  spawn contract end-to-end.
