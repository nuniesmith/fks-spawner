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
| `POST` | `/notifications/{name}/test` | yes (db only) | Send a one-off "connected" probe to one channel; reports whether the webhook accepted it (also records a `test_sent`/`test_failed` delivery-ledger row) |
| `GET` | `/notifications/history` | yes (db only) | Delivery ledger (`notification_log`, fks 013): one row per webhook send ATTEMPT — real events + test probes — so "did the 3am crash page actually send?" is answerable in the WebUI. `?limit=` (default 100 / cap 1000) + `?event=` filter; response `{db_enabled, entries:[{ts,event,bot_id,channel_name,kind,outcome,status_code,detail}]}` newest-first; graceful `db_enabled:false` without a DB. NEVER returns a webhook URL (the table doesn't hold one) |
| `POST` | `/events` | **dual** (db only) | Generic event INGEST for non-spawner emitters (bots raise `risk_halt`, the advisor `edge_decay`) so their alerts flow through the SAME channel store, filters, and delivery ledger instead of a parallel per-bot Discord path. Body `{event, bot_id?, mode?, detail?}`; `event` MUST be on the server-side allowlist (`risk_halt`, `edge_decay`) — arbitrary strings can NOT mint wire kinds; `detail` capped at 512 + `source=ingest`-marked. 202 accepted · 400 unknown/non-ingestable kind · 401 missing token · 403 wrong token. Best-effort off-path dispatch. **DUAL-AUTH (plan-03 D2):** this is the ONE route that accepts EITHER the internal token OR the scoped `EVENTS_TOKEN` in `X-Internal-Token`; every OTHER route is internal-token-only, so a bot holding only the scoped token opens ONLY this mailbox (blast-radius pin). FAIL-CLOSED: empty `EVENTS_TOKEN` disables the scoped path (internal token only). See the auth note below |
| `GET` `POST` | `/configs` | yes (db only) | List / save (UPSERT) reusable spawn configs (optional self-contained `bot_id`, stored in the `config_json` blob) |
| `DELETE` | `/configs/{name}` | yes (db only) | Soft-delete a saved config |
| `POST` | `/configs/{name}/respawn` | yes (db only) | Atomically redeploy a saved config's bot: stop→force-remove the existing `fks-bot-{bot_id}` container (idempotent — skips cleanly if it isn't running) THEN spawn a fresh one through the SAME `/spawn` path, so CURRENT stored secrets are re-injected (rotated keys picked up) and the config's `:latest` image runs (freshly-built code). Body `{ bot_id? }` overrides the config's stored id; bot_id resolves override > config > 400. The remove is awaited BEFORE the spawn (never two live containers for one bot_id); a residual 409 name-conflict is a clear error, not a half-state. Returns `{ bot_id, old_container_id, new_container_id, status, image }`. NOTE: recreates from the current image — it does NOT rebuild the image from source (see follow-up) |
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

**Scoped `EVENTS_TOKEN` (plan-03 D2, bot→spawner ingest).** `POST /events` is the
ONE route with a widened auth: it accepts EITHER `NGINX_INTERNAL_TOKEN` OR the
scoped `EVENTS_TOKEN` in the same `X-Internal-Token` header
(`auth::require_events_or_internal_token`, constant-time). Every OTHER route
stays internal-token-only, so a bot handed only the scoped token can open ONLY
the events mailbox — never `/spawn`, `/secrets`, `/transfers`, … (the
blast-radius property, pinned by the `scoped_events_token_opens_only_the_events_route`
integration test). **FAIL-CLOSED:** `EVENTS_TOKEN` unset/empty (the default)
DISABLES the scoped path entirely — only the internal token opens `/events`; an
unset token is never an open door. The token value is NEVER logged. `EVENTS_TOKEN`
also gates spawn-env injection: when non-empty, every spawned bot gets
`SPAWNER_EVENTS_URL` (default `http://fks_bot_spawner:8090/events`, override
`SPAWNER_EVENTS_URL`) + `SPAWNER_EVENTS_TOKEN` in its env so it can raise
`risk_halt` through the ingest; empty ⇒ NOTHING is injected (additive, zero
behaviour change until the operator sets it). Precedence: an operator-provided
value already in the stored config's env WINS (`or_insert`, same as
`inject_secrets`). The compose passthrough `EVENTS_TOKEN=${EVENTS_TOKEN:-}` on the
`fks_bot_spawner` service is a separate one-line fks PR (schema/compose live in
the fks repo).

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

## Bot-status contract & the net-worth truth guards

The net-worth sampler (`src/net_worth.rs`) polls each running bot's `/status`
JSON (the `crypto-bot-core::status` HTTP server — see that crate's
`src/status.rs`) and writes what it parses straight into the durable
`net_worth_snapshots` treasury history. Because that table is the source of
truth for real money, `sampler::probe()` runs every reading through a chain
of REFUSAL guards before it is ever recorded — a guard skips the poll (log a
gap, try again next tick) rather than let a plausible-looking-but-wrong figure
through. As of this writing there are five (`metrics::refusal`: `stale`,
`incomplete`, `fake_paper`, `unaccounted`, `not_ready`); each has its own
Prometheus counter reason so `NetWorthSamplingPausedTooLong` never asserts a
cause it can't back up. **A new guard extends this chain — it does not get
added as a parallel check elsewhere.**

- **`expected_venues`.** A bot declares this once, at `crypto_bot_core::status::init(bot,
  market, expected_venues)` — a static count of how many venues it is
  configured with (e.g. `spot-portfolio` passes `cfg.exchanges.len()`), fixed
  for the process lifetime. `net_worth.rs::venue_set_is_complete` compares it
  against the live venue array length to catch a PARTIAL sum (a venue that
  never checked in looks perfectly fresh otherwise). Field absent → `None`
  ("unknown", not "incomplete") so a bot that predates the field is still
  recorded — unverifiable is not the same as untrustworthy (see the #35/#38
  history in `net_worth.rs`). This is why `expected_venues` alone doesn't
  close every gap: a bot that never publishes it (the deployed funding bot
  does not) gets `None` from this guard for its entire life.

- **Invariant: an empty per-venue array is NEVER a valid "checked, zero
  venues" reading.** `crypto-bot-core`'s `/status` publishes the
  `exchanges`/`venues` array key from the moment its HTTP server binds — an
  uninitialised `BTreeMap` serialises to `[]`, not an absent key — so a bot
  whose engine hasn't completed a single venue cycle yet (a respawn's
  startup window, observed to take several real seconds) serves a body that
  is byte-identical to "I checked every venue and have nothing to report":
  `net_worth_usd` sums the empty set to `0.0`, nothing is stale (there's
  nothing to be stale), nothing is paper, and a bot without
  `expected_venues` reads as unknown-not-incomplete. **Every other guard
  waves this through** — that's exactly what let it corrupt the treasury
  series in production (confirmed live 2026-08-13). `net_worth.rs::venue_entries`
  therefore returns `Option<Vec<VenueFreshness>>`, not a bare `Vec`: `None`
  means the bot's `/status` shape has no per-venue breakdown at all (nothing
  to check — a different, unaffected case), `Some(vec)` means the key IS
  present and must be inspected — and `Some(vec![])` is refused
  unconditionally by `venues_not_yet_populated`, because no real-money bot
  legitimately has zero configured venues. **Any future bot-status field
  that is "empty vs. not-yet-populated" ambiguous must preserve this
  distinction — prefer an explicit absent-key/empty-array split (or a
  dedicated readiness flag) over a bare `Vec`/default-empty value that a
  downstream reader cannot tell apart from "not ready".**

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

Hardened (auth + HTTP integration tests) and DB-backed in `fks_db`:
- `bot_runs` history (`/runs`), `bot_configs` saved spawn templates
  (`GET`/`POST /configs`, `DELETE /configs/{name}`), and `exchange_secrets`
  credential storage (`POST /secrets`, `GET /secrets/status`) — all db-gated.
- `/containers` enriches running bots with live CPU% + memory from the Docker
  stats API (pure CPU%/mem math is unit-tested).
- **Notification sender** (`src/notifications.rs`): lifecycle + platform events
  are dispatched to configured Discord webhook channels (URL decrypted via the
  `SecretsCipher`). Wire kinds (`ALL_EVENT_KINDS`, a FROZEN contract shared with
  the WebUI channel filters): `bot_spawned` / `bot_stopped` / `bot_removed` /
  `bot_error` / `bot_crashed` / `bot_restarted` / `live_flip` / `key_rotation` /
  `net_worth_milestone` / `risk_halt` / `edge_decay`. `ALWAYS_DELIVERED_KINDS`
  (`bot_crashed`, `bot_restarted`, `live_flip`, `risk_halt`) bypass every scoped
  filter — page-worthy events a stale allowlist must never silently drop.
  Best-effort + off the critical path (each dispatch is `tokio::spawn`ed; webhook
  failures are logged + counted, never propagated), gated on `NOTIFY_ENABLED`
  (default true — opt-out). Channel `events=[]` is catch-all; a non-empty list
  filters by kind. `POST /notifications/{name}/test` sends a one-off probe.
  Webhook URLs are NEVER logged (channel name only). Emission points: `live_flip`
  = every live-mode `spawn_bot`; `key_rotation` = `secrets_handler` upsert;
  `bot_restarted` = supervisor `maybe_restart` Ok arm; `net_worth_milestone` =
  net-worth sampler tick (pure crossing detector, env `NET_WORTH_MILESTONE_STEP`,
  default 0 = OFF, in-memory anchor re-baselines on restart); `risk_halt` /
  `edge_decay` = `POST /events` ingest.
- **Delivery ledger** (`notification_log`, fks 013): the dispatcher writes ONE
  best-effort, DETACHED row per send attempt (`sent` / `http_error` /
  `send_failed` / `decrypt_failed`; test probes `test_sent` / `test_failed`) — a
  ledger write can never delay or fail a webhook send. `detail` is a ≤512-char
  event snippet, NEVER the URL. Read via `GET /notifications/history`; retention
  is a `prune_notification_log(keep=5000)` sweep piggybacked on the net-worth
  sampler tick (not a SQL cron job).
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
- **Boot-time bot reconciliation** (`src/boot_reconcile.rs`): closes the gap
  found live 2026-08-13/14 — a host reboot brings every infra container back
  via Docker's restart policy, but the spawner itself comes back tracking
  ZERO bots, and live-money bot containers (spawned with no restart policy)
  don't survive the reboot at all; nothing else re-read what was running and
  brought it back, so a human had to notice and respawn both by hand. At
  startup (awaited, right after the Postgres connect attempt and before the
  HTTP listener starts), every active saved config with a `bot_id` has its
  latest `bot_runs` row looked up **by container name** (`fks-bot-{bot_id}`
  — the container may be gone entirely, so there is nothing left to
  `inspect` by id). A row left OPEN (`running`/`spawning` — the exact signal
  `supervisor::run_is_open` uses to tell a crash from a clean stop) means the
  bot was never cleanly stopped via the API. Docker is asked for ground truth
  BEFORE respawning anything: a bot already RUNNING is left untouched (so a
  routine spawner-only redeploy, with the bots up the whole time, never
  bounces them); a clean stop (`stopped`/`pruned`) is left down; an `error`
  row (already handled by the crash-supervisor, restarted or deliberately
  left down per that config's own `restart_policy`) is not second-guessed.
  Respawns go through the SAME `respawn_from_config` path
  `POST /configs/{name}/respawn` uses. DB-only, degrades to a logged no-op
  without Postgres (never fails spawner boot); opt-out via
  `BOOT_RECONCILE_ENABLED=false` (default on). Each respawn fires the
  always-delivered `bot_restarted` notification with a boot-reconciliation
  detail string, and increments `fks_spawner_boot_reconcile_respawns_total`.
  The pure decision logic (`decide`) is unit-tested against the idempotency
  (already-running), deliberate-decision (clean stop / already-errored), and
  never-spawned cases. Does **not** eliminate the funding-bot
  paper-journal-loss issue (container-local, no volume — separate, already
  known) since a reconciled respawn still starts the bot fresh; it just does
  so automatically instead of requiring a human to notice.
- Wired into the WebUI `/bots` route; `fks-bot-example` / `crypto-demo` demo the
  spawn contract end-to-end.
