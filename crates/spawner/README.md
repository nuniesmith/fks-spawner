# spawner — FKS Bot Spawner

> Rust HTTP service that creates, manages, and deletes Docker containers on
> the fly so the WebUI can run **isolated, observable, ad-hoc workloads** —
> bots, model training, optimisation runs, tests — and stream their full
> session logs back to the browser.

| | |
|---|---|
| **Container** | `fks_bot_spawner` (image `nuniesmith/fks:spawner`) |
| **Port (host)** | `127.0.0.1:8090` |
| **Reverse proxy** | `https://<your-tailnet-host>.ts.net/api/bots/*` |
| **Status** | 0.1 — HTTP API + DB persistence + Prometheus SD complete |

---

## What it does

1. **Spawns labelled bot containers** from a whitelisted image prefix
   (`fks-bot-` by default), with CPU/memory caps and `no-new-privileges`
   security. Every container gets `fks.bot=true`, `fks.bot_id=<uuid>`,
   `fks.mode=<paper|live|backtest|optimise|train>` labels.
2. **Streams container logs over Server-Sent Events** at
   `GET /container/:id/logs` so the WebUI can `<EventSource>` a long-running
   training job and show output in real time.
3. **Records every spawn / stop / remove in `bot_runs`** (Postgres) so the
   WebUI can show run history, runtime, and exit reason — the table is
   defined in the fks repo's `src/sql/spawner/002_spawner.sql` with a trigger
   that computes `runtime_secs` automatically.
4. **Writes a Prometheus file_sd config** to `/prometheus-sd/bots.json` on
   every lifecycle event, so each bot's `:9091/metrics` is scraped without a
   Prometheus reload.
5. **Auto-prunes** exited/dead containers after a configurable threshold
   (default 5 minutes) so old runs don't accumulate.
6. **Re-fires each active edge's backtest weekly** (edge-decay detection,
   `EDGE_DECAY_ENABLED` — default off) through the same guarded trigger path
   as `POST /edges/{id}/backtest`, so the advisor's Sunday report always has
   a fresh run to compare against last week's.

---

## API

| Method | Path | Body / Query | Returns |
|---|---|---|---|
| `GET` | `/health` | — | `{status, running_bots, max_bots, ...}` |
| `GET` | `/metrics` | — | Prometheus text |
| `POST` | `/spawn` | `SpawnRequest` JSON | `SpawnResponse` (201) |
| `GET` | `/containers` | — | `{containers: [...], total, running}` |
| `GET` | `/container/:id` | — | `ContainerInfo` |
| `DELETE` | `/container/:id` | — | `ActionResponse` |
| `POST` | `/container/:id/stop` | — | `ActionResponse` |
| `POST` | `/container/:id/restart` | — | `ActionResponse` |
| `GET` | `/container/:id/logs` | `?tail=N` | SSE stream of `event: log` |
| `GET` | `/runs` *(db only)* | `?limit=N` | `{runs: [...], total, db_enabled}` |
| `POST` | `/secrets` *(db only)* | `{exchange, api_key, api_secret, api_passphrase?}` | Stores encrypted exchange creds (never read back) |
| `GET` | `/secrets/status` *(db only)* | — | Which exchanges have keys configured (booleans, no values) |
| `GET` / `POST` | `/configs` *(db only)* | `ConfigRequest` JSON on POST | List / UPSERT reusable spawn templates (image + env + `secrets`) |
| `DELETE` | `/configs/:name` *(db only)* | — | Soft-delete a saved config |

### `SpawnRequest`

```json
{
  "image": "fks-bot-arbitrage:latest",
  "bot_id": "my-bot",                  // optional — auto-generated UUID if omitted
  "mode": "paper",                      // paper | live | backtest | optimise | train
  "env": { "EXCHANGE": "kucoin" },
  "labels": { "team": "trading" },
  "cpu_limit": 0.5,                     // fractional cores; defaults to DEFAULT_CPU_LIMIT
  "memory_limit_mb": 256,               // defaults to DEFAULT_MEMORY_LIMIT_MB
  "cmd": ["/bin/bot", "--flag"],        // optional CMD override
  "entrypoint": ["/sbin/tini", "--"]    // optional ENTRYPOINT override
}
```

### Safety guards (returned as `400` / `429`)

- `image` must start with `ALLOWED_IMAGE_PREFIX` (defaults to `fks-bot-`).
- Refuses to spawn when `MAX_CONCURRENT_BOTS` are already **running** — only
  running containers occupy slots; exited/dead one-shots (e.g. finished
  backtests awaiting auto-prune) don't count toward the cap.
- Every container is forced onto `ALLOWED_NETWORK` (default `fks_network`).
- `cap_drop: ALL` and `security_opt: no-new-privileges:true` are applied
  unconditionally (in `docker_client.rs::spawn`).

---

## Configuration

All settings come from environment variables; defaults are baked into the
`Config::from_env()` constructor.

| Var | Default | Purpose |
|---|---|---|
| `SPAWNER_HOST` | `0.0.0.0` | Bind address |
| `SPAWNER_PORT` | `8090` | Bind port |
| `ALLOWED_IMAGE_PREFIX` | `fks-bot-` | Image whitelist prefix |
| `MAX_CONCURRENT_BOTS` | `20` | Hard cap on **running** bots (exited containers awaiting prune don't count) |
| `ALLOWED_NETWORK` | `fks_network` | Docker network to attach containers to |
| `DEFAULT_CPU_LIMIT` | `1.0` | Fractional cores per bot (override per-spawn) |
| `DEFAULT_MEMORY_LIMIT_MB` | `512` | Memory cap per bot (override per-spawn) |
| `DEFAULT_CPU_SHARES` | `1024` | Relative CPU weight |
| `PROMETHEUS_SD_PATH` | `/prometheus-sd/bots.json` | File_sd output path |
| `BOT_METRICS_PORT` | `9091` | Port each bot exposes `/metrics` on |
| `PRUNE_AFTER_SECS` | `300` | Stopped-container retention |
| `PRUNE_INTERVAL_SECS` | `60` | Auto-prune sweep interval |
| `SPAWNER_DATABASE_URL` / `DATABASE_URL` | *(empty)* | Postgres URL — empty = stateless mode |
| `BACKTEST_DB_URL` | *(empty)* | Postgres URL handed to backtest containers as their `BACKTEST_DB_URL` env. Point it at the **scoped `fks_backtest` role** (UPDATE on its own `backtest_runs` row only — created by the fks repo's SQL bootstrap) so an arbitrary-code backtest image can't read `exchange_secrets` or rewrite the treasury ledger. Empty = fall back to the spawner's own `database_url` (full `fks_user` privileges) with a loud warning at boot and per run. |
| `SPAWNER_SECRETS_KEY` | *(empty)* | 64 hex chars (32 bytes). Enables ChaCha20-Poly1305 encryption of `exchange_secrets` at rest (`enc:v1:` wire format). Empty = stored as legacy plaintext; invalid = secrets DB disabled (fail-safe, never plaintext fallback). |
| `NGINX_INTERNAL_TOKEN` | *(empty)* | Shared secret validated on every protected route. Empty = dev mode (auth disabled — announced with a loud boot warning). |
| `REQUIRE_INTERNAL_TOKEN` | `false` | Hardened posture: `true` makes an empty `NGINX_INTERNAL_TOKEN` a fatal misconfiguration — the spawner refuses to boot rather than serving the money-adjacent routes unauthenticated. Set it in any deployment where the port could be reachable without the nginx hop. |
| `EDGE_DECAY_ENABLED` | `false` | Weekly edge-backtest scheduler (edge-decay detection): re-fires every active edge with a `backtest_image` through the same guarded trigger path as `POST /edges/{id}/backtest`. Opt-in. |
| `EDGE_DECAY_WEEKDAY` | `0` | Weekly fire weekday, days-from-Sunday (0 = Sunday; clamped 0–6). |
| `EDGE_DECAY_HOUR_UTC` | `16` | Weekly fire hour, UTC (clamped 0–23). Default Sun 16:00 UTC lands ~6h before the advisor's Sun 18:00 ET report in either DST phase. |
| `EDGE_DECAY_MINUTE_UTC` | `0` | Weekly fire minute, UTC (clamped 0–59). |
| `EDGE_DECAY_INTERVAL_SECS` | *(unset)* | Testing override: switches the scheduler from the weekly wall-clock cadence to a fixed-interval loop. |
| `RUST_LOG` | `info,spawner=debug` | tracing-subscriber filter |

---

## Postgres persistence (`db` feature, on by default)

The crate has two feature configurations:

```bash
# Default — DB writes enabled
cargo build -p spawner

# Stateless — no sqlx, no Postgres writes
cargo build -p spawner --no-default-features
```

When `db` is enabled and `DATABASE_URL` is set, the spawner:

1. Connects with a 5-conn pool on startup. **Connection failure is
   non-fatal** — it logs a warning and runs stateless.
2. Probes for the `bot_runs` table. **Missing schema is non-fatal** — it
   logs a warning and skips writes.
3. Writes one row per spawn (`status='running'`).
4. Updates `status='stopped'` + `stopped_at=NOW()` on stop/remove. The
   `compute_bot_run_runtime` trigger fills `runtime_secs` automatically.
5. Exposes `GET /runs?limit=N` for the WebUI to render history.

All DB writes happen in `tokio::spawn` — they **never block** the HTTP
response on a slow Postgres. Failures are logged with `warn!`.

The schema lives in the **fks repo** at `src/sql/spawner/` and is baked into
the postgres image (`25-spawner.sql` et al. under
`/docker-entrypoint-initdb.d/`), so a fresh volume bootstraps it
automatically. To re-apply on an existing database (the scripts are
idempotent):

```bash
docker compose exec postgres \
  psql -U fks_user -d ruby_db -f /docker-entrypoint-initdb.d/25-spawner.sql
```

---

## Deployment

Already wired up in the repo — no further infra changes are required:

| Where | What |
|---|---|
| `docker-compose.yml` | `fks_bot_spawner` service, port `127.0.0.1:8090`, mounts `/var/run/docker.sock` and `prometheus_sd:/prometheus-sd` |
| `infrastructure/docker/services/spawner/Dockerfile` | Multi-stage Rust build (`workspace` target → `runtime`) |
| `infrastructure/config/nginx/conf.d/dev.conf` | `/api/bots/*` (rewritten) and `/api/spawner/*` (passthrough) routes to the service |
| `infrastructure/config/prometheus/prometheus.yml` | `fks-spawner` scrape job + `fks-bots` `file_sd_configs` |
| `src/sql/spawner/*.sql` | spawner schema (`bot_configs` + `bot_runs` in `002_spawner.sql`, plus secrets / notifications / net-worth / treasury / edge-factory / backtest-role), baked into the postgres image |

To bring it up:

```bash
docker compose up -d fks_bot_spawner
curl http://localhost:8090/health
# {"status":"ok","running_bots":0,"max_bots":20,...}
```

---

## Authentication

The spawner sits behind nginx, which forwards every proxied request with
`X-Internal-Token: ${NGINX_INTERNAL_TOKEN}`. When `NGINX_INTERNAL_TOKEN`
is non-empty the spawner validates that header on every **protected**
route and returns:

- `401` if the header is missing
- `403` if the header doesn't match

`/health` and `/metrics` are intentionally exempt so:

- The Docker `HEALTHCHECK` in `infrastructure/docker/services/spawner/Dockerfile`
  can hit the spawner directly inside the container.
- Prometheus can scrape `/metrics` over the `fks_network` Docker network.

Leave `NGINX_INTERNAL_TOKEN` empty for local dev to disable auth — the
disabled posture is announced with a **loud boot warning** (every protected
route, including the money-adjacent ones, is unauthenticated). Set
`REQUIRE_INTERNAL_TOKEN=true` to fail closed instead: an empty token then
refuses to boot (`auth::check_internal_auth_posture`).

Implementation: `src/auth.rs`. The token compare is constant-time so a
byte-by-byte timing leak isn't possible.

## Testing

```bash
# Unit + HTTP integration tests
cargo test -p spawner          # ~32 unit (incl. 7 secrets_crypto) + 20 integration

# Stateless-mode build
cargo check -p spawner --no-default-features

# Default (db) build
cargo check -p spawner
```

### `DockerOps` trait + `MockDockerClient`

`src/docker_client.rs` now exposes a `DockerOps` trait. Production wires
`Arc<DockerClient>` into `AppState.docker`; integration tests under
`tests/integration.rs` wire an `Arc<MockDockerClient>` that maintains an
in-memory `HashMap<id, ContainerInfo>` and runs the entire HTTP stack
in-process via `tower::ServiceExt::oneshot`.

The integration suite covers:

- Health + metrics reachable without auth, even when auth is enabled.
- Spawn rejects images outside the allowed prefix (400).
- Full spawn → list → remove round-trip with state assertions.
- Auth: missing token (401), wrong token (403), correct token (200),
  empty config token (no-op).

---

## Known limitations / future work

- **No log persistence**: the SSE log endpoint streams from the live
  Docker socket. When a container is pruned, its logs are gone. If we
  need durable logs, mount Loki/Promtail at the bot level (the existing
  Loki stack already collects all container logs by label).
