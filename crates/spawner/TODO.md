# spawner — TODO

> **Repo:** `github.com/nuniesmith/fks-spawner` (crate: `crates/spawner/`)
> **Last synced:** 2026-07-13

## P0 — Pre-publish

- [ ] **Name conflict on crates.io.** `spawner` is almost certainly taken. Rename candidates: `fks-spawner`, `bot-spawner`, `docker-bot-spawner`. Decide before publish.
- [ ] **Cargo.toml metadata.** Missing fields: `license`, `repository`, `documentation`, `readme`, `keywords`, `categories`.
- [x] **`LICENSE` file at the crate root** — MIT, present at `crates/spawner/LICENSE`.
- [x] **Edition 2024** — bumped 2021 → 2024 (matches the repo-wide standard) and
      added `rust-version = "1.94.1"`. `cargo fix --edition` applied the only code
      change (RPIT `+ use<>` in `docker_client::stream_logs`); rustfmt re-styled
      imports to the 2024 edition; clippy `-D warnings` + tests stay green.
- [ ] **Decide whether to publish at all.** It's a binary-only Docker service; crates.io makes sense only if downstream users want it as a library. If not, drop the publish goal and ship via Docker Hub only.

## P1 — Feature work

- [ ] **`bot_configs` template UI** — the `bot_configs` table is part of the schema but unused. Add a preset library: save spawn-form values as a named row, then `POST /spawn?from_config=<name>` fills the rest.
- [ ] **Persistent log capture** — when a container is pruned, its logs disappear. Loki/Promtail already collects all container logs by label, so consider whether spawner needs its own capture or can just point at Loki for archived runs.
- [ ] **Mobile / narrow-screen polish** on `/bots` — current grid assumes desktop terminal layout.

## P1 — Test coverage

- [ ] **Postgres test fixture** — today the `db` feature is exercised only when `DATABASE_URL` is set. A `testcontainers`-backed integration test that exercises the real `BotRunStore` would catch SQL changes.

## P2 — Quality of life

- [ ] **Per-container resource limits in the UI** — today the spawn form has CPU and memory inputs. Add cgroup-pid-limits + disk-quota knobs when they matter for training jobs.
- [ ] **Container lifecycle events on the bus** — broadcast spawn/stop/restart events on Redis pub/sub so other services (e.g. Grafana alerting) can react.

## P3 — Future

- [ ] **Multi-host Docker** — today the spawner talks to one Docker daemon via the socket. For scaling, accept a `DOCKER_HOST` env var per spawner instance and route bot containers across multiple hosts.
- [ ] **Image build endpoint** — `POST /build` that builds a `fks-bot-*` image from a git URL + ref + path. Sketchy from a security standpoint; tabled until there's a clear use case.

---

## ✅ Recently shipped

- HTTP API + Docker SDK wrapper + Prometheus self-metrics + file_sd writer (initial PRs in `fks`).
- Postgres persistence via `BotRunStore` (PR #12).
- `/bots` WebUI route (PR #13).
- Build rot + Axum 0.8 path syntax + Bollard 0.19 cleanup (PRs #11, #14, #18).
- `X-Internal-Token` auth middleware + `DockerOps` trait + 10 HTTP integration tests (PR #18).
- `fks-bot-example` reference image demonstrating the `:9091/metrics` contract (PR #17).
- Auto-scroll on the `/bots` log viewer + `api.*` callsite fixes (PR #19).
- Promoted from `src/spawner/` to `crates/spawner/` as its own nested workspace (PR #21 cleanup + reorg).
- Restart / log-SSE / `/runs` integration tests — suite 10 → 13 (PR #57).
- Bollard 0.19 deprecation migration fully landed — `query_parameters::*OptionsBuilder`, no `#![allow(deprecated)]` shim (the P0 item was stale; verified complete).
- Root `Cargo.toml` workspace-members refreshed so `cargo check` from repo root works (PR #21).
- Per-workspace CI job in `.github/workflows/rust.yml` (PR #23) — spawner's job has been passing throughout the CI green-up arc.
- The "README polish — no fks path references" item from earlier was verified clean (zero `fks` references in `crates/spawner/README.md`).
- Treasury layer (`/transfers` + `/accounts` + `/profit`, PR #1) and read-only treasury nodes (cold-BTC watcher, rithmic sampler, manual snapshot, PR #2).
- Edge factory v1 — `/edges` registry + containerized backtest runs (PR #3).
- 2026-07-11 audit mediums: scoped `BACKTEST_DB_URL` (low-privilege `fks_backtest` role, loud degraded fallback), running-only `MAX_CONCURRENT_BOTS` cap, per-account `/net-worth` window (PR #4).
- Audit lows: net-PnL-based W/L in `crypto-bot-core` status + loud internal-auth posture (`REQUIRE_INTERNAL_TOKEN` fail-closed opt-in) (PR #5).
- Weekly edge-backtest scheduler (edge-decay detection, `EDGE_DECAY_*` env, default Sun 16:00 UTC, opt-in) (PR #6).
