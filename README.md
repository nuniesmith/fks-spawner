# fks-spawner — the FKS bot factory

The **bot factory runtime** for the [FKS trading platform](https://github.com/nuniesmith/fks):
the container-lifecycle service that spawns bots, the shared bot SDK, and the
bot crates themselves. Split out of `fks` (which remains the operational /
orchestration root that deploys all of this).

| Path | What it is |
|------|------------|
| `crates/spawner/` | **spawner** — axum HTTP service managing Docker bot containers (spawn / stop / logs / run history / secrets). Own `CLAUDE.md` + `TODO.md` + `README.md`. |
| `crates/crypto-bot-core/` | **bot SDK** — shared, edge-free scaffolding (Discord alerts, JSONL journal, `:9091` status server implementing the FKS bot contract). |
| `bots/fks-bot-example/` | reference bot — canonical example of consuming the published `rustrade-framework` from crates.io; the template the spawner launches. |
| `bots/crypto-demo/` | working multi-symbol demo bot (paper by default) — exercises rustrade + indicators-ta + exchange-apiws end to end. |
| `bots/rustrade-exchange-apiws/` | `rustrade::ExchangeClient` adapters over `exchange-apiws` (KuCoin futures + Kraken spot) — the live order path library. |
| `bots/spot-portfolio/` | **production** multi-exchange spot rebalancer. Own `README.md` + `SPOT.md`. Path-depends on `../../crates/crypto-bot-core`. |

Every crate here is a **standalone Cargo workspace** (each has its own
`[workspace]` block + committed `Cargo.lock`) resolving crates.io / git deps —
there is deliberately no root `Cargo.toml`. The `crates/` + `bots/` layout
mirrors the original `fks` tree so `spot-portfolio`'s relative path dep keeps
working unchanged.

## Build & test

```bash
# spawner (db feature on by default; also builds --no-default-features)
cd crates/spawner              && cargo test --workspace --locked

# bot SDK
cd crates/crypto-bot-core      && cargo test --workspace --locked

# bots
cd bots/fks-bot-example        && cargo test --workspace --locked
cd bots/crypto-demo            && cargo test --workspace --locked
cd bots/rustrade-exchange-apiws && cargo test --workspace --locked
cd bots/spot-portfolio         && cargo test --workspace --locked
```

## Docker images

```bash
# spot-portfolio bot image — build context is THIS REPO ROOT (the crate
# path-deps ../../crates/crypto-bot-core, so both must be in the context):
docker build -f bots/spot-portfolio/Dockerfile -t fks-bot-crypto-spot:latest .
```

The image bakes the operator's tuned (gitignored) `spot-portfolio.toml` when it
sits next to the example in `bots/spot-portfolio/`; otherwise it bakes the
`.example.toml`. Either way the build **fails if `live = true`** would bake —
going live is an explicit runtime decision, never a default.

## Notes

- **Spawner's Postgres schema is NOT here.** It lives in the fks repo at
  `src/sql/spawner/` — the DB bootstrap is baked into the postgres image and
  travels with the fks stack.
- `bots/crypto-futures` (private, in `fks-state`) git-pins
  `rustrade-exchange-apiws` and `crypto-bot-core` **to this repo** by rev
  (bump the `rev` there deliberately when those crates change here).
- No secrets in the tree: bot configs are tracked as `*.example.toml` only;
  API keys arrive as env, injected by the spawner's encrypted secret store.
