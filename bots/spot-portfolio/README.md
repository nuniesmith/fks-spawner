# spot-portfolio

Multi-exchange **spot portfolio rebalancer** (NOT an edge): %-target baskets +
a cash reserve, rebalanced on drift and on new deposits across Kraken /
Crypto.com / KuCoin spot. Migrated from the `crypto` repo (`src/spot/*`); the
full operating doc is **[SPOT.md](SPOT.md)**.

Standalone crate (own `[workspace]` + `Cargo.lock`, like the other `bots/*`),
with the generic scaffolding (Discord alerts, JSONL journal, FKS `:9091`
status server) shared via the path dep `../../crates/crypto-bot-core`.

Two binaries:

| bin | what |
|-----|------|
| `spot-portfolio` | the rebalancer (dry-run unless the config says otherwise) |
| `spot-optimize` | read-only tuning sweep over real Kraken daily OHLC — no credentials |

## Build & run (host)

```bash
cargo build --release --bin spot-portfolio
cp spot-portfolio.example.toml spot-portfolio.toml   # then edit; keys go in .env
```

## Docker image (the spawner's `fks-bot-crypto-spot`)

The build context is **this repo's root** (`fks-spawner` — the crate path-deps
`crates/crypto-bot-core`):

```bash
docker build -f bots/spot-portfolio/Dockerfile -t fks-bot-crypto-spot:latest .
```

Same image contract as the old `crypto` repo's `--target spot` build: bakes
`spot-portfolio.toml` if the operator's (gitignored) copy sits in this
directory, else the example; **fails the build if `live = true` would bake**
(dry-run image by design — going live must be a deliberate, explicit
override); `SPOT_PORTFOLIO_CONFIG=/etc/crypto/spot-portfolio.toml`,
`BOT_STATUS_PORT=9091`, non-root, `ENTRYPOINT spot-portfolio`. Keys arrive as
env from the spawner's secret store — never baked.

> The futures/funding sibling (the trading edges) lives in the private
> `fks-state` repo (`bots/crypto-futures`) and git-deps `crypto-bot-core`
> from this repo.
