# AGENTS.md — fks-spawner

The FKS bot factory: the spawner container-lifecycle service (`crates/spawner/` — axum HTTP service managing Docker bot containers: spawn / stop / logs / run history / secrets), the shared bot SDK (`crates/crypto-bot-core/` — Discord alerts, JSONL journal, `:9091` status server implementing the FKS bot contract), and the bot crates themselves. Split out of `fks`, which remains the orchestration root that deploys all of this.

## Shared context

Read the shared agent context first: `soul.md` and `memory.md` (especially §5) in the [agents repo](https://github.com/nuniesmith/agents). `crates/spawner/` has its own `CLAUDE.md` — read it when working there.

## Rules

- **Bots spawned from here trade real money** — the live Kraken crypto bot (`fks-bot-crypto-regime`) is a spawner-managed bot built on `crypto-bot-core`. Changes to the SDK or the spawner's lifecycle/secrets handling can affect it: dry-run first, Jordan's explicit OK before anything live changes state, and never run two bots against the same exchange account.
- Bot secrets are decrypted with `SPAWNER_SECRETS_KEY` on the host. Never print secret values anywhere; presence and length only.
- `bots/fks-bot-example/` is the canonical template for consuming the published `rustrade-framework` from crates.io — keep it building.
- Verify with `cargo test` / clippy and the repo CI (`rust.yml`) before calling anything done; a PR is ready only when its own CI is green on its own head.
