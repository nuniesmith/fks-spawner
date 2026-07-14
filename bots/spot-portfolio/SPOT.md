# Spot Portfolio Bot

Multi-exchange spot HODL: %-target baskets + a stablecoin/fiat **cash reserve**,
rebalanced on drift and on new deposits. Venues: **Kraken**, **Crypto.com**, and
**KuCoin spot** — all behind the `SpotExchange` trait. (Bybit is excluded: not
available in Canada.) Lives at `fks-spawner/bots/spot-portfolio` as its own
crate; the KuCoin **futures** dip bot it used to share a repo with now lives in
the private `fks-state` repo (`bots/crypto-futures`). One KuCoin key can still
drive both — keys are account-wide — but the env names stay distinct.

## Build
```
cargo build --release --bin spot-portfolio
```
Binary: `target/release/spot-portfolio`.

> `exchange-apiws` comes from **crates.io (`0.9.0`)** — the old `fa35543` git
> pin's Crypto.com `get-tickers` + v1 user-balance fixes shipped in the
> published release — so this builds reproducibly (CI / Docker) with no sibling
> checkout. `Cargo.lock` pins the exact version.

## Configure
```
cp spot-portfolio.example.toml spot-portfolio.toml   # then edit
```
Each `[[exchange]]` block: assets + weights, `cash` currency, `reserve_pct`,
`band`, `cooldown_secs`, `min_trade_usd`, `deposit_trigger_usd`. Weights are
normalized; the un-targeted remainder is the cash reserve.

Keys are **not** in the config — put them in `.env` (same dir):
```
KRAKEN_API_KEY=...      KRAKEN_API_SECRET=...           # trade + query, NOT withdraw
CRYPTOCOM_API_KEY=...   CRYPTOCOM_API_SECRET=...
KUCOIN_API_KEY=...      KUCOIN_API_SECRET=...   KUCOIN_API_PASSPHRASE=...
```
KuCoin spot keys are optional if the futures bot's `KC_KEY`/`KC_SECRET`/
`KC_PASSPHRASE` already grant spot permission — the spot adapter falls back to
them. A KuCoin key is account-wide, so one key can drive both spot and futures.

## Modes (decided per venue, automatically)
- **paper** — no usable keys: simulated book, real prices, no orders.
- **dry-run** — keys valid but `live = false`: reads your REAL balances, logs the
  would-be trades, places nothing.
- **live** — keys valid + `live = true`: real market orders.

## Going live (recommended order)
1. `live = false`, keys in `.env` → run; watch it read your real balances and log
   the rebalance plan for a while.
2. Set `live = true` with **small** baskets; watch ONE rebalance execute and
   verify the fills (Kraken fills are confirmed from closed-orders).
3. Run unattended as a spawner-managed container (below).

## Run as a container (the platform way)
The systemd units are retired — the platform runs the bot as a spawner-managed
`fks-bot-*` container. Build from **this repo's root** (`fks-spawner` — the
crate path-deps `crates/crypto-bot-core`):
```
docker build -f bots/spot-portfolio/Dockerfile -t fks-bot-crypto-spot:latest .
```
Then spawn it from the WebUI `/bots` (or the spawner API) with `secrets: [...]`
injection — the image bakes a **dry-run** config by design and keys arrive as
env from the encrypted secret store. See [README.md](README.md).

## Status / metrics
The bot serves `GET /health`, `/metrics` (Prometheus), and `/status` (JSON:
per-exchange balances, holdings, drift, recent trades, and the all-venue net
worth) on `BOT_STATUS_PORT` (default 9091). The server ships from
`crates/crypto-bot-core` (`status` module); the platform-wide bot contract is
documented in the fks repo's `docs/architecture/PLATFORM_ARCHITECTURE.md` §5.1.

```
curl -s localhost:9091/status | jq '{net_worth_usd, exchanges: [.exchanges[] | {exchange, mode, total_value}]}'
```

## Status / known gaps (confidence tiers)
- **Kraken** — *production path complete*: real fill verification (closed-orders
  by txid) + deposit detection + per-pair lot-precision rounding. Verify with a
  small live rebalance before trusting it unattended.
- **KuCoin spot** — genuine spot (no margin/leverage); quote-denominated (`funds`)
  market buys and **real fill verification** (`/api/v1/orders/{id}` →
  `dealSize`/`dealFunds`). But it's driven by raw JSON and is **untested against a
  live account**; reads the `trade` account only. Confirm with a tiny live trade.
- **Crypto.com** — prices work; balances/orders are parsed defensively but
  **untested against a live account**, and fills are *estimated* (no order
  read-back). Confirm with a tiny live trade first.
- **Order precision** — all three venues round order quantities to the venue's
  real per-pair precision: Kraken `lot_decimals`, KuCoin `baseIncrement`/
  `quoteIncrement` (and it enforces `baseMinSize`/`quoteMinSize`), Crypto.com
  `quantity_decimals`. Quantities are floored (never rounded up) so a rounded qty
  can't exceed the balance. `min_trade_usd` still gates dust below a USD floor.
- **Alerts / journal** — set `alert_webhook` (Discord) and `journal` (JSONL path)
  in the config to get the same notifications/records as the futures bot.
