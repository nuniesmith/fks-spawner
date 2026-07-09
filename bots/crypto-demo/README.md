# crypto-demo

A **working rustrade bot** over crypto pairs that exercises the whole published
FKS stack together, paper-trading over time. It's the answer to "give me a
running setup like the kucoin repo to leave running and confirm everything
works with rustrade, janus, and the crates."

```text
  exchange-apiws (KuCoin Futures klines)      ← market data (no API key needed)
       │  CandleSource::poll
       ▼
  rustrade CandlePollerService ──► MarketDataBus ──► EmaCrossBrain
       (one supervised task per symbol)               (indicators-ta: EMA + ATR)
                                                         │ Decision (Buy/Sell + ATR stop)
                                                         ▼
  rustrade ExecutionService ── risk gate ──► MockExchange (paper)
       (sizing • session PnL • circuit breaker)
                                                         │ signals
                                                         ▼
  paper PnL tracker ──► fks_bot_* metrics  +  handle.record_trade_outcome()
                          (:9091/metrics)        (feeds SessionPnl + breaker)
```

| Layer | Crate | What it does here |
|-------|-------|-------------------|
| Framework | `rustrade-framework` 0.2 | `Bot` + `Supervisor` + `ExecutionService` + risk (sizing, session PnL, circuit breaker) |
| TA math | `indicators-ta` 0.1 | incremental `EMA` (fast/slow) + `ATR` driving the cross signals |
| Exchange I/O | `exchange-apiws` 0.1 | KuCoin Futures kline polling (public market data) |

> **PAPER mode only.** Trades hit a `MockExchange` — no real orders, no
> credentials required. Safe to leave running for days. It's the template for
> real bots (and the future private `strategies/`): swap `MockExchange` for a
> real `ExchangeClient` adapter and `EmaCrossBrain` for your strategy (or a
> `rustrade::Brain` that calls janus's brain API).

## Run

```bash
# Live KuCoin data, paper trades, default pairs (XBT/ETH/SOL):
cargo run -p crypto-demo

# Pick pairs / cadence:
DEMO_SYMBOLS=XBTUSDTM,ETHUSDTM DEMO_POLL_SECS=30 cargo run -p crypto-demo

# Fully offline (synthetic random-walk data — no network, good for CI / demos):
DEMO_SOURCE=synthetic cargo run -p crypto-demo

# Let JANUS make the decisions (the janus ↔ rustrade tie-in):
# (forward service: :8180 native janus; :7001 from the host against the compose stack)
DEMO_BRAIN=janus JANUS_HTTP_URL=http://localhost:8180 cargo run -p crypto-demo
```

## Two brains (`DEMO_BRAIN`)

The same wiring runs with either decision-maker:

| `DEMO_BRAIN` | Who decides | How |
|--------------|-------------|-----|
| `ema-cross` *(default)* | the demo itself | local `indicators-ta` EMA cross + ATR stop |
| `janus` | **janus** | computes EMA/ATR features → `POST {JANUS_HTTP_URL}/api/v1/signals/generate` → maps janus's `Buy`/`Sell`/`Hold` (+ confidence, stop, take-profit) onto a rustrade `Decision` |

`JanusBrain` is the realisation of "janus for the trading brain": only the
`Brain` swaps — candle pollers, supervisor, risk gate, paper exchange, and the
`fks_bot_*` metrics are identical. It's resilient: if janus is unreachable or
returns no signal it **holds** (never crashes), so a long run survives janus
restarts. Point `JANUS_HTTP_URL` at the janus **forward** service (default
`http://localhost:8180`, janus's native forward REST port); inside the FKS
compose network that's `http://fks_janus:8180`, and from the host against the
compose stack it's `http://localhost:7001`. (`:8080` / `fks_janus:8080` is the
janus *api* service — signal generation 404s there.)

Then watch it work:

```bash
curl -s localhost:9091/metrics | grep fks_bot_
#   fks_bot_signals_total   <n>
#   fks_bot_trades_total    <n>
#   fks_bot_pnl_dollars     <±>
#   fks_bot_win_rate        0..1
#   fks_bot_uptime_seconds  <n>
curl -s localhost:9091/health      # → ok
```

## Configuration (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `DEMO_SOURCE` | `kucoin` | `kucoin` (live) or `synthetic` (offline) |
| `DEMO_BRAIN` | `ema-cross` | `ema-cross` (local) or `janus` (delegate to janus) |
| `JANUS_HTTP_URL` | `http://localhost:8180` | janus **forward** service (when `DEMO_BRAIN=janus`) — `fks_janus:8180` in-network, `localhost:7001` from the host |
| `DEMO_SYMBOLS` | `XBTUSDTM,ETHUSDTM,SOLUSDTM` | comma-separated pairs (KuCoin Futures symbols) |
| `DEMO_POLL_SECS` | `60` | how often to poll for new candles |
| `DEMO_CANDLE_SECS` | `60` | candle interval (maps to KuCoin granularity) |
| `DEMO_WARMUP_CANDLES` | `100` | history bars fetched to warm the indicators |
| `BOT_METRICS_PORT` | `9091` | Prometheus `/metrics` + `/health` port |
| `FKS_BOT_ID` | `crypto-demo` | identity label (the spawner injects this) |
| `RUST_LOG` | `info,crypto_demo=info` | tracing filter |

## Docker / spawner

Builds and runs exactly like `fks-bot-example`. The image is tagged
`fks-bot-crypto-demo` so it matches the spawner's `ALLOWED_IMAGE_PREFIX`
(default `fks-bot-`):

```bash
# Build both bot images from the repo root:
./run.sh build-bots
# …or just this one:
docker build -f infrastructure/docker/services/crypto-demo/Dockerfile \
             -t fks-bot-crypto-demo:latest .

docker run --rm -p 9091:9091 fks-bot-crypto-demo:latest                       # live
docker run --rm -e DEMO_SOURCE=synthetic -p 9091:9091 fks-bot-crypto-demo:latest  # offline
```

Spawn it from the WebUI `/bots` page, or via the spawner API:

```json
{ "image": "fks-bot-crypto-demo:latest", "mode": "paper",
  "env": { "DEMO_SYMBOLS": "XBTUSDTM,ETHUSDTM" } }
```

The image exposes the documented `fks_bot_*` series, so the FKS spawner's
Prometheus `file_sd` job scrapes it with no extra config.

## How it maps to your kucoin bot

| kucoin repo (hand-rolled) | crypto-demo (on the framework) |
|---------------------------|--------------------------------|
| `main.rs` task spawning + supervised loop | `rustrade::Bot` + `Supervisor` |
| `bot/candle_poller.rs` | `CandleSource` + `CandlePollerService` |
| `bot/strategy.rs` (indicators) | `EmaCrossBrain` (`indicators-ta`) |
| `bot/circuit_breaker.rs`, `pnl.rs`, `sizing.rs` | `rustrade-risk` (config-driven) |
| `KuCoinClient` (exchange-apiws) | same crate, via `CandleSource` |
| `fks_bot_*`-style metrics | `metrics.rs` + `:9091/metrics` |

The kucoin bot's richer SAR strategy and live order execution are the natural
next step: keep this wiring, replace the brain and the exchange adapter.

## Files

- `src/source.rs` — `KucoinCandleSource` (live) + `SyntheticCandleSource` (offline)
- `src/brain.rs` — `EmaCrossBrain`, the `indicators-ta` strategy
- `src/paper.rs` — paper position book → metrics + risk PnL
- `src/mock_exchange.rs` — paper `ExchangeClient`
- `src/metrics.rs` / `src/server.rs` — `fks_bot_*` Prometheus surface
- `src/main.rs` — wiring: build sources, brain, bot, risk config, run
