# fks-bot-example

Reference bot image for the **FKS spawner**. Spawn it from `/bots`,
watch its log stream, see its metrics show up in Prometheus / Grafana.

This is the smallest end-to-end thing you can run as a `fks-bot-*`
container. Production trading bots replace `HeartbeatBrain` with real
strategy logic but keep everything else.

## What it does

```
┌──────────────────────────────────────────────────┐
│ fks-bot-example container                        │
│                                                  │
│   ┌────────────┐    publishes       ┌────────┐   │
│   │ synthetic  │  ──────────────►   │ market │   │
│   │ ticker     │  (1 tick/sec)      │ bus    │   │
│   └────────────┘                    └───┬────┘   │
│                                         │        │
│                                  ┌──────▼─────┐  │
│                                  │ Heartbeat  │  │
│                                  │ Brain      │  │
│                                  └─────┬──────┘  │
│                                        │         │
│                              records   │ signals │
│                              ┌─────────▼──┐      │
│                              │ Prometheus │      │
│                              │ /metrics   ├─►:9091
│                              └────────────┘      │
└──────────────────────────────────────────────────┘
```

1. A `MockExchange` stands in for a real broker (every order succeeds, no
   network calls).
2. `HeartbeatBrain` increments counters on every market event, emits a
   buy/sell signal every `BOT_SIGNAL_EVERY` events (default 6), and records
   a synthetic trade every `BOT_TRADE_EVERY` events (default 20).
3. A synthetic ticker publisher feeds the brain ~1 tick/sec on a random
   walk so the counters tick up.
4. An axum HTTP server on `:9091` serves `/health` (liveness probe) and
   `/metrics` (Prometheus text exposition) with the **five required
   `fks_bot_*` series**:

   | Series                   | Type    | Range  |
   |--------------------------|---------|--------|
   | `fks_bot_pnl_dollars`    | gauge   | any    |
   | `fks_bot_signals_total`  | counter | 0..    |
   | `fks_bot_trades_total`   | counter | 0..    |
   | `fks_bot_win_rate`       | gauge   | 0..1   |
   | `fks_bot_uptime_seconds` | gauge   | 0..    |

## Configuration (env vars)

| Var                | Default       | Purpose                               |
|--------------------|---------------|---------------------------------------|
| `FKS_BOT_ID`       | `local-dev`   | Set by spawner — surfaced in logs     |
| `FKS_BOT_MODE`     | `paper`       | Set by spawner — informational label  |
| `BOT_SYMBOL`       | `BTCUSDT`     | Symbol the synthetic ticker reports   |
| `BOT_METRICS_PORT` | `9091`        | Where `/metrics` listens              |
| `BOT_SIGNAL_EVERY` | `6`           | Emit a non-Hold decision every N events |
| `BOT_TRADE_EVERY`  | `20`          | Record a synthetic trade every N events |
| `RUST_LOG`         | `info,fks_bot_example=debug` | tracing-subscriber filter |

## Run locally (without Docker)

```bash
cargo run -p fks-bot-example
# in another terminal:
curl -s http://localhost:9091/metrics | grep fks_bot_
```

After ~30 seconds you should see non-zero values across all five series.

## Run via the spawner

```bash
# Build the image first (from repo root):
docker build -f infrastructure/docker/services/fks-bot-example/Dockerfile \
             -t fks-bot-example:latest .

# Then spawn from the WebUI at /bots, or via curl:
curl -X POST http://localhost:8090/spawn \
  -H 'Content-Type: application/json' \
  -d '{
        "image": "fks-bot-example:latest",
        "mode":  "paper",
        "env":   { "BOT_SYMBOL": "ETHUSDT" }
      }'
```

The spawner forces `fks.bot=true`, `fks.bot_id=<uuid>`, `fks.mode=paper`
labels, mounts the container on `fks_network`, and writes the SD entry to
`/prometheus-sd/bots.json` so Prometheus picks it up automatically.

## Testing

```bash
cargo test -p fks-bot-example
```

4 unit tests cover the heartbeat brain's signal cadence + health surface
and the metrics module's exposition + win-rate accounting.

## Code layout

```
src/
├── main.rs          # entry point, env parsing, wiring
├── brain.rs         # HeartbeatBrain (Brain impl)
├── mock_exchange.rs # always-succeed ExchangeClient impl
├── metrics.rs       # prometheus crate Lazy<Counter|Gauge> globals
├── server.rs        # axum :9091 — /health + /metrics
└── ticker.rs        # synthetic random-walk ticker publisher
```

## When you're ready to write a real bot

Copy this directory, then:

1. Replace `MockExchange` with a real `ExchangeClient` impl
   (`rustrade-kucoin::KucoinExchangeAdapter`, or a new
   `rustrade-bybit`/`rustrade-binance` adapter).
2. Replace `HeartbeatBrain` with your strategy logic. Keep the
   `metrics::record_signal()` / `metrics::record_trade(pnl)` calls so
   the spawner's monitoring still works.
3. Replace the synthetic ticker with a real feed handler — see
   `examples/kucoin-v2/src/poller.rs` for a REST-polling pattern, or
   `exchange-apiws` for a WebSocket pattern.
4. Update `infrastructure/docker/services/<your-bot>/Dockerfile` to
   build your binary instead of `fks-bot-example`. Keep the image name
   prefixed `fks-bot-*` so the spawner allows it.

The framework owns supervision, restart policy, sizing, session PnL, and
graceful shutdown — your bot owns the strategy and the metrics.
