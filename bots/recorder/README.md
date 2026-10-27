# fks-bot-recorder

Records raw public market data, append-only: a venue's websocket streams and
REST snapshots, one file per symbol, stream and UTC day, with a **gap ledger**
so missing data is never mistaken for a quiet market. The spawner runs one
container per recorder instance, configured from a template's form; nothing
here needs a key.

Ported from the `btc` repo's recorder, which has recorded BTCUSDT on oryx since
2026-10-21, and generalized (EDGE_FACTORY_PLAN §5). Python on purpose: the
dataset matters more than the language, and this code is proven against the
two Binance traps below.

## Settings (environment)

| Variable | Default | Meaning |
|---|---|---|
| `RECORDER_SYMBOLS` | required unless market-wide streams are set | comma-separated, e.g. `BTCUSDT,ETHUSDT` |
| `RECORDER_VENUE` | `binance-usdm` | the only venue so far |
| `RECORDER_STREAMS` | all three (none without symbols) | `forceOrder` (liquidations), `bookTicker` (best bid/ask), `markPrice@1s` (mark, index, funding) |
| `RECORDER_MARKET_STREAMS` | none | one feed for every symbol: `forceOrder@arr` (every liquidation on the exchange, ~0.7 msg/s, a few MB a day) or `markPrice@arr` (every symbol's mark price and funding, ~229 symbols a message) |
| `RECORDER_REST` | both (none without symbols) | `openInterest`, `premiumIndex` |
| `RECORDER_REST_SECONDS` | `300` | between REST snapshots, at least 60 |
| `RECORDER_BACKFILL_OI` | `0` | `1` also backfills open interest from Binance Vision daily metrics, as the btc recorder did while its REST access was geo-blocked |
| `DATA_DIR` | `/data` | where the files go |
| `RECORDER_PORT` | `9091` | `/health`, `/metrics`, `/status` |
| `FKS_BOT_ID` | `local` | set by the spawner |
| `HTTPS_PROXY` | unset | optional egress proxy |

A setting the recorder can't use stops it at startup with a message saying
which one, before it opens a connection.

## Files, under `DATA_DIR`

| Path | Holds |
|---|---|
| `<SYMBOL>/<stream>/YYYY-MM-DD.jsonl` | `{"recv_ts_ms", "stream", "data"}` per message: the raw frame plus our receive time. Gzipped once the UTC day closes, never edited after. `markPrice@1s` goes in `markPrice/`. |
| `<SYMBOL>/snapshots/YYYY-MM-DD.jsonl` | REST snapshots, same envelope, `"stream": "rest:snapshots"` |
| `ALL/<stream>/YYYY-MM-DD.jsonl` | market-wide streams, same envelope (`"stream": "!forceOrder@arr"`) |
| `_gaps/YYYY-MM-DD.jsonl` | `{"group", "start_ms", "end_ms", "reason"}`, one line per stretch a connection recorded nothing |
| `_state/status.json` | the heartbeat `/status` serves |

The btc recorder's layout is the same minus the `<SYMBOL>/` level, so its
history copies in by moving `forceOrder/`, `bookTicker/`, `markPrice/` and
`snapshots/` under `BTCUSDT/`.

### The gap ledger

A gap is time a **connection** recorded nothing, by reason:

| Reason | From | To |
|---|---|---|
| `startup` | the process start | the connection's first connect |
| `recorder_down` | the previous run's last heartbeat (written at least every 10 s, and on a clean stop) | the first connect |
| `disconnect` | the disconnect | the reconnect |
| `stall` | the connection's last message, when none of its streams has said anything for 5 minutes and it is recycled | the reconnect |

One quiet stream is never a gap: `forceOrder` prints only when liquidations
happen and can be silent for hours. Its silence is logged (after 10 minutes,
then hourly) and visible in `/status`.

## `:9091`

- `/health`: 200 while the heartbeat is under 60 s old, else 503.
- `/status`: the heartbeat, as JSON.
- `/metrics`: Prometheus text:
  - `fks_recorder_messages_total{symbol,stream}`;
  - `fks_recorder_bytes_written_total{symbol,stream}`;
  - `fks_recorder_last_message_age_seconds{symbol,stream}`, absent until the stream's first message;
  - `fks_recorder_connected{group}`;
  - `fks_recorder_reconnects_total{group}`;
  - `fks_recorder_gap_seconds_total{group}`;
  - `fks_recorder_rest_snapshots_total{symbol}` and `fks_recorder_rest_errors_total{symbol}`;
  - `fks_recorder_up` and `fks_recorder_uptime_seconds`.

## The two Binance traps

- **Connection: close.** The gateway answers the websocket upgrade with
  `Connection: close` in its 101 response, and strict clients reject that.
  websocket-client is pinned at 1.9.2, and one narrow patch drops only its
  `connection` header check. Status 101 and `Sec-WebSocket-Accept` are still
  verified. The recorder refuses to start if the library's internals changed.
- **The endpoint split.** Futures streams are split across `/market` and
  `/public` base paths, and the legacy combined base acknowledges a market
  subscription but never delivers a message. So each base path gets its own
  connection: liquidations and mark price on `/market`, best bid/ask on
  `/public`. Each one is a "group" in the gap ledger and the metrics.

## Run it

```bash
docker build -f bots/recorder/Dockerfile -t fks-bot-recorder .   # from the repo root
mkdir -p /tmp/rec && chmod 777 /tmp/rec
docker run --rm -e RECORDER_SYMBOLS=BTCUSDT -v /tmp/rec:/data -p 127.0.0.1:9091:9091 fks-bot-recorder
curl -s localhost:9091/status
```

The image runs as uid 1000. A mounted data folder must be writable by it; the
spawner creates each instance's folder that way.

## Tests

```bash
pip install -r bots/recorder/requirements.txt -r bots/recorder/requirements-dev.txt
python -m pytest bots/recorder/tests -q
```

The tests need no network. `.github/workflows/recorder.yml` runs them on pull
requests and publishes `ghcr.io/nuniesmith/fks-bot-recorder` from `main`.
