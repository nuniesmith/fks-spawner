# fks-bot-history

Downloads Binance's public archive, [data.binance.vision](https://data.binance.vision),
and KuCoin's public market data (see [KuCoin](#kucoin-api-not-an-archive)),
into the lake and keeps it current: candles, funding and futures metrics,
every file checked against its published sha256 and logged in a manifest. The
recorder captures what the archive doesn't keep (liquidations, best bid/ask,
mark price each second); this gives the edge search years of history the
recorder can't, for free and with no key. The spawner runs one container per
instance, configured from a template's form.

## Settings (environment)

| Variable | Default | Meaning |
|---|---|---|
| `HISTORY_MARKET` | required | `spot` or `usdm` (Binance USDⓈ-M futures), `kucoin-futures` or `kucoin-spot`; each template sets it |
| `HISTORY_SYMBOLS` | required | comma-separated, e.g. `BTCUSDT,ETHUSDT`; KuCoin contracts by KuCoin's name (`XBTUSDTM` is BTC); KuCoin spot pairs as `BTCUSDT` (USDT or USDC quotes) |
| `HISTORY_DATASETS` | `klines` | `klines`; `usdm` also has `fundingRate` and `metrics`; `kucoin-futures` also has `fundingRate` |
| `HISTORY_INTERVALS` | `1h,1d` | kline intervals, `1m` to `1d`; KuCoin `1h`, `4h`, `1d` |
| `HISTORY_START` | `2020-01` | the first month, `YYYY-MM`, or a year `YYYY` for its January; 2017 or later, not in the future |
| `HISTORY_POLL_HOURS` | `6` | hours between checks for new files, 0.25 to 48 |
| `HISTORY_CONCURRENCY` | `4` | parallel downloads, at most 8 |
| `HISTORY_BASE_URL` | per market | `https://data.binance.vision`, `https://api-futures.kucoin.com` or `https://api.kucoin.com` |
| `DATA_DIR` | `/data` | where the files go |
| `RECORDER_PORT` | `9091` | `/health`, `/metrics`, `/status` |
| `FKS_BOT_ID` | `local` | set by the spawner |

A setting it can't use stops it at startup with a message saying which one.

## What it fetches

The archive publishes each dataset monthly, daily, or both, about a day after
the period closes. Each check asks for:

| Dataset | Files | From |
|---|---|---|
| `klines` | every complete month since `HISTORY_START`, plus daily files for the last 35 days (they cover each month until its monthly file appears) | spot and usdm |
| `fundingRate` | every complete month | usdm |
| `metrics` | every day since `HISTORY_START` (open interest, top-trader and taker long/short ratios, in 5-minute rows) | usdm |

Old daily kline files are kept when the month's file arrives: where both
exist, read the monthly one.

A 404 is an answer, not an error. Within 45 days of the period's end it means
"not yet", and the file is asked for again each check. Older, it means
"never" (a month before the symbol listed, say): it is remembered in
`_state/not_published.json` and asked for again weekly, in case the archive
back-fills. Any other failure, including a checksum mismatch, is an error,
retried the next check, and nothing is written.

## Files, under `DATA_DIR`

| Path | Holds |
|---|---|
| `<SYMBOL>/<dataset>/[<interval>/]<archive file name>.zip` | the archive's own zip, byte for byte, e.g. `BTCUSDT/klines/1h/BTCUSDT-1h-2026-09.zip` |
| `_manifest.jsonl` | one line per download: path, URL, sha256, bytes, dataset, symbol, interval, period, date, when |
| `_state/status.json` | the heartbeat `/status` serves |
| `_state/not_published.json` | the old 404s, by file name |

The market isn't repeated inside: the spawner names the instance's lake folder
`raw/binance/<market>/<instance>`. A file is written as `<name>.part`,
fsynced, and renamed into place only after its sha256 matches the published
`.CHECKSUM`, so a file in the lake is always a whole, verified copy.

### Traps in the archive's files

- **Spot klines are in microseconds from 2025 on.** Spot open and close times
  are 16 digits (`1788220800000000`), futures still 13 (milliseconds), and
  spot files before 2025 are milliseconds too. Normalize when reading.
- **Spot klines have no header row; futures klines do** (`open_time,open,...`).
- **Metrics rows are not in time order.** Each day has its 288 five-minute
  rows, but 152 of 172 files checked had them shuffled: sort when reading.
- **Funding times aren't always round**: `calc_time` can be a few
  milliseconds past the funding hour (`1788220800005`).

## KuCoin: API, not an archive

KuCoin publishes no files, so `kucoin-futures` and `kucoin-spot` page its
public API (no key) into the same plan as the archive: every complete month
as a monthly file, plus daily files for the last 35 days, for candles **and**
funding (the API answers any period, so funding is kept daily too). A file is
written once a period has closed and never changed; the code is
`history/kucoin.py`.

| Path | Columns |
|---|---|
| `<SYMBOL>/klines/<interval>/<SYMBOL>-<interval>-<YYYY-MM \| YYYY-MM-DD>.csv.gz` | `open_time_ms,open,high,low,close,volume,turnover` |
| `<SYMBOL>/fundingRate/<SYMBOL>-fundingRate-<YYYY-MM \| YYYY-MM-DD>.csv.gz` | `funding_time_ms,funding_rate` |

Values are written exactly as KuCoin sent them. The gzip header has no
timestamp, so the same rows give the same sha256. Each manifest line also says
what the API covered: `rows`, `first_ms`, `last_ms`, and for candles
`missing_after_first` and `missing_at_end` (slots with no candle), for funding
`max_gap_ms`. A listing month starts late and says so; a gap inside is
counted, not hidden. A period with no rows is "missing", like the archive's
404. A candle period that closed under 2 days ago and lacks its last candle is
also "missing" (asked again next check), so a lagging answer never becomes a
permanent gap.

### Traps in KuCoin's API (each silently loses rows if ignored)

- **Funding answers hold at most 100 rows, and they are the NEWEST 100.** A
  wide window drops its start with no error. Paged backward.
- **Futures candles: at most 200 rows, OLDEST first.** Paged forward.
- **Spot candles: at most 1500 rows, newest first, and the columns are
  `[time, open, CLOSE, HIGH, LOW, volume, turnover]`**, with time in seconds.
  Futures candles are `[time_ms, open, high, low, close, ...]`. Reordered to
  open, high, low, close on write.
- **Spot pairs are dashed** (`BTC-USDT`); the spawner's symbol format has no
  dash, so they are entered as `BTCUSDT` and dashed here.
- **Funding times moved in mid-2025**: KuCoin settled at 04:00, 12:00 and 20:00
  UTC until about 2025-06-15, then at 00:00, 08:00 and 16:00 like Binance. Some
  contracts settle every 4 h or 1 h; read the times, never assume them.
- **Hourly candles don't reach far back** (none for 2021 when asked on
  2026-11-06); daily candles go back to listing. Older hourly months come back
  empty and are recorded as never published.
- **Funding can predate a contract's `firstOpenDate`** (XBTUSDTM's starts
  2020-03-26, four days earlier).

**KuCoin's funding is not Binance's.** Compared by day on 2026-11-06, the two
move together (correlation mostly 0.5 to 0.9) but KuCoin's was often far
higher: BTC 52% a year against 26% in 2021-11, SOL 91% against 45% in
2024-03. Research for a KuCoin position must use KuCoin's funding.

Measured on 2026-11-06: XBTUSDTM and SOLUSDTM daily candles and funding since
2020-03 (438 files, 2 MB) plus BTCUSDT spot (115 files, 0.5 MB), in one
check, with no errors; every funding rate and close matched a separate fetch
of the same history.

## `:9091`

- `/health`: 200 while the heartbeat (written every 10 s) is under 60 s old,
  else 503.
- `/status`: the heartbeat as JSON (`"kind": "history"`), with each series'
  counts from the last finished check (`expected`, `present`, `pending`,
  `not_published`, `errors`, `bytes`, and the `first` and `last` dates on
  disk), and the running check's progress in `cycle` (`planned`, `checked`).
  A check counts into a new table and swaps it in when done, so the counts
  never dip to zero mid-check.
- `/metrics`: Prometheus text:
  - `fks_history_files{dataset,symbol,interval,state}`, the counts above by
    state (`interval="-"` for funding and metrics);
  - `fks_history_bytes{dataset,symbol,interval}`;
  - `fks_history_downloads_total` and `fks_history_errors_total`, this run;
  - `fks_history_cycle_running`;
  - `fks_history_last_success_timestamp_seconds`, when the last check finished
    with no errors (absent until one has);
  - `fks_history_up`.

## Run it

```bash
docker build -f bots/history/Dockerfile -t fks-bot-history .   # from the repo root
mkdir -p /tmp/hist && chmod 777 /tmp/hist
docker run --rm -e HISTORY_MARKET=usdm -e HISTORY_SYMBOLS=BTCUSDT \
  -e HISTORY_DATASETS=klines,fundingRate,metrics -e HISTORY_START=2026-01 \
  -v /tmp/hist:/data -p 127.0.0.1:9091:9091 fks-bot-history
curl -s localhost:9091/status
```

Measured on 2026-10-27: BTCUSDT and ETHUSDT futures since 2026-08 (1d klines,
funding, metrics) were 252 files and 2.5 MB, fetched in 52 s. A month of 1h
spot klines is about 43 KB, and a day of metrics about 11 KB.

## Tests

```bash
pip install -r bots/history/requirements.txt -r bots/history/requirements-dev.txt
python -m pytest bots/history/tests -q
```

The tests need no network. `.github/workflows/history.yml` runs them on pull
requests and publishes `ghcr.io/nuniesmith/fks-bot-history` from `main`.
