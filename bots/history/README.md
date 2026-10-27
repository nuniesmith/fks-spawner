# fks-bot-history

Downloads Binance's public archive, [data.binance.vision](https://data.binance.vision),
into the lake and keeps it current: candles, funding and futures metrics,
every file checked against its published sha256 and logged in a manifest. The
recorder captures what the archive doesn't keep (liquidations, best bid/ask,
mark price each second); this gives the edge search years of history the
recorder can't, for free and with no key. The spawner runs one container per
instance, configured from a template's form.

## Settings (environment)

| Variable | Default | Meaning |
|---|---|---|
| `HISTORY_MARKET` | required | `spot` or `usdm` (USDⓈ-M futures); each template sets it |
| `HISTORY_SYMBOLS` | required | comma-separated, e.g. `BTCUSDT,ETHUSDT` |
| `HISTORY_DATASETS` | `klines` | `klines`; `usdm` also has `fundingRate` and `metrics` |
| `HISTORY_INTERVALS` | `1h,1d` | kline intervals, `1m` to `1d` |
| `HISTORY_START` | `2020-01` | the first month, `YYYY-MM`, or a year `YYYY` for its January; 2017 or later, not in the future |
| `HISTORY_POLL_HOURS` | `6` | hours between checks for new files, 0.25 to 48 |
| `HISTORY_CONCURRENCY` | `4` | parallel downloads, at most 8 |
| `HISTORY_BASE_URL` | `https://data.binance.vision` | the archive |
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

## `:9091`

- `/health`: 200 while the heartbeat (written every 10 s) is under 60 s old,
  else 503.
- `/status`: the heartbeat as JSON (`"kind": "history"`), with each series'
  counts: `expected`, `present`, `pending`, `not_published`, `errors`, `bytes`,
  and the `first` and `last` dates on disk.
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
