# fks-bot-cme-history

CME futures **1-minute bars from Massive** (massive.com), one UTC day at a
time, **stitched into one series per root by volume**, written to the lake
for the search job's `orb` family. It keeps the series current, which makes
a hypothesis registered now hold an **unseen forward holdout**.

The spawner runs it from the `history.cme-massive` template (fks). Name the
instance **`cme`**: its folder is then `raw/cme/massive/cme`, the one the
Register-hypothesis form joins with the imported cache:
`raw/cme/massive-cache+raw/cme/massive/cme`.

## The key

Massive needs an API key. Store it in **Settings → API keys** as provider
*Other…*, slug `massive`: the key is the Massive key, and the secret, which
the store requires but Massive doesn't use, can be any placeholder (`unused`).
The template names `massive` in `secrets_allowed`, the one kind of credential
a recorder may hold (fks-spawner `templates.rs` `DATA_VENDOR_SECRETS`: a data
vendor's key reads data, it can't place an order), and the spawner injects it
as `MASSIVE_API_KEY` at every spawn. Creating the instance needs an admin.

## Settings (environment)

| Variable | Meaning |
|---|---|
| `CME_ROOTS` | product roots, e.g. `ES,NQ,GC,MGC` |
| `CME_START` | the first UTC day, default `2026-09-14`: the Monday after the imported cache ends |
| `CME_POLL_HOURS` | hours between checks (default 6) |
| `MASSIVE_API_KEY` | injected by the spawner |
| `MASSIVE_RPM` | requests a minute (default 5, the free tier) |

## How a day is chosen

- **By the previous weekday's volume**, among the three nearest contracts not
  expired: known before the day starts, so no look-ahead.
- **Never backwards**: once on a contract, only it and later expiries.
- **One contract per day.** The futures repo's cache, which rolled on two
  rules at once, interleaved two contracts minute by minute on some roll
  days; one choice per day can't.
- A day is recorded once it is over (two hours past UTC midnight).
  Saturdays are skipped; a day with no bars (a holiday) gets no file.

## Files (append-only)

| Path | Holds |
|---|---|
| `<ROOT>/1m/<ROOT>-1m-<YYYY-MM-DD>.csv.gz` | the stitched day: `open_time_ms,open,high,low,close,volume` |
| `<ROOT>/contracts/<TICKER>/<TICKER>-1m-<day>.csv.gz` | every candidate's own bars, for audit |
| `<ROOT>/rolls.csv` | day, from, to, gap (new minus old close at the first shared minute), `volume` |
| `_manifest.jsonl` | every file: sha256, bytes, rows, contract |
| `_state/<ROOT>.json` | the last day, the contract, the volumes the next choice reads |

A file that exists is never rewritten; state is saved after every day, so a
failure redoes only the failed day at the next poll.

## Rate

Per root and day: one request per candidate (3), plus a contracts listing at
most weekly. At 5 requests a minute, four roots cost about 2.5 minutes a day.

`:9091` serves `/health`, `/metrics` (`fks_cme_history_*`) and `/status`.
