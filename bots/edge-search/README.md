# fks-bot-edge-search

One search of a registered hypothesis, and its verdict: stages 3 and 4 of the
edge process (EDGE_PROCESS_2026-10-26). The spawner runs it as a **one-shot
job instance** from the `job.edge-search` template. It reads the hypothesis,
reads the dataset it names from the lake (read-only), walks the parameter
grid forward over the training part, scores the holdout **once**, judges the
clauses U1–U9, and writes one `edge_experiments` row through the scoped
research role. Then it exits.

A job runs once. The spawner never restarts it and boot reconciliation never
re-runs it: a search run twice counts its trials twice in N.

## Settings (environment)

| Variable | Set by | Meaning |
|---|---|---|
| `EDGE_HYPOTHESIS_ID` | the template's form | the registered hypothesis to test |
| `EDGE_FAMILY` | the template's form | the strategy family: `trend-band` (default) or `orb` |
| `FKS_DB_URL` | the spawner, at spawn | the scoped research role (`fks_backtest`, fks 025) |
| `LAKE_DIR` | the spawner | the lake, read-only (`/lake`) |
| `DATA_DIR` | the spawner | this job's folder, `jobs/<instance>` (`/data`) |
| `STATUS_PORT` | | `/health`, `/metrics`, `/status` while it runs (9091) |

## What the hypothesis must say

- **dataset**: `<lake folder> <interval> <from>..<to>`, for example
  `raw/binance/spot/spot-history 1d 2017-08-17..2026-10-25`. The folder is
  a history recorder's. Monthly and daily files are merged, and timestamps
  are normalized: spot is in microseconds from 2025, futures in
  milliseconds. KuCoin folders (`raw/kucoin/futures/...`,
  `raw/kucoin/spot/...`) are the same recorder's gzipped CSV and read the
  same way.
  **A perpetual pays funding.** A perpetual folder (`raw/binance/usdm/...` or
  `raw/kucoin/futures/...`) is read with its `fundingRate` files, and a
  position held through a bar pays every settlement inside the bar's
  (open, close], or receives it when the rate is negative. The selection,
  both baselines (holding the perpetual pays funding too) and the random
  benchmark all pay it. It is measured, not estimated, so U10's cost stress
  doesn't multiply it. Funding that is missing, or has no settlement for more
  than 12 hours inside the bars, stops the search with the gap named: a
  perpetual priced as if it were free to hold is a different instrument. On
  KuCoin, 2020-26, funding cost the live trend rule 12-19 CAGR points a year
  (19.6% a year paid on BTC while in the market, 28.4% on ETH). It is also
  venue-specific: read the venue the position would trade on.
  A CME folder (`raw/cme/...`, the `orb` family) is one stitched 1-minute
  series per root, `<ROOT>/1m/<ROOT>-1m-<period>.csv.gz`, with `rolls.csv`
  and `roll_flips.csv`. Folders join with `+`, the first winning a whole UTC
day (never a session stitched from two folders):
  `raw/cme/massive-cache+raw/cme/massive/cme 1m 2024-09-09..2027-05-31`.
- **params**: the family's parameters, each a range with a step (the grid)
  or a list of choices. `trend-band` tunes `lookback` (bars) and `band`
  (a fraction); `orb` tunes `orb_minutes`, `target_r` and `exit`
  (`target` or `trail`). At most 5,000 grid points.
- **metric**: `net return after fees` (CAGR), `sharpe after fees`, or
  `return over max drawdown after fees` (Calmar).
- **baseline**: `buy-and-hold`, or `trend-200d-5pct` (the live rule), for
  `trend-band`; `session-hold` (long each session, open to close) for `orb`.
- **costs**: taker plus slippage are paid on every position change (and a
  perpetual's funding on every bar held, above). An `orb`
  hypothesis states `commission_usd_rt` ($ per contract, round turn) and
  `slippage_ticks` (per side) instead, with the bps fields at 0; the job
  refuses one that sets both rather than charge twice.
- **evidence** (fks 028): `trades` (the default) or `low-frequency`,
  declared at registration and frozen. It decides how U4 is judged; see
  below. Choosing it after seeing a trade count is what the freeze stops.

A search that can't run as registered (a bad dataset string, a parameter
the family doesn't tune, too little data) writes nothing and counts nothing.
It exits 2 with the reason. So does a window that **isn't complete yet**:
the split follows the data, and a forward holdout searched early would
move into data already seen. The data must reach within 4 days of the
registered end.

## The `orb` family (`edge_search/orb.py`)

An opening-range breakout on CME futures, one trade per session, judged on a
session grid (one bar per trading day, 252 a year). Its rules are fixed in
code, so they are part of every `orb` hypothesis:

| | |
|---|---|
| Session | ES/MES/NQ/MNQ 09:30-16:00, GC/MGC 08:20-13:30, New York time (DST-aware) |
| Range | the first `orb_minutes` minutes' high and low, every minute present |
| Entry | the first 1-minute close beyond the range, either side, at that close plus slippage; one trade per session |
| Stop | the other side of the range |
| Exit | `target`: a limit at `target_r` × risk, filled when a bar trades a tick through it. `trail`: touching it moves the stop 1R behind the best price since. Open at the last bar: out at its close |
| Fills | a stop the bar opens through fills at the open; a bar touching stop and target is the stop |
| No trade | roll and interleaved days (`rolls.csv`, `roll_flips.csv`), US exchange holidays, sessions under 98% of their minutes |
| Return | P&L over the notional at entry, one contract: roots pool as equal weights. MES/MNQ trade the ES/NQ series at their own point value |

U1 counts sessions with at least half their minutes (early closes are
present) over the weekdays that aren't holidays. U8's random strategies keep
each symbol's holdout trade count and holding times, at random sessions,
minutes after the range and sides, with the same costs.

`bots/history/tools/import_futures_cache.py` imported the futures repo's
Massive cache (ES, NQ, GC, MGC, 2024-09-09..2026-09-11) into
`raw/cme/massive-cache`, recording the cache's interleaved roll days instead
of repairing them.

## How it judges

1. **Split by time.** The last `holdout_share` of the window is the holdout.
   Nothing looks at it until step 4.
2. **Every grid point is a trial.** Each is scored on the training part, in
   `folds` consecutive stretches after the longest lookback's warm-up.
3. **Selection.** The best mean fold score is selected.
4. **The holdout, once.** The selection, its baseline and its grid
   neighbours are scored on the holdout.
5. **The clauses.** Every one must hold for USABLE:

| | Clause | As computed |
|---|---|---|
| U1 | dataset gate | every symbol's bars from its listing to the window's end ≥ 99% present |
| U2 | pre-registration | registered before the search (the database refuses otherwise too) |
| U3 | beats the baseline | holdout score > the baseline's on the same holdout |
| U4 | enough trades | `trades`: holdout entries, pooled over symbols, ≥ 30. `low-frequency`: ≥ 10 pooled, over ≥ 3 symbols, every symbol trading at least once |
| U5 | overfit gap | holdout ≥ 50% of the selection score, both above their baselines |
| U6 | neighbourhood | every one-step grid neighbour of the winner beats the baseline on the holdout |
| U7 | deflated Sharpe | P(true Sharpe > the best of N null draws) ≥ 0.95, with N = every trial ever run for the edge |
| U8 | random benchmark | beats ≥ 95% of 1,000 random strategies with the same trades, spell length and costs |
| U9 | no edge of the range | the winner sits on neither end of a numeric range |

**The low-frequency path** is EDGE_PROCESS §3–4's route for an edge that
trades a few times a year per coin (a trend band): no history gives it 30
holdout trades, so it is judged on many coins with ONE shared parameter set.
U3, U5, U7 and U8 are already judged on the pooled trades; only U4 changes.
Three coins over a 2-year holdout give a trend band about 7 trades, short of
10: the low-frequency path needs more coins, not a lower bar.

**The regime split** is reported in `verdict.json` for every search (not a
clause): each holdout bar is "up" when the coins' mean trailing return over
`regime_lookback_days` (200), known at the bar before, is positive, and
"down" otherwise; the selection and the baseline are scored in each.

The thresholds are EDGE_PROCESS §9's **proposals (†)**, kept in
`config.THRESHOLDS` with a version. The version goes into every
experiment's `config_fingerprint`.

The pool is equal-weight over the symbols listed at each bar. A position is
decided on a bar's close and earns the next bar's return.

## Files, in `DATA_DIR/<study_name>/`

| File | Holds |
|---|---|
| `trials.csv` | every grid point: its parameters, fold scores, selection score and trades |
| `holdout.csv` | each holdout bar's pool return, for the selection and the baseline |
| `verdict.json` | the row as written, every clause with its numbers, coverage, N, the deflated Sharpe, the random share, the evidence path, holdout trades per symbol, the regime split, the thresholds, and on a perpetual `funding`: each symbol's mean rate a year and what the selection paid a year in the market, over the whole series and the holdout |

## Tests

```bash
pip install -r bots/edge-search/requirements.txt -r bots/edge-search/requirements-dev.txt
python -m pytest bots/edge-search/tests -q
```

The tests use synthetic series and need no network or database. They
include a random walk (never USABLE) and long regimes that beat
buy-and-hold but trade too rarely on three coins (U4).
