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
| `EDGE_FAMILY` | the template's form | the strategy family, default `trend-band` |
| `FKS_DB_URL` | the spawner, at spawn | the scoped research role (`fks_backtest`, fks 025) |
| `LAKE_DIR` | the spawner | the lake, read-only (`/lake`) |
| `DATA_DIR` | the spawner | this job's folder, `jobs/<instance>` (`/data`) |
| `STATUS_PORT` | | `/health`, `/metrics`, `/status` while it runs (9091) |

## What the hypothesis must say

- **dataset**: `<lake folder> <interval> <from>..<to>`, for example
  `raw/binance/spot/spot-history 1d 2017-08-17..2026-10-25`. The folder is
  a history recorder's. Monthly and daily files are merged, and timestamps
  are normalized: spot is in microseconds from 2025, futures in
  milliseconds.
- **params**: the family's parameters, each a range with a step (the grid)
  or a list of choices. `trend-band` tunes `lookback` (bars) and `band`
  (a fraction). At most 5,000 grid points.
- **metric**: `net return after fees` (CAGR), `sharpe after fees`, or
  `return over max drawdown after fees` (Calmar).
- **baseline**: `buy-and-hold`, or `trend-200d-5pct` (the live rule).
- **costs**: taker plus slippage are paid on every position change.

A search that can't run as registered (a bad dataset string, a parameter
the family doesn't tune, too little data) writes nothing and counts nothing.
It exits 2 with the reason.

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
| U4 | enough trades | holdout entries, pooled over symbols, ≥ 30 |
| U5 | overfit gap | holdout ≥ 50% of the selection score, both above their baselines |
| U6 | neighbourhood | every one-step grid neighbour of the winner beats the baseline on the holdout |
| U7 | deflated Sharpe | P(true Sharpe > the best of N null draws) ≥ 0.95, with N = every trial ever run for the edge |
| U8 | random benchmark | beats ≥ 95% of 1,000 random strategies with the same trades, spell length and costs |
| U9 | no edge of the range | the winner sits on neither end of a numeric range |

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
| `verdict.json` | the row as written, every clause with its numbers, coverage, N, the deflated Sharpe, the random share and the thresholds |

## Tests

```bash
pip install -r bots/edge-search/requirements.txt -r bots/edge-search/requirements-dev.txt
python -m pytest bots/edge-search/tests -q
```

The tests use synthetic series and need no network or database. They
include a random walk (never USABLE) and long regimes that beat
buy-and-hold but trade too rarely on three coins (U4).
