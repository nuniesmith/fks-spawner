//! Offline backtest of the spot-rebalancing portfolio.
//!
//! Replays a historical price series through the **real** (pure)
//! [`rebalance::plan`] — the exact logic the live bot uses — starting fully in
//! cash, applying every triggered rebalance (with a taker fee + a per-bar
//! cooldown), and scoring the result by total return / max drawdown / Sharpe.
//! This lets `band` / `reserve_pct` / `cooldown` be tuned against history
//! without risking live money.
//!
//! The simulation is intentionally simple and *relative*: fills are at the bar
//! price (no intrabar slippage model beyond the flat fee), and the reserve
//! currency earns nothing. Treat results as a comparative guide for tuning, not
//! a promise of live performance.

use std::collections::HashMap;

use super::exchange::Side;
use super::rebalance::{self, Holding, Target};

/// Tunable parameters for one backtest run — mirrors the live config's rebalance
/// knobs plus a fee and an annualization factor.
#[derive(Debug, Clone)]
pub struct BacktestParams {
    /// Fraction of value held in the cash reserve (0.0–1.0).
    pub reserve_pct: f64,
    /// Relative drift band that triggers a rebalance.
    pub band: f64,
    /// Minimum bars between rebalances (the backtest's cooldown, in price steps).
    pub cooldown_bars: usize,
    /// Skip trades below this notional (dust / venue minimum).
    pub min_trade_usd: f64,
    /// Taker fee charged on every fill (e.g. 0.001 = 10 bps).
    pub fee_pct: f64,
    /// Bars per year, for annualizing Sharpe (365 daily, 8760 hourly).
    pub bars_per_year: f64,
}

impl Default for BacktestParams {
    fn default() -> Self {
        Self {
            reserve_pct: 0.2,
            band: 0.25,
            cooldown_bars: 1,
            min_trade_usd: 10.0,
            fee_pct: 0.001,
            bars_per_year: 365.0,
        }
    }
}

/// The outcome of a backtest over a price history.
#[derive(Debug, Clone)]
pub struct BacktestResult {
    /// Portfolio value at the last bar.
    pub final_value: f64,
    /// `final / initial - 1`.
    pub total_return: f64,
    /// Worst peak-to-trough drawdown over the run, as a fraction.
    pub max_drawdown: f64,
    /// Annualized Sharpe from per-bar returns (0 risk-free).
    pub sharpe: f64,
    /// Number of rebalances executed.
    pub rebalances: usize,
    /// Number of price bars replayed.
    pub bars: usize,
}

impl BacktestResult {
    /// A single-line summary for sweep output.
    pub fn summary_line(&self) -> String {
        format!(
            "return {:+.1}%  maxDD {:.1}%  sharpe {:.2}  rebalances {}  bars {}",
            self.total_return * 100.0,
            self.max_drawdown * 100.0,
            self.sharpe,
            self.rebalances,
            self.bars,
        )
    }
}

/// Replay `prices` (a series of `{asset → price}` snapshots, oldest first)
/// through the rebalancer, starting from `initial_cash` fully in cash. Every bar
/// runs the real [`rebalance::plan`]; when it triggers past the cooldown, the
/// trades are applied to the simulated book with `fee_pct` charged per fill.
pub fn backtest_portfolio(
    prices: &[HashMap<String, f64>],
    targets: &[Target],
    initial_cash: f64,
    params: &BacktestParams,
) -> BacktestResult {
    let mut qty: HashMap<String, f64> = targets.iter().map(|t| (t.name.clone(), 0.0)).collect();
    let mut cash = initial_cash;
    let mut last_rebalance: Option<usize> = None;
    let mut rebalances = 0usize;
    let mut net_worths: Vec<f64> = Vec::with_capacity(prices.len());

    for (i, px) in prices.iter().enumerate() {
        let holdings: Vec<Holding> = targets
            .iter()
            .map(|t| Holding {
                name: t.name.clone(),
                qty: *qty.get(&t.name).unwrap_or(&0.0),
                price: *px.get(&t.name).unwrap_or(&0.0),
            })
            .collect();

        let p = rebalance::plan(
            &holdings,
            cash,
            targets,
            params.reserve_pct,
            params.band,
            params.min_trade_usd,
        );
        net_worths.push(p.total_value);

        let past_cooldown = last_rebalance.is_none_or(|last| i - last >= params.cooldown_bars);
        if p.triggered && !p.trades.is_empty() && past_cooldown {
            for tr in &p.trades {
                let q = qty.entry(tr.name.clone()).or_insert(0.0);
                match tr.side {
                    Side::Buy => {
                        cash -= tr.usd * (1.0 + params.fee_pct);
                        *q += tr.volume;
                    }
                    Side::Sell => {
                        cash += tr.usd * (1.0 - params.fee_pct);
                        *q -= tr.volume;
                    }
                }
            }
            last_rebalance = Some(i);
            rebalances += 1;
        }
    }

    let initial = net_worths.first().copied().unwrap_or(initial_cash);
    let final_value = net_worths.last().copied().unwrap_or(cash);
    BacktestResult {
        final_value,
        total_return: if initial > 0.0 {
            final_value / initial - 1.0
        } else {
            0.0
        },
        max_drawdown: max_drawdown(&net_worths),
        sharpe: sharpe(&net_worths, params.bars_per_year),
        rebalances,
        bars: prices.len(),
    }
}

/// Worst peak-to-trough drawdown of an equity curve, as a fraction.
fn max_drawdown(nw: &[f64]) -> f64 {
    let mut peak = f64::MIN;
    let mut worst = 0.0_f64;
    for &v in nw {
        if v > peak {
            peak = v;
        }
        if peak > 0.0 {
            worst = worst.max((peak - v) / peak);
        }
    }
    worst
}

/// Annualized Sharpe of an equity curve's per-bar returns (0 risk-free rate).
fn sharpe(nw: &[f64], bars_per_year: f64) -> f64 {
    let rets: Vec<f64> = nw
        .windows(2)
        .filter(|w| w[0] > 0.0)
        .map(|w| w[1] / w[0] - 1.0)
        .collect();
    if rets.len() < 2 {
        return 0.0;
    }
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (rets.len() - 1) as f64;
    let sd = var.sqrt();
    if sd <= 0.0 {
        return 0.0;
    }
    (mean / sd) * bars_per_year.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets() -> Vec<Target> {
        vec![
            Target {
                name: "BTC".into(),
                weight: 0.6,
            },
            Target {
                name: "ETH".into(),
                weight: 0.4,
            },
        ]
    }

    fn series(points: &[(f64, f64)]) -> Vec<HashMap<String, f64>> {
        points
            .iter()
            .map(|(b, e)| HashMap::from([("BTC".to_string(), *b), ("ETH".to_string(), *e)]))
            .collect()
    }

    #[test]
    fn max_drawdown_of_known_curve() {
        // 100 → 120 (peak) → 90 (−25% from peak) → 110.
        assert!((max_drawdown(&[100.0, 120.0, 90.0, 110.0]) - 0.25).abs() < 1e-9);
        assert_eq!(
            max_drawdown(&[100.0, 110.0, 120.0]),
            0.0,
            "monotonic up = no DD"
        );
    }

    #[test]
    fn flat_prices_hold_value_after_initial_deployment() {
        // Prices never move → after the initial deployment there's nothing to
        // rebalance, and net worth stays ~ initial (minus the one-time entry fee).
        let px = series(&[(100.0, 100.0); 10]);
        let r = backtest_portfolio(&px, &targets(), 1000.0, &BacktestParams::default());
        assert!(
            r.total_return.abs() < 0.01,
            "flat market ≈ flat return, got {}",
            r.total_return
        );
        assert_eq!(r.rebalances, 1, "only the initial cash deployment");
        assert!(r.max_drawdown < 0.02);
    }

    #[test]
    fn a_rising_basket_produces_positive_return() {
        // Both assets double over the window → the deployed basket ~doubles
        // (minus the cash reserve, which stays flat).
        let mut pts = Vec::new();
        for i in 0..20 {
            let f = 1.0 + i as f64 / 19.0; // 1.0 → 2.0
            pts.push((100.0 * f, 100.0 * f));
        }
        let px = series(&pts);
        let params = BacktestParams {
            reserve_pct: 0.2,
            ..Default::default()
        };
        let r = backtest_portfolio(&px, &targets(), 1000.0, &params);
        // ~80% invested doubling → roughly +80% overall; allow a wide band.
        assert!(
            r.total_return > 0.5,
            "rising basket should gain, got {:+.1}%",
            r.total_return * 100.0
        );
        assert!(r.final_value > 1000.0);
    }

    #[test]
    fn params_default_are_sane() {
        let p = BacktestParams::default();
        assert!(p.band > 0.0 && p.reserve_pct >= 0.0 && p.fee_pct >= 0.0);
    }
}
