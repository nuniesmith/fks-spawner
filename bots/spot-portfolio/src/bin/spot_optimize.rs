//! Grid-sweep optimizer for the spot rebalancer's tuning knobs.
//!
//! Loads the live `spot-portfolio.toml` basket, fetches real daily OHLC history
//! for its assets from Kraken's **public** REST (no API keys needed), aligns the
//! closes into one price series, and replays that series through the same pure
//! [`backtest_portfolio`] the tests use — once per `(band, reserve_pct,
//! cooldown)` combination in a grid. It ranks the combos by total return, and
//! also surfaces the best risk-adjusted (Sharpe) and the best drawdown-capped
//! pick, then prints a recommended `max_drawdown_pct` circuit-breaker derived
//! from the winning config's realized drawdown.
//!
//! Read-only: places no orders, needs no credentials. This is a *comparative*
//! tuning aid over one history window — not a promise of live performance.
//!
//! Usage: `spot-optimize [config.toml]` (env `SPOT_CONFIG`, default
//! `spot-portfolio.toml`). Knobs via env:
//!   SWEEP_VENUE          venue block to tune (default: first `[[exchange]]`)
//!   SWEEP_INTERVAL_MINS  candle interval (default 1440 = daily)
//!   SWEEP_FEE_PCT        taker fee per fill (default 0.0026 = Kraken taker)
//!   SWEEP_INITIAL_USD    starting cash (default 1000)
//!   SWEEP_MAX_DD         drawdown cap for the "capped" pick (default 0.20)
//!   SWEEP_LAST_N         only backtest the most recent N bars (default: all) —
//!                        re-run with a smaller N to probe the recent regime

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use exchange_apiws::kraken::KrakenRestClient;
use serde_json::Value;

use spot_portfolio::spot::backtest::{BacktestParams, BacktestResult, backtest_portfolio};
use spot_portfolio::spot::config::PortfolioConfig;
use spot_portfolio::spot::rebalance::Target;

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Kraken public-OHLC pair altname for a basket asset (BTC is **XBT** there).
fn kraken_pair(asset: &str) -> String {
    match asset.to_ascii_uppercase().as_str() {
        "BTC" | "XBT" => "XBTUSD".to_string(),
        other => format!("{other}USD"),
    }
}

/// Extract `{unix_time → close}` from a Kraken `get_ohlc` response. The pair
/// array elements are `[time, open, high, low, close, vwap, volume, count]`
/// with the numeric fields serialized as strings. The **last** element is the
/// current, still-forming candle (its close is the live mid-interval price, not
/// a settled close), so it's dropped — otherwise the final bar of the backtest
/// would be scored against an incomplete candle.
fn closes_by_time(resp: &Value) -> Result<HashMap<i64, f64>> {
    let obj = resp.as_object().context("OHLC response is not an object")?;
    let arr = obj
        .iter()
        .find(|(k, _)| *k != "last")
        .map(|(_, v)| v)
        .and_then(Value::as_array)
        .context("OHLC response has no pair array")?;
    // Drop the trailing forming candle (keep only settled bars).
    let settled = &arr[..arr.len().saturating_sub(1)];
    let mut out = HashMap::with_capacity(settled.len());
    for row in settled {
        let cols = row.as_array().context("OHLC row is not an array")?;
        let time = cols
            .first()
            .and_then(Value::as_i64)
            .context("OHLC row missing time")?;
        let close = cols
            .get(4)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .context("OHLC row missing close")?;
        out.insert(time, close);
    }
    Ok(out)
}

/// Fetch every asset's close history and align on the timestamps common to all,
/// oldest first, so each bar is a complete `{asset → price}` snapshot.
async fn fetch_aligned(
    client: &KrakenRestClient,
    assets: &[String],
    interval_mins: u32,
) -> Result<Vec<HashMap<String, f64>>> {
    let mut per_asset: HashMap<String, HashMap<i64, f64>> = HashMap::new();
    let mut common: Option<std::collections::BTreeSet<i64>> = None;
    for asset in assets {
        let pair = kraken_pair(asset);
        let resp = client
            .get_ohlc(&pair, interval_mins)
            .await
            .with_context(|| format!("fetching {pair} OHLC"))?;
        let closes = closes_by_time(&resp).with_context(|| format!("parsing {pair} OHLC"))?;
        let times: std::collections::BTreeSet<i64> = closes.keys().copied().collect();
        common = Some(match common {
            None => times,
            Some(prev) => prev.intersection(&times).copied().collect(),
        });
        per_asset.insert(asset.clone(), closes);
    }
    let common = common.unwrap_or_default();
    let mut series = Vec::with_capacity(common.len());
    for t in common {
        let snap: HashMap<String, f64> = assets
            .iter()
            .map(|a| (a.clone(), per_asset[a][&t]))
            .collect();
        series.push(snap);
    }
    Ok(series)
}

/// One grid cell: the tried params plus the result of backtesting them.
struct Cell {
    band: f64,
    reserve_pct: f64,
    cooldown_bars: usize,
    result: BacktestResult,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    let path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SPOT_CONFIG").ok())
        .unwrap_or_else(|| "spot-portfolio.toml".to_string());
    let cfg = PortfolioConfig::load(&path).with_context(|| format!("loading {path}"))?;

    // Pick the venue block to tune (named, else the first).
    let venue = std::env::var("SWEEP_VENUE").ok();
    let ex = match &venue {
        Some(v) => cfg
            .exchanges
            .iter()
            .find(|e| e.name.eq_ignore_ascii_case(v))
            .with_context(|| format!("no [[exchange]] named {v}"))?,
        None => cfg.exchanges.first().context("config has no exchanges")?,
    };

    // Normalize the basket into rebalance Targets (weights need not sum to 1).
    let targets: Vec<Target> = ex
        .targets
        .iter()
        .map(|(name, weight)| Target {
            name: name.clone(),
            weight: *weight,
        })
        .collect();
    if targets.is_empty() {
        bail!("venue {} has no target assets", ex.name);
    }
    let assets: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();

    let interval_mins = env_u32("SWEEP_INTERVAL_MINS", 1440);
    let fee_pct = env_f64("SWEEP_FEE_PCT", 0.0026);
    let initial_usd = env_f64("SWEEP_INITIAL_USD", 1000.0);
    let dd_cap = env_f64("SWEEP_MAX_DD", 0.20);
    let bars_per_year = 1440.0 / interval_mins as f64 * 365.0;

    println!(
        "tuning venue '{}' basket [{}] @ {}m candles (fee {:.2}%, start ${:.0})",
        ex.name,
        assets.join(", "),
        interval_mins,
        fee_pct * 100.0,
        initial_usd,
    );

    let client = KrakenRestClient::new().context("building Kraken public client")?;
    let mut series = fetch_aligned(&client, &assets, interval_mins)
        .await
        .context("fetching aligned price history")?;
    // Optionally restrict to the most recent N bars to probe a recent regime.
    if let Some(n) = std::env::var("SWEEP_LAST_N")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        && n < series.len()
    {
        series.drain(0..series.len() - n);
    }
    if series.len() < 30 {
        bail!(
            "only {} aligned bars — too little history to tune (need ≥30)",
            series.len()
        );
    }
    let span_days = series.len() as f64 * interval_mins as f64 / 1440.0;
    println!(
        "fetched {} aligned bars (~{:.0} days)\n",
        series.len(),
        span_days
    );

    // The sweep grid. Overridable ranges could come from env; these cover the
    // sane operating envelope for a drift rebalancer.
    let bands = [0.10, 0.15, 0.20, 0.25, 0.30, 0.40];
    let reserves = [0.10, 0.15, 0.20, 0.25, 0.30];
    let cooldowns = [1usize, 2, 3, 5];

    let mut cells: Vec<Cell> = Vec::with_capacity(bands.len() * reserves.len() * cooldowns.len());
    for &band in &bands {
        for &reserve_pct in &reserves {
            for &cooldown_bars in &cooldowns {
                let params = BacktestParams {
                    reserve_pct,
                    band,
                    cooldown_bars,
                    min_trade_usd: ex.min_trade_usd,
                    fee_pct,
                    bars_per_year,
                };
                let result = backtest_portfolio(&series, &targets, initial_usd, &params);
                cells.push(Cell {
                    band,
                    reserve_pct,
                    cooldown_bars,
                    result,
                });
            }
        }
    }

    // Rank by total return (desc) for the main table.
    cells.sort_by(|a, b| {
        b.result
            .total_return
            .partial_cmp(&a.result.total_return)
            .unwrap()
    });

    println!("── top 10 by total return ─────────────────────────────────────────────");
    println!("  band  reserve  cooldown(bars) | result");
    for c in cells.iter().take(10) {
        println!(
            "  {:>4.0}%  {:>5.0}%  {:>9}      | {}",
            c.band * 100.0,
            c.reserve_pct * 100.0,
            c.cooldown_bars,
            c.result.summary_line(),
        );
    }

    let best_return = &cells[0];
    let best_sharpe = cells
        .iter()
        .max_by(|a, b| a.result.sharpe.partial_cmp(&b.result.sharpe).unwrap())
        .unwrap();
    let best_capped = cells
        .iter()
        .filter(|c| c.result.max_drawdown <= dd_cap)
        .max_by(|a, b| {
            a.result
                .total_return
                .partial_cmp(&b.result.total_return)
                .unwrap()
        });

    let show = |label: &str, c: &Cell| {
        println!(
            "  {label:<24} band {:.0}%  reserve {:.0}%  cooldown {} bars  → {}",
            c.band * 100.0,
            c.reserve_pct * 100.0,
            c.cooldown_bars,
            c.result.summary_line(),
        );
    };
    println!("\n── recommendations ────────────────────────────────────────────────────");
    show("best total return", best_return);
    show("best sharpe", best_sharpe);
    match best_capped {
        Some(c) => show(&format!("best return, DD ≤ {:.0}%", dd_cap * 100.0), c),
        None => println!("  (no config kept drawdown under {:.0}%)", dd_cap * 100.0),
    }

    // A circuit-breaker sized above the winning config's realized drawdown, so it
    // catches a runaway/bug loss rather than normal market drawdown. Round up to
    // the next 5% and clamp into a sane band.
    let realized_dd = best_capped.unwrap_or(best_return).result.max_drawdown;
    let suggested = ((realized_dd * 1.6 / 0.05).ceil() * 0.05).clamp(0.10, 0.60);
    println!(
        "\n  suggested max_drawdown_pct ≈ {:.2}  (realized DD {:.1}% × 1.6, rounded)",
        suggested,
        realized_dd * 100.0,
    );
    println!(
        "  mapping to live config: band {{:.0}}% → band=<frac>, reserve {{:.0}}% →\n\
         \x20 reserve_pct=<frac>, cooldown N bars → cooldown_secs = N × {interval_mins}×60 secs\n\
         \x20 (NOT N seconds).\n\
         \x20 note: single-window sweep — re-run across regimes (SWEEP_INTERVAL_MINS,\n\
         \x20 different windows) before trusting a value in live config."
    );
    Ok(())
}
