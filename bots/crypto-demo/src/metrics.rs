//! Prometheus metrics — implements the `fks_bot_*` contract that the
//! FKS spawner's Prometheus `file_sd_configs` job expects to scrape.
//!
//! The contract is documented in
//! `infrastructure/config/prometheus/prometheus.yml` under the `fks-bots`
//! job. Every bot container that wants to be discovered + scraped must
//! expose **all five** of the series below at `:9091/metrics`.
//!
//! Series                    | Type    | Range  | Meaning
//! --------------------------|---------|--------|---------------------------
//! `fks_bot_pnl_dollars`     | gauge   | any    | Current cumulative P&L
//! `fks_bot_signals_total`   | counter | 0..    | Total signals emitted
//! `fks_bot_trades_total`    | counter | 0..    | Total trades executed
//! `fks_bot_win_rate`        | gauge   | 0..1   | wins / total_trades
//! `fks_bot_uptime_seconds`  | gauge   | 0..    | Seconds since process start
//!
//! All metrics are labeled with the `fks.bot_id` label that the spawner
//! sets on the SD target — Prometheus' `file_sd_configs` lift those into
//! per-target labels automatically, so the exposition itself is unlabelled.

use std::sync::Arc;
use std::time::Instant;

use once_cell::sync::Lazy;
use prometheus::{Counter, Encoder, Gauge, TextEncoder, register_counter, register_gauge};

/// Cumulative P&L in account currency, signed.
pub static PNL_DOLLARS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_bot_pnl_dollars",
        "Current cumulative P&L in account currency"
    )
    .expect("metric registration failed")
});

/// Total signals (non-Hold decisions) emitted since startup.
pub static SIGNALS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_bot_signals_total",
        "Total signals emitted since startup"
    )
    .expect("metric registration failed")
});

/// Total trades executed since startup.
pub static TRADES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_bot_trades_total",
        "Total trades executed since startup"
    )
    .expect("metric registration failed")
});

/// Win rate (wins / total trades) in [0.0, 1.0]. Set explicitly by the bot
/// after each trade settles; default 0.0 before any trades.
pub static WIN_RATE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_bot_win_rate",
        "Cumulative win rate (wins / total trades)"
    )
    .expect("metric registration failed")
});

/// Seconds since process start. Updated by a background task so it stays
/// accurate even when the bot isn't actively trading.
pub static UPTIME_SECONDS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_bot_uptime_seconds",
        "Seconds since the bot process started"
    )
    .expect("metric registration failed")
});

/// Tracks `wins` / `total_trades` so [`record_trade`] can keep
/// `WIN_RATE` accurate without a re-derivation pass.
#[derive(Default)]
struct WinTracker {
    wins: u64,
    total: u64,
}

static WIN_TRACKER: Lazy<std::sync::Mutex<WinTracker>> =
    Lazy::new(|| std::sync::Mutex::new(WinTracker::default()));

/// Record a closed trade. Bumps `fks_bot_trades_total`, adjusts
/// `fks_bot_pnl_dollars` by `pnl`, and updates `fks_bot_win_rate` based on
/// whether `pnl > 0`. Call once per trade.
pub fn record_trade(pnl: f64) {
    TRADES_TOTAL.inc();
    PNL_DOLLARS.add(pnl);

    let mut t = WIN_TRACKER.lock().expect("WinTracker mutex poisoned");
    t.total += 1;
    if pnl > 0.0 {
        t.wins += 1;
    }
    let rate = if t.total > 0 {
        t.wins as f64 / t.total as f64
    } else {
        0.0
    };
    WIN_RATE.set(rate);
}

/// Record a non-Hold signal. Bumps `fks_bot_signals_total`.
pub fn record_signal() {
    SIGNALS_TOTAL.inc();
}

/// Render all metrics in Prometheus text exposition format.
pub fn render() -> String {
    // Touch each Lazy so the registry knows about them before first scrape.
    let _ = &*PNL_DOLLARS;
    let _ = &*SIGNALS_TOTAL;
    let _ = &*TRADES_TOTAL;
    let _ = &*WIN_RATE;
    let _ = &*UPTIME_SECONDS;

    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}

/// Background task that updates `fks_bot_uptime_seconds` once per second.
/// Spawn this once at startup. Pass an `Arc<Instant>` of the process-start
/// time so multiple subsystems can share the reference.
pub async fn uptime_loop(start: Arc<Instant>) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        UPTIME_SECONDS.set(start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_trade_updates_all_three_metrics() {
        // These tests share a global registry; running serially is fine
        // because we assert deltas, not absolute values.
        let trades_before = TRADES_TOTAL.get();
        let pnl_before = PNL_DOLLARS.get();

        record_trade(12.34);
        record_trade(-4.56);
        record_trade(7.89);

        assert!((TRADES_TOTAL.get() - trades_before - 3.0).abs() < 1e-9);
        // pnl: 12.34 - 4.56 + 7.89 = 15.67
        assert!((PNL_DOLLARS.get() - pnl_before - 15.67).abs() < 1e-6);

        // win_rate is global state — we only assert it's between 0 and 1.
        let wr = WIN_RATE.get();
        assert!((0.0..=1.0).contains(&wr), "win_rate out of range: {wr}");
    }

    #[test]
    fn render_includes_all_required_series() {
        let text = render();
        assert!(text.contains("fks_bot_pnl_dollars"), "missing pnl");
        assert!(text.contains("fks_bot_signals_total"), "missing signals");
        assert!(text.contains("fks_bot_trades_total"), "missing trades");
        assert!(text.contains("fks_bot_win_rate"), "missing win_rate");
        assert!(text.contains("fks_bot_uptime_seconds"), "missing uptime");
    }
}
