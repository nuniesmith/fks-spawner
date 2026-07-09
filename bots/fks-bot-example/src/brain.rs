//! `HeartbeatBrain` — generates synthetic signals and trades for demo
//! purposes so the `fks_bot_*` metric series have non-zero values.
//!
//! The pattern:
//!   - Every event increments an internal counter.
//!   - Every `signal_every` events emits a non-Hold decision (alternating
//!     buy/sell) and bumps `fks_bot_signals_total`.
//!   - Every `trade_every` events records a synthetic trade with a
//!     normally-distributed P&L drawn from `pnl_mean ± pnl_std`, which
//!     bumps `fks_bot_trades_total`, adjusts `fks_bot_pnl_dollars`, and
//!     updates `fks_bot_win_rate`.
//!
//! Production brains will replace this with real strategy logic, but the
//! shape — `Brain::on_event` returning a `Decision`, with side effects on
//! the metric module — is the same.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rand::RngExt;
use rustrade::{Brain, BrainHealth, Decision, MarketDataEvent, Position, Result};

use crate::metrics;

/// Configuration knobs for the heartbeat brain. All defaults pick values
/// that produce a noticeable counter rate at the spawner's 30-second
/// scrape interval (≈ 1 signal / 6 events × ~1 event/sec ≈ 10 signals/min).
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Emit a non-Hold decision every N events. Lower = more signals.
    pub signal_every: u64,
    /// Record a synthetic trade every N events. Lower = more trades.
    pub trade_every: u64,
    /// Mean P&L per synthetic trade (in account currency).
    pub pnl_mean: f64,
    /// Standard deviation of P&L per synthetic trade.
    pub pnl_std: f64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            signal_every: 6,
            trade_every: 20,
            pnl_mean: 1.0,
            pnl_std: 5.0,
        }
    }
}

/// A `Brain` impl that emits synthetic signals + trades on a deterministic
/// cadence so the example image produces all five `fks_bot_*` series at
/// realistic values.
pub struct HeartbeatBrain {
    name: String,
    config: HeartbeatConfig,
    events_seen: AtomicU64,
    non_hold_decisions: AtomicU64,
}

impl HeartbeatBrain {
    /// Build a new brain with the given configuration.
    pub fn new(name: impl Into<String>, config: HeartbeatConfig) -> Self {
        Self {
            name: name.into(),
            config,
            events_seen: AtomicU64::new(0),
            non_hold_decisions: AtomicU64::new(0),
        }
    }

    fn maybe_record_trade(&self, n: u64) {
        if self.config.trade_every == 0 || !n.is_multiple_of(self.config.trade_every) {
            return;
        }
        // Sample from N(pnl_mean, pnl_std) using the central-limit hack —
        // sum 12 uniforms and subtract 6, which is good enough for a demo
        // and avoids dragging in `rand_distr` for one call site.
        let mut rng = rand::rng();
        let u: f64 = (0..12).map(|_| rng.random::<f64>()).sum::<f64>() - 6.0;
        let pnl = self.config.pnl_mean + self.config.pnl_std * u;

        metrics::record_trade(pnl);

        tracing::info!(event_count = n, pnl = pnl, "synthetic trade recorded");
    }
}

#[async_trait]
impl Brain for HeartbeatBrain {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_event(&self, event: &MarketDataEvent, _position: &Position) -> Result<Decision> {
        let n = self.events_seen.fetch_add(1, Ordering::Relaxed) + 1;

        // Synthetic trade (independent of the signal cadence so we keep
        // the math simple — every brain decides differently anyway).
        self.maybe_record_trade(n);

        // Decide whether to emit a signal.
        if self.config.signal_every == 0 || !n.is_multiple_of(self.config.signal_every) {
            return Ok(Decision::hold());
        }

        let prev = self.non_hold_decisions.fetch_add(1, Ordering::Relaxed);
        metrics::record_signal();

        // Alternate buy/sell so neither side gets stuck and the position
        // logic in the framework can exercise both directions.
        let decision = if prev.is_multiple_of(2) {
            Decision::buy(0.6)
        } else {
            Decision::sell(0.6)
        };

        tracing::debug!(
            symbol = %event.symbol(),
            decision = ?decision.signal,
            event_count = n,
            "signal emitted",
        );
        Ok(decision)
    }

    async fn health(&self) -> BrainHealth {
        BrainHealth {
            healthy: true,
            events_processed: self.events_seen.load(Ordering::Relaxed),
            non_hold_decisions: self.non_hold_decisions.load(Ordering::Relaxed),
            details: serde_json::json!({
                "kind": "heartbeat",
                "signal_every": self.config.signal_every,
                "trade_every": self.config.trade_every,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rustrade::{MarketDataEvent, Position, Price, Symbol, Tick, Volume};

    fn fake_event() -> MarketDataEvent {
        MarketDataEvent::Ticker {
            exchange: "test".into(),
            symbol: Symbol::from("BTCUSDT"),
            tick: Tick {
                symbol: Symbol::from("BTCUSDT"),
                timestamp: Utc::now(),
                bid: Price(100.0),
                ask: Price(100.1),
                bid_size: Volume(1.0),
                ask_size: Volume(1.0),
                last_price: Some(Price(100.05)),
                last_size: Some(Volume(0.1)),
            },
        }
    }

    #[tokio::test]
    async fn signal_cadence_matches_config() {
        let brain = HeartbeatBrain::new(
            "test",
            HeartbeatConfig {
                signal_every: 3,
                trade_every: 0, // disable trades for this test
                pnl_mean: 0.0,
                pnl_std: 0.0,
            },
        );

        let event = fake_event();
        let pos = Position::FLAT;

        // First two events should hold; third should signal.
        let d1 = brain.on_event(&event, &pos).await.unwrap();
        let d2 = brain.on_event(&event, &pos).await.unwrap();
        let d3 = brain.on_event(&event, &pos).await.unwrap();

        assert!(matches!(d1.signal, rustrade::SignalType::Hold));
        assert!(matches!(d2.signal, rustrade::SignalType::Hold));
        assert!(!matches!(d3.signal, rustrade::SignalType::Hold));
    }

    #[tokio::test]
    async fn health_reports_event_counts() {
        let brain = HeartbeatBrain::new("test", HeartbeatConfig::default());
        let event = fake_event();
        let pos = Position::FLAT;
        for _ in 0..5 {
            let _ = brain.on_event(&event, &pos).await.unwrap();
        }
        let h = brain.health().await;
        assert!(h.healthy);
        assert_eq!(h.events_processed, 5);
    }
}
