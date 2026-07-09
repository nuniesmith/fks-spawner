//! `EmaCrossBrain` — a real (if simple) strategy built on `indicators-ta`.
//!
//! Demonstrates the canonical rustrade `Brain` pattern with live indicator
//! state behind a `Mutex` (the same pattern documented in rustrade's
//! `docs/writing-a-brain.md`):
//!
//! - Maintains per-symbol incremental `EMA` (fast + slow) and `ATR` from
//!   `indicators-ta`, updated on every closed candle.
//! - Emits a **Buy** when the fast EMA crosses above the slow EMA, **Sell**
//!   on the reverse cross, and otherwise **Hold**.
//! - Attaches an ATR-based stop to each entry and defers sizing to the
//!   framework's risk layer (the brain decides direction, not size).
//!
//! This is intentionally lighter than the kucoin SAR stack — it's a reference
//! that exercises the framework + indicators-ta cleanly, not production IP.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use indicators::{ATR, EMA};
use rustrade::{Brain, BrainHealth, Decision, MarketDataEvent, Position, Price, Result};

use crate::metrics;

/// Tunables for the EMA-cross strategy.
#[derive(Debug, Clone)]
pub struct EmaCrossConfig {
    pub fast_period: usize,
    pub slow_period: usize,
    pub atr_period: usize,
    /// Stop distance as a multiple of ATR.
    pub stop_atr_mult: f64,
}

impl Default for EmaCrossConfig {
    fn default() -> Self {
        Self {
            fast_period: 9,
            slow_period: 21,
            atr_period: 14,
            stop_atr_mult: 2.0,
        }
    }
}

/// Per-symbol incremental indicator state + last cross direction.
struct SymbolState {
    fast: EMA,
    slow: EMA,
    atr: ATR,
    /// Sign of (fast - slow) on the previous ready candle: +1, -1, or 0.
    last_side: i8,
}

impl SymbolState {
    fn new(cfg: &EmaCrossConfig) -> Self {
        Self {
            fast: EMA::new(cfg.fast_period),
            slow: EMA::new(cfg.slow_period),
            atr: ATR::new(cfg.atr_period),
            last_side: 0,
        }
    }
}

/// EMA-cross brain. One indicator set per symbol, created lazily on first
/// candle so the brain handles any symbol the bot feeds it.
pub struct EmaCrossBrain {
    name: String,
    cfg: EmaCrossConfig,
    state: Mutex<HashMap<String, SymbolState>>,
    events: Mutex<u64>,
    signals: Mutex<u64>,
}

impl EmaCrossBrain {
    pub fn new(name: impl Into<String>, cfg: EmaCrossConfig) -> Self {
        Self {
            name: name.into(),
            cfg,
            state: Mutex::new(HashMap::new()),
            events: Mutex::new(0),
            signals: Mutex::new(0),
        }
    }
}

#[async_trait]
impl Brain for EmaCrossBrain {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> Result<Decision> {
        // Only react to closed candles.
        let (symbol, candle) = match event {
            MarketDataEvent::Candle { symbol, candle, .. } => (symbol, candle),
            _ => return Ok(Decision::hold()),
        };

        *self.events.lock().unwrap() += 1;

        let mut map = self.state.lock().unwrap();
        let st = map
            .entry(symbol.0.clone())
            .or_insert_with(|| SymbolState::new(&self.cfg));

        st.fast.update(candle.close);
        st.slow.update(candle.close);
        st.atr.update(candle.high, candle.low, candle.close);

        // Need both EMAs warm before trading.
        if !st.fast.is_ready() || !st.slow.is_ready() {
            return Ok(Decision::hold());
        }

        let spread = st.fast.value() - st.slow.value();
        let side = if spread > 0.0 { 1i8 } else { -1i8 };
        let crossed = st.last_side != 0 && side != st.last_side;
        let prev_side = st.last_side;
        st.last_side = side;

        // No cross → hold (or stay in position).
        if !crossed {
            return Ok(Decision::hold());
        }

        // ATR-based stop distance (falls back to a tiny % if ATR not ready).
        let atr = if st.atr.is_ready() {
            st.atr.value()
        } else {
            candle.close * 0.01
        };
        let stop_dist = atr * self.cfg.stop_atr_mult;

        metrics::record_signal();
        *self.signals.lock().unwrap() += 1;

        // A reversed Buy/Sell while holding the opposite position is handled
        // as a stop-and-reverse by the framework's execution layer. `position`
        // is informational here (qty != 0.0 ⇒ already in a trade).
        let in_position = !position.is_flat();

        let decision = if side > 0 {
            // Fast crossed above slow → go/stay long.
            tracing::info!(
                symbol = %symbol.0, close = candle.close, atr,
                prev_side, in_position, "EMA cross UP → buy"
            );
            // No size hint — defer to the framework's SizingConfig (a rustrade
            // invariant: the brain decides direction, the risk layer sizes).
            Decision::buy(0.7).with_stop(Price(candle.close - stop_dist))
        } else {
            tracing::info!(
                symbol = %symbol.0, close = candle.close, atr,
                prev_side, in_position, "EMA cross DOWN → sell"
            );
            Decision::sell(0.7).with_stop(Price(candle.close + stop_dist))
        };

        let _ = in_position; // informational only; reversal handled downstream
        Ok(decision)
    }

    async fn health(&self) -> BrainHealth {
        let events = *self.events.lock().unwrap();
        let signals = *self.signals.lock().unwrap();
        BrainHealth {
            healthy: true,
            events_processed: events,
            non_hold_decisions: signals,
            details: serde_json::json!({
                "kind": "ema-cross",
                "fast": self.cfg.fast_period,
                "slow": self.cfg.slow_period,
                "symbols_tracked": self.state.lock().unwrap().len(),
            }),
        }
    }
}
