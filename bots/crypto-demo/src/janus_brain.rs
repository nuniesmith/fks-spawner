//! `JanusBrain` — a `rustrade::Brain` that delegates the decision to **janus**.
//!
//! This is the janus ↔ rustrade tie-in: instead of deciding locally (like
//! [`crate::brain::EmaCrossBrain`]), this brain computes indicator features
//! from `indicators-ta`, POSTs them to janus's signal API, and maps janus's
//! reply onto a rustrade [`Decision`]. The framework wiring around it
//! (candle pollers, supervisor, risk gate, paper exchange, metrics) is
//! identical — only the brain changes.
//!
//! **v2 — risk-verdict aware.** Beyond the directional signal, the brain routes
//! it through janus's **risk engine** instead of acting on the raw signal:
//!
//! ```text
//!   candle ─► JanusBrain ─(EMA/ATR)─► POST /api/v1/signals/generate ─► direction+conf
//!                                                  │
//!                 POST /api/v1/risk/validate  ◄────┤   (veto? → Hold)
//!                 POST /api/v1/risk/calculate/position-size  ◄─ (risk-sized quantity)
//!                                                  ▼
//!                   rustrade::Decision { side, SizeHint::Quantity(janus qty), stop, tp }
//!
//!   on_fill ─► POST /api/v1/risk/portfolio/positions        (open exposure)
//!          └─► POST /api/v1/risk/portfolio/positions/close  (realised PnL on a closing fill)
//! ```
//!
//! So janus's risk layer is the authority on **whether** to trade (validate) and
//! **how big** (position-size), and it sees both the open exposure *and* the
//! realised PnL when a trade closes — the brain mirrors each symbol's position
//! from the fill stream to compute that figure. Toggle with
//! [`JanusBrainConfig::use_risk_engine`].
//!
//! Resilience: every janus call **fails open** — if the signal or risk API is
//! unreachable the brain holds or falls back to framework sizing (never throws),
//! so a long demo run survives janus being down or partial. Set `JANUS_HTTP_URL`
//! to the forward service (default `http://localhost:8080`).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use indicators::{ATR, EMA};
use rustrade::{
    Brain, BrainHealth, Decision, Fill, MarketDataEvent, Position, Price, Result, SizeHint, Volume,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::metrics;

// ── Wire types (mirror janus services/forward/src/api/server.rs) ──────────────

/// Request body for `POST /api/v1/signals/generate`.
#[derive(Debug, Serialize)]
struct GenerateSignalRequest {
    symbol: String,
    timeframe: String,
    analysis: IndicatorAnalysisDto,
    current_price: f64,
    enable_ml: bool,
}

/// Indicator features janus consumes. Field names match janus exactly.
#[derive(Debug, Default, Serialize)]
struct IndicatorAnalysisDto {
    ema_fast: Option<f64>,
    ema_slow: Option<f64>,
    ema_cross: f64,
    rsi: Option<f64>,
    rsi_signal: f64,
    macd_line: Option<f64>,
    macd_signal: Option<f64>,
    macd_histogram: Option<f64>,
    macd_cross: f64,
    bb_upper: Option<f64>,
    bb_middle: Option<f64>,
    bb_lower: Option<f64>,
    bb_position: f64,
    atr: Option<f64>,
    trend_strength: f64,
    volatility: f64,
}

/// Response body from `/api/v1/signals/generate`.
#[derive(Debug, Deserialize)]
struct SignalResponse {
    signal: Option<TradingSignalDto>,
    #[serde(default)]
    filtered: bool,
    #[serde(default)]
    processing_time_ms: f64,
}

#[derive(Debug, Deserialize)]
struct TradingSignalDto {
    /// janus serialises this via `{:?}` → "StrongBuy" / "Buy" / "Hold" /
    /// "Sell" / "StrongSell". We also accept the SCREAMING_SNAKE forms.
    signal_type: String,
    #[serde(default)]
    confidence: f64,
    #[serde(default)]
    stop_loss: Option<f64>,
    #[serde(default)]
    take_profit: Option<f64>,
}

/// Normalised janus decision direction.
#[derive(Clone, Copy, PartialEq)]
enum JanusSide {
    Buy,
    Sell,
    Hold,
}

fn parse_signal_type(s: &str) -> JanusSide {
    match s.to_ascii_lowercase().replace(['_', '-'], "").as_str() {
        "buy" | "strongbuy" => JanusSide::Buy,
        "sell" | "strongsell" => JanusSide::Sell,
        _ => JanusSide::Hold,
    }
}

// ── Risk-API wire types (mirror janus services/forward/src/api/risk_rest.rs) ──
// v2: instead of acting on the raw signal, the brain asks janus's risk engine
// to *validate* the signal and *size* the position, then reports fills back so
// janus's portfolio + affinity stay current.

#[derive(Debug, Serialize)]
struct SignalDto {
    symbol: String,
    signal_type: String, // "Buy" / "Sell" / "Hold"
    timeframe: String,
    confidence: f64,
    entry_price: Option<f64>,
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
}

#[derive(Debug, Serialize)]
struct MarketDataDto {
    current_price: f64,
    atr: Option<f64>,
    support: Option<f64>,
    resistance: Option<f64>,
    volatility: Option<f64>,
    recent_high: Option<f64>,
    recent_low: Option<f64>,
}

#[derive(Debug, Serialize)]
struct PositionDto {
    symbol: String,
    entry_price: f64,
    quantity: f64,
    side: String, // "Long" / "Short"
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    position_value: f64,
    risk_amount: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ValidateSignalRequest {
    signal: SignalDto,
}

#[derive(Debug, Deserialize)]
struct ValidateSignalResponse {
    is_valid: bool,
    #[serde(default)]
    validation_errors: Vec<String>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CalculatePositionSizeRequest {
    signal: SignalDto,
    market_data: MarketDataDto,
    sizing_method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PositionSizeResponse {
    quantity: f64,
    #[serde(default)]
    position_value: f64,
    #[serde(default)]
    risk_amount: f64,
}

#[derive(Debug, Serialize)]
struct AddPositionRequest {
    position: PositionDto,
}

/// Close (outcome) report — posted when a fill reduces/closes a position so
/// janus folds the realised PnL into its portfolio. Matches janus's
/// `ClosePositionRequest` (`POST /api/v1/risk/portfolio/positions/close`).
#[derive(Debug, Serialize)]
struct ClosePositionDto {
    symbol: String,
    realized_pnl: f64,
    exit_price: Option<f64>,
    quantity: Option<f64>,
    side: Option<String>,
    reason: Option<String>,
}

// ── Local position mirror (for realised-PnL on close) ─────────────────────────

/// A bot-side mirror of a symbol's position, maintained from the fill stream.
/// The framework computes realised PnL internally but doesn't expose it to a
/// `Brain`, so the brain reconstructs it here to report trade *outcomes*.
#[derive(Clone, Copy, Default)]
struct LocalPos {
    /// Signed quantity: positive = long, negative = short.
    qty: f64,
    /// Volume-weighted average entry price of the currently-open position.
    avg_entry: f64,
}

/// Apply a fill to a local position. Returns the new position and, when the fill
/// **reduced or closed** it, the realised PnL of the closed portion (gross of
/// fees). Mirrors the framework's own reduce/flip accounting.
fn apply_fill(
    prior: LocalPos,
    side: rustrade::Side,
    price: f64,
    size: f64,
) -> (LocalPos, Option<f64>) {
    let signed = match side {
        rustrade::Side::Buy => size,
        rustrade::Side::Sell => -size,
    };
    // Opening from flat or adding in the same direction: no realised PnL; roll
    // the volume-weighted average entry forward.
    if prior.qty == 0.0 || prior.qty.signum() == signed.signum() {
        let abs_prior = prior.qty.abs();
        let avg_entry = if abs_prior == 0.0 {
            price
        } else {
            (prior.avg_entry * abs_prior + price * size) / (abs_prior + size)
        };
        return (
            LocalPos {
                qty: prior.qty + signed,
                avg_entry,
            },
            None,
        );
    }
    // Reducing, closing, or flipping: realise PnL on the closed quantity.
    let closed_qty = prior.qty.abs().min(size);
    let direction = prior.qty.signum(); // +1 closing a long, -1 closing a short
    let realised = (price - prior.avg_entry) * direction * closed_qty;
    let new_qty = prior.qty + signed;
    let avg_entry = if new_qty == 0.0 {
        0.0
    } else if new_qty.signum() == prior.qty.signum() {
        prior.avg_entry // partial reduce — same side, entry unchanged
    } else {
        price // flipped past flat — the remainder opens at the fill price
    };
    (
        LocalPos {
            qty: new_qty,
            avg_entry,
        },
        Some(realised),
    )
}

/// The fill's side as a label for the close report (the action that reduced the
/// position).
fn side_label(side: rustrade::Side) -> &'static str {
    match side {
        rustrade::Side::Buy => "Buy",
        rustrade::Side::Sell => "Sell",
    }
}

// ── Per-symbol indicator state ────────────────────────────────────────────────

struct SymbolState {
    fast: EMA,
    slow: EMA,
    atr: ATR,
    /// Last side janus advised, to avoid re-emitting the same direction.
    last_side: JanusSide,
}

impl SymbolState {
    fn new(cfg: &JanusBrainConfig) -> Self {
        Self {
            fast: EMA::new(cfg.fast_period),
            slow: EMA::new(cfg.slow_period),
            atr: ATR::new(cfg.atr_period),
            last_side: JanusSide::Hold,
        }
    }
}

/// Configuration for the janus-backed brain.
#[derive(Debug, Clone)]
pub struct JanusBrainConfig {
    pub fast_period: usize,
    pub slow_period: usize,
    pub atr_period: usize,
    pub timeframe: String,
    /// Minimum confidence janus must report before we act.
    pub min_confidence: f64,
    /// v2: route the directional signal through janus's **risk engine**
    /// (`/risk/validate` + `/risk/calculate/position-size`) and report fills to
    /// its portfolio (`/risk/portfolio/positions`). When janus's risk API is
    /// unreachable the brain degrades gracefully to v1 behaviour (direct sizing
    /// + the signal's own stop), so a demo run survives janus being partial.
    pub use_risk_engine: bool,
}

impl Default for JanusBrainConfig {
    fn default() -> Self {
        Self {
            fast_period: 9,
            slow_period: 21,
            atr_period: 14,
            timeframe: "1m".into(),
            min_confidence: 0.5,
            use_risk_engine: true,
        }
    }
}

/// A brain that asks janus for the decision on each closed candle.
pub struct JanusBrain {
    name: String,
    cfg: JanusBrainConfig,
    base_url: String,
    http: reqwest::Client,
    state: Mutex<HashMap<String, SymbolState>>,
    /// Per-symbol position mirror, for computing realised PnL on a closing fill.
    positions: Mutex<HashMap<String, LocalPos>>,
    events: Mutex<u64>,
    signals: Mutex<u64>,
    errors: Mutex<u64>,
}

impl JanusBrain {
    /// Build the brain. `base_url` is the janus forward service, e.g.
    /// `http://localhost:8080` (read from `JANUS_HTTP_URL`).
    pub fn new(
        name: impl Into<String>,
        cfg: JanusBrainConfig,
        base_url: impl Into<String>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .expect("reqwest client");
        Self {
            name: name.into(),
            cfg,
            base_url: base_url.into(),
            http,
            state: Mutex::new(HashMap::new()),
            positions: Mutex::new(HashMap::new()),
            events: Mutex::new(0),
            signals: Mutex::new(0),
            errors: Mutex::new(0),
        }
    }

    /// POST the analysis to janus; `Ok(None)` means "no actionable signal".
    async fn ask_janus(
        &self,
        symbol: &str,
        price: f64,
        analysis: IndicatorAnalysisDto,
    ) -> std::result::Result<Option<TradingSignalDto>, reqwest::Error> {
        let url = format!("{}/api/v1/signals/generate", self.base_url);
        let req = GenerateSignalRequest {
            symbol: symbol.to_string(),
            timeframe: self.cfg.timeframe.clone(),
            analysis,
            current_price: price,
            enable_ml: false,
        };
        let resp: SignalResponse = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!(
            symbol,
            filtered = resp.filtered,
            ms = resp.processing_time_ms,
            "janus replied"
        );
        Ok(resp.signal)
    }

    /// Ask janus's risk engine to validate the signal. `Ok(true)` = approved (or
    /// the endpoint is absent/old and returns nothing parseable — fail open so a
    /// missing risk service doesn't silently halt trading).
    async fn risk_validate(&self, signal: &SignalDto) -> std::result::Result<bool, reqwest::Error> {
        let url = format!("{}/api/v1/risk/validate", self.base_url);
        let resp: ValidateSignalResponse = self
            .http
            .post(&url)
            .json(&ValidateSignalRequest {
                signal: signal.clone_shallow(),
            })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !resp.is_valid {
            warn!(
                errors = ?resp.validation_errors,
                warnings = ?resp.warnings,
                "janus risk vetoed the signal"
            );
        }
        Ok(resp.is_valid)
    }

    /// Ask janus's risk engine to size the position. Returns the quantity
    /// (base/contract units) it recommends, or `None` if it declines / errors.
    async fn risk_size(
        &self,
        signal: &SignalDto,
        market: MarketDataDto,
    ) -> std::result::Result<Option<f64>, reqwest::Error> {
        let url = format!("{}/api/v1/risk/calculate/position-size", self.base_url);
        let resp: PositionSizeResponse = self
            .http
            .post(&url)
            .json(&CalculatePositionSizeRequest {
                signal: signal.clone_shallow(),
                market_data: market,
                sizing_method: None,
            })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!(
            quantity = resp.quantity,
            notional = resp.position_value,
            risk = resp.risk_amount,
            "janus risk sized the position"
        );
        Ok((resp.quantity > 0.0).then_some(resp.quantity))
    }

    /// Report an opened position to janus's portfolio so its account-level risk
    /// + affinity learning see it. Best-effort — logged, never fatal.
    async fn report_position(&self, position: PositionDto) {
        let url = format!("{}/api/v1/risk/portfolio/positions", self.base_url);
        if let Err(e) = self
            .http
            .post(&url)
            .json(&AddPositionRequest { position })
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            debug!(error = %e, "janus portfolio position report failed (non-fatal)");
        }
    }

    /// Report a closed trade's outcome (realised PnL) to janus so its portfolio
    /// daily PnL + affinity learning see how the trade resolved. Best-effort.
    async fn report_close(&self, close: ClosePositionDto) {
        let url = format!("{}/api/v1/risk/portfolio/positions/close", self.base_url);
        if let Err(e) = self
            .http
            .post(&url)
            .json(&close)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            debug!(error = %e, "janus portfolio close report failed (non-fatal)");
        }
    }
}

impl SignalDto {
    /// Cheap clone for re-sending the same signal to multiple risk endpoints.
    fn clone_shallow(&self) -> Self {
        Self {
            symbol: self.symbol.clone(),
            signal_type: self.signal_type.clone(),
            timeframe: self.timeframe.clone(),
            confidence: self.confidence,
            entry_price: self.entry_price,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
        }
    }
}

#[async_trait]
impl Brain for JanusBrain {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_event(&self, event: &MarketDataEvent, _position: &Position) -> Result<Decision> {
        let (symbol, candle) = match event {
            MarketDataEvent::Candle { symbol, candle, .. } => (symbol, candle),
            _ => return Ok(Decision::hold()),
        };

        *self.events.lock().unwrap() += 1;

        // Update indicators and snapshot the feature vector under the lock,
        // then release it before the await (HTTP) — Brain::on_event is async
        // and the MutexGuard isn't Send.
        let (analysis, atr_val, prev_side) = {
            let mut map = self.state.lock().unwrap();
            let st = map
                .entry(symbol.0.clone())
                .or_insert_with(|| SymbolState::new(&self.cfg));

            st.fast.update(candle.close);
            st.slow.update(candle.close);
            st.atr.update(candle.high, candle.low, candle.close);

            if !st.fast.is_ready() || !st.slow.is_ready() {
                return Ok(Decision::hold());
            }

            let fast = st.fast.value();
            let slow = st.slow.value();
            let atr = if st.atr.is_ready() {
                st.atr.value()
            } else {
                0.0
            };
            let ema_cross = (fast - slow).signum();

            let analysis = IndicatorAnalysisDto {
                ema_fast: Some(fast),
                ema_slow: Some(slow),
                ema_cross,
                atr: if atr > 0.0 { Some(atr) } else { None },
                // Rough volatility proxy = ATR as a fraction of price.
                volatility: if candle.close > 0.0 {
                    atr / candle.close
                } else {
                    0.0
                },
                trend_strength: ema_cross,
                ..Default::default()
            };
            (analysis, atr, st.last_side)
        };

        // Ask janus. On any transport error, hold (don't kill the run).
        let signal = match self.ask_janus(&symbol.0, candle.close, analysis).await {
            Ok(s) => s,
            Err(e) => {
                *self.errors.lock().unwrap() += 1;
                warn!(symbol = %symbol.0, error = %e, "janus request failed — holding");
                return Ok(Decision::hold());
            }
        };

        let Some(sig) = signal else {
            return Ok(Decision::hold());
        };

        let side = parse_signal_type(&sig.signal_type);
        if side == JanusSide::Hold || sig.confidence < self.cfg.min_confidence {
            return Ok(Decision::hold());
        }
        if side == prev_side {
            return Ok(Decision::hold()); // already on this side
        }

        let conf = sig.confidence.clamp(0.0, 1.0);

        // Prefer janus's own stop; fall back to a 2×ATR stop.
        let stop = sig.stop_loss.unwrap_or_else(|| {
            let dist = if atr_val > 0.0 {
                atr_val * 2.0
            } else {
                candle.close * 0.01
            };
            match side {
                JanusSide::Buy => candle.close - dist,
                _ => candle.close + dist,
            }
        });

        // ── v2: route through janus's risk engine (validate + size) ─────
        // Degrades gracefully: a transport error fails open (don't halt on a
        // blip); a reachable veto holds; sizing falls back to the framework's.
        let mut size_hint = SizeHint::Default;
        if self.cfg.use_risk_engine {
            let signal_dto = SignalDto {
                symbol: symbol.0.clone(),
                signal_type: if side == JanusSide::Buy {
                    "Buy"
                } else {
                    "Sell"
                }
                .into(),
                timeframe: self.cfg.timeframe.clone(),
                confidence: conf,
                entry_price: Some(candle.close),
                stop_loss: Some(stop),
                take_profit: sig.take_profit,
            };
            match self.risk_validate(&signal_dto).await {
                Ok(false) => return Ok(Decision::hold()), // janus risk vetoed
                Ok(true) => {}
                Err(e) => {
                    *self.errors.lock().unwrap() += 1;
                    warn!(error = %e, "risk validate failed — proceeding without veto");
                }
            }
            let market = MarketDataDto {
                current_price: candle.close,
                atr: (atr_val > 0.0).then_some(atr_val),
                volatility: (candle.close > 0.0).then(|| atr_val / candle.close),
                support: None,
                resistance: None,
                recent_high: None,
                recent_low: None,
            };
            match self.risk_size(&signal_dto, market).await {
                Ok(Some(qty)) => size_hint = SizeHint::Quantity(Volume(qty)),
                Ok(None) => {}
                Err(e) => {
                    *self.errors.lock().unwrap() += 1;
                    warn!(error = %e, "risk size failed — default sizing");
                }
            }
        }

        // Record + count only now that the risk engine didn't veto.
        if let Some(st) = self.state.lock().unwrap().get_mut(&symbol.0) {
            st.last_side = side;
        }
        metrics::record_signal();
        *self.signals.lock().unwrap() += 1;

        tracing::info!(
            symbol = %symbol.0, signal = %sig.signal_type, confidence = conf,
            close = candle.close, ?size_hint, "janus signal → risk-checked decision"
        );

        let mut decision = match side {
            JanusSide::Buy => Decision::buy(conf),
            _ => Decision::sell(conf),
        }
        .with_stop(Price(stop))
        .with_size_hint(size_hint);
        if let Some(tp) = sig.take_profit {
            decision = decision.with_take_profit(Price(tp));
        }
        Ok(decision)
    }

    async fn on_fill(&self, fill: &Fill) -> Result<()> {
        // v2: maintain a local position mirror from the fill stream so we can
        // tell janus both the open exposure *and* the realised PnL when a trade
        // closes. Best-effort — janus being down never breaks the bot.
        if !self.cfg.use_risk_engine {
            return Ok(());
        }
        let sym = fill.symbol.as_str().to_string();
        let price = fill.price.value();
        let size = fill.size.value();

        // Update the mirror under the lock, then drop it *before* any await.
        let (new_pos, realised) = {
            let mut positions = self.positions.lock().unwrap();
            let prior = positions.get(&sym).copied().unwrap_or_default();
            let (new_pos, realised) = apply_fill(prior, fill.side, price, size);
            if new_pos.qty == 0.0 {
                positions.remove(&sym);
            } else {
                positions.insert(sym.clone(), new_pos);
            }
            (new_pos, realised)
        };

        // A reducing/closing fill realised PnL — report the outcome.
        if let Some(pnl) = realised {
            self.report_close(ClosePositionDto {
                symbol: sym.clone(),
                realized_pnl: pnl,
                exit_price: Some(price),
                quantity: Some(size),
                side: Some(side_label(fill.side).to_string()),
                reason: Some("fill".to_string()),
            })
            .await;
        }

        // Whatever remains open (added, reduced-but-not-flat, or a flip) is the
        // current exposure — report it so janus tracks the live position.
        if new_pos.qty != 0.0 {
            let (side, qty) = if new_pos.qty > 0.0 {
                ("Long", new_pos.qty)
            } else {
                ("Short", -new_pos.qty)
            };
            self.report_position(PositionDto {
                symbol: sym,
                entry_price: new_pos.avg_entry,
                quantity: qty,
                side: side.to_string(),
                stop_loss: None,
                take_profit: None,
                position_value: new_pos.avg_entry * qty,
                risk_amount: None,
            })
            .await;
        }
        Ok(())
    }

    async fn health(&self) -> BrainHealth {
        let events = *self.events.lock().unwrap();
        let signals = *self.signals.lock().unwrap();
        let errors = *self.errors.lock().unwrap();
        BrainHealth {
            // Unhealthy only if every request so far has failed (janus down).
            healthy: events == 0 || errors < events,
            events_processed: events,
            non_hold_decisions: signals,
            details: serde_json::json!({
                "kind": "janus",
                "base_url": self.base_url,
                "janus_errors": errors,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalPos, apply_fill};
    use rustrade::Side;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn opening_from_flat_sets_entry_and_no_pnl() {
        let (pos, pnl) = apply_fill(LocalPos::default(), Side::Buy, 100.0, 2.0);
        assert!(pnl.is_none());
        assert!(approx(pos.qty, 2.0));
        assert!(approx(pos.avg_entry, 100.0));
    }

    #[test]
    fn adding_same_side_rolls_weighted_average() {
        let p1 = apply_fill(LocalPos::default(), Side::Buy, 100.0, 2.0).0;
        let (p2, pnl) = apply_fill(p1, Side::Buy, 110.0, 2.0);
        assert!(pnl.is_none());
        assert!(approx(p2.qty, 4.0));
        assert!(approx(p2.avg_entry, 105.0)); // (100*2 + 110*2)/4
    }

    #[test]
    fn full_close_realises_pnl_and_goes_flat() {
        let long = apply_fill(LocalPos::default(), Side::Buy, 100.0, 2.0).0;
        let (pos, pnl) = apply_fill(long, Side::Sell, 120.0, 2.0);
        assert!(approx(pnl.unwrap(), 40.0)); // (120-100)*+1*2
        assert!(approx(pos.qty, 0.0));
    }

    #[test]
    fn partial_close_realises_on_closed_qty_keeps_entry() {
        let long = apply_fill(LocalPos::default(), Side::Buy, 100.0, 4.0).0;
        let (pos, pnl) = apply_fill(long, Side::Sell, 110.0, 1.0);
        assert!(approx(pnl.unwrap(), 10.0)); // (110-100)*1
        assert!(approx(pos.qty, 3.0));
        assert!(approx(pos.avg_entry, 100.0)); // unchanged on a partial reduce
    }

    #[test]
    fn short_close_realises_correct_sign() {
        let short = apply_fill(LocalPos::default(), Side::Sell, 100.0, 2.0).0;
        assert!(approx(short.qty, -2.0));
        // Buy back cheaper than the short entry → profit.
        let (pos, pnl) = apply_fill(short, Side::Buy, 90.0, 2.0);
        assert!(approx(pnl.unwrap(), 20.0)); // (90-100)*-1*2
        assert!(approx(pos.qty, 0.0));
    }

    #[test]
    fn flip_realises_prior_and_opens_remainder_at_fill() {
        let long = apply_fill(LocalPos::default(), Side::Buy, 100.0, 2.0).0;
        // Sell 5: closes the 2 long (realised on 2), opens 3 short at 120.
        let (pos, pnl) = apply_fill(long, Side::Sell, 120.0, 5.0);
        assert!(approx(pnl.unwrap(), 40.0)); // (120-100)*+1*2
        assert!(approx(pos.qty, -3.0));
        assert!(approx(pos.avg_entry, 120.0));
    }
}
