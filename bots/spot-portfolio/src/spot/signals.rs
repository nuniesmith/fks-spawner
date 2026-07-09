//! janus AI → spot **signal bridge**: read the trading brain's per-symbol
//! signals from Redis and turn them into a *bounded tilt* on the rebalancer's
//! target weights.
//!
//! Design invariants (why this is safe to point at live money):
//! - **Bounded.** Each asset's *pre-normalization* weight is scaled by at most
//!   ±`tilt_max`. After the rebalancer renormalizes the invested sleeve, the
//!   effective per-asset move is bounded by ~`2·tilt_max/(1−tilt_max)` and
//!   unsignaled assets shift slightly as the sleeve re-splits — but the asset set
//!   and the cash reserve are exactly preserved (the AI only redistributes
//!   *within* the invested sleeve; it never levers up or drains the reserve).
//! - **Fail-safe.** A missing / stale / low-confidence signal, or any Redis or
//!   parse error, leaves that asset on its **config** weight. With janus down the
//!   bot behaves exactly as it does today.
//! - **Gated downstream.** Tilted targets flow through the same
//!   [`rebalance::plan`](super::rebalance::plan) and the same guardrails
//!   (trade-size cap, slippage, cooldown) as untilted ones.
//! - **Observable first.** In [`AiMode::Shadow`] the tilt is computed and logged
//!   but NOT traded on — the influence can be watched before it's trusted.
//!
//! janus publishes `Signal { symbol, signal_type, confidence, … }` bodies at
//! `janus:signal:{id}`, indexed newest-first per symbol in the sorted set
//! `janus:signals:symbol:{SYMBOL}` where `SYMBOL` is the pair with `/`→`_`
//! (e.g. `BTC/USDT` ⇒ `BTC_USDT`), score = publish time in ms. Its direction
//! maps 1:1 onto the spot asset. A signal janus itself vetoed carries a
//! `metadata.gate` starting with `block…` — the bridge honors that and won't
//! tilt on it.

use std::collections::HashMap;

use anyhow::Result;
use tracing::warn;

use super::config::AiMode;
use super::rebalance::Target;

/// One asset's latest AI signal, reduced to the fields the tilt needs. `age_secs`
/// is precomputed by the reader so the tilt math stays pure/clock-free.
#[derive(Debug, Clone)]
pub struct AssetSignal {
    /// Direction: `+1` buy, `-1` sell, `0` hold/close (no tilt).
    pub direction: i8,
    /// Model confidence in the action, `0.0`–`1.0`.
    pub confidence: f64,
    /// Seconds since the signal was published (negative = clock skew ⇒ ignored).
    pub age_secs: i64,
    /// Whether janus flagged the signal as gate-suppressed (⇒ no tilt).
    pub blocked: bool,
}

/// Gating + magnitude knobs for the tilt (mirrors the `ai_*` config fields).
#[derive(Debug, Clone, Copy)]
pub struct TiltParams {
    /// Minimum confidence to act on; below this the asset keeps its config weight.
    pub min_confidence: f64,
    /// Ignore a signal older than this many seconds.
    pub max_age_secs: i64,
    /// Max relative tilt applied to a target weight (e.g. `0.10` = ±10%).
    pub tilt_max: f64,
}

/// Everything `run_cycle` needs to apply (or shadow) the AI tilt for one poll.
pub struct AiTilt {
    /// Coupling mode (`shadow` logs only; `on` trades the tilt). Never `off` here
    /// — an `off` config skips building this entirely.
    pub mode: AiMode,
    /// The gating/magnitude knobs.
    pub params: TiltParams,
    /// Latest signal per asset name (e.g. `"BTC"`), as read this poll.
    pub signals: HashMap<String, AssetSignal>,
}

/// Signed tilt factor in `[-tilt_max, +tilt_max]` for one signal, or `0.0` when
/// any gate fails (blocked, hold, stale, or below the confidence floor). The
/// magnitude scales linearly with confidence *above* the floor.
fn tilt_factor(sig: &AssetSignal, p: &TiltParams) -> f64 {
    if sig.blocked || sig.direction == 0 || sig.confidence < p.min_confidence {
        return 0.0;
    }
    if sig.age_secs < 0 || sig.age_secs > p.max_age_secs {
        return 0.0;
    }
    let span = (1.0 - p.min_confidence).max(1e-9);
    let scaled = ((sig.confidence - p.min_confidence) / span).clamp(0.0, 1.0);
    p.tilt_max * scaled * sig.direction as f64
}

/// Apply the bounded tilt to `base`, returning new targets. An asset with no
/// signal (or a gated one) keeps its base weight. Weights are left un-normalized:
/// [`rebalance::plan`](super::rebalance::plan) normalizes them, and the cash
/// reserve is a separate config value, so the tilt only re-splits the invested
/// sleeve.
pub fn apply_tilt(
    base: &[Target],
    signals: &HashMap<String, AssetSignal>,
    p: &TiltParams,
) -> Vec<Target> {
    base.iter()
        .map(|t| {
            let f = signals.get(&t.name).map_or(0.0, |s| tilt_factor(s, p));
            Target {
                name: t.name.clone(),
                weight: t.weight * (1.0 + f),
            }
        })
        .collect()
}

/// Human-readable per-asset tilt (relative weight change), for logging. `None`
/// when nothing moved. e.g. `"BTC +8%, ETH -5%"`.
pub fn tilt_summary(base: &[Target], tilted: &[Target]) -> Option<String> {
    let mut parts = Vec::new();
    for (b, t) in base.iter().zip(tilted.iter()) {
        if b.weight > 0.0 && (t.weight - b.weight).abs() > 1e-9 {
            parts.push(format!(
                "{} {:+.0}%",
                b.name,
                (t.weight / b.weight - 1.0) * 100.0
            ));
        }
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// Per-asset evaluation string for the shadow log — each signal's
/// direction/confidence/age and whether it would tilt (and if not, why). Emitted
/// every poll so the shadow trial is analyzable, e.g.
/// `"BTC buy/0.79/361s/blocked, SOL sell/0.90/40s/act"`.
pub fn eval_summary(signals: &HashMap<String, AssetSignal>, p: &TiltParams) -> String {
    if signals.is_empty() {
        return "no signals".to_string();
    }
    let mut items: Vec<String> = signals
        .iter()
        .map(|(asset, s)| {
            let dir = match s.direction {
                d if d > 0 => "buy",
                d if d < 0 => "sell",
                _ => "hold",
            };
            let status = if tilt_factor(s, p) != 0.0 {
                "act"
            } else if s.blocked {
                "blocked"
            } else if s.direction == 0 {
                "hold"
            } else if s.confidence < p.min_confidence {
                "low-conf"
            } else {
                "stale"
            };
            format!("{asset} {dir}/{:.2}/{}s/{status}", s.confidence, s.age_secs)
        })
        .collect();
    items.sort();
    items.join(", ")
}

/// Map a spot asset to the janus signal-key symbol (pair with `/`→`_`). `None`
/// for assets janus doesn't trade (e.g. `CRO`) — they simply never tilt.
fn janus_symbol(asset: &str) -> Option<&'static str> {
    match asset.to_ascii_uppercase().as_str() {
        "BTC" | "XBT" => Some("BTC_USDT"),
        "ETH" => Some("ETH_USDT"),
        "SOL" => Some("SOL_USDT"),
        _ => None,
    }
}

/// Build the `asset → janus symbol` map for the assets janus can inform.
pub fn default_symbol_map(assets: &[String]) -> HashMap<String, String> {
    assets
        .iter()
        .filter_map(|a| janus_symbol(a).map(|s| (a.clone(), s.to_string())))
        .collect()
}

/// Reads janus signals from Redis. Holds one multiplexed async connection
/// (cheap to clone per call). All reads are best-effort — see [`Self::fetch`].
pub struct SignalSource {
    conn: redis::aio::MultiplexedConnection,
    /// asset name (`"BTC"`) → janus symbol (`"BTCUSDT"`).
    symbol_map: HashMap<String, String>,
}

impl SignalSource {
    /// Connect to Redis and validate the symbol map is non-empty. Fails only on a
    /// bad URL / unreachable Redis at startup; per-poll reads never fail the bot.
    pub async fn connect(url: &str, symbol_map: HashMap<String, String>) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { conn, symbol_map })
    }

    /// Latest signal per mapped asset. **Fail-safe:** a Redis or parse error for
    /// an asset omits it (the caller then keeps that asset's config weight), and
    /// the poll continues — one bad read never blocks the rebalance.
    pub async fn fetch(&self, now_ms: i64) -> HashMap<String, AssetSignal> {
        let mut out = HashMap::new();
        for (asset, symbol) in &self.symbol_map {
            match self.fetch_one(symbol, now_ms).await {
                Ok(Some(sig)) => {
                    out.insert(asset.clone(), sig);
                }
                Ok(None) => {}
                Err(e) => warn!(
                    asset = %asset,
                    error = format!("{e:#}"),
                    "AI signal read failed — asset keeps its config weight"
                ),
            }
        }
        out
    }

    /// Read + parse the newest signal for one janus symbol.
    async fn fetch_one(&self, symbol: &str, now_ms: i64) -> Result<Option<AssetSignal>> {
        let mut conn = self.conn.clone();
        let key = format!("janus:signals:symbol:{}", symbol.to_ascii_uppercase());
        // Newest id + its publish-time score (ms).
        let top: Vec<(String, f64)> = redis::cmd("ZREVRANGE")
            .arg(&key)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;
        let Some((id, score_ms)) = top.into_iter().next() else {
            return Ok(None);
        };
        let body: Option<String> = redis::cmd("GET")
            .arg(format!("janus:signal:{id}"))
            .query_async(&mut conn)
            .await?;
        let Some(body) = body else { return Ok(None) };
        let v: serde_json::Value = serde_json::from_str(&body)?;
        let age_secs = ((now_ms as f64 - score_ms) / 1000.0).round() as i64;
        Ok(Some(parse_signal(&v, age_secs)))
    }
}

/// Reduce a janus signal JSON body (+ precomputed age) to an [`AssetSignal`].
/// `signal_type` → direction; a `metadata.gate` starting with `block…` (janus's
/// own veto, e.g. a volatility filter) ⇒ `blocked`, as does a top-level `blocked`
/// bool. Pure, so the wire-format parsing is unit-tested without Redis.
fn parse_signal(v: &serde_json::Value, age_secs: i64) -> AssetSignal {
    let direction = match v
        .get("signal_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("hold")
    {
        s if s.eq_ignore_ascii_case("buy") => 1i8,
        s if s.eq_ignore_ascii_case("sell") => -1,
        _ => 0,
    };
    let confidence = v
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    let gate = v
        .get("metadata")
        .and_then(|m| m.get("gate"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let blocked = gate.to_ascii_lowercase().starts_with("block")
        || v.get("blocked")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
    AssetSignal {
        direction,
        confidence,
        age_secs,
        blocked,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Vec<Target> {
        vec![
            Target {
                name: "BTC".into(),
                weight: 0.5,
            },
            Target {
                name: "ETH".into(),
                weight: 0.3,
            },
            Target {
                name: "SOL".into(),
                weight: 0.2,
            },
        ]
    }
    fn params() -> TiltParams {
        TiltParams {
            min_confidence: 0.65,
            max_age_secs: 900,
            tilt_max: 0.10,
        }
    }
    fn sig(direction: i8, confidence: f64, age_secs: i64) -> AssetSignal {
        AssetSignal {
            direction,
            confidence,
            age_secs,
            blocked: false,
        }
    }
    fn weight_of(ts: &[Target], name: &str) -> f64 {
        ts.iter().find(|t| t.name == name).unwrap().weight
    }

    #[test]
    fn full_confidence_buy_tilts_up_by_tilt_max() {
        let f = tilt_factor(&sig(1, 1.0, 0), &params());
        assert!((f - 0.10).abs() < 1e-9, "conf=1 buy ⇒ +tilt_max, got {f}");
    }

    #[test]
    fn full_confidence_sell_tilts_down_by_tilt_max() {
        let f = tilt_factor(&sig(-1, 1.0, 0), &params());
        assert!((f + 0.10).abs() < 1e-9, "conf=1 sell ⇒ -tilt_max, got {f}");
    }

    #[test]
    fn below_confidence_floor_does_not_tilt() {
        assert_eq!(tilt_factor(&sig(1, 0.60, 0), &params()), 0.0);
    }

    #[test]
    fn hold_and_blocked_do_not_tilt() {
        assert_eq!(tilt_factor(&sig(0, 0.99, 0), &params()), 0.0, "hold");
        let mut s = sig(1, 0.99, 0);
        s.blocked = true;
        assert_eq!(tilt_factor(&s, &params()), 0.0, "blocked");
    }

    #[test]
    fn stale_or_future_signal_does_not_tilt() {
        assert_eq!(tilt_factor(&sig(1, 0.99, 901), &params()), 0.0, "stale");
        assert_eq!(
            tilt_factor(&sig(1, 0.99, -5), &params()),
            0.0,
            "future/skew"
        );
    }

    #[test]
    fn confidence_scales_between_floor_and_one() {
        // Halfway from floor (0.65) to 1.0 ⇒ half of tilt_max.
        let f = tilt_factor(&sig(1, 0.825, 0), &params());
        assert!((f - 0.05).abs() < 1e-3, "expected ~+0.05, got {f}");
    }

    #[test]
    fn apply_tilt_moves_only_signaled_assets_and_stays_positive() {
        let mut signals = HashMap::new();
        signals.insert("BTC".to_string(), sig(1, 1.0, 0)); // +10%
        signals.insert("ETH".to_string(), sig(-1, 1.0, 0)); // -10%
        // SOL: no signal ⇒ unchanged.
        let out = apply_tilt(&base(), &signals, &params());
        assert!((weight_of(&out, "BTC") - 0.55).abs() < 1e-9); // 0.5 * 1.10
        assert!((weight_of(&out, "ETH") - 0.27).abs() < 1e-9); // 0.3 * 0.90
        assert!((weight_of(&out, "SOL") - 0.20).abs() < 1e-9); // untouched
        assert!(out.iter().all(|t| t.weight > 0.0));
    }

    #[test]
    fn tilt_is_bounded_even_at_max_confidence() {
        let mut signals = HashMap::new();
        signals.insert("BTC".to_string(), sig(1, 1.0, 0));
        let out = apply_tilt(&base(), &signals, &params());
        // Never more than tilt_max above base.
        assert!(weight_of(&out, "BTC") <= 0.5 * (1.0 + 0.10) + 1e-9);
    }

    #[test]
    fn summary_reports_relative_change_or_none() {
        let mut signals = HashMap::new();
        signals.insert("BTC".to_string(), sig(1, 1.0, 0));
        let out = apply_tilt(&base(), &signals, &params());
        assert_eq!(tilt_summary(&base(), &out).as_deref(), Some("BTC +10%"));
        // No signals ⇒ no change ⇒ None.
        assert_eq!(tilt_summary(&base(), &base()), None);
    }

    #[test]
    fn symbol_map_covers_majors_and_skips_others() {
        let m = default_symbol_map(&[
            "BTC".to_string(),
            "ETH".to_string(),
            "SOL".to_string(),
            "CRO".to_string(),
        ]);
        assert_eq!(m.get("BTC").map(String::as_str), Some("BTC_USDT"));
        assert_eq!(m.get("SOL").map(String::as_str), Some("SOL_USDT"));
        assert!(
            !m.contains_key("CRO"),
            "CRO has no janus signal ⇒ never tilts"
        );
    }

    #[test]
    fn parse_signal_maps_direction_and_confidence() {
        let v = serde_json::json!({
            "signal_type": "buy", "confidence": 0.82, "metadata": {"gate": "ok"}
        });
        let s = parse_signal(&v, 42);
        assert_eq!(s.direction, 1);
        assert!((s.confidence - 0.82).abs() < 1e-9);
        assert_eq!(s.age_secs, 42);
        assert!(!s.blocked);
        assert_eq!(
            parse_signal(&serde_json::json!({"signal_type": "sell"}), 0).direction,
            -1
        );
        assert_eq!(
            parse_signal(&serde_json::json!({"signal_type": "hold"}), 0).direction,
            0
        );
    }

    #[test]
    fn eval_summary_labels_each_signal_status() {
        let mut sigs = HashMap::new();
        sigs.insert("BTC".to_string(), sig(1, 0.90, 10)); // acts
        let mut eth = sig(1, 0.90, 10);
        eth.blocked = true;
        sigs.insert("ETH".to_string(), eth); // blocked
        sigs.insert("SOL".to_string(), sig(-1, 0.50, 10)); // below floor
        let s = eval_summary(&sigs, &params());
        assert!(s.contains("BTC buy/0.90/10s/act"), "{s}");
        assert!(s.contains("ETH buy/0.90/10s/blocked"), "{s}");
        assert!(s.contains("SOL sell/0.50/10s/low-conf"), "{s}");
        assert_eq!(eval_summary(&HashMap::new(), &params()), "no signals");
    }

    #[test]
    fn parse_signal_honors_metadata_gate_block() {
        // A real janus body: buy + high confidence but vetoed by a gate ⇒ blocked,
        // and a blocked signal produces no tilt even at max confidence.
        let v = serde_json::json!({
            "signal_type": "buy",
            "confidence": 0.82,
            "metadata": {"gate": "block_vol_filter:volatility too low (2% < 15%)"}
        });
        let s = parse_signal(&v, 0);
        assert!(s.blocked, "a block… gate must mark the signal blocked");
        assert_eq!(tilt_factor(&s, &params()), 0.0);
    }
}
