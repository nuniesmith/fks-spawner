//! TOML configuration for the spot portfolio bot.
//!
//! One file describes the whole portfolio: global settings plus a list of
//! per-exchange baskets (assets + weights) with a cash-reserve target. API keys
//! stay in the environment, never in this file. See `spot-portfolio.example.toml`.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

/// The whole portfolio config.
#[derive(Debug, Clone, Deserialize)]
pub struct PortfolioConfig {
    /// Seconds between portfolio checks.
    #[serde(default = "default_poll")]
    pub poll_secs: u64,
    /// Place real orders. Default false (dry-run).
    #[serde(default)]
    pub live: bool,
    /// Per-venue LIVE allowlist — a safety gate ON TOP of the global `live`
    /// switch. `None`/absent ⇒ every venue is eligible for live orders when
    /// `live = true` (backward-compatible). When set, ONLY the listed venues
    /// place real orders; all others stay **dry-run** even under `live = true`.
    /// Use it to keep an unverified venue in dry-run while a verified one trades,
    /// e.g. `live_venues = ["kraken"]`. Matched case-insensitively by venue name.
    #[serde(default)]
    pub live_venues: Option<Vec<String>>,
    /// LIVE-MONEY circuit breaker: if portfolio net worth falls more than this
    /// fraction below its running high-water mark, HALT live trading (every venue
    /// drops to dry-run) and fire a critical alert. The halt is sticky — resuming
    /// live needs an operator restart, so it can't auto-resume into a fall.
    /// `None`/absent = disabled. e.g. `0.15` = halt on a 15% drawdown.
    #[serde(default)]
    pub max_drawdown_pct: Option<f64>,
    /// Skip (and alert on) any single rebalance trade whose notional exceeds this
    /// fraction of the venue's total value — a guard against a mispriced/buggy
    /// plan moving far more than a rebalance ever should. `None`/absent = disabled.
    /// e.g. `0.5` = never let one trade exceed half the venue.
    #[serde(default)]
    pub max_trade_pct: Option<f64>,
    /// Alert when a LIVE fill's average price deviates from the planned price by
    /// more than this fraction (slippage / bad fill / mispriced order). Default
    /// `0.02` (2%); `0` disables. Alert-only — it never blocks a fill.
    #[serde(default = "default_max_slippage")]
    pub max_slippage_pct: f64,
    /// Post-trade reconciliation: after LIVE orders, the next cycle checks that
    /// each traded asset's balance actually moved by the filled amount. If a
    /// traded asset's quantity diverges from the expected post-fill quantity by
    /// more than this fraction, alert — the exchange may not have settled the
    /// order as reported. `None`/absent = disabled. e.g. `0.03` (3%).
    #[serde(default)]
    pub reconcile_tolerance_pct: Option<f64>,
    /// janus AI signal coupling mode (default `off`). `off` ignores janus
    /// entirely (unchanged behavior); `shadow` reads signals and LOGS the tilt it
    /// *would* apply but still trades on the config weights (observe before
    /// trusting it with live money); `on` applies the bounded tilt live. The AI
    /// can only redistribute weight WITHIN the invested sleeve — it never changes
    /// the asset set or the cash reserve, and every tilted trade still passes the
    /// same guardrails (trade cap, slippage, cooldown).
    #[serde(default)]
    pub ai_signals: AiMode,
    /// Minimum janus signal confidence (0–1) to act on. Below this, the asset
    /// keeps its config weight. Default `0.65`.
    #[serde(default = "default_ai_min_confidence")]
    pub ai_min_confidence: f64,
    /// Maximum RELATIVE tilt the AI may apply to any asset's target weight
    /// (`0.10` = ±10% of the base weight). A hard cap on AI authority. Default `0.10`.
    #[serde(default = "default_ai_tilt_max")]
    pub ai_tilt_max: f64,
    /// Ignore a janus signal older than this many seconds (stale ⇒ no tilt).
    /// Default `900` (15 min).
    #[serde(default = "default_ai_max_age")]
    pub ai_max_signal_age_secs: i64,
    /// Redis URL janus publishes signals to. Falls back to the `REDIS_URL` env,
    /// then `redis://redis:6379/0`. Only read when `ai_signals` != `off`.
    pub ai_redis_url: Option<String>,
    /// Discord webhook for alerts (optional).
    pub alert_webhook: Option<String>,
    /// Path to append a JSONL trade journal (optional).
    pub journal: Option<String>,
    /// Simulated starting cash per exchange for a keyless paper run.
    #[serde(default = "default_paper")]
    pub paper_usd: f64,
    /// The per-exchange baskets (TOML `[[exchange]]` blocks).
    #[serde(rename = "exchange", default)]
    pub exchanges: Vec<ExchangeConfig>,
}

/// One exchange's basket + rebalancing settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    /// Venue id: `"kraken"` | `"cryptocom"` | `"kucoin"` (KuCoin spot).
    pub name: String,
    /// Cash/quote currency held as the reserve (e.g. `"USD"`, `"USDT"`, `"USDC"`).
    #[serde(default = "default_cash")]
    pub cash: String,
    /// Fraction of value to keep in cash (0.0–1.0).
    #[serde(default)]
    pub reserve_pct: f64,
    /// Relative drift band that triggers a rebalance.
    #[serde(default = "default_band")]
    pub band: f64,
    /// Minimum seconds between rebalances (anti-whipsaw).
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
    /// Skip trades below this cash value (dust / venue minimum).
    #[serde(default = "default_min_trade")]
    pub min_trade_usd: f64,
    /// Rebalance immediately (bypassing the cooldown) when the cash balance jumps
    /// by more than this between cycles — i.e. a deposit landed. 0 = disabled.
    #[serde(default)]
    pub deposit_trigger_usd: f64,
    /// Target weights per asset (need not sum to 1; normalized at use).
    #[serde(default)]
    pub targets: BTreeMap<String, f64>,
}

fn default_poll() -> u64 {
    300
}
fn default_paper() -> f64 {
    1000.0
}
fn default_cash() -> String {
    "USD".to_string()
}
fn default_band() -> f64 {
    0.25
}
fn default_cooldown() -> u64 {
    3600
}
fn default_min_trade() -> f64 {
    10.0
}
fn default_max_slippage() -> f64 {
    0.02
}
fn default_ai_min_confidence() -> f64 {
    0.65
}
fn default_ai_tilt_max() -> f64 {
    0.10
}
fn default_ai_max_age() -> i64 {
    900
}

/// janus AI → spot coupling mode (how much authority the model's signals get).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AiMode {
    /// Ignore janus entirely (default) — targets come only from config.
    #[default]
    Off,
    /// Read signals and LOG the tilt that *would* apply, but trade on the config
    /// weights. Lets the AI's influence be observed before it touches live money.
    Shadow,
    /// Apply the bounded tilt to the live targets.
    On,
}

impl PortfolioConfig {
    /// Whether `venue` is cleared to place REAL orders: the global `live` switch
    /// must be ON **and** (there is no allowlist, or the venue is on it). This is
    /// the per-venue safety gate — an unverified venue absent from a configured
    /// `live_venues` stays in dry-run even when `live = true`. Case-insensitive.
    pub fn venue_is_live(&self, venue: &str) -> bool {
        self.live
            && self
                .live_venues
                .as_ref()
                .is_none_or(|allow| allow.iter().any(|v| v.eq_ignore_ascii_case(venue)))
    }

    /// Resolved Redis URL for the janus signal bridge: the explicit
    /// `ai_redis_url`, else the `REDIS_URL` env, else the compose default.
    pub fn resolved_ai_redis_url(&self) -> String {
        self.ai_redis_url
            .clone()
            .or_else(|| std::env::var("REDIS_URL").ok())
            .unwrap_or_else(|| "redis://redis:6379/0".to_string())
    }

    /// Load + parse + validate the TOML config at `path`.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: PortfolioConfig =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse + validate from a TOML string (used by tests / callers with inline config).
    pub fn from_toml_str(text: &str) -> Result<Self> {
        let cfg: PortfolioConfig = toml::from_str(text).context("parsing config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.exchanges.is_empty(),
            "config has no [[exchange]] entries"
        );
        for e in &self.exchanges {
            ensure!(
                (0.0..1.0).contains(&e.reserve_pct),
                "{}: reserve_pct must be in [0, 1), got {}",
                e.name,
                e.reserve_pct
            );
            ensure!(!e.targets.is_empty(), "{}: no target assets", e.name);
            ensure!(
                e.targets.values().all(|w| *w > 0.0),
                "{}: all target weights must be positive",
                e.name
            );
        }
        if let Some(dd) = self.max_drawdown_pct {
            // Open interval (0, 1): 0.0 would make the breaker a hair-trigger
            // (trip on the first downtick below the peak); use None to disable.
            ensure!(
                dd > 0.0 && dd < 1.0,
                "max_drawdown_pct must be in (0, 1), got {dd}"
            );
        }
        if let Some(mt) = self.max_trade_pct {
            ensure!(
                mt > 0.0 && mt <= 1.0,
                "max_trade_pct must be in (0, 1], got {mt}"
            );
        }
        ensure!(
            (0.0..1.0).contains(&self.max_slippage_pct),
            "max_slippage_pct must be in [0, 1), got {}",
            self.max_slippage_pct
        );
        if let Some(rt) = self.reconcile_tolerance_pct {
            ensure!(
                (0.0..1.0).contains(&rt),
                "reconcile_tolerance_pct must be in [0, 1), got {rt}"
            );
        }
        ensure!(
            (0.0..=1.0).contains(&self.ai_min_confidence),
            "ai_min_confidence must be in [0, 1], got {}",
            self.ai_min_confidence
        );
        ensure!(
            (0.0..1.0).contains(&self.ai_tilt_max),
            "ai_tilt_max must be in [0, 1), got {}",
            self.ai_tilt_max
        );
        ensure!(
            self.ai_max_signal_age_secs > 0,
            "ai_max_signal_age_secs must be > 0, got {}",
            self.ai_max_signal_age_secs
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config_with_defaults() {
        let cfg = PortfolioConfig::from_toml_str(
            r#"
            live = true
            [[exchange]]
            name = "kraken"
            reserve_pct = 0.2
            [exchange.targets]
            BTC = 0.5
            ETH = 0.3
            SOL = 0.2
        "#,
        )
        .unwrap();
        assert!(cfg.live);
        assert_eq!(cfg.poll_secs, 300); // default
        assert_eq!(cfg.exchanges.len(), 1);
        let k = &cfg.exchanges[0];
        assert_eq!(k.name, "kraken");
        assert_eq!(k.cash, "USD"); // default
        assert!((k.reserve_pct - 0.2).abs() < 1e-9);
        assert!((k.band - 0.25).abs() < 1e-9); // default
        assert_eq!(k.targets.len(), 3);
    }

    #[test]
    fn rejects_a_reserve_at_or_above_one() {
        let err = PortfolioConfig::from_toml_str(
            r#"
            [[exchange]]
            name = "kraken"
            reserve_pct = 1.0
            [exchange.targets]
            BTC = 1.0
        "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("reserve_pct"), "{err}");
    }

    #[test]
    fn rejects_empty_config() {
        assert!(PortfolioConfig::from_toml_str("poll_secs = 60").is_err());
    }

    fn cfg_with(live: bool, allow: &str) -> PortfolioConfig {
        PortfolioConfig::from_toml_str(&format!(
            r#"
            live = {live}
            {allow}
            [[exchange]]
            name = "kraken"
            [exchange.targets]
            BTC = 1.0
            "#
        ))
        .unwrap()
    }

    #[test]
    fn venue_is_live_no_allowlist_defaults_to_all_when_live() {
        // Backward-compatible: no `live_venues` ⇒ every venue is live under `live=true`.
        let cfg = cfg_with(true, "");
        assert!(cfg.venue_is_live("kraken"));
        assert!(cfg.venue_is_live("kucoin"));
        assert!(cfg.venue_is_live("cryptocom"));
    }

    #[test]
    fn venue_is_live_allowlist_restricts_to_listed_venues() {
        let cfg = cfg_with(true, r#"live_venues = ["kraken"]"#);
        assert!(cfg.venue_is_live("kraken"));
        assert!(
            !cfg.venue_is_live("kucoin"),
            "unlisted venue must stay dry-run"
        );
        assert!(!cfg.venue_is_live("cryptocom"));
        // Case-insensitive.
        assert!(cfg.venue_is_live("Kraken"));
    }

    #[test]
    fn venue_is_live_false_when_global_live_off() {
        // The global switch dominates: live=false ⇒ nothing is live, allowlist or not.
        let cfg = cfg_with(false, r#"live_venues = ["kraken"]"#);
        assert!(!cfg.venue_is_live("kraken"));
        let cfg = cfg_with(false, "");
        assert!(!cfg.venue_is_live("kraken"));
    }

    #[test]
    fn venue_is_live_empty_allowlist_permits_nothing() {
        // An explicit empty allowlist means "no venue is live" (distinct from unset).
        let cfg = cfg_with(true, r#"live_venues = []"#);
        assert!(!cfg.venue_is_live("kraken"));
    }
}
