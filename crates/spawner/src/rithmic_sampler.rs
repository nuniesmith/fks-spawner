// =============================================================================
// rithmic_sampler.rs — the Rithmic account-balance sampler (P0.6, source='rithmic')
//
// A READ-ONLY treasury node. It polls the fks-state `rithmic-connector`'s
// read-only HTTP surface (the same `/positions` endpoint the connector serves
// on its bot-contract port :9091) for the account-level balance and writes ONE
// net_worth_snapshots row per tick:
//
//     account_id = "rithmic:<account_id>"   net_worth = account_balance
//     currency   = "USD"   venue = "rithmic"   source = "rithmic"
//
// WHY /positions: the rithmic-connector is doctrine-bound READ-ONLY (it opens
// ONLY the Rithmic PnL plant, never the order plant). Its `GET /positions`
// response (see fks-state/crates/rithmic-connector/src/positions.rs) carries an
// optional `account_summary { account_id, net_quantity, open_pnl, day_pnl,
// account_balance }`. We read `account_summary.account_balance` — the figure
// Rithmic reports for the funded futures account — and nothing else.
//
// GATING: OFF unless RITHMIC_SAMPLER_URL is set (e.g.
// http://fks_rithmic_connector:9091). The connector is itself gated on live
// Rithmic credentials and is usually DOWN in dev, so an unreachable connector,
// a non-2xx, or a response with no `account_summary` yet is a SILENT debug skip
// — never fatal, never a zero row.
//
// The parse logic is pure + always compiled (+ unit-tested); the sampler itself
// needs an HTTP client + the Postgres store, so it is gated behind `db`.
// =============================================================================

use crate::net_worth::{NetWorthSnapshot, SOURCE_RITHMIC};

/// Default sampling cadence in seconds when RITHMIC_SAMPLE_INTERVAL_SECS is
/// unset. Balance moves slowly relative to fills; this is a treasury reading,
/// not a live PnL tick.
pub const DEFAULT_SAMPLE_INTERVAL_SECS: u64 = 300;

/// The `venue` tag stamped on rithmic snapshot rows.
pub const VENUE_RITHMIC: &str = "rithmic";

/// Prefix that namespaces a rithmic account id into the shared account_id space
/// (so `ACCT1` at Rithmic becomes `rithmic:ACCT1` in net_worth_snapshots).
pub const ACCOUNT_PREFIX: &str = "rithmic:";

// ─────────────────────────────────────────────────────────────────────────────
// Config — env-gated. OFF unless RITHMIC_SAMPLER_URL is set.
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the Rithmic balance sampler, read from the environment.
#[derive(Debug, Clone, Default)]
pub struct RithmicSamplerConfig {
    /// Base URL of the rithmic-connector's HTTP surface (no trailing slash).
    /// `None` = sampler disabled. Env: RITHMIC_SAMPLER_URL.
    pub url: Option<String>,
    /// Seconds between ticks. Env: RITHMIC_SAMPLE_INTERVAL_SECS (default 300).
    pub interval_secs: u64,
}

impl RithmicSamplerConfig {
    /// Read the sampler config from the environment.
    pub fn from_env() -> Self {
        let url = std::env::var("RITHMIC_SAMPLER_URL")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty());
        let interval_secs = std::env::var("RITHMIC_SAMPLE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(DEFAULT_SAMPLE_INTERVAL_SECS);
        Self { url, interval_secs }
    }

    /// The sampler runs only when a connector URL is configured.
    pub fn enabled(&self) -> bool {
        self.url.is_some()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure logic: parse the connector's /positions body for the account balance
// ─────────────────────────────────────────────────────────────────────────────

/// An account balance reading parsed from the connector's `/positions` body.
#[derive(Debug, Clone, PartialEq)]
pub struct RithmicBalance {
    /// The Rithmic account id (from `account_summary.account_id`, falling back
    /// to the top-level `account`).
    pub account_id: String,
    /// The reported account balance in USD.
    pub account_balance: f64,
}

impl RithmicBalance {
    /// The namespaced account id (`rithmic:<id>`) used for the snapshot row.
    pub fn namespaced_account_id(&self) -> String {
        format!("{ACCOUNT_PREFIX}{}", self.account_id)
    }
}

/// Parse the account balance out of the rithmic-connector's `GET /positions`
/// JSON body. Returns `None` when:
///   - the body isn't JSON, or has no `account_summary` (no account update has
///     arrived yet — the connector is up but hasn't seen the account), or
///   - the balance is missing / non-finite / not strictly positive (Rithmic
///     leaves `account_balance` at 0.0 when it hasn't reported one — a 0 row
///     would be a misleading treasury reading, so we skip it).
///
/// Currency is always USD for these accounts (documented on the writer).
pub fn parse_positions_balance(body: &str) -> Option<RithmicBalance> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let summary = v.get("account_summary")?;
    // account_balance may arrive as a JSON number or (defensively) a string.
    let account_balance = match summary.get("account_balance")? {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if !account_balance.is_finite() || account_balance <= 0.0 {
        return None;
    }
    // Prefer the summary's account id; fall back to the view-level `account`.
    let account_id = summary
        .get("account_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| v.get("account").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();
    Some(RithmicBalance {
        account_id,
        account_balance,
    })
}

/// Build a net-worth snapshot row from a parsed balance reading.
pub fn snapshot_from_balance(bal: &RithmicBalance) -> NetWorthSnapshot {
    NetWorthSnapshot::for_account(
        bal.namespaced_account_id(),
        bal.account_balance,
        "USD",
        Some(VENUE_RITHMIC.to_string()),
        SOURCE_RITHMIC,
    )
}

/// Build the connector's `/positions` URL from its base.
pub fn positions_url(base: &str) -> String {
    format!("{}/positions", base.trim_end_matches('/'))
}

// ─────────────────────────────────────────────────────────────────────────────
// The sampler — needs an HTTP client + the Postgres store (db feature)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod sampler {
    use std::time::Duration;

    use tracing::{debug, info, warn};

    use super::{
        RithmicSamplerConfig, parse_positions_balance, positions_url, snapshot_from_balance,
    };
    use crate::db::BotRunStore;
    use crate::metrics;

    /// Per-request HTTP timeout. Short so a hung connector can't stall the loop.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Polls the rithmic-connector's `/positions` endpoint and appends a
    /// net-worth snapshot per tick.
    pub struct RithmicSampler {
        client: reqwest::Client,
    }

    impl Default for RithmicSampler {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RithmicSampler {
        pub fn new() -> Self {
            let client = reqwest::Client::builder()
                .timeout(PROBE_TIMEOUT)
                .user_agent(concat!("fks-spawner/", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap_or_default();
            Self { client }
        }

        /// One tick: GET /positions, parse the account balance, write one row.
        /// BEST-EFFORT: connector down / non-2xx / no account summary yet →
        /// SILENT debug skip (the connector is usually gated off in dev).
        pub async fn sample_once(&self, url: &str, store: &BotRunStore) {
            let Some(bal) = self.probe(url).await else {
                return;
            };
            let snap = snapshot_from_balance(&bal);
            match store.record_net_worth(&snap).await {
                Ok(()) => {
                    metrics::NET_WORTH_SNAPSHOTS_TOTAL.inc();
                    debug!(
                        account_id = %snap.bot_id,
                        balance = bal.account_balance,
                        "rithmic sampler: snapshot recorded"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "rithmic sampler: snapshot insert failed");
                }
            }
        }

        /// GET /positions and parse the balance. `None` = unreachable, non-2xx,
        /// unreadable, or no account balance reported yet (all debug-logged).
        async fn probe(&self, base: &str) -> Option<super::RithmicBalance> {
            let url = positions_url(base);
            let resp = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    debug!(error = %e, "rithmic sampler: /positions unreachable (connector likely down)");
                    return None;
                }
            };
            if !resp.status().is_success() {
                debug!(status = %resp.status(), "rithmic sampler: /positions non-2xx");
                return None;
            }
            let body = resp.text().await.ok()?;
            match parse_positions_balance(&body) {
                some @ Some(_) => some,
                None => {
                    debug!("rithmic sampler: no account balance in /positions — skipped");
                    None
                }
            }
        }
    }

    /// Run the sampler loop forever, one tick every `interval_secs`. Spawned as
    /// a detached background task from `main`; only started when the sampler is
    /// enabled AND a Postgres store is configured.
    pub async fn run_sampler(config: RithmicSamplerConfig, store: BotRunStore) {
        let Some(url) = config.url.clone() else {
            return; // enabled() was false — nothing to poll.
        };
        let interval = Duration::from_secs(config.interval_secs);
        let sampler = RithmicSampler::new();
        info!(
            interval_secs = config.interval_secs,
            "rithmic balance sampler started"
        );
        loop {
            tokio::time::sleep(interval).await;
            sampler.sample_once(&url, &store).await;
        }
    }
}

#[cfg(feature = "db")]
pub use sampler::{RithmicSampler, run_sampler};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic body mirroring the connector's `PositionsView` shape
    /// (fks-state/crates/rithmic-connector/src/positions.rs).
    fn positions_body(balance: &str, account_id: &str) -> String {
        format!(
            r#"{{
                "account":"{account_id}",
                "count":1,
                "positions":[{{"symbol":"ESU6","net_quantity":2}}],
                "account_summary":{{
                    "account_id":"{account_id}","net_quantity":2,
                    "open_pnl":15.0,"day_pnl":22.5,"account_balance":{balance}
                }},
                "read_only":true,
                "order_plant_open":false
            }}"#
        )
    }

    #[test]
    fn config_enabled_only_with_url() {
        let mut c = RithmicSamplerConfig::default();
        assert!(!c.enabled(), "default disabled");
        c.url = Some("http://fks_rithmic_connector:9091".to_string());
        assert!(c.enabled());
    }

    #[test]
    fn parses_account_balance_and_id() {
        let bal = parse_positions_balance(&positions_body("52000.00", "ACCT1")).unwrap();
        assert_eq!(bal.account_id, "ACCT1");
        assert_eq!(bal.account_balance, 52000.0);
        assert_eq!(bal.namespaced_account_id(), "rithmic:ACCT1");
    }

    #[test]
    fn parses_account_balance_as_string() {
        // Defensive: accept a stringified balance too.
        let body = r#"{"account_summary":{"account_id":"ACCT2","account_balance":"12345.67"}}"#;
        let bal = parse_positions_balance(body).unwrap();
        assert_eq!(bal.account_balance, 12345.67);
        assert_eq!(bal.account_id, "ACCT2");
    }

    #[test]
    fn falls_back_to_top_level_account_id() {
        let body = r#"{"account":"ACCT9","account_summary":{"account_balance":800.0}}"#;
        let bal = parse_positions_balance(body).unwrap();
        assert_eq!(bal.account_id, "ACCT9");
    }

    #[test]
    fn none_when_no_account_summary() {
        // Connector up but no account update yet → nothing to record.
        let body = r#"{"account":"","count":0,"positions":[],"account_summary":null,
                       "read_only":true,"order_plant_open":false}"#;
        assert!(parse_positions_balance(body).is_none());
    }

    #[test]
    fn none_on_zero_or_negative_balance() {
        // Rithmic leaves account_balance at 0.0 when unreported — skip, don't
        // write a misleading zero row.
        assert!(parse_positions_balance(&positions_body("0.0", "ACCT1")).is_none());
        assert!(parse_positions_balance(&positions_body("-5.0", "ACCT1")).is_none());
    }

    #[test]
    fn none_on_non_json_or_missing_balance() {
        assert!(parse_positions_balance("not json").is_none());
        assert!(parse_positions_balance(r#"{"account_summary":{"account_id":"A"}}"#).is_none());
    }

    #[test]
    fn snapshot_carries_namespace_venue_currency_source() {
        let bal = RithmicBalance {
            account_id: "ACCT1".to_string(),
            account_balance: 52000.0,
        };
        let snap = snapshot_from_balance(&bal);
        assert_eq!(snap.bot_id, "rithmic:ACCT1");
        assert_eq!(snap.net_worth, 52000.0);
        assert_eq!(snap.currency, "USD");
        assert_eq!(snap.venue.as_deref(), Some("rithmic"));
        assert_eq!(snap.source, SOURCE_RITHMIC);
    }

    #[test]
    fn positions_url_appends_path() {
        assert_eq!(
            positions_url("http://fks_rithmic_connector:9091"),
            "http://fks_rithmic_connector:9091/positions"
        );
        assert_eq!(
            positions_url("http://host:9091/"),
            "http://host:9091/positions"
        );
    }
}
