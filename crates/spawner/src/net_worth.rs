// =============================================================================
// net_worth.rs — the durable net-worth history sampler
//
// Background writer for the `net_worth_snapshots` table (see
// src/sql/spawner/006_net_worth_snapshots.sql). On a configurable interval
// (NET_WORTH_SAMPLE_INTERVAL_SECS, default 300s) it:
//
//   1. Lists running bot containers via the DockerOps trait (the spawner
//      already tracks every `fks.bot=true` container + its DNS name).
//   2. GETs each bot's `/status` document on the bot metrics port (:9091 by
//      default) over the internal `fks_network`.
//   3. Parses a net-worth figure out of that JSON.
//   4. Appends one `net_worth_snapshots` row per bot.
//
// WHY /status (not /metrics): the FKS bot contract (docs PLATFORM_ARCHITECTURE
// §5.1) says every bot serves `/health` + `/metrics`, and the crypto bots
// ADD a rich `/status` JSON document carrying net worth / per-venue totals —
// the same document the WebUI `/exchanges` pages read. The roadmap (§4.2)
// specs this sampler as polling `/status`. Bots that expose no net worth
// (e.g. the demo bots, which only emit `fks_bot_pnl_dollars`) simply return no
// recognised field and are skipped with a debug log — never fatal.
//
// DESIGN CONTRACT (mirrors notifications.rs):
//   - BEST-EFFORT. A bot that is unreachable, times out, returns non-2xx, or
//     exposes no net-worth field is skipped with a debug log — never fatal,
//     never blocks the other bots or the loop.
//   - Runs entirely off any request path (it is its own background task), so
//     DB writes are awaited inline in the detached task rather than needing a
//     further `tokio::spawn`.
//   - Per-request timeout so a hung bot can never stall the sweep.
//
// The parse/target-building logic is pure and always compiled (+ unit-tested).
// The sampler itself needs an HTTP client + the Postgres store, so it is gated
// behind the `db` feature alongside the rest of the persistence layer.
// =============================================================================

use crate::models::{ContainerInfo, NetWorthManualRequest};

/// Default sampling cadence in seconds when NET_WORTH_SAMPLE_INTERVAL_SECS is
/// unset. Coarse on purpose: this is a years-horizon backbone, not a live tick.
pub const DEFAULT_SAMPLE_INTERVAL_SECS: u64 = 300;

/// Candidate top-level JSON keys for a bot's net worth, in priority order.
///
/// The exact field name lives in the (private) crypto bots' `/status`
/// contract, so we probe the plausible spellings and take the first numeric
/// hit. USD-explicit names win over ambiguous ones. Extend this list rather
/// than guessing a single name if a bot uses something new.
const NET_WORTH_KEYS: &[&str] = &[
    "net_worth_usd",
    "net_worth",
    "total_value_usd",
    "total_value",
    "networth",
    "equity_usd",
    "equity",
];

/// Candidate keys for the denomination of the net-worth figure.
const CURRENCY_KEYS: &[&str] = &["currency", "net_worth_currency", "quote_currency"];

/// Candidate keys for an optional venue/exchange tag.
const VENUE_KEYS: &[&str] = &["venue", "exchange"];

/// A net-worth reading parsed out of a bot's `/status` document. Currency
/// defaults to USD; venue is absent for a bot-level total.
#[derive(Debug, Clone, PartialEq)]
pub struct NetWorthReading {
    pub net_worth: f64,
    pub currency: String,
    pub venue: Option<String>,
}

/// One row destined for `net_worth_snapshots`. `ts` is intentionally omitted —
/// the table defaults it to `NOW()` so the DB clock is authoritative.
///
/// The `bot_id` column doubles as the ACCOUNT id: for bot-status rows it is the
/// `fks.bot_id` label, and for the read-only treasury nodes (onchain / rithmic /
/// manual) it is the logical account id (e.g. `btc-cold`, `rithmic:ACCT1`). The
/// `source` column disambiguates who wrote the row (see the `SOURCE_*`
/// constants); it defaults to `bot_status` in the table.
#[derive(Debug, Clone, PartialEq)]
pub struct NetWorthSnapshot {
    /// Account id (stored in the `bot_id` column — see the struct doc).
    pub bot_id: String,
    pub net_worth: f64,
    pub currency: String,
    pub venue: Option<String>,
    /// Row writer tag stored in the `source` column.
    pub source: String,
}

/// `source` values for `net_worth_snapshots`. `bot_status` is the periodic
/// sampler polling a bot's `/status`; the P0.6 read-only treasury nodes each
/// stamp their own so a row's provenance is never ambiguous:
///   - `onchain`  — the cold-BTC watcher (derived xpub / explicit addresses)
///   - `rithmic`  — the Rithmic account-balance sampler
///   - `manual`   — a hand-entered snapshot (POST /net-worth)
pub const SOURCE_BOT_STATUS: &str = "bot_status";
pub const SOURCE_ONCHAIN: &str = "onchain";
pub const SOURCE_RITHMIC: &str = "rithmic";
pub const SOURCE_MANUAL: &str = "manual";

impl NetWorthSnapshot {
    /// Build a snapshot row for `bot_id` from a parsed `/status` reading,
    /// tagging it as sampler-sourced (`source = bot_status`).
    pub fn from_reading(bot_id: impl Into<String>, reading: NetWorthReading) -> Self {
        Self {
            bot_id: bot_id.into(),
            net_worth: reading.net_worth,
            currency: reading.currency,
            venue: reading.venue,
            source: SOURCE_BOT_STATUS.to_string(),
        }
    }

    /// Build a snapshot row for an arbitrary account + writer `source`. This is
    /// the constructor the read-only treasury nodes (onchain / rithmic / manual)
    /// use: `account_id` lands in the `bot_id` column, and `source` records who
    /// wrote it. No I/O and no privilege — building a row can only ever RECORD a
    /// net-worth reading, never move funds.
    pub fn for_account(
        account_id: impl Into<String>,
        net_worth: f64,
        currency: impl Into<String>,
        venue: Option<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            bot_id: account_id.into(),
            net_worth,
            currency: currency.into(),
            venue,
            source: source.into(),
        }
    }
}

/// Generous-but-bounded cap on the manual snapshot's account_id, mirroring the
/// treasury registry's identifier cap.
const MAX_ACCOUNT_ID_LEN: usize = 128;

/// Validate + normalise a `POST /net-worth` (manual) submission into a
/// [`NetWorthSnapshot`] tagged `source = manual`. Pure so the request shaping is
/// unit-testable without a database. Errors are operator-facing 400 messages.
pub fn validate_manual_snapshot(req: &NetWorthManualRequest) -> Result<NetWorthSnapshot, String> {
    let account_id = req.account_id.trim();
    if account_id.is_empty() {
        return Err("account_id is required".to_string());
    }
    if account_id.len() > MAX_ACCOUNT_ID_LEN {
        return Err(format!(
            "account_id too long (max {MAX_ACCOUNT_ID_LEN} chars)"
        ));
    }

    // Serde already rejects JSON NaN/Infinity, but keep the guard so a row can
    // never carry a non-finite value.
    if !req.net_worth.is_finite() {
        return Err("net_worth must be a finite number".to_string());
    }

    let currency = req.currency.trim().to_uppercase();
    if currency.is_empty() || currency.len() > 16 {
        return Err("currency must be 1-16 chars".to_string());
    }

    let venue = req
        .venue
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    Ok(NetWorthSnapshot::for_account(
        account_id,
        req.net_worth,
        currency,
        venue,
        SOURCE_MANUAL,
    ))
}

/// Coerce a JSON value to `f64`, accepting either a JSON number or a numeric
/// string (some status servers serialise money as a string to avoid float
/// ambiguity). Rejects non-finite values.
fn value_as_f64(v: &serde_json::Value) -> Option<f64> {
    let n = match v {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    n.is_finite().then_some(n)
}

fn first_string(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| {
        v.get(*k)
            .and_then(serde_json::Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
}

/// Extract a net-worth reading from a bot's `/status` JSON body.
///
/// Returns `None` when the body isn't JSON, or carries no recognised numeric
/// net-worth field — the caller treats that as "this bot doesn't report net
/// worth" and skips it. Currency defaults to USD when unspecified.
pub fn parse_status_net_worth(body: &str) -> Option<NetWorthReading> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let net_worth = NET_WORTH_KEYS
        .iter()
        .find_map(|k| v.get(*k).and_then(value_as_f64))?;
    let currency = first_string(&v, CURRENCY_KEYS).unwrap_or_else(|| "USD".to_string());
    let venue = first_string(&v, VENUE_KEYS);
    Some(NetWorthReading {
        net_worth,
        currency,
        venue,
    })
}

/// Build the `/status` URL for a bot from its container name + the bot metrics
/// port. Container names resolve over `fks_network`'s Docker DNS, so
/// `fks-bot-<id>:<port>` reaches the bot directly (same host:port the
/// Prometheus SD file targets).
pub fn status_url(container_name: &str, port: u16) -> String {
    format!("http://{container_name}:{port}/status")
}

/// From a list of bot containers, produce `(bot_id, status_url)` pairs for the
/// ones worth polling: state == "running" with a usable name + bot_id. Pure so
/// the discovery/filtering half is unit-testable against a `MockDockerClient`
/// without any HTTP.
pub fn running_status_targets(bots: &[ContainerInfo], port: u16) -> Vec<(String, String)> {
    bots.iter()
        .filter(|b| b.state == "running" && !b.name.is_empty() && !b.bot_id.is_empty())
        .map(|b| (b.bot_id.clone(), status_url(&b.name, port)))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// The sampler — needs an HTTP client + the Postgres store (db feature)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod sampler {
    use std::sync::Arc;
    use std::time::Duration;

    use tracing::{debug, warn};

    use super::{NetWorthSnapshot, parse_status_net_worth, running_status_targets};
    use crate::config::Config;
    use crate::db::BotRunStore;
    use crate::docker_client::DockerOps;
    use crate::metrics;

    /// Per-bot HTTP timeout. Short so one hung/slow bot can never stall the
    /// sweep of the others.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Polls running bots' `/status` endpoints and appends `net_worth_snapshots`
    /// rows. Cheap to construct (builds one reqwest client reused across the
    /// loop).
    pub struct NetWorthSampler {
        client: reqwest::Client,
    }

    impl Default for NetWorthSampler {
        fn default() -> Self {
            Self::new()
        }
    }

    impl NetWorthSampler {
        pub fn new() -> Self {
            // `build()` only fails on a TLS backend init error; fall back to the
            // default client (still functional, just without our timeout preset).
            let client = reqwest::Client::builder()
                .timeout(PROBE_TIMEOUT)
                .user_agent(concat!("fks-spawner/", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap_or_default();
            Self { client }
        }

        /// One sweep: list running bots, probe each for net worth, insert a row
        /// per bot that reports it. BEST-EFFORT throughout — every failure is
        /// logged (debug/warn) and swallowed; never returns an error.
        pub async fn sample_once(
            &self,
            docker: &dyn DockerOps,
            config: &Config,
            store: &BotRunStore,
        ) {
            let bots = match docker.list_bots().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "net-worth sampler: failed to list bots");
                    return;
                }
            };

            let targets = running_status_targets(&bots, config.bot_metrics_port);
            debug!(
                count = targets.len(),
                "net-worth sampler: polling running bots"
            );

            for (bot_id, url) in targets {
                let Some(reading) = self.probe(&bot_id, &url).await else {
                    // Bot doesn't expose net worth (or was unreachable) — skip.
                    continue;
                };
                let snap = NetWorthSnapshot::from_reading(&bot_id, reading);
                match store.record_net_worth(&snap).await {
                    Ok(()) => {
                        metrics::NET_WORTH_SNAPSHOTS_TOTAL.inc();
                        debug!(
                            bot_id = %snap.bot_id,
                            currency = %snap.currency,
                            "net-worth sampler: snapshot recorded"
                        );
                    }
                    Err(e) => {
                        warn!(bot_id = %bot_id, error = %e, "net-worth sampler: insert failed");
                    }
                }
            }

            // ── Piggyback: stale backtest-run sweep (edge factory) ──────────
            // One-shot backtest containers report their own results row and
            // exit; a container that dies silently leaves its backtest_runs
            // row 'running' forever. Rather than a dedicated reaper (not
            // needed for v1), this tick sweeps rows with
            // `finished_at IS NULL AND started_at < now() - interval
            // '2 hours'` to 'failed' — one cheap UPDATE per sampler sweep,
            // best-effort like everything else in this loop.
            match store.sweep_stale_backtest_runs().await {
                Ok(0) => {}
                Ok(n) => {
                    warn!(
                        swept = n,
                        "backtest sweep: marked stale running backtest runs failed"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "backtest sweep: stale-run sweep failed");
                }
            }
        }

        /// GET one bot's `/status` and parse its net worth. `None` = unreachable,
        /// non-2xx, unreadable, or no recognised net-worth field (all debug-logged,
        /// none fatal).
        async fn probe(&self, bot_id: &str, url: &str) -> Option<super::NetWorthReading> {
            let resp = match self.client.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    debug!(bot_id = %bot_id, error = %e, "net-worth sampler: /status unreachable");
                    return None;
                }
            };
            if !resp.status().is_success() {
                debug!(
                    bot_id = %bot_id,
                    status = %resp.status(),
                    "net-worth sampler: /status non-2xx"
                );
                return None;
            }
            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => {
                    debug!(bot_id = %bot_id, error = %e, "net-worth sampler: /status body unreadable");
                    return None;
                }
            };
            match parse_status_net_worth(&body) {
                some @ Some(_) => some,
                None => {
                    debug!(bot_id = %bot_id, "net-worth sampler: no net-worth field in /status — skipped");
                    None
                }
            }
        }
    }

    /// Run the sampler loop forever, one sweep every
    /// `NET_WORTH_SAMPLE_INTERVAL_SECS`. Spawned as a detached background task
    /// from `main`; only started when a Postgres store is configured.
    pub async fn run_sampler(docker: Arc<dyn DockerOps>, config: Arc<Config>, store: BotRunStore) {
        let interval = Duration::from_secs(config.net_worth_sample_interval_secs);
        let sampler = NetWorthSampler::new();
        loop {
            tokio::time::sleep(interval).await;
            sampler.sample_once(docker.as_ref(), &config, &store).await;
        }
    }
}

#[cfg(feature = "db")]
pub use sampler::{NetWorthSampler, run_sampler};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn container(name: &str, bot_id: &str, state: &str) -> ContainerInfo {
        ContainerInfo {
            id: bot_id.to_string(),
            id_full: bot_id.to_string(),
            name: name.to_string(),
            image: "fks-bot-x:latest".to_string(),
            status: String::new(),
            state: state.to_string(),
            bot_id: bot_id.to_string(),
            mode: "paper".to_string(),
            created_at: None,
            started_at: None,
            finished_at: None,
            labels: HashMap::new(),
            cpu_percent: None,
            memory_bytes: None,
            memory_limit_bytes: None,
            exit_code: None,
        }
    }

    // ── parse: field discovery ───────────────────────────────────────────────

    #[test]
    fn parses_net_worth_usd() {
        let r = parse_status_net_worth(r#"{"net_worth_usd": 12345.67}"#).unwrap();
        assert_eq!(r.net_worth, 12345.67);
        assert_eq!(r.currency, "USD", "currency defaults to USD");
        assert!(r.venue.is_none());
    }

    #[test]
    fn prefers_net_worth_usd_over_total_value() {
        // Both present → the higher-priority key wins.
        let r = parse_status_net_worth(r#"{"total_value": 1.0, "net_worth_usd": 999.0}"#).unwrap();
        assert_eq!(r.net_worth, 999.0);
    }

    #[test]
    fn falls_back_to_total_value() {
        let r = parse_status_net_worth(r#"{"total_value": 10500.0}"#).unwrap();
        assert_eq!(r.net_worth, 10500.0);
    }

    #[test]
    fn accepts_numeric_string_value() {
        // Some status servers serialise money as a string.
        let r = parse_status_net_worth(r#"{"net_worth": "4200.50"}"#).unwrap();
        assert_eq!(r.net_worth, 4200.50);
    }

    #[test]
    fn reads_currency_and_venue_when_present() {
        let r =
            parse_status_net_worth(r#"{"net_worth": 100.0, "currency": "EUR", "venue": "kraken"}"#)
                .unwrap();
        assert_eq!(r.currency, "EUR");
        assert_eq!(r.venue.as_deref(), Some("kraken"));
    }

    // ── parse: rejection ─────────────────────────────────────────────────────

    #[test]
    fn none_when_no_net_worth_field() {
        // A demo bot's /status (or /metrics-only bot) has no net-worth field.
        assert!(parse_status_net_worth(r#"{"pnl_dollars": 12.0, "uptime": 99}"#).is_none());
    }

    #[test]
    fn none_for_non_numeric_or_non_json() {
        assert!(parse_status_net_worth(r#"{"net_worth": "not-a-number"}"#).is_none());
        assert!(parse_status_net_worth(r#"{"net_worth": null}"#).is_none());
        assert!(parse_status_net_worth("not json at all").is_none());
        assert!(parse_status_net_worth("").is_none());
    }

    #[test]
    fn none_for_non_finite() {
        // JSON can't hold NaN/Inf as a number, but a stringified one is rejected.
        assert!(parse_status_net_worth(r#"{"net_worth": "inf"}"#).is_none());
        assert!(parse_status_net_worth(r#"{"net_worth": "NaN"}"#).is_none());
    }

    // ── snapshot row building ────────────────────────────────────────────────

    #[test]
    fn snapshot_from_reading_tags_source_and_bot() {
        let reading = NetWorthReading {
            net_worth: 500.0,
            currency: "USD".to_string(),
            venue: Some("kucoin".to_string()),
        };
        let snap = NetWorthSnapshot::from_reading("eth-scalper", reading);
        assert_eq!(snap.bot_id, "eth-scalper");
        assert_eq!(snap.net_worth, 500.0);
        assert_eq!(snap.currency, "USD");
        assert_eq!(snap.venue.as_deref(), Some("kucoin"));
        assert_eq!(snap.source, SOURCE_BOT_STATUS);
    }

    #[test]
    fn for_account_sets_account_id_and_source() {
        // The treasury-node constructor: account_id → bot_id column, explicit
        // source/venue/currency preserved.
        let snap = NetWorthSnapshot::for_account(
            "btc-cold",
            123_456.78,
            "USD",
            Some("cold-btc".to_string()),
            SOURCE_ONCHAIN,
        );
        assert_eq!(snap.bot_id, "btc-cold");
        assert_eq!(snap.net_worth, 123_456.78);
        assert_eq!(snap.currency, "USD");
        assert_eq!(snap.venue.as_deref(), Some("cold-btc"));
        assert_eq!(snap.source, "onchain");
    }

    // ── status url ───────────────────────────────────────────────────────────

    #[test]
    fn status_url_uses_container_name_and_port() {
        assert_eq!(
            status_url("fks-bot-eth-scalper", 9091),
            "http://fks-bot-eth-scalper:9091/status"
        );
    }

    // ── target filtering ─────────────────────────────────────────────────────

    // ── manual snapshot validation ───────────────────────────────────────────

    fn manual_req(json: &str) -> NetWorthManualRequest {
        serde_json::from_str(json).expect("valid NetWorthManualRequest JSON")
    }

    #[test]
    fn manual_snapshot_minimal_validates_with_defaults() {
        let req = manual_req(r#"{"account_id":"  apex-payout ","net_worth":48250.5}"#);
        let snap = validate_manual_snapshot(&req).expect("valid");
        assert_eq!(snap.bot_id, "apex-payout", "account_id trimmed");
        assert_eq!(snap.net_worth, 48250.5);
        assert_eq!(snap.currency, "USD", "currency defaults to USD");
        assert!(snap.venue.is_none());
        assert_eq!(snap.source, SOURCE_MANUAL);
    }

    #[test]
    fn manual_snapshot_normalises_currency_and_venue() {
        let req = manual_req(
            r#"{"account_id":"bank","net_worth":1000.0,"currency":"cad","venue":"  chase  "}"#,
        );
        let snap = validate_manual_snapshot(&req).expect("valid");
        assert_eq!(snap.currency, "CAD");
        assert_eq!(snap.venue.as_deref(), Some("chase"));
    }

    #[test]
    fn manual_snapshot_rejects_blank_id_and_non_finite() {
        let blank = manual_req(r#"{"account_id":"   ","net_worth":10.0}"#);
        assert!(validate_manual_snapshot(&blank).is_err());

        let mut nan = manual_req(r#"{"account_id":"a","net_worth":1.0}"#);
        nan.net_worth = f64::NAN;
        assert!(validate_manual_snapshot(&nan).is_err());
        nan.net_worth = f64::INFINITY;
        assert!(validate_manual_snapshot(&nan).is_err());
    }

    #[test]
    fn manual_snapshot_allows_negative_and_zero_values() {
        // Unlike a transfer, a net-worth snapshot may legitimately be zero (an
        // emptied account) or negative (a margin/debt balance).
        assert!(
            validate_manual_snapshot(&manual_req(r#"{"account_id":"a","net_worth":0.0}"#)).is_ok()
        );
        assert!(
            validate_manual_snapshot(&manual_req(r#"{"account_id":"a","net_worth":-500.0}"#))
                .is_ok()
        );
    }

    #[test]
    fn running_targets_skips_non_running_and_incomplete() {
        let bots = vec![
            container("fks-bot-a", "a", "running"),
            container("fks-bot-b", "b", "exited"), // stopped → skipped
            container("", "c", "running"),         // no name → skipped
            container("fks-bot-d", "", "running"), // no bot_id → skipped
        ];
        let targets = running_status_targets(&bots, 9091);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, "a");
        assert_eq!(targets[0].1, "http://fks-bot-a:9091/status");
    }
}
