// =============================================================================
// btc_watch.rs — the cold-BTC on-chain net-worth watcher (P0.6, source='onchain')
//
// A READ-ONLY treasury node. It derives receive/change addresses from a public
// account xpub (BIP32) and/or reads an explicit address list, queries their
// confirmed balance from a public Esplora API, prices BTC→USD off Kraken's
// public ticker, and writes ONE net_worth_snapshots row per tick:
//
//     account_id = BTC_WATCH_ACCOUNT_ID (default "btc-cold")
//     net_worth  = USD value of the summed on-chain balance
//     currency   = "USD"        venue = "cold-btc"       source = "onchain"
//
// SAFETY BY CONSTRUCTION: this node holds NO private keys and imports NO signing
// path. An xpub is public-key material — it can DERIVE addresses and observe
// their balances, but it can NEVER move funds. The raw BTC quantity behind the
// USD figure is always recoverable on-chain (the addresses are deterministic
// from the xpub), so this row is an auditable read, not a custodial claim.
//
// DERIVATION SCOPE: derives BIP84 native-segwit (p2wpkh, `bc1…`) addresses from
// the account-level xpub — receive branch (m/.../0/i) and change branch
// (m/.../1/i) for i in 0..gap. A "gap" of 20 (the BIP44 default) covers a fresh
// or lightly-used wallet; a DEEP wallet that has handed out more than `gap`
// addresses on either branch needs BTC_WATCH_GAP raised to at least its highest
// used index, or those funds are invisible to this watcher. Wallets on a
// different script type (legacy p2pkh / nested p2sh-segwit) should instead pass
// their addresses verbatim via BTC_WATCH_ADDRESSES.
//
// DESIGN CONTRACT (mirrors net_worth.rs):
//   - BEST-EFFORT. Esplora unreachable, a non-2xx, an unparseable body, or a
//     failed price fetch → the whole tick is SKIPPED with a debug/warn log. We
//     never crash, and never write a partial or zero row (a partial balance
//     would corrupt the net-worth series). Only a fully-summed, priced total is
//     written.
//   - The parse/derive/convert logic is pure + always compiled (+ unit-tested).
//     The watcher itself needs an HTTP client + the Postgres store, so it is
//     gated behind the `db` feature alongside the rest of the persistence layer.
// =============================================================================

use std::str::FromStr;

// Snapshot types are only referenced by the db-gated watcher below; the pure
// parse/derive helpers don't touch persistence.
#[cfg(feature = "db")]
use crate::net_worth::{NetWorthSnapshot, SOURCE_ONCHAIN};

/// Default watcher cadence in seconds. Coarse on purpose (a cold backbone moves
/// rarely), and it hits public APIs we don't want to hammer.
pub const DEFAULT_WATCH_INTERVAL_SECS: u64 = 3600;

/// Default BIP44 address gap limit — how many receive AND change addresses to
/// derive from the xpub. 20 is the standard; raise it for a deep wallet.
pub const DEFAULT_GAP_LIMIT: u32 = 20;

/// Default logical account id for the cold-BTC snapshot rows.
pub const DEFAULT_ACCOUNT_ID: &str = "btc-cold";

/// Default public Esplora API base (Blockstream's). Overridable to point at a
/// self-hosted Esplora / Electrs (`ESPLORA_API_BASE`).
pub const DEFAULT_ESPLORA_BASE: &str = "https://blockstream.info/api";

/// Kraken public ticker for the BTC/USD spot mark. No API key, no new exchange
/// dependency — a plain JSON GET. `XBTUSD` resolves to the `XXBTZUSD` pair.
pub const KRAKEN_TICKER_URL: &str = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD";

/// The `venue` tag stamped on cold-BTC snapshot rows.
pub const VENUE_COLD_BTC: &str = "cold-btc";

/// Satoshis per whole bitcoin.
const SATS_PER_BTC: f64 = 100_000_000.0;

// ─────────────────────────────────────────────────────────────────────────────
// Config — env-gated. The watcher is OFF unless an xpub OR an address list is
// configured. Parsing is pure so the enable/derive logic is unit-testable.
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the cold-BTC watcher, read from the environment.
#[derive(Debug, Clone)]
pub struct BtcWatchConfig {
    /// Account-level public xpub to derive addresses from. `None` = derive
    /// nothing (rely on `addresses`). Env: BTC_WATCH_XPUB.
    pub xpub: Option<String>,
    /// Explicit watch addresses (any script type), verbatim. Env:
    /// BTC_WATCH_ADDRESSES (comma-separated).
    pub addresses: Vec<String>,
    /// Gap limit — receive+change indices derived from the xpub. Env:
    /// BTC_WATCH_GAP (default 20).
    pub gap: u32,
    /// Seconds between ticks. Env: BTC_WATCH_INTERVAL_SECS (default 3600).
    pub interval_secs: u64,
    /// Logical account id for the snapshot rows. Env: BTC_WATCH_ACCOUNT_ID
    /// (default "btc-cold").
    pub account_id: String,
    /// Esplora API base (no trailing slash). Env: ESPLORA_API_BASE.
    pub esplora_base: String,
}

impl Default for BtcWatchConfig {
    /// A disabled watcher (no xpub, no addresses) — the safe default used by
    /// tests and stateless builds.
    fn default() -> Self {
        Self {
            xpub: None,
            addresses: Vec::new(),
            gap: DEFAULT_GAP_LIMIT,
            interval_secs: DEFAULT_WATCH_INTERVAL_SECS,
            account_id: DEFAULT_ACCOUNT_ID.to_string(),
            esplora_base: DEFAULT_ESPLORA_BASE.to_string(),
        }
    }
}

impl BtcWatchConfig {
    /// Read the watcher config from the environment.
    pub fn from_env() -> Self {
        let xpub = std::env::var("BTC_WATCH_XPUB")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let addresses = parse_addresses_env(std::env::var("BTC_WATCH_ADDRESSES").ok().as_deref());
        let gap = std::env::var("BTC_WATCH_GAP")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|g| *g > 0)
            .unwrap_or(DEFAULT_GAP_LIMIT);
        let interval_secs = std::env::var("BTC_WATCH_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(DEFAULT_WATCH_INTERVAL_SECS);
        let account_id = std::env::var("BTC_WATCH_ACCOUNT_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_ACCOUNT_ID.to_string());
        let esplora_base = std::env::var("ESPLORA_API_BASE")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_ESPLORA_BASE.to_string());
        Self {
            xpub,
            addresses,
            gap,
            interval_secs,
            account_id,
            esplora_base,
        }
    }

    /// The watcher runs only when there is something to watch: an xpub OR at
    /// least one explicit address.
    pub fn enabled(&self) -> bool {
        self.xpub.is_some() || !self.addresses.is_empty()
    }
}

/// Parse the `BTC_WATCH_ADDRESSES` env value: comma-separated, trimmed, blanks
/// dropped, duplicates removed (order preserved). Pure + unit-tested.
pub fn parse_addresses_env(raw: Option<&str>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(raw) = raw {
        for part in raw.split(',') {
            let a = part.trim();
            if !a.is_empty() && !out.iter().any(|e| e == a) {
                out.push(a.to_string());
            }
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure logic: address derivation, balance parsing, price parsing, conversion
// ─────────────────────────────────────────────────────────────────────────────

/// Derive the first `gap` BIP84 native-segwit (p2wpkh) receive AND change
/// addresses from an account-level xpub. Returns `2 * gap` addresses
/// (receive branch `0/i` then change branch `1/i` for i in 0..gap).
///
/// Pure + read-only: an xpub yields public keys only, so this can derive
/// addresses but never a spending key. Errors on an unparseable/rejected xpub
/// (e.g. a `zpub`/`ypub`, which must be re-encoded to the `xpub` prefix first).
pub fn derive_watch_addresses(xpub_str: &str, gap: u32) -> Result<Vec<String>, String> {
    use bitcoin::bip32::{ChildNumber, Xpub};
    use bitcoin::secp256k1::Secp256k1;
    use bitcoin::{Address, CompressedPublicKey, KnownHrp, NetworkKind};

    let xpub = Xpub::from_str(xpub_str.trim())
        .map_err(|e| format!("invalid BTC_WATCH_XPUB (expects an 'xpub…' account key): {e}"))?;
    let secp = Secp256k1::verification_only();
    // Bech32 human-readable prefix from the xpub's network kind (an xpub only
    // carries Main/Test, which is all bech32 encoding needs).
    let hrp = match xpub.network {
        NetworkKind::Main => KnownHrp::Mainnet,
        NetworkKind::Test => KnownHrp::Testnets,
    };

    let mut out = Vec::with_capacity((gap as usize) * 2);
    for branch in [0u32, 1u32] {
        let branch_child =
            ChildNumber::from_normal_idx(branch).map_err(|e| format!("bad branch index: {e}"))?;
        for index in 0..gap {
            let index_child = ChildNumber::from_normal_idx(index)
                .map_err(|e| format!("bad address index: {e}"))?;
            let derived = xpub
                .derive_pub(&secp, &[branch_child, index_child])
                .map_err(|e| format!("xpub derivation failed: {e}"))?;
            let compressed = CompressedPublicKey(derived.public_key);
            let addr = Address::p2wpkh(&compressed, hrp);
            out.push(addr.to_string());
        }
    }
    Ok(out)
}

/// The full set of addresses to watch: derived (when an xpub is set) plus the
/// explicit list, de-duplicated (order: derived first, then explicit). Pure.
pub fn resolve_watch_addresses(config: &BtcWatchConfig) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(xpub) = config.xpub.as_deref() {
        for a in derive_watch_addresses(xpub, config.gap)? {
            if !out.iter().any(|e| e == &a) {
                out.push(a);
            }
        }
    }
    for a in &config.addresses {
        if !out.iter().any(|e| e == a) {
            out.push(a.clone());
        }
    }
    Ok(out)
}

/// Confirmed balance of one address in satoshis, parsed from an Esplora
/// `GET /address/{addr}` JSON body. Balance = `chain_stats.funded_txo_sum −
/// spent_txo_sum` (confirmed chain state only; mempool is intentionally
/// excluded so the figure is stable). Returns `None` if the body isn't the
/// expected shape.
pub fn parse_esplora_balance_sats(body: &str) -> Option<i64> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let chain = v.get("chain_stats")?;
    let funded = chain.get("funded_txo_sum")?.as_u64()?;
    let spent = chain.get("spent_txo_sum")?.as_u64()?;
    Some(funded as i64 - spent as i64)
}

/// The BTC/USD last-trade price parsed from Kraken's public ticker body
/// (`result.<pair>.c[0]`, the last trade price, serialised as a string).
/// Returns `None` on a non-empty `error` array or any missing/unparseable
/// field. Only a finite, strictly-positive price is accepted.
pub fn parse_kraken_price(body: &str) -> Option<f64> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    // Kraken reports failures in a top-level `error` array; bail if it's set.
    if let Some(errs) = v.get("error").and_then(|e| e.as_array())
        && !errs.is_empty()
    {
        return None;
    }
    // `result` is an object keyed by the resolved pair name (e.g. XXBTZUSD);
    // take the first (only) entry rather than hard-coding the key.
    let result = v.get("result")?.as_object()?;
    let pair = result.values().next()?;
    let last = pair.get("c")?.as_array()?.first()?.as_str()?;
    let price = last.trim().parse::<f64>().ok()?;
    (price.is_finite() && price > 0.0).then_some(price)
}

/// Convert a satoshi balance to USD at a given BTC/USD price. Returns `None` if
/// the result isn't finite (defensive — the caller never writes a bad row).
pub fn sats_to_usd(sats: i64, btc_usd: f64) -> Option<f64> {
    let usd = (sats as f64 / SATS_PER_BTC) * btc_usd;
    usd.is_finite().then_some(usd)
}

/// Build the Esplora address URL: `{base}/address/{addr}`.
pub fn esplora_address_url(base: &str, addr: &str) -> String {
    format!("{}/address/{}", base.trim_end_matches('/'), addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// The watcher — needs an HTTP client + the Postgres store (db feature)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod watcher {
    use std::sync::Arc;
    use std::time::Duration;

    use tracing::{debug, info, warn};

    use super::{
        BtcWatchConfig, KRAKEN_TICKER_URL, NetWorthSnapshot, SOURCE_ONCHAIN, VENUE_COLD_BTC,
        esplora_address_url, parse_esplora_balance_sats, parse_kraken_price,
        resolve_watch_addresses, sats_to_usd,
    };
    use crate::db::BotRunStore;
    use crate::metrics;

    /// Per-request HTTP timeout. Short so a hung public API can't stall the tick.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Polls the configured BTC addresses' on-chain balances, prices them, and
    /// writes one net-worth snapshot per tick.
    pub struct BtcWatcher {
        client: reqwest::Client,
    }

    impl Default for BtcWatcher {
        fn default() -> Self {
            Self::new()
        }
    }

    impl BtcWatcher {
        pub fn new() -> Self {
            let client = reqwest::Client::builder()
                .timeout(PROBE_TIMEOUT)
                .user_agent(concat!("fks-spawner/", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap_or_default();
            Self { client }
        }

        /// One tick: derive/collect addresses, sum their confirmed balance,
        /// price BTC→USD, write ONE onchain snapshot. BEST-EFFORT: any failure
        /// (bad xpub, esplora down, price down) is logged and the tick is
        /// skipped — never a partial/zero write.
        pub async fn watch_once(&self, config: &BtcWatchConfig, store: &BotRunStore) {
            let addresses = match resolve_watch_addresses(config) {
                Ok(a) => a,
                Err(e) => {
                    // A bad xpub is a config error, not a transient one — warn.
                    warn!(error = %e, "cold-BTC watcher: address resolution failed — skipping tick");
                    return;
                }
            };
            if addresses.is_empty() {
                debug!("cold-BTC watcher: no addresses configured — nothing to do");
                return;
            }

            // Sum confirmed balance across every address. A SINGLE failure aborts
            // the tick: a partial sum would understate net worth and corrupt the
            // series, so we'd rather skip and retry next interval.
            let mut total_sats: i64 = 0;
            for addr in &addresses {
                match self.address_balance_sats(&config.esplora_base, addr).await {
                    Some(sats) => total_sats += sats,
                    None => {
                        debug!(
                            address = %addr,
                            "cold-BTC watcher: balance fetch failed — skipping tick (no partial write)"
                        );
                        return;
                    }
                }
            }

            let Some(price) = self.btc_usd_price().await else {
                warn!("cold-BTC watcher: BTC/USD price unavailable — skipping tick");
                return;
            };

            let Some(usd) = sats_to_usd(total_sats, price) else {
                warn!(
                    sats = total_sats,
                    price, "cold-BTC watcher: non-finite USD value — skipping tick"
                );
                return;
            };

            // Note: the raw BTC quantity (total_sats) is chain-recoverable and
            // logged here for the audit trail; only the USD figure is persisted
            // (the snapshot series is USD-denominated).
            let snap = NetWorthSnapshot::for_account(
                &config.account_id,
                usd,
                "USD",
                Some(VENUE_COLD_BTC.to_string()),
                SOURCE_ONCHAIN,
            );
            match store.record_net_worth(&snap).await {
                Ok(()) => {
                    metrics::NET_WORTH_SNAPSHOTS_TOTAL.inc();
                    debug!(
                        account_id = %config.account_id,
                        addresses = addresses.len(),
                        sats = total_sats,
                        btc_usd = price,
                        usd,
                        "cold-BTC watcher: onchain snapshot recorded"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "cold-BTC watcher: snapshot insert failed");
                }
            }
        }

        /// GET one address's confirmed balance in sats. `None` = unreachable,
        /// non-2xx, or unparseable (all debug-logged, none fatal).
        async fn address_balance_sats(&self, base: &str, addr: &str) -> Option<i64> {
            let url = esplora_address_url(base, addr);
            let resp = match self.client.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    // Strip the request URL from the reqwest error (uniform with
                    // the webhook paths) — the address is logged separately.
                    debug!(address = %addr, error = %reqwest::Error::without_url(e), "cold-BTC watcher: esplora unreachable");
                    return None;
                }
            };
            if !resp.status().is_success() {
                debug!(address = %addr, status = %resp.status(), "cold-BTC watcher: esplora non-2xx");
                return None;
            }
            let body = resp.text().await.ok()?;
            parse_esplora_balance_sats(&body)
        }

        /// GET the BTC/USD spot price off Kraken's public ticker. `None` on any
        /// failure (debug-logged).
        async fn btc_usd_price(&self) -> Option<f64> {
            let resp = match self.client.get(KRAKEN_TICKER_URL).send().await {
                Ok(r) => r,
                Err(e) => {
                    debug!(error = %reqwest::Error::without_url(e), "cold-BTC watcher: kraken ticker unreachable");
                    return None;
                }
            };
            if !resp.status().is_success() {
                debug!(status = %resp.status(), "cold-BTC watcher: kraken ticker non-2xx");
                return None;
            }
            let body = resp.text().await.ok()?;
            parse_kraken_price(&body)
        }
    }

    /// Run the watcher loop forever, one tick every `interval_secs`. Spawned as
    /// a detached background task from `main`; only started when the watcher is
    /// enabled AND a Postgres store is configured.
    pub async fn run_watcher(config: Arc<crate::config::Config>, store: BotRunStore) {
        let btc = &config.btc_watch;
        let interval = Duration::from_secs(btc.interval_secs);
        let watcher = BtcWatcher::new();
        info!(
            interval_secs = btc.interval_secs,
            gap = btc.gap,
            has_xpub = btc.xpub.is_some(),
            explicit_addresses = btc.addresses.len(),
            account_id = %btc.account_id,
            "cold-BTC watcher started"
        );
        loop {
            tokio::time::sleep(interval).await;
            watcher.watch_once(btc, &store).await;
        }
    }
}

#[cfg(feature = "db")]
pub use watcher::{BtcWatcher, run_watcher};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── env / config parsing ─────────────────────────────────────────────────

    #[test]
    fn parse_addresses_env_trims_dedups_and_drops_blanks() {
        assert_eq!(parse_addresses_env(None), Vec::<String>::new());
        assert_eq!(parse_addresses_env(Some("   ")), Vec::<String>::new());
        assert_eq!(
            parse_addresses_env(Some(" bc1qaaa , bc1qbbb ,, bc1qaaa ,bc1qccc")),
            vec![
                "bc1qaaa".to_string(),
                "bc1qbbb".to_string(),
                "bc1qccc".to_string()
            ]
        );
    }

    #[test]
    fn config_enabled_only_with_xpub_or_addresses() {
        let mut c = BtcWatchConfig::default();
        assert!(!c.enabled(), "default is disabled");
        c.addresses = vec!["bc1qaaa".to_string()];
        assert!(c.enabled(), "explicit address enables");
        c.addresses.clear();
        c.xpub = Some("xpub...".to_string());
        assert!(c.enabled(), "xpub enables");
    }

    // ── esplora balance parsing ──────────────────────────────────────────────

    #[test]
    fn parse_esplora_balance_subtracts_spent_from_funded() {
        let body = r#"{
            "address":"bc1qexample",
            "chain_stats":{"funded_txo_count":3,"funded_txo_sum":150000,
                           "spent_txo_count":1,"spent_txo_sum":50000,"tx_count":4},
            "mempool_stats":{"funded_txo_sum":9999,"spent_txo_sum":0}
        }"#;
        // Confirmed balance only: 150000 − 50000 = 100000 sats (mempool ignored).
        assert_eq!(parse_esplora_balance_sats(body), Some(100_000));
    }

    #[test]
    fn parse_esplora_balance_zero_when_all_spent() {
        let body = r#"{"chain_stats":{"funded_txo_sum":42000,"spent_txo_sum":42000}}"#;
        assert_eq!(parse_esplora_balance_sats(body), Some(0));
    }

    #[test]
    fn parse_esplora_balance_none_on_bad_shape() {
        assert_eq!(parse_esplora_balance_sats("not json"), None);
        assert_eq!(parse_esplora_balance_sats("{}"), None);
        assert_eq!(
            parse_esplora_balance_sats(r#"{"chain_stats":{"funded_txo_sum":10}}"#),
            None,
            "missing spent_txo_sum"
        );
    }

    // ── kraken price parsing ─────────────────────────────────────────────────

    #[test]
    fn parse_kraken_price_reads_last_trade() {
        let body = r#"{"error":[],"result":{"XXBTZUSD":{
            "a":["61001.0","1","1.0"],"b":["61000.5","2","2.0"],
            "c":["61000.10","0.00500000"],"v":["100","200"]
        }}}"#;
        assert_eq!(parse_kraken_price(body), Some(61000.10));
    }

    #[test]
    fn parse_kraken_price_none_on_error_array() {
        let body = r#"{"error":["EQuery:Unknown asset pair"],"result":{}}"#;
        assert_eq!(parse_kraken_price(body), None);
    }

    #[test]
    fn parse_kraken_price_none_on_missing_or_bad_fields() {
        assert_eq!(parse_kraken_price("not json"), None);
        assert_eq!(parse_kraken_price(r#"{"error":[],"result":{}}"#), None);
        assert_eq!(
            parse_kraken_price(r#"{"error":[],"result":{"XXBTZUSD":{"c":["-1","0"]}}}"#),
            None,
            "non-positive price rejected"
        );
    }

    // ── sats → usd ───────────────────────────────────────────────────────────

    #[test]
    fn sats_to_usd_converts_whole_and_fractional_btc() {
        // 1 BTC (100M sats) at $60,000 → $60,000.
        assert_eq!(sats_to_usd(100_000_000, 60_000.0), Some(60_000.0));
        // 0.5 BTC at $60,000 → $30,000.
        assert_eq!(sats_to_usd(50_000_000, 60_000.0), Some(30_000.0));
        // Zero balance → $0 (a legit reading, but the watcher never writes a
        // ZERO row from a *skipped* tick — only from a genuinely-empty wallet).
        assert_eq!(sats_to_usd(0, 60_000.0), Some(0.0));
    }

    // ── esplora url ──────────────────────────────────────────────────────────

    #[test]
    fn esplora_url_joins_base_and_address() {
        assert_eq!(
            esplora_address_url("https://blockstream.info/api", "bc1qexample"),
            "https://blockstream.info/api/address/bc1qexample"
        );
        // Trailing slash on the base is tolerated.
        assert_eq!(
            esplora_address_url("https://esplora.local/api/", "bc1qx"),
            "https://esplora.local/api/address/bc1qx"
        );
    }

    // ── xpub derivation (BIP84 test vector) ──────────────────────────────────

    // Account-0 key for the canonical BIP84 mnemonic
    // "abandon abandon abandon abandon abandon abandon abandon abandon abandon
    //  abandon abandon about" (BIP84 §"Test vectors"), re-encoded from its
    // native `zpub…` prefix to the `xpub…` version bytes the `bitcoin` crate's
    // Xpub parser accepts (same key + chain code, different version prefix).
    const BIP84_ACCOUNT_XPUB: &str = "xpub6CatWdiZiodmUeTDp8LT5or8nmbKNcuyvz7WyksVFkKB4RHwCD3XyuvPEbvqAQY3rAPshWcMLoP2fMFMKHPJ4ZeZXYVUhLv1VMrjPC7PW6V";

    // Known first receive (m/.../0/0) + change (m/.../1/0) addresses from the
    // BIP84 spec's test vectors for that account.
    const BIP84_FIRST_RECEIVE: &str = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu";
    const BIP84_FIRST_CHANGE: &str = "bc1q8c6fshw2dlwun7ekn9qwf37cu2rn755upcp6el";

    #[test]
    fn derive_watch_addresses_matches_bip84_vectors() {
        // gap=1 → one receive (0/0) then one change (1/0).
        let addrs = derive_watch_addresses(BIP84_ACCOUNT_XPUB, 1).expect("valid xpub derives");
        assert_eq!(addrs.len(), 2, "gap=1 → receive + change = 2 addresses");
        assert_eq!(addrs[0], BIP84_FIRST_RECEIVE, "m/.../0/0");
        assert_eq!(addrs[1], BIP84_FIRST_CHANGE, "m/.../1/0");
    }

    #[test]
    fn derive_watch_addresses_count_is_two_times_gap() {
        let addrs = derive_watch_addresses(BIP84_ACCOUNT_XPUB, 20).expect("valid xpub");
        assert_eq!(addrs.len(), 40, "20 receive + 20 change");
        // First receive still matches the vector regardless of gap.
        assert_eq!(addrs[0], BIP84_FIRST_RECEIVE);
        // The change branch starts at index `gap` in the flat list.
        assert_eq!(addrs[20], BIP84_FIRST_CHANGE);
    }

    #[test]
    fn derive_watch_addresses_rejects_non_xpub() {
        // A `zpub` (BIP84 native prefix) is rejected — must be re-encoded to xpub.
        let zpub = "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs";
        assert!(derive_watch_addresses(zpub, 1).is_err());
        assert!(derive_watch_addresses("not-a-key", 1).is_err());
    }

    // ── resolve: derived + explicit, de-duplicated ───────────────────────────

    #[test]
    fn resolve_combines_derived_and_explicit_without_dupes() {
        let config = BtcWatchConfig {
            xpub: Some(BIP84_ACCOUNT_XPUB.to_string()),
            // Re-list the first derived receive address: it must NOT double up.
            addresses: vec![BIP84_FIRST_RECEIVE.to_string(), "bc1qextra".to_string()],
            gap: 1,
            ..BtcWatchConfig::default()
        };
        let all = resolve_watch_addresses(&config).expect("resolves");
        // 2 derived (receive+change) + 1 genuinely-new explicit = 3.
        assert_eq!(all.len(), 3, "deduped: {all:?}");
        assert!(all.contains(&"bc1qextra".to_string()));
        // The duplicate receive address appears exactly once.
        assert_eq!(all.iter().filter(|a| *a == BIP84_FIRST_RECEIVE).count(), 1);
    }

    #[test]
    fn resolve_explicit_only_needs_no_xpub() {
        let config = BtcWatchConfig {
            addresses: vec!["bc1qaaa".to_string(), "bc1qbbb".to_string()],
            ..BtcWatchConfig::default()
        };
        assert_eq!(
            resolve_watch_addresses(&config).unwrap(),
            vec!["bc1qaaa".to_string(), "bc1qbbb".to_string()]
        );
    }
}
