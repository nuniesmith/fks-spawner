// =============================================================================
// config.rs — FKS Bot Spawner configuration
//
// All values are read from environment variables with sane defaults.
// Set these in docker-compose.yml or the .env file.
// =============================================================================

use std::collections::HashMap;
use std::env;

use crate::btc_watch::BtcWatchConfig;
use crate::edge_decay::EdgeDecayConfig;
use crate::rithmic_sampler::RithmicSamplerConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Address to bind the HTTP server on.
    pub host: String,
    /// Port to bind the HTTP server on.
    pub port: u16,

    /// Only images whose name starts with this prefix are allowed to be spawned.
    /// Default: "fks-bot-" — prevents arbitrary image execution.
    pub allowed_image_prefix: String,

    /// Hard cap on simultaneously running bot containers.
    pub max_concurrent_bots: usize,

    /// Docker network that all spawned containers must join.
    pub allowed_network: String,

    /// Default CPU quota in fractional cores (e.g. 1.0 = one full core).
    pub default_cpu_limit: f64,

    /// Default memory limit in bytes (derived from DEFAULT_MEMORY_LIMIT_MB).
    pub default_memory_bytes: i64,

    /// Default CPU shares (relative weight; 1024 = normal priority).
    pub default_cpu_shares: i64,

    /// Hard upper bound on a spawn request's `cpu_limit` (fractional cores).
    /// Rejects absurd requests that could starve the host. Env: MAX_CPU_LIMIT.
    pub max_cpu_limit: f64,

    /// Hard upper bound on a spawn request's `memory_limit_mb`.
    /// Env: MAX_MEMORY_LIMIT_MB.
    pub max_memory_mb: i64,

    /// Path where the Prometheus file-based SD config is written.
    pub prometheus_sd_path: String,

    /// Port that each spawned bot container exposes for Prometheus scraping.
    pub bot_metrics_port: u16,

    /// Seconds a stopped container is kept before auto-prune removes it.
    /// Keyed on the container's FINISHED time (via inspect), not created time.
    pub prune_after_secs: i64,

    /// Longer retention (seconds) for QUARANTINED containers: live-mode bots and
    /// any bot that exited unexpectedly (crashed). These are kept for forensics
    /// instead of being fast-pruned like one-shot backtests. Env:
    /// PRUNE_LIVE_AFTER_SECS (default 604800 = 7 days).
    pub prune_live_after_secs: i64,

    /// How often (in seconds) the background reconcile/prune task runs.
    pub prune_interval_secs: u64,

    /// How often (in seconds) the net-worth sampler polls each running bot's
    /// `/status` endpoint and appends a `net_worth_snapshots` row. Coarse by
    /// design (years-horizon backbone, not a live tick). Env:
    /// NET_WORTH_SAMPLE_INTERVAL_SECS. Only runs when the DB is configured.
    pub net_worth_sample_interval_secs: u64,

    /// Per-bot override for how often THAT bot is expected to refresh its own
    /// `/status` freshness stamp (`updated` / `positions[].updated`) — the
    /// bot's marking cadence, which is NOT the same thing as
    /// `net_worth_sample_interval_secs` (how often WE poll it). Keyed by
    /// `fks.bot_id`. Parsed from `NET_WORTH_BOT_CADENCE_SECS`:
    /// `bot_id=seconds[,bot_id=seconds...]`, e.g.
    /// `crypto-funding=3600` for a bot whose candle-driven position marks
    /// only advance once an hour by design (`granularity = "60"` minutes in
    /// its own config — see `crate::net_worth::expected_mark_cadence_secs`
    /// and the 2026-08-16/17 measurement in that module's doc comment: the
    /// funding bot's readings were refused as stale for ~50 minutes of every
    /// hour because the sampler judged an hourly-marking bot against a
    /// 10-minute tolerance derived from the global 300s poll interval).
    ///
    /// A `bot_id` ABSENT from this map falls back to
    /// `net_worth_sample_interval_secs` — the pre-existing, TIGHTER default —
    /// not to something more tolerant: an unrecognised/unconfigured cadence
    /// must fail toward the conservative behaviour that predates this field
    /// (refuse promptly), never toward infinite patience. Malformed entries
    /// (non-numeric, zero, or an empty bot_id) are dropped individually
    /// rather than invalidating the whole map, and a dropped entry falls back
    /// to the same conservative default.
    pub bot_mark_cadence_secs: HashMap<String, u64>,

    /// Milestone step (in the net-worth currency, USD) for the
    /// `net_worth_milestone` event: the sampler notifies when total net worth
    /// crosses an integer multiple of this, in either direction. `0.0` (default)
    /// = OFF. Env: NET_WORTH_MILESTONE_STEP. See `crate::net_worth`.
    pub net_worth_milestone_step: f64,

    /// Postgres connection string. Empty = stateless mode (no DB writes).
    /// Recognised env vars (in order): SPAWNER_DATABASE_URL, DATABASE_URL.
    pub database_url: String,

    /// Postgres URL handed to backtest containers as their BACKTEST_DB_URL —
    /// a SCOPED, low-privilege role (fks_backtest: UPDATE on its own
    /// backtest_runs row, nothing else) so a malicious or compromised
    /// backtest image can't read exchange_secrets or rewrite the treasury
    /// ledger. Empty = fall back to `database_url` (the spawner's own full
    /// fks_user credentials) with a loud warning per run — functional but
    /// visibly degraded. Env: BACKTEST_DB_URL.
    pub backtest_database_url: String,

    /// Shared secret nginx injects on internal traffic via
    /// `proxy_set_header X-Internal-Token "${NGINX_INTERNAL_TOKEN}"`.
    /// When this is non-empty, all routes except `/health` and `/metrics`
    /// reject requests that don't carry the matching header value.
    /// Empty = no auth (dev mode); the disabled posture is logged loudly at
    /// startup (see `crate::auth::check_internal_auth_posture`) so a
    /// misconfigured prod box can never fail open silently.
    pub internal_token: String,

    /// Hardened posture: when `true`, an empty `internal_token` is a fatal
    /// misconfiguration — the spawner refuses to boot rather than serving the
    /// money-adjacent routes unauthenticated. Default `false` (dev passthrough
    /// with a loud warning). Env: REQUIRE_INTERNAL_TOKEN. Set this in any
    /// deployment where the spawner port could be reachable without the nginx
    /// hop that injects `X-Internal-Token`.
    pub require_internal_auth: bool,

    /// SCOPED bot→spawner ingest token (plan-03 D2). When non-empty, `POST
    /// /events` accepts EITHER this value OR `internal_token` in the
    /// `X-Internal-Token` header — and NO other route accepts it (the
    /// blast-radius property: a compromised bot holding this token can open
    /// ONLY the events mailbox, never `/spawn`, `/secrets`, `/transfers`, …).
    /// FAIL-CLOSED: empty (the default) DISABLES the scoped path entirely —
    /// only the internal token opens `/events`, an unset token is never an open
    /// door. Also gates env injection: empty ⇒ spawned bots receive NO
    /// `SPAWNER_EVENTS_*` env (additive, zero behaviour change until set). The
    /// value is NEVER logged. Env: EVENTS_TOKEN.
    pub events_token: String,

    /// The `/events` ingest URL injected into spawned bots as
    /// `SPAWNER_EVENTS_URL` (alongside `SPAWNER_EVENTS_TOKEN`) when
    /// `events_token` is non-empty. Default is the spawner's container-DNS name
    /// on `fks_network` (`http://fks_bot_spawner:8090/events`) — the address a
    /// sibling bot container reaches it at. Env: SPAWNER_EVENTS_URL.
    pub events_url: String,

    /// Whether bot-lifecycle events are dispatched to configured notification
    /// channels (Discord webhooks). Opt-out: defaults to `true`. With zero
    /// channels configured it is a cheap no-op regardless; set
    /// `NOTIFY_ENABLED=false` to hard-disable the sender. Env: NOTIFY_ENABLED.
    pub notify_enabled: bool,

    /// Cold-BTC on-chain watcher (read-only, source='onchain'). OFF unless
    /// BTC_WATCH_XPUB and/or BTC_WATCH_ADDRESSES is configured. See
    /// `crate::btc_watch`.
    pub btc_watch: BtcWatchConfig,

    /// Rithmic account-balance sampler (read-only, source='rithmic'). OFF unless
    /// RITHMIC_SAMPLER_URL is set. See `crate::rithmic_sampler`.
    pub rithmic_sampler: RithmicSamplerConfig,

    /// Weekly edge-backtest scheduler (EDGE-DECAY DETECTION). OFF unless
    /// EDGE_DECAY_ENABLED=true; fires each active containerized edge's backtest
    /// on a weekly cadence so the advisor's Sunday report has a fresh point to
    /// compare. See `crate::edge_decay`.
    pub edge_decay: EdgeDecayConfig,

    /// Boot-time bot reconciliation (`crate::boot_reconcile`). ON by default —
    /// the whole point is safety-by-default: after a host reboot, live-money
    /// bot containers spawned with no restart policy don't come back at all,
    /// and nothing else re-reads what was running before and brings it back.
    /// At startup, a saved config whose last `bot_runs` row was left OPEN
    /// (never cleanly stopped via the API) and that isn't already running in
    /// Docker is respawned from that config. Set `BOOT_RECONCILE_ENABLED=false`
    /// to disable for a deploy scenario that wants manual control instead. Env:
    /// BOOT_RECONCILE_ENABLED.
    pub boot_reconcile_enabled: bool,
}

impl Config {
    pub fn from_env() -> Self {
        let default_memory_mb: i64 = env_parse("DEFAULT_MEMORY_LIMIT_MB", 512);
        Self {
            host: env::var("SPAWNER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env_parse("SPAWNER_PORT", 8090),
            allowed_image_prefix: env::var("ALLOWED_IMAGE_PREFIX")
                .unwrap_or_else(|_| "fks-bot-".to_string()),
            max_concurrent_bots: env_parse("MAX_CONCURRENT_BOTS", 20),
            allowed_network: env::var("ALLOWED_NETWORK")
                .unwrap_or_else(|_| "fks_network".to_string()),
            default_cpu_limit: env_parse_f64("DEFAULT_CPU_LIMIT", 1.0),
            default_memory_bytes: default_memory_mb * 1024 * 1024,
            default_cpu_shares: env_parse("DEFAULT_CPU_SHARES", 1024),
            max_cpu_limit: env_parse_f64("MAX_CPU_LIMIT", 8.0),
            max_memory_mb: env_parse("MAX_MEMORY_LIMIT_MB", 16384),
            prometheus_sd_path: env::var("PROMETHEUS_SD_PATH")
                .unwrap_or_else(|_| "/prometheus-sd/bots.json".to_string()),
            bot_metrics_port: env_parse("BOT_METRICS_PORT", 9091),
            prune_after_secs: env_parse("PRUNE_AFTER_SECS", 300),
            prune_live_after_secs: env_parse("PRUNE_LIVE_AFTER_SECS", 604_800),
            prune_interval_secs: env_parse("PRUNE_INTERVAL_SECS", 60),
            net_worth_sample_interval_secs: env_parse(
                "NET_WORTH_SAMPLE_INTERVAL_SECS",
                crate::net_worth::DEFAULT_SAMPLE_INTERVAL_SECS,
            ),
            bot_mark_cadence_secs: parse_bot_cadence_map(
                &env::var("NET_WORTH_BOT_CADENCE_SECS").unwrap_or_default(),
            ),
            net_worth_milestone_step: env_parse_f64("NET_WORTH_MILESTONE_STEP", 0.0),
            database_url: env::var("SPAWNER_DATABASE_URL")
                .or_else(|_| env::var("DATABASE_URL"))
                .unwrap_or_default(),
            backtest_database_url: env::var("BACKTEST_DB_URL").unwrap_or_default(),
            internal_token: env::var("NGINX_INTERNAL_TOKEN").unwrap_or_default(),
            require_internal_auth: env_parse_bool("REQUIRE_INTERNAL_TOKEN", false),
            events_token: env::var("EVENTS_TOKEN").unwrap_or_default(),
            events_url: env::var("SPAWNER_EVENTS_URL")
                .unwrap_or_else(|_| "http://fks_bot_spawner:8090/events".to_string()),
            notify_enabled: env_parse_bool("NOTIFY_ENABLED", true),
            btc_watch: BtcWatchConfig::from_env(),
            rithmic_sampler: RithmicSamplerConfig::from_env(),
            edge_decay: EdgeDecayConfig::from_env(),
            boot_reconcile_enabled: env_parse_bool("BOOT_RECONCILE_ENABLED", true),
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_parse_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Parse `NET_WORTH_BOT_CADENCE_SECS` — a comma-separated `bot_id=seconds`
/// list — into the per-bot cadence override map. Pure so it is unit-testable
/// without mutating process env (see the `bot_cadence_map_*` tests below).
///
/// Each entry is validated independently: an empty/whitespace-only bot_id, an
/// unparsable seconds value, or a zero/negative value is DROPPED rather than
/// poisoning the whole map — a fat-fingered single entry degrades that one
/// bot to the conservative default (see `Config::bot_mark_cadence_secs`),
/// never to a map so broken every bot loses its override. Later duplicate
/// bot_ids overwrite earlier ones (last-wins, matching `env`/`labels`
/// parsing elsewhere in this crate).
fn parse_bot_cadence_map(raw: &str) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((bot_id, secs)) = entry.split_once('=') else {
            continue;
        };
        let bot_id = bot_id.trim();
        if bot_id.is_empty() {
            continue;
        }
        let Ok(secs) = secs.trim().parse::<u64>() else {
            continue;
        };
        if secs == 0 {
            continue;
        }
        map.insert(bot_id.to_string(), secs);
    }
    map
}

/// Parse a boolean env flag. Accepts the common truthy/falsey spellings
/// (`true/false`, `1/0`, `yes/no`, `on/off`, case-insensitive); anything
/// unrecognised falls back to `default`.
fn env_parse_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_env` returns sensible defaults when no env vars are set.
    /// Note: this test runs in the same process as other tests so it cannot
    /// safely mutate the environment; we only assert defaults that are not
    /// overridden by the test runner.
    #[test]
    fn defaults_are_safe() {
        // Build a Config manually rather than from_env() to avoid env coupling.
        let cfg = Config {
            host: "0.0.0.0".to_string(),
            port: 8090,
            allowed_image_prefix: "fks-bot-".to_string(),
            max_concurrent_bots: 20,
            allowed_network: "fks_network".to_string(),
            default_cpu_limit: 1.0,
            default_memory_bytes: 512 * 1024 * 1024,
            default_cpu_shares: 1024,
            max_cpu_limit: 8.0,
            max_memory_mb: 16384,
            prometheus_sd_path: "/prometheus-sd/bots.json".to_string(),
            bot_metrics_port: 9091,
            prune_after_secs: 300,
            prune_live_after_secs: 604_800,
            prune_interval_secs: 60,
            net_worth_sample_interval_secs: 300,
            bot_mark_cadence_secs: HashMap::new(),
            net_worth_milestone_step: 0.0,
            database_url: String::new(),
            backtest_database_url: String::new(),
            internal_token: String::new(),
            require_internal_auth: false,
            events_token: String::new(),
            events_url: "http://fks_bot_spawner:8090/events".to_string(),
            notify_enabled: true,
            btc_watch: BtcWatchConfig::default(),
            rithmic_sampler: RithmicSamplerConfig::default(),
            edge_decay: EdgeDecayConfig::default(),
            boot_reconcile_enabled: true,
        };
        assert_eq!(cfg.bind_addr(), "0.0.0.0:8090");
        assert!(
            cfg.database_url.is_empty(),
            "DB should default to stateless"
        );
        assert!(cfg.allowed_image_prefix.starts_with("fks-bot-"));
    }

    #[test]
    fn bind_addr_formats_correctly() {
        let cfg = Config {
            host: "127.0.0.1".to_string(),
            port: 12345,
            allowed_image_prefix: "x".into(),
            max_concurrent_bots: 1,
            allowed_network: "n".into(),
            default_cpu_limit: 0.5,
            default_memory_bytes: 1,
            default_cpu_shares: 1,
            max_cpu_limit: 8.0,
            max_memory_mb: 16384,
            prometheus_sd_path: "/x".into(),
            bot_metrics_port: 9091,
            prune_after_secs: 0,
            prune_live_after_secs: 604_800,
            prune_interval_secs: 0,
            net_worth_sample_interval_secs: 0,
            bot_mark_cadence_secs: HashMap::new(),
            net_worth_milestone_step: 0.0,
            database_url: String::new(),
            backtest_database_url: String::new(),
            internal_token: String::new(),
            require_internal_auth: false,
            events_token: String::new(),
            events_url: "http://fks_bot_spawner:8090/events".to_string(),
            notify_enabled: true,
            btc_watch: BtcWatchConfig::default(),
            rithmic_sampler: RithmicSamplerConfig::default(),
            edge_decay: EdgeDecayConfig::default(),
            boot_reconcile_enabled: true,
        };
        assert_eq!(cfg.bind_addr(), "127.0.0.1:12345");
    }

    // ── NET_WORTH_BOT_CADENCE_SECS parsing ──────────────────────────────────

    #[test]
    fn bot_cadence_map_parses_valid_pairs() {
        let map = parse_bot_cadence_map("crypto-funding=3600,crypto-spot=300");
        assert_eq!(map.get("crypto-funding"), Some(&3600));
        assert_eq!(map.get("crypto-spot"), Some(&300));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn bot_cadence_map_empty_string_is_empty_map() {
        assert!(parse_bot_cadence_map("").is_empty());
        assert!(parse_bot_cadence_map("   ").is_empty());
    }

    #[test]
    fn bot_cadence_map_drops_malformed_entries_without_poisoning_the_rest() {
        // No '=', empty bot_id, non-numeric seconds, and a zero cadence are
        // each dropped individually; the one well-formed entry still lands.
        let map = parse_bot_cadence_map(
            "crypto-funding=3600, no-equals-sign, =900, crypto-broken=abc, crypto-zero=0",
        );
        assert_eq!(
            map.len(),
            1,
            "only the well-formed entry should survive: {map:?}"
        );
        assert_eq!(map.get("crypto-funding"), Some(&3600));
    }

    #[test]
    fn bot_cadence_map_last_duplicate_wins() {
        let map = parse_bot_cadence_map("crypto-funding=3600,crypto-funding=7200");
        assert_eq!(map.get("crypto-funding"), Some(&7200));
    }
}
