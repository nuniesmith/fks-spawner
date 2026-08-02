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
    /// The bot's OWN `updated` epoch-seconds stamp for this figure, when it
    /// publishes one. `None` = the bot does not report freshness, so the
    /// reading cannot be staleness-checked (recorded, as before).
    pub updated: Option<u64>,
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
/// A `bot_status` row that was recorded from a FROZEN reading, before the
/// staleness guard existed (see src/sql/spawner/015_net_worth_stale_provenance.sql).
/// Retained for the audit trail; EXCLUDED from every read path so derived
/// figures are computed only from readings that were actually fresh.
pub const SOURCE_BOT_STATUS_STALE: &str = "bot_status_stale";
pub const SOURCE_ONCHAIN: &str = "onchain";
pub const SOURCE_RITHMIC: &str = "rithmic";
pub const SOURCE_MANUAL: &str = "manual";

// ─────────────────────────────────────────────────────────────────────────────
// Net-worth milestone detection (pure — unit-tested; wired into the sampler)
// ─────────────────────────────────────────────────────────────────────────────

/// Hysteresis as a fraction of the milestone step (10%): a crossing must clear
/// the next boundary by this much before it fires, which kills notification spam
/// when the total oscillates right on a boundary. See [`detect_milestone`].
pub const MILESTONE_HYSTERESIS_FRAC: f64 = 0.10;

/// A milestone crossing and its direction (the boundary value crossed).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MilestoneCross {
    /// Total rose through this boundary (green, a gain).
    Up(f64),
    /// Total fell through this boundary (amber, a drawdown).
    Down(f64),
}

/// The result of feeding a fresh total to the milestone detector: an optional
/// crossing to notify, plus the `last` milestone anchor to persist for the next
/// tick (unchanged when nothing crossed).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MilestoneUpdate {
    pub cross: Option<MilestoneCross>,
    pub last: f64,
}

/// PURE milestone-crossing detector (total-only v1, both directions).
///
/// - `last` is the last-announced milestone boundary (a multiple of `step`; on
///   the very first sample the caller baselines it to `current`'s boundary so
///   process start never fires — a restart re-baselines, which is acceptable).
/// - `current` is the fresh total.
/// - `step` is `NET_WORTH_MILESTONE_STEP`; `<= 0` (or a non-finite total)
///   disables detection entirely (`cross: None`, `last` unchanged).
/// - `hysteresis` (e.g. `step * MILESTONE_HYSTERESIS_FRAC`) is how far past the
///   NEXT boundary `current` must move before a crossing counts. Because the
///   anchor snaps to the crossed boundary, sub-step jitter never re-fires and
///   the hysteresis band absorbs jitter sitting exactly on a boundary.
///
/// Fires at most one crossing per call (the boundary `current` snapped to), so a
/// multi-step jump announces the furthest boundary reached rather than spamming
/// one row per intervening step.
pub fn detect_milestone(last: f64, current: f64, step: f64, hysteresis: f64) -> MilestoneUpdate {
    if step <= 0.0 || !current.is_finite() || !last.is_finite() {
        return MilestoneUpdate { cross: None, last };
    }
    if current >= last + step + hysteresis {
        // Snap to the highest whole multiple of `step` at or below `current`.
        let boundary = (current / step).floor() * step;
        return MilestoneUpdate {
            cross: Some(MilestoneCross::Up(boundary)),
            last: boundary,
        };
    }
    if current <= last - step - hysteresis {
        // Snap to the lowest whole multiple of `step` at or above `current`.
        let boundary = (current / step).ceil() * step;
        return MilestoneUpdate {
            cross: Some(MilestoneCross::Down(boundary)),
            last: boundary,
        };
    }
    MilestoneUpdate { cross: None, last }
}

/// Baseline anchor for the FIRST observed total after (re)start: the whole
/// multiple of `step` at or below `current`, so the next tick only fires on a
/// genuine new crossing. `step <= 0` yields `0.0` (detection is OFF anyway).
pub fn milestone_baseline(current: f64, step: f64) -> f64 {
    if step <= 0.0 || !current.is_finite() {
        return 0.0;
    }
    (current / step).floor() * step
}

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
/// Keys a bot may use to stamp when its status figure was produced.
const UPDATED_KEYS: [&str; 4] = ["updated", "updated_at", "ts", "timestamp"];

/// Wall-clock epoch seconds (the same basis the bots stamp `updated` with).
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether a reading is too old to record as a fresh sample.
///
/// On 2026-07-22 the live spot bot lost DNS for every venue for 65 minutes.
/// Its `/status` kept serving the last good figure, the sampler recorded that
/// frozen number every 5 minutes stamped `ts=NOW()`, and the durable treasury
/// series gained **15 consecutive rows identical to 8 decimal places**
/// (206.26301554) spanning 75 minutes — a flat plateau that never happened,
/// feeding the D5 profit decomposition and the milestone detector. Nothing
/// about the write looked wrong; the row was simply not true.
///
/// A reading older than `max_age_secs` is refused rather than recorded. Gaps
/// are honest; fabricated flat rows are not. Returns false when the bot
/// publishes no stamp (nothing to check) or when clocks disagree such that
/// the stamp is in the future (treated as fresh — a skewed clock must not
/// silently blank the series).
pub fn reading_is_stale(updated: Option<u64>, now_secs: u64, max_age_secs: u64) -> bool {
    match updated {
        None => false,
        Some(u) if u > now_secs => false,
        Some(u) => now_secs.saturating_sub(u) > max_age_secs,
    }
}

pub fn parse_status_net_worth(body: &str) -> Option<NetWorthReading> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let net_worth = NET_WORTH_KEYS
        .iter()
        .find_map(|k| v.get(*k).and_then(value_as_f64))?;
    let currency = first_string(&v, CURRENCY_KEYS).unwrap_or_else(|| "USD".to_string());
    let venue = first_string(&v, VENUE_KEYS);
    // The bot stamps every snapshot it publishes (crypto-bot-core status.rs).
    // Ignoring it is how a FROZEN reading became 15 "fresh" rows on 2026-07-22.
    //
    // The production spot bot publishes NO root-level stamp — freshness lives
    // per-venue in `exchanges[]`. And `net_worth_usd` is a SUM across those
    // venues, so it is only as fresh as its STALEST component: one dead venue
    // makes the total wrong even while the others tick. Hence min() over the
    // per-venue stamps, with a root-level stamp used only as a fallback for
    // bots that publish one (crypto-demo, fks-bot-example).
    // Gate ONLY on venues in `live` mode.
    //
    // A stale stamp means two very different things depending on the bot. A
    // LIVE venue reports a real broker balance and refreshes every cycle
    // (~90s observed), so an old stamp means the bot cannot reach it — it is
    // BLIND and its net worth is frozen. A PAPER bot marks its book on trade
    // events, so its account snapshot is legitimately hours old while nothing
    // happens — it is IDLE and its net worth is perfectly correct.
    //
    // Conflating the two took the funding bot's treasury series offline for
    // 15 hours on 2026-07-27: 176 consecutive skips against a stamp that was
    // 15.5h old BY DESIGN. Idle is not blind.
    let updated = real_money_venue_stamps(&v).into_iter().min().or_else(|| {
        UPDATED_KEYS
            .iter()
            .find_map(|k| v.get(*k).and_then(value_as_f64))
            .filter(|n| *n > 0.0)
            .map(|n| n as u64)
    });
    Some(NetWorthReading {
        net_worth,
        currency,
        venue,
        updated,
    })
}

/// Does a REAL-MONEY bot report a PAPER venue?
///
/// spot-portfolio silently degrades a venue to paper when its build-time key
/// check fails — bad key after a rotation, or the venue's auth endpoint
/// erroring during the respawn (portfolio.rs:157-183, `Some(new_paper(&targets,
/// cfg.paper_usd))` on both the missing-keys and balances-error paths). That
/// paper venue is seeded with `paper_usd` FAKE cash, and
/// `StatusState::net_worth()` sums every venue's total_value indiscriminately
/// — so the published net worth silently contains money that does not exist.
///
/// Every other guard here waves it through BY DESIGN: paper venues are exempt
/// from the staleness stamp (#35/#36), count toward completeness (#38), and
/// are excluded from the freshness alerts (fks #238). Paper was treated as
/// harmless because a paper BOT is harmless. A paper VENUE on a live bot is
/// the opposite: it is the signature of a credential failure, and its balance
/// is fiction.
///
/// `bot.mode != "paper" && any(venue.mode == "paper")` is exactly that
/// signature. A wholly paper bot (crypto-funding) is unaffected.
pub fn has_fake_paper_venue(body: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let bot_mode = first_string(&v, &["mode"]).unwrap_or_default();
    if bot_mode.eq_ignore_ascii_case("paper") {
        return false; // a paper bot's paper venues are legitimate
    }
    venue_entries(&v)
        .into_iter()
        .any(|e| e.mode.eq_ignore_ascii_case("paper"))
}

/// Is the served venue set COMPLETE?
///
/// A bot's venue map only gains an entry after that venue's first SUCCESSFUL
/// cycle (spot-portfolio `update_venue` runs at the END of a cycle, and the
/// cycle short-circuits when a price/balance fetch fails). So a venue that is
/// down when the process starts — an exchange outage during the respawn that
/// follows a key rotation, say — is simply ABSENT from `exchanges[]`, and
/// `net_worth_usd` is a partial sum of only the venues that did report.
///
/// Every other guard here is blind to that: the venues that ARE present carry
/// fresh stamps, so the reading looks perfectly healthy while silently
/// omitting an entire exchange's balance. This is the 2026-07-22 failure with
/// a different mechanism — a number that is WRONG rather than OLD, and harder
/// to spot because it still moves.
///
/// `None` when the bot publishes no `expected_venues` (older builds): unknown,
/// so recorded as before rather than blanked.
pub fn venue_set_is_complete(body: &str) -> Option<bool> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let expected = v.get("expected_venues").and_then(value_as_f64)? as usize;
    if expected == 0 {
        return None;
    }
    Some(venue_entries(&v).len() >= expected)
}

/// How many open positions does the bot report?
///
/// Prefers the published `open_positions` COUNT (crypto-bot-core status.rs)
/// and falls back to the length of the `positions[]` array that older builds
/// publish, so this reads both shapes.
fn open_position_count(v: &serde_json::Value) -> usize {
    if let Some(n) = v.get("open_positions").and_then(value_as_f64) {
        return n.max(0.0) as usize;
    }
    v.get("positions")
        .and_then(|a| a.as_array())
        .map_or(0, Vec::len)
}

/// Does the bot hold open positions that its own net-worth figure does not
/// account for?
///
/// `net_worth_usd` meant two different things depending on the bot's shape.
/// The SPOT bot's venue totals are `cash + Σ holdings[].value` — a genuine
/// mark-to-market portfolio value. The FUTURES/funding bot's venue snapshot is
/// its paper ledger's REALIZED equity (`cash == total_value`, `holdings: []`),
/// which moves only when a round trip closes; the open trade lives in a
/// separate `positions[]` array the total never touched. `/treasury` and
/// `check_net_worth_milestone` SUM THEM TOGETHER.
///
/// Observed on the live paper funding bot 2026-08-02: `net_worth_usd`
/// 11288.31 with an open AVAXUSDTM long at +6.889% on 3000 USDT notional —
/// ~207 USDT (1.8%) of the book missing from the recorded "net worth", and
/// `positions[].updated` 14 hours fresher than `exchanges[].updated`. The
/// direction is what makes it dangerous: an ADVERSE open position makes the
/// treasury series show NO DRAWDOWN AT ALL until the trade closes — a flat
/// line indistinguishable from a bot that is doing fine. Unbounded in
/// principle, and precisely the shape of every other failure this week: a
/// number that looks authoritative and is not what it claims.
///
/// The spawner CANNOT repair this from outside. `ret_pct` is a ratio, not
/// dollars, and whether a venue total already marks positions to market is
/// bot knowledge (a live exchange's account equity does; a paper ledger's
/// booked equity does not) — inferring it would manufacture exactly the kind
/// of plausible-looking figure this guard exists to refuse. So the bot
/// declares, via `net_worth_usd_complete`, and this refuses what the bot
/// cannot vouch for:
///
///   - `net_worth_usd_complete: false` — the bot says the figure omits an
///     open position. Refuse.
///   - field ABSENT with open positions — a build predating the contract. It
///     cannot vouch for the figure either way, so refuse.
///   - field absent, no open positions — the spot bot and every non-futures
///     bot. Recorded exactly as before.
///
/// A gap in the series is honest; a treasury that cannot show a drawdown is
/// not.
pub fn open_positions_unaccounted(body: &str) -> bool {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    match v.get("net_worth_usd_complete").and_then(|c| c.as_bool()) {
        // The bot vouches for its own figure (or says it cannot).
        Some(complete) => !complete,
        // No declaration: only a bot carrying open positions is at risk.
        None => open_position_count(&v) > 0,
    }
}

/// `updated` stamps for venues holding REAL money.
///
/// The spot bot serves `exchanges: [{exchange, mode, total_value, updated}]`
/// (crypto-bot-core `VenueStatus`); older/other shapes use `venues`. Paper
/// Excludes ONLY `paper`. The test is "is this real money", NOT "may this
/// venue place orders" — a spot venue reports `dry-run` when it is outside the
/// live_venues allowlist OR when the drawdown breaker has halted it
/// (`vlive = venue.live && !halted`, spot-portfolio portfolio.rs:342), and in
/// both cases it still holds and reports REAL balances ("real balances, logs
/// would-be orders"). Keying on `live` disengaged this guard precisely when a
/// venue got halted — the moment its figures matter most.
///
/// An empty result means "nothing here can be checked", which reads as fresh
/// rather than blanking the series.
fn real_money_venue_stamps(v: &serde_json::Value) -> Vec<u64> {
    venue_entries(v)
        .into_iter()
        .filter(|e| !e.mode.eq_ignore_ascii_case("paper"))
        .filter_map(|e| e.updated)
        .collect()
}

/// One venue's freshness, for both the staleness decision and the
/// per-venue Prometheus gauge (P-26).
#[derive(Debug, Clone, PartialEq)]
pub struct VenueFreshness {
    pub exchange: String,
    pub mode: String,
    pub updated: Option<u64>,
}

/// Parse the per-venue array out of a `/status` body. Empty when the bot
/// publishes no venue breakdown.
pub fn parse_venue_freshness(body: &str) -> Vec<VenueFreshness> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .map(|v| venue_entries(&v))
        .unwrap_or_default()
}

fn venue_entries(v: &serde_json::Value) -> Vec<VenueFreshness> {
    const VENUE_ARRAY_KEYS: [&str; 2] = ["exchanges", "venues"];
    VENUE_ARRAY_KEYS
        .iter()
        .find_map(|k| v.get(*k).and_then(|a| a.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    let exchange = first_string(e, &["exchange", "venue", "name"])?;
                    Some(VenueFreshness {
                        exchange,
                        mode: first_string(e, &["mode"]).unwrap_or_else(|| "?".to_string()),
                        updated: e
                            .get("updated")
                            .and_then(value_as_f64)
                            .filter(|n| *n > 0.0)
                            .map(|n| n as u64),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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

    use super::{
        MILESTONE_HYSTERESIS_FRAC, MilestoneCross, NetWorthSnapshot, detect_milestone,
        has_fake_paper_venue, milestone_baseline, now_epoch_secs, open_positions_unaccounted,
        parse_status_net_worth, parse_venue_freshness, reading_is_stale, running_status_targets,
        venue_set_is_complete,
    };
    use crate::config::Config;
    use crate::db::BotRunStore;
    use crate::docker_client::DockerOps;
    use crate::metrics;
    use crate::notifications::{NotificationDispatcher, NotificationEvent};

    /// Per-bot HTTP timeout. Short so one hung/slow bot can never stall the
    /// sweep of the others.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

    /// How many delivery-ledger rows to retain; the rest are pruned on the
    /// sampler tick (the established piggyback pattern — the backtest staleness
    /// sweep already rides here).
    const NOTIFICATION_LOG_KEEP: i64 = 5000;

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

            // Retire the venue gauge for bots that are no longer running.
            // probe() — the only other path that touches the gauge — is called
            // for RUNNING bots only, so without this a stopped/crashed/removed
            // bot keeps exporting its last ages forever. A bot stopped while a
            // venue sat above the threshold would pin BotVenueStale on the
            // money channel permanently.
            //
            // Deliberately placed AFTER a SUCCESSFUL list_bots: on a Docker API
            // error we return above without touching the gauge, because
            // retiring every bot on a transient failure would flap the series
            // (and BotVenueFreshnessMissing) once a minute.
            let running_ids: Vec<String> = targets.iter().map(|(id, _)| id.clone()).collect();
            metrics::retire_absent_bots(&running_ids);
            debug!(
                count = targets.len(),
                "net-worth sampler: polling running bots"
            );

            for (bot_id, url) in targets {
                let Some(reading) = self.probe(&bot_id, &url).await else {
                    // Bot doesn't expose net worth (or was unreachable) — skip.
                    continue;
                };
                // Refuse a figure the BOT ITSELF stamps as old. Tolerate two
                // sample intervals (a single missed cycle is normal jitter);
                // beyond that the bot is serving a frozen value and writing it
                // with ts=NOW() manufactures history. A GAP in the series is
                // honest; a flat plateau that never happened is not.
                let max_age = config.net_worth_sample_interval_secs.max(1) * 2;
                if reading_is_stale(reading.updated, now_epoch_secs(), max_age) {
                    let age = reading
                        .updated
                        .map(|u| now_epoch_secs().saturating_sub(u))
                        .unwrap_or(0);
                    warn!(
                        bot_id = %bot_id,
                        age_secs = age,
                        max_age_secs = max_age,
                        "net-worth sampler: /status figure is STALE — skipping rather than \
                         recording a frozen value as fresh (see the 2026-07-22 DNS blackout, \
                         which wrote 15 identical rows into the treasury series)"
                    );
                    metrics::NET_WORTH_STALE_SKIPPED_TOTAL
                        .with_label_values(&[metrics::refusal::STALE])
                        .inc();
                    continue;
                }
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

            // ── Piggyback: notification-log retention ────────────────────────
            // The delivery ledger grows one row per webhook send; trim it beyond
            // the newest N here rather than with a SQL cron job (same tick, same
            // best-effort discipline as the backtest sweep above).
            match store.prune_notification_log(NOTIFICATION_LOG_KEEP).await {
                Ok(0) => {}
                Ok(n) => debug!(pruned = n, "notification-log retention sweep"),
                Err(e) => warn!(error = %e, "notification-log retention sweep failed"),
            }
        }

        /// GET one bot's `/status` and parse its net worth. `None` = unreachable,
        /// non-2xx, unreadable, or no recognised net-worth field (all debug-logged,
        /// none fatal).
        async fn probe(&self, bot_id: &str, url: &str) -> Option<super::NetWorthReading> {
            let resp = match self.client.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    debug!(bot_id = %bot_id, error = %reqwest::Error::without_url(e), "net-worth sampler: /status unreachable");
                    metrics::retire_venue_ages(bot_id);
                    return None;
                }
            };
            if !resp.status().is_success() {
                debug!(
                    bot_id = %bot_id,
                    status = %resp.status(),
                    "net-worth sampler: /status non-2xx"
                );
                metrics::retire_venue_ages(bot_id);
                return None;
            }
            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => {
                    debug!(bot_id = %bot_id, error = %e, "net-worth sampler: /status body unreadable");
                    metrics::retire_venue_ages(bot_id);
                    return None;
                }
            };
            // Per-venue freshness (P-26). Rides the /status body we already
            // fetched — no extra request. Exported even when the bot publishes
            // no net worth, because a dead venue is worth seeing regardless.
            let now = now_epoch_secs();
            let ages: Vec<(String, String, f64)> = parse_venue_freshness(&body)
                .into_iter()
                .filter_map(|v| {
                    v.updated
                        .map(|u| (v.exchange, v.mode, now.saturating_sub(u) as f64))
                })
                .collect();
            // ALWAYS publish, even when empty. The retirement pass lives inside
            // set_venue_ages, so skipping the call on an empty set was exactly
            // what let a vanished venue keep exporting its last age forever —
            // a frozen gauge reads as FRESH and silently un-fires the very
            // alerts it feeds. A gauge that lies is worse than no gauge.
            metrics::set_venue_ages(bot_id, &ages);

            // A real-money bot reporting a PAPER venue is a credential failure
            // wearing a healthy face: that venue holds fabricated cash which is
            // summed into net_worth_usd. Refuse before anything else looks at
            // the number — it is not merely stale or partial, it is FICTION.
            if has_fake_paper_venue(&body) {
                warn!(
                    bot_id = %bot_id,
                    "net-worth sampler: a REAL-MONEY bot is reporting a PAPER venue \
                     — its keys failed validation and it is running on fabricated \
                     cash. Refusing the sample; fix the venue's credentials."
                );
                metrics::NET_WORTH_STALE_SKIPPED_TOTAL
                    .with_label_values(&[metrics::refusal::FAKE_PAPER])
                    .inc();
                return None;
            }

            // A PARTIAL venue set means net_worth_usd is a partial sum. Refuse
            // it: a wrong total recorded as fresh is the same class of harm as
            // a frozen one, and harder to spot because the number still moves.
            if venue_set_is_complete(&body) == Some(false) {
                let seen = parse_venue_freshness(&body).len();
                warn!(
                    bot_id = %bot_id,
                    venues_reporting = seen,
                    "net-worth sampler: INCOMPLETE venue set — a venue has not \
                     checked in, so net worth would be a partial sum. Skipping."
                );
                metrics::NET_WORTH_STALE_SKIPPED_TOTAL
                    .with_label_values(&[metrics::refusal::INCOMPLETE])
                    .inc();
                return None;
            }

            // A bot holding open positions its own net-worth figure does not
            // account for is publishing REALIZED CASH under the field the
            // treasury reads as TOTAL PORTFOLIO VALUE — and summing that
            // beside a spot bot's mark-to-market total. The worst case is the
            // quiet one: while an open position bleeds, the recorded series
            // stays perfectly flat and the treasury shows NO DRAWDOWN AT ALL
            // until the trade closes.
            if open_positions_unaccounted(&body) {
                warn!(
                    bot_id = %bot_id,
                    "net-worth sampler: the bot reports OPEN POSITIONS its net-worth \
                     figure does not account for (net_worth_usd_complete=false or \
                     absent), so the figure is realized cash, not net worth. Skipping \
                     — recording it would sum two different quantities into /treasury \
                     and hide an open drawdown. Fix: have the brain stamp \
                     notional_usdt on its ENTRY record and declare its venue basis \
                     via set_venue_total_marks_positions()."
                );
                metrics::NET_WORTH_STALE_SKIPPED_TOTAL
                    .with_label_values(&[metrics::refusal::UNACCOUNTED])
                    .inc();
                return None;
            }

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
        // Clamp to >=1s so `NET_WORTH_SAMPLE_INTERVAL_SECS=0` (a natural
        // "disable" guess) can't become a busy-loop hammering every bot's
        // /status + the pool. Mirrors the `supervisor::run` guard.
        let interval = Duration::from_secs(config.net_worth_sample_interval_secs.max(1));
        let sampler = NetWorthSampler::new();
        // In-memory milestone anchor — the last-announced boundary. `None` until
        // the first total is observed (then baselined without firing). A process
        // restart re-baselines from the current total (acceptable per OD-5).
        let mut last_milestone: Option<f64> = None;
        loop {
            tokio::time::sleep(interval).await;
            sampler.sample_once(docker.as_ref(), &config, &store).await;
            check_net_worth_milestone(&config, &store, &mut last_milestone).await;
        }
    }

    /// Roll up the freshest snapshot per account into a single total and run the
    /// pure milestone detector against the in-memory anchor, dispatching a
    /// `net_worth_milestone` event on a crossing. OFF unless
    /// `NET_WORTH_MILESTONE_STEP > 0`. Best-effort throughout (never fatal).
    async fn check_net_worth_milestone(
        config: &Config,
        store: &BotRunStore,
        last_milestone: &mut Option<f64>,
    ) {
        let step = config.net_worth_milestone_step;
        if step <= 0.0 {
            return; // milestone detection disabled
        }

        // Freshest snapshot PER account (limit=1 windows one row per bot_id — the
        // same per-account roll-up the /net-worth window query established), summed
        // into the treasury total.
        let rows = match store.list_net_worth(None, 1).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "milestone: net-worth roll-up query failed");
                return;
            }
        };
        if rows.is_empty() {
            return; // no data yet — nothing to baseline against
        }
        let total: f64 = rows.iter().map(|r| r.net_worth).sum();
        if !total.is_finite() {
            return;
        }

        // First observation after (re)start: baseline the anchor, don't fire.
        let last = match last_milestone {
            Some(l) => *l,
            None => {
                let base = milestone_baseline(total, step);
                *last_milestone = Some(base);
                debug!(total, base, "milestone: baselined anchor (no notification)");
                return;
            }
        };

        let hysteresis = step * MILESTONE_HYSTERESIS_FRAC;
        let update = detect_milestone(last, total, step, hysteresis);
        *last_milestone = Some(update.last);

        let Some(cross) = update.cross else {
            return;
        };
        if !config.notify_enabled {
            return; // detector state advanced, but sending is hard-disabled
        }
        let (boundary, up) = match cross {
            MilestoneCross::Up(b) => (b, true),
            MilestoneCross::Down(b) => (b, false),
        };
        let ev = NotificationEvent::net_worth_milestone(total, boundary, up);
        let store = store.clone();
        // Detached, best-effort — never block the sampler loop on a webhook.
        tokio::spawn(async move {
            NotificationDispatcher::new(store).dispatch(ev).await;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // probe() wiring — a guard that is never CALLED is a guard that does
    // nothing, and a pure-function suite structurally cannot see that. These
    // drive the real HTTP path against a loopback /status server.
    // ─────────────────────────────────────────────────────────────────────────
    #[cfg(test)]
    mod probe_tests {
        use super::*;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        /// Serve `body` once on a loopback port; returns its `/status` URL.
        async fn serve_status_once(body: &'static str) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback");
            let addr = listener.local_addr().expect("local addr");
            tokio::spawn(async move {
                if let Ok((mut sock, _)) = listener.accept().await {
                    let mut buf = [0u8; 2048];
                    let _ = sock.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                }
            });
            format!("http://{addr}/status")
        }

        /// The live funding bot's real body: reachable, 200, parseable, and
        /// past all five existing guards — so if probe() still hands back a
        /// reading, the open-position guard is not wired in.
        #[tokio::test]
        async fn probe_refuses_a_status_whose_net_worth_omits_an_open_position() {
            let url = serve_status_once(crate::net_worth::tests::LIVE_FUNDING_STATUS).await;
            let got = NetWorthSampler::new().probe("crypto-funding", &url).await;
            assert!(
                got.is_none(),
                "probe returned {got:?} — realized cash would have been recorded as net worth"
            );
        }

        /// The same bot once it accounts for its book: recorded, with the open
        /// position inside the figure.
        #[tokio::test]
        async fn probe_records_a_status_that_accounts_for_its_positions() {
            const ACCOUNTED: &str = r#"{"bot": "kucoin-futures", "mode": "paper",
                "net_worth_usd": 11494.969349255287, "net_worth_usd_complete": true,
                "venue_total_usd": 11288.306994792156,
                "unrealized_pnl_usd": 206.66235446313075, "open_positions": 1,
                "exchanges": [{"exchange": "kucoin-futures", "mode": "paper",
                    "total_value": 11288.306994792156, "updated": 1785603708}],
                "positions": [{"symbol": "AVAXUSDTM", "ret_pct": 6.888745148771025}]}"#;
            let url = serve_status_once(ACCOUNTED).await;
            let got = NetWorthSampler::new()
                .probe("crypto-funding", &url)
                .await
                .expect("an accounted figure must still be recorded");
            assert!((got.net_worth - 11_494.969_349_255_287).abs() < 1e-9);
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

    // ── net worth meant two things (2026-08-02, gap #11) ────────────────────
    /// The live PAPER FUNDING bot's ACTUAL `/status`, read from the container
    /// on 2026-08-02 (events + a fresher `positions[].updated` trimmed for
    /// width, values verbatim). `exchanges[0]` is its paper ledger's REALIZED
    /// equity — `cash == total_value`, `holdings: []` — and the open AVAXUSDTM
    /// long is nowhere in that number.
    pub(super) const LIVE_FUNDING_STATUS: &str = r#"{
        "bot": "kucoin-futures", "market": "futures", "mode": "paper",
        "net_worth_usd": 11288.306994792156, "pnl_usd": 30.403891963142087,
        "exchanges": [
            {"exchange": "kucoin-futures", "mode": "paper", "cash_asset": "USDT",
             "cash": 11288.306994792156, "total_value": 11288.306994792156,
             "holdings": [], "updated": 1785603708}
        ],
        "positions": [
            {"symbol": "AVAXUSDTM", "dir": 1, "entry_px": 6.184, "mark_px": 6.61,
             "ret_pct": 6.888745148771025, "entry_ts_ms": 1785628855628,
             "updated": 1785654051}
        ]
    }"#;

    /// ~207 USDT of open book (3000 notional × 6.889%) absent from a figure
    /// that `/treasury` sums beside the spot bot's mark-to-market total.
    #[test]
    fn the_live_funding_bots_unaccounted_open_position_is_refused() {
        assert!(
            open_positions_unaccounted(LIVE_FUNDING_STATUS),
            "realized cash published as net worth while a position is open"
        );

        // And prove EVERY other guard waves it straight through — which is
        // exactly why this check has to exist separately.
        assert!(
            !has_fake_paper_venue(LIVE_FUNDING_STATUS),
            "#39: a wholly paper bot's paper venue is legitimate"
        );
        assert_eq!(
            venue_set_is_complete(LIVE_FUNDING_STATUS),
            None,
            "#38: no expected_venues → unknown, not incomplete"
        );
        let r = parse_status_net_worth(LIVE_FUNDING_STATUS).expect("parses");
        assert_eq!(
            r.updated, None,
            "#35: a paper venue contributes no stamp — idle is not blind"
        );
        assert!(
            !reading_is_stale(r.updated, 1_785_656_153, 600),
            "#32: unverifiable reads as fresh"
        );
        assert_eq!(
            r.net_worth, 11_288.306_994_792_156,
            "and the figure the sampler would have written is the pre-trade cash"
        );
    }

    /// The repair: once the bot marks its open positions into the figure it
    /// publishes, it vouches for it and the sample is recorded again.
    #[test]
    fn a_bot_that_accounts_for_its_positions_is_recorded() {
        let body = r#"{"mode": "paper", "net_worth_usd": 11494.97,
            "net_worth_usd_complete": true, "venue_total_usd": 11288.306994792156,
            "unrealized_pnl_usd": 206.66235446313075, "open_positions": 1,
            "exchanges": [
                {"exchange": "kucoin-futures", "mode": "paper", "total_value": 11288.306994792156,
                 "updated": 1785603708}
            ],
            "positions": [{"symbol": "AVAXUSDTM", "ret_pct": 6.888745148771025}]}"#;
        assert!(!open_positions_unaccounted(body));
        let r = parse_status_net_worth(body).expect("parses");
        assert_eq!(
            r.net_worth, 11_494.97,
            "and the recorded figure now includes the open book"
        );
    }

    /// A bot that says its own figure is incomplete is refused even if the
    /// spawner can see no positions — the bot's own word beats our inference.
    #[test]
    fn an_explicit_incomplete_declaration_is_always_refused() {
        let body = r#"{"net_worth_usd": 11288.31, "net_worth_usd_complete": false,
            "open_positions": 1, "positions": []}"#;
        assert!(open_positions_unaccounted(body));
    }

    /// The #35 regression guard, restated for this guard: the funding bot when
    /// it is FLAT is perfectly correct and must keep being recorded. Refusing
    /// a healthy bot is how its treasury series went dark for 15 hours.
    #[test]
    fn a_flat_paper_funding_bot_is_still_recorded() {
        let flat = r#"{"bot": "kucoin-futures", "mode": "paper",
            "net_worth_usd": 11288.306994792156, "positions": [],
            "exchanges": [{"exchange": "kucoin-futures", "mode": "paper",
                           "total_value": 11288.306994792156, "updated": 1785603708}]}"#;
        assert!(
            !open_positions_unaccounted(flat),
            "a flat book is fully accounted for — do NOT blank an idle bot"
        );
        // Including the pre-contract build, which publishes no declaration.
        let flat_new = r#"{"mode": "paper", "net_worth_usd": 11288.306994792156,
            "net_worth_usd_complete": true, "open_positions": 0, "positions": []}"#;
        assert!(!open_positions_unaccounted(flat_new));
    }

    /// The live SPOT bot — REAL MONEY, three live venues, `positions: []`.
    /// Its venue totals are `cash + Σ holdings[].value`, already
    /// mark-to-market, so this guard must be a complete no-op for it.
    #[test]
    fn the_live_spot_bot_is_untouched_by_the_position_guard() {
        let live_spot = r#"{"bot": "spot-portfolio", "mode": "live",
            "net_worth_usd": 178.9053809138768, "expected_venues": 3, "positions": [],
            "exchanges": [
                {"exchange": "Crypto.com",  "mode": "live", "total_value": 30.72607211026281,
                 "cash": 4.867793328, "updated": 1785656078,
                 "holdings": [{"asset": "BTC", "qty": 0.00016915, "value": 10.729536332}]},
                {"exchange": "Kraken",      "mode": "live", "total_value": 76.656122085614,
                 "cash": 15.1884, "updated": 1785656076,
                 "holdings": [{"asset": "BTC", "qty": 0.00048515, "value": 30.76578725}]},
                {"exchange": "KuCoin-spot", "mode": "live", "total_value": 71.52318671799999,
                 "cash": 17.35702511, "updated": 1785656077,
                 "holdings": [{"asset": "BTC", "qty": 0.00041014, "value": 26.039255418}]}
            ]}"#;
        assert!(
            !open_positions_unaccounted(live_spot),
            "the live REAL-MONEY spot series must not gain a new way to go dark"
        );
        // The bot-shape fixtures used by the other guards carry no `positions`
        // key at all; none of them may start being refused.
        assert!(!open_positions_unaccounted(LIVE_SPOT_STATUS));
        assert!(!open_positions_unaccounted(r#"{"net_worth_usd": 165.0}"#));
        assert!(!open_positions_unaccounted("not json"));
        assert!(!open_positions_unaccounted(""));
    }

    /// The published COUNT is authoritative when present, so a bot may drop
    /// the (potentially large) array without silently reading as flat.
    #[test]
    fn the_published_count_outranks_the_array() {
        let counted = r#"{"net_worth_usd": 1.0, "open_positions": 2}"#;
        assert!(open_positions_unaccounted(counted));
        let zero = r#"{"net_worth_usd": 1.0, "open_positions": 0,
                       "positions": [{"symbol": "GHOST"}]}"#;
        assert!(
            !open_positions_unaccounted(zero),
            "an explicit 0 count is a statement, not a missing field"
        );
    }

    // ── fabricated paper cash on a live bot (2026-07-28, instance #5) ───────
    /// The botched-key-rotation shape: bot mode "live", but Kraken silently
    /// degraded to paper with $1000 of cash that does not exist, and
    /// net_worth() summed it in.
    #[test]
    fn a_paper_venue_on_a_live_bot_is_refused() {
        let body = r#"{"bot": "spot-portfolio", "mode": "live",
            "net_worth_usd": 1160.54, "expected_venues": 3, "exchanges": [
            {"exchange": "Crypto.com",  "mode": "live",  "updated": 1785127277},
            {"exchange": "Kraken",      "mode": "paper", "updated": 1785127274},
            {"exchange": "KuCoin-spot", "mode": "live",  "updated": 1785127275}
        ]}"#;
        assert!(
            has_fake_paper_venue(body),
            "fabricated cash must be detected"
        );
        // And prove every OTHER guard waves it through — which is why this
        // check has to exist separately.
        assert_eq!(venue_set_is_complete(body), Some(true), "looks complete");
        let r = parse_status_net_worth(body).expect("parses");
        assert!(
            !reading_is_stale(r.updated, 1_785_127_280, 600),
            "looks fresh"
        );
    }

    #[test]
    fn a_wholly_paper_bot_is_not_flagged() {
        // crypto-funding: paper bot, paper venue, entirely legitimate. This is
        // the #35 regression guard — do NOT re-break the funding series.
        let body = r#"{"bot": "crypto-funding", "mode": "paper",
            "net_worth_usd": 11190.0, "exchanges": [
            {"exchange": "kucoin-futures", "mode": "paper", "updated": 1785000000}
        ]}"#;
        assert!(!has_fake_paper_venue(body));
    }

    #[test]
    fn an_all_real_live_bot_is_not_flagged() {
        assert!(!has_fake_paper_venue(LIVE_SPOT_STATUS));
        // dry-run is real money, not fake cash — must not trip this.
        let dry = r#"{"mode": "live", "net_worth_usd": 100.0, "exchanges": [
            {"exchange": "Kraken", "mode": "dry-run", "updated": 1785127274}
        ]}"#;
        assert!(!has_fake_paper_venue(dry));
        // Garbage and missing fields are not "fake".
        assert!(!has_fake_paper_venue("not json"));
        assert!(!has_fake_paper_venue(r#"{"net_worth_usd": 1.0}"#));
    }

    // ── an incomplete venue set is a WRONG total (2026-07-28 review) ────────
    #[test]
    fn a_missing_venue_makes_the_total_incomplete() {
        // Kraken was down when the bot started, so it never entered the venue
        // map. The two venues that DID report are perfectly fresh — every
        // staleness check passes — but net_worth_usd omits Kraken's balance.
        let body = r#"{"net_worth_usd": 129.29, "expected_venues": 3, "exchanges": [
            {"exchange": "Crypto.com",  "mode": "live", "updated": 1785127277},
            {"exchange": "KuCoin-spot", "mode": "live", "updated": 1785127275}
        ]}"#;
        assert_eq!(venue_set_is_complete(body), Some(false));
        // Proof the other guards are blind to it:
        let r = parse_status_net_worth(body).expect("parses");
        assert!(
            !reading_is_stale(r.updated, 1_785_127_280, 600),
            "the present venues are fresh — staleness cannot catch this"
        );
    }

    #[test]
    fn a_full_venue_set_is_complete() {
        let body = r#"{"net_worth_usd": 165.12, "expected_venues": 3, "exchanges": [
            {"exchange": "Crypto.com",  "mode": "live",    "updated": 1785127277},
            {"exchange": "Kraken",      "mode": "dry-run", "updated": 1785127274},
            {"exchange": "KuCoin-spot", "mode": "live",    "updated": 1785127275}
        ]}"#;
        assert_eq!(venue_set_is_complete(body), Some(true));
    }

    #[test]
    fn an_older_bot_without_expected_venues_is_unknown_not_incomplete() {
        // Must NOT blank the series for bots that predate the field.
        let body = r#"{"net_worth_usd": 165.0, "exchanges": [
            {"exchange": "Kraken", "mode": "live", "updated": 1785127274}
        ]}"#;
        assert_eq!(venue_set_is_complete(body), None);
        assert_eq!(venue_set_is_complete(r#"{"net_worth_usd": 1.0}"#), None);
        assert_eq!(venue_set_is_complete("not json"), None);
        // expected_venues:0 is meaningless, not "everything is missing".
        assert_eq!(
            venue_set_is_complete(r#"{"expected_venues": 0, "exchanges": []}"#),
            None
        );
    }

    // ── dry-run is REAL MONEY (2026-07-28 review finding) ───────────────────
    /// A spot venue publishes mode="dry-run" when it is outside the live_venues
    /// allowlist OR when the drawdown breaker has halted it
    /// (`vlive = venue.live && !halted`). It still holds real balances. Gating
    /// on mode=="live" disengaged the guard exactly when a venue was halted.
    #[test]
    fn a_dry_run_venue_still_gates_staleness() {
        let body = r#"{"net_worth_usd": 165.0, "exchanges": [
            {"exchange": "Kraken", "mode": "dry-run", "updated": 1785123600}
        ]}"#;
        let r = parse_status_net_worth(body).expect("parses");
        assert_eq!(
            r.updated,
            Some(1_785_123_600),
            "a halted/non-allowlisted venue holds REAL balances and must still be checked"
        );
        assert!(reading_is_stale(r.updated, 1_785_123_600 + 3600, 600));
    }

    #[test]
    fn a_halted_bot_does_not_lose_its_staleness_protection() {
        // The drawdown breaker flips EVERY venue to dry-run at once. Before the
        // fix this emptied the stamp set and made the reading unverifiable.
        let now = 1_785_127_280u64;
        let frozen = now - 65 * 60;
        let body = format!(
            r#"{{"net_worth_usd": 206.26301554, "exchanges": [
                {{"exchange": "Crypto.com", "mode": "dry-run", "updated": {frozen}}},
                {{"exchange": "Kraken",     "mode": "dry-run", "updated": {frozen}}},
                {{"exchange": "KuCoin",     "mode": "dry-run", "updated": {frozen}}}
            ]}}"#
        );
        let r = parse_status_net_worth(&body).expect("parses");
        assert!(
            reading_is_stale(r.updated, now, 600),
            "a halted bot serving frozen figures must still be refused"
        );
    }

    #[test]
    fn paper_is_still_the_only_exemption() {
        // The #35 regression fix must survive: paper alone is exempt.
        let body = r#"{"net_worth_usd": 11190.0, "exchanges": [
            {"exchange": "kucoin-futures", "mode": "paper", "updated": 1785000000}
        ]}"#;
        assert_eq!(parse_status_net_worth(body).unwrap().updated, None);
        // And a mixed bot takes the stalest REAL-money venue.
        let mixed = r#"{"net_worth_usd": 100.0, "exchanges": [
            {"exchange": "sim",    "mode": "paper",   "updated": 1785000000},
            {"exchange": "Kraken", "mode": "dry-run", "updated": 1785086000},
            {"exchange": "KuCoin", "mode": "live",    "updated": 1785086500}
        ]}"#;
        assert_eq!(
            parse_status_net_worth(mixed).unwrap().updated,
            Some(1_785_086_000)
        );
    }

    // ── idle is not blind (2026-07-27 regression) ───────────────────────────
    /// The funding bot: a PAPER venue whose account snapshot is legitimately
    /// 15.5 hours old because it marks its book on trade events, not on a
    /// timer. Gating on this stamp took its treasury series offline for 15
    /// hours (176 consecutive skips) before this test existed.
    #[test]
    fn a_paper_venue_stamp_never_gates_the_sample() {
        let body = r#"{"net_worth_usd": 11190.18636279, "exchanges": [
            {"exchange": "kucoin-futures", "mode": "paper", "updated": 1785000000}
        ]}"#;
        let r = parse_status_net_worth(body).expect("parses");
        assert_eq!(
            r.updated, None,
            "a paper venue must not contribute a staleness stamp — idle is not blind"
        );
        assert!(!reading_is_stale(r.updated, 1_785_000_000 + 55_643, 600));
    }

    #[test]
    fn a_live_venue_still_gates_even_beside_a_paper_one() {
        // Mixed bot: the live leg is what can go blind, so it must still gate.
        let body = r#"{"net_worth_usd": 100.0, "exchanges": [
            {"exchange": "kucoin-futures", "mode": "paper", "updated": 1785000000},
            {"exchange": "Kraken",         "mode": "live",  "updated": 1785086000}
        ]}"#;
        let r = parse_status_net_worth(body).expect("parses");
        assert_eq!(
            r.updated,
            Some(1_785_086_000),
            "the LIVE venue supplies the stamp"
        );
        assert!(reading_is_stale(r.updated, 1_785_086_000 + 700, 600));
    }

    #[test]
    fn the_live_spot_bot_is_unaffected_by_the_paper_carve_out() {
        // All three of its venues are live, so the 2026-07-22 protection holds.
        let r = parse_status_net_worth(LIVE_SPOT_STATUS).expect("parses");
        assert_eq!(r.updated, Some(1_785_127_274));
        let now = 1_785_127_274 + 65 * 60;
        assert!(
            reading_is_stale(r.updated, now, 600),
            "blackout must still be refused"
        );
    }

    // ── per-venue freshness (P-26) ──────────────────────────────────────────
    /// The REAL shape the production spot bot serves: no root `updated`, three
    /// venues under `exchanges[]`, each with its own stamp. The first cut of
    /// the staleness guard only read a root stamp and was therefore a complete
    /// no-op against this body — the exact bot it was written to protect.
    const LIVE_SPOT_STATUS: &str = r#"{
        "bot": "spot-portfolio", "mode": "live", "net_worth_usd": 165.12386300312613,
        "exchanges": [
            {"exchange": "Crypto.com",  "mode": "live", "total_value": 55.84, "updated": 1785127277},
            {"exchange": "Kraken",      "mode": "live", "total_value": 35.82, "updated": 1785127274},
            {"exchange": "KuCoin-spot", "mode": "live", "total_value": 73.44, "updated": 1785127275}
        ]
    }"#;

    #[test]
    fn freshness_comes_from_the_stalest_venue_not_the_root() {
        let r = parse_status_net_worth(LIVE_SPOT_STATUS).expect("parses");
        // net_worth is a SUM across venues, so it is only as fresh as its
        // oldest component — the min, never the max.
        assert_eq!(r.updated, Some(1_785_127_274));
        assert!((r.net_worth - 165.123_863_003_126_13).abs() < 1e-9);
    }

    #[test]
    fn one_dead_venue_makes_the_total_stale_even_when_others_tick() {
        // Kraken frozen an hour ago; the other two refreshed seconds ago. The
        // SUM is wrong, so the sample must be refused.
        let body = r#"{"net_worth_usd": 165.0, "exchanges": [
            {"exchange": "Crypto.com", "mode": "live", "updated": 1785127277},
            {"exchange": "Kraken",     "mode": "live", "updated": 1785123600},
            {"exchange": "KuCoin",     "mode": "live", "updated": 1785127275}
        ]}"#;
        let r = parse_status_net_worth(body).expect("parses");
        assert_eq!(r.updated, Some(1_785_123_600));
        assert!(reading_is_stale(r.updated, 1_785_127_280, 600));
    }

    #[test]
    fn venue_freshness_exposes_every_venue_with_mode() {
        let v = parse_venue_freshness(LIVE_SPOT_STATUS);
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].exchange, "Crypto.com");
        assert_eq!(v[0].mode, "live");
        assert_eq!(v[1].updated, Some(1_785_127_274));
        // A body with no venue breakdown yields nothing rather than erroring.
        assert!(parse_venue_freshness(r#"{"net_worth_usd": 1.0}"#).is_empty());
        assert!(parse_venue_freshness("not json").is_empty());
    }

    #[test]
    fn root_stamp_is_the_fallback_when_no_venues_are_published() {
        let body = r#"{"net_worth_usd": 10.0, "updated": 1785000000}"#;
        assert_eq!(
            parse_status_net_worth(body).unwrap().updated,
            Some(1_785_000_000)
        );
        // And a venue array with no stamps falls back to the root too.
        let mixed = r#"{"net_worth_usd": 10.0, "updated": 1785000000,
                        "exchanges": [{"exchange": "Kraken", "mode": "live"}]}"#;
        assert_eq!(
            parse_status_net_worth(mixed).unwrap().updated,
            Some(1_785_000_000)
        );
    }

    #[test]
    fn the_2026_07_22_blackout_would_now_be_refused() {
        // Every venue frozen together — the DNS signature. 65 minutes stale.
        let now = 1_785_127_280u64;
        let frozen = now - 65 * 60;
        let body = format!(
            r#"{{"net_worth_usd": 206.26301554, "exchanges": [
                {{"exchange": "Crypto.com", "mode": "live", "updated": {frozen}}},
                {{"exchange": "Kraken",     "mode": "live", "updated": {frozen}}},
                {{"exchange": "KuCoin",     "mode": "live", "updated": {frozen}}}
            ]}}"#
        );
        let r = parse_status_net_worth(&body).expect("parses");
        assert!(
            reading_is_stale(r.updated, now, 600),
            "the frozen 206.26301554 reading must be refused, not written 15 times"
        );
    }

    // ── staleness guard (2026-07-22 DNS blackout) ───────────────────────────
    #[test]
    fn stale_reading_is_refused_fresh_one_is_kept() {
        let now = 1_785_000_000u64;
        let max_age = 600; // 2 x a 300s interval
        // Fresh: stamped this cycle.
        assert!(!reading_is_stale(Some(now), now, max_age));
        // One missed cycle — tolerated, this is normal jitter.
        assert!(!reading_is_stale(Some(now - 300), now, max_age));
        // Exactly at the bound is still acceptable.
        assert!(!reading_is_stale(Some(now - 600), now, max_age));
        // Beyond it: the bot is serving a frozen figure.
        assert!(reading_is_stale(Some(now - 601), now, max_age));
        // The real incident: 65 minutes of frozen /status.
        assert!(reading_is_stale(Some(now - 65 * 60), now, max_age));
    }

    #[test]
    fn missing_or_future_stamp_is_not_treated_as_stale() {
        let now = 1_785_000_000u64;
        // A bot that publishes no stamp cannot be checked — record it, as before.
        assert!(!reading_is_stale(None, now, 600));
        // Clock skew putting the stamp ahead must NOT blank the series.
        assert!(!reading_is_stale(Some(now + 120), now, 600));
    }

    #[test]
    fn parse_extracts_the_bots_updated_stamp() {
        // With a stamp (the shape crypto-bot-core publishes).
        let r = parse_status_net_worth(r#"{"net_worth_usd": 206.26, "updated": 1785000000}"#)
            .expect("parses");
        assert_eq!(r.updated, Some(1_785_000_000));
        // Without one — still parses, just unverifiable.
        let r2 = parse_status_net_worth(r#"{"net_worth_usd": 206.26}"#).expect("parses");
        assert_eq!(r2.updated, None);
        // Garbage/zero stamp is treated as absent rather than 1970.
        let r3 = parse_status_net_worth(r#"{"net_worth_usd": 1.0, "updated": 0}"#).expect("parses");
        assert_eq!(r3.updated, None);
    }

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
            updated: None,
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

    // ── milestone crossing detector (pure) ──────────────────────────────────

    const STEP: f64 = 1000.0;
    fn hyst() -> f64 {
        STEP * MILESTONE_HYSTERESIS_FRAC // 100.0
    }

    #[test]
    fn milestone_step_zero_is_off() {
        let u = detect_milestone(10_000.0, 999_999.0, 0.0, 0.0);
        assert_eq!(u.cross, None);
        assert_eq!(u.last, 10_000.0, "anchor untouched when disabled");
    }

    #[test]
    fn milestone_up_cross_snaps_and_fires() {
        // From an anchor of 10_000, clearing 11_000 by the hysteresis fires Up.
        let u = detect_milestone(10_000.0, 11_200.0, STEP, hyst());
        assert_eq!(u.cross, Some(MilestoneCross::Up(11_000.0)));
        assert_eq!(u.last, 11_000.0);
    }

    #[test]
    fn milestone_up_multistep_jump_reports_furthest_boundary() {
        // A jump of several steps announces the furthest boundary, not each one.
        let u = detect_milestone(10_000.0, 13_400.0, STEP, hyst());
        assert_eq!(u.cross, Some(MilestoneCross::Up(13_000.0)));
        assert_eq!(u.last, 13_000.0);
    }

    #[test]
    fn milestone_down_cross_fires_amber() {
        let u = detect_milestone(10_000.0, 8_800.0, STEP, hyst());
        assert_eq!(u.cross, Some(MilestoneCross::Down(9_000.0)));
        assert_eq!(u.last, 9_000.0);
    }

    #[test]
    fn milestone_no_cross_within_a_step() {
        // Moving less than a full step (+ hysteresis) from the anchor: nothing.
        let u = detect_milestone(10_000.0, 10_500.0, STEP, hyst());
        assert_eq!(u.cross, None);
        assert_eq!(u.last, 10_000.0);
        let u = detect_milestone(10_000.0, 9_500.0, STEP, hyst());
        assert_eq!(u.cross, None);
    }

    #[test]
    fn milestone_oscillation_with_hysteresis_does_not_respam() {
        // Total jitters around the 11_000 boundary while anchored at 10_000.
        // Within the hysteresis band (±100) it must NEVER fire.
        for total in [11_000.0, 11_050.0, 10_960.0, 11_099.0] {
            let u = detect_milestone(10_000.0, total, STEP, hyst());
            assert_eq!(u.cross, None, "jitter at {total} must not fire");
            assert_eq!(u.last, 10_000.0);
        }
        // Once it clears the band (>= 11_100) it fires exactly once; then the
        // anchor snaps to 11_000 and a dip back to 11_000 does NOT re-fire down.
        let fired = detect_milestone(10_000.0, 11_150.0, STEP, hyst());
        assert_eq!(fired.cross, Some(MilestoneCross::Up(11_000.0)));
        let back = detect_milestone(fired.last, 11_000.0, STEP, hyst());
        assert_eq!(back.cross, None, "sub-step dip must not re-fire");
    }

    #[test]
    fn milestone_baseline_snaps_below() {
        assert_eq!(milestone_baseline(10_450.0, STEP), 10_000.0);
        assert_eq!(milestone_baseline(0.0, STEP), 0.0);
        // A baseline followed by a real crossing fires from the baseline anchor.
        let base = milestone_baseline(10_450.0, STEP);
        let u = detect_milestone(base, 11_200.0, STEP, hyst());
        assert_eq!(u.cross, Some(MilestoneCross::Up(11_000.0)));
    }

    #[test]
    fn milestone_ignores_non_finite_total() {
        let u = detect_milestone(10_000.0, f64::NAN, STEP, hyst());
        assert_eq!(u.cross, None);
        assert_eq!(u.last, 10_000.0);
    }
}
