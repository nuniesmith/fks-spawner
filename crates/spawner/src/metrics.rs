// =============================================================================
// metrics.rs — Prometheus metrics for the FKS Bot Spawner itself
//
// Exposes the spawner's operational metrics at GET /metrics.
// These describe the spawner service, not the individual bots.
// (Bot-level metrics are scraped by Prometheus directly from each bot's :9091.)
// =============================================================================

use once_cell::sync::Lazy;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder, register_counter,
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec,
};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

// ─────────────────────────────────────────────────────────────────────────────
// Counters
// ─────────────────────────────────────────────────────────────────────────────

pub static SPAWNS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_spawns_total",
        "Total number of bot containers spawned"
    )
    .expect("metric registration failed")
});

pub static SPAWN_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_spawn_errors_total",
        "Total number of failed spawn attempts"
    )
    .expect("metric registration failed")
});

pub static STOPS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_stops_total",
        "Total number of bot stop operations"
    )
    .expect("metric registration failed")
});

pub static REMOVES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_removes_total",
        "Total number of bot remove operations"
    )
    .expect("metric registration failed")
});

pub static PRUNE_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_prune_total",
        "Total number of containers removed by auto-prune"
    )
    .expect("metric registration failed")
});

pub static NOTIFY_SENT_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_notify_sent_total",
        "Total number of notification webhook POSTs that succeeded (2xx)"
    )
    .expect("metric registration failed")
});

pub static NOTIFY_FAILED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_notify_failed_total",
        "Total number of notification webhook POSTs that failed (non-2xx, timeout, or decrypt error)"
    )
    .expect("metric registration failed")
});

pub static NET_WORTH_SNAPSHOTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_net_worth_snapshots_total",
        "Total number of net_worth_snapshots rows the sampler has written"
    )
    .expect("metric registration failed")
});

// ─────────────────────────────────────────────────────────────────────────────
// Gauges
// ─────────────────────────────────────────────────────────────────────────────

pub static RUNNING_BOTS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_spawner_running_bots",
        "Number of bot containers currently in the 'running' state"
    )
    .expect("metric registration failed")
});

pub static LIVE_BOTS_RUNNING: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_spawner_live_bots_running",
        "Number of live-mode (fks.mode=live) bot containers currently running"
    )
    .expect("metric registration failed")
});

pub static CRASHED_BOTS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "fks_spawner_crashed_bots",
        "Number of long-lived bot containers currently retained after an unexpected exit (crash)"
    )
    .expect("metric registration failed")
});

/// Age, in seconds, of each venue's last successful account refresh — as
/// stamped by the BOT, not by the scraper. This is the metric that would have
/// made the 2026-07-22 DNS blackout visible within minutes instead of being
/// found in the database five days later: every venue's age climbing together
/// is a connectivity failure, one venue climbing alone is a venue problem.
///
/// A venue that vanishes from `/status` stops being exported rather than
/// reporting a stale age forever (see `set_venue_ages`).
pub static VENUE_STATUS_AGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "fks_bot_venue_status_age_seconds",
        "Seconds since a bot venue last refreshed its account snapshot (bot-stamped)",
        &["bot_id", "exchange", "mode"]
    )
    .expect("metric registration failed")
});

/// Net-worth samples SKIPPED because the bot's own stamp said the figure was
/// stale. Non-zero means the treasury series has an honest gap; it is the
/// counterpart to the silent fabrication this replaced.
/// Net-worth samples the sampler REFUSED, labelled by why.
///
/// Five distinct refusals share this metric and they are NOT the same event:
///   stale        the bot-stamped figure is older than we tolerate
///   incomplete   a venue is missing, so the total is a partial sum
///   fake_paper   a real-money bot is reporting a paper venue (a key check failed)
///   unaccounted  the bot holds an open position it will not vouch for
///   not_ready    the venue array is present but EMPTY — the engine has not
///                completed a single venue cycle since process start (the
///                startup window), not a bot that checked and found nothing
///
/// The label is not decoration. `NetWorthSamplingPausedTooLong` pages on this
/// counter, and until now its annotation asserted staleness as the cause and
/// its runbook sent the operator to BotAllVenuesStale / BotVenueStale. Most of
/// the refusals have nothing to do with venue staleness, so the page
/// asserted a cause that was false and pointed at alerts that could not fire —
/// the same "authoritative claim built on the wrong thing" this whole guard
/// chain exists to prevent, reproduced in the alert about the guard chain.
///
/// Query the old total with `sum(fks_spawner_net_worth_stale_skipped_total)`;
/// query the actionable one with `by (reason)`.
pub static NET_WORTH_STALE_SKIPPED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "fks_spawner_net_worth_stale_skipped_total",
        "Net-worth samples refused, by reason (stale|incomplete|fake_paper|unaccounted|not_ready)",
        &["reason"]
    )
    .expect("metric registration failed")
});

/// The refusal reasons, so a call site cannot invent a label string.
pub mod refusal {
    pub const STALE: &str = "stale";
    pub const INCOMPLETE: &str = "incomplete";
    pub const FAKE_PAPER: &str = "fake_paper";
    pub const UNACCOUNTED: &str = "unaccounted";
    pub const NOT_READY: &str = "not_ready";
}

// ─────────────────────────────────────────────────────────────────────────────
// Histograms
// ─────────────────────────────────────────────────────────────────────────────

pub static SPAWN_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "fks_spawner_spawn_duration_seconds",
        "Time taken to create and start a bot container",
        &["image_prefix"]
    )
    .expect("metric registration failed")
});

/// Drop every venue series for a bot.
///
/// Called when `/status` cannot be reached, answers non-2xx, or is unreadable.
/// A GaugeVec child persists once created, so without this a bot that stops
/// answering keeps exporting its LAST age forever — frozen at a small,
/// healthy-looking number. Both freshness alerts then evaluate against a lie
/// and stay green through exactly the outage they exist to catch (the gauge
/// reproducing, one level up, the frozen-reading bug it was built to expose).
///
/// Removing the series is the honest state: absent, not "fine".
///
/// CORRECTION to an earlier version of this comment, which claimed `up == 0`
/// and BotStopped cover the bot being down. They do NOT for a deliberate
/// stop/remove: the Prometheus SD file is rewritten to drop non-running bots,
/// so no `up` series survives to be 0. Coverage for that path comes from
/// `retire_absent_bots` (this module) removing the series entirely.
pub fn retire_venue_ages(bot_id: &str) {
    set_venue_ages(bot_id, &[]);
}

/// Retire every bot that is no longer in the running set.
///
/// `set_venue_ages` / `retire_venue_ages` are only reachable from `probe()`,
/// and `probe()` only runs for bots whose container state is "running". So a
/// bot that is stopped, crashes, or is removed is never probed again and its
/// venue series linger FROZEN at their last ages until the spawner restarts.
///
/// That is not merely untidy. Stop a bot while one of its venues sits at 1200s
/// and `BotVenueStale` fires on the money channel FOREVER. The mitigation
/// claimed elsewhere ("up == 0 covers it") does NOT hold for a deliberate
/// stop/remove: the Prometheus SD file is rewritten to drop non-running bots,
/// so no `up` series exists at all — neither `up == 0` nor
/// BotVenueFreshnessMissing (which requires `up == 1`) can fire.
///
/// Called once per sampler tick with the bots that ARE running.
pub fn retire_absent_bots(running: &[String]) {
    let live: HashSet<&str> = running.iter().map(String::as_str).collect();
    let gone: Vec<String> = match SEEN.lock() {
        Ok(seen) => seen
            .keys()
            .filter(|b| !live.contains(b.as_str()))
            .cloned()
            .collect(),
        Err(_) => return,
    };
    for bot_id in gone {
        set_venue_ages(&bot_id, &[]);
        if let Ok(mut seen) = SEEN.lock() {
            seen.remove(&bot_id);
        }
    }
}

/// One venue's exported age: `(exchange, mode, age_seconds)`.
pub type VenueAge = (String, String, f64);

/// Publish per-venue ages for one bot, retiring labels for venues that are no
/// longer reported.
///
/// A GaugeVec keeps a child series forever once created. Without this, a venue
/// that disappears from `/status` (bot reconfigured, venue removed) would keep
/// exporting its last age — which then climbs forever and fires the alert
/// permanently. Retiring the stale label sets keeps "age is high" meaningful.
/// exchange + mode label pair, per bot.
type VenueLabels = HashSet<(String, String)>;

/// Which venue label sets are currently exported, per bot. Shared by
/// `set_venue_ages` (diff-retire within one bot) and `retire_absent_bots`
/// (retire whole bots that no longer run).
static SEEN: Lazy<Mutex<HashMap<String, VenueLabels>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn set_venue_ages(bot_id: &str, ages: &[VenueAge]) {
    let current: HashSet<(String, String)> = ages
        .iter()
        .map(|(ex, mode, _)| (ex.clone(), mode.clone()))
        .collect();

    for (exchange, mode, age) in ages {
        VENUE_STATUS_AGE
            .with_label_values(&[bot_id, exchange, mode])
            .set(*age);
    }

    if let Ok(mut seen) = SEEN.lock() {
        if let Some(prev) = seen.get(bot_id) {
            for (exchange, mode) in prev.difference(&current) {
                let _ = VENUE_STATUS_AGE.remove_label_values(&[bot_id, exchange, mode]);
            }
        }
        seen.insert(bot_id.to_string(), current);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Render
// ─────────────────────────────────────────────────────────────────────────────

/// Encode all registered Prometheus metrics to text format.
pub fn render() -> String {
    // Touch each lazy to ensure they're registered before first scrape.
    let _ = &*SPAWNS_TOTAL;
    let _ = &*SPAWN_ERRORS_TOTAL;
    let _ = &*STOPS_TOTAL;
    let _ = &*REMOVES_TOTAL;
    let _ = &*PRUNE_TOTAL;
    let _ = &*NOTIFY_SENT_TOTAL;
    let _ = &*NOTIFY_FAILED_TOTAL;
    let _ = &*NET_WORTH_SNAPSHOTS_TOTAL;
    let _ = &*RUNNING_BOTS;
    let _ = &*LIVE_BOTS_RUNNING;
    let _ = &*CRASHED_BOTS;
    let _ = &*VENUE_STATUS_AGE;
    let _ = &*NET_WORTH_STALE_SKIPPED_TOTAL;
    let _ = &*SPAWN_DURATION;

    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `retire_absent_bots` operates on the whole SEEN map by design, so a test
    /// calling it would retire bots another test registered concurrently. These
    /// tests share process-global prometheus + SEEN state and must not overlap.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Count exported children of the venue gauge for one bot_id.
    fn series_for(bot_id: &str) -> usize {
        prometheus::gather()
            .iter()
            .filter(|mf| mf.name() == "fks_bot_venue_status_age_seconds")
            .flat_map(|mf| mf.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.name() == "bot_id" && l.value() == bot_id)
            })
            .count()
    }

    #[test]
    fn retiring_a_bot_removes_every_venue_series() {
        let _g = serial();
        // Unique id: the prometheus registry is process-global and tests run
        // in parallel.
        let bot = "test-retire-all";
        set_venue_ages(
            bot,
            &[
                ("Kraken".into(), "live".into(), 10.0),
                ("KuCoin".into(), "live".into(), 12.0),
                ("Crypto.com".into(), "dry-run".into(), 11.0),
            ],
        );
        assert_eq!(series_for(bot), 3, "three venues should be exported");

        // /status went unreachable. A frozen series reads as FRESH and would
        // silently un-fire the alerts it feeds; absent is the honest state.
        retire_venue_ages(bot);
        assert_eq!(series_for(bot), 0, "an unreachable bot must export nothing");
    }

    #[test]
    fn a_vanished_venue_is_retired_while_its_siblings_survive() {
        let _g = serial();
        let bot = "test-retire-one";
        set_venue_ages(
            bot,
            &[
                ("Kraken".into(), "live".into(), 10.0),
                ("KuCoin".into(), "live".into(), 12.0),
            ],
        );
        assert_eq!(series_for(bot), 2);

        // Bot reconfigured: Kraken removed. Its series must not linger and
        // climb forever, which would pin BotVenueStale permanently.
        set_venue_ages(bot, &[("KuCoin".into(), "live".into(), 13.0)]);
        assert_eq!(series_for(bot), 1, "only the surviving venue stays");
    }

    #[test]
    fn a_bot_that_stops_running_is_retired() {
        let _g = serial();
        // The gap #37 left open: probe() only runs for RUNNING bots, so a
        // stopped/crashed/removed bot was never retired and its series froze
        // at their last ages — pinning BotVenueStale on the money channel with
        // no `up` series left for any rule to key off.
        let gone = "test-absent-gone";
        let stays = "test-absent-stays";
        set_venue_ages(gone, &[("Kraken".into(), "live".into(), 1200.0)]);
        set_venue_ages(stays, &[("KuCoin".into(), "live".into(), 10.0)]);
        assert_eq!(series_for(gone), 1);
        assert_eq!(series_for(stays), 1);

        // Next tick: only `stays` is still running.
        retire_absent_bots(&[stays.to_string()]);
        assert_eq!(
            series_for(gone),
            0,
            "a bot that stopped must export nothing"
        );
        assert_eq!(series_for(stays), 1, "a running bot must be untouched");
    }

    #[test]
    fn retiring_absent_bots_with_everything_running_changes_nothing() {
        let _g = serial();
        let a = "test-absent-keep-a";
        set_venue_ages(a, &[("Kraken".into(), "live".into(), 5.0)]);
        assert_eq!(series_for(a), 1);
        retire_absent_bots(&[a.to_string()]);
        assert_eq!(series_for(a), 1);
    }

    #[test]
    fn every_refusal_reason_is_its_own_series() {
        let _g = serial();
        // NOTE ON WHAT THIS TESTS, because the first version of it was VACUOUS.
        //
        // Incrementing the four labels here and then asserting four series exist
        // proves only that a CounterVec has labels. It passes just as happily
        // when every call site in net_worth.rs writes `STALE` — which is exactly
        // the bug. Verified: collapsing all four call sites to STALE left that
        // version green.
        //
        // So this asserts the CALL SITES. The page reading this counter names a
        // CAUSE; four different refusals sharing one label made that name wrong
        // three times out of four, and sent the operator to a runbook pointing
        // at alerts that cannot fire.
        const SRC: &str = include_str!("net_worth.rs");

        for r in [
            "STALE",
            "INCOMPLETE",
            "FAKE_PAPER",
            "UNACCOUNTED",
            "NOT_READY",
        ] {
            assert!(
                SRC.contains(&format!("refusal::{r}")),
                "net_worth.rs must label a refusal with refusal::{r} — otherwise the \
                 NetWorthSamplingPausedTooLong page asserts a cause it cannot know"
            );
        }

        // And every increment must carry SOME label: a bare .inc() would compile
        // away the distinction again.
        let bare = SRC.matches("NET_WORTH_STALE_SKIPPED_TOTAL.inc()").count();
        assert_eq!(bare, 0, "found {bare} unlabelled refusal increments");

        // The metric itself still works as a labelled vector.
        NET_WORTH_STALE_SKIPPED_TOTAL
            .with_label_values(&[refusal::STALE])
            .inc();
    }

    #[test]
    fn retiring_an_unknown_bot_is_a_no_op() {
        let _g = serial();
        retire_venue_ages("test-never-seen");
        assert_eq!(series_for("test-never-seen"), 0);
    }
}
