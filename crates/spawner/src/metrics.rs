// =============================================================================
// metrics.rs — Prometheus metrics for the FKS Bot Spawner itself
//
// Exposes the spawner's operational metrics at GET /metrics.
// These describe the spawner service, not the individual bots.
// (Bot-level metrics are scraped by Prometheus directly from each bot's :9091.)
// =============================================================================

use once_cell::sync::Lazy;
use prometheus::{
    Counter, Encoder, Gauge, GaugeVec, HistogramVec, TextEncoder, register_counter, register_gauge,
    register_gauge_vec, register_histogram_vec,
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
pub static NET_WORTH_STALE_SKIPPED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "fks_spawner_net_worth_stale_skipped_total",
        "Net-worth samples refused because the bot-stamped figure was too old"
    )
    .expect("metric registration failed")
});

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
/// Removing the series is the honest state: absent, not "fine". `up == 0` and
/// BotStopped still cover the bot being down.
pub fn retire_venue_ages(bot_id: &str) {
    set_venue_ages(bot_id, &[]);
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
pub fn set_venue_ages(bot_id: &str, ages: &[VenueAge]) {
    /// exchange + mode label pair, per bot.
    type VenueLabels = HashSet<(String, String)>;
    static SEEN: Lazy<Mutex<HashMap<String, VenueLabels>>> =
        Lazy::new(|| Mutex::new(HashMap::new()));

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

    /// Count exported children of the venue gauge for one bot_id.
    fn series_for(bot_id: &str) -> usize {
        prometheus::gather()
            .iter()
            .filter(|mf| mf.get_name() == "fks_bot_venue_status_age_seconds")
            .flat_map(|mf| mf.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "bot_id" && l.get_value() == bot_id)
            })
            .count()
    }

    #[test]
    fn retiring_a_bot_removes_every_venue_series() {
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
    fn retiring_an_unknown_bot_is_a_no_op() {
        retire_venue_ages("test-never-seen");
        assert_eq!(series_for("test-never-seen"), 0);
    }
}
