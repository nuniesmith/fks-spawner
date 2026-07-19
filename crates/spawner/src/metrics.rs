// =============================================================================
// metrics.rs — Prometheus metrics for the FKS Bot Spawner itself
//
// Exposes the spawner's operational metrics at GET /metrics.
// These describe the spawner service, not the individual bots.
// (Bot-level metrics are scraped by Prometheus directly from each bot's :9091.)
// =============================================================================

use once_cell::sync::Lazy;
use prometheus::{
    Counter, Encoder, Gauge, HistogramVec, TextEncoder, register_counter, register_gauge,
    register_histogram_vec,
};

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
    let _ = &*SPAWN_DURATION;

    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    encoder.encode(&families, &mut buf).unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}
