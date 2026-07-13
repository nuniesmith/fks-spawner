// =============================================================================
// edge_decay.rs — the weekly edge-backtest scheduler (EDGE-DECAY DETECTION)
//
// WHY (the edge-portfolio doctrine, fks-state docs/ARCHITECTURE.md): an edge
// is only alive while its measured advantage holds. A backtest run is a point
// measurement; decay is only visible ACROSS runs over time. This task, on a
// weekly cadence, re-fires every ACTIVE, containerized edge's backtest through
// the SAME internal trigger path POST /edges/{id}/backtest uses (so every
// spawn guard applies unchanged) — nothing more. The DRIFT comparison between
// this week's run and last week's lives in the advisor's Sunday report
// (fks-state crates/advisor); the scheduler's only job is to make sure a fresh
// run EXISTS each week so the advisor has two points to compare.
//
// CADENCE: weekly, at a wall-clock time chosen to land BEFORE the advisor's
// Sunday 18:00 ET report so the fresh results are in the ledger when the report
// assembles. The default (Sunday 16:00 UTC = 12:00 EDT / 11:00 EST) gives a
// ~6-hour buffer regardless of DST — the spawner keeps no timezone db, so the
// knob is expressed in UTC and documented against the ET report. An
// EDGE_DECAY_INTERVAL_SECS override switches to a fixed-interval loop (testing
// / non-weekly cadence).
//
// GATING: OFF unless EDGE_DECAY_ENABLED=true (safe default — a stray weekly
// spawn storm must be opt-in). DB-only (it lists edges from the registry and
// fires through the store-backed trigger path). Respects the concurrency cap:
// each fire goes through the same pre-check as the HTTP handler, and a
// cap-reached fire stops the sweep (the rest retry next week) rather than
// hammering a full host.
//
// The schedule math + the "which edges to fire" selection are PURE and always
// compiled (+ unit-tested), mirroring net_worth.rs / rithmic_sampler.rs: the
// loop itself needs the store + the api trigger path, so it is gated behind
// the `db` feature alongside the rest of the persistence layer.
// =============================================================================

use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveTime, TimeZone, Utc};

/// Default weekday for the weekly fire, as days-from-Sunday (0 = Sunday). The
/// advisor's weekly report is Sun 18:00 ET, so we fire earlier the SAME day.
pub const DEFAULT_WEEKDAY_SUN0: u32 = 0;

/// Default hour (UTC) for the weekly fire. 16:00 UTC = 12:00 EDT / 11:00 EST —
/// comfortably (~6h) before the advisor's Sun 18:00 ET report in either DST
/// phase, so a run fired now is COMPLETED and in the ledger by report time.
pub const DEFAULT_HOUR_UTC: u32 = 16;

/// Default minute (UTC) for the weekly fire.
pub const DEFAULT_MINUTE_UTC: u32 = 0;

// ─────────────────────────────────────────────────────────────────────────────
// Config — env-gated. OFF unless EDGE_DECAY_ENABLED=true.
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the weekly edge-backtest scheduler, read from the
/// environment. Default-OFF; the weekly wall-clock knobs are only consulted
/// when [`interval_secs`](Self::interval_secs) is unset.
#[derive(Debug, Clone)]
pub struct EdgeDecayConfig {
    /// Master switch. Env: EDGE_DECAY_ENABLED (default false).
    pub enabled: bool,
    /// Fixed-interval override in seconds. `Some(n)` runs a plain every-`n`
    /// loop (like the samplers) INSTEAD of the weekly wall-clock schedule —
    /// intended for testing / non-weekly cadences. Env: EDGE_DECAY_INTERVAL_SECS
    /// (must be > 0 to take effect).
    pub interval_secs: Option<u64>,
    /// Weekly fire weekday as days-from-Sunday (0 = Sun .. 6 = Sat). Env:
    /// EDGE_DECAY_WEEKDAY (clamped to 0..=6; default 0).
    pub weekday_sun0: u32,
    /// Weekly fire hour, UTC. Env: EDGE_DECAY_HOUR_UTC (clamped 0..=23; default 16).
    pub hour_utc: u32,
    /// Weekly fire minute, UTC. Env: EDGE_DECAY_MINUTE_UTC (clamped 0..=59; default 0).
    pub minute_utc: u32,
}

impl Default for EdgeDecayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: None,
            weekday_sun0: DEFAULT_WEEKDAY_SUN0,
            hour_utc: DEFAULT_HOUR_UTC,
            minute_utc: DEFAULT_MINUTE_UTC,
        }
    }
}

impl EdgeDecayConfig {
    /// Read the scheduler config from the environment.
    pub fn from_env() -> Self {
        let enabled = env_bool("EDGE_DECAY_ENABLED", false);
        let interval_secs = std::env::var("EDGE_DECAY_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|n| *n > 0);
        let weekday_sun0 = std::env::var("EDGE_DECAY_WEEKDAY")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|d| *d <= 6)
            .unwrap_or(DEFAULT_WEEKDAY_SUN0);
        let hour_utc = std::env::var("EDGE_DECAY_HOUR_UTC")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|h| *h <= 23)
            .unwrap_or(DEFAULT_HOUR_UTC);
        let minute_utc = std::env::var("EDGE_DECAY_MINUTE_UTC")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|m| *m <= 59)
            .unwrap_or(DEFAULT_MINUTE_UTC);
        Self {
            enabled,
            interval_secs,
            weekday_sun0,
            hour_utc,
            minute_utc,
        }
    }

    /// The scheduler runs only when explicitly enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// How long to sleep before the next fire, computed from `now`. In
    /// fixed-interval mode this is the constant interval; otherwise it is the
    /// duration until the next weekly wall-clock occurrence (UTC).
    pub fn next_delay(&self, now: DateTime<Utc>) -> Duration {
        match self.interval_secs {
            Some(secs) if secs > 0 => Duration::from_secs(secs),
            _ => duration_until_next(now, self.weekday_sun0, self.hour_utc, self.minute_utc),
        }
    }
}

/// Parse a boolean env flag (true/1/yes/on vs false/0/no/off, case-insensitive;
/// anything unrecognised falls back to `default`). Mirrors config.rs.
fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure schedule math — duration until the next weekly wall-clock occurrence
// ─────────────────────────────────────────────────────────────────────────────

/// Duration from `now` until the next occurrence of `weekday_sun0` (days from
/// Sunday, 0 = Sun) at `hour:minute` UTC. If the target time is TODAY but has
/// already passed (or is exactly now), rolls forward a full week — so the loop
/// can never double-fire at the same instant. Inputs are defensively clamped
/// (config already clamps them).
pub fn duration_until_next(
    now: DateTime<Utc>,
    weekday_sun0: u32,
    hour: u32,
    minute: u32,
) -> Duration {
    let weekday_sun0 = weekday_sun0 % 7;
    let hour = hour.min(23);
    let minute = minute.min(59);
    let target_time = NaiveTime::from_hms_opt(hour, minute, 0).unwrap_or_default();

    let today_sun0 = now.weekday().num_days_from_sunday();
    let days_ahead = (i64::from(weekday_sun0) - i64::from(today_sun0)).rem_euclid(7);
    let candidate_date = now.date_naive() + ChronoDuration::days(days_ahead);
    let mut candidate = Utc.from_utc_datetime(&candidate_date.and_time(target_time));
    if candidate <= now {
        candidate += ChronoDuration::days(7);
    }
    (candidate - now)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0))
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure edge selection — which edges get a weekly backtest fired
// ─────────────────────────────────────────────────────────────────────────────

/// The minimal view of an edge the selection needs — decoupled from the
/// db-gated `EdgeRow` so the rule is unit-testable without the DB feature.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EdgeSelector<'a> {
    pub edge_id: &'a str,
    pub active: bool,
    /// The registered backtest image NAME (None / blank = not containerized).
    pub backtest_image: Option<&'a str>,
}

/// Pick the edges to fire a weekly backtest for: ACTIVE edges that carry a
/// non-blank `backtest_image`. This inherently skips `janus-adaptive` (the
/// adaptive edge has no backtest image) and any retired/soft-deleted edge.
/// Returns `(edge_id, trimmed_image)` pairs, preserving input order.
pub fn select_edges_to_backtest<'a>(edges: &[EdgeSelector<'a>]) -> Vec<(&'a str, &'a str)> {
    edges
        .iter()
        .filter(|e| e.active)
        .filter_map(|e| {
            let image = e.backtest_image.map(str::trim).filter(|s| !s.is_empty())?;
            Some((e.edge_id, image))
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// The scheduler loop — needs the store + the api trigger path (db feature)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod scheduler {
    use tracing::{debug, info, warn};

    use super::{EdgeDecayConfig, EdgeSelector, select_edges_to_backtest};
    use crate::api::{AppState, BacktestTrigger, trigger_edge_backtest};

    /// The params handed to a scheduled backtest: the edge's registered
    /// defaults (an empty override object — the same as a bare
    /// POST /edges/{id}/backtest).
    fn scheduled_params() -> serde_json::Value {
        serde_json::json!({})
    }

    /// One weekly sweep: list the registry, fire a backtest for every active
    /// containerized edge through the shared trigger path. BEST-EFFORT — a
    /// per-edge infrastructure error is logged and the sweep moves on; only a
    /// reached concurrency cap (host full) or a missing DB stops it early.
    pub async fn sweep_once(state: &AppState) {
        let Some(store) = state.store.as_ref() else {
            debug!("edge-decay sweep: no DB — nothing to fire");
            return;
        };
        let edges = match store.list_edges().await {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "edge-decay sweep: failed to list edges");
                return;
            }
        };
        let selectors: Vec<EdgeSelector<'_>> = edges
            .iter()
            .map(|e| EdgeSelector {
                edge_id: &e.edge_id,
                active: e.active,
                backtest_image: e.backtest_image.as_deref(),
            })
            .collect();
        let to_fire = select_edges_to_backtest(&selectors);
        info!(
            fire = to_fire.len(),
            registered = edges.len(),
            "edge-decay sweep: firing weekly backtests"
        );

        let params = scheduled_params();
        for (edge_id, _image) in to_fire {
            match trigger_edge_backtest(state, edge_id, &params).await {
                Ok(BacktestTrigger::Spawned { run_id, resp }) => {
                    info!(
                        edge_id,
                        run_id,
                        container_id = %resp.container_id,
                        "edge-decay: weekly backtest fired"
                    );
                }
                Ok(BacktestTrigger::CapReached { running }) => {
                    warn!(
                        edge_id,
                        running,
                        "edge-decay: concurrency cap reached — stopping sweep; \
                         remaining edges retry next week"
                    );
                    break;
                }
                Ok(BacktestTrigger::NoDb) => {
                    // Raced from Some(store) to None — nothing more to do.
                    warn!("edge-decay: DB went away mid-sweep — stopping");
                    break;
                }
                Ok(BacktestTrigger::UnknownEdge | BacktestTrigger::NotContainerized) => {
                    // Selection already filtered these; a race (edge retired
                    // between list and fire) lands here — skip, not fatal.
                    debug!(edge_id, "edge-decay: edge no longer backtestable — skipped");
                }
                Err(e) => {
                    warn!(edge_id, error = %e, "edge-decay: backtest trigger failed");
                }
            }
        }
    }

    /// Run the scheduler loop forever: sleep until the next scheduled fire,
    /// sweep, repeat. Spawned as a detached background task from `main`; only
    /// started when the scheduler is ENABLED and a Postgres store is
    /// configured. Sleeping BEFORE the first sweep mirrors the samplers (no
    /// fire at boot) and, in weekly mode, waits until the real wall-clock slot.
    pub async fn run_scheduler(state: AppState, config: EdgeDecayConfig) {
        if state.store.is_none() {
            info!("edge-decay scheduler: DB disabled — not starting (nothing to fire)");
            return;
        }
        loop {
            let delay = config.next_delay(chrono::Utc::now());
            info!(
                sleep_secs = delay.as_secs(),
                "edge-decay scheduler: sleeping until next weekly fire"
            );
            tokio::time::sleep(delay).await;
            sweep_once(&state).await;
        }
    }
}

#[cfg(feature = "db")]
pub use scheduler::{run_scheduler, sweep_once};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no network, no Docker)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).unwrap()
    }

    // ── config gating ─────────────────────────────────────────────────────────

    #[test]
    fn config_defaults_are_off_and_weekly() {
        let c = EdgeDecayConfig::default();
        assert!(!c.enabled(), "default MUST be off (safe)");
        assert!(c.interval_secs.is_none(), "weekly by default, not interval");
        assert_eq!(c.weekday_sun0, 0, "Sunday");
        assert_eq!(c.hour_utc, 16, "16:00 UTC — before the Sun 18:00 ET report");
        assert_eq!(c.minute_utc, 0);
    }

    // ── schedule math ─────────────────────────────────────────────────────────

    #[test]
    fn interval_override_beats_the_weekly_clock() {
        let c = EdgeDecayConfig {
            enabled: true,
            interval_secs: Some(3600),
            ..EdgeDecayConfig::default()
        };
        // Any `now` yields the fixed interval, ignoring the weekday/hour knobs.
        assert_eq!(
            c.next_delay(utc(2026, 7, 15, 9, 0, 0)),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn weekly_delay_targets_the_next_matching_slot() {
        // 2026-07-15 is a Wednesday. Next Sunday 16:00 UTC is 2026-07-19 16:00.
        let now = utc(2026, 7, 15, 9, 0, 0); // Wed 09:00 UTC
        let d = duration_until_next(now, 0, 16, 0);
        // Wed 09:00 → Sun 16:00 = 4 days + 7 hours = 356_400 s.
        assert_eq!(d.as_secs(), 4 * 86_400 + 7 * 3600);
    }

    #[test]
    fn weekly_same_day_before_target_is_today() {
        // 2026-07-19 is a Sunday. At 09:00 UTC the 16:00 slot is still ahead.
        let now = utc(2026, 7, 19, 9, 0, 0);
        let d = duration_until_next(now, 0, 16, 0);
        assert_eq!(d.as_secs(), 7 * 3600, "same-day, 7h ahead");
    }

    #[test]
    fn weekly_same_day_after_target_rolls_a_week() {
        // Sunday 17:00 UTC — the 16:00 slot passed; next is +7 days.
        let now = utc(2026, 7, 19, 17, 0, 0);
        let d = duration_until_next(now, 0, 16, 0);
        assert_eq!(d.as_secs(), 7 * 86_400 - 3600, "next Sunday, 23h short of 7d");
    }

    #[test]
    fn weekly_exactly_at_target_rolls_a_week_no_double_fire() {
        // At EXACTLY the target instant, roll forward — the loop must not
        // immediately re-fire the slot it just handled.
        let now = utc(2026, 7, 19, 16, 0, 0);
        let d = duration_until_next(now, 0, 16, 0);
        assert_eq!(d.as_secs(), 7 * 86_400);
    }

    #[test]
    fn weekly_delay_is_always_within_a_week_and_positive() {
        // Sweep the whole week at hourly resolution: the next fire is always
        // in (0, 7 days] and never zero (no busy-loop).
        let mut t = utc(2026, 7, 13, 0, 0, 0); // Monday 00:00
        for _ in 0..(24 * 7 + 5) {
            let d = duration_until_next(t, 0, 16, 0).as_secs();
            assert!(d > 0 && d <= 7 * 86_400, "delay {d}s out of range at {t}");
            t += ChronoDuration::hours(1);
        }
    }

    // ── edge selection ────────────────────────────────────────────────────────

    fn sel<'a>(id: &'a str, active: bool, image: Option<&'a str>) -> EdgeSelector<'a> {
        EdgeSelector {
            edge_id: id,
            active,
            backtest_image: image,
        }
    }

    #[test]
    fn selects_active_containerized_edges_only() {
        let edges = vec![
            sel("orb", true, Some("fks-bot-backtest-orb:latest")),
            sel(
                "funding-reversion",
                true,
                Some("fks-bot-backtest-crypto-futures:latest"),
            ),
            // adaptive edge: no backtest image → skipped
            sel("janus-adaptive", true, None),
            // inactive/retired → skipped even with an image
            sel("retired-edge", false, Some("fks-bot-backtest-old:latest")),
            // blank/whitespace image → skipped (never a bare fks-bot- spawn)
            sel("blank-image", true, Some("   ")),
        ];
        let fired = select_edges_to_backtest(&edges);
        assert_eq!(
            fired,
            vec![
                ("orb", "fks-bot-backtest-orb:latest"),
                ("funding-reversion", "fks-bot-backtest-crypto-futures:latest"),
            ],
            "only active + containerized, order preserved"
        );
    }

    #[test]
    fn selection_trims_the_image_and_handles_empty_registry() {
        let edges = vec![sel("orb", true, Some("  fks-bot-backtest-orb:latest  "))];
        assert_eq!(
            select_edges_to_backtest(&edges),
            vec![("orb", "fks-bot-backtest-orb:latest")],
            "image is trimmed"
        );
        assert!(
            select_edges_to_backtest(&[]).is_empty(),
            "empty registry fires nothing"
        );
    }
}
