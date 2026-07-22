// =============================================================================
// supervisor.rs — crash-aware reconcile + prune for spawner-managed bots
//
// Replaces the old naive auto-prune (which force-removed every exited bot
// container whose *created* timestamp was older than the threshold — so a
// long-running live bot that crashed at 3am was removed, with its logs and
// journal, on the first 60s sweep). This module keeps the prune sweep but makes
// it crash-aware and forensics-safe:
//
//   1. PRUNE EXEMPTION — prune keys on the container's *finished* time (via
//      inspect), NOT its created time. Live-mode containers (and any bot that
//      exited unexpectedly) are QUARANTINED: retained for a long, configurable
//      window (`prune_live_after_secs`) instead of the short one-shot window
//      (`prune_after_secs`). One-shot backtest containers (`mode="backtest"`)
//      keep the fast short-window prune — they are MEANT to exit and be reaped.
//
//   2. CRASH DETECTION — a long-lived bot (not a one-shot backtest) found
//      exited/dead with a NON-ZERO exit code while its `bot_runs` row is still
//      OPEN (status still 'spawning'/'running', i.e. it was never stopped via
//      the API) → a crash. Exit code 0 is a clean shutdown (graceful stop or a
//      self-requested exit) and is NEVER a crash, even with an open row — so an
//      out-of-band `docker stop`/`compose down` or a not-yet-committed
//      `record_stop` cannot masquerade as one. A bot that has since come back
//      up (running sibling) is RECOVERED and no longer counts. For a real
//      crash we close the ledger row (status='error'), emit a red `bot_crashed`
//      notification (exactly once per crashed container), and keep the
//      container for the forensics window.
//
//   3. BOUNDED RESTART — opt-in per config (`restart_policy` in the config's
//      config_json). Default OFF, so nothing changes until a config opts in.
//      When enabled, a crashed bot is respawned FROM ITS SAVED CONFIG through
//      the existing pre-flighted `respawn_from_config` path, bounded to
//      `max_restarts` per `window_secs` with exponential backoff.
//
// The *decision* logic (`decide`, `plan_restart`) is pure and unit-tested; the
// async `tick` wires it to Docker + the store + the notifier. The prune/gauge
// half runs with or without a database; crash-record + restart are db-gated
// (there is no ledger to close and no config to restart from without one).
//
// PAPER-PATH SAFETY: a healthy running bot is never a prune or crash candidate
// (only exited/dead containers are examined), so the running paper funding bot
// and the live spot bot are untouched while up. Auto-restart is default-OFF, so
// no bot is respawned unless its config explicitly opts in.
// =============================================================================

use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::metrics;
use crate::models::ContainerInfo;
use crate::prometheus_sd;

/// The `fks.mode` label one-shot backtest containers carry. They are EXPECTED
/// to exit on their own (and their `bot_runs` row is intentionally left open),
/// so they are NEVER treated as crashes and prune on the short retention.
const ONE_SHOT_MODE: &str = "backtest";
/// The `fks.mode` label live-money bots carry — quarantined from fast prune.
const LIVE_MODE: &str = "live";

// ─────────────────────────────────────────────────────────────────────────────
// Restart policy (opt-in, parsed from a config's config_json.restart_policy)
// ─────────────────────────────────────────────────────────────────────────────

/// Per-config auto-restart policy. Absent from a config = OFF (the current
/// behaviour); a crashed bot with no policy is recorded + alerted but never
/// respawned. Lives in the config's JSONB `config_json` blob (no schema
/// migration), mirroring how `bot_id`/`env`/`secrets` are stored.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RestartPolicy {
    /// Master switch. `false` (the default when the object is present but the
    /// field omitted) means the policy is inert.
    #[serde(default)]
    pub enabled: bool,
    /// Max restart attempts allowed within `window_secs`. Once reached, the bot
    /// is left down and an "auto-restart budget exhausted" alert fires.
    #[serde(default = "default_max_restarts")]
    pub max_restarts: usize,
    /// Sliding window (seconds) over which `max_restarts` is counted.
    #[serde(default = "default_window_secs")]
    pub window_secs: i64,
    /// Base backoff (seconds); attempt N waits `base * 2^N`, capped at
    /// `backoff_max_secs`.
    #[serde(default = "default_backoff_base_secs")]
    pub backoff_base_secs: u64,
    /// Upper bound on the computed backoff delay (seconds).
    #[serde(default = "default_backoff_max_secs")]
    pub backoff_max_secs: u64,
}

fn default_max_restarts() -> usize {
    3
}
fn default_window_secs() -> i64 {
    3600
}
fn default_backoff_base_secs() -> u64 {
    10
}
fn default_backoff_max_secs() -> u64 {
    300
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_restarts: default_max_restarts(),
            window_secs: default_window_secs(),
            backoff_base_secs: default_backoff_base_secs(),
            backoff_max_secs: default_backoff_max_secs(),
        }
    }
}

/// What to do about a crashed bot given how many restarts already happened in
/// the current window. Pure — the caller supplies the count, applies the delay,
/// and records the attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum RestartAction {
    /// Respawn after `delay_secs` (exponential backoff).
    Restart { delay_secs: u64 },
    /// The per-window budget is spent — leave the bot down and alert.
    Exhausted,
}

/// Decide whether (and after how long) to restart a crashed bot. Bounded:
/// `attempts_in_window >= max_restarts` ⇒ `Exhausted`; otherwise an exponential
/// backoff `base * 2^attempts`, saturating and capped at `backoff_max_secs`.
pub fn plan_restart(attempts_in_window: usize, policy: &RestartPolicy) -> RestartAction {
    if !policy.enabled || attempts_in_window >= policy.max_restarts {
        return RestartAction::Exhausted;
    }
    let factor = 2u64
        .checked_pow(attempts_in_window as u32)
        .unwrap_or(u64::MAX);
    let delay = policy
        .backoff_base_secs
        .saturating_mul(factor)
        .min(policy.backoff_max_secs);
    RestartAction::Restart { delay_secs: delay }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure sweep decision — the heart of the prune-exemption + crash detection
// ─────────────────────────────────────────────────────────────────────────────

/// A long-lived bot container found exited without an operator stop.
#[derive(Debug, Clone, PartialEq)]
pub struct CrashedBot {
    pub container_id: String,
    pub bot_id: String,
    pub mode: String,
    pub image: String,
    pub exit_code: Option<i64>,
}

/// The outcome of one sweep over the current bot containers.
#[derive(Debug, Default, PartialEq)]
pub struct SweepPlan {
    /// Long-lived bots that exited without an operator stop (this sweep).
    pub crashes: Vec<CrashedBot>,
    /// Container ids eligible for removal now (finished-time past retention).
    pub prune: Vec<String>,
    /// Running bot containers (all modes) — the `fks_spawner_running_bots` gauge.
    pub running_total: usize,
    /// Running bot containers in live mode — `fks_spawner_live_bots_running`.
    pub running_live: usize,
    /// Long-lived bots currently retained as crashed (open OR error status) —
    /// the `fks_spawner_crashed_bots` gauge. Includes this sweep's fresh
    /// crashes so the gauge fires on the SAME tick a crash is detected.
    pub crashed_present: usize,
}

fn run_is_open(status: Option<&str>) -> bool {
    matches!(status, Some("spawning") | Some("running"))
}
fn run_is_error(status: Option<&str>) -> bool {
    matches!(status, Some("error"))
}

/// Decide, from the current bot containers and each exited one's `bot_runs`
/// status, which are crashes and which are prune-eligible.
///
/// `items` pairs each bot container with its latest `bot_runs` status (`None`
/// when there is no DB, or no row). `now` is the reference time; `finished_at`
/// (falling back to `created_at`) is the retention clock — NOT `created_at`
/// alone, which was the original bug.
pub fn decide(
    items: &[(ContainerInfo, Option<String>)],
    now: DateTime<Utc>,
    prune_after_secs: i64,
    prune_live_after_secs: i64,
) -> SweepPlan {
    let mut plan = SweepPlan::default();

    // Pre-pass: bot_ids that currently have a RUNNING container. A retained
    // crashed container whose bot has since come back up — an operator respawn
    // or a successful bounded auto-restart — is RECOVERED: the outage is over,
    // so it must not keep the crash gauge pinned >0 (and the LiveBotCrashed page
    // firing) for the whole 7-day forensics window. The container stays retained
    // for forensics; it just no longer counts as an active crash.
    let running_bot_ids: std::collections::HashSet<&str> = items
        .iter()
        .filter(|(c, _)| c.state == "running" && !c.bot_id.is_empty())
        .map(|(c, _)| c.bot_id.as_str())
        .collect();

    for (c, status) in items {
        match c.state.as_str() {
            "running" => {
                plan.running_total += 1;
                if c.mode == LIVE_MODE {
                    plan.running_live += 1;
                }
            }
            "exited" | "dead" => {
                let one_shot = c.mode == ONE_SHOT_MODE;
                let status = status.as_deref();
                let open = run_is_open(status);
                let is_error = run_is_error(status);

                // A container that exited 0 shut down CLEANLY — a graceful stop
                // (SIGTERM trapped) or a self-requested exit — never a crash,
                // even if its ledger row was never closed via the API. That
                // covers an out-of-band `docker stop` / `compose down`, a clean
                // self-exit, and the window before a best-effort `record_stop`
                // has committed. A genuine crash (panic, OOM-kill, segfault, any
                // fatal signal) always exits NON-ZERO, so this never masks one.
                let clean_exit = c.exit_code == Some(0);

                // Bot already back up (see `running_bot_ids`): don't page,
                // don't restart, don't count — just let normal prune retention
                // apply below.
                let recovered = !c.bot_id.is_empty() && running_bot_ids.contains(c.bot_id.as_str());

                // CRASH: a long-lived bot exited NON-ZERO while its ledger row is
                // still open (never stopped via the API). Record it and retain
                // the container this tick — never prune a just-detected crash.
                if !one_shot && open && !clean_exit && !recovered {
                    plan.crashes.push(CrashedBot {
                        container_id: c.id.clone(),
                        bot_id: c.bot_id.clone(),
                        mode: c.mode.clone(),
                        image: c.image.clone(),
                        exit_code: c.exit_code,
                    });
                    plan.crashed_present += 1;
                    continue;
                }
                if !one_shot && is_error && !recovered {
                    plan.crashed_present += 1;
                }

                // PRUNE EXEMPTION: live-mode and crashed (error) long-lived
                // bots get the long forensics/quarantine retention; one-shot
                // backtests and clean-stopped non-live bots get the short one.
                let quarantined = !one_shot && (c.mode == LIVE_MODE || is_error);
                let retention = if quarantined {
                    prune_live_after_secs
                } else {
                    prune_after_secs
                };

                let reference = c.finished_at.or(c.created_at);
                let eligible = reference
                    .map(|t| now.signed_duration_since(t).num_seconds() >= retention)
                    .unwrap_or(false);
                if eligible {
                    plan.prune.push(c.id.clone());
                }
            }
            // created / paused / restarting — leave alone.
            _ => {}
        }
    }

    plan
}

// ─────────────────────────────────────────────────────────────────────────────
// Async reconcile loop — wires the pure decision to Docker + store + notifier
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory restart bookkeeping, owned by the single supervisor task. Tracks
/// recent restart attempt times per bot_id so `plan_restart` can enforce the
/// per-window budget across ticks. No DB/schema needed.
///
/// Its only non-test consumer is `maybe_restart` (db-gated — a restart needs a
/// saved config to respawn from), so on the stateless build the field/methods
/// have no runtime reader.
#[derive(Debug, Default)]
#[cfg_attr(not(feature = "db"), allow(dead_code))]
pub struct RestartTracker {
    attempts: std::collections::HashMap<String, Vec<DateTime<Utc>>>,
    /// Container ids whose crash has already been handled (notified + restart
    /// evaluated) this process-lifetime. Crash handling keys on the ledger row
    /// being OPEN, so if `record_error` fails to CLOSE the row (a DB blip — and
    /// a host OOM/reboot that kills the bot is exactly the event that also
    /// stresses Postgres), `decide` would re-detect the SAME container as a
    /// crash on every 60s tick and re-page + re-restart it. This set makes crash
    /// handling idempotent per container independent of the DB write succeeding.
    handled: std::collections::HashSet<String>,
}

#[cfg_attr(not(feature = "db"), allow(dead_code))]
impl RestartTracker {
    /// Prune attempts older than the window and return the remaining count.
    fn count_in_window(&mut self, bot_id: &str, window_secs: i64, now: DateTime<Utc>) -> usize {
        let cutoff = now - chrono::Duration::seconds(window_secs);
        let v = self.attempts.entry(bot_id.to_string()).or_default();
        v.retain(|t| *t >= cutoff);
        v.len()
    }
    fn record(&mut self, bot_id: &str, now: DateTime<Utc>) {
        self.attempts
            .entry(bot_id.to_string())
            .or_default()
            .push(now);
    }
    /// Mark a crashed container as handled. Returns `true` the FIRST time (do
    /// the work), `false` on every subsequent call (already notified/restarted
    /// — skip so a re-detected-but-unclosed crash cannot spam).
    fn mark_handled(&mut self, container_id: &str) -> bool {
        self.handled.insert(container_id.to_string())
    }
    /// Drop handled entries for containers no longer present (pruned/removed) so
    /// the set cannot grow unbounded across the process lifetime.
    fn retain_handled(&mut self, present: &std::collections::HashSet<String>) {
        self.handled.retain(|id| present.contains(id));
    }
}

/// Run the supervisor forever: every `interval_secs`, reconcile bot state
/// (crash detection + gauges + prune). Modelled on the other background tasks
/// (sleep-first cadence).
pub async fn run(state: crate::api::AppState, interval_secs: u64) {
    let interval = Duration::from_secs(interval_secs.max(1));
    let mut tracker = RestartTracker::default();
    loop {
        tokio::time::sleep(interval).await;
        tick(&state, &mut tracker).await;
    }
}

/// One reconcile pass. Never panics; every failure degrades to a warn.
pub async fn tick(state: &crate::api::AppState, tracker: &mut RestartTracker) {
    let docker = state.docker.as_ref();
    let config = &state.config;

    let bots = match docker.list_bots().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "supervisor: list_bots failed");
            return;
        }
    };

    // Gather (container, run_status). Exited/dead containers are inspected so we
    // have their real finished_at + exit_code (the list summary lacks both) and
    // their ledger status decides crash-vs-clean-stop.
    let mut items: Vec<(ContainerInfo, Option<String>)> = Vec::with_capacity(bots.len());
    for b in bots {
        match b.state.as_str() {
            "exited" | "dead" => {
                let info = docker.inspect(&b.id).await.unwrap_or(b);
                let status = fetch_run_status(state, &info.id).await;
                items.push((info, status));
            }
            _ => items.push((b, None)),
        }
    }

    let plan = decide(
        &items,
        Utc::now(),
        config.prune_after_secs,
        config.prune_live_after_secs,
    );

    // Gauges (always available).
    metrics::RUNNING_BOTS.set(plan.running_total as f64);
    metrics::LIVE_BOTS_RUNNING.set(plan.running_live as f64);
    metrics::CRASHED_BOTS.set(plan.crashed_present as f64);

    // Handle crashes: close the ledger row, alert, and (opt-in) restart.
    for crash in &plan.crashes {
        handle_crash(state, crash, tracker).await;
    }

    // Forget handled crashes whose container is gone, so the dedup set cannot
    // grow without bound. `items` is every bot container this sweep saw.
    let present: std::collections::HashSet<String> =
        items.iter().map(|(c, _)| c.id.clone()).collect();
    tracker.retain_handled(&present);

    // Prune eligible containers.
    let mut pruned = 0usize;
    for id in &plan.prune {
        match docker.remove(id).await {
            Ok(_) => {
                info!(container = %&id[..12.min(id.len())], "supervisor: pruned stopped container");
                pruned += 1;
            }
            Err(e) => warn!(container = %id, error = %e, "supervisor: prune remove failed"),
        }
    }
    if pruned > 0 {
        metrics::PRUNE_TOTAL.inc_by(pruned as f64);
        prometheus_sd::update_sd_file(docker, config).await;
        notify_pruned(state, pruned);
    }
}

/// Fetch the latest `bot_runs` status for a container (`None` without a DB).
async fn fetch_run_status(state: &crate::api::AppState, container_id: &str) -> Option<String> {
    #[cfg(feature = "db")]
    {
        let store = state.store.as_ref()?;
        match store.run_status(container_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, container = %container_id, "supervisor: run_status query failed");
                None
            }
        }
    }
    #[cfg(not(feature = "db"))]
    {
        let _ = (state, container_id);
        None
    }
}

/// Close the crashed run's ledger row, emit a red `bot_crashed` alert, and
/// (opt-in) schedule a bounded, backed-off restart from the saved config.
async fn handle_crash(
    state: &crate::api::AppState,
    crash: &CrashedBot,
    tracker: &mut RestartTracker,
) {
    // Idempotency guard: notify + restart AT MOST once per crashed container.
    // `decide` re-adds a crash every tick while its ledger row stays open, so if
    // the close below (`record_error`) fails, without this gate every subsequent
    // 60s tick would re-page and re-restart the same crash.
    if !tracker.mark_handled(&crash.container_id) {
        return;
    }

    warn!(
        container_id = %crash.container_id,
        bot_id = %crash.bot_id,
        mode = %crash.mode,
        exit_code = ?crash.exit_code,
        "supervisor: detected unexpected bot exit (crash)"
    );

    let detail = match crash.exit_code {
        Some(code) => format!("unexpected exit (crash), exit_code={code}"),
        None => "unexpected exit (crash)".to_string(),
    };

    #[cfg(feature = "db")]
    {
        if let Some(store) = state.store.as_ref()
            && let Err(e) = store.record_error(&crash.container_id, &detail).await
        {
            warn!(error = %e, container_id = %crash.container_id, "supervisor: record_error failed");
        }

        crate::api::spawn_dispatch(
            state,
            crate::notifications::NotificationEvent::crashed(
                &crash.bot_id,
                &crash.image,
                &crash.mode,
                &detail,
            ),
        );

        maybe_restart(state, crash, tracker).await;
    }
    #[cfg(not(feature = "db"))]
    {
        let _ = (state, tracker, detail);
    }
}

/// Opt-in bounded restart. No-op unless the crashed bot's saved config carries
/// an enabled `restart_policy`. Respawns through the pre-flighted
/// `respawn_from_config` — the same path `/configs/{name}/respawn` uses.
#[cfg(feature = "db")]
async fn maybe_restart(
    state: &crate::api::AppState,
    crash: &CrashedBot,
    tracker: &mut RestartTracker,
) {
    let Some(store) = state.store.as_ref() else {
        return;
    };
    if crash.bot_id.is_empty() {
        return;
    }

    let cfg = match store.get_config_by_bot_id(&crash.bot_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return, // no saved config → nothing to restart from
        Err(e) => {
            warn!(error = %e, bot_id = %crash.bot_id, "supervisor: config lookup for restart failed");
            return;
        }
    };
    let Some(policy) = cfg.restart_policy.clone() else {
        return; // no policy → default OFF (current behaviour)
    };
    if !policy.enabled {
        return;
    }

    let now = Utc::now();
    let attempts = tracker.count_in_window(&crash.bot_id, policy.window_secs, now);

    match plan_restart(attempts, &policy) {
        RestartAction::Restart { delay_secs } => {
            tracker.record(&crash.bot_id, now);
            info!(
                bot_id = %crash.bot_id,
                attempt = attempts + 1,
                max = policy.max_restarts,
                delay_secs,
                "supervisor: scheduling bounded auto-restart of crashed bot"
            );
            let state = state.clone();
            let bot_id = crash.bot_id.clone();
            // Carry the attempt bookkeeping into the detached restart so the
            // recovery event can report "attempt N/M" (policy/attempts don't
            // outlive this arm otherwise).
            let attempt_no = attempts + 1;
            let max_restarts = policy.max_restarts;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                match crate::api::respawn_from_config(&state, &cfg, bot_id.clone()).await {
                    Ok((_old, resp)) => {
                        info!(bot_id = %bot_id, new_container_id = %resp.container_id, "supervisor: auto-restarted crashed bot");
                        // Close the loop opened by the (always-delivered) crash
                        // page: emit an always-delivered `bot_restarted` so a
                        // crash-scoped channel learns the bot self-healed.
                        crate::api::spawn_dispatch(
                            &state,
                            crate::notifications::NotificationEvent::restarted(
                                &bot_id,
                                &resp.image,
                                &resp.mode,
                                &format!(
                                    "attempt {attempt_no}/{max_restarts}, new container {}",
                                    resp.container_id
                                ),
                            ),
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, bot_id = %bot_id, "supervisor: auto-restart failed");
                        crate::api::spawn_dispatch(
                            &state,
                            crate::notifications::NotificationEvent::error(
                                &bot_id,
                                "",
                                "live",
                                &format!("auto-restart failed: {e}"),
                            ),
                        );
                    }
                }
            });
        }
        RestartAction::Exhausted => {
            warn!(
                bot_id = %crash.bot_id,
                max = policy.max_restarts,
                window_secs = policy.window_secs,
                "supervisor: auto-restart budget exhausted — leaving bot down"
            );
            crate::api::spawn_dispatch(
                state,
                crate::notifications::NotificationEvent::error(
                    &crash.bot_id,
                    &crash.image,
                    &crash.mode,
                    &format!(
                        "auto-restart budget exhausted ({} in {}s) — bot left down",
                        policy.max_restarts, policy.window_secs
                    ),
                ),
            );
        }
    }
}

/// Best-effort grey prune notification (preserves the prior auto-prune
/// behaviour). No-op without a DB / with notifications disabled.
fn notify_pruned(state: &crate::api::AppState, count: usize) {
    #[cfg(feature = "db")]
    crate::api::spawn_dispatch(
        state,
        crate::notifications::NotificationEvent::pruned(count),
    );
    #[cfg(not(feature = "db"))]
    let _ = (state, count);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure decision logic (no Docker, no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn container(
        id: &str,
        state: &str,
        mode: &str,
        created_secs_ago: i64,
        finished_secs_ago: Option<i64>,
    ) -> ContainerInfo {
        let now = Utc::now();
        ContainerInfo {
            id: id.to_string(),
            id_full: id.to_string(),
            name: format!("fks-bot-{id}"),
            image: "fks-bot-x:latest".to_string(),
            status: state.to_string(),
            state: state.to_string(),
            bot_id: id.to_string(),
            mode: mode.to_string(),
            created_at: Some(now - chrono::Duration::seconds(created_secs_ago)),
            started_at: Some(now - chrono::Duration::seconds(created_secs_ago)),
            finished_at: finished_secs_ago.map(|s| now - chrono::Duration::seconds(s)),
            labels: std::collections::HashMap::new(),
            cpu_percent: None,
            memory_bytes: None,
            memory_limit_bytes: None,
            exit_code: None,
        }
    }

    // ── Prune: created-vs-finished ──────────────────────────────────────────

    #[test]
    fn prune_keys_on_finished_not_created_time() {
        // A one-shot created long ago (10h) but that only FINISHED 10s ago must
        // NOT be pruned — the old bug keyed on created_at and would remove it.
        let items = vec![(
            container("bt1", "exited", "backtest", 36_000, Some(10)),
            // one-shot ledger rows stay open; that must not matter for backtests
            Some("running".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(
            plan.prune.is_empty(),
            "recently-finished one-shot must survive (finished-time keying)"
        );
        assert!(plan.crashes.is_empty(), "a backtest is never a crash");
    }

    #[test]
    fn prune_removes_old_finished_one_shot() {
        let items = vec![(
            container("bt2", "exited", "backtest", 36_000, Some(3600)),
            Some("running".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.prune, vec!["bt2".to_string()]);
    }

    #[test]
    fn prune_falls_back_to_created_when_no_finished_time() {
        // exited with no finished_at recorded → created_at is the clock.
        let items = vec![(
            container("bt3", "exited", "backtest", 3600, None),
            Some("stopped".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.prune, vec!["bt3".to_string()]);
    }

    // ── Prune exemption: live-mode + crashed quarantine ─────────────────────

    #[test]
    fn live_bot_is_never_fast_pruned() {
        // A live bot cleanly stopped 10 min ago: past the 300s one-shot window
        // but well within the 7d live-quarantine window → retained.
        let items = vec![(
            container("live1", "exited", "live", 86_400, Some(600)),
            Some("stopped".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(
            plan.prune.is_empty(),
            "live-mode container must be quarantined, not fast-pruned"
        );
    }

    #[test]
    fn crashed_bot_is_quarantined_not_pruned() {
        // A paper bot that already crashed (ledger status='error') finished
        // 10 min ago — past the short window but retained for forensics.
        let items = vec![(
            container("p1", "exited", "paper", 86_400, Some(600)),
            Some("error".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(plan.prune.is_empty(), "crashed bot retained for forensics");
        assert_eq!(plan.crashed_present, 1, "counted in the crashed gauge");
    }

    #[test]
    fn crashed_bot_pruned_after_forensics_window() {
        let items = vec![(
            container("p1", "exited", "paper", 800_000, Some(700_000)),
            Some("error".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.prune, vec!["p1".to_string()]);
    }

    // ── Crash detection ─────────────────────────────────────────────────────

    #[test]
    fn open_ledger_row_on_exited_live_bot_is_a_crash() {
        let mut c = container("spot", "exited", "live", 86_400, Some(30));
        c.exit_code = Some(139);
        let items = vec![(c, Some("running".to_string()))];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.crashes.len(), 1);
        assert_eq!(plan.crashes[0].bot_id, "spot");
        assert_eq!(plan.crashes[0].exit_code, Some(139));
        assert_eq!(plan.crashed_present, 1);
        assert!(
            plan.prune.is_empty(),
            "a just-detected crash is never pruned the same tick"
        );
    }

    #[test]
    fn cleanly_stopped_bot_is_not_a_crash() {
        let items = vec![(
            container("d1", "exited", "paper", 86_400, Some(30)),
            Some("stopped".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(plan.crashes.is_empty());
        assert_eq!(plan.crashed_present, 0);
    }

    #[test]
    fn clean_exit_zero_with_open_row_is_not_a_crash() {
        // A long-lived bot that exited 0 (graceful stop / self-exit) while its
        // ledger row was never closed via the API (out-of-band `docker stop`,
        // or a record_stop that hasn't committed). Exit 0 ⇒ clean, NOT a crash.
        let mut c = container("spot", "exited", "live", 86_400, Some(30));
        c.exit_code = Some(0);
        let items = vec![(c, Some("running".to_string()))];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(
            plan.crashes.is_empty(),
            "exit 0 is a clean shutdown, never a crash"
        );
        assert_eq!(plan.crashed_present, 0, "clean exit does not pin the gauge");
        assert!(
            plan.prune.is_empty(),
            "a live container is still quarantined (not fast-pruned) after a clean exit"
        );
    }

    #[test]
    fn clean_exit_zero_paper_bot_prunes_on_short_window() {
        // A non-live bot that self-exited 0 long ago: clean, so it takes the
        // SHORT retention (not the crash quarantine).
        let mut c = container("p", "exited", "paper", 4000, Some(3600));
        c.exit_code = Some(0);
        let items = vec![(c, Some("running".to_string()))];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(plan.crashes.is_empty());
        assert_eq!(plan.prune, vec!["p".to_string()]);
    }

    #[test]
    fn nonzero_exit_with_open_row_is_still_a_crash() {
        // Guard the fix: exit 0 exemption must not swallow a real crash. A
        // SIGTERM-not-trapped kill (143) or OOM (137) is non-zero ⇒ crash.
        for code in [1, 101, 134, 137, 139, 143] {
            let mut c = container("spot", "exited", "live", 86_400, Some(30));
            c.exit_code = Some(code);
            let items = vec![(c, Some("running".to_string()))];
            let plan = decide(&items, Utc::now(), 300, 604_800);
            assert_eq!(plan.crashes.len(), 1, "exit {code} must be a crash");
        }
    }

    #[test]
    fn recovered_bot_clears_crash_gauge() {
        // Finding-2 regression: a crashed container (row closed to 'error') whose
        // bot has come back up (a NEW running container with the same bot_id,
        // e.g. after a successful auto-restart or an operator respawn) must NOT
        // keep the LiveBotCrashed page firing for the 7-day forensics window.
        let mut old = container("old_ctr", "exited", "live", 86_400, Some(60));
        old.bot_id = "spot".to_string();
        let mut new = container("new_ctr", "running", "live", 30, None);
        new.bot_id = "spot".to_string();
        let items = vec![(old, Some("error".to_string())), (new, None)];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(
            plan.crashed_present, 0,
            "recovered bot must not pin the crash gauge"
        );
        assert_eq!(plan.running_live, 1);
    }

    #[test]
    fn recovered_fresh_crash_is_not_repaged_or_restarted() {
        // Same, for a still-OPEN row (crash detected the same tick the bot is
        // already back up): it must not be pushed as a fresh crash (no re-page,
        // no double auto-restart).
        let mut old = container("old_ctr", "exited", "live", 86_400, Some(30));
        old.bot_id = "spot".to_string();
        old.exit_code = Some(139);
        let mut new = container("new_ctr", "running", "live", 5, None);
        new.bot_id = "spot".to_string();
        let items = vec![(old, Some("running".to_string())), (new, None)];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(
            plan.crashes.is_empty(),
            "a recovered crash must not be re-paged/restarted"
        );
        assert_eq!(plan.crashed_present, 0);
    }

    #[test]
    fn unrecovered_crash_still_pins_gauge() {
        // Control for the two tests above: no running sibling ⇒ still a crash.
        let mut old = container("old_ctr", "exited", "live", 86_400, Some(30));
        old.bot_id = "spot".to_string();
        old.exit_code = Some(139);
        let items = vec![(old, Some("error".to_string()))];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.crashed_present, 1);
    }

    #[test]
    fn finished_backtest_with_open_row_is_not_a_crash() {
        // The critical false-positive guard: a finished backtest's bot_runs row
        // is never closed (stays 'running'), but it must NOT be a crash.
        let items = vec![(
            container("bt", "exited", "backtest", 600, Some(30)),
            Some("running".to_string()),
        )];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(plan.crashes.is_empty());
        assert_eq!(plan.crashed_present, 0);
    }

    #[test]
    fn no_db_status_does_not_false_report_crash() {
        // Without a DB (status None) we cannot prove a crash → never report one,
        // but a live bot is still quarantined and a one-shot still prunes.
        let items = vec![
            (container("live", "exited", "live", 86_400, Some(30)), None),
            (
                container("bt", "exited", "backtest", 36_000, Some(3600)),
                None,
            ),
        ];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert!(plan.crashes.is_empty());
        assert!(plan.prune.contains(&"bt".to_string()));
        assert!(!plan.prune.contains(&"live".to_string()));
    }

    // ── Gauges ──────────────────────────────────────────────────────────────

    #[test]
    fn running_gauges_count_by_mode() {
        let items = vec![
            (container("a", "running", "live", 10, None), None),
            (container("b", "running", "paper", 10, None), None),
            (container("c", "running", "live", 10, None), None),
            (
                container("d", "exited", "paper", 10, Some(1)),
                Some("stopped".into()),
            ),
        ];
        let plan = decide(&items, Utc::now(), 300, 604_800);
        assert_eq!(plan.running_total, 3);
        assert_eq!(plan.running_live, 2);
    }

    // ── Restart policy (bounded + backoff) ──────────────────────────────────

    fn policy() -> RestartPolicy {
        RestartPolicy {
            enabled: true,
            max_restarts: 3,
            window_secs: 3600,
            backoff_base_secs: 10,
            backoff_max_secs: 300,
        }
    }

    #[test]
    fn restart_backoff_is_exponential() {
        assert_eq!(
            plan_restart(0, &policy()),
            RestartAction::Restart { delay_secs: 10 }
        );
        assert_eq!(
            plan_restart(1, &policy()),
            RestartAction::Restart { delay_secs: 20 }
        );
        assert_eq!(
            plan_restart(2, &policy()),
            RestartAction::Restart { delay_secs: 40 }
        );
    }

    #[test]
    fn restart_is_bounded_by_max() {
        assert_eq!(plan_restart(3, &policy()), RestartAction::Exhausted);
        assert_eq!(plan_restart(9, &policy()), RestartAction::Exhausted);
    }

    #[test]
    fn restart_backoff_is_capped() {
        let mut p = policy();
        p.max_restarts = 20;
        p.backoff_base_secs = 10;
        p.backoff_max_secs = 300;
        // 10 * 2^10 = 10240, capped to 300.
        assert_eq!(
            plan_restart(10, &p),
            RestartAction::Restart { delay_secs: 300 }
        );
    }

    #[test]
    fn disabled_policy_never_restarts() {
        let mut p = policy();
        p.enabled = false;
        assert_eq!(plan_restart(0, &p), RestartAction::Exhausted);
    }

    #[test]
    fn restart_tracker_windows_out_old_attempts() {
        let mut t = RestartTracker::default();
        let now = Utc::now();
        // Two attempts: one 2h ago (outside a 1h window), one 1m ago.
        t.attempts.insert(
            "bot".to_string(),
            vec![
                now - chrono::Duration::hours(2),
                now - chrono::Duration::minutes(1),
            ],
        );
        assert_eq!(t.count_in_window("bot", 3600, now), 1);
    }

    #[test]
    fn mark_handled_is_idempotent_per_container() {
        // Finding-3 regression: crash handling must fire once per container even
        // if the ledger close keeps failing and decide re-emits it every tick.
        let mut t = RestartTracker::default();
        assert!(t.mark_handled("ctr_a"), "first handling does the work");
        assert!(!t.mark_handled("ctr_a"), "re-detected crash is skipped");
        assert!(!t.mark_handled("ctr_a"));
        assert!(
            t.mark_handled("ctr_b"),
            "a different crash is handled fresh"
        );
    }

    #[test]
    fn retain_handled_drops_gone_containers() {
        let mut t = RestartTracker::default();
        t.mark_handled("ctr_a");
        t.mark_handled("ctr_b");
        let present: std::collections::HashSet<String> =
            ["ctr_b".to_string()].into_iter().collect();
        t.retain_handled(&present);
        // ctr_a was pruned → forgotten, so if its id is ever reused it handles fresh.
        assert!(t.mark_handled("ctr_a"), "gone container is forgotten");
        assert!(
            !t.mark_handled("ctr_b"),
            "still-present container stays handled"
        );
    }

    #[test]
    fn restart_policy_defaults_when_partial_json() {
        // A config that only flips `enabled` inherits sane defaults.
        let p: RestartPolicy =
            serde_json::from_value(serde_json::json!({"enabled": true})).unwrap();
        assert!(p.enabled);
        assert_eq!(p.max_restarts, 3);
        assert_eq!(p.window_secs, 3600);
        assert_eq!(p.backoff_base_secs, 10);
        assert_eq!(p.backoff_max_secs, 300);
    }
}
