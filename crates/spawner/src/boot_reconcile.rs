// =============================================================================
// boot_reconcile.rs — boot-time bot reconciliation
//
// THE GAP THIS CLOSES (confirmed live 2026-08-13/14): `oryx` (the host) did a
// clean reboot. Every infra container came back via Docker's restart policy,
// but `fks_bot_spawner` came back tracking ZERO bots — and the two live-money
// bot containers (`crypto-spot`, `crypto-funding`) are spawned with NO restart
// policy (see `docker_client::spawn`), so a host reboot removes them ENTIRELY,
// not just stops them. Nothing on spawner startup re-read what was running
// before and brought it back; a human had to notice and manually rebuild +
// respawn both bots by hand.
//
// This module closes that gap: at startup, for every active saved config that
// carries a `bot_id`, look at the LATEST `bot_runs` row for that bot (keyed by
// container NAME, `fks-bot-{bot_id}` — the container itself may no longer
// exist at all, so there is nothing to `inspect` by id, only the ledger row).
// If that row is still OPEN ('running'/'spawning' — the exact same signal
// `supervisor::run_is_open` uses to tell a crash from an intentional stop) the
// bot was never cleanly stopped via the API: either the process/host
// disappeared out from under it, or it is still up and the SPAWNER is what
// restarted (a routine redeploy). Docker is asked for ground truth to tell
// those two apart — a bot already running is left alone.
//
// SAFETY properties (all load-bearing, see the module doc for `decide`):
//   1. IDEMPOTENT — a bot Docker reports as currently RUNNING is never
//      touched, however its ledger row reads. A spawner-only redeploy while
//      the bots are still up must never bounce them.
//   2. DB-DEGRADES-STATELESS — no store (Postgres unreachable at boot, same
//      bounded-retry-then-stateless posture as `BotRunStore::try_connect`) ⇒
//      log and return, never panic, never fail spawner startup.
//   3. OPT-OUTABLE — `BOOT_RECONCILE_ENABLED=false` disables this entirely.
//      Default ON: the whole point is a safety net that doesn't require a
//      human to remember to ask for it.
//   4. NEVER RESURRECTS A DELIBERATE DECISION — a clean stop ('stopped' /
//      'pruned') is left down. An 'error' row (a crash the supervisor already
//      recorded, and — per that config's OWN `restart_policy` — either
//      restarted or deliberately left down) is also left alone; boot
//      reconciliation does not second-guess a decision the crash-supervisor
//      already made, it only recovers the "nobody was watching" case where
//      the process disappeared before anything could react at all.
//
// The *decision* logic (`decide`) is pure and unit-tested; `run` wires it to
// the store + Docker + the existing `respawn_from_config` path (the same one
// `POST /configs/{name}/respawn` uses). It does NOT eliminate the
// funding-bot paper-journal-loss issue (container-local, no volume) — a
// reconciled respawn still starts the bot fresh, same as any other respawn;
// it just does so automatically instead of requiring a human to notice.
// =============================================================================

#![cfg(feature = "db")]

use std::collections::{HashMap, HashSet};

use tracing::{info, warn};

use crate::api::{AppState, respawn_from_config, spawn_dispatch};
use crate::db::BotConfigRow;
use crate::metrics;
use crate::notifications::NotificationEvent;
use crate::supervisor::run_is_open;

// ─────────────────────────────────────────────────────────────────────────────
// Pure decision logic
// ─────────────────────────────────────────────────────────────────────────────

/// One active saved config's bot, paired with the latest `bot_runs` status
/// found for its container name. `None` = no `bot_runs` row exists at all yet
/// (a saved config that was never actually spawned) — nothing to reconcile.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigRunState {
    pub config_name: String,
    pub bot_id: String,
    pub last_status: Option<String>,
}

/// A bot `decide` selected for respawn, with the status that triggered it
/// (folded into the log line / notification so the "why" is auditable, not
/// silent magic).
#[derive(Debug, Clone, PartialEq)]
pub struct ReconcileTarget {
    pub config_name: String,
    pub bot_id: String,
    pub last_status: String,
}

/// The outcome of one boot-time reconciliation pass. Every bucket is kept
/// (not just `respawn`) so the caller can log a complete, auditable picture
/// of what was checked and why each bot was or wasn't touched.
#[derive(Debug, Default, PartialEq)]
pub struct ReconcilePlan {
    /// Bots left OPEN and NOT currently running in Docker — respawn these.
    pub respawn: Vec<ReconcileTarget>,
    /// Bots Docker reports as already running — left alone (property 1:
    /// idempotent against a spawner-only redeploy).
    pub already_running: Vec<String>,
    /// Bots whose last row shows a clean stop (`stopped`/`pruned`) — an
    /// intentional shutdown, never resurrected.
    pub clean_stop: Vec<String>,
    /// Bots whose last row is `error` — a crash already recorded (and
    /// resolved, one way or another) by the supervisor. Boot reconciliation
    /// does not second-guess that decision.
    pub already_errored: Vec<String>,
    /// Bots with no `bot_runs` row at all — a saved config never spawned.
    pub never_spawned: Vec<String>,
}

/// Decide which bots to respawn at boot.
///
/// `configs` is every active saved config that carries a `bot_id`, each
/// paired with the latest `bot_runs` status found for it (by container name).
/// `currently_running` is the set of bot_ids Docker reports as RUNNING right
/// now — ground truth, checked BEFORE deciding to respawn anything, so a bot
/// that is actually up (the spawner itself just restarted; the bots never
/// went anywhere) is never touched regardless of what its ledger row says.
///
/// "Open" mirrors `supervisor::run_is_open` exactly (`spawning`/`running`) —
/// the identical signal the crash-supervisor uses to tell a real crash from
/// an intentional stop. A row can only be open here because nothing ever
/// closed it: no `record_stop` (clean API stop), no `record_remove` (prune),
/// no `record_error` (crash already handled). That is precisely what a host
/// reboot — or the spawner process itself dying — looks like from the
/// ledger's point of view.
pub fn decide(configs: &[ConfigRunState], currently_running: &HashSet<String>) -> ReconcilePlan {
    let mut plan = ReconcilePlan::default();

    for c in configs {
        if currently_running.contains(&c.bot_id) {
            plan.already_running.push(c.bot_id.clone());
            continue;
        }

        match c.last_status.as_deref() {
            Some(status) if run_is_open(Some(status)) => {
                plan.respawn.push(ReconcileTarget {
                    config_name: c.config_name.clone(),
                    bot_id: c.bot_id.clone(),
                    last_status: status.to_string(),
                });
            }
            Some("error") => plan.already_errored.push(c.bot_id.clone()),
            Some(_) => plan.clean_stop.push(c.bot_id.clone()),
            None => plan.never_spawned.push(c.bot_id.clone()),
        }
    }

    plan
}

// ─────────────────────────────────────────────────────────────────────────────
// Async wiring — store + Docker + the existing respawn_from_config path
// ─────────────────────────────────────────────────────────────────────────────

/// Run boot-time reconciliation once. Called from `main` right after the
/// Postgres connection attempt, before the HTTP listener starts accepting
/// traffic — so the box is either fully reconciled or has clearly logged why
/// it skipped before it is called "ready". Never panics and never returns an
/// error: every failure mode degrades to a `warn!` and either skips one bot
/// or the whole pass, matching the "keeps trying, never blocks boot" posture
/// the rest of this crate's optional DB-backed startup uses (see
/// `db::BotRunStore::try_connect`).
pub async fn run(state: &AppState) {
    if !state.config.boot_reconcile_enabled {
        info!(
            "boot reconciliation disabled (BOOT_RECONCILE_ENABLED=false) — \
             bots left exactly as spawner startup found them"
        );
        return;
    }

    let Some(store) = state.store.as_ref() else {
        // Same posture as every other DB-only startup step: Postgres was
        // unreachable after BotRunStore::try_connect's bounded retries, or
        // DATABASE_URL is unset. There is no ledger to consult and no saved
        // config to respawn from — degrade to a no-op, not a crash.
        info!(
            "boot reconciliation skipped — spawner DB not connected (stateless boot); \
             no bot_runs ledger to check, so nothing can be safely reconciled"
        );
        return;
    };

    let configs = match store.list_configs().await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "boot reconciliation: list_configs failed — skipping this pass");
            return;
        }
    };

    // Only configs that name a bot_id are respawn-ready at all (mirrors
    // `resolve_respawn_bot_id`'s own requirement). De-duplicate by bot_id:
    // two active configs sharing one bot_id "shouldn't happen"
    // (db::get_config_by_bot_id's doc comment), but a boot-time safety net
    // must not attempt two respawns of the same bot_id if it ever does — keep
    // the last one seen (configs arrive name-ordered) and warn on the
    // collision so the data anomaly itself is visible.
    let mut by_bot_id: HashMap<String, BotConfigRow> = HashMap::new();
    for cfg in configs.into_iter().filter(|c| c.bot_id.is_some()) {
        let bot_id = cfg.bot_id.clone().expect("filtered Some above");
        if let Some(prev) = by_bot_id.insert(bot_id.clone(), cfg) {
            warn!(
                bot_id = %bot_id,
                other_config = %prev.name,
                "boot reconciliation: multiple active configs share this bot_id — \
                 only the later one in listing order is considered"
            );
        }
    }

    if by_bot_id.is_empty() {
        info!("boot reconciliation: no saved configs with a bot_id — nothing to check");
        return;
    }

    // Latest ledger status per bot, keyed by container NAME (not id — the
    // container itself may be gone entirely after a host reboot, so id-based
    // lookup has nothing to find). A per-bot query failure is skipped, not
    // fatal to the whole pass.
    let mut run_states = Vec::with_capacity(by_bot_id.len());
    for cfg in by_bot_id.values() {
        let bot_id = cfg.bot_id.clone().expect("filtered Some above");
        let container_name = format!("fks-bot-{bot_id}");
        match store
            .latest_run_status_by_container_name(&container_name)
            .await
        {
            Ok(last_status) => run_states.push(ConfigRunState {
                config_name: cfg.name.clone(),
                bot_id,
                last_status,
            }),
            Err(e) => warn!(
                error = %e,
                bot_id = %bot_id,
                "boot reconciliation: bot_runs lookup failed — skipping this bot this pass"
            ),
        }
    }

    // Ground truth from Docker BEFORE deciding anything — property 1
    // (idempotent). A bot that is actually running right now (the spawner
    // itself just restarted; the bots never went anywhere) must never be
    // bounced just because its ledger row happens to still read 'running'.
    let currently_running: HashSet<String> = match state.docker.list_bots().await {
        Ok(bots) => bots
            .iter()
            .filter(|b| b.state == "running" && !b.bot_id.is_empty())
            .map(|b| b.bot_id.clone())
            .collect(),
        Err(e) => {
            // Cannot verify current state ⇒ cannot safely decide anything.
            // Never blind-respawn on an assumption about Docker we couldn't
            // check — that risks a duplicate live container far more than it
            // risks leaving a genuinely-down bot down one more tick (the
            // crash-supervisor's next sweep, or the operator, still catches
            // that).
            warn!(
                error = %e,
                "boot reconciliation: Docker list_bots failed — cannot verify current \
                 container state, aborting this pass (nothing respawned)"
            );
            return;
        }
    };

    let plan = decide(&run_states, &currently_running);

    for bot_id in &plan.already_running {
        info!(bot_id = %bot_id, "boot reconciliation: already running — left untouched");
    }
    for bot_id in &plan.clean_stop {
        info!(bot_id = %bot_id, "boot reconciliation: last stop was clean — left down");
    }
    for bot_id in &plan.already_errored {
        info!(
            bot_id = %bot_id,
            "boot reconciliation: last run ended in error (already handled by the \
             crash-supervisor) — not second-guessed here"
        );
    }
    for bot_id in &plan.never_spawned {
        info!(bot_id = %bot_id, "boot reconciliation: saved config never spawned — nothing to do");
    }

    if plan.respawn.is_empty() {
        info!(
            checked = run_states.len(),
            "boot reconciliation: nothing to respawn — every bot is either already running \
             or was cleanly stopped"
        );
        return;
    }

    warn!(
        count = plan.respawn.len(),
        bots = ?plan.respawn.iter().map(|t| t.bot_id.as_str()).collect::<Vec<_>>(),
        "boot reconciliation: FIRING — respawning bots that were running when the spawner \
         last had a view of them and are not running now (host reboot / unclean shutdown \
         recovery, not a clean stop)"
    );

    for target in &plan.respawn {
        let Some(cfg) = by_bot_id.get(&target.bot_id) else {
            // Unreachable in practice (target came from by_bot_id's own keys)
            // but never index-panic on it.
            warn!(bot_id = %target.bot_id, "boot reconciliation: config vanished mid-pass — skipping");
            continue;
        };

        warn!(
            bot_id = %target.bot_id,
            config = %target.config_name,
            last_status = %target.last_status,
            "boot reconciliation: respawning bot — last bot_runs row was never cleanly \
             closed via the API (crash/reboot recovery)"
        );

        match respawn_from_config(state, cfg, target.bot_id.clone()).await {
            Ok((old_container_id, resp)) => {
                metrics::BOOT_RECONCILE_RESPAWNS_TOTAL.inc();
                info!(
                    bot_id = %target.bot_id,
                    config = %target.config_name,
                    old_container_id = ?old_container_id,
                    new_container_id = %resp.container_id,
                    image = %resp.image,
                    "boot reconciliation: respawned successfully"
                );
                spawn_dispatch(
                    state,
                    NotificationEvent::restarted(
                        &target.bot_id,
                        &resp.image,
                        &resp.mode,
                        &format!(
                            "boot-time reconciliation: last known state was '{}' (never \
                             cleanly stopped) and it was not running in Docker at spawner \
                             startup — auto-respawned from saved config '{}'",
                            target.last_status, target.config_name
                        ),
                    ),
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    bot_id = %target.bot_id,
                    config = %target.config_name,
                    "boot reconciliation: respawn FAILED — bot left down, needs manual attention"
                );
                spawn_dispatch(
                    state,
                    NotificationEvent::error(
                        &target.bot_id,
                        &cfg.image,
                        &cfg.mode,
                        &format!("boot reconciliation respawn failed: {e}"),
                    ),
                );
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure decision logic (no Docker, no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn state(config_name: &str, bot_id: &str, last_status: Option<&str>) -> ConfigRunState {
        ConfigRunState {
            config_name: config_name.to_string(),
            bot_id: bot_id.to_string(),
            last_status: last_status.map(str::to_string),
        }
    }

    // ── the tonight's-incident scenario ─────────────────────────────────────

    #[test]
    fn open_row_not_running_in_docker_is_respawned() {
        // Exactly tonight's incident: bot_runs still says 'running' (never
        // cleanly stopped), and the container is entirely gone from Docker
        // (host reboot removed a no-restart-policy container).
        let configs = vec![state("crypto-spot-live", "crypto-spot", Some("running"))];
        let running = HashSet::new();
        let plan = decide(&configs, &running);
        assert_eq!(plan.respawn.len(), 1);
        assert_eq!(plan.respawn[0].bot_id, "crypto-spot");
        assert_eq!(plan.respawn[0].config_name, "crypto-spot-live");
        assert_eq!(plan.respawn[0].last_status, "running");
        assert!(plan.already_running.is_empty());
    }

    #[test]
    fn open_row_with_spawning_status_is_also_respawned() {
        // A bot that died mid-startup (never reached 'running') is just as
        // much a never-cleanly-stopped bot as one that got there.
        let configs = vec![state(
            "crypto-funding-paper",
            "crypto-funding",
            Some("spawning"),
        )];
        let plan = decide(&configs, &HashSet::new());
        assert_eq!(plan.respawn.len(), 1);
        assert_eq!(plan.respawn[0].bot_id, "crypto-funding");
    }

    // ── idempotency: a routine spawner-only redeploy must not bounce a live bot ──

    #[test]
    fn already_running_bot_is_never_respawned_even_with_open_row() {
        // The spawner itself restarted (routine redeploy) while the bots kept
        // running the whole time — Docker ground truth says RUNNING, so this
        // must be left alone regardless of what the ledger row says.
        let configs = vec![state("crypto-spot-live", "crypto-spot", Some("running"))];
        let mut running = HashSet::new();
        running.insert("crypto-spot".to_string());
        let plan = decide(&configs, &running);
        assert!(
            plan.respawn.is_empty(),
            "a currently-running bot must never be touched"
        );
        assert_eq!(plan.already_running, vec!["crypto-spot".to_string()]);
    }

    // ── never resurrect a deliberate decision ───────────────────────────────

    #[test]
    fn clean_stop_is_never_respawned() {
        for clean in ["stopped", "pruned"] {
            let configs = vec![state("cfg", "bot-a", Some(clean))];
            let plan = decide(&configs, &HashSet::new());
            assert!(
                plan.respawn.is_empty(),
                "status {clean} must not be respawned"
            );
            assert_eq!(plan.clean_stop, vec!["bot-a".to_string()]);
        }
    }

    #[test]
    fn already_errored_row_is_not_second_guessed() {
        // The crash-supervisor already detected this crash, closed the row to
        // 'error', and either restarted it (per that config's restart_policy)
        // or deliberately left it down after exhausting the budget. Boot
        // reconciliation does not re-decide that.
        let configs = vec![state("cfg", "bot-a", Some("error"))];
        let plan = decide(&configs, &HashSet::new());
        assert!(plan.respawn.is_empty());
        assert_eq!(plan.already_errored, vec!["bot-a".to_string()]);
    }

    #[test]
    fn never_spawned_config_is_skipped() {
        // A saved config with no bot_runs row at all — nothing ran yet.
        let configs = vec![state("cfg", "bot-a", None)];
        let plan = decide(&configs, &HashSet::new());
        assert!(plan.respawn.is_empty());
        assert_eq!(plan.never_spawned, vec!["bot-a".to_string()]);
    }

    // ── mixed fleet ──────────────────────────────────────────────────────────

    #[test]
    fn mixed_fleet_only_the_open_and_absent_bot_is_respawned() {
        let configs = vec![
            state("crypto-spot-live", "crypto-spot", Some("running")), // gone, was open -> respawn
            state("crypto-funding-paper", "crypto-funding", Some("running")), // still up -> skip
            state("old-demo", "demo-bot", Some("stopped")),            // clean stop -> skip
            state("dead-edge", "edge-x", Some("error")),               // already handled -> skip
            state("unused", "never-run", None),                        // never spawned -> skip
        ];
        let mut running = HashSet::new();
        running.insert("crypto-funding".to_string());

        let plan = decide(&configs, &running);
        assert_eq!(plan.respawn.len(), 1);
        assert_eq!(plan.respawn[0].bot_id, "crypto-spot");
        assert_eq!(plan.already_running, vec!["crypto-funding".to_string()]);
        assert_eq!(plan.clean_stop, vec!["demo-bot".to_string()]);
        assert_eq!(plan.already_errored, vec!["edge-x".to_string()]);
        assert_eq!(plan.never_spawned, vec!["never-run".to_string()]);
    }

    #[test]
    fn empty_configs_is_a_clean_no_op() {
        let plan = decide(&[], &HashSet::new());
        assert_eq!(plan, ReconcilePlan::default());
    }
}
