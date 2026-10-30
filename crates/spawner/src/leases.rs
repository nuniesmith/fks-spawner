// =============================================================================
// leases.rs — account leases: one live bot per exchange account
//
// THE INCIDENT THIS EXISTS FOR (2026-10-11 → 10-15): the cutover that folded
// the Kraken regime bot into the spawner never stopped the standalone, so TWO
// bots traded the same Kraken account for four days — both invested the same
// deposit, and nothing in either system noticed, because nothing anywhere
// modelled "one account ⇒ one trader". Container-name uniqueness stops two
// containers with one bot_id; it says nothing about two DIFFERENT bot_ids
// pointed at the same exchange account, which is the failure that costs money.
//
// THE MECHANISM (EDGE_FACTORY_PLAN §6.3): a bot may declare the account it
// trades — `account` on its spawn request / saved config, e.g.
// "kraken:spot:default". Before any container is created for such a bot, the
// spawner must hold that account's row in `account_leases`
// (src/sql/spawner/022_account_leases.sql, fks repo): account is the PRIMARY
// KEY, so an account has at most one holder. Acquiring is a single atomic
// upsert (see `BotRunStore::acquire_account_lease`): it succeeds when the
// account is unleased, when the SAME bot_id already holds it (a respawn
// re-acquiring its own lease is idempotent), or when the previous lease has
// EXPIRED; it is refused while another bot holds a live lease, and the spawn
// fails with `SpawnerError::AccountLeaseHeld` (HTTP 409) plus a
// `fks_spawner_account_lease_refusals_total` increment.
//
// LIVENESS: a lease is only as trustworthy as its heartbeat. The background
// loop in this module refreshes `heartbeat_at`/`expires_at` for every lease
// whose bot_id Docker reports RUNNING — Docker is the ground truth, so a
// lease can never be kept alive for a bot that is not actually up, and a bot
// whose container is gone stops being refreshed and its lease expires on its
// own (`account_lease_ttl_secs`, default 90s) with no manual cleanup. That
// expiry is also the recovery path: if this process dies, a restarted spawner
// can re-acquire within one TTL instead of the account being locked forever.
// Removing a bot's container releases its lease immediately (see api.rs),
// so the normal lifecycle never waits for expiry. A plain STOP does not
// release: `POST /container/{id}/restart` can revive a stopped container
// without passing the spawn path, so the stopped bot keeps its account
// until it is removed or its lease lapses — a stopped bot can never be
// displaced and then restarted on top of its successor.
//
// THE CHOKE POINT: every spawn path that can carry an operator-declared
// account funnels through `api::spawn_bot` — POST /spawn directly, and
// POST /configs/{name}/respawn, supervisor auto-restart, boot reconcile and
// instance start all via `api::respawn_from_config` → `spawn_bot` (instances
// are created through `spawn_bot` too). The lease is acquired there, after
// every cheaper validation, immediately before the Docker create. The one
// path that bypasses `spawn_bot` — edge backtests calling `DockerOps::spawn`
// directly — builds its request server-side and never declares an account,
// so there is nothing for it to lease. `respawn_from_config` ADDITIONALLY
// pre-flights the lease (read-only) before tearing the old container down,
// mirroring its mode-contradiction pre-flight: a guard that fires only after
// the destructive step converts a refusable request into an outage.
//
// HONEST LIMIT: the lease governs spawner-managed bots only. The standalone
// crypto-bot-live container is outside it, so during the fold-in the cutover
// ORDER (standalone stopped first) remains the guard for that one migration;
// from the moment a bot runs under the spawner, FKS itself can never run two
// of them on one account again — UI respawn, botsync races and template
// instances included.
//
// That holds only for bots that DECLARE their account, so a live spawn with
// exchange credentials and no `account` is refused at admission
// (`api::reject_unleased_live_trader`). Until 2026-10-30 it wasn't, and a
// re-spawned saved config ran a second, unleased Kraken trader.
//
// This module owns the pure decision + validation + heartbeat loop; the SQL
// lives with the rest of the store in `crate::db`. NOT the spot order-hold
// system (unresolved-fill holds inside the spot bot, bots/spot-portfolio) —
// a different mechanism at a different layer, untouched by this one.
// =============================================================================

#![cfg(feature = "db")]

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::api::AppState;
use crate::error::SpawnerError;

/// A held account lease, as the spawn path needs it after a successful
/// acquire: which account, and which bot_id holds it. Used to bind the new
/// container id onto the row (so stop/release can target exactly this run)
/// and to roll the lease back if the Docker create then fails.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountLease {
    pub account: String,
    pub bot_id: String,
}

/// The current holder of an account, as far as a decision needs it. Mirrors
/// the columns of `account_leases` the acquire decision reads.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaseState {
    pub bot_id: String,
    pub expires_at: DateTime<Utc>,
}

/// Whether an acquire attempt may proceed. The refusal carries no payload:
/// the caller re-reads the holder for the error message (it needs the row
/// anyway), keeping this decision a pure function of (existing, bot, now).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireDecision {
    /// No live lease stands in the way: take (or refresh) the lease.
    Granted,
    /// Another bot holds a live lease on the account: refuse the spawn.
    HeldByOther,
}

/// The outcome of a store-level acquire attempt
/// (`BotRunStore::acquire_account_lease`): granted, or refused with the
/// live holder named so the refusal can say whose lease stands in the way.
#[derive(Debug, Clone, PartialEq)]
pub enum AcquireOutcome {
    Granted,
    HeldByOther {
        holder_bot_id: String,
        expires_at: DateTime<Utc>,
    },
}

/// The acquire decision, pure — the exact semantics
/// `BotRunStore::acquire_account_lease` implements in one atomic upsert
/// (its `WHERE` clause is this function, clause for clause):
///
///   * no row                                  → Granted
///   * row held by the SAME bot_id             → Granted (idempotent
///     re-acquire: a respawn/restart of the holder never refuses itself,
///     even if its lease has lapsed in the meantime)
///   * row held by another bot but EXPIRED     → Granted (take over; a
///     crashed holder's lease must not lock the account forever)
///   * row held by another bot, still live     → HeldByOther
pub fn decide_acquire(
    existing: Option<&LeaseState>,
    bot_id: &str,
    now: DateTime<Utc>,
) -> AcquireDecision {
    match existing {
        None => AcquireDecision::Granted,
        Some(lease) if lease.bot_id == bot_id => AcquireDecision::Granted,
        Some(lease) if lease.expires_at <= now => AcquireDecision::Granted,
        Some(_) => AcquireDecision::HeldByOther,
    }
}

/// Validate + normalise a declared account string (`kraken:spot:default`).
/// The value becomes a Postgres PRIMARY KEY and a Prometheus label, so it is
/// held to a tight charset: ASCII letters, digits and `: . _ -` (the colon
/// separates venue:market:account segments), 1..=128 chars after trimming.
/// Returns the trimmed value to store.
pub fn validate_account(raw: &str) -> Result<String, SpawnerError> {
    let account = raw.trim();
    if account.is_empty() || account.len() > 128 {
        return Err(SpawnerError::InvalidRequest(format!(
            "account must be 1..=128 characters (got {})",
            account.len()
        )));
    }
    if !account
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '.' | '_' | '-'))
    {
        return Err(SpawnerError::InvalidRequest(
            "account may only contain ASCII letters, digits and ':', '.', '_' or '-' \
             (expected shape: venue:market:account, e.g. kraken:spot:default)"
                .to_string(),
        ));
    }
    Ok(account.to_string())
}

/// The account a request/config actually declares, normalised: `None` (or a
/// blank string) means "no account" — no lease, behaviour exactly as before
/// this feature existed. A present-but-malformed value is an ERROR, never a
/// silent None: a bot that tried to declare an account must not end up
/// running unleased because its declaration was dropped.
pub fn declared_account(raw: Option<&str>) -> Result<Option<String>, SpawnerError> {
    match raw {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => validate_account(s).map(Some),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat loop — keeps live leases live, lets dead ones lapse
// ─────────────────────────────────────────────────────────────────────────────

/// Run the lease heartbeat forever: every `account_lease_heartbeat_secs`,
/// extend the lease of every bot Docker reports RUNNING. Sleep-first
/// cadence, modelled on `crate::supervisor::run`; never panics — every
/// failure degrades to a warn and the next tick retries.
///
/// Only leases whose bot_id is currently running are refreshed (the store
/// call is a single UPDATE keyed on bot_id), so a crashed/exited bot's
/// lease is left to expire, and a lease taken over by another bot in the
/// meantime is never refreshed by its old holder's heartbeat.
pub async fn run(state: AppState) {
    let interval = std::time::Duration::from_secs(state.config.account_lease_heartbeat_secs.max(1));
    loop {
        tokio::time::sleep(interval).await;
        tick(&state).await;
    }
}

/// One heartbeat pass. Split from `run` so the wiring is testable in
/// principle; the decision itself ("a lease is refreshed iff its holder is
/// running") lives in the store's UPDATE.
async fn tick(state: &AppState) {
    let Some(store) = state.store.as_ref() else {
        return; // stateless: no leases exist to heartbeat
    };
    let bots = match state.docker.list_bots().await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "account-lease heartbeat: list_bots failed");
            return;
        }
    };
    let running: Vec<(&String, &String)> = bots
        .iter()
        .filter(|b| b.state == "running" && !b.bot_id.is_empty())
        .map(|b| (&b.bot_id, &b.id_full))
        .collect();
    if running.is_empty() {
        return;
    }
    let (bot_ids, container_ids): (Vec<String>, Vec<String>) = running
        .into_iter()
        .map(|(b, c)| (b.clone(), c.clone()))
        .unzip();
    match store
        .heartbeat_account_leases(
            &bot_ids,
            &container_ids,
            state.config.account_lease_ttl_secs,
        )
        .await
    {
        Ok(0) => {}
        Ok(n) => {
            info!(leases = n, "account-lease heartbeat: refreshed live leases");
        }
        Err(e) => {
            warn!(error = %e, "account-lease heartbeat failed — leases may lapse if this persists");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure decision + validation (no DB, no Docker)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_760_000_000, 0).expect("valid ts")
    }

    fn lease(bot_id: &str, expires_in_secs: i64) -> LeaseState {
        LeaseState {
            bot_id: bot_id.to_string(),
            expires_at: now() + Duration::seconds(expires_in_secs),
        }
    }

    #[test]
    fn unleased_account_is_granted() {
        assert_eq!(
            decide_acquire(None, "crypto-regime", now()),
            AcquireDecision::Granted
        );
    }

    #[test]
    fn live_lease_held_by_another_bot_is_refused() {
        // THE incident scenario: crypto-regime holds kraken:spot:default and
        // a second config tries to spawn onto the same account.
        let held = lease("crypto-regime", 60);
        assert_eq!(
            decide_acquire(Some(&held), "crypto-spot-2", now()),
            AcquireDecision::HeldByOther
        );
    }

    #[test]
    fn expired_lease_is_taken_over() {
        // A crashed holder's lease lapses; the next bot must not be locked
        // out of the account forever.
        let stale = lease("crashed-bot", -1);
        assert_eq!(
            decide_acquire(Some(&stale), "crypto-regime", now()),
            AcquireDecision::Granted
        );
    }

    #[test]
    fn own_lease_reacquires_idempotently() {
        // A respawn/restart of the holder itself — live lease or lapsed.
        let live = lease("crypto-regime", 60);
        assert_eq!(
            decide_acquire(Some(&live), "crypto-regime", now()),
            AcquireDecision::Granted
        );
        let lapsed = lease("crypto-regime", -3600);
        assert_eq!(
            decide_acquire(Some(&lapsed), "crypto-regime", now()),
            AcquireDecision::Granted,
            "a bot re-acquiring its OWN lease is never refused, even after it lapsed"
        );
    }

    #[test]
    fn lease_expiring_exactly_now_is_expired() {
        // Boundary: expires_at == now means the TTL has run out (the SQL
        // uses expires_at <= NOW() the same way).
        let edge = lease("other-bot", 0);
        assert_eq!(
            decide_acquire(Some(&edge), "crypto-regime", now()),
            AcquireDecision::Granted
        );
    }

    #[test]
    fn account_validation_accepts_the_documented_shape() {
        assert_eq!(
            validate_account("kraken:spot:default").expect("valid"),
            "kraken:spot:default"
        );
        assert_eq!(
            validate_account("  kraken  ").expect("trimmed"),
            "kraken",
            "surrounding whitespace is trimmed"
        );
    }

    #[test]
    fn account_validation_rejects_bad_shapes() {
        for bad in ["", "   ", "kraken spot", "kraken/spot", "kraken;DROP"] {
            assert!(
                validate_account(bad).is_err(),
                "account {bad:?} must be rejected"
            );
        }
        assert!(
            validate_account(&"a".repeat(129)).is_err(),
            "an over-long account must be rejected"
        );
    }

    #[test]
    fn declared_account_treats_absent_and_blank_as_none() {
        assert_eq!(declared_account(None).expect("ok"), None);
        assert_eq!(declared_account(Some("")).expect("ok"), None);
        assert_eq!(declared_account(Some("   ")).expect("ok"), None);
        assert_eq!(
            declared_account(Some("kraken:spot:default")).expect("ok"),
            Some("kraken:spot:default".to_string())
        );
        assert!(
            declared_account(Some("kraken spot")).is_err(),
            "a malformed declaration errors instead of silently meaning 'no lease'"
        );
    }
}
