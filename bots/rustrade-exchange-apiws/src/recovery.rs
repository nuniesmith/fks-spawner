//! Ambiguous-fill recovery for the KuCoin adapter's order-submit path (A.7).
//!
//! A market entry or reduce-only close that fails **ambiguously** — a transport
//! timeout after the request may already have reached the matching engine, or a
//! duplicate-`clientOid` rejection proving a prior attempt landed — must never
//! be blindly assumed filled *or* unfilled: either guess can leave an untracked
//! live position or a double entry. This module turns that ambiguity into a
//! deterministic answer by re-querying the venue with the **retained
//! `clientOid`** ([`KuCoinClient::get_order_by_client_oid`]) and reconciling the
//! true state, with bounded, non-double-submitting retries.
//!
//! The `clientOid` minted per logical submit is the idempotency key: it is
//! reused across every retry and every re-query, so even a re-place after a
//! (possibly wrong) "never reached" verdict is safe — KuCoin de-duplicates the
//! `clientOid` and surfaces a duplicate rejection, which loops back into a
//! resolve that adopts the real order.

use std::time::Duration;

use exchange_apiws::ExchangeError;

/// Bounded retry / backoff policy for order-submit recovery.
///
/// `max_attempts` counts the *total* submit attempts (the first try plus
/// retries), so it can never spin unbounded. Backoff grows exponentially from
/// `base_backoff`, capped at `max_backoff`.
#[derive(Debug, Clone)]
pub struct RecoveryPolicy {
    /// Total submit attempts before surfacing (first attempt + retries). `>= 1`.
    pub max_attempts: u32,
    /// Backoff before the first retry; doubles each subsequent retry.
    pub base_backoff: Duration,
    /// Hard cap on any single backoff sleep.
    pub max_backoff: Duration,
}

impl Default for RecoveryPolicy {
    /// 4 total attempts (1 + 3 retries), 250 ms base backoff, 4 s cap — a few
    /// seconds of bounded recovery, appropriate for a 60-minute-bar bot where a
    /// missed order matters far more than a few seconds of latency.
    fn default() -> Self {
        Self {
            max_attempts: 4,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(4),
        }
    }
}

impl RecoveryPolicy {
    /// Backoff to sleep *after* attempt number `attempt` (1-indexed) before the
    /// next one: `base * 2^(attempt-1)`, capped at `max_backoff`.
    #[must_use]
    pub(crate) fn backoff_after(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let mult = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
        let base_ms = u64::try_from(self.base_backoff.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(base_ms.saturating_mul(mult)).min(self.max_backoff)
    }
}

/// `true` when a `byClientOid` resolve error means the order **never reached the
/// matching engine** (so it is safe to re-place the same `clientOid`), as
/// opposed to a transient query failure.
///
/// KuCoin surfaces an unknown `clientOid` as an [`ExchangeError::Api`] whose
/// code is `100004` and/or whose message says the *order* does not exist. We
/// accept the canonical code and a small set of **order-scoped** phrases — the
/// "already exists" duplicate message (handled separately as *ambiguous*) can
/// never match these, and, critically, an unrelated gateway/routing envelope
/// (`"service not found"`, `"symbol not found"`) must NOT be misread as "the
/// order is absent" → that would wrongly re-place a possibly-landed order. So a
/// bare `"not found"` no longer qualifies; the phrase must name the order.
#[must_use]
pub(crate) fn is_order_not_found(e: &ExchangeError) -> bool {
    match e {
        ExchangeError::Api { code, message } => {
            // KuCoin Futures' canonical "order does not exist" code.
            if code == "100004" {
                return true;
            }
            let m = message.to_ascii_lowercase();
            m.contains("order does not exist")
                || m.contains("order not exist")
                || m.contains("order_not_exist")
                || m.contains("no such order")
                // "The order does not exist." — order-scoped, unlike a bare
                // "not found"/"not exist" that a non-order envelope could carry.
                || m.contains("does not exist")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(code: &str, msg: &str) -> ExchangeError {
        ExchangeError::Api {
            code: code.to_string(),
            message: msg.to_string(),
        }
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        let p = RecoveryPolicy {
            max_attempts: 8,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
        };
        assert_eq!(p.backoff_after(1), Duration::from_millis(100));
        assert_eq!(p.backoff_after(2), Duration::from_millis(200));
        assert_eq!(p.backoff_after(3), Duration::from_millis(400));
        // 800ms would exceed the 500ms cap.
        assert_eq!(p.backoff_after(4), Duration::from_millis(500));
        // Never panics on a large attempt count.
        assert_eq!(p.backoff_after(1000), Duration::from_millis(500));
    }

    #[test]
    fn not_found_matches_kucoin_messages_but_not_duplicate() {
        assert!(is_order_not_found(&api(
            "400100",
            "The order does not exist."
        )));
        assert!(is_order_not_found(&api("100004", "order not exist")));
        assert!(is_order_not_found(&api("400100", "no such order")));
        // Matched by the canonical code even if the message is opaque.
        assert!(is_order_not_found(&api("100004", "")));
        // A duplicate-clientOid rejection (the ambiguous case) must NOT read as
        // not-found — "already exists" contains neither "not exist" nor friends.
        assert!(!is_order_not_found(&api(
            "400100",
            "clientOid already exists"
        )));
        assert!(!is_order_not_found(&api("400100", "clientOid duplicate")));
        // A NON-order-scoped gateway/routing "not found" must NOT masquerade as
        // "the order is absent" — otherwise a live order could be wrongly
        // re-placed. Bare "not found"/"not exist" no longer qualify.
        assert!(!is_order_not_found(&api("500000", "service not found")));
        assert!(!is_order_not_found(&api("404000", "not found")));
        assert!(!is_order_not_found(&api("400100", "symbol not found")));
        // Transport errors are never "not found" (outcome unknown).
        assert!(!is_order_not_found(&ExchangeError::Auth("x".into())));
    }

    #[test]
    fn default_policy_is_bounded() {
        let p = RecoveryPolicy::default();
        assert!(p.max_attempts >= 1);
        assert!(p.max_attempts <= 8, "recovery must stay tightly bounded");
    }
}
