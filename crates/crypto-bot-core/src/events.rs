//! Bot→spawner event ingest — the fire-and-forget path a bot uses to raise a
//! platform `risk_halt` (a trade-cap / kill-switch trip — NOT a drawdown halt)
//! through the spawner's `POST /events` mailbox, so it flows through the SAME
//! channel store, filters, and delivery ledger as every other notification
//! instead of a parallel, history-less per-bot Discord webhook.
//!
//! Configured from the environment at construction — BOTH vars are injected by
//! the spawner at spawn time, but only when its `EVENTS_TOKEN` is set:
//!   - `SPAWNER_EVENTS_URL`   — the ingest endpoint (e.g.
//!     `http://fks_bot_spawner:8090/events`, the spawner's container-DNS name on
//!     `fks_network`).
//!   - `SPAWNER_EVENTS_TOKEN` — the SCOPED ingest token, sent as the
//!     `X-Internal-Token` header. The `/events` handler accepts EITHER this
//!     scoped token OR the full internal token through that same header; the
//!     scoped one opens ONLY the events mailbox, so a compromised bot can't
//!     reach `/spawn`, `/secrets`, `/transfers`, ….
//!
//! Either var absent/empty ⇒ a NO-OP client (exactly like
//! [`crate::alerts::Alerter`] with no webhook). Every method returns
//! immediately; the trading hot path is never blocked or failed.

use std::time::Duration;

use serde_json::{Value, json};
use tracing::{debug, warn};

/// Wire event kind for a bot-side risk-guard trip. Matches the spawner's
/// ingest allowlist (`crate` boundary — kept as a literal here so the SDK has no
/// dependency on the spawner crate).
const EVENT_RISK_HALT: &str = "risk_halt";

/// Max length of the `detail` field (matches the spawner's embed/ledger budget;
/// the server also caps it — this keeps the wire payload tight).
const DETAIL_MAX: usize = 512;

/// Fire-and-forget client for the spawner's `POST /events` ingest. Cloneable and
/// cheap to share — the inner `reqwest::Client` is `Arc`-backed.
#[derive(Clone)]
pub struct EventClient {
    url: Option<String>,
    token: Option<String>,
    client: Option<reqwest::Client>,
}

impl EventClient {
    /// Build from `SPAWNER_EVENTS_URL` + `SPAWNER_EVENTS_TOKEN` in the
    /// environment. When EITHER is absent or blank this is a no-op client — the
    /// bot runs completely unchanged when the operator hasn't set `EVENTS_TOKEN`
    /// on the spawner (so nothing is injected).
    pub fn from_env() -> Self {
        let url = non_empty(std::env::var("SPAWNER_EVENTS_URL").ok());
        let token = non_empty(std::env::var("SPAWNER_EVENTS_TOKEN").ok());
        Self::new(url, token)
    }

    /// Build from explicit values (used by [`Self::from_env`] and tests). A live
    /// HTTP client is constructed ONLY when both a url and a token are present;
    /// otherwise the client is a no-op. Installs the rustls(ring) crypto
    /// provider (shared with exchange-apiws / [`crate::alerts::Alerter`]) so an
    /// `https://` ingest URL works too.
    pub fn new(url: Option<String>, token: Option<String>) -> Self {
        let client = match (&url, &token) {
            (Some(_), Some(_)) => {
                exchange_apiws::ensure_crypto_provider();
                Some(reqwest::Client::new())
            }
            _ => None,
        };
        Self { url, token, client }
    }

    /// Whether this client will actually POST (both env vars were present). A
    /// no-op client reports `false`.
    pub fn is_active(&self) -> bool {
        self.client.is_some()
    }

    /// Raise a `risk_halt` event: a bot-side risk guard (trade-cap / kill-switch
    /// — NOT a drawdown halt) tripped. Fire-and-forget — spawns the POST and
    /// returns immediately, NEVER blocking or failing the caller's trade path
    /// (same contract as [`crate::alerts::Alerter::notify`]). `detail` is
    /// truncated to 512 chars. No-op when unconfigured.
    ///
    /// Must be called from within a Tokio runtime (it spawns the request).
    pub fn fire_risk_halt(
        &self,
        bot_id: impl Into<String>,
        mode: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let (Some(url), Some(token), Some(client)) =
            (self.url.clone(), self.token.clone(), self.client.clone())
        else {
            return;
        };
        let body = event_payload(
            EVENT_RISK_HALT,
            &bot_id.into(),
            &mode.into(),
            &detail.into(),
        );
        tokio::spawn(async move {
            let res = client
                .post(&url)
                .header("X-Internal-Token", token)
                .json(&body)
                .timeout(Duration::from_secs(5))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => debug!("risk_halt event ingested"),
                Ok(r) => warn!(status = %r.status(), "event ingest returned non-2xx"),
                // The token rides an `X-Internal-Token` header, not the URL, so
                // the ingest URL isn't itself a secret — but strip it anyway so
                // reqwest's Display can never surface a request URL into logs
                // (uniform with the webhook alert paths).
                Err(e) => {
                    warn!(error = %reqwest::Error::without_url(e), "event ingest POST failed")
                }
            }
        });
    }
}

/// Build the `POST /events` JSON body: `{event, bot_id, mode, detail}`. Pure
/// (the only transform is truncating `detail` to `DETAIL_MAX` chars), so the
/// wire shape + cap are unit-tested without a network.
fn event_payload(event: &str, bot_id: &str, mode: &str, detail: &str) -> Value {
    json!({
        "event": event,
        "bot_id": bot_id,
        "mode": mode,
        "detail": truncate_chars(detail, DETAIL_MAX),
    })
}

/// `Some(v)` only when `v` is non-blank after trimming; `None` otherwise.
fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.trim().is_empty())
}

/// Truncate to at most `max` CHARS (not bytes) so a multi-byte boundary is never
/// split.
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() > max {
        s.chars().take(max).collect()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconfigured_client_is_a_noop() {
        // Missing either env var ⇒ inactive (no client). fire_* must not panic
        // even outside a Tokio runtime, because it returns before spawning.
        assert!(!EventClient::new(None, None).is_active());
        assert!(!EventClient::new(Some("http://x/events".into()), None).is_active());
        assert!(!EventClient::new(None, Some("tok".into())).is_active());

        let c = EventClient::new(None, None);
        c.fire_risk_halt("bot-1", "paper", "trade-cap tripped"); // no panic, no runtime
    }

    #[test]
    fn both_present_makes_an_active_client() {
        let c = EventClient::new(
            Some("http://fks_bot_spawner:8090/events".into()),
            Some("scoped-token".into()),
        );
        assert!(c.is_active());
    }

    #[test]
    fn blank_env_values_are_treated_as_absent() {
        assert_eq!(non_empty(Some("   ".into())), None);
        assert_eq!(non_empty(Some(String::new())), None);
        assert_eq!(non_empty(None), None);
        assert_eq!(non_empty(Some("v".into())), Some("v".into()));
    }

    #[test]
    fn payload_has_the_ingest_shape() {
        let p = event_payload(
            "risk_halt",
            "spot-portfolio",
            "live",
            "trade-cap: $900 > $500",
        );
        assert_eq!(p["event"], "risk_halt");
        assert_eq!(p["bot_id"], "spot-portfolio");
        assert_eq!(p["mode"], "live");
        assert_eq!(p["detail"], "trade-cap: $900 > $500");
        // Exactly the four keys the spawner's EventIngestRequest reads.
        let obj = p.as_object().unwrap();
        assert_eq!(obj.len(), 4);
    }

    #[test]
    fn detail_is_capped_at_512_chars() {
        let long = "x".repeat(1000);
        let p = event_payload("risk_halt", "b", "paper", &long);
        assert_eq!(p["detail"].as_str().unwrap().chars().count(), DETAIL_MAX);
    }

    #[test]
    fn detail_cap_never_splits_a_multibyte_char() {
        // 600 'é' (2 bytes each) — truncation must be on a char boundary.
        let s = "é".repeat(600);
        let out = truncate_chars(&s, DETAIL_MAX);
        assert_eq!(out.chars().count(), DETAIL_MAX);
        assert!(out.chars().all(|c| c == 'é')); // valid UTF-8, no split
    }
}
