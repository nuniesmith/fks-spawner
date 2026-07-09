//! Discord webhook alerts — fire-and-forget notifications on trade events so a
//! live run can be watched remotely without tailing logs. Enabled by
//! `DIP_ALERT_WEBHOOK=<discord webhook url>`; a no-op when unset.

use std::time::Duration;

use serde_json::json;
use tracing::{debug, warn};

/// Posts messages to a Discord webhook (`{"content": ...}`). Cloneable and cheap
/// to share — the inner `reqwest::Client` is `Arc`-backed.
#[derive(Clone)]
pub struct Alerter {
    webhook: Option<String>,
    client: Option<reqwest::Client>,
}

impl Alerter {
    /// Build from an optional Discord webhook URL. Installs the rustls(ring)
    /// crypto provider (shared with exchange-apiws) so the HTTPS POST works, and
    /// only builds a client when a webhook is actually configured.
    pub fn new(webhook: Option<String>) -> Self {
        let client = webhook.as_ref().map(|_| {
            exchange_apiws::ensure_crypto_provider();
            reqwest::Client::new()
        });
        Self { webhook, client }
    }

    /// Post `msg` to the webhook, fire-and-forget (never blocks the caller, never
    /// fails a trade). Logs a non-2xx or transport error. No-op when unconfigured.
    ///
    /// Must be called from within a Tokio runtime (it spawns the request).
    pub fn notify(&self, msg: impl Into<String>) {
        let (Some(url), Some(client)) = (self.webhook.clone(), self.client.clone()) else {
            return;
        };
        let msg = msg.into();
        tokio::spawn(async move {
            let res = client
                .post(&url)
                .json(&json!({ "content": msg }))
                .timeout(Duration::from_secs(10))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => debug!("alert sent"),
                Ok(r) => warn!(status = %r.status(), "alert: discord returned non-2xx"),
                Err(e) => warn!(error = %e, "alert: discord webhook POST failed"),
            }
        });
    }

    /// Like [`Self::notify`] but awaits the POST — use at shutdown, where a
    /// spawned task might not finish before the runtime stops.
    pub async fn notify_blocking(&self, msg: impl Into<String>) {
        let (Some(url), Some(client)) = (&self.webhook, &self.client) else {
            return;
        };
        let _ = client
            .post(url)
            .json(&json!({ "content": msg.into() }))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
    }
}
