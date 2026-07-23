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
                // A webhook URL's path IS the secret token — reqwest's Display
                // appends the request URL, so strip it before logging.
                Err(e) => {
                    warn!(error = %reqwest::Error::without_url(e), "alert: discord webhook POST failed")
                }
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

#[cfg(test)]
mod tests {
    /// A webhook URL's path IS its secret token, and reqwest's `Display`
    /// appends the request URL to transport errors — so logging the raw error
    /// (`%e`) would leak the token. The `notify`/`notify_blocking` error arms
    /// strip it with `reqwest::Error::without_url` before logging; this test
    /// proves the stripping actually removes the token from the rendered error.
    ///
    /// We force a real transport error (connection refused to a closed local
    /// port — no network egress, deterministic offline) against a URL whose
    /// path carries a fake token, then assert the token is present in the raw
    /// error's `Display` but absent once `without_url` is applied.
    #[tokio::test]
    async fn without_url_strips_webhook_token_from_error_display() {
        const FAKE_TOKEN: &str = "s3cr3t-webhook-token-do-not-log";
        // Port 1 is closed → immediate connection-refused; reqwest attaches the
        // request URL to the resulting transport error.
        let url = format!("http://127.0.0.1:1/api/webhooks/123456/{FAKE_TOKEN}");

        // Building a reqwest Client requires the rustls crypto provider (the
        // production `Alerter::new` installs it the same way).
        exchange_apiws::ensure_crypto_provider();
        let err = reqwest::Client::new()
            .post(&url)
            .send()
            .await
            .expect_err("POST to a closed local port must fail");

        // Precondition: the raw error really does carry the token (otherwise the
        // test would pass vacuously and never guard the leak).
        assert!(
            err.to_string().contains(FAKE_TOKEN),
            "sanity: raw reqwest error should embed the URL/token; got: {err}"
        );

        // The fix: without_url removes the URL, so the token cannot reach a log.
        let stripped = reqwest::Error::without_url(err);
        assert!(
            !stripped.to_string().contains(FAKE_TOKEN),
            "without_url must strip the token from the error Display; got: {stripped}"
        );
    }
}
