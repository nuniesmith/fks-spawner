// =============================================================================
// notifications.rs — the notification SENDER (consumer half)
//
// PR #179 added the STORE + management API for notification channels (Discord
// webhooks, URL encrypted at rest). This module is the consumer half: given a
// bot-lifecycle event, it loads the matching channels, decrypts each target
// URL via the existing `SecretsCipher` (through `BotRunStore`), formats a
// compact Discord embed, and POSTs it to every channel.
//
// DESIGN CONTRACT
//   - BEST-EFFORT. A webhook failure/timeout is logged + counted, NEVER
//     propagated. Dispatch must never block or fail a bot lifecycle operation
//     — callers fire it via `tokio::spawn` off the critical path.
//   - Per-POST timeout of 5s; POSTs fire concurrently across channels.
//   - The webhook URL is a bearer capability: it is NEVER logged. Only the
//     channel NAME appears in logs.
//   - Events filter: a channel with `events == []` is CATCH-ALL (receives
//     everything); a non-empty list only receives matching event kinds.
//
// The event-kind constants, the `NotificationEvent` type, the payload builder
// and the events-filter predicate are always compiled (and unit-tested). The
// `NotificationDispatcher` itself needs the channel store + an HTTP client, so
// it is gated behind the `db` feature alongside the rest of the persistence.
// =============================================================================

use chrono::{DateTime, Utc};

use crate::models::SpawnResponse;

// ─────────────────────────────────────────────────────────────────────────────
// Event kinds — the public notification vocabulary
//
// A channel's `events` filter matches against these exact strings. They are the
// stable wire contract shared with the WebUI's channel-configuration form.
// ─────────────────────────────────────────────────────────────────────────────

/// A bot container was created and started successfully.
pub const EVENT_BOT_SPAWNED: &str = "bot_spawned";
/// A bot container was gracefully stopped.
pub const EVENT_BOT_STOPPED: &str = "bot_stopped";
/// A bot container was removed (explicit delete or auto-prune).
pub const EVENT_BOT_REMOVED: &str = "bot_removed";
/// A bot lifecycle operation failed (e.g. a spawn that never started).
pub const EVENT_BOT_ERROR: &str = "bot_error";
/// A RUNNING bot exited unexpectedly (crash) — the supervisor detected an
/// exited container whose `bot_runs` row was never closed via the API. Distinct
/// from `bot_error` (a failed spawn) so a live-money crash can be routed/paged
/// on its own, and page-worthy: this is the 3am-panic signal.
pub const EVENT_BOT_CRASHED: &str = "bot_crashed";
/// A bounded auto-restart of a crashed bot SUCCEEDED — the resolution of the
/// always-delivered crash page, so it is itself always delivered (a channel
/// that saw the crash must see the recovery).
pub const EVENT_BOT_RESTARTED: &str = "bot_restarted";
/// A bot entered `mode=live` — real money is now moving. Emitted on EVERY live
/// spawn (covers `/spawn`, respawn, and supervisor restarts, which all funnel
/// through the spawn path). Page-worthy → always delivered.
pub const EVENT_LIVE_FLIP: &str = "live_flip";
/// Exchange API credentials were stored/overwritten via `/secrets`. Carries the
/// exchange NAME only — NEVER key material — plus the operational reminder that
/// running bots need a respawn to pick up rotated keys.
pub const EVENT_KEY_ROTATION: &str = "key_rotation";
/// Total net worth crossed a configured `NET_WORTH_MILESTONE_STEP` boundary
/// (either direction). OFF by default (step 0).
pub const EVENT_NET_WORTH_MILESTONE: &str = "net_worth_milestone";
/// A bot-side risk guard tripped (trade-cap / kill-switch — NOT a drawdown
/// halt). Arrives via the generic `POST /events` ingest. Page-worthy on a live
/// bot by the same logic as a crash → always delivered.
pub const EVENT_RISK_HALT: &str = "risk_halt";
/// The advisor detected edge drift on a weekly re-backtest. Arrives via the
/// generic `POST /events` ingest (the drift comparison lives in fks-state).
pub const EVENT_EDGE_DECAY: &str = "edge_decay";

/// All known event kinds, in emission-priority order. Handy for docs/tests.
/// This is a FROZEN wire contract shared with the WebUI channel filters — the
/// exact snake_case strings a stored `events` allowlist matches against.
pub const ALL_EVENT_KINDS: &[&str] = &[
    EVENT_BOT_SPAWNED,
    EVENT_BOT_STOPPED,
    EVENT_BOT_REMOVED,
    EVENT_BOT_ERROR,
    EVENT_BOT_CRASHED,
    EVENT_BOT_RESTARTED,
    EVENT_LIVE_FLIP,
    EVENT_KEY_ROTATION,
    EVENT_NET_WORTH_MILESTONE,
    EVENT_RISK_HALT,
    EVENT_EDGE_DECAY,
];

/// Discord caps embed field values at 1024 chars and titles at 256; we stay
/// well under with a conservative field cap.
const MAX_FIELD_VALUE_LEN: usize = 512;

// ─────────────────────────────────────────────────────────────────────────────
// NotificationEvent — the thing the dispatcher renders + sends
// ─────────────────────────────────────────────────────────────────────────────

/// One bot-lifecycle event to notify about. Deliberately owns its strings so it
/// can be moved into a detached `tokio::spawn` future off the request path.
#[derive(Debug, Clone)]
pub struct NotificationEvent {
    /// One of the `EVENT_*` kinds — matched against each channel's filter.
    pub event: String,
    /// Bot identifier. For stop/remove/error paths where only the container id
    /// is known, this carries the container id (still the useful handle).
    pub bot_id: String,
    /// Docker image, when known (empty for stop/remove where we only hold an id).
    pub image: String,
    /// Execution mode label, when known.
    pub mode: String,
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Optional extra context (e.g. an error message, or a prune summary).
    pub detail: Option<String>,
}

impl NotificationEvent {
    /// `bot_spawned` — full context is available from the spawn response.
    pub fn spawned(resp: &SpawnResponse) -> Self {
        Self {
            event: EVENT_BOT_SPAWNED.to_string(),
            bot_id: resp.bot_id.clone(),
            image: resp.image.clone(),
            mode: resp.mode.clone(),
            timestamp: Utc::now(),
            detail: None,
        }
    }

    /// `bot_stopped` — the stop handler only holds the container id.
    pub fn stopped(container_id: &str) -> Self {
        Self::from_container_id(EVENT_BOT_STOPPED, container_id, None)
    }

    /// `bot_removed` — explicit delete; the handler only holds the container id.
    pub fn removed(container_id: &str) -> Self {
        Self::from_container_id(EVENT_BOT_REMOVED, container_id, None)
    }

    /// `bot_removed` — emitted by the auto-prune sweep. Carries a count summary
    /// rather than a single container id.
    pub fn pruned(count: usize) -> Self {
        Self {
            event: EVENT_BOT_REMOVED.to_string(),
            bot_id: "auto-prune".to_string(),
            image: String::new(),
            mode: String::new(),
            timestamp: Utc::now(),
            detail: Some(format!("auto-pruned {count} stopped container(s)")),
        }
    }

    /// `bot_error` — a failed lifecycle operation. `bot_id` is a best-effort
    /// hint (may be empty when the spawn never got an id), `image` the image
    /// that was requested.
    pub fn error(bot_id: &str, image: &str, mode: &str, message: &str) -> Self {
        Self {
            event: EVENT_BOT_ERROR.to_string(),
            bot_id: bot_id.to_string(),
            image: image.to_string(),
            mode: mode.to_string(),
            timestamp: Utc::now(),
            detail: Some(message.to_string()),
        }
    }

    /// `bot_crashed` — a running bot exited unexpectedly. Full context is known
    /// (the supervisor holds the container's labels), so bot_id/image/mode are
    /// populated and `detail` carries the exit code.
    pub fn crashed(bot_id: &str, image: &str, mode: &str, detail: &str) -> Self {
        Self {
            event: EVENT_BOT_CRASHED.to_string(),
            bot_id: bot_id.to_string(),
            image: image.to_string(),
            mode: mode.to_string(),
            timestamp: Utc::now(),
            detail: Some(detail.to_string()),
        }
    }

    /// `bot_restarted` — a bounded auto-restart succeeded. `detail` carries the
    /// attempt count + the new container id (never a webhook URL).
    pub fn restarted(bot_id: &str, image: &str, mode: &str, detail: &str) -> Self {
        Self {
            event: EVENT_BOT_RESTARTED.to_string(),
            bot_id: bot_id.to_string(),
            image: image.to_string(),
            mode: mode.to_string(),
            timestamp: Utc::now(),
            detail: Some(detail.to_string()),
        }
    }

    /// `live_flip` — a bot entered live mode. Built from the spawn response so
    /// bot_id/image/mode are populated; emitted IN ADDITION to `bot_spawned`.
    pub fn live_flip(resp: &SpawnResponse) -> Self {
        Self {
            event: EVENT_LIVE_FLIP.to_string(),
            bot_id: resp.bot_id.clone(),
            image: resp.image.clone(),
            mode: resp.mode.clone(),
            timestamp: Utc::now(),
            detail: Some("live mode — real money now moving".to_string()),
        }
    }

    /// `key_rotation` — exchange credentials were stored. Carries the exchange
    /// name ONLY (no key material, by construction — this struct has no field
    /// for it) plus the respawn reminder that IS the point of the notification.
    pub fn key_rotation(exchange: &str) -> Self {
        Self {
            event: EVENT_KEY_ROTATION.to_string(),
            bot_id: exchange.to_string(),
            image: String::new(),
            mode: String::new(),
            timestamp: Utc::now(),
            detail: Some(format!(
                "{exchange} keys stored — running bots using them need a respawn to pick up the change"
            )),
        }
    }

    /// `net_worth_milestone` — total net worth crossed `milestone` (`up` = green
    /// gain, `down` = amber drawdown). The direction is carried in `detail` (and
    /// drives the payload stripe colour).
    pub fn net_worth_milestone(total: f64, milestone: f64, up: bool) -> Self {
        let (arrow, dir) = if up { ("▲", "up") } else { ("▼", "down") };
        Self {
            event: EVENT_NET_WORTH_MILESTONE.to_string(),
            bot_id: "treasury".to_string(),
            image: String::new(),
            mode: String::new(),
            timestamp: Utc::now(),
            detail: Some(format!(
                "{arrow} total net worth crossed ${milestone:.0} {dir} (now ${total:.2})"
            )),
        }
    }

    /// Generic constructor for an event arriving through `POST /events`. The
    /// caller has already validated `event` against the ingest allowlist; this
    /// only shapes the struct (image is unknown for an externally-sourced event).
    pub fn from_ingest(event: &str, bot_id: &str, mode: &str, detail: &str) -> Self {
        Self {
            event: event.to_string(),
            bot_id: bot_id.to_string(),
            image: String::new(),
            mode: mode.to_string(),
            timestamp: Utc::now(),
            detail: (!detail.is_empty()).then(|| detail.to_string()),
        }
    }

    fn from_container_id(event: &str, container_id: &str, detail: Option<String>) -> Self {
        Self {
            event: event.to_string(),
            bot_id: container_id.to_string(),
            image: String::new(),
            mode: String::new(),
            timestamp: Utc::now(),
            detail,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Events-filter matching
// ─────────────────────────────────────────────────────────────────────────────

/// Page-worthy event kinds that MUST reach every channel regardless of its
/// `events` filter. `bot_crashed` is a brand-new kind; a channel configured
/// (before this kind existed) with an explicit allowlist like
/// `["bot_error","bot_stopped"]` would otherwise SILENTLY drop the 3am
/// live-money crash page. A page you cannot afford to lose to a stale filter is
/// always delivered.
pub const ALWAYS_DELIVERED_KINDS: &[&str] = &[
    EVENT_BOT_CRASHED,
    // The resolution of the always-delivered crash page — a channel that was
    // paged on the crash must also see the self-heal (OD-3).
    EVENT_BOT_RESTARTED,
    // Real money starting to move is the same page-worthiness class as a crash
    // and must not be droppable by a stale scoped filter (OD-4).
    EVENT_LIVE_FLIP,
    // A tripped risk guard on a live bot is page-worthy by the crash logic (D1).
    EVENT_RISK_HALT,
];

/// Whether a channel whose subscription filter is `events` should receive an
/// event of kind `event`.
///
/// A page-worthy kind (`ALWAYS_DELIVERED_KINDS`) is delivered unconditionally.
/// Otherwise: an EMPTY filter is the catch-all — it receives every event; a
/// non-empty filter only matches when it contains the event kind verbatim.
pub fn channel_wants(events: &[String], event: &str) -> bool {
    ALWAYS_DELIVERED_KINDS.contains(&event)
        || events.is_empty()
        || events.iter().any(|e| e == event)
}

// ─────────────────────────────────────────────────────────────────────────────
// Discord payload builder
// ─────────────────────────────────────────────────────────────────────────────

/// Build the Discord webhook JSON body for an event: a single compact embed
/// with a coloured stripe and a handful of inline fields (bot_id, image, mode,
/// event) plus an ISO-8601 timestamp. Empty field values are rendered as "—"
/// because Discord rejects empty embed-field values.
pub fn discord_payload(ev: &NotificationEvent) -> serde_json::Value {
    // Discord accepts an integer `color`; pick a stripe per event kind.
    let color: u32 = match ev.event.as_str() {
        EVENT_BOT_SPAWNED => 0x2E_CC71,   // green
        EVENT_BOT_STOPPED => 0xE6_7E22,   // orange
        EVENT_BOT_REMOVED => 0x95_A5A6,   // grey
        EVENT_BOT_ERROR => 0xE7_4C3C,     // red
        EVENT_BOT_CRASHED => 0xE7_4C3C,   // red (page-worthy)
        EVENT_BOT_RESTARTED => 0x2E_CC71, // green (recovery)
        EVENT_LIVE_FLIP => 0xC0_392B,     // deep red (real money now moving)
        EVENT_KEY_ROTATION => 0x9B_59B6,  // purple (credentials)
        // Green on a gain, amber on a drawdown — direction rides in `detail`.
        EVENT_NET_WORTH_MILESTONE => {
            if ev.detail.as_deref().is_some_and(|d| d.contains('▼')) {
                0xF3_9C12 // amber (down)
            } else {
                0x2E_CC71 // green (up)
            }
        }
        EVENT_RISK_HALT => 0xE7_4C3C,  // red (page-worthy)
        EVENT_EDGE_DECAY => 0xF3_9C12, // amber (drift)
        _ => 0x34_98DB,                // blue (unknown)
    };

    let mut fields = vec![
        field("bot_id", &ev.bot_id, true),
        field("image", &ev.image, true),
        field("mode", &ev.mode, true),
        field("event", &ev.event, true),
    ];
    if let Some(detail) = &ev.detail {
        fields.push(field("detail", detail, false));
    }

    serde_json::json!({
        "username": "FKS Spawner",
        "embeds": [{
            "title": format!("FKS · {}", ev.event),
            "color": color,
            "fields": fields,
            "timestamp": ev.timestamp.to_rfc3339(),
        }],
    })
}

/// One embed field; blank values become "—" (Discord rejects empty values) and
/// long values are truncated to Discord's field-value budget.
fn field(name: &str, value: &str, inline: bool) -> serde_json::Value {
    let v = value.trim();
    let v = if v.is_empty() {
        "—".to_string()
    } else if v.len() > MAX_FIELD_VALUE_LEN {
        format!("{}…", &v[..MAX_FIELD_VALUE_LEN])
    } else {
        v.to_string()
    };
    serde_json::json!({ "name": name, "value": v, "inline": inline })
}

/// Max length of a delivery-ledger `detail` snippet. Matches the DB column's
/// truncation contract (`013_notification_log.sql`: detail ≤512, NEVER the URL).
pub const MAX_LEDGER_DETAIL_LEN: usize = 512;

/// The `detail` snippet persisted to the delivery ledger for an event: the
/// event's own `detail`, truncated to [`MAX_LEDGER_DETAIL_LEN`]. NEVER contains
/// the webhook URL (this only ever reads `NotificationEvent.detail`, which is
/// operator-facing context, never a target). Pure + always-compiled so it is
/// unit-testable without the `db` feature.
pub fn ledger_detail(ev: &NotificationEvent) -> String {
    match &ev.detail {
        Some(d) => d.chars().take(MAX_LEDGER_DETAIL_LEN).collect(),
        None => String::new(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The dispatcher — needs the channel store + an HTTP client (db feature)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod dispatcher {
    use std::time::Duration;

    use chrono::Utc;
    use tracing::{debug, warn};

    use super::{NotificationEvent, channel_wants, discord_payload, ledger_detail};
    use crate::db::{BotRunStore, NotificationLogRow};
    use crate::metrics;

    /// Per-POST timeout. Kept short so a hung webhook can never pile up.
    const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

    /// Transport recorded in the delivery ledger. Only Discord webhooks reach
    /// the dispatcher today (dispatch skips other channel kinds).
    const LEDGER_TRANSPORT: &str = "discord_webhook";

    /// Synthetic `event` value ledgered for a one-off `/test` probe — not a wire
    /// kind. The history view tells probes apart by the `test_sent`/`test_failed`
    /// outcome; this keeps the `event` column non-empty and self-describing.
    const TEST_EVENT: &str = "test";

    /// Outcome of a one-off test send (POST /notifications/{name}/test).
    #[derive(Debug)]
    pub enum TestOutcome {
        /// The webhook accepted the message (2xx).
        Delivered,
        /// No channel is stored under that name.
        NotFound,
        /// The webhook responded, but with a non-2xx status.
        HttpStatus(u16),
        /// The request never completed (DNS/connect/timeout) or the URL could
        /// not be decrypted. Carries a short, URL-free reason.
        Failed(String),
    }

    /// Loads channels, decrypts webhook URLs, and POSTs Discord payloads.
    /// Cheap to construct (clones the `BotRunStore` pool handle + builds a
    /// reqwest client), so callers build one per dispatch.
    pub struct NotificationDispatcher {
        store: BotRunStore,
        client: reqwest::Client,
    }

    impl NotificationDispatcher {
        pub fn new(store: BotRunStore) -> Self {
            // A per-request timeout guards each POST; `build()` only fails on a
            // TLS backend init error, in which case fall back to the default
            // client (still functional, just without our timeout preset).
            let client = reqwest::Client::builder()
                .timeout(WEBHOOK_TIMEOUT)
                .user_agent(concat!("fks-spawner/", env!("CARGO_PKG_VERSION")))
                .build()
                .unwrap_or_default();
            Self { store, client }
        }

        /// Dispatch `ev` to every channel whose filter matches. BEST-EFFORT:
        /// loads channels, fans out the POSTs concurrently, and swallows all
        /// failures (logged + counted). Never returns an error.
        pub async fn dispatch(&self, ev: NotificationEvent) {
            let channels = match self.store.list_channels().await {
                Ok(c) => c,
                Err(e) => {
                    // No channel names to leak here; the error is DB-side.
                    warn!(error = %e, event = %ev.event, "notify: failed to load channels");
                    return;
                }
            };

            let payload = discord_payload(&ev);
            // Ledger context carried into each detached write — event kind, the
            // bot handle, and a truncated detail snippet. NEVER the webhook URL.
            let event = ev.event.clone();
            let bot_id = ev.bot_id.clone();
            let detail = ledger_detail(&ev);
            let mut sends = Vec::new();
            for ch in channels {
                // Only Discord webhooks are wired today; skip unknown kinds
                // rather than guessing a transport.
                if ch.kind != "discord_webhook" {
                    continue;
                }
                if !channel_wants(&ch.events, &ev.event) {
                    continue;
                }
                sends.push(self.deliver(
                    ch.name,
                    payload.clone(),
                    event.clone(),
                    bot_id.clone(),
                    detail.clone(),
                ));
            }

            if sends.is_empty() {
                return;
            }
            // Fire concurrently; each future is self-contained + best-effort.
            futures_util::future::join_all(sends).await;
        }

        /// Fire a DETACHED, best-effort delivery-ledger write. NEVER awaited on
        /// the send path — a ledger failure (or slow DB) must not delay or fail
        /// a webhook delivery, exactly like the module's best-effort contract.
        /// `detail` is already a URL-free, truncated snippet (see `ledger_detail`).
        fn spawn_ledger(
            &self,
            channel_name: String,
            event: String,
            bot_id: String,
            outcome: &'static str,
            status_code: Option<i16>,
            detail: String,
        ) {
            let store = self.store.clone();
            let row = NotificationLogRow {
                ts: Utc::now(),
                event,
                bot_id,
                channel_name,
                kind: LEDGER_TRANSPORT.to_string(),
                outcome: outcome.to_string(),
                status_code,
                detail,
            };
            tokio::spawn(async move {
                if let Err(e) = store.record_notification(&row).await {
                    // No URL to leak — the row + error are both channel/DB-side.
                    warn!(error = %e, "notify: delivery-ledger write failed");
                }
            });
        }

        /// Decrypt one channel's URL and POST the payload. Logs the channel
        /// NAME only — never the URL. After each terminal outcome it fires a
        /// DETACHED ledger row (best-effort, off the send path).
        async fn deliver(
            &self,
            name: String,
            payload: serde_json::Value,
            event: String,
            bot_id: String,
            detail: String,
        ) {
            let url = match self.store.get_channel_target(&name).await {
                Ok(Some(u)) => u,
                Ok(None) => {
                    // A race, not a send attempt — no ledger row (the SQL
                    // outcome set has no "vanished" arm).
                    warn!(channel = %name, "notify: channel vanished before send");
                    return;
                }
                Err(e) => {
                    warn!(channel = %name, error = %e, "notify: channel URL decrypt failed");
                    metrics::NOTIFY_FAILED_TOTAL.inc();
                    self.spawn_ledger(name, event, bot_id, "decrypt_failed", None, detail);
                    return;
                }
            };

            match self.client.post(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let code = resp.status().as_u16() as i16;
                    metrics::NOTIFY_SENT_TOTAL.inc();
                    debug!(channel = %name, status = %resp.status(), "notify: delivered");
                    self.spawn_ledger(name, event, bot_id, "sent", Some(code), detail);
                }
                Ok(resp) => {
                    let code = resp.status().as_u16() as i16;
                    metrics::NOTIFY_FAILED_TOTAL.inc();
                    warn!(channel = %name, status = %resp.status(), "notify: webhook non-2xx");
                    self.spawn_ledger(name, event, bot_id, "http_error", Some(code), detail);
                }
                Err(e) => {
                    metrics::NOTIFY_FAILED_TOTAL.inc();
                    // A webhook URL's path IS the secret token, and reqwest's
                    // Display appends the request URL — strip it with
                    // `without_url` so only the channel NAME reaches the log.
                    warn!(channel = %name, error = %reqwest::Error::without_url(e), "notify: webhook POST failed");
                    self.spawn_ledger(name, event, bot_id, "send_failed", None, detail);
                }
            }
        }

        /// Send a one-off "connected" message to a single channel by name and
        /// report the outcome. Used by POST /notifications/{name}/test to
        /// verify an operator's webhook actually works.
        pub async fn send_test(&self, name: &str) -> TestOutcome {
            let url = match self.store.get_channel_target(name).await {
                Ok(Some(u)) => u,
                Ok(None) => return TestOutcome::NotFound,
                Err(e) => {
                    warn!(channel = %name, error = %e, "notify test: URL decrypt failed");
                    self.spawn_ledger(
                        name.to_string(),
                        TEST_EVENT.to_string(),
                        String::new(),
                        "test_failed",
                        None,
                        "connectivity probe — URL decrypt failed".to_string(),
                    );
                    return TestOutcome::Failed("channel URL could not be decrypted".to_string());
                }
            };

            let payload = serde_json::json!({
                "username": "FKS Spawner",
                "content": "FKS notifications connected ✅",
            });

            match self.client.post(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let code = resp.status().as_u16();
                    metrics::NOTIFY_SENT_TOTAL.inc();
                    debug!(channel = %name, "notify test: delivered");
                    self.spawn_ledger(
                        name.to_string(),
                        TEST_EVENT.to_string(),
                        String::new(),
                        "test_sent",
                        Some(code as i16),
                        "connectivity probe".to_string(),
                    );
                    TestOutcome::Delivered
                }
                Ok(resp) => {
                    let code = resp.status().as_u16();
                    metrics::NOTIFY_FAILED_TOTAL.inc();
                    warn!(channel = %name, status = %resp.status(), "notify test: non-2xx");
                    self.spawn_ledger(
                        name.to_string(),
                        TEST_EVENT.to_string(),
                        String::new(),
                        "test_failed",
                        Some(code as i16),
                        "connectivity probe — webhook returned non-2xx".to_string(),
                    );
                    TestOutcome::HttpStatus(code)
                }
                Err(e) => {
                    metrics::NOTIFY_FAILED_TOTAL.inc();
                    // The webhook URL's path IS the secret token — strip it via
                    // `without_url` before logging (see `deliver`).
                    warn!(channel = %name, error = %reqwest::Error::without_url(e), "notify test: POST failed");
                    self.spawn_ledger(
                        name.to_string(),
                        TEST_EVENT.to_string(),
                        String::new(),
                        "test_failed",
                        None,
                        "connectivity probe — webhook request failed".to_string(),
                    );
                    TestOutcome::Failed("webhook request failed".to_string())
                }
            }
        }
    }
}

#[cfg(feature = "db")]
pub use dispatcher::{NotificationDispatcher, TestOutcome};

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: &str) -> NotificationEvent {
        NotificationEvent {
            event: kind.to_string(),
            bot_id: "eth-scalper".to_string(),
            image: "fks-bot-eth:v1".to_string(),
            mode: "paper".to_string(),
            timestamp: DateTime::parse_from_rfc3339("2026-07-07T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            detail: None,
        }
    }

    // ── events filter ──────────────────────────────────────────────────────

    #[test]
    fn empty_filter_is_catch_all() {
        let filter: Vec<String> = vec![];
        for kind in ALL_EVENT_KINDS {
            assert!(
                channel_wants(&filter, kind),
                "empty filter must receive {kind}"
            );
        }
    }

    #[test]
    fn specific_filter_matches_only_listed_kinds() {
        let filter = vec![EVENT_BOT_SPAWNED.to_string(), EVENT_BOT_ERROR.to_string()];
        assert!(channel_wants(&filter, EVENT_BOT_SPAWNED));
        assert!(channel_wants(&filter, EVENT_BOT_ERROR));
        assert!(!channel_wants(&filter, EVENT_BOT_STOPPED));
        assert!(!channel_wants(&filter, EVENT_BOT_REMOVED));
    }

    #[test]
    fn specific_filter_rejects_unknown_kind() {
        let filter = vec![EVENT_BOT_SPAWNED.to_string()];
        assert!(!channel_wants(&filter, "totally_made_up"));
    }

    #[test]
    fn page_worthy_kind_bypasses_explicit_filter() {
        // Finding-4 regression: a pre-existing channel with an explicit allowlist
        // that predates `bot_crashed` must STILL receive the crash page — a
        // page-worthy kind is never silently dropped by a stale filter.
        let filter = vec![EVENT_BOT_ERROR.to_string(), EVENT_BOT_STOPPED.to_string()];
        assert!(!filter.iter().any(|e| e == EVENT_BOT_CRASHED));
        assert!(
            channel_wants(&filter, EVENT_BOT_CRASHED),
            "bot_crashed must be delivered despite an allowlist that omits it"
        );
        // Non-page kinds are still filtered normally.
        assert!(!channel_wants(&filter, EVENT_BOT_SPAWNED));
        // And every always-delivered kind bypasses the filter.
        for kind in ALWAYS_DELIVERED_KINDS {
            assert!(channel_wants(&filter, kind));
        }
    }

    // ── payload shape ──────────────────────────────────────────────────────

    #[test]
    fn payload_has_single_embed_with_expected_fields() {
        let p = discord_payload(&ev(EVENT_BOT_SPAWNED));

        let embeds = p["embeds"].as_array().expect("embeds array");
        assert_eq!(embeds.len(), 1, "exactly one embed");
        let embed = &embeds[0];

        assert_eq!(embed["title"], "FKS · bot_spawned");
        assert_eq!(embed["timestamp"], "2026-07-07T12:00:00+00:00");
        assert_eq!(embed["color"], 0x2E_CC71);

        // Collect field name→value for order-independent assertions.
        let fields = embed["fields"].as_array().expect("fields array");
        let pairs: std::collections::HashMap<&str, &str> = fields
            .iter()
            .map(|f| (f["name"].as_str().unwrap(), f["value"].as_str().unwrap()))
            .collect();
        assert_eq!(pairs["bot_id"], "eth-scalper");
        assert_eq!(pairs["image"], "fks-bot-eth:v1");
        assert_eq!(pairs["mode"], "paper");
        assert_eq!(pairs["event"], "bot_spawned");
        // No detail on a plain spawn.
        assert!(!pairs.contains_key("detail"));
    }

    #[test]
    fn error_payload_includes_detail_and_red_stripe() {
        let e = NotificationEvent::error("eth-scalper", "fks-bot-eth:v1", "live", "boom: OOM");
        let p = discord_payload(&e);
        let embed = &p["embeds"][0];
        assert_eq!(embed["color"], 0xE7_4C3C);
        let fields = embed["fields"].as_array().unwrap();
        let detail = fields
            .iter()
            .find(|f| f["name"] == "detail")
            .expect("detail field present");
        assert_eq!(detail["value"], "boom: OOM");
        assert_eq!(detail["inline"], false);
    }

    #[test]
    fn crashed_payload_is_red_with_exit_detail() {
        let e = NotificationEvent::crashed(
            "crypto-spot-live",
            "fks-bot-crypto-spot:latest",
            "live",
            "unexpected exit (crash), exit_code=139",
        );
        assert_eq!(e.event, EVENT_BOT_CRASHED);
        let p = discord_payload(&e);
        let embed = &p["embeds"][0];
        assert_eq!(embed["color"], 0xE7_4C3C, "crash is a red page");
        let fields = embed["fields"].as_array().unwrap();
        let detail = fields
            .iter()
            .find(|f| f["name"] == "detail")
            .expect("detail field present");
        assert_eq!(detail["value"], "unexpected exit (crash), exit_code=139");
        // A catch-all channel (empty filter) receives it.
        assert!(channel_wants(&[], EVENT_BOT_CRASHED));
    }

    #[test]
    fn blank_fields_render_as_dash() {
        // stop/remove events carry only a container id → image/mode are blank.
        let p = discord_payload(&NotificationEvent::stopped("abc123def456"));
        let embed = &p["embeds"][0];
        let fields = embed["fields"].as_array().unwrap();
        let pairs: std::collections::HashMap<&str, &str> = fields
            .iter()
            .map(|f| (f["name"].as_str().unwrap(), f["value"].as_str().unwrap()))
            .collect();
        assert_eq!(pairs["bot_id"], "abc123def456");
        assert_eq!(
            pairs["image"], "—",
            "blank image must not be an empty string"
        );
        assert_eq!(pairs["mode"], "—", "blank mode must not be an empty string");
    }

    #[test]
    fn long_field_value_is_truncated() {
        let huge = "x".repeat(MAX_FIELD_VALUE_LEN + 100);
        let e = NotificationEvent::error("id", &huge, "paper", "err");
        let p = discord_payload(&e);
        let fields = p["embeds"][0]["fields"].as_array().unwrap();
        let image = fields.iter().find(|f| f["name"] == "image").unwrap();
        let val = image["value"].as_str().unwrap();
        assert!(
            val.chars().count() <= MAX_FIELD_VALUE_LEN + 1,
            "truncated value stays within Discord's budget (+ ellipsis)"
        );
        assert!(val.ends_with('…'));
    }

    #[test]
    fn spawned_constructor_maps_response_fields() {
        let resp = SpawnResponse {
            container_id: "cid".to_string(),
            container_name: "fks-bot-x".to_string(),
            bot_id: "x".to_string(),
            image: "fks-bot-x:latest".to_string(),
            mode: "paper".to_string(),
            started_at: Utc::now(),
        };
        let e = NotificationEvent::spawned(&resp);
        assert_eq!(e.event, EVENT_BOT_SPAWNED);
        assert_eq!(e.bot_id, "x");
        assert_eq!(e.image, "fks-bot-x:latest");
        assert_eq!(e.mode, "paper");
    }

    // ── new event kinds (plan-03 Phase C/D) ─────────────────────────────────

    /// Every kind renders a non-blue stripe (blue is the unknown-kind fallback),
    /// so a new kind that forgot its `discord_payload` arm can't ship silently.
    #[test]
    fn every_known_kind_has_a_dedicated_stripe() {
        for kind in ALL_EVENT_KINDS {
            let color = discord_payload(&ev(kind))["embeds"][0]["color"]
                .as_u64()
                .unwrap();
            assert_ne!(
                color, 0x34_98DB,
                "{kind} falls through to the unknown-kind blue stripe"
            );
        }
    }

    /// The always-delivered set is EXACTLY the four page-worthy kinds — a
    /// regression that added (or dropped) one would flip a stale scoped filter's
    /// behaviour for a money-adjacent page.
    #[test]
    fn always_delivered_set_is_the_page_worthy_four() {
        let empty: Vec<String> = vec![];
        for kind in [
            EVENT_BOT_CRASHED,
            EVENT_BOT_RESTARTED,
            EVENT_LIVE_FLIP,
            EVENT_RISK_HALT,
        ] {
            assert!(ALWAYS_DELIVERED_KINDS.contains(&kind), "{kind} must page");
            // Bypasses even an allowlist that omits it.
            let scoped = vec![EVENT_BOT_STOPPED.to_string()];
            assert!(channel_wants(&scoped, kind));
        }
        // Non-page kinds are filtered normally (an empty filter still catches
        // all, but a scoped filter that omits them drops them).
        for kind in [
            EVENT_KEY_ROTATION,
            EVENT_NET_WORTH_MILESTONE,
            EVENT_EDGE_DECAY,
        ] {
            assert!(!ALWAYS_DELIVERED_KINDS.contains(&kind));
            assert!(channel_wants(&empty, kind), "empty filter catches {kind}");
            let scoped = vec![EVENT_BOT_STOPPED.to_string()];
            assert!(
                !channel_wants(&scoped, kind),
                "{kind} must be droppable by a scoped filter"
            );
        }
    }

    #[test]
    fn restarted_is_green_recovery_with_attempt_detail() {
        let e = NotificationEvent::restarted(
            "eth-scalper",
            "fks-bot-eth:v1",
            "live",
            "attempt 1/3, new container abc123",
        );
        assert_eq!(e.event, EVENT_BOT_RESTARTED);
        let p = discord_payload(&e);
        assert_eq!(p["embeds"][0]["color"], 0x2E_CC71, "recovery is green");
        let detail = p["embeds"][0]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == "detail")
            .unwrap();
        assert_eq!(detail["value"], "attempt 1/3, new container abc123");
    }

    #[test]
    fn live_flip_is_deep_red_from_response() {
        let resp = SpawnResponse {
            container_id: "cid".to_string(),
            container_name: "fks-bot-x".to_string(),
            bot_id: "crypto-spot-live".to_string(),
            image: "fks-bot-crypto-spot:latest".to_string(),
            mode: "live".to_string(),
            started_at: Utc::now(),
        };
        let e = NotificationEvent::live_flip(&resp);
        assert_eq!(e.event, EVENT_LIVE_FLIP);
        assert_eq!(e.bot_id, "crypto-spot-live");
        assert_eq!(e.mode, "live");
        assert_eq!(discord_payload(&e)["embeds"][0]["color"], 0xC0_392B);
    }

    /// SECURITY: a key_rotation event must carry the exchange name + the respawn
    /// reminder and NOTHING that looks like key material. The struct has no
    /// field for a secret by construction; this locks the rendered payload too.
    #[test]
    fn key_rotation_carries_reminder_never_key_material() {
        let e = NotificationEvent::key_rotation("kraken");
        assert_eq!(e.event, EVENT_KEY_ROTATION);
        let detail = e.detail.as_deref().unwrap();
        assert!(detail.contains("kraken"));
        assert!(
            detail.contains("respawn"),
            "the reminder IS the point of the event"
        );
        // Whole rendered payload, serialised, must not leak secret-shaped keys.
        let rendered = discord_payload(&e).to_string();
        for banned in ["api_key", "api_secret", "passphrase", "secret"] {
            assert!(
                !rendered.contains(banned),
                "key_rotation payload leaked '{banned}'"
            );
        }
        assert_eq!(discord_payload(&e)["embeds"][0]["color"], 0x9B_59B6);
    }

    #[test]
    fn net_worth_milestone_direction_drives_arrow_and_stripe() {
        let up = NotificationEvent::net_worth_milestone(105_000.0, 100_000.0, true);
        assert!(up.detail.as_deref().unwrap().contains('▲'));
        assert!(up.detail.as_deref().unwrap().contains("up"));
        assert_eq!(
            discord_payload(&up)["embeds"][0]["color"],
            0x2E_CC71,
            "a gain is green"
        );

        let down = NotificationEvent::net_worth_milestone(95_000.0, 100_000.0, false);
        assert!(down.detail.as_deref().unwrap().contains('▼'));
        assert!(down.detail.as_deref().unwrap().contains("down"));
        assert_eq!(
            discord_payload(&down)["embeds"][0]["color"],
            0xF3_9C12,
            "a drawdown is amber"
        );
    }

    #[test]
    fn ingest_constructor_shapes_external_event() {
        let e = NotificationEvent::from_ingest(
            EVENT_RISK_HALT,
            "eth-scalper",
            "live",
            "trade-cap tripped [source=ingest]",
        );
        assert_eq!(e.event, EVENT_RISK_HALT);
        assert_eq!(e.bot_id, "eth-scalper");
        assert_eq!(e.mode, "live");
        assert_eq!(e.image, "", "external events have no image");
        assert_eq!(
            e.detail.as_deref(),
            Some("trade-cap tripped [source=ingest]")
        );
        // Blank detail collapses to None (no empty "detail" field rendered).
        let blank = NotificationEvent::from_ingest(EVENT_EDGE_DECAY, "", "", "");
        assert!(blank.detail.is_none());
        assert_eq!(discord_payload(&e)["embeds"][0]["color"], 0xE7_4C3C);
    }

    // ── ledger detail snippet ───────────────────────────────────────────────

    #[test]
    fn ledger_detail_truncates_and_never_holds_a_url() {
        // None detail → empty snippet.
        assert_eq!(ledger_detail(&ev(EVENT_BOT_SPAWNED)), "");

        // A long detail is truncated to the column budget (char-safe).
        let long = "z".repeat(MAX_LEDGER_DETAIL_LEN + 200);
        let e = NotificationEvent::error("id", "img", "live", &long);
        let snippet = ledger_detail(&e);
        assert_eq!(snippet.chars().count(), MAX_LEDGER_DETAIL_LEN);

        // The snippet only ever reflects `detail` — never the (absent) URL.
        let e2 = NotificationEvent::crashed("b", "i", "live", "exit_code=139");
        assert_eq!(ledger_detail(&e2), "exit_code=139");
    }
}
