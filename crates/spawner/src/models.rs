// =============================================================================
// models.rs — FKS Bot Spawner request/response types
// =============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// Spawn request
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for POST /spawn
#[derive(Debug, Deserialize)]
pub struct SpawnRequest {
    /// Docker image to run. Must start with ALLOWED_IMAGE_PREFIX.
    pub image: String,

    /// Human-readable bot name / identifier (used as container name suffix).
    /// If omitted a UUID is generated.
    pub bot_id: Option<String>,

    /// Execution mode label — informational, stored as container label.
    #[serde(default = "default_mode")]
    pub mode: String,

    /// Environment variables injected into the container.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Extra labels applied to the container (merged with mandatory fks.* labels).
    #[serde(default)]
    pub labels: HashMap<String, String>,

    /// CPU limit in fractional cores. Overrides the server default.
    pub cpu_limit: Option<f64>,

    /// Memory limit in megabytes. Overrides the server default.
    pub memory_limit_mb: Option<i64>,

    /// Optional command override (replaces the image's CMD).
    pub cmd: Option<Vec<String>>,

    /// Optional entrypoint override.
    pub entrypoint: Option<Vec<String>>,

    /// Exchanges whose stored credentials (POST /secrets) are injected into
    /// the container env as `{EXCHANGE}_API_KEY` / `{EXCHANGE}_API_SECRET`
    /// (+ `_API_PASSPHRASE` when stored) — the names the crypto bots read.
    /// Requires the spawner DB; the spawn FAILS if any requested exchange has
    /// no stored credentials (never silently start a keyless bot that asked
    /// for keys). Explicit `env` entries win over injected ones.
    #[serde(default)]
    pub secrets: Vec<String>,
}

fn default_mode() -> String {
    "paper".to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Secrets request (POST /secrets)
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /secrets` — stores exchange API credentials.
///
/// SECURITY: the WebUI browser only ever SUBMITS this; the spawner persists it
/// server-side and never returns the key/secret. Keys unlock the authenticated
/// order path (`exchange-apiws`), which stays behind the manual execution gate.
#[derive(Debug, Deserialize)]
pub struct SecretRequest {
    /// Exchange identifier, e.g. "kraken", "kucoin", "binance".
    pub exchange: String,

    /// API key (public part).
    pub api_key: String,

    /// API secret (private part) — stored server-side, never returned.
    pub api_secret: String,

    /// Optional passphrase (KuCoin / Coinbase). Omitted for Kraken / Binance.
    #[serde(default)]
    pub api_passphrase: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Notification channel request (POST /notifications)
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /notifications` — stores an operator-configured
/// notification channel (a Discord webhook today).
///
/// SECURITY: the WebUI browser only ever SUBMITS this; the spawner persists the
/// `url` server-side (encrypted with the same cipher as exchange keys) and
/// never returns it. A Discord webhook URL is a bearer capability — anyone
/// holding it can post to the channel — so it is treated as a secret.
///
/// BOUNDARY: this is the STORE only. Actually SENDING to the channel is a
/// consumer-side follow-up (a notifier task / bots / janus reading channels).
#[derive(Debug, Deserialize)]
pub struct NotificationChannelRequest {
    /// Operator-chosen channel name (the UPSERT key), e.g. "ops-alerts".
    pub name: String,

    /// Transport kind. Defaults to "discord_webhook"; future kinds
    /// (slack_webhook, telegram, generic_webhook) validate the same way.
    #[serde(default = "default_channel_kind")]
    pub kind: String,

    /// The webhook URL — stored encrypted at rest, never returned.
    pub url: String,

    /// Subscribed event names. An EMPTY list is the catch-all ("send
    /// everything"); specific names (e.g. "spawn", "stop", "live_flip",
    /// "pnl_digest") subscribe to just those events.
    #[serde(default)]
    pub events: Vec<String>,
}

fn default_channel_kind() -> String {
    "discord_webhook".to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved spawn config (POST /configs) — a reusable, named spawn template
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /configs` — persists a named spawn template in
/// `bot_configs`. Resource limits + env live in the row's `config_json` (the
/// spawner's sqlx build has no decimal feature, so the NUMERIC `cpu_limit`
/// column is left to the JSON blob rather than bound directly).
#[derive(Debug, Deserialize)]
pub struct ConfigRequest {
    /// Unique human-readable name (the upsert key).
    pub name: String,
    /// Docker image (validated against the prefix when actually spawned).
    pub image: String,
    /// Execution mode label.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Optional CPU limit in fractional cores.
    pub cpu_limit: Option<f64>,
    /// Optional memory limit in megabytes.
    pub memory_mb: Option<i32>,
    /// Environment variables for the spawn.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Exchanges whose stored credentials should be injected at spawn time
    /// (mirrors `SpawnRequest.secrets`), so a saved template is fully
    /// self-contained: image + env + which keys the bot needs.
    #[serde(default)]
    pub secrets: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved dock layout (POST /ui/layouts) — a named WebUI workspace arrangement
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /ui/layouts` — persists a named dock layout in
/// `ui_layouts`. The `layout` is the opaque serialized dockview envelope the
/// `/workspace` client produces; the spawner stores it verbatim (plaintext —
/// a layout carries no secrets). Re-posting the same name UPSERTs.
#[derive(Debug, Deserialize)]
pub struct LayoutRequest {
    /// Unique human-readable name (the upsert key).
    pub name: String,
    /// The serialized dockview layout envelope, stored verbatim.
    pub layout: serde_json::Value,
}

// ─────────────────────────────────────────────────────────────────────────────
// Treasury: transfer ledger + account registry requests (POST /transfers,
// POST /accounts) — see src/sql/spawner/007_treasury.sql + crate::treasury
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for `POST /transfers` — appends one signed cash-flow row to
/// the `transfers` ledger so net-worth drift decomposes into deposits vs
/// trading profit (GET /profit).
///
/// SIGN CONVENTION: `amount` is signed from the account's point of view —
/// positive = money INTO the account (deposit), negative = money OUT
/// (withdrawal). Validated (finite, non-zero, allowlisted kind/source) by
/// `treasury::validate_transfer` before touching the store.
#[derive(Debug, Deserialize)]
pub struct TransferRequest {
    /// Which account the flow belongs to: a bot_id (the fks.bot_id label, so
    /// the ledger joins net_worth_snapshots) or an accounts-registry id.
    pub account_id: String,

    /// Signed flow (positive = in, negative = out). Must be finite, non-zero.
    pub amount: f64,

    /// What the flow is: deposit | withdrawal | payout | sweep.
    pub kind: String,

    /// Which writer produced the row: manual (operator entry, the default) |
    /// bot_detected (a bot noticing an unexplained balance jump).
    #[serde(default = "default_transfer_source")]
    pub source: String,

    /// Denomination of `amount`. Defaults to USD.
    #[serde(default = "default_currency")]
    pub currency: String,

    /// Free-form operator annotation ("July DCA", "APEX payout #3", …).
    #[serde(default)]
    pub note: Option<String>,

    /// Optional explicit timestamp (RFC3339) for backfilled entries — the
    /// operator recording a past deposit after the fact. Omitted = the DB
    /// stamps NOW().
    #[serde(default)]
    pub ts: Option<DateTime<Utc>>,
}

fn default_transfer_source() -> String {
    "manual".to_string()
}

fn default_currency() -> String {
    "USD".to_string()
}

/// Request body for `POST /accounts` — creates/UPSERTs one row of the
/// `accounts` registry (the multi-account treasury topology's source of
/// truth). Re-posting the same `account_id` overwrites.
///
/// SECURITY: deliberately NO credential fields — API keys live in the
/// encrypted `exchange_secrets` store (POST /secrets), never in the registry.
#[derive(Debug, Deserialize)]
pub struct AccountRequest {
    /// Logical account identity (the UPSERT key). For bot-traded accounts,
    /// the fks.bot_id label, so registry rows join transfers/net-worth.
    pub account_id: String,

    /// Human-friendly label for the WebUI.
    #[serde(default)]
    pub display_name: Option<String>,

    /// Treasury tier: 0 = cold-BTC backbone, 1 = personal-crypto,
    /// 2 = rithmic-main, 3 = prop-copy-target.
    pub tier: i16,

    /// Coarse classification: personal-crypto | paper | prop | cold-storage | …
    pub account_class: String,

    /// Venue/exchange/broker the account lives at (kraken, rithmic, …).
    #[serde(default)]
    pub venue: Option<String>,

    /// How the platform interacts with the account:
    /// watch | bot-trade | human-trade-source | copy-target.
    pub role: String,

    /// Prop firm the account belongs to (None for non-prop accounts).
    #[serde(default)]
    pub firm: Option<String>,

    /// Copy-trading compliance posture. Defaults to manual-mirror (a human
    /// confirms every mirrored fill); auto-fill only where firm rules allow.
    #[serde(default = "default_compliance_flag")]
    pub compliance_flag: String,

    /// Operator-set risk policy JSON object (enforced by later phases).
    #[serde(default)]
    pub risk_caps: Option<serde_json::Value>,

    /// Operator-set sizing policy JSON object.
    #[serde(default)]
    pub sizing: Option<serde_json::Value>,

    /// Soft-delete flag; defaults to true. DELETE /accounts/{id} flips it off.
    #[serde(default = "default_true")]
    pub active: bool,
}

fn default_compliance_flag() -> String {
    "manual-mirror".to_string()
}

fn default_true() -> bool {
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// Spawn response
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct SpawnResponse {
    pub container_id: String,
    pub container_name: String,
    pub bot_id: String,
    pub image: String,
    pub mode: String,
    pub started_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Container info (returned by GET /containers and GET /container/:id)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContainerInfo {
    /// Short container ID (12 chars).
    pub id: String,
    /// Full 64-char container ID.
    pub id_full: String,
    pub name: String,
    pub image: String,
    pub status: String,
    pub state: String,
    pub bot_id: String,
    pub mode: String,
    pub created_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub labels: HashMap<String, String>,
    /// CPU usage percent (0–100 per core), if available.
    pub cpu_percent: Option<f64>,
    /// Memory usage in bytes, if available.
    pub memory_bytes: Option<i64>,
    /// Memory limit in bytes, if the container has one.
    pub memory_limit_bytes: Option<i64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Container resource stats (live CPU + memory)
// ─────────────────────────────────────────────────────────────────────────────

/// Live resource usage for one container, derived from the Docker stats API.
/// Used to enrich `ContainerInfo` on the `/containers` listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerStats {
    /// CPU usage percent (0–100 per core × online cpus). None when unavailable.
    pub cpu_percent: Option<f64>,
    /// Resident memory in bytes (total usage minus reclaimable cache).
    pub memory_bytes: Option<i64>,
    /// Configured memory limit in bytes, if any.
    pub memory_limit_bytes: Option<i64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Action responses
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    pub ok: bool,
    pub container_id: String,
    pub action: String,
    pub message: String,
}

impl ActionResponse {
    pub fn ok(container_id: impl Into<String>, action: impl Into<String>) -> Self {
        let action = action.into();
        Self {
            ok: true,
            message: format!("{} completed", action),
            container_id: container_id.into(),
            action,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Health
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
    pub version: &'static str,
    pub running_bots: usize,
    pub max_bots: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// Error response (serialised as JSON for 4xx/5xx)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            detail: None,
        }
    }
    #[allow(dead_code)] // public surface for richer error responses
    pub fn with_detail(error: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            detail: Some(detail.into()),
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_request_minimal_deserializes_with_defaults() {
        let raw = r#"{"image": "fks-bot-arbitrage:latest"}"#;
        let req: SpawnRequest = serde_json::from_str(raw).expect("valid JSON");
        assert_eq!(req.image, "fks-bot-arbitrage:latest");
        assert!(req.bot_id.is_none());
        assert_eq!(req.mode, "paper", "default mode should be 'paper'");
        assert!(req.env.is_empty());
        assert!(req.labels.is_empty());
        assert!(req.cmd.is_none());
    }

    #[test]
    fn spawn_request_full_deserializes() {
        let raw = r#"{
            "image": "fks-bot-eth:v1",
            "bot_id": "my-bot",
            "mode": "live",
            "env": {"KEY": "value"},
            "labels": {"team": "trading"},
            "cpu_limit": 0.5,
            "memory_limit_mb": 256,
            "cmd": ["/bin/bot", "--flag"],
            "entrypoint": ["/sbin/init"]
        }"#;
        let req: SpawnRequest = serde_json::from_str(raw).expect("valid JSON");
        assert_eq!(req.bot_id.as_deref(), Some("my-bot"));
        assert_eq!(req.mode, "live");
        assert_eq!(req.env.get("KEY").map(String::as_str), Some("value"));
        assert_eq!(req.labels.get("team").map(String::as_str), Some("trading"));
        assert_eq!(req.cpu_limit, Some(0.5));
        assert_eq!(req.memory_limit_mb, Some(256));
        assert_eq!(
            req.cmd.as_deref().map(|v| v.len()),
            Some(2),
            "cmd vec should have 2 entries"
        );
        assert!(req.entrypoint.is_some());
    }

    #[test]
    fn secret_request_deserializes_with_optional_passphrase() {
        // Kraken / Binance: no passphrase → defaults to None.
        let raw = r#"{"exchange":"kraken","api_key":"k","api_secret":"s"}"#;
        let req: SecretRequest = serde_json::from_str(raw).expect("valid JSON");
        assert_eq!(req.exchange, "kraken");
        assert_eq!(req.api_key, "k");
        assert_eq!(req.api_secret, "s");
        assert!(req.api_passphrase.is_none(), "passphrase defaults to None");

        // KuCoin / Coinbase: passphrase present.
        let raw2 = r#"{"exchange":"kucoin","api_key":"k","api_secret":"s","api_passphrase":"p"}"#;
        let req2: SecretRequest = serde_json::from_str(raw2).expect("valid JSON");
        assert_eq!(req2.api_passphrase.as_deref(), Some("p"));
    }

    #[test]
    fn notification_channel_request_defaults_kind_and_events() {
        // Minimal: kind defaults to discord_webhook, events defaults to empty
        // (catch-all).
        let raw = r#"{"name":"ops-alerts","url":"https://discord.com/api/webhooks/1/abc"}"#;
        let req: NotificationChannelRequest = serde_json::from_str(raw).expect("valid JSON");
        assert_eq!(req.name, "ops-alerts");
        assert_eq!(
            req.kind, "discord_webhook",
            "kind defaults to discord_webhook"
        );
        assert_eq!(req.url, "https://discord.com/api/webhooks/1/abc");
        assert!(
            req.events.is_empty(),
            "events defaults to empty (catch-all)"
        );
    }

    #[test]
    fn notification_channel_request_full_deserializes() {
        let raw = r#"{
            "name":"pnl",
            "kind":"discord_webhook",
            "url":"https://discord.com/api/webhooks/2/xyz",
            "events":["spawn","stop","pnl_digest"]
        }"#;
        let req: NotificationChannelRequest = serde_json::from_str(raw).expect("valid JSON");
        assert_eq!(req.events.len(), 3);
        assert_eq!(req.events[2], "pnl_digest");
    }

    #[test]
    fn action_response_ok_builds_expected_payload() {
        let r = ActionResponse::ok("abc123", "stop");
        assert!(r.ok);
        assert_eq!(r.container_id, "abc123");
        assert_eq!(r.action, "stop");
        assert_eq!(r.message, "stop completed");
    }

    #[test]
    fn error_response_with_detail_serializes_both_fields() {
        let e = ErrorResponse::with_detail("InvalidImage", "prefix mismatch");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["error"], "InvalidImage");
        assert_eq!(v["detail"], "prefix mismatch");
    }

    #[test]
    fn error_response_new_omits_detail_field_or_serializes_null() {
        // detail is Option<String>; serde_json serializes None as null by default.
        let e = ErrorResponse::new("NotFound");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["error"], "NotFound");
        assert!(v["detail"].is_null());
    }
}
