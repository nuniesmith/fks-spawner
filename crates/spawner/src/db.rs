// =============================================================================
// db.rs — optional Postgres persistence for the FKS Bot Spawner
//
// Wraps the `bot_runs` table defined in src/sql/ruby/007_spawner.sql. The
// spawner runs perfectly without a database — the `BotRunStore` is wrapped
// in `Option` everywhere it's used, and missing/failed Postgres connections
// degrade gracefully to "stateless" operation (logged as a warning at boot).
//
// Schema this code expects (see 007_spawner.sql for the full definition):
//
//   bot_runs (
//       id              UUID PRIMARY KEY,
//       bot_config_id   UUID NULL,
//       container_id    TEXT NOT NULL,
//       container_name  TEXT,
//       image           TEXT NOT NULL,
//       mode            TEXT NOT NULL DEFAULT 'paper',
//       status          TEXT NOT NULL DEFAULT 'spawning',
//       started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//       stopped_at      TIMESTAMPTZ,
//       runtime_secs    INTEGER,        -- computed by trigger
//       error_message   TEXT,
//       ...
//   )
//
// Status values used here mirror the CHECK constraint in the SQL:
//   'spawning' | 'running' | 'stopping' | 'stopped' | 'error' | 'pruned'
// =============================================================================

#![cfg(feature = "db")]

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::SpawnerError;
use crate::models::{AccountRequest, ConfigRequest};
use crate::net_worth::NetWorthSnapshot;
use crate::secrets_crypto::SecretsCipher;
use crate::treasury::NewTransfer;

/// One exchange's decrypted API credentials, as fetched by
/// [`BotRunStore::get_secret`] for spawn-time env injection. Never serialized
/// or logged — values flow only into the spawned container's env.
pub struct ExchangeCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// BotRunStore — thin wrapper around a sqlx PgPool. Scoped to bot_runs ops plus
// the exchange_secrets credential store (003_secrets.sql); both share the pool.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct BotRunStore {
    pool: PgPool,
    /// At-rest cipher for `exchange_secrets` (SPAWNER_SECRETS_KEY). The store
    /// is the encryption boundary: values are encrypted in `upsert_secret`,
    /// and a future spawn-time injection path decrypts here too.
    cipher: SecretsCipher,
}

impl BotRunStore {
    /// Connect to Postgres. Returns `Ok(None)` when `database_url` is empty
    /// or the connection fails — callers treat that as "stateless mode" and
    /// continue running.
    ///
    /// A present-but-INVALID `SPAWNER_SECRETS_KEY` also disables the DB
    /// (fail-safe): the operator configured encryption, so we refuse to fall
    /// back to plaintext secret writes.
    pub async fn try_connect(database_url: &str) -> Option<Self> {
        if database_url.is_empty() {
            info!("spawner DB disabled (DATABASE_URL not set) — running stateless");
            return None;
        }

        let cipher = match SecretsCipher::from_env() {
            Ok(c) => c,
            Err(e) => {
                error!(
                    error = %e,
                    "SPAWNER_SECRETS_KEY invalid — refusing to enable the spawner DB \
                     (would risk plaintext secret writes); running stateless"
                );
                return None;
            }
        };
        if cipher.is_encrypting() {
            info!("exchange_secrets encryption-at-rest ENABLED (SPAWNER_SECRETS_KEY set)");
        }

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await;

        match pool {
            Ok(pool) => {
                info!(url_host = %sanitize_url(database_url), "spawner connected to Postgres");
                Some(Self { pool, cipher })
            }
            Err(e) => {
                warn!(
                    error = %e,
                    url_host = %sanitize_url(database_url),
                    "spawner failed to connect to Postgres — running stateless"
                );
                None
            }
        }
    }

    /// Check the bot_runs table exists. Logs a warning if it doesn't but does
    /// not fail — we want the spawner to keep running even if migrations
    /// haven't been applied yet.
    pub async fn check_schema(&self) -> bool {
        match sqlx::query(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'bot_runs'",
        )
        .fetch_optional(&self.pool)
        .await
        {
            Ok(Some(_)) => true,
            Ok(None) => {
                warn!(
                    "bot_runs table not found — apply src/sql/ruby/007_spawner.sql \
                     (writes to bot_runs will be skipped)"
                );
                false
            }
            Err(e) => {
                warn!(error = %e, "schema probe failed — writes to bot_runs may fail");
                false
            }
        }
    }

    /// Insert a new bot_runs row when a container has been successfully
    /// created and started.
    pub async fn record_spawn(&self, args: RecordSpawn<'_>) -> Result<Uuid, SpawnerError> {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO bot_runs (\
                 id, container_id, container_name, image, mode, status, started_at\
             ) VALUES ($1, $2, $3, $4, $5, 'running', $6)",
        )
        .bind(id)
        .bind(args.container_id)
        .bind(args.container_name)
        .bind(args.image)
        .bind(args.mode)
        .bind(args.started_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(run_id = %id, container_id = %args.container_id, "bot_runs row inserted");
        Ok(id)
    }

    /// Mark a run as stopping → stopped. The DB trigger will compute
    /// `runtime_secs` from `started_at` automatically. Matches by short
    /// container_id (the spawner exposes 12-char IDs everywhere).
    pub async fn record_stop(&self, container_id: &str) -> Result<(), SpawnerError> {
        let rows = sqlx::query(
            "UPDATE bot_runs \
             SET status = 'stopped', stopped_at = NOW() \
             WHERE container_id = $1 AND stopped_at IS NULL",
        )
        .bind(container_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(
            container_id = %container_id,
            rows_affected = rows.rows_affected(),
            "bot_runs row updated to stopped"
        );
        Ok(())
    }

    /// Mark a run as removed/pruned. Used by both DELETE /container/:id and
    /// the auto-prune background task.
    pub async fn record_remove(&self, container_id: &str) -> Result<(), SpawnerError> {
        let rows = sqlx::query(
            "UPDATE bot_runs \
             SET status = CASE WHEN status = 'stopped' THEN 'pruned' ELSE 'stopped' END, \
                 stopped_at = COALESCE(stopped_at, NOW()) \
             WHERE container_id = $1",
        )
        .bind(container_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(
            container_id = %container_id,
            rows_affected = rows.rows_affected(),
            "bot_runs row updated to pruned/stopped"
        );
        Ok(())
    }

    /// Record a failure — used when spawn fails AFTER container creation
    /// (e.g. start_container failed).
    #[allow(dead_code)] // exposed for future use; spawn() currently rolls back via remove()
    pub async fn record_error(
        &self,
        container_id: &str,
        message: &str,
    ) -> Result<(), SpawnerError> {
        sqlx::query(
            "UPDATE bot_runs \
             SET status = 'error', error_message = $2, stopped_at = NOW() \
             WHERE container_id = $1",
        )
        .bind(container_id)
        .bind(message)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Recent run history — newest first. Limit is clamped to 1..=500.
    pub async fn recent_runs(&self, limit: i64) -> Result<Vec<BotRunRow>, SpawnerError> {
        let limit = limit.clamp(1, 500);
        let rows = sqlx::query(
            "SELECT id, container_id, container_name, image, mode, status, \
                    started_at, stopped_at, runtime_secs, error_message \
             FROM bot_runs \
             ORDER BY started_at DESC \
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows.into_iter().map(BotRunRow::from_row).collect())
    }

    // ── exchange_secrets (see src/sql/spawner/003_secrets.sql) ──────────────
    // The WebUI submits exchange API credentials here; they are stored
    // server-side and never returned. `upsert_secret` writes them (overwriting
    // any prior row for that exchange); `configured_exchanges` reports only
    // which exchanges are set — never the key/secret material. With
    // SPAWNER_SECRETS_KEY set, values are ChaCha20-Poly1305-encrypted at rest
    // (enc:v1:… wire format; legacy plaintext rows still decrypt/pass through).

    /// Store (UPSERT) API credentials for one exchange. `exchange` is the
    /// primary key, so re-submitting overwrites rather than duplicating.
    /// Values are encrypted at rest when SPAWNER_SECRETS_KEY is configured.
    pub async fn upsert_secret(
        &self,
        exchange: &str,
        api_key: &str,
        api_secret: &str,
        api_passphrase: Option<&str>,
    ) -> Result<(), SpawnerError> {
        let map_crypto = |e: String| SpawnerError::Other(format!("secret encryption failed: {e}"));
        let api_key = self.cipher.encrypt(api_key).map_err(map_crypto)?;
        let api_secret = self.cipher.encrypt(api_secret).map_err(map_crypto)?;
        let api_passphrase = api_passphrase
            .map(|p| self.cipher.encrypt(p).map_err(map_crypto))
            .transpose()?;

        sqlx::query(
            "INSERT INTO exchange_secrets (exchange, api_key, api_secret, api_passphrase) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (exchange) DO UPDATE \
             SET api_key = EXCLUDED.api_key, \
                 api_secret = EXCLUDED.api_secret, \
                 api_passphrase = EXCLUDED.api_passphrase, \
                 updated_at = NOW()",
        )
        .bind(exchange)
        .bind(&api_key)
        .bind(&api_secret)
        .bind(api_passphrase.as_deref())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(
            exchange = %exchange,
            encrypted = self.cipher.is_encrypting(),
            "exchange_secrets row upserted"
        );
        Ok(())
    }

    /// Decrypt one stored credential value. Legacy plaintext rows pass
    /// through unchanged; an encrypted value with a missing/wrong key is an
    /// error, never returned as-is.
    pub fn decrypt_secret(&self, stored: &str) -> Result<String, SpawnerError> {
        self.cipher
            .decrypt(stored)
            .map_err(|e| SpawnerError::Other(format!("secret decryption failed: {e}")))
    }

    /// Fetch + decrypt one exchange's stored credentials for spawn-time env
    /// injection. `Ok(None)` = no row stored for that exchange.
    pub async fn get_secret(
        &self,
        exchange: &str,
    ) -> Result<Option<ExchangeCredentials>, SpawnerError> {
        let row = sqlx::query(
            "SELECT api_key, api_secret, api_passphrase \
             FROM exchange_secrets WHERE exchange = $1",
        )
        .bind(exchange)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let Some(row) = row else { return Ok(None) };
        let api_key: String = row.get("api_key");
        let api_secret: String = row.get("api_secret");
        let api_passphrase: Option<String> = row.get("api_passphrase");

        Ok(Some(ExchangeCredentials {
            api_key: self.decrypt_secret(&api_key)?,
            api_secret: self.decrypt_secret(&api_secret)?,
            api_passphrase: api_passphrase
                .map(|p| self.decrypt_secret(&p))
                .transpose()?,
        }))
    }

    /// Which exchanges have credentials stored — newest update first. Returns
    /// only metadata (exchange, whether a passphrase is set, last update); the
    /// key/secret values are deliberately never selected.
    pub async fn configured_exchanges(&self) -> Result<Vec<SecretStatusRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT exchange, (api_passphrase IS NOT NULL) AS has_passphrase, updated_at \
             FROM exchange_secrets \
             ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows.into_iter().map(SecretStatusRow::from_row).collect())
    }

    /// Delete one exchange's stored credentials (hard delete — the encrypted
    /// row is gone, unlike bot_configs' soft-delete). Returns whether a row
    /// existed.
    pub async fn delete_secret(&self, exchange: &str) -> Result<bool, SpawnerError> {
        let res = sqlx::query("DELETE FROM exchange_secrets WHERE exchange = $1")
            .bind(exchange)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let removed = res.rows_affected() > 0;
        debug!(exchange = %exchange, removed, "exchange_secrets row deleted");
        Ok(removed)
    }

    // ── notification_channels (see src/sql/spawner/004_notifications.sql) ────
    // Operator-configured notification channels (Discord webhooks today). The
    // WebUI submits a channel here; the target URL is stored server-side and
    // never returned. `upsert_channel` writes it (encrypting the URL at rest,
    // overwriting any prior row for that name); `list_channels` reports only
    // name/kind/events — never the URL. `get_channel_target` decrypts the URL
    // for the notification sender (`crate::notifications`), which is the only
    // consumer that ever sees it (outbound webhook POST, never an HTTP GET).

    /// Store (UPSERT) a notification channel. `name` is the primary key, so
    /// re-submitting overwrites rather than duplicating. The `url` is encrypted
    /// at rest when SPAWNER_SECRETS_KEY is configured (same cipher as exchange
    /// secrets — a webhook URL is a bearer capability).
    pub async fn upsert_channel(
        &self,
        name: &str,
        kind: &str,
        url: &str,
        events: &[String],
    ) -> Result<(), SpawnerError> {
        let map_crypto = |e: String| SpawnerError::Other(format!("webhook encryption failed: {e}"));
        let target = self.cipher.encrypt(url).map_err(map_crypto)?;
        let events_json = serde_json::json!(events);

        sqlx::query(
            "INSERT INTO notification_channels (name, kind, target, events) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (name) DO UPDATE \
             SET kind = EXCLUDED.kind, \
                 target = EXCLUDED.target, \
                 events = EXCLUDED.events, \
                 updated_at = NOW()",
        )
        .bind(name)
        .bind(kind)
        .bind(&target)
        .bind(events_json)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(
            name = %name,
            kind = %kind,
            encrypted = self.cipher.is_encrypting(),
            "notification_channels row upserted"
        );
        Ok(())
    }

    /// List configured notification channels — newest update first. Returns
    /// only metadata (name, kind, events, last update); the target URL is
    /// deliberately never selected.
    pub async fn list_channels(&self) -> Result<Vec<NotificationChannelRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT name, kind, events, updated_at \
             FROM notification_channels \
             ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows
            .into_iter()
            .map(NotificationChannelRow::from_row)
            .collect())
    }

    /// Fetch + decrypt one channel's target URL for the notification sender
    /// (see `crate::notifications`). `Ok(None)` = no row stored for that name.
    /// Never exposed over an HTTP GET — the decrypted URL is a bearer capability
    /// that only flows into an outbound webhook POST.
    pub async fn get_channel_target(&self, name: &str) -> Result<Option<String>, SpawnerError> {
        let row = sqlx::query("SELECT target FROM notification_channels WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;

        let Some(row) = row else { return Ok(None) };
        let target: String = row.get("target");
        Ok(Some(self.decrypt_secret(&target)?))
    }

    /// Delete one notification channel by name (hard delete). Returns whether a
    /// row existed.
    pub async fn delete_channel(&self, name: &str) -> Result<bool, SpawnerError> {
        let res = sqlx::query("DELETE FROM notification_channels WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        let removed = res.rows_affected() > 0;
        debug!(name = %name, removed, "notification_channels row deleted");
        Ok(removed)
    }

    // ── bot_configs (see src/sql/spawner/002_spawner.sql) ───────────────────
    // Reusable named spawn templates. Resource limits + env live in the row's
    // JSONB `config_json` (the sqlx build has no decimal feature, so the NUMERIC
    // `cpu_limit` column is left to the blob rather than bound directly).

    /// Save (UPSERT by name) a spawn config; returns its id.
    pub async fn upsert_config(&self, req: &ConfigRequest) -> Result<Uuid, SpawnerError> {
        let config_json = serde_json::json!({
            "cpu_limit": req.cpu_limit,
            "memory_mb": req.memory_mb,
            "env": req.env,
            "secrets": req.secrets,
        });
        let row = sqlx::query(
            "INSERT INTO bot_configs (name, image, mode, config_json) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (name) DO UPDATE \
             SET image = EXCLUDED.image, \
                 mode = EXCLUDED.mode, \
                 config_json = EXCLUDED.config_json, \
                 is_active = TRUE, \
                 updated_at = NOW() \
             RETURNING id",
        )
        .bind(&req.name)
        .bind(&req.image)
        .bind(&req.mode)
        .bind(config_json)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
        debug!(name = %req.name, config_id = %id, "bot_configs row upserted");
        Ok(id)
    }

    /// All active saved configs, name-ordered.
    pub async fn list_configs(&self) -> Result<Vec<BotConfigRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT id, name, image, mode, config_json \
             FROM bot_configs \
             WHERE is_active = TRUE \
             ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows.into_iter().map(BotConfigRow::from_row).collect())
    }

    /// Soft-delete a config by name (sets `is_active = FALSE`). Returns whether
    /// a row was affected.
    pub async fn deactivate_config(&self, name: &str) -> Result<bool, SpawnerError> {
        let r = sqlx::query(
            "UPDATE bot_configs SET is_active = FALSE, updated_at = NOW() \
             WHERE name = $1 AND is_active = TRUE",
        )
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(r.rows_affected() > 0)
    }

    // ── ui_layouts (see src/sql/spawner/005_ui_layouts.sql) ──────────────────
    // Named WebUI dock layouts, stored plaintext (a layout carries no secrets),
    // so the operator's arrangements follow them across devices.

    /// Save (UPSERT by name) a dock layout. Returns whether a NEW row was
    /// created (`true`) vs. an existing one overwritten (`false`).
    pub async fn upsert_layout(
        &self,
        name: &str,
        layout: &serde_json::Value,
    ) -> Result<bool, SpawnerError> {
        let row = sqlx::query(
            "INSERT INTO ui_layouts (name, layout) \
             VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE \
             SET layout = EXCLUDED.layout, updated_at = NOW() \
             RETURNING (xmax = 0) AS inserted",
        )
        .bind(name)
        .bind(layout)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let inserted: bool = row.try_get("inserted").unwrap_or(false);
        debug!(name, inserted, "ui_layouts row upserted");
        Ok(inserted)
    }

    /// All saved layout names + their last-updated time (NOT the blobs), so the
    /// picker stays light. Name-ordered.
    pub async fn list_layouts(&self) -> Result<Vec<UiLayoutSummaryRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT name, updated_at::text AS updated_at \
             FROM ui_layouts ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows
            .into_iter()
            .map(|r| UiLayoutSummaryRow {
                name: r.try_get("name").unwrap_or_default(),
                updated_at: r.try_get("updated_at").unwrap_or_default(),
            })
            .collect())
    }

    /// Fetch one full layout envelope by name.
    pub async fn get_layout(&self, name: &str) -> Result<Option<serde_json::Value>, SpawnerError> {
        let row = sqlx::query("SELECT layout FROM ui_layouts WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(row.and_then(|r| r.try_get::<serde_json::Value, _>("layout").ok()))
    }

    /// Hard-delete a layout by name. Returns whether a row was removed.
    pub async fn delete_layout(&self, name: &str) -> Result<bool, SpawnerError> {
        let r = sqlx::query("DELETE FROM ui_layouts WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(r.rows_affected() > 0)
    }

    // ── net_worth_snapshots (see src/sql/spawner/006_net_worth_snapshots.sql) ─
    // Append-only net-worth history written by the periodic sampler
    // (`crate::net_worth`). One row per running bot per sweep. `ts` defaults to
    // NOW() in the table so the DB clock stamps the reading.

    /// Insert one net-worth snapshot row. The NUMERIC `net_worth` column is
    /// bound as text + cast (`$2::numeric`) because this sqlx build has no
    /// decimal feature — the same reason `bot_configs.cpu_limit` lives in JSON
    /// (see `upsert_config`). Formatting an `f64` via `Display` yields the
    /// shortest round-trippable decimal, so no precision is lost on the way in.
    pub async fn record_net_worth(&self, snap: &NetWorthSnapshot) -> Result<(), SpawnerError> {
        sqlx::query(
            "INSERT INTO net_worth_snapshots (bot_id, net_worth, currency, venue, source) \
             VALUES ($1, $2::numeric, $3, $4, $5)",
        )
        .bind(&snap.bot_id)
        .bind(snap.net_worth.to_string())
        .bind(&snap.currency)
        .bind(snap.venue.as_deref())
        .bind(&snap.source)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;

        debug!(
            bot_id = %snap.bot_id,
            currency = %snap.currency,
            "net_worth_snapshots row inserted"
        );
        Ok(())
    }

    /// Read recent net-worth snapshots for `GET /net-worth`. Selects the most
    /// recent `limit` rows (optionally filtered to one `bot_id`) but returns
    /// them oldest → newest so the WebUI can plot the series left-to-right
    /// without reversing. The `NUMERIC` column is cast to `float8` in SQL
    /// because this sqlx build has no decimal feature — the mirror of the
    /// text/`::numeric` round-trip `record_net_worth` uses on the way in.
    /// Clamp/normalise the inputs with [`net_worth_query_plan`] first.
    pub async fn list_net_worth(
        &self,
        bot_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<NetWorthSnapshotRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT bot_id, ts, net_worth::float8 AS net_worth, currency, venue \
             FROM ( \
                 SELECT bot_id, ts, net_worth, currency, venue \
                 FROM net_worth_snapshots \
                 WHERE ($1::text IS NULL OR bot_id = $1) \
                 ORDER BY ts DESC \
                 LIMIT $2 \
             ) recent \
             ORDER BY ts ASC",
        )
        .bind(bot_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows
            .into_iter()
            .map(NetWorthSnapshotRow::from_row)
            .collect())
    }

    // ── transfers + accounts (see src/sql/spawner/007_treasury.sql) ──────────
    // The treasury layer: `transfers` is the signed cash-flow ledger (positive
    // = into the account, negative = out) that lets GET /profit decompose
    // net-worth drift into deposits vs trading profit; `accounts` is the
    // multi-account topology registry (NO credentials — those stay in
    // exchange_secrets). Inputs are validated/normalised by `crate::treasury`
    // before they reach here.

    /// Append one validated transfer to the ledger; returns the new row id.
    /// The NUMERIC `amount` is bound as text + `::numeric` cast because this
    /// sqlx build has no decimal feature (same as `record_net_worth`). A
    /// `None` ts lets the DB default the row to NOW(); a backfilled manual
    /// entry carries its own timestamp.
    pub async fn insert_transfer(&self, t: &NewTransfer) -> Result<i64, SpawnerError> {
        let row = sqlx::query(
            "INSERT INTO transfers (account_id, ts, amount, currency, kind, source, note) \
             VALUES ($1, COALESCE($2::timestamptz, NOW()), $3::numeric, $4, $5, $6, $7) \
             RETURNING id",
        )
        .bind(&t.account_id)
        .bind(t.ts)
        .bind(t.amount.to_string())
        .bind(&t.currency)
        .bind(&t.kind)
        .bind(&t.source)
        .bind(t.note.as_deref())
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let id: i64 = row.try_get("id").map_err(map_sqlx)?;
        debug!(id, account_id = %t.account_id, kind = %t.kind, "transfers row inserted");
        Ok(id)
    }

    /// Read a window of the ledger for `GET /transfers`. Selects the most
    /// recent `limit` rows (optionally filtered to one `account_id`) but
    /// returns them oldest → newest, mirroring `list_net_worth`. The NUMERIC
    /// `amount` is cast to `float8` in SQL (no decimal feature). Clamp and
    /// normalise the inputs with [`crate::treasury::transfers_query_plan`]
    /// first.
    pub async fn list_transfers(
        &self,
        account_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<TransferRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT id, account_id, ts, amount::float8 AS amount, currency, kind, source, note \
             FROM ( \
                 SELECT id, account_id, ts, amount, currency, kind, source, note \
                 FROM transfers \
                 WHERE ($1::text IS NULL OR account_id = $1) \
                 ORDER BY ts DESC \
                 LIMIT $2 \
             ) recent \
             ORDER BY ts ASC",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows.into_iter().map(TransferRow::from_row).collect())
    }

    /// Save (UPSERT by account_id) one registry account — the `bot_configs`
    /// UPSERT pattern. Returns whether a NEW row was created (`true`) vs an
    /// existing one overwritten (`false`). Absent risk_caps/sizing default to
    /// `{}` to match the table default.
    pub async fn upsert_account(&self, req: &AccountRequest) -> Result<bool, SpawnerError> {
        let empty = serde_json::json!({});
        let row = sqlx::query(
            "INSERT INTO accounts (account_id, display_name, tier, account_class, venue, \
                                   role, firm, compliance_flag, risk_caps, sizing, active) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (account_id) DO UPDATE \
             SET display_name    = EXCLUDED.display_name, \
                 tier            = EXCLUDED.tier, \
                 account_class   = EXCLUDED.account_class, \
                 venue           = EXCLUDED.venue, \
                 role            = EXCLUDED.role, \
                 firm            = EXCLUDED.firm, \
                 compliance_flag = EXCLUDED.compliance_flag, \
                 risk_caps       = EXCLUDED.risk_caps, \
                 sizing          = EXCLUDED.sizing, \
                 active          = EXCLUDED.active, \
                 updated_at      = NOW() \
             RETURNING (xmax = 0) AS inserted",
        )
        .bind(req.account_id.trim())
        .bind(req.display_name.as_deref())
        .bind(req.tier)
        .bind(req.account_class.trim())
        .bind(req.venue.as_deref())
        .bind(req.role.trim())
        .bind(req.firm.as_deref())
        .bind(req.compliance_flag.trim())
        .bind(req.risk_caps.as_ref().unwrap_or(&empty))
        .bind(req.sizing.as_ref().unwrap_or(&empty))
        .bind(req.active)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let inserted: bool = row.try_get("inserted").unwrap_or(false);
        debug!(account_id = %req.account_id.trim(), inserted, "accounts row upserted");
        Ok(inserted)
    }

    /// All registry accounts, active first (then tier, then id) so the WebUI
    /// list leads with what matters.
    pub async fn list_accounts(&self) -> Result<Vec<AccountRow>, SpawnerError> {
        let rows = sqlx::query(
            "SELECT account_id, display_name, tier, account_class, venue, role, firm, \
                    compliance_flag, risk_caps, sizing, active, created_at, updated_at \
             FROM accounts \
             ORDER BY active DESC, tier ASC, account_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        Ok(rows.into_iter().map(AccountRow::from_row).collect())
    }

    /// Soft-delete an account (active = FALSE, the bot_configs pattern) —
    /// history keyed by the account_id (transfers / net-worth rows) is never
    /// dropped. Returns whether an active row was affected.
    pub async fn deactivate_account(&self, account_id: &str) -> Result<bool, SpawnerError> {
        let r = sqlx::query(
            "UPDATE accounts SET active = FALSE, updated_at = NOW() \
             WHERE account_id = $1 AND active = TRUE",
        )
        .bind(account_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(r.rows_affected() > 0)
    }

    /// Gather the raw inputs for `GET /profit`: the first + last net-worth
    /// snapshots for `account_id` in the window (`since`..now — snapshots key
    /// on bot_id, which IS the account id for bot-traded accounts) plus the
    /// signed transfer amounts strictly AFTER the first snapshot and up to
    /// the last (a flow already reflected in the first snapshot must not be
    /// double-counted). The pure arithmetic lives in
    /// [`crate::treasury::decompose_profit`].
    pub async fn profit_inputs(
        &self,
        account_id: &str,
        since: Option<DateTime<Utc>>,
    ) -> Result<ProfitInputs, SpawnerError> {
        let start = sqlx::query(
            "SELECT ts, net_worth::float8 AS net_worth FROM net_worth_snapshots \
             WHERE bot_id = $1 AND ($2::timestamptz IS NULL OR ts >= $2) \
             ORDER BY ts ASC LIMIT 1",
        )
        .bind(account_id)
        .bind(since)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?
        .map(snapshot_point);

        let end = sqlx::query(
            "SELECT ts, net_worth::float8 AS net_worth FROM net_worth_snapshots \
             WHERE bot_id = $1 AND ($2::timestamptz IS NULL OR ts >= $2) \
             ORDER BY ts DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(since)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?
        .map(snapshot_point);

        // No snapshots in the window → nothing to decompose (the handler
        // reports null figures rather than inventing a zero baseline).
        let (Some((start_ts, _)), Some((end_ts, _))) = (start, end) else {
            return Ok(ProfitInputs {
                start,
                end,
                transfer_amounts: Vec::new(),
            });
        };

        let rows = sqlx::query(
            "SELECT amount::float8 AS amount FROM transfers \
             WHERE account_id = $1 AND ts > $2 AND ts <= $3",
        )
        .bind(account_id)
        .bind(start_ts)
        .bind(end_ts)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let transfer_amounts = rows
            .iter()
            .map(|r| r.try_get("amount").unwrap_or(0.0))
            .collect();

        Ok(ProfitInputs {
            start,
            end,
            transfer_amounts,
        })
    }
}

// ── GET /net-worth request shaping (pure — unit-tested) ──────────────────────

/// Default number of net-worth snapshot rows returned by `GET /net-worth`.
pub const NET_WORTH_DEFAULT_LIMIT: i64 = 500;
/// Hard cap on the number of rows `GET /net-worth` will return.
pub const NET_WORTH_MAX_LIMIT: i64 = 5000;

/// Pure request-shaping for `GET /net-worth`. Clamps `limit` into
/// `1..=NET_WORTH_MAX_LIMIT` (defaulting to [`NET_WORTH_DEFAULT_LIMIT`] when
/// absent) and normalises the optional `bot_id` filter (trimmed; blank → no
/// filter). Split out from the handler + query so the query-shaping logic is
/// unit-testable without a live database. Returns `(bot_id_filter, limit)`.
pub fn net_worth_query_plan(bot_id: Option<&str>, limit: Option<i64>) -> (Option<String>, i64) {
    let limit = limit
        .unwrap_or(NET_WORTH_DEFAULT_LIMIT)
        .clamp(1, NET_WORTH_MAX_LIMIT);
    let bot_id = bot_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    (bot_id, limit)
}

// ─────────────────────────────────────────────────────────────────────────────
// DTOs
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments to `record_spawn` — borrowed strings to avoid pointless clones.
pub struct RecordSpawn<'a> {
    pub container_id: &'a str,
    pub container_name: &'a str,
    pub image: &'a str,
    pub mode: &'a str,
    pub started_at: DateTime<Utc>,
}

/// A row from `bot_runs` exposed via GET /runs.
#[derive(Debug, serde::Serialize)]
pub struct BotRunRow {
    pub id: Uuid,
    pub container_id: String,
    pub container_name: Option<String>,
    pub image: String,
    pub mode: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub runtime_secs: Option<i32>,
    pub error_message: Option<String>,
}

impl BotRunRow {
    fn from_row(r: PgRow) -> Self {
        Self {
            id: r.try_get("id").unwrap_or_else(|_| Uuid::nil()),
            container_id: r.try_get("container_id").unwrap_or_default(),
            container_name: r.try_get("container_name").ok(),
            image: r.try_get("image").unwrap_or_default(),
            mode: r.try_get("mode").unwrap_or_default(),
            status: r.try_get("status").unwrap_or_default(),
            started_at: r.try_get("started_at").unwrap_or_else(|_| Utc::now()),
            stopped_at: r.try_get("stopped_at").ok(),
            runtime_secs: r.try_get("runtime_secs").ok(),
            error_message: r.try_get("error_message").ok(),
        }
    }
}

/// A row from `net_worth_snapshots` exposed via GET /net-worth. `net_worth` is
/// the account value at `ts` in `currency`; `venue` is null for a bot-level
/// total. Serialises to `{bot_id, ts, net_worth, currency, venue}`.
#[derive(Debug, serde::Serialize)]
pub struct NetWorthSnapshotRow {
    pub bot_id: String,
    pub ts: DateTime<Utc>,
    pub net_worth: f64,
    pub currency: String,
    pub venue: Option<String>,
}

impl NetWorthSnapshotRow {
    fn from_row(r: PgRow) -> Self {
        Self {
            bot_id: r.try_get("bot_id").unwrap_or_default(),
            ts: r.try_get("ts").unwrap_or_else(|_| Utc::now()),
            // `net_worth::float8` in the query — a bare NUMERIC can't decode
            // without sqlx's decimal feature.
            net_worth: r.try_get("net_worth").unwrap_or(0.0),
            currency: r.try_get("currency").unwrap_or_default(),
            // Nullable column — NULL decodes as Err, which `.ok()` maps to None.
            venue: r.try_get("venue").ok(),
        }
    }
}

/// A row from `transfers` exposed via GET /transfers. `amount` is signed:
/// positive = into the account (deposit), negative = out (withdrawal).
#[derive(Debug, serde::Serialize)]
pub struct TransferRow {
    pub id: i64,
    pub account_id: String,
    pub ts: DateTime<Utc>,
    pub amount: f64,
    pub currency: String,
    pub kind: String,
    pub source: String,
    pub note: Option<String>,
}

impl TransferRow {
    fn from_row(r: PgRow) -> Self {
        Self {
            id: r.try_get("id").unwrap_or_default(),
            account_id: r.try_get("account_id").unwrap_or_default(),
            ts: r.try_get("ts").unwrap_or_else(|_| Utc::now()),
            // `amount::float8` in the query — a bare NUMERIC can't decode
            // without sqlx's decimal feature.
            amount: r.try_get("amount").unwrap_or(0.0),
            currency: r.try_get("currency").unwrap_or_default(),
            kind: r.try_get("kind").unwrap_or_default(),
            source: r.try_get("source").unwrap_or_default(),
            // Nullable column — NULL decodes as Err, which `.ok()` maps to None.
            note: r.try_get("note").ok(),
        }
    }
}

/// A row from `accounts` exposed via GET /accounts. Topology + policy
/// metadata only — the registry holds NO credentials by design (keys live in
/// the encrypted exchange_secrets store).
#[derive(Debug, serde::Serialize)]
pub struct AccountRow {
    pub account_id: String,
    pub display_name: Option<String>,
    /// 0 = cold-BTC backbone, 1 = personal-crypto, 2 = rithmic-main,
    /// 3 = prop-copy-target.
    pub tier: i16,
    pub account_class: String,
    pub venue: Option<String>,
    pub role: String,
    pub firm: Option<String>,
    pub compliance_flag: String,
    pub risk_caps: serde_json::Value,
    pub sizing: serde_json::Value,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl AccountRow {
    fn from_row(r: PgRow) -> Self {
        Self {
            account_id: r.try_get("account_id").unwrap_or_default(),
            display_name: r.try_get("display_name").ok(),
            tier: r.try_get("tier").unwrap_or(0),
            account_class: r.try_get("account_class").unwrap_or_default(),
            venue: r.try_get("venue").ok(),
            role: r.try_get("role").unwrap_or_default(),
            firm: r.try_get("firm").ok(),
            compliance_flag: r.try_get("compliance_flag").unwrap_or_default(),
            risk_caps: r
                .try_get("risk_caps")
                .unwrap_or_else(|_| serde_json::json!({})),
            sizing: r
                .try_get("sizing")
                .unwrap_or_else(|_| serde_json::json!({})),
            active: r.try_get("active").unwrap_or(false),
            created_at: r.try_get("created_at").unwrap_or_else(|_| Utc::now()),
            updated_at: r.try_get("updated_at").unwrap_or_else(|_| Utc::now()),
        }
    }
}

/// Raw inputs for the `GET /profit` decomposition, as gathered by
/// [`BotRunStore::profit_inputs`]: the first/last net-worth snapshots in the
/// window plus the signed transfer amounts between them. Fed to the pure
/// [`crate::treasury::decompose_profit`].
pub struct ProfitInputs {
    /// First snapshot in the window: (ts, net_worth). None = no data.
    pub start: Option<(DateTime<Utc>, f64)>,
    /// Last snapshot in the window: (ts, net_worth). None = no data.
    pub end: Option<(DateTime<Utc>, f64)>,
    /// Signed transfer amounts with start.ts < ts <= end.ts.
    pub transfer_amounts: Vec<f64>,
}

/// A row from `exchange_secrets` exposed via GET /secrets/status. Carries only
/// non-sensitive metadata — never the key, secret, or passphrase value.
#[derive(Debug, serde::Serialize)]
pub struct SecretStatusRow {
    pub exchange: String,
    pub has_passphrase: bool,
    pub updated_at: DateTime<Utc>,
}

impl SecretStatusRow {
    fn from_row(r: PgRow) -> Self {
        Self {
            exchange: r.try_get("exchange").unwrap_or_default(),
            has_passphrase: r.try_get("has_passphrase").unwrap_or(false),
            updated_at: r.try_get("updated_at").unwrap_or_else(|_| Utc::now()),
        }
    }
}

/// A row from `notification_channels` exposed via GET /notifications. Carries
/// only non-sensitive metadata — never the target URL. An empty `events` list
/// is the catch-all ("send everything").
#[derive(Debug, serde::Serialize)]
pub struct NotificationChannelRow {
    pub name: String,
    pub kind: String,
    pub events: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

impl NotificationChannelRow {
    fn from_row(r: PgRow) -> Self {
        let events = r
            .try_get::<serde_json::Value, _>("events")
            .ok()
            .and_then(|v| v.as_array().cloned())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            name: r.try_get("name").unwrap_or_default(),
            kind: r.try_get("kind").unwrap_or_default(),
            events,
            updated_at: r.try_get("updated_at").unwrap_or_else(|_| Utc::now()),
        }
    }
}

/// A `ui_layouts` list entry exposed via GET /ui/layouts — the name + when it
/// was last saved (the full layout blob is fetched per-name via GET
/// /ui/layouts/{name}, keeping the list light).
#[derive(Debug, serde::Serialize)]
pub struct UiLayoutSummaryRow {
    pub name: String,
    pub updated_at: String,
}

/// A row from `bot_configs` exposed via GET /configs. Resource limits + env are
/// unpacked from the row's JSONB `config_json`.
#[derive(Debug, serde::Serialize)]
pub struct BotConfigRow {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub mode: String,
    pub cpu_limit: Option<f64>,
    pub memory_mb: Option<i32>,
    pub env: HashMap<String, String>,
    /// Exchanges whose stored credentials the template injects at spawn time.
    pub secrets: Vec<String>,
}

impl BotConfigRow {
    fn from_row(r: PgRow) -> Self {
        let cfg: serde_json::Value = r.try_get("config_json").unwrap_or(serde_json::Value::Null);
        let cpu_limit = cfg.get("cpu_limit").and_then(serde_json::Value::as_f64);
        let memory_mb = cfg
            .get("memory_mb")
            .and_then(serde_json::Value::as_i64)
            .map(|n| n as i32);
        let env = cfg
            .get("env")
            .and_then(serde_json::Value::as_object)
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let secrets = cfg
            .get("secrets")
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            id: r.try_get("id").unwrap_or_else(|_| Uuid::nil()),
            name: r.try_get("name").unwrap_or_default(),
            image: r.try_get("image").unwrap_or_default(),
            mode: r.try_get("mode").unwrap_or_default(),
            cpu_limit,
            memory_mb,
            env,
            secrets,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn map_sqlx(e: sqlx::Error) -> SpawnerError {
    SpawnerError::Other(format!("postgres: {e}"))
}

/// Decode one `(ts, net_worth::float8)` snapshot row for `profit_inputs`.
fn snapshot_point(r: PgRow) -> (DateTime<Utc>, f64) {
    (
        r.try_get("ts").unwrap_or_else(|_| Utc::now()),
        r.try_get("net_worth").unwrap_or(0.0),
    )
}

/// Strip user:password from a postgres URL for safe logging.
/// `postgres://user:pass@host:5432/db` → `host:5432/db`
fn sanitize_url(url: &str) -> String {
    if let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) {
        if let Some((_, host)) = after_scheme.split_once('@') {
            return host.to_string();
        }
        return after_scheme.to_string();
    }
    "<unparseable>".to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{NET_WORTH_DEFAULT_LIMIT, NET_WORTH_MAX_LIMIT, net_worth_query_plan, sanitize_url};

    #[test]
    fn sanitize_strips_credentials() {
        assert_eq!(
            sanitize_url("postgres://fks_user:secret@postgres:5432/ruby_db"),
            "postgres:5432/ruby_db"
        );
    }

    #[test]
    fn sanitize_handles_no_creds() {
        assert_eq!(
            sanitize_url("postgres://postgres:5432/ruby_db"),
            "postgres:5432/ruby_db"
        );
    }

    #[test]
    fn sanitize_handles_garbage() {
        assert_eq!(sanitize_url("not-a-url"), "<unparseable>");
    }

    #[test]
    fn net_worth_query_plan_defaults_and_clamps_limit() {
        assert_eq!(net_worth_query_plan(None, None).1, NET_WORTH_DEFAULT_LIMIT);
        assert_eq!(net_worth_query_plan(None, Some(10)).1, 10);
        // Below the floor clamps up to 1; a silly-large limit clamps to the cap.
        assert_eq!(net_worth_query_plan(None, Some(0)).1, 1);
        assert_eq!(net_worth_query_plan(None, Some(-5)).1, 1);
        assert_eq!(
            net_worth_query_plan(None, Some(i64::MAX)).1,
            NET_WORTH_MAX_LIMIT
        );
    }

    #[test]
    fn net_worth_query_plan_normalises_bot_id_filter() {
        // Absent / blank / whitespace-only → no filter.
        assert_eq!(net_worth_query_plan(None, None).0, None);
        assert_eq!(net_worth_query_plan(Some(""), None).0, None);
        assert_eq!(net_worth_query_plan(Some("   "), None).0, None);
        // A real value is trimmed and kept.
        assert_eq!(
            net_worth_query_plan(Some("  eth-scalper "), None).0,
            Some("eth-scalper".to_string())
        );
    }
}
