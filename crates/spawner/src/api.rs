// =============================================================================
// api.rs — Axum HTTP router + handlers for FKS Bot Spawner
//
// Routes:
//   GET    /health                    → HealthResponse (JSON)
//   GET    /metrics                   → Prometheus text
//   GET    /containers                → Vec<ContainerInfo> (JSON)
//   GET    /container/{id}             → ContainerInfo (JSON)
//   POST   /spawn                     → SpawnResponse (JSON)
//   DELETE /container/{id}             → ActionResponse (JSON)
//   POST   /container/{id}/stop        → ActionResponse (JSON)
//   POST   /container/{id}/restart     → ActionResponse (JSON)
//   GET    /container/{id}/logs        → SSE stream (text/event-stream)
// =============================================================================

use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    response::{
        Json,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::{info, warn};

use crate::{
    auth::require_internal_token,
    config::Config,
    docker_client::DockerOps,
    error::SpawnerError,
    metrics,
    models::{ActionResponse, HealthResponse, SpawnRequest, SpawnResponse},
    prometheus_sd,
};

#[cfg(feature = "db")]
use crate::db::{BotRunStore, RecordSpawn};
#[cfg(feature = "db")]
use crate::models::{
    AccountRequest, ConfigRequest, LayoutRequest, NotificationChannelRequest, SecretRequest,
    TransferRequest,
};
#[cfg(feature = "db")]
use crate::notifications::{NotificationDispatcher, NotificationEvent, TestOutcome};
#[cfg(feature = "db")]
use crate::treasury::{
    decompose_profit, transfers_query_plan, validate_account, validate_transfer,
};

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    /// Backend driver — production uses `DockerClient` (talks to a real
    /// Docker daemon); tests inject `MockDockerClient` for handler-level
    /// integration tests without a daemon.
    pub docker: Arc<dyn DockerOps>,
    pub config: Arc<Config>,
    /// Optional Postgres-backed bot_runs persistence. None = stateless mode.
    #[cfg(feature = "db")]
    pub store: Option<BotRunStore>,
}

/// Fire a best-effort notification dispatch OFF the critical path.
///
/// Gated on `NOTIFY_ENABLED` (default true) and on the channel store being
/// configured (with no DB there are no channels to load). The dispatch itself
/// is best-effort — every webhook failure is logged + counted inside the
/// dispatcher and NEVER propagated, so this can never affect the lifecycle
/// response. `tokio::spawn` detaches it so we don't await it on the request.
#[cfg(feature = "db")]
fn spawn_dispatch(state: &AppState, ev: NotificationEvent) {
    if !state.config.notify_enabled {
        return;
    }
    let Some(store) = state.store.clone() else {
        return;
    };
    tokio::spawn(async move {
        NotificationDispatcher::new(store).dispatch(ev).await;
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Router
// ─────────────────────────────────────────────────────────────────────────────

/// Build the spawner's HTTP router.
///
/// Routes are split across two sub-routers:
///
/// - **Public** (`/health`, `/metrics`) — always reachable. Used by the
///   Docker healthcheck and by Prometheus scraping over the
///   `fks_network` Docker network.
/// - **Protected** (everything else) — wrapped in the
///   [`require_internal_token`] middleware. When
///   `Config.internal_token` is non-empty, requests must carry
///   `X-Internal-Token: <value>` (set by nginx). When empty, the
///   middleware is a no-op so direct local-dev requests still work.
pub fn build_router(state: AppState) -> Router {
    let public = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

    let protected = Router::new()
        .route("/spawn", post(spawn_handler))
        .route("/containers", get(list_containers_handler))
        .route("/container/{id}", get(inspect_handler))
        .route("/container/{id}", delete(remove_handler))
        .route("/container/{id}/stop", post(stop_handler))
        .route("/container/{id}/restart", post(restart_handler))
        .route("/container/{id}/logs", get(logs_sse_handler));

    #[cfg(feature = "db")]
    let protected = protected
        .route("/runs", get(runs_handler))
        .route("/net-worth", get(net_worth_handler))
        .route("/secrets", post(secrets_handler))
        .route("/secrets/status", get(secrets_status_handler))
        .route("/secrets/{exchange}", delete(delete_secret_handler))
        .route(
            "/notifications",
            get(list_notifications_handler).post(save_notification_handler),
        )
        .route("/notifications/{name}", delete(delete_notification_handler))
        .route(
            "/notifications/{name}/test",
            post(test_notification_handler),
        )
        .route(
            "/configs",
            get(list_configs_handler).post(save_config_handler),
        )
        .route("/configs/{name}", delete(delete_config_handler))
        .route(
            "/ui/layouts",
            get(list_layouts_handler).post(save_layout_handler),
        )
        .route(
            "/ui/layouts/{name}",
            get(get_layout_handler).delete(delete_layout_handler),
        )
        .route(
            "/transfers",
            get(list_transfers_handler).post(record_transfer_handler),
        )
        .route(
            "/accounts",
            get(list_accounts_handler).post(save_account_handler),
        )
        .route("/accounts/{id}", delete(delete_account_handler))
        .route("/profit", get(profit_handler));

    let protected = protected
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_internal_token,
        ))
        .with_state(state);

    Router::new().merge(public).merge(protected)
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /health
// ─────────────────────────────────────────────────────────────────────────────

async fn health_handler(
    State(state): State<AppState>,
) -> Result<Json<HealthResponse>, SpawnerError> {
    let bots = state.docker.list_bots().await?;
    let running = bots.iter().filter(|b| b.state == "running").count();

    metrics::RUNNING_BOTS.set(running as f64);

    Ok(Json(HealthResponse {
        status: "ok",
        service: "fks-bot-spawner",
        version: env!("CARGO_PKG_VERSION"),
        running_bots: running,
        max_bots: state.config.max_concurrent_bots,
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /metrics
// ─────────────────────────────────────────────────────────────────────────────

async fn metrics_handler() -> (StatusCode, String) {
    (StatusCode::OK, metrics::render())
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /spawn
// ─────────────────────────────────────────────────────────────────────────────

async fn spawn_handler(
    State(state): State<AppState>,
    Json(req): Json<SpawnRequest>,
) -> Result<(StatusCode, Json<SpawnResponse>), SpawnerError> {
    let t = Instant::now();
    let image_prefix = req
        .image
        .split(':')
        .next()
        .unwrap_or(&req.image)
        .to_string();

    // Best-effort context for a bot_error notification if the spawn fails
    // (captured before `req` is moved into the Docker call / secrets rebind).
    #[cfg(feature = "db")]
    let (bot_id_hint, image_hint, mode_hint) = (
        req.bot_id.clone().unwrap_or_default(),
        req.image.clone(),
        req.mode.clone(),
    );

    // ── Spawn-time secrets injection ────────────────────────────────────────
    // `secrets: ["kraken", …]` injects that exchange's stored credentials as
    // {EXCHANGE}_API_KEY / _API_SECRET (+ _API_PASSPHRASE when stored) — the
    // env names the crypto bots read. Fails the spawn loudly when the DB is
    // unavailable or an exchange has no stored credentials: a bot that asked
    // for keys must never silently start keyless. Explicit request `env`
    // entries win over injected ones (documented on SpawnRequest).
    #[cfg(not(feature = "db"))]
    if !req.secrets.is_empty() {
        return Err(SpawnerError::InvalidRequest(
            "secrets injection requires the db feature (stateless build)".to_string(),
        ));
    }
    #[cfg(feature = "db")]
    let req = {
        let mut req = req;
        if !req.secrets.is_empty() {
            if req.secrets.len() > 10 {
                return Err(SpawnerError::InvalidRequest(
                    "too many secrets requested (max 10 exchanges)".to_string(),
                ));
            }
            let Some(store) = state.store.as_ref() else {
                return Err(SpawnerError::InvalidRequest(
                    "secrets injection requires the spawner Postgres DB (not configured)"
                        .to_string(),
                ));
            };
            for exchange in &req.secrets {
                let ex = exchange.trim().to_lowercase();
                if ex.is_empty()
                    || ex.len() > 32
                    || !ex
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                {
                    return Err(SpawnerError::InvalidRequest(format!(
                        "invalid exchange name in secrets: '{exchange}'"
                    )));
                }
                let Some(creds) = store.get_secret(&ex).await? else {
                    return Err(SpawnerError::InvalidRequest(format!(
                        "no stored credentials for exchange '{ex}' — submit them via \
                         POST /secrets first"
                    )));
                };
                let prefix = ex.to_uppercase().replace('-', "_");
                req.env
                    .entry(format!("{prefix}_API_KEY"))
                    .or_insert(creds.api_key);
                req.env
                    .entry(format!("{prefix}_API_SECRET"))
                    .or_insert(creds.api_secret);
                if let Some(passphrase) = creds.api_passphrase {
                    req.env
                        .entry(format!("{prefix}_API_PASSPHRASE"))
                        .or_insert(passphrase);
                }
                // Log only the exchange — never the credential values.
                info!(exchange = %ex, "injecting stored exchange credentials into spawn env");
            }
        }
        req
    };

    let resp = match state.docker.spawn(req).await {
        Ok(resp) => resp,
        Err(e) => {
            metrics::SPAWN_ERRORS_TOTAL.inc();
            warn!(error = %e, "spawn failed");
            // A failed spawn is a bot_error event (best-effort, off-path).
            #[cfg(feature = "db")]
            spawn_dispatch(
                &state,
                NotificationEvent::error(&bot_id_hint, &image_hint, &mode_hint, &e.to_string()),
            );
            return Err(e);
        }
    };

    metrics::SPAWNS_TOTAL.inc();
    metrics::SPAWN_DURATION
        .with_label_values(&[&image_prefix])
        .observe(t.elapsed().as_secs_f64());

    info!(
        container_id = %resp.container_id,
        bot_id = %resp.bot_id,
        image = %resp.image,
        "bot spawned successfully"
    );

    // Persist to bot_runs (best-effort — never block the response on DB).
    #[cfg(feature = "db")]
    if let Some(store) = state.store.clone() {
        let args = OwnedSpawnRecord::from(&resp);
        tokio::spawn(async move {
            if let Err(e) = store
                .record_spawn(RecordSpawn {
                    container_id: &args.container_id,
                    container_name: &args.container_name,
                    image: &args.image,
                    mode: &args.mode,
                    started_at: args.started_at,
                })
                .await
            {
                warn!(error = %e, container_id = %args.container_id, "record_spawn failed");
            }
        });
    }

    // Notify configured channels of the successful spawn (best-effort, off-path).
    #[cfg(feature = "db")]
    spawn_dispatch(&state, NotificationEvent::spawned(&resp));

    // Update Prometheus SD file asynchronously — don't block the response.
    let docker = state.docker.clone();
    let config = state.config.clone();
    tokio::spawn(async move {
        prometheus_sd::update_sd_file(docker.as_ref(), &config).await;
    });

    Ok((StatusCode::CREATED, Json(resp)))
}

/// Owned snapshot of a `SpawnResponse` for use inside `tokio::spawn` futures
/// (the borrowed `RecordSpawn` can't outlive the handler).
#[cfg(feature = "db")]
struct OwnedSpawnRecord {
    container_id: String,
    container_name: String,
    image: String,
    mode: String,
    started_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(feature = "db")]
impl From<&SpawnResponse> for OwnedSpawnRecord {
    fn from(r: &SpawnResponse) -> Self {
        Self {
            container_id: r.container_id.clone(),
            container_name: r.container_name.clone(),
            image: r.image.clone(),
            mode: r.mode.clone(),
            started_at: r.started_at,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /containers
// ─────────────────────────────────────────────────────────────────────────────

async fn list_containers_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let mut bots = state.docker.list_bots().await?;
    let running = bots.iter().filter(|b| b.state == "running").count();
    metrics::RUNNING_BOTS.set(running as f64);

    // Enrich running containers with live CPU/memory — best-effort and
    // concurrent, each bounded by a short timeout so a slow stat can't stall the
    // listing. Failures simply leave cpu_percent/memory_bytes as None.
    // (Only this listing pays for stats; /health stays a cheap label query.)
    let stats = futures_util::future::join_all(bots.iter().map(|b| {
        let docker = state.docker.clone();
        let id = b.id_full.clone();
        let is_running = b.state == "running";
        async move {
            if !is_running {
                return None;
            }
            match tokio::time::timeout(Duration::from_secs(3), docker.stats(&id)).await {
                Ok(Ok(s)) => Some(s),
                _ => None,
            }
        }
    }))
    .await;
    for (b, s) in bots.iter_mut().zip(stats) {
        if let Some(s) = s {
            b.cpu_percent = s.cpu_percent;
            b.memory_bytes = s.memory_bytes;
            b.memory_limit_bytes = s.memory_limit_bytes;
        }
    }

    Ok(Json(serde_json::json!({
        "containers": bots,
        "total": bots.len(),
        "running": running,
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /container/{id}
// ─────────────────────────────────────────────────────────────────────────────

async fn inspect_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let info = state.docker.inspect(&id).await?;
    Ok(Json(serde_json::to_value(info)?))
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE /container/{id}
// ─────────────────────────────────────────────────────────────────────────────

async fn remove_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ActionResponse>, SpawnerError> {
    state.docker.remove(&id).await?;
    metrics::REMOVES_TOTAL.inc();

    #[cfg(feature = "db")]
    if let Some(store) = state.store.clone() {
        let id_owned = id.clone();
        tokio::spawn(async move {
            if let Err(e) = store.record_remove(&id_owned).await {
                warn!(error = %e, container_id = %id_owned, "record_remove failed");
            }
        });
    }

    // Notify configured channels of the removal (best-effort, off-path).
    #[cfg(feature = "db")]
    spawn_dispatch(&state, NotificationEvent::removed(&id));

    let docker = state.docker.clone();
    let config = state.config.clone();
    tokio::spawn(async move {
        prometheus_sd::update_sd_file(docker.as_ref(), &config).await;
    });

    Ok(Json(ActionResponse::ok(&id, "remove")))
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /container/{id}/stop
// ─────────────────────────────────────────────────────────────────────────────

async fn stop_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ActionResponse>, SpawnerError> {
    state.docker.stop(&id).await?;
    metrics::STOPS_TOTAL.inc();

    #[cfg(feature = "db")]
    if let Some(store) = state.store.clone() {
        let id_owned = id.clone();
        tokio::spawn(async move {
            if let Err(e) = store.record_stop(&id_owned).await {
                warn!(error = %e, container_id = %id_owned, "record_stop failed");
            }
        });
    }

    // Notify configured channels of the stop (best-effort, off-path).
    #[cfg(feature = "db")]
    spawn_dispatch(&state, NotificationEvent::stopped(&id));

    let docker = state.docker.clone();
    let config = state.config.clone();
    tokio::spawn(async move {
        prometheus_sd::update_sd_file(docker.as_ref(), &config).await;
    });

    Ok(Json(ActionResponse::ok(&id, "stop")))
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /container/{id}/restart
// ─────────────────────────────────────────────────────────────────────────────

async fn restart_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ActionResponse>, SpawnerError> {
    state.docker.restart(&id).await?;
    Ok(Json(ActionResponse::ok(&id, "restart")))
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /container/{id}/logs  → SSE
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LogsQuery {
    /// Number of tail lines to return before following. Default: 100.
    tail: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /runs  (db feature only) — recent bot_runs history
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[derive(Deserialize)]
struct RunsQuery {
    /// Max rows to return (clamped to 1..=500). Default: 50.
    limit: Option<i64>,
}

#[cfg(feature = "db")]
async fn runs_handler(
    State(state): State<AppState>,
    Query(params): Query<RunsQuery>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        // DB not configured — return an empty list so the WebUI degrades gracefully.
        return Ok(Json(serde_json::json!({
            "runs": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.recent_runs(params.limit.unwrap_or(50)).await?;
    Ok(Json(serde_json::json!({
        "runs": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /net-worth  (db feature only) — recent net_worth_snapshots history
//
// Returns a flat JSON array `[{bot_id, ts, net_worth, currency, venue}]`
// ordered by ts (oldest → newest within the window) so the WebUI can plot it
// directly. `?bot_id=` filters to one bot; `?limit=` bounds the most-recent
// rows returned (default 500, capped 5000). Without a database configured it
// degrades to `[]` so the panel just shows "no data" rather than erroring.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[derive(Deserialize)]
struct NetWorthQuery {
    /// Optional exact-match filter on the `fks.bot_id` label. Blank = all bots.
    bot_id: Option<String>,
    /// Max rows to return (clamped to 1..=5000). Default: 500.
    limit: Option<i64>,
}

#[cfg(feature = "db")]
async fn net_worth_handler(
    State(state): State<AppState>,
    Query(params): Query<NetWorthQuery>,
) -> Result<Json<Vec<crate::db::NetWorthSnapshotRow>>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        // DB not configured — empty history so the WebUI degrades gracefully.
        return Ok(Json(Vec::new()));
    };

    let (bot_id, limit) = crate::db::net_worth_query_plan(params.bot_id.as_deref(), params.limit);
    let rows = store.list_net_worth(bot_id.as_deref(), limit).await?;
    Ok(Json(rows))
}

// ─────────────────────────────────────────────────────────────────────────────
// Secrets  (db feature only) — exchange API credential storage
//
// SECURITY: the WebUI browser only ever SUBMITS credentials here; they are
// never returned. POST stores (UPSERT by exchange); GET /secrets/status reports
// only which exchanges are configured (never the key/secret material). With
// SPAWNER_SECRETS_KEY set, values are encrypted at rest (ChaCha20-Poly1305,
// see secrets_crypto.rs); unset falls back to the legacy plaintext storage.
// Every route here is additionally gated by X-Internal-Token.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
async fn secrets_handler(
    State(state): State<AppState>,
    Json(req): Json<SecretRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let exchange = req.exchange.trim().to_lowercase();
    let api_key = req.api_key.trim();
    let api_secret = req.api_secret.trim();
    let api_passphrase = req
        .api_passphrase
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if exchange.is_empty() || api_key.is_empty() || api_secret.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "exchange, api_key and api_secret are required",
            })),
        ));
    }

    let Some(store) = state.store.as_ref() else {
        // No Postgres configured — can't persist. Tell the caller honestly
        // instead of pretending the credentials were saved.
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "secret storage requires the spawner Postgres DB",
            })),
        ));
    };

    // AWAIT the write — unlike bot_runs (fire-and-forget via tokio::spawn) we
    // confirm the credential persisted before reporting success to the operator.
    store
        .upsert_secret(&exchange, api_key, api_secret, api_passphrase)
        .await?;

    // Log only the exchange — never the key or secret.
    info!(exchange = %exchange, "stored exchange API credentials");

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "exchange": exchange })),
    ))
}

#[cfg(feature = "db")]
async fn delete_secret_handler(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let exchange = exchange.trim().to_lowercase();
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(
            serde_json::json!({ "ok": false, "db_enabled": false }),
        ));
    };

    let removed = store.delete_secret(&exchange).await?;
    // Log only the exchange — never credentials.
    info!(exchange = %exchange, removed, "deleted exchange API credentials");
    Ok(Json(
        serde_json::json!({ "ok": removed, "exchange": exchange }),
    ))
}

#[cfg(feature = "db")]
async fn secrets_status_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        // DB not configured — empty list so the WebUI degrades gracefully.
        return Ok(Json(serde_json::json!({
            "exchanges": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.configured_exchanges().await?;
    Ok(Json(serde_json::json!({
        "exchanges": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Notification channels  (db feature only) — operator-configured webhooks
//
// SECURITY: the WebUI browser only ever SUBMITS a channel here; the target URL
// is never returned. POST stores (UPSERT by name); GET reports only
// name/kind/events (never the URL); DELETE removes one channel. The URL is
// encrypted at rest with the same cipher as exchange secrets (a Discord webhook
// URL is a bearer capability). Every route is additionally gated by
// X-Internal-Token.
//
// SENDER: the consumer half lives in `crate::notifications`. Lifecycle handlers
// fire a best-effort, off-critical-path `NotificationDispatcher` (gated on
// NOTIFY_ENABLED); POST /notifications/{name}/test sends a one-off probe to a
// single channel so the WebUI can verify a webhook end-to-end.
// ─────────────────────────────────────────────────────────────────────────────

/// Max length of a submitted webhook URL — generous but bounded to reject
/// obviously-bogus input before it hits the cipher / DB.
#[cfg(feature = "db")]
const MAX_WEBHOOK_URL_LEN: usize = 2048;

/// Known transport kinds. Only `discord_webhook` is meaningful today; the list
/// is the validation allowlist so a typo'd kind is a 400, not a silent store.
#[cfg(feature = "db")]
const KNOWN_CHANNEL_KINDS: &[&str] = &["discord_webhook"];

#[cfg(feature = "db")]
async fn save_notification_handler(
    State(state): State<AppState>,
    Json(req): Json<NotificationChannelRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let name = req.name.trim().to_string();
    let kind = req.kind.trim().to_lowercase();
    let url = req.url.trim();
    // Normalise events: trim, drop blanks, de-dup while preserving order. An
    // empty resulting list is the catch-all.
    let mut events: Vec<String> = Vec::new();
    for e in &req.events {
        let e = e.trim().to_string();
        if !e.is_empty() && !events.contains(&e) {
            events.push(e);
        }
    }

    if name.is_empty() || url.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "name and url are required",
            })),
        ));
    }
    if name.len() > 64 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "name too long (max 64 chars)",
            })),
        ));
    }
    if url.len() > MAX_WEBHOOK_URL_LEN
        || !(url.starts_with("https://") || url.starts_with("http://"))
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "url must be an http(s) URL under 2048 chars",
            })),
        ));
    }
    if !KNOWN_CHANNEL_KINDS.contains(&kind.as_str()) {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("unknown kind '{kind}' (supported: discord_webhook)"),
            })),
        ));
    }

    let Some(store) = state.store.as_ref() else {
        // No Postgres configured — can't persist. Tell the caller honestly
        // instead of pretending the channel was saved.
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "notification storage requires the spawner Postgres DB",
            })),
        ));
    };

    // AWAIT the write — confirm the channel persisted before reporting success.
    store.upsert_channel(&name, &kind, url, &events).await?;

    // Log only the name/kind — never the webhook URL.
    info!(name = %name, kind = %kind, "stored notification channel");

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "name": name, "kind": kind })),
    ))
}

#[cfg(feature = "db")]
async fn list_notifications_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        // DB not configured — empty list so the WebUI degrades gracefully.
        return Ok(Json(serde_json::json!({
            "channels": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.list_channels().await?;
    Ok(Json(serde_json::json!({
        "channels": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

#[cfg(feature = "db")]
async fn delete_notification_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let name = name.trim().to_string();
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(
            serde_json::json!({ "ok": false, "db_enabled": false }),
        ));
    };

    let removed = store.delete_channel(&name).await?;
    info!(name = %name, removed, "deleted notification channel");
    Ok(Json(serde_json::json!({ "ok": removed, "name": name })))
}

/// POST /notifications/{name}/test — send a one-off "connected" message to a
/// single channel and report whether the webhook accepted it. Lets the WebUI
/// "Test" button verify an operator's webhook end-to-end. Best-effort: a failed
/// delivery is a 200 with `ok:false` (a legit test result, not a server error);
/// a missing channel is a 404; no DB is a 503. NEVER logs the webhook URL.
#[cfg(feature = "db")]
async fn test_notification_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let name = name.trim().to_string();

    let Some(store) = state.store.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "notification test requires the spawner Postgres DB",
            })),
        );
    };

    let outcome = NotificationDispatcher::new(store).send_test(&name).await;
    match outcome {
        TestOutcome::Delivered => {
            info!(name = %name, "notification test delivered");
            (
                StatusCode::OK,
                Json(serde_json::json!({ "ok": true, "name": name })),
            )
        }
        TestOutcome::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "ok": false,
                "name": name,
                "error": "no notification channel with that name",
            })),
        ),
        TestOutcome::HttpStatus(code) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": false,
                "name": name,
                "status": code,
                "error": "webhook returned a non-2xx status",
            })),
        ),
        TestOutcome::Failed(reason) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": false,
                "name": name,
                "error": reason,
            })),
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved spawn configs  (db feature only) — reusable named spawn templates
//
// Persisted in `bot_configs`. The browser saves the spawn form as a named
// config (POST), lists them (GET), and removes them (DELETE, soft). The actual
// image-prefix / concurrency guards still apply at /spawn time.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
async fn save_config_handler(
    State(state): State<AppState>,
    Json(req): Json<ConfigRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    if req.name.trim().is_empty() || req.image.trim().is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "name and image are required",
            })),
        ));
    }

    let Some(store) = state.store.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "config storage requires the spawner Postgres DB",
            })),
        ));
    };

    let id = store.upsert_config(&req).await?;
    info!(name = %req.name, "saved bot config");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "id": id, "name": req.name })),
    ))
}

#[cfg(feature = "db")]
async fn list_configs_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(serde_json::json!({
            "configs": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.list_configs().await?;
    Ok(Json(serde_json::json!({
        "configs": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

#[cfg(feature = "db")]
async fn delete_config_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(
            serde_json::json!({ "ok": false, "db_enabled": false }),
        ));
    };

    let removed = store.deactivate_config(&name).await?;
    Ok(Json(serde_json::json!({ "ok": removed, "name": name })))
}

// ── ui_layouts: named WebUI dock layouts (see src/sql/spawner/005_ui_layouts.sql).
// Plaintext (a layout carries no secrets); returned by GET so arrangements can
// follow the operator across devices. ──────────────────────────────────────────

/// POST /ui/layouts — create/UPSERT a named dock layout.
#[cfg(feature = "db")]
async fn save_layout_handler(
    State(state): State<AppState>,
    Json(req): Json<LayoutRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let name = req.name.trim();
    if name.is_empty() || name.len() > 120 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "name is required (1–120 chars)",
            })),
        ));
    }
    if !req.layout.is_object() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "layout must be a JSON object",
            })),
        ));
    }

    let Some(store) = state.store.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "layout storage requires the spawner Postgres DB",
            })),
        ));
    };

    let created = store.upsert_layout(name, &req.layout).await?;
    info!(name, created, "saved ui layout");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "name": name, "created": created })),
    ))
}

/// GET /ui/layouts — list saved layout names + last-updated (not the blobs).
#[cfg(feature = "db")]
async fn list_layouts_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(serde_json::json!({
            "layouts": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.list_layouts().await?;
    Ok(Json(serde_json::json!({
        "layouts": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

/// GET /ui/layouts/{name} — fetch one full layout envelope.
#[cfg(feature = "db")]
async fn get_layout_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ok": false, "db_enabled": false })),
        ));
    };

    match store.get_layout(&name).await? {
        Some(layout) => Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "name": name, "layout": layout })),
        )),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "ok": false, "name": name, "error": "no such layout" })),
        )),
    }
}

/// DELETE /ui/layouts/{name} — remove one saved layout.
#[cfg(feature = "db")]
async fn delete_layout_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(
            serde_json::json!({ "ok": false, "db_enabled": false }),
        ));
    };

    let removed = store.delete_layout(&name).await?;
    Ok(Json(serde_json::json!({ "ok": removed, "name": name })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Treasury  (db feature only) — transfers ledger + accounts registry + /profit
//
// WHY: net_worth_snapshots shows drift, but drift conflates deposits with
// trading profit (the operator DCAs cash in regularly; the spot bot's own
// status code documents "later deposits show up as PnL"). POST /transfers
// records the signed external cash flows; GET /profit joins them against the
// snapshots to report what an account actually EARNED. The accounts registry
// is the multi-account topology's source of truth (NO credentials — those
// live in exchange_secrets). Schema: src/sql/spawner/007_treasury.sql. Pure
// validation/arithmetic lives in crate::treasury (unit-tested); these
// handlers just wire it to the store.
// ─────────────────────────────────────────────────────────────────────────────

/// POST /transfers — append one signed cash-flow row (positive = deposit in,
/// negative = withdrawal out). Validation (allowlisted kind/source, finite
/// non-zero amount) precedes the store check, so a bad row is a 400 with or
/// without a database. The write is AWAITED (like /secrets) — success means
/// the ledger row persisted, since a silently dropped deposit would corrupt
/// every later profit decomposition.
#[cfg(feature = "db")]
async fn record_transfer_handler(
    State(state): State<AppState>,
    Json(req): Json<TransferRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let transfer = match validate_transfer(&req) {
        Ok(t) => t,
        Err(error) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "ok": false, "error": error })),
            ));
        }
    };

    let Some(store) = state.store.as_ref() else {
        // No Postgres configured — can't persist. Tell the caller honestly
        // instead of pretending the ledger row was recorded.
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "the transfers ledger requires the spawner Postgres DB",
            })),
        ));
    };

    let id = store.insert_transfer(&transfer).await?;
    info!(
        id,
        account_id = %transfer.account_id,
        kind = %transfer.kind,
        source = %transfer.source,
        "transfer recorded"
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ok": true,
            "id": id,
            "account_id": transfer.account_id,
            "kind": transfer.kind,
            "amount": transfer.amount,
        })),
    ))
}

/// Query params for GET /transfers (mirrors /net-worth).
#[cfg(feature = "db")]
#[derive(Deserialize)]
struct TransfersQuery {
    /// Optional exact-match filter on account_id. Blank = all accounts.
    account_id: Option<String>,
    /// Max rows to return (clamped to 1..=5000). Default: 500.
    limit: Option<i64>,
}

/// GET /transfers — a window of the ledger as a flat JSON array
/// `[{id, account_id, ts, amount, currency, kind, source, note}]`, ordered
/// oldest → newest (like /net-worth) so the WebUI renders it directly.
/// Degrades to `[]` without a database.
#[cfg(feature = "db")]
async fn list_transfers_handler(
    State(state): State<AppState>,
    Query(params): Query<TransfersQuery>,
) -> Result<Json<Vec<crate::db::TransferRow>>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        // DB not configured — empty ledger so the WebUI degrades gracefully.
        return Ok(Json(Vec::new()));
    };

    let (account_id, limit) = transfers_query_plan(params.account_id.as_deref(), params.limit);
    let rows = store.list_transfers(account_id.as_deref(), limit).await?;
    Ok(Json(rows))
}

/// GET /accounts — the registry, active accounts first.
#[cfg(feature = "db")]
async fn list_accounts_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(serde_json::json!({
            "accounts": [],
            "total": 0,
            "db_enabled": false,
        })));
    };

    let rows = store.list_accounts().await?;
    Ok(Json(serde_json::json!({
        "accounts": rows,
        "total": rows.len(),
        "db_enabled": true,
    })))
}

/// POST /accounts — create/UPSERT one registry account by account_id (the
/// /configs UPSERT pattern). Validation (tier range, role/compliance
/// allowlists) precedes the store check. Carries NO credentials by design.
#[cfg(feature = "db")]
async fn save_account_handler(
    State(state): State<AppState>,
    Json(req): Json<AccountRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let account_id = match validate_account(&req) {
        Ok(id) => id,
        Err(error) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "ok": false, "error": error })),
            ));
        }
    };

    let Some(store) = state.store.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "db_enabled": false,
                "error": "the account registry requires the spawner Postgres DB",
            })),
        ));
    };

    let created = store.upsert_account(&req).await?;
    info!(account_id = %account_id, created, tier = req.tier, "account registered");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "account_id": account_id, "created": created })),
    ))
}

/// DELETE /accounts/{id} — soft-delete (active = FALSE). The account's
/// history (transfers / net-worth rows keyed by its id) is never dropped.
#[cfg(feature = "db")]
async fn delete_account_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, SpawnerError> {
    let id = id.trim().to_string();
    let Some(store) = state.store.as_ref() else {
        return Ok(Json(
            serde_json::json!({ "ok": false, "db_enabled": false }),
        ));
    };

    let removed = store.deactivate_account(&id).await?;
    info!(account_id = %id, removed, "account deactivated");
    Ok(Json(serde_json::json!({ "ok": removed, "account_id": id })))
}

/// Query params for GET /profit.
#[cfg(feature = "db")]
#[derive(Deserialize)]
struct ProfitQuery {
    /// Required: which account to decompose.
    account_id: Option<String>,
    /// Optional RFC3339 window start. Omitted = the account's full history.
    since: Option<String>,
}

/// Build the null-figure /profit envelope (no DB, or no snapshots in range).
#[cfg(feature = "db")]
fn profit_empty_body(
    account_id: &str,
    since: Option<chrono::DateTime<chrono::Utc>>,
    db_enabled: bool,
) -> serde_json::Value {
    serde_json::json!({
        "account_id": account_id,
        "since": since,
        "db_enabled": db_enabled,
        "start_ts": null,
        "end_ts": null,
        "start_net_worth": null,
        "end_net_worth": null,
        "delta": null,
        "deposits_in": null,
        "withdrawals_out": null,
        "net_inflows": null,
        "profit": null,
        "transfers": 0,
    })
}

/// GET /profit — decompose one account's net-worth drift into deposits vs
/// trading profit over `?since=`..now.
///
/// The window is bounded by the FIRST and LAST net_worth_snapshots rows in
/// range (snapshots key on bot_id, which is the account id for bot-traded
/// accounts); the transfers strictly between those snapshots explain the
/// external cash flows, and `profit = delta − net_inflows` is what the
/// account actually earned (see `treasury::decompose_profit`). With no
/// database, or no snapshots in the window, the figures come back null —
/// never an invented zero baseline.
#[cfg(feature = "db")]
async fn profit_handler(
    State(state): State<AppState>,
    Query(params): Query<ProfitQuery>,
) -> Result<(StatusCode, Json<serde_json::Value>), SpawnerError> {
    let account_id = params
        .account_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(account_id) = account_id else {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "account_id is required",
            })),
        ));
    };

    let since = match params
        .since
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        None => None,
        Some(raw) => match chrono::DateTime::parse_from_rfc3339(raw) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(_) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "ok": false,
                        "error": "since must be an RFC3339 timestamp (e.g. 2026-01-01T00:00:00Z)",
                    })),
                ));
            }
        },
    };

    let Some(store) = state.store.as_ref() else {
        // DB not configured — null figures so the WebUI degrades gracefully.
        return Ok((
            StatusCode::OK,
            Json(profit_empty_body(account_id, since, false)),
        ));
    };

    let inputs = store.profit_inputs(account_id, since).await?;
    let body = match (inputs.start, inputs.end) {
        (Some((start_ts, start_nw)), Some((end_ts, end_nw))) => {
            let d = decompose_profit(start_nw, end_nw, &inputs.transfer_amounts);
            serde_json::json!({
                "account_id": account_id,
                "since": since,
                "db_enabled": true,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_net_worth": d.start_net_worth,
                "end_net_worth": d.end_net_worth,
                "delta": d.delta,
                "deposits_in": d.deposits_in,
                "withdrawals_out": d.withdrawals_out,
                "net_inflows": d.net_inflows,
                "profit": d.profit,
                "transfers": inputs.transfer_amounts.len(),
            })
        }
        // No snapshots in the window — nothing to decompose.
        _ => profit_empty_body(account_id, since, true),
    };
    Ok((StatusCode::OK, Json(body)))
}

async fn logs_sse_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<LogsQuery>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let log_stream = state.docker.stream_logs(&id, params.tail);

    let sse_stream = log_stream
        .map(|line| Ok::<_, Infallible>(Event::default().event("log").data(line.trim_end())));

    Sse::new(sse_stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}
