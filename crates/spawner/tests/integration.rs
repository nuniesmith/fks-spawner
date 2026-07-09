//! HTTP integration tests for the spawner.
//!
//! Drives the real `axum::Router` from `spawner::api::build_router` with a
//! `MockDockerClient` that maintains in-memory container state. No real
//! Docker daemon required; tests run on every `cargo test` invocation.
//!
//! Coverage:
//!   - Health + metrics endpoints are reachable without auth.
//!   - Spawn rejects images that don't match the allowed prefix (400).
//!   - Spawn → list → inspect → stop → remove lifecycle round-trips.
//!   - The auth middleware:
//!       * dev mode (empty token) lets unauthenticated requests through
//!       * configured token rejects missing header (401)
//!       * configured token rejects mismatched header (403)
//!       * configured token allows correct header (2xx)
//!
//! These are *integration* tests in the cargo sense (they live in
//! `tests/`) but they don't talk to anything external — the entire stack
//! runs in-process via `tower::ServiceExt::oneshot`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::Utc;
use tokio_stream::Stream;
use tower::util::ServiceExt;

use spawner::api::{AppState, build_router};
use spawner::config::Config;
use spawner::docker_client::{DockerOps, LogStream};
use spawner::error::{SpawnerError, SpawnerResult};
use spawner::models::{ContainerInfo, ContainerStats, SpawnRequest, SpawnResponse};

// ─────────────────────────────────────────────────────────────────────────────
// MockDockerClient — in-memory implementation of DockerOps
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct MockDockerClient {
    state: Arc<Mutex<MockState>>,
    /// Optional override of `allowed_image_prefix` so the mock can fail on
    /// bad images the same way the real client does.
    allowed_prefix: String,
    /// Hard cap on concurrent containers, mirrored from `Config`.
    max_concurrent: usize,
}

#[derive(Default)]
struct MockState {
    containers: HashMap<String, ContainerInfo>,
}

impl MockDockerClient {
    fn from_config(cfg: &Config) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState::default())),
            allowed_prefix: cfg.allowed_image_prefix.clone(),
            max_concurrent: cfg.max_concurrent_bots,
        }
    }
}

#[async_trait]
impl DockerOps for MockDockerClient {
    async fn spawn(&self, req: SpawnRequest) -> SpawnerResult<SpawnResponse> {
        if !req.image.starts_with(&self.allowed_prefix) {
            return Err(SpawnerError::InvalidImage(req.image));
        }

        let mut state = self.state.lock().expect("MockState mutex poisoned");
        if state.containers.len() >= self.max_concurrent {
            return Err(SpawnerError::TooManyBots(state.containers.len()));
        }

        let bot_id = req.bot_id.unwrap_or_else(|| "test-id".to_string());
        let container_id: String = format!("{:0>12}", bot_id.chars().take(12).collect::<String>());
        let container_name = format!("fks-bot-{}", bot_id);

        let mut labels: HashMap<String, String> = req.labels.clone();
        labels.insert("fks.bot".into(), "true".into());
        labels.insert("fks.bot_id".into(), bot_id.clone());
        labels.insert("fks.mode".into(), req.mode.clone());

        let now = Utc::now();
        let info = ContainerInfo {
            id: container_id.clone(),
            id_full: container_id.clone(),
            name: container_name.clone(),
            image: req.image.clone(),
            status: "Up 0 seconds".to_string(),
            state: "running".to_string(),
            bot_id: bot_id.clone(),
            mode: req.mode.clone(),
            created_at: Some(now),
            started_at: Some(now),
            finished_at: None,
            labels,
            cpu_percent: None,
            memory_bytes: None,
            memory_limit_bytes: None,
        };
        state.containers.insert(container_id.clone(), info);

        Ok(SpawnResponse {
            container_id: container_id.clone(),
            container_name,
            bot_id,
            image: req.image,
            mode: req.mode,
            started_at: now,
        })
    }

    async fn stop(&self, id: &str) -> SpawnerResult<()> {
        let mut state = self.state.lock().expect("MockState mutex poisoned");
        match state.containers.get_mut(id) {
            Some(c) => {
                c.state = "exited".to_string();
                c.finished_at = Some(Utc::now());
                Ok(())
            }
            None => Err(SpawnerError::NotFound(id.to_string())),
        }
    }

    async fn restart(&self, id: &str) -> SpawnerResult<()> {
        let mut state = self.state.lock().expect("MockState mutex poisoned");
        match state.containers.get_mut(id) {
            Some(c) => {
                c.state = "running".to_string();
                c.finished_at = None;
                Ok(())
            }
            None => Err(SpawnerError::NotFound(id.to_string())),
        }
    }

    async fn remove(&self, id: &str) -> SpawnerResult<()> {
        let mut state = self.state.lock().expect("MockState mutex poisoned");
        if state.containers.remove(id).is_some() {
            Ok(())
        } else {
            Err(SpawnerError::NotFound(id.to_string()))
        }
    }

    async fn inspect(&self, id: &str) -> SpawnerResult<ContainerInfo> {
        let state = self.state.lock().expect("MockState mutex poisoned");
        state
            .containers
            .get(id)
            .cloned()
            .ok_or_else(|| SpawnerError::NotFound(id.to_string()))
    }

    async fn list_bots(&self) -> SpawnerResult<Vec<ContainerInfo>> {
        let state = self.state.lock().expect("MockState mutex poisoned");
        Ok(state.containers.values().cloned().collect())
    }

    async fn stats(&self, id: &str) -> SpawnerResult<ContainerStats> {
        let state = self.state.lock().expect("MockState mutex poisoned");
        if state.containers.contains_key(id) {
            Ok(ContainerStats {
                cpu_percent: Some(12.5),
                memory_bytes: Some(64 * 1024 * 1024),
                memory_limit_bytes: Some(256 * 1024 * 1024),
            })
        } else {
            Err(SpawnerError::NotFound(id.to_string()))
        }
    }

    fn stream_logs(&self, _id: &str, _tail: Option<String>) -> LogStream {
        // Empty stream — log streaming isn't exercised by the lifecycle tests.
        let stream: Pin<Box<dyn Stream<Item = String> + Send>> = Box::pin(tokio_stream::empty());
        stream
    }

    async fn auto_prune(&self) -> SpawnerResult<usize> {
        Ok(0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test scaffolding
// ─────────────────────────────────────────────────────────────────────────────

fn test_config(internal_token: &str) -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port: 8090,
        allowed_image_prefix: "fks-bot-".to_string(),
        max_concurrent_bots: 20,
        allowed_network: "fks_network".to_string(),
        default_cpu_limit: 1.0,
        default_memory_bytes: 256 * 1024 * 1024,
        default_cpu_shares: 1024,
        max_cpu_limit: 8.0,
        max_memory_mb: 16384,
        prometheus_sd_path: "/tmp/spawner-test-sd.json".to_string(),
        bot_metrics_port: 9091,
        prune_after_secs: 300,
        prune_interval_secs: 60,
        net_worth_sample_interval_secs: 300,
        database_url: String::new(),
        internal_token: internal_token.to_string(),
        notify_enabled: true,
    }
}

fn build_app(config: Config) -> (Router, Arc<MockDockerClient>) {
    let mock = Arc::new(MockDockerClient::from_config(&config));
    let docker: Arc<dyn DockerOps> = mock.clone();
    let state = AppState {
        docker,
        config: Arc::new(config),
        #[cfg(feature = "db")]
        store: None,
    };
    (build_router(state), mock)
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("body is utf-8")
}

// ─────────────────────────────────────────────────────────────────────────────
// Public endpoints (no auth)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_returns_ok_without_auth() {
    let (app, _) = build_app(test_config("any-token-set-here"));
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"status\":\"ok\""), "body was: {body}");
}

#[tokio::test]
async fn metrics_returns_text_without_auth() {
    let (app, _) = build_app(test_config("any-token-set-here"));
    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Spawn validation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn spawn_rejects_image_outside_allowed_prefix() {
    let (app, _) = build_app(test_config(""));
    let body = serde_json::json!({ "image": "evil-image:latest" }).to_string();

    let resp = app
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let payload = body_string(resp).await;
    assert!(payload.contains("not allowed"), "body was: {payload}");
}

#[tokio::test]
async fn spawn_then_list_then_remove_round_trips() {
    let (app, mock) = build_app(test_config(""));

    // Initial list is empty.
    let resp = app
        .clone()
        .oneshot(Request::get("/containers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_string(resp).await.contains("\"total\":0"));

    // Spawn a container.
    let body = serde_json::json!({
        "image": "fks-bot-example:latest",
        "bot_id": "round-trip-id",
        "mode": "paper"
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let payload = body_string(resp).await;
    let resp_json: SpawnResponse = serde_json::from_str(&payload).unwrap();
    assert_eq!(resp_json.bot_id, "round-trip-id");
    assert_eq!(resp_json.mode, "paper");

    // Mock state has 1 container.
    {
        let state = mock.state.lock().unwrap();
        assert_eq!(state.containers.len(), 1);
    }

    // /containers now returns it.
    let resp = app
        .clone()
        .oneshot(Request::get("/containers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"total\":1"), "body: {payload}");
    assert!(payload.contains("round-trip-id"), "body: {payload}");

    // DELETE /container/:id removes it.
    let id = &resp_json.container_id;
    let resp = app
        .clone()
        .oneshot(
            Request::delete(format!("/container/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Mock state empty again.
    {
        let state = mock.state.lock().unwrap();
        assert!(
            state.containers.is_empty(),
            "state: {:?}",
            state.containers.keys()
        );
    }
}

#[tokio::test]
async fn containers_listing_includes_live_stats() {
    // The /containers listing enriches each running container with live CPU%
    // and memory from the stats path (mocked here to canned values).
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "image": "fks-bot-example:latest",
        "bot_id": "stats-bot",
        "mode": "paper"
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = app
        .oneshot(Request::get("/containers").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"cpu_percent\":12.5"), "body: {payload}");
    assert!(
        payload.contains("\"memory_bytes\":67108864"),
        "body: {payload}"
    );
    assert!(
        payload.contains("\"memory_limit_bytes\":268435456"),
        "body: {payload}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Restart / logs-SSE / runs  (previously uncovered — see crates/spawner/TODO.md)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restart_round_trips() {
    let (app, _) = build_app(test_config(""));

    // Spawn a container so there is something to restart.
    let body = serde_json::json!({
        "image": "fks-bot-example:latest",
        "bot_id": "rs-test",
        "mode": "paper"
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let spawned: SpawnResponse = serde_json::from_str(&body_string(resp).await).unwrap();

    // POST /container/{id}/restart succeeds and echoes the action.
    let id = &spawned.container_id;
    let resp = app
        .oneshot(
            Request::post(format!("/container/{id}/restart"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("restart"), "body was: {payload}");
}

#[tokio::test]
async fn logs_endpoint_returns_sse_stream() {
    let (app, _) = build_app(test_config(""));

    // The mock streams no log lines, but the endpoint must still answer with an
    // SSE response (status 200 + text/event-stream), not buffer or 404.
    let resp = app
        .oneshot(
            Request::get("/container/some-id/logs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let ctype = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ctype.starts_with("text/event-stream"),
        "expected SSE content-type, got: {ctype:?}"
    );
}

#[cfg(feature = "db")]
#[tokio::test]
async fn runs_degrades_gracefully_without_db() {
    // No DATABASE_URL configured (store: None) — /runs must degrade to an empty
    // list with `db_enabled: false` so the WebUI keeps working, not 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/runs").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn net_worth_degrades_gracefully_without_db() {
    // No DATABASE_URL configured (store: None) — /net-worth must degrade to an
    // empty JSON array (not 500), including when the ?bot_id=/?limit= filters
    // are supplied (exercises the Query extractor + query-plan wiring).
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/net-worth?bot_id=eth-scalper&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert_eq!(payload, "[]", "body: {payload}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Secrets endpoints (db feature) — graceful behaviour without a live Postgres
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn secrets_status_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /secrets/status must degrade to an
    // empty list with db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/secrets/status").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn secrets_post_without_db_returns_503() {
    // A well-formed credential submission with no DB configured must report an
    // honest 503 (storage unavailable), not a fake success.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "exchange": "kraken",
        "api_key": "pub-key",
        "api_secret": "priv-secret"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/secrets")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn secrets_post_rejects_missing_fields() {
    // Validation runs before the store check, so a blank secret is rejected
    // (400) regardless of whether Postgres is configured.
    let (app, _) = build_app(test_config(""));

    // Blank api_secret.
    let body = serde_json::json!({
        "exchange": "kraken",
        "api_key": "k",
        "api_secret": ""
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/secrets")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn secrets_delete_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — DELETE /secrets/{exchange} must degrade
    // to ok:false + db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::delete("/secrets/kraken")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"ok\":false"), "body: {payload}");
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Notification channels (db feature) — graceful behaviour without a live Postgres
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_list_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /notifications must degrade to an
    // empty list with db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/notifications").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
    assert!(payload.contains("\"channels\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_post_without_db_returns_503() {
    // A well-formed channel submission with no DB configured must report an
    // honest 503 (storage unavailable), not a fake success.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "ops-alerts",
        "kind": "discord_webhook",
        "url": "https://discord.com/api/webhooks/1/abc",
        "events": []
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/notifications")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_post_rejects_missing_url() {
    // Validation runs before the store check, so a blank url is rejected (400)
    // regardless of whether Postgres is configured.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "ops-alerts",
        "url": ""
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/notifications")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_post_rejects_non_http_url() {
    // A non-http(s) URL is rejected (400) before touching the store.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "ops-alerts",
        "url": "ftp://example.com/hook"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/notifications")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_post_rejects_unknown_kind() {
    // An unrecognised kind is a 400 (allowlist), not a silent store.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "ops-alerts",
        "kind": "carrier_pigeon",
        "url": "https://discord.com/api/webhooks/1/abc"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/notifications")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_delete_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — DELETE /notifications/{name} must degrade
    // to ok:false + db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::delete("/notifications/ops-alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"ok\":false"), "body: {payload}");
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_test_route_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — POST /notifications/{name}/test must
    // report an honest 503 (storage unavailable), never 500, and must never
    // attempt a webhook POST.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::post("/notifications/ops-alerts/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"ok\":false"), "body: {payload}");
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_test_route_is_token_gated() {
    // With a configured token, the test route rejects an unauthenticated
    // request (401) before touching the store — same gate as the other
    // /notifications routes.
    let (app, _) = build_app(test_config("s3cr3t"));

    let resp = app
        .oneshot(
            Request::post("/notifications/ops-alerts/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved spawn configs (db feature) — graceful behaviour without a live Postgres
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn configs_list_degrades_without_db() {
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/configs").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn config_save_without_db_returns_503() {
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "demo-paper",
        "image": "fks-bot-crypto-demo:latest",
        "mode": "paper"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/configs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn config_save_rejects_missing_name() {
    let (app, _) = build_app(test_config(""));

    // Blank name → 400 (validation precedes the store check).
    let body = serde_json::json!({ "name": "", "image": "fks-bot-x:latest" }).to_string();

    let resp = app
        .oneshot(
            Request::post("/configs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ─────────────────────────────────────────────────────────────────────────────
// Net-worth sampler — discovery/target-building over the DockerOps trait
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn net_worth_sampler_targets_only_running_bots() {
    // The sampler discovers who to poll via DockerOps::list_bots (mocked here)
    // and builds `http://<container_name>:<port>/status` for each RUNNING bot.
    // Stopped bots must not be polled. This exercises the same discovery half
    // the real sampler uses, without needing a bot HTTP server.
    use spawner::docker_client::DockerOps;
    use spawner::models::SpawnRequest;
    use spawner::net_worth::running_status_targets;

    let config = test_config("");
    let port = config.bot_metrics_port;
    let mock = MockDockerClient::from_config(&config);

    // Two running bots …
    for bot_id in ["alpha", "beta"] {
        mock.spawn(SpawnRequest {
            image: "fks-bot-example:latest".to_string(),
            bot_id: Some(bot_id.to_string()),
            mode: "paper".to_string(),
            env: HashMap::new(),
            labels: HashMap::new(),
            cpu_limit: None,
            memory_limit_mb: None,
            cmd: None,
            entrypoint: None,
            secrets: vec![],
        })
        .await
        .expect("spawn");
    }

    // … then stop one (state → exited).
    let beta_id = format!("{:0>12}", "beta");
    mock.stop(&beta_id).await.expect("stop");

    let bots = mock.list_bots().await.expect("list_bots");
    let mut targets = running_status_targets(&bots, port);
    targets.sort();

    assert_eq!(
        targets.len(),
        1,
        "only the running bot is a target: {targets:?}"
    );
    assert_eq!(targets[0].0, "alpha");
    assert_eq!(targets[0].1, format!("http://fks-bot-alpha:{port}/status"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Auth middleware
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn auth_disabled_when_token_empty() {
    let (app, _) = build_app(test_config("")); // empty = dev mode
    let body = serde_json::json!({ "image": "fks-bot-x:latest" }).to_string();

    let resp = app
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    // No auth required, so the request succeeds.
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn auth_rejects_missing_token_header() {
    let (app, _) = build_app(test_config("super-secret"));

    let resp = app
        .oneshot(Request::get("/containers").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let payload = body_string(resp).await;
    assert!(payload.contains("missing"), "body: {payload}");
}

#[tokio::test]
async fn auth_rejects_wrong_token_header() {
    let (app, _) = build_app(test_config("super-secret"));

    let resp = app
        .oneshot(
            Request::get("/containers")
                .header("X-Internal-Token", "wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn auth_accepts_correct_token_header() {
    let (app, _) = build_app(test_config("super-secret"));

    let resp = app
        .oneshot(
            Request::get("/containers")
                .header("X-Internal-Token", "super-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn auth_does_not_apply_to_health_even_when_enabled() {
    let (app, _) = build_app(test_config("super-secret"));

    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn auth_does_not_apply_to_metrics_even_when_enabled() {
    let (app, _) = build_app(test_config("super-secret"));

    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

// ─────────────────────────────────────────────────────────────────────────────
// Saved dock layouts (db feature) — graceful behaviour without a live Postgres
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_list_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /ui/layouts must degrade to an empty
    // list with db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/ui/layouts").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
    assert!(payload.contains("\"layouts\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_post_without_db_returns_503() {
    // A well-formed layout with no DB configured reports an honest 503, not a
    // fake success.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "name": "trading-desk",
        "layout": { "grid": {}, "panels": {} }
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/ui/layouts")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_post_rejects_missing_name() {
    // Validation runs before the store check: a blank name is a 400 regardless
    // of DB availability.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({ "name": "", "layout": { "grid": {} } }).to_string();

    let resp = app
        .oneshot(
            Request::post("/ui/layouts")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_post_rejects_non_object_layout() {
    // The layout must be a JSON object (a dockview envelope), not an array or
    // scalar — rejected 400 before the store.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({ "name": "x", "layout": [1, 2, 3] }).to_string();

    let resp = app
        .oneshot(
            Request::post("/ui/layouts")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_get_one_without_db_returns_503() {
    // GET /ui/layouts/{name} with no DB is an honest 503, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/ui/layouts/trading-desk")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_delete_degrades_gracefully_without_db() {
    // DELETE /ui/layouts/{name} with no DB degrades to ok:false + db_enabled:false.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::delete("/ui/layouts/trading-desk")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"ok\":false"), "body: {payload}");
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn layouts_routes_are_token_gated() {
    // With a configured token, the layout routes reject unauthenticated
    // requests (401) before touching the store — same gate as the rest.
    let (app, _) = build_app(test_config("s3cr3t"));

    let resp = app
        .oneshot(Request::get("/ui/layouts").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
