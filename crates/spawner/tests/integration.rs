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

/// A forced fault the mock injects on the NEXT `spawn`, so tests can drive the
/// respawn abort branches without a real Docker daemon. Variants are only
/// constructed by the `db`-gated respawn tests.
#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "db"), allow(dead_code))]
enum SpawnFault {
    /// Docker's 409 "container name already in use" — `is_name_conflict()` true.
    NameConflict,
    /// Any other Docker failure (500) — propagated raw, not wrapped as an abort.
    Generic,
}

impl SpawnFault {
    fn into_error(self) -> SpawnerError {
        let (status_code, message) = match self {
            SpawnFault::NameConflict => (
                409,
                "Conflict. The container name \"/fks-bot-x\" is already in use".to_string(),
            ),
            SpawnFault::Generic => (500, "kaboom".to_string()),
        };
        SpawnerError::Docker(bollard::errors::Error::DockerResponseServerError {
            status_code,
            message,
        })
    }
}

#[derive(Clone, Default)]
struct MockDockerClient {
    state: Arc<Mutex<MockState>>,
    /// Optional override of `allowed_image_prefix` so the mock can fail on
    /// bad images the same way the real client does.
    allowed_prefix: String,
    /// Hard cap on concurrent containers, mirrored from `Config`.
    max_concurrent: usize,
    /// When set, the NEXT `spawn` returns this fault instead of creating a
    /// container (drives the respawn abort branches). One-shot: cleared on use.
    spawn_fault: Arc<Mutex<Option<SpawnFault>>>,
}

impl MockDockerClient {
    /// Arm a one-shot spawn fault for the next `spawn` call.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    fn fail_next_spawn(&self, fault: SpawnFault) {
        *self.spawn_fault.lock().expect("fault mutex") = Some(fault);
    }
}

#[derive(Default)]
struct MockState {
    containers: HashMap<String, ContainerInfo>,
}

impl MockState {
    /// Resolve a caller-supplied identifier (container id OR `fks-bot-{bot_id}`
    /// name) to the map key, mirroring real Docker, which accepts both. The
    /// respawn path inspects/stops/removes by NAME, so the mock must honour it.
    fn resolve_key(&self, ident: &str) -> Option<String> {
        if self.containers.contains_key(ident) {
            return Some(ident.to_string());
        }
        self.containers
            .iter()
            .find(|(_, c)| c.name == ident)
            .map(|(k, _)| k.clone())
    }
}

impl MockDockerClient {
    fn from_config(cfg: &Config) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState::default())),
            allowed_prefix: cfg.allowed_image_prefix.clone(),
            max_concurrent: cfg.max_concurrent_bots,
            spawn_fault: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl DockerOps for MockDockerClient {
    async fn spawn(&self, req: SpawnRequest) -> SpawnerResult<SpawnResponse> {
        // A one-shot armed fault fires here (after any real daemon would have
        // reached create/start), so the respawn abort branches are exercised
        // with the old container already removed.
        if let Some(fault) = self.spawn_fault.lock().expect("fault mutex").take() {
            return Err(fault.into_error());
        }
        if !req.image.starts_with(&self.allowed_prefix) {
            return Err(SpawnerError::InvalidImage(req.image));
        }

        let mut state = self.state.lock().expect("MockState mutex poisoned");
        // Mirror the real client's cap rule: only RUNNING containers occupy
        // slots (exited one-shots awaiting prune must not wedge the cap).
        let running = state
            .containers
            .values()
            .filter(|c| c.state == "running")
            .count();
        if running >= self.max_concurrent {
            return Err(SpawnerError::TooManyBots(running));
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
            exit_code: None,
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
        match state.resolve_key(id) {
            Some(key) => {
                let c = state
                    .containers
                    .get_mut(&key)
                    .expect("resolved key present");
                c.state = "exited".to_string();
                c.finished_at = Some(Utc::now());
                Ok(())
            }
            None => Err(SpawnerError::NotFound(id.to_string())),
        }
    }

    async fn restart(&self, id: &str) -> SpawnerResult<()> {
        let mut state = self.state.lock().expect("MockState mutex poisoned");
        match state.resolve_key(id) {
            Some(key) => {
                let c = state
                    .containers
                    .get_mut(&key)
                    .expect("resolved key present");
                c.state = "running".to_string();
                c.finished_at = None;
                Ok(())
            }
            None => Err(SpawnerError::NotFound(id.to_string())),
        }
    }

    async fn remove(&self, id: &str) -> SpawnerResult<()> {
        let mut state = self.state.lock().expect("MockState mutex poisoned");
        match state.resolve_key(id) {
            Some(key) => {
                state.containers.remove(&key);
                Ok(())
            }
            None => Err(SpawnerError::NotFound(id.to_string())),
        }
    }

    async fn inspect(&self, id: &str) -> SpawnerResult<ContainerInfo> {
        let state = self.state.lock().expect("MockState mutex poisoned");
        state
            .resolve_key(id)
            .and_then(|key| state.containers.get(&key).cloned())
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
        prune_live_after_secs: 604_800,
        prune_interval_secs: 60,
        net_worth_sample_interval_secs: 300,
        net_worth_milestone_step: 0.0,
        database_url: String::new(),
        backtest_database_url: String::new(),
        internal_token: internal_token.to_string(),
        require_internal_auth: false,
        events_token: String::new(),
        events_url: "http://fks_bot_spawner:8090/events".to_string(),
        notify_enabled: true,
        btc_watch: spawner::btc_watch::BtcWatchConfig::default(),
        rithmic_sampler: spawner::rithmic_sampler::RithmicSamplerConfig::default(),
        edge_decay: spawner::edge_decay::EdgeDecayConfig::default(),
    }
}

fn build_app(config: Config) -> (Router, Arc<MockDockerClient>) {
    let (state, mock) = build_state(config);
    (build_router(state), mock)
}

/// Build an `AppState` (with the mock docker + no store) without wrapping it in
/// a router — for handler-helper tests that call into `spawner::api` directly.
fn build_state(config: Config) -> (AppState, Arc<MockDockerClient>) {
    let mock = Arc::new(MockDockerClient::from_config(&config));
    let docker: Arc<dyn DockerOps> = mock.clone();
    let state = AppState {
        docker,
        config: Arc::new(config),
        #[cfg(feature = "db")]
        store: None,
    };
    (state, mock)
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
// Concurrency cap — only RUNNING containers occupy MAX_CONCURRENT_BOTS slots
// ─────────────────────────────────────────────────────────────────────────────

/// JSON body for a minimal paper spawn of `bot_id`.
fn spawn_body(bot_id: &str) -> String {
    serde_json::json!({
        "image": "fks-bot-example:latest",
        "bot_id": bot_id,
        "mode": "paper"
    })
    .to_string()
}

async fn post_spawn(app: &Router, bot_id: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::post("/spawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(spawn_body(bot_id)))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn spawn_cap_counts_only_running_containers() {
    // The audit regression: the cap counted EXITED containers (list_bots uses
    // .all(true)), so accumulated one-shot backtest containers wedged ALL
    // spawns until auto_prune caught up. Exited containers must free their
    // slot immediately.
    let mut config = test_config("");
    config.max_concurrent_bots = 1;
    let (app, _) = build_app(config);

    // Fill the single slot.
    let resp = post_spawn(&app, "cap-a").await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let a: SpawnResponse = serde_json::from_str(&body_string(resp).await).unwrap();

    // Cap full → refused with 429.
    let resp = post_spawn(&app, "cap-b").await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    // Stop the running bot (mock flips state → "exited"; the container still
    // exists and still carries fks.bot=true, exactly like a finished one-shot
    // backtest awaiting prune).
    let resp = app
        .clone()
        .oneshot(
            Request::post(format!("/container/{}/stop", a.container_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The exited container must NOT hold the slot.
    let resp = post_spawn(&app, "cap-b").await;
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "an exited container must not occupy a cap slot"
    );
}

#[cfg(feature = "db")]
#[tokio::test]
async fn backtest_cap_precheck_fires_before_any_ledger_write() {
    // POST /edges/{id}/backtest pre-checks the concurrency cap BEFORE it
    // would open a backtest_runs row (finding: rows were inserted first, so
    // cap-refused retries grew the ledger unbounded). With the cap full the
    // handler must 429 before ever reaching the store gate (which would be a
    // 503 here — no DB is configured, so reaching it proves no ledger write
    // could have happened).
    let mut config = test_config("");
    config.max_concurrent_bots = 1;
    let (app, _) = build_app(config);

    // Fill the single slot with a running bot.
    let resp = post_spawn(&app, "cap-hog").await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let hog: SpawnResponse = serde_json::from_str(&body_string(resp).await).unwrap();

    // Cap full → 429 from the pre-check, NOT the store gate's 503.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/edges/funding-reversion/backtest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "cap pre-check must refuse before any DB path is reached"
    );

    // Stop the hog (state → exited): the slot frees, and the SAME request now
    // falls through to the store gate (503, stateless build) — proving both
    // the running-only counting and the check ordering.
    let resp = app
        .clone()
        .oneshot(
            Request::post(format!("/container/{}/stop", hog.container_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(
            Request::post("/edges/funding-reversion/backtest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
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

#[cfg(feature = "db")]
#[tokio::test]
async fn manual_net_worth_post_without_db_returns_503() {
    // A well-formed manual snapshot with no DB configured must report an honest
    // 503 (storage unavailable), not a fake success — like /transfers, a
    // silently dropped snapshot would corrupt the net-worth series.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "account_id": "apex-payout",
        "net_worth": 48250.5,
        "venue": "apex"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/net-worth")
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
async fn manual_net_worth_post_rejects_blank_account_id() {
    // Validation runs before the store check: a blank account_id is a 400
    // regardless of DB availability.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({ "account_id": "   ", "net_worth": 100.0 }).to_string();

    let resp = app
        .oneshot(
            Request::post("/net-worth")
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
async fn manual_net_worth_post_is_token_gated() {
    // With a configured token, POST /net-worth rejects an unauthenticated
    // request (401) before touching validation/store — same gate as the rest.
    let (app, _) = build_app(test_config("s3cr3t"));

    let body = serde_json::json!({ "account_id": "apex", "net_worth": 1.0 }).to_string();

    let resp = app
        .oneshot(
            Request::post("/net-worth")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
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
// Notification history (db feature) — graceful behaviour + auth without a DB
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_history_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /notifications/history must return the
    // graceful shape {db_enabled:false, entries:[]}, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/notifications/history")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"entries\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn notifications_history_is_token_gated() {
    // With a configured token, the history route rejects an unauthenticated
    // request (401) before touching the store — the static /history segment
    // must be reachable (not swallowed by /notifications/{name}) and gated.
    let (app, _) = build_app(test_config("s3cr3t"));

    let resp = app
        .oneshot(
            Request::get("/notifications/history?limit=10&event=bot_crashed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// Generic event ingest POST /events (db feature) — allowlist + auth + caps
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn events_ingest_accepts_allowlisted_kind() {
    // A valid, allowlisted kind is ACCEPTED (202) even without a DB — dispatch
    // is best-effort/off-path and simply no-ops with no channels configured.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "event": "risk_halt",
        "bot_id": "crypto-spot-live",
        "mode": "live",
        "detail": "trade-cap tripped"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"ok\":true"), "body: {payload}");
    assert!(
        payload.contains("\"event\":\"risk_halt\""),
        "body: {payload}"
    );
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_ingest_rejects_unknown_kind() {
    // An arbitrary string must NOT mint a wire kind — 400, not 202. Even a real
    // spawner-minted kind that isn't ingestable (bot_spawned) is rejected here.
    let (app, _) = build_app(test_config(""));

    for bad in ["totally_made_up", "bot_spawned"] {
        let body = serde_json::json!({ "event": bad }).to_string();
        let resp = app
            .clone()
            .oneshot(
                Request::post("/events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "'{bad}' must be rejected"
        );
    }
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_ingest_is_token_gated() {
    // With a configured token, POST /events rejects an unauthenticated request
    // (401) before validating the body — the ingest is money-adjacent.
    let (app, _) = build_app(test_config("s3cr3t"));

    let body = serde_json::json!({ "event": "risk_halt" }).to_string();
    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scoped EVENTS_TOKEN dual-auth on POST /events (plan-03 D2)
//
// The ingest is the ONE route with a widened auth: it accepts EITHER the
// internal token OR a scoped EVENTS_TOKEN in the same `X-Internal-Token` header.
// FAIL-CLOSED: an unset EVENTS_TOKEN disables the scoped path (only the internal
// token works). BLAST RADIUS: the scoped token opens ONLY /events — never any
// other route. Convention (matches the rest of the suite): a missing header is
// 401, a present-but-wrong token is 403.
// ─────────────────────────────────────────────────────────────────────────────

/// Build a config with both an internal token and a scoped events token set.
#[cfg(feature = "db")]
fn config_with_events(internal: &str, events: &str) -> Config {
    let mut cfg = test_config(internal);
    cfg.events_token = events.to_string();
    cfg
}

#[cfg(feature = "db")]
fn events_body() -> Body {
    Body::from(serde_json::json!({ "event": "risk_halt" }).to_string())
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_accepts_internal_token() {
    // The internal token opens /events whether or not a scoped token is set.
    for cfg in [
        test_config("internal-tok"),
        config_with_events("internal-tok", "scoped-ev"),
    ] {
        let (app, _) = build_app(cfg);
        let resp = app
            .oneshot(
                Request::post("/events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Internal-Token", "internal-tok")
                    .body(events_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_accepts_scoped_token_when_set() {
    // The scoped EVENTS_TOKEN opens /events when configured — this is the whole
    // point of the widened auth (bots hold only this token).
    let (app, _) = build_app(config_with_events("internal-tok", "scoped-ev"));
    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .header("X-Internal-Token", "scoped-ev")
                .body(events_body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_rejects_scoped_token_when_events_token_unset() {
    // FAIL CLOSED: with EVENTS_TOKEN empty the scoped path is DISABLED — a token
    // that would have been the scoped one is just a wrong token now (403). An
    // unset token is never an open door.
    let (app, _) = build_app(test_config("internal-tok")); // events_token = ""
    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .header("X-Internal-Token", "scoped-ev")
                .body(events_body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_rejects_a_token_matching_neither() {
    // A token that is neither the internal nor the scoped one is rejected (403),
    // even though a scoped token IS configured.
    let (app, _) = build_app(config_with_events("internal-tok", "scoped-ev"));
    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .header("X-Internal-Token", "not-either-token")
                .body(events_body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn events_missing_header_with_scoped_token_set_is_401() {
    // Missing header is still 401 (not 403), even with a scoped token configured.
    let (app, _) = build_app(config_with_events("internal-tok", "scoped-ev"));
    let resp = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .body(events_body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// BLAST-RADIUS PIN: the scoped events token opens ONLY /events. The SAME token
/// that is ACCEPTED (202) on /events is REJECTED (403) on every other protected
/// route — proving a compromised bot holding it can reach nothing but the
/// events mailbox.
#[cfg(feature = "db")]
#[tokio::test]
async fn scoped_events_token_opens_only_the_events_route() {
    let cfg = config_with_events("internal-tok", "scoped-ev");

    // 1. Opens /events.
    let (app, _) = build_app(cfg.clone());
    let ok = app
        .oneshot(
            Request::post("/events")
                .header(header::CONTENT_TYPE, "application/json")
                .header("X-Internal-Token", "scoped-ev")
                .body(events_body())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        ok.status(),
        StatusCode::ACCEPTED,
        "scoped token must open /events"
    );

    // 2. Rejected on other protected routes — the blast-radius pin. Each of
    //    these accepts the INTERNAL token only; the scoped token is a stranger.
    for (method, uri) in [
        ("GET", "/containers"),
        ("POST", "/spawn"),
        ("GET", "/secrets/status"),
        ("GET", "/transfers"),
        ("GET", "/net-worth"),
    ] {
        let (app, _) = build_app(cfg.clone());
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header("X-Internal-Token", "scoped-ev")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "scoped events token must NOT open {method} {uri}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Spawn-env injection of the scoped ingest vars (plan-03 D2)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn events_env_injected_when_token_set() {
    let mut cfg = test_config("internal-tok");
    cfg.events_token = "scoped-ev".to_string();
    cfg.events_url = "http://fks_bot_spawner:8090/events".to_string();

    let mut env: HashMap<String, String> = HashMap::new();
    spawner::api::inject_events_env(&cfg, &mut env);

    assert_eq!(
        env.get("SPAWNER_EVENTS_URL").map(String::as_str),
        Some("http://fks_bot_spawner:8090/events")
    );
    assert_eq!(
        env.get("SPAWNER_EVENTS_TOKEN").map(String::as_str),
        Some("scoped-ev")
    );
}

#[test]
fn events_env_absent_when_token_empty() {
    let cfg = test_config("internal-tok"); // events_token = ""
    let mut env: HashMap<String, String> = HashMap::new();
    spawner::api::inject_events_env(&cfg, &mut env);
    assert!(
        env.is_empty(),
        "empty EVENTS_TOKEN must inject nothing (additive), got {env:?}"
    );
}

#[test]
fn operator_config_env_wins_over_injected_events_vars() {
    // Precedence: a value already in the stored config env is NEVER overwritten.
    let mut cfg = test_config("internal-tok");
    cfg.events_token = "scoped-ev".to_string();
    cfg.events_url = "http://fks_bot_spawner:8090/events".to_string();

    let mut env: HashMap<String, String> = HashMap::new();
    env.insert(
        "SPAWNER_EVENTS_URL".to_string(),
        "http://operator-override:9999/events".to_string(),
    );
    env.insert(
        "SPAWNER_EVENTS_TOKEN".to_string(),
        "operator-token".to_string(),
    );

    spawner::api::inject_events_env(&cfg, &mut env);

    assert_eq!(
        env.get("SPAWNER_EVENTS_URL").map(String::as_str),
        Some("http://operator-override:9999/events"),
        "operator-provided URL must win"
    );
    assert_eq!(
        env.get("SPAWNER_EVENTS_TOKEN").map(String::as_str),
        Some("operator-token"),
        "operator-provided token must win"
    );
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
// Atomic respawn (POST /configs/{name}/respawn) — cleanup helper + handler
// gates. The full happy path (load saved config → cleanup → re-spawn) needs a
// live Postgres for the config store and is covered by the db_integration
// harness; here we drive the safety-critical cleanup half against the mock and
// the no-DB / auth gates through the real router.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn remove_existing_bot_is_idempotent_when_absent() {
    // The respawn must work even if the bot was already stopped / never
    // started: no container named fks-bot-{id} → Ok(None), nothing torn down.
    use spawner::docker_client::remove_existing_bot;

    let config = test_config("");
    let mock = MockDockerClient::from_config(&config);

    let removed = remove_existing_bot(&mock, "fks-bot-never-existed")
        .await
        .expect("absent cleanup is not an error");
    assert!(removed.is_none(), "no container ⇒ None, not an error");
}

#[tokio::test]
async fn remove_existing_bot_stops_and_removes_when_present() {
    // A running container named fks-bot-{id} is stopped THEN force-removed, and
    // its id is returned. A second call is a clean no-op (idempotent) — proving
    // the remove actually completed (no lingering container to race a respawn).
    use spawner::docker_client::{DockerOps, remove_existing_bot};
    use spawner::models::SpawnRequest;

    let config = test_config("");
    let mock = MockDockerClient::from_config(&config);

    mock.spawn(SpawnRequest {
        image: "fks-bot-example:latest".to_string(),
        bot_id: Some("crypto-spot-live".to_string()),
        mode: "live".to_string(),
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

    // One container is live.
    assert_eq!(mock.list_bots().await.unwrap().len(), 1);

    let removed = remove_existing_bot(&mock, "fks-bot-crypto-spot-live")
        .await
        .expect("present cleanup succeeds")
        .expect("returns the removed container id");
    assert!(!removed.is_empty(), "removed container id echoed back");

    // The removal completed — no container left to double up with a fresh spawn.
    assert!(
        mock.list_bots().await.unwrap().is_empty(),
        "the old container must be gone before any respawn"
    );

    // Idempotent: a second cleanup finds nothing.
    let again = remove_existing_bot(&mock, "fks-bot-crypto-spot-live")
        .await
        .expect("second cleanup ok");
    assert!(again.is_none(), "second cleanup is a clean no-op");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_without_db_returns_503() {
    // No DATABASE_URL (store: None) — there is no config store to load from, so
    // the respawn reports an honest 503 rather than a blind spawn.
    let (app, mock) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::post("/configs/crypto-spot-live/respawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    // Crucially: nothing was spawned without a config to rebuild from.
    let state = mock.state.lock().unwrap();
    assert!(state.containers.is_empty(), "no container without a config");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_is_token_gated() {
    // With a configured token, the respawn route rejects an unauthenticated
    // request (401) before touching the store — same gate as the other
    // /configs routes.
    let (app, _) = build_app(test_config("s3cr3t"));

    let resp = app
        .oneshot(
            Request::post("/configs/crypto-spot-live/respawn")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_accepts_empty_body() {
    // A bare POST with NO body (the "use the config's own bot_id" case) must not
    // be a 400/415 for a missing JSON payload — it should reach the store gate
    // (503 here, no DB) exactly like an explicit `{}`.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::post("/configs/crypto-spot-live/respawn")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
}

// ─────────────────────────────────────────────────────────────────────────────
// respawn_from_config — the pre-flight → remove → spawn core (adversarial
// review). These drive the safety-critical wiring against the mock without a
// live config store: the invariant is NO-SILENT-DEAD-BOT — a PREDICTABLE
// failure (bad image, cap full, unresolved secret) must abort BEFORE the
// running container is torn down, leaving the live bot up.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
fn config_row(image: &str, bot_id: &str, secrets: Vec<String>) -> spawner::db::BotConfigRow {
    spawner::db::BotConfigRow {
        id: uuid::Uuid::nil(),
        name: "spot-live".to_string(),
        image: image.to_string(),
        mode: "live".to_string(),
        cpu_limit: None,
        memory_mb: None,
        env: HashMap::new(),
        secrets,
        bot_id: Some(bot_id.to_string()),
        restart_policy: None,
    }
}

/// Spawn a running `fks-bot-{bot_id}` into the mock (the "already live" bot).
#[cfg(feature = "db")]
async fn seed_running(mock: &MockDockerClient, image: &str, bot_id: &str) {
    mock.spawn(SpawnRequest {
        image: image.to_string(),
        bot_id: Some(bot_id.to_string()),
        mode: "live".to_string(),
        env: HashMap::new(),
        labels: HashMap::new(),
        cpu_limit: None,
        memory_limit_mb: None,
        cmd: None,
        entrypoint: None,
        secrets: vec![],
    })
    .await
    .expect("seed spawn");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_from_config_removes_old_and_spawns_new() {
    // Happy path: an existing live container is torn down and replaced, and both
    // the old id and the fresh response are returned. Exactly one bot remains.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let (state, mock) = build_state(test_config(""));
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;

    let cfg = config_row("fks-bot-spot:latest", "crypto-spot-live", vec![]);
    let (old_id, resp) = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect("respawn succeeds");

    assert!(old_id.is_some(), "the removed container id is echoed back");
    assert_eq!(resp.bot_id, "crypto-spot-live");

    let bots = mock.list_bots().await.unwrap();
    assert_eq!(
        bots.len(),
        1,
        "no double: exactly one container after respawn"
    );
    assert_eq!(bots[0].state, "running");
    assert_eq!(bots[0].name, "fks-bot-crypto-spot-live");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_bad_image_prefix_aborts_before_teardown() {
    // The headline finding: a predictable failure (disallowed image prefix) must
    // be caught in pre-flight, BEFORE remove_existing_bot — the live spot bot
    // stays up. Previously the old container was removed first, then the spawn
    // failed, leaving the real-money bot dead.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let (state, mock) = build_state(test_config(""));
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;

    let cfg = config_row("evil-image:latest", "crypto-spot-live", vec![]);
    let err = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect_err("disallowed image is rejected");
    assert!(matches!(err, SpawnerError::InvalidImage(_)), "{err:?}");

    // The live bot MUST still be running — never destroyed for a predictable fail.
    let bots = mock.list_bots().await.unwrap();
    assert_eq!(bots.len(), 1, "live bot untouched");
    assert_eq!(bots[0].state, "running");
    assert_eq!(bots[0].name, "fks-bot-crypto-spot-live");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_cap_full_aborts_before_teardown() {
    // Cap already full with ANOTHER bot and the target not yet running: the
    // pre-flight cap check refuses before any teardown, and the occupying bot is
    // left untouched.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let mut config = test_config("");
    config.max_concurrent_bots = 1;
    let (state, mock) = build_state(config);
    seed_running(&mock, "fks-bot-spot:latest", "other-bot").await;

    let cfg = config_row("fks-bot-spot:latest", "crypto-spot-live", vec![]);
    let err = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect_err("cap full is refused");
    assert!(matches!(err, SpawnerError::TooManyBots(1)), "{err:?}");

    let bots = mock.list_bots().await.unwrap();
    assert_eq!(bots.len(), 1);
    assert_eq!(
        bots[0].bot_id, "other-bot",
        "the occupying bot is untouched"
    );
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_of_running_target_does_not_false_trip_its_own_cap() {
    // At cap=1 with the TARGET bot itself occupying the slot, a respawn must
    // SUCCEED: removing the old container frees the slot, so the cap pre-check
    // excludes the target. (A naive pre-check would refuse and never redeploy.)
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let mut config = test_config("");
    config.max_concurrent_bots = 1;
    let (state, mock) = build_state(config);
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;

    let cfg = config_row("fks-bot-spot:latest", "crypto-spot-live", vec![]);
    let (_old, resp) = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect("respawn of the running target at cap succeeds");
    assert_eq!(resp.bot_id, "crypto-spot-live");
    assert_eq!(mock.list_bots().await.unwrap().len(), 1);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_secret_unresolved_aborts_before_teardown() {
    // The rotate-then-respawn footgun: a config requests exchange secrets that
    // cannot be resolved (here: no store configured). The secret check lives in
    // pre-flight, so it aborts BEFORE teardown — the live bot survives rather
    // than being killed and left unable to restart keyless.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let (state, mock) = build_state(test_config(""));
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;

    let cfg = config_row(
        "fks-bot-spot:latest",
        "crypto-spot-live",
        vec!["kraken".to_string()],
    );
    let err = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect_err("unresolved secret is rejected");
    assert!(matches!(err, SpawnerError::InvalidRequest(_)), "{err:?}");

    let bots = mock.list_bots().await.unwrap();
    assert_eq!(
        bots.len(),
        1,
        "live bot untouched by a pre-flight secret failure"
    );
    assert_eq!(bots[0].state, "running");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_409_after_removal_reports_clear_abort() {
    // Pre-flight passes but Docker still 409s on create (the old name lingers):
    // the handler must return the clear "respawn aborted" message, NOT a raw
    // Docker error — and never a second live bot.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let (state, mock) = build_state(test_config(""));
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;
    mock.fail_next_spawn(SpawnFault::NameConflict);

    let cfg = config_row("fks-bot-spot:latest", "crypto-spot-live", vec![]);
    let err = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect_err("409 surfaces as an abort");
    let msg = err.to_string();
    assert!(
        msg.contains("respawn aborted"),
        "expected clear abort, got: {msg}"
    );
    assert!(!err.is_name_conflict(), "wrapped, not the raw 409");
    // The old container was removed first — no second live bot; the operator
    // must act on the abort (documents the current behaviour).
    assert!(mock.list_bots().await.unwrap().is_empty());
}

#[cfg(feature = "db")]
#[tokio::test]
async fn respawn_non_conflict_spawn_error_propagates_raw() {
    // A non-409 spawn failure after teardown is propagated as-is (NOT wrapped in
    // the name-conflict abort message), so the operator sees the real cause.
    use spawner::api::respawn_from_config;
    use spawner::docker_client::DockerOps;

    let (state, mock) = build_state(test_config(""));
    seed_running(&mock, "fks-bot-spot:latest", "crypto-spot-live").await;
    mock.fail_next_spawn(SpawnFault::Generic);

    let cfg = config_row("fks-bot-spot:latest", "crypto-spot-live", vec![]);
    let err = respawn_from_config(&state, &cfg, "crypto-spot-live".to_string())
        .await
        .expect_err("generic spawn failure propagates");
    let msg = err.to_string();
    assert!(msg.contains("kaboom"), "raw cause preserved, got: {msg}");
    assert!(
        !msg.contains("respawn aborted"),
        "not the 409 wrapper: {msg}"
    );
    assert!(mock.list_bots().await.unwrap().is_empty());
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
// Edge factory (db feature) — registry round-trips gracefully without a live
// Postgres; the backtest endpoint validates before the store and reuses the
// exact /spawn machinery (exercised against the MockDockerClient)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn edges_list_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /edges must degrade to an empty
    // list with db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/edges").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
    assert!(payload.contains("\"edges\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn edges_post_without_db_returns_503() {
    // A well-formed registration with no DB configured must report an honest
    // 503, not a fake success — the POST /edges round-trip degrades like
    // /accounts.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "edge_id": "funding-reversion",
        "edge_type": "rule",
        "asset_scope": ["ETHUSDTM"],
        "status": "research",
        "backtest_image": "fks-bot-backtest-crypto-futures:latest"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/edges")
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
async fn edges_post_rejects_bad_type_status_and_id() {
    // Validation precedes the store check: allowlist misses and a
    // non-identifier edge_id are 400s regardless of DB availability.
    let (app, _) = build_app(test_config(""));

    for body in [
        serde_json::json!({ "edge_id": "x", "edge_type": "vibes" }),
        serde_json::json!({ "edge_id": "x", "edge_type": "rule", "status": "moon" }),
        serde_json::json!({ "edge_id": "has space", "edge_type": "rule" }),
        serde_json::json!({ "edge_id": "x", "edge_type": "rule", "asset_scope": "ETH" }),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::post("/edges")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "body: {body}");
    }
}

#[cfg(feature = "db")]
#[tokio::test]
async fn edges_delete_degrades_gracefully_without_db() {
    // DELETE /edges/{id} with no DB degrades to ok:false + db_enabled:false.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::delete("/edges/funding-reversion")
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
async fn edge_backtests_list_degrades_gracefully_without_db() {
    // GET /edges/{id}/backtests with no DB degrades to an empty run list
    // (including with ?limit=, exercising the Query extractor wiring).
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/edges/funding-reversion/backtests?limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"runs\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn backtest_post_without_db_returns_503() {
    // Without a DB there is no registry to resolve the edge in and no run
    // ledger — an honest 503, never a blind spawn.
    let (app, mock) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::post("/edges/funding-reversion/backtest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    // And crucially: nothing was spawned.
    let state = mock.state.lock().unwrap();
    assert!(state.containers.is_empty(), "no container without a ledger");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn backtest_post_rejects_malformed_edge_id_and_params() {
    // Validation precedes the store check: an edge id outside the container-
    // name charset, or non-object params, are 400s with or without a DB —
    // the unknown-edge 400 itself needs the registry (integration-tested
    // against a live Postgres at deploy time).
    let (app, _) = build_app(test_config(""));

    // Malformed edge id ("%20" decodes to a space — not identifier-shaped).
    let resp = app
        .clone()
        .oneshot(
            Request::post("/edges/bad%20id/backtest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::json!({}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Non-object params.
    let resp = app
        .oneshot(
            Request::post("/edges/funding-reversion/backtest")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "params": [1, 2, 3] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn backtest_spawn_request_round_trips_through_the_spawn_path() {
    // The happy-path spawn: POST /edges/{id}/backtest builds its SpawnRequest
    // via edges::build_backtest_spawn_request and hands it to the SAME
    // DockerOps::spawn the /spawn handler uses. The full HTTP path needs a
    // live registry row (Postgres), so the handler-side lookups are covered
    // at deploy time; here the spawn half is driven against the
    // MockDockerClient, which records what a real daemon would have been
    // asked to run.
    use spawner::edges::build_backtest_spawn_request;

    let config = test_config("");
    let mock = MockDockerClient::from_config(&config);

    let params = serde_json::json!({ "days": 365, "symbols": ["ETHUSDTM"] });
    let req = build_backtest_spawn_request(
        "funding-reversion",
        42,
        "fks-bot-backtest-crypto-futures:latest",
        &params,
        "postgres://fks_user:pw@postgres:5432/fks_db",
    );
    let resp = mock.spawn(req).await.expect("spawn accepted");

    // The bot identity follows the bt-{edge_id}-{run_id} contract and the
    // container carries the fks-bot- name prefix the guard demands.
    assert_eq!(resp.bot_id, "bt-funding-reversion-42");
    assert_eq!(resp.container_name, "fks-bot-bt-funding-reversion-42");
    assert_eq!(resp.mode, "backtest");

    // The mock recorded the container with the edge-attribution label.
    let state = mock.state.lock().unwrap();
    assert_eq!(state.containers.len(), 1);
    let info = state.containers.values().next().unwrap();
    assert_eq!(info.image, "fks-bot-backtest-crypto-futures:latest");
    assert_eq!(
        info.labels.get("fks.edge_id").map(String::as_str),
        Some("funding-reversion")
    );
    assert_eq!(info.mode, "backtest");
}

#[tokio::test]
async fn backtest_spawn_request_still_hits_the_image_prefix_guard() {
    // Reusing the spawn path means reusing its guards: a registry row whose
    // backtest_image doesn't carry the fks-bot- prefix is refused at spawn
    // time exactly like a hand-rolled /spawn request would be.
    use spawner::edges::build_backtest_spawn_request;

    let config = test_config("");
    let mock = MockDockerClient::from_config(&config);

    let req = build_backtest_spawn_request(
        "funding-reversion",
        1,
        "evil-image:latest",
        &serde_json::json!({}),
        "postgres://x",
    );
    let err = mock.spawn(req).await.expect_err("prefix guard fires");
    assert!(matches!(err, SpawnerError::InvalidImage(_)), "{err:?}");

    let state = mock.state.lock().unwrap();
    assert!(state.containers.is_empty());
}

#[cfg(feature = "db")]
#[tokio::test]
async fn edge_routes_are_token_gated() {
    // With a configured token, the edge-factory routes reject
    // unauthenticated requests (401) before touching the store — same gate
    // as the rest.
    let (app, _) = build_app(test_config("s3cr3t"));

    for req in [
        Request::get("/edges").body(Body::empty()).unwrap(),
        Request::get("/edges/x/backtests")
            .body(Body::empty())
            .unwrap(),
        Request::post("/edges/x/backtest")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    ] {
        let uri = req.uri().clone();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "route: {uri}");
    }
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

// ─────────────────────────────────────────────────────────────────────────────
// Treasury (db feature) — transfers ledger + accounts registry + /profit
// round-tripping gracefully without a live Postgres
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[tokio::test]
async fn transfers_list_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /transfers must degrade to an empty
    // JSON array (not 500), including with the ?account_id=/?limit= filters
    // (exercises the Query extractor + query-plan wiring, like /net-worth).
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/transfers?account_id=spot-portfolio&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert_eq!(payload, "[]", "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn transfers_post_without_db_returns_503() {
    // A well-formed deposit with no DB configured must report an honest 503
    // (ledger unavailable), not a fake success — a silently dropped deposit
    // would corrupt every later profit decomposition.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "account_id": "spot-portfolio",
        "amount": 250.0,
        "kind": "deposit",
        "note": "July DCA"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/transfers")
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
async fn transfers_post_rejects_unknown_kind() {
    // Validation runs before the store check: a kind outside the allowlist is
    // a 400 regardless of DB availability.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "account_id": "spot-portfolio",
        "amount": 250.0,
        "kind": "donation"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/transfers")
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
async fn transfers_post_rejects_zero_amount() {
    // A zero flow is meaningless in the ledger — 400 before the store.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "account_id": "spot-portfolio",
        "amount": 0.0,
        "kind": "deposit"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/transfers")
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
async fn accounts_list_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /accounts must degrade to an empty
    // list with db_enabled:false, never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/accounts").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"total\":0"), "body: {payload}");
    assert!(payload.contains("\"accounts\":[]"), "body: {payload}");
}

#[cfg(feature = "db")]
#[tokio::test]
async fn accounts_post_without_db_returns_503() {
    // A well-formed registration with no DB configured must report an honest
    // 503, not a fake success.
    let (app, _) = build_app(test_config(""));

    let body = serde_json::json!({
        "account_id": "kraken-main",
        "tier": 1,
        "account_class": "personal-crypto",
        "role": "bot-trade",
        "venue": "kraken"
    })
    .to_string();

    let resp = app
        .oneshot(
            Request::post("/accounts")
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
async fn accounts_post_rejects_bad_tier_and_role() {
    // Validation precedes the store check: out-of-range tier / unknown role
    // are 400s regardless of DB availability.
    let (app, _) = build_app(test_config(""));

    for body in [
        serde_json::json!({
            "account_id": "x",
            "tier": 9,
            "account_class": "prop",
            "role": "watch"
        }),
        serde_json::json!({
            "account_id": "x",
            "tier": 3,
            "account_class": "prop",
            "role": "yolo-trade"
        }),
    ] {
        let resp = app
            .clone()
            .oneshot(
                Request::post("/accounts")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "body: {body}");
    }
}

#[cfg(feature = "db")]
#[tokio::test]
async fn accounts_delete_degrades_gracefully_without_db() {
    // DELETE /accounts/{id} with no DB degrades to ok:false + db_enabled:false.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::delete("/accounts/kraken-main")
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
async fn profit_degrades_gracefully_without_db() {
    // No DATABASE_URL (store: None) — GET /profit must degrade to a stable
    // null-figure envelope (200, db_enabled:false), never 500.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/profit?account_id=spot-portfolio&since=2026-01-01T00:00:00Z")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let payload = body_string(resp).await;
    assert!(payload.contains("\"db_enabled\":false"), "body: {payload}");
    assert!(payload.contains("\"profit\":null"), "body: {payload}");
    assert!(
        payload.contains("\"account_id\":\"spot-portfolio\""),
        "body: {payload}"
    );
}

#[cfg(feature = "db")]
#[tokio::test]
async fn profit_requires_account_id() {
    // /profit is per-account by definition — no account_id is a 400.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(Request::get("/profit").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn profit_rejects_malformed_since() {
    // A non-RFC3339 ?since= is a 400, not a silent full-history read.
    let (app, _) = build_app(test_config(""));

    let resp = app
        .oneshot(
            Request::get("/profit?account_id=x&since=yesterday")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[cfg(feature = "db")]
#[tokio::test]
async fn treasury_routes_are_token_gated() {
    // With a configured token, the treasury routes reject unauthenticated
    // requests (401) before touching the store — same gate as the rest.
    let (app, _) = build_app(test_config("s3cr3t"));

    for req in [
        Request::get("/transfers").body(Body::empty()).unwrap(),
        Request::get("/accounts").body(Body::empty()).unwrap(),
        Request::get("/profit?account_id=x")
            .body(Body::empty())
            .unwrap(),
    ] {
        let uri = req.uri().clone();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "route: {uri}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Supervisor — prune exemption wiring (via the mock docker, no DB)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn supervisor_prunes_one_shot_but_quarantines_live_and_keeps_running() {
    // prune_after_secs = 0 → an exited one-shot is immediately prune-eligible;
    // live-mode containers get the long quarantine window instead.
    let mut cfg = test_config("");
    cfg.prune_after_secs = 0;
    cfg.prune_live_after_secs = 604_800;
    let (state, mock) = build_state(cfg);

    let req = |bot_id: &str, mode: &str| -> SpawnRequest {
        serde_json::from_value(serde_json::json!({
            "image": "fks-bot-x:latest",
            "bot_id": bot_id,
            "mode": mode,
        }))
        .unwrap()
    };

    // Running live bot (must survive), a finished one-shot backtest (must be
    // pruned), and a stopped live bot (must be quarantined, NOT fast-pruned).
    state.docker.spawn(req("live-run", "live")).await.unwrap();
    let bt = state.docker.spawn(req("bt-1", "backtest")).await.unwrap();
    let stopped_live = state
        .docker
        .spawn(req("live-stopped", "live"))
        .await
        .unwrap();
    state.docker.stop(&bt.container_id).await.unwrap();
    state.docker.stop(&stopped_live.container_id).await.unwrap();

    let mut tracker = spawner::supervisor::RestartTracker::default();
    spawner::supervisor::tick(&state, &mut tracker).await;

    let bot_ids: Vec<String> = {
        let s = mock.state.lock().unwrap();
        s.containers.values().map(|c| c.bot_id.clone()).collect()
    };
    assert!(
        bot_ids.contains(&"live-run".to_string()),
        "a running live bot is never a prune candidate"
    );
    assert!(
        bot_ids.contains(&"live-stopped".to_string()),
        "an exited live bot is quarantined for forensics, not fast-pruned"
    );
    assert!(
        !bot_ids.contains(&"bt-1".to_string()),
        "a finished one-shot backtest is pruned on the short retention"
    );
}
