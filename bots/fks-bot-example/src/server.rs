//! Tiny HTTP server exposing `/metrics` (Prometheus text exposition) and
//! `/health` (liveness probe). Mirrors the spawner's own metrics server
//! so monitoring infra has a uniform contract across spawner + bots.
//!
//! The server is bound to the configured port (default `9091`) on
//! `0.0.0.0` so the spawner-managed Docker network can reach it. There is
//! deliberately no auth — the entire fks-bots scrape job is internal-only.

use std::sync::Arc;

use axum::{Router, response::IntoResponse, routing::get};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::metrics;

/// Shared state passed to handlers — currently empty but kept as a struct
/// so additional dependencies (e.g. a brain reference for `/health` to
/// surface `BrainHealth`) can be added without changing every signature.
#[derive(Clone, Default)]
pub struct ServerState;

/// Build the axum router.
pub fn build_router(state: ServerState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(Arc::new(state))
}

async fn metrics_handler() -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        metrics::render(),
    )
}

async fn health_handler() -> impl IntoResponse {
    "ok"
}

/// Run the metrics server until `cancel` is triggered.
///
/// Returns `Ok(())` after a graceful shutdown. Listen errors propagate as
/// `Err`; logging is left to the caller.
pub async fn run(port: u16, cancel: CancellationToken) -> anyhow::Result<()> {
    let app = build_router(ServerState);
    let bind = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            warn!(addr = %bind, error = %e, "metrics server failed to bind");
            return Err(e.into());
        }
    };

    info!(addr = %bind, "metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    info!("metrics server shut down");
    Ok(())
}
