// =============================================================================
// error.rs — domain error types for the FKS Bot Spawner
// =============================================================================

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpawnerError {
    #[error("image '{0}' is not allowed — must start with the configured prefix")]
    InvalidImage(String),

    #[error("invalid spawn request: {0}")]
    InvalidRequest(String),

    #[error("concurrent bot limit reached ({0} running)")]
    TooManyBots(usize),

    #[error("container '{0}' not found")]
    NotFound(String),

    #[error("Docker API error: {0}")]
    Docker(#[from] bollard::errors::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialisation error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

impl SpawnerError {
    /// True when this is Docker's 409 "container name already in use" conflict.
    /// The respawn path uses it to turn a still-present old container into a
    /// clear, actionable error (the old container wasn't fully removed) instead
    /// of a raw 500 — and to guarantee it never proceeds to a second live bot.
    pub fn is_name_conflict(&self) -> bool {
        matches!(
            self,
            SpawnerError::Docker(bollard::errors::Error::DockerResponseServerError {
                status_code: 409,
                ..
            })
        )
    }

    pub fn http_status(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            SpawnerError::InvalidImage(_) => StatusCode::BAD_REQUEST,
            SpawnerError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            SpawnerError::TooManyBots(_) => StatusCode::TOO_MANY_REQUESTS,
            SpawnerError::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Allow returning SpawnerError directly from Axum handlers.
impl axum::response::IntoResponse for SpawnerError {
    fn into_response(self) -> axum::response::Response {
        use crate::models::ErrorResponse;
        use axum::Json;

        let status = self.http_status();
        let body = Json(ErrorResponse::new(self.to_string()));

        tracing::warn!(error = %body.0.error, status = %status, "request error");

        (status, body).into_response()
    }
}

pub type SpawnerResult<T> = Result<T, SpawnerError>;
