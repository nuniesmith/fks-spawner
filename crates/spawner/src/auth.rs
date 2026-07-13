// =============================================================================
// auth.rs — `X-Internal-Token` middleware for the FKS Bot Spawner
//
// nginx already sets `X-Internal-Token: ${NGINX_INTERNAL_TOKEN}` on every
// proxied request to the spawner. This middleware validates that header
// against the spawner's own copy of the token (loaded from the same
// env var at startup) and rejects any request that doesn't carry it.
//
// When `Config.internal_token` is empty the middleware is a no-op — useful
// for local dev where running the binary directly bypasses nginx.
//
// `/health` and `/metrics` are intentionally NOT protected by this layer:
//   - The Docker healthcheck (`curl http://localhost:8090/health`) talks
//     to the spawner directly inside the container, bypassing nginx.
//   - Prometheus scrapes `/metrics` over the `fks_network` Docker network,
//     also bypassing nginx.
// Both endpoints are register on the public sub-router in `api.rs`; only
// the lifecycle routes go through this middleware.
// =============================================================================

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header::HeaderName},
    middleware::Next,
    response::Response,
};
use tracing::{info, warn};

use crate::api::AppState;
use crate::models::ErrorResponse;

/// Header name. Case-insensitive on the wire; normalised lower-case here.
pub const HEADER: &str = "x-internal-token";

/// Constant-time string compare to keep the auth check from leaking
/// information via timing on a per-byte mismatch. The cost over `==` is
/// negligible for tokens this short (16-64 bytes).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Announce the internal-auth posture once at startup and fail closed when the
/// operator has explicitly demanded it. Call from `main` right after loading
/// `Config`, before binding the server.
///
/// The middleware itself still short-circuits to a dev passthrough on an empty
/// token (see [`require_internal_token`]); this function makes that decision
/// *loud* instead of silent, so an empty `NGINX_INTERNAL_TOKEN` on a
/// money-adjacent deployment can never fail open unnoticed:
///
/// - token set                         → `Ok`, auth ENFORCED (info log).
/// - token empty + `require` = `true`  → `Err`: refuse to boot (fail closed).
/// - token empty + `require` = `false` → `Ok`, but a LOUD warning is logged
///   that every protected route is unauthenticated (dev passthrough).
///
/// `require` is `Config.require_internal_auth` (`REQUIRE_INTERNAL_TOKEN`).
pub fn check_internal_auth_posture(internal_token: &str, require: bool) -> Result<(), String> {
    if !internal_token.is_empty() {
        info!(
            "internal-token auth ENABLED — X-Internal-Token required on all routes except /health and /metrics"
        );
        return Ok(());
    }
    if require {
        return Err(
            "REQUIRE_INTERNAL_TOKEN is set but NGINX_INTERNAL_TOKEN is empty — refusing to boot \
             with internal auth DISABLED on money-adjacent routes (/transfers, /accounts, \
             /net-worth, /edges, /secrets, /spawn). Set NGINX_INTERNAL_TOKEN or unset \
             REQUIRE_INTERNAL_TOKEN for dev."
                .to_string(),
        );
    }
    warn!(
        "!!! internal-token auth DISABLED (NGINX_INTERNAL_TOKEN empty) — every route except \
         /health and /metrics is UNAUTHENTICATED, including /transfers, /accounts, /net-worth, \
         /edges, /secrets and /spawn. This is DEV-ONLY; set NGINX_INTERNAL_TOKEN in any \
         deployment where the port is reachable, or set REQUIRE_INTERNAL_TOKEN=true to fail closed."
    );
    Ok(())
}

/// Reject requests missing or carrying the wrong `X-Internal-Token`.
/// `Config.internal_token == ""` means the middleware is disabled.
pub async fn require_internal_token(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let configured = state.config.internal_token.as_str();
    if configured.is_empty() {
        // Dev mode — no auth.
        return Ok(next.run(req).await);
    }

    let header_name = HeaderName::from_static(HEADER);
    let presented = req.headers().get(&header_name);

    match presented {
        Some(value) if constant_time_eq(value.as_bytes(), configured.as_bytes()) => {
            Ok(next.run(req).await)
        }
        Some(_) => {
            warn!("rejected request with mismatched X-Internal-Token");
            Err((
                StatusCode::FORBIDDEN,
                Json(ErrorResponse::new("invalid X-Internal-Token")),
            ))
        }
        None => {
            warn!("rejected request without X-Internal-Token");
            Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::new("missing X-Internal-Token")),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{check_internal_auth_posture, constant_time_eq};

    #[test]
    fn constant_time_eq_matches_normal_eq_for_correct_inputs() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn posture_ok_when_token_set_regardless_of_require() {
        // The oryx posture: token set → boots either way, auth enforced.
        assert!(check_internal_auth_posture("s3cret", false).is_ok());
        assert!(check_internal_auth_posture("s3cret", true).is_ok());
    }

    #[test]
    fn posture_warns_but_boots_when_token_empty_and_not_required() {
        // Dev passthrough: empty token is allowed (loud warn), boot proceeds.
        assert!(check_internal_auth_posture("", false).is_ok());
    }

    #[test]
    fn posture_fails_closed_when_token_empty_but_required() {
        // Hardened posture: empty token + REQUIRE_INTERNAL_TOKEN → refuse boot.
        let err = check_internal_auth_posture("", true).unwrap_err();
        assert!(err.contains("REQUIRE_INTERNAL_TOKEN"));
    }
}
