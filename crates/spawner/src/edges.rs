// =============================================================================
// edges.rs — pure logic for the edge factory (edge registry + backtest runs)
//
// WHY (the edge-portfolio doctrine, fks-state docs/ARCHITECTURE.md): the
// platform runs a PORTFOLIO of edges — janus's adaptive edge plus hand-found
// operator rule-edges (orb, funding-reversion, …) — and every edge faces the
// SAME honest validation bar: backtest → paper in its own spawner container →
// validate → promote → demote on decay. The `edges` registry
// (src/sql/spawner/008_edge_factory.sql) is the portfolio's source of truth
// (the accounts-registry pattern); `backtest_runs` is the factory's run
// ledger.
//
// THE BACKTEST CONTRACT (POST /edges/{id}/backtest): the spawner INSERTs a
// backtest_runs row (status 'running'), then spawns the edge's registered
// `backtest_image` as a one-shot fks-bot-* container through the SAME
// internal spawn path /spawn uses (image-prefix guard, forced network,
// cap_drop ALL, no-new-privileges). The container is handed:
//
//   BACKTEST_RUN_ID   — its backtest_runs row id
//   BACKTEST_EDGE_ID  — the edge it is validating
//   BACKTEST_PARAMS   — the request's params object as a JSON string
//   BACKTEST_DB_URL   — the spawner's own Postgres URL (same ruby_db; the
//                       container is on fks_network so `postgres:5432`
//                       resolves), with which the container UPDATEs ITS OWN
//                       row: status completed|failed + results + finished_at
//
// One-shot: the container exits when done (no restart policy is set on
// spawned bots). A container that dies without reporting leaves its row
// 'running'; the staleness sweep (see BotRunStore::sweep_stale_backtest_runs,
// piggybacked on the net-worth sampler tick) marks rows older than 2 hours
// 'failed'.
//
// Everything in this module is PURE (validation, request shaping) and always
// compiled, mirroring treasury.rs: the handlers + sqlx queries it feeds are
// db-gated in api.rs / db.rs, but the request-shaping logic stays
// unit-testable without a live database.
// =============================================================================

use std::collections::HashMap;

use crate::models::{EdgeRequest, SpawnRequest};

// ─────────────────────────────────────────────────────────────────────────────
// Allowlists — mirror the CHECK constraints in 008_edge_factory.sql so a
// typo'd submission is a 400 at the API edge, not a Postgres constraint error.
// ─────────────────────────────────────────────────────────────────────────────

/// Valid `edges.edge_type` values: 'adaptive' (janus's learning core) |
/// 'rule' (hand-found operator rule-edge). Mirrors the SQL CHECK constraint.
pub const EDGE_TYPES: &[&str] = &["adaptive", "rule"];

/// Valid `edges.status` values — the factory lifecycle. Mirrors the SQL
/// CHECK constraint. Demotion on decay moves a live edge back down.
pub const EDGE_STATUSES: &[&str] = &["research", "paper", "live", "retired"];

/// Max `edge_id` length. Tighter than the treasury's 128 because the edge id
/// lands in the backtest container's bot_id (`bt-{edge_id}-{run_id}`), which
/// the spawn path caps at 64 chars: 3 ("bt-") + 40 + 1 + 20 (i64 digits) = 64.
pub const MAX_EDGE_ID_LEN: usize = 40;

/// Cap on the serialized `params` object (it travels as ONE env var into the
/// backtest container) — generous but bounded.
pub const MAX_PARAMS_LEN: usize = 16 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// GET /edges/{id}/backtests request shaping (mirrors the /runs limit rules)
// ─────────────────────────────────────────────────────────────────────────────

/// Default number of runs returned by `GET /edges/{id}/backtests`.
pub const BACKTESTS_DEFAULT_LIMIT: i64 = 50;
/// Hard cap on the number of runs returned.
pub const BACKTESTS_MAX_LIMIT: i64 = 500;

/// Pure request-shaping for `GET /edges/{id}/backtests`: clamp `limit` into
/// `1..=BACKTESTS_MAX_LIMIT`, defaulting to [`BACKTESTS_DEFAULT_LIMIT`].
pub fn backtests_query_plan(limit: Option<i64>) -> i64 {
    limit
        .unwrap_or(BACKTESTS_DEFAULT_LIMIT)
        .clamp(1, BACKTESTS_MAX_LIMIT)
}

// ─────────────────────────────────────────────────────────────────────────────
// edge_id validation — shared by POST /edges and POST /edges/{id}/backtest
// ─────────────────────────────────────────────────────────────────────────────

/// Validate + trim an edge id. Identifier-shaped by construction
/// (`[A-Za-z0-9._-]`, 1..=40 chars) because it becomes part of the backtest
/// container's bot_id / Docker name — validating here means a registered edge
/// can never fail the spawn path's name check later. Errors are
/// operator-facing 400 messages.
pub fn validate_edge_id(raw: &str) -> Result<String, String> {
    let edge_id = raw.trim();
    if edge_id.is_empty() {
        return Err("edge_id is required".to_string());
    }
    if edge_id.len() > MAX_EDGE_ID_LEN {
        return Err(format!("edge_id too long (max {MAX_EDGE_ID_LEN} chars)"));
    }
    if !edge_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(
            "edge_id may only contain ASCII letters, digits, '.', '_' or '-' \
             (it names the backtest container)"
                .to_string(),
        );
    }
    Ok(edge_id.to_string())
}

/// Validate a `POST /edges` submission against the registry's allowlists
/// (edge_type, status; asset_scope must be a JSON array and
/// validation_record a JSON object when supplied). Returns the trimmed
/// `edge_id` on success. The registry carries NO credentials by design —
/// `backtest_image` is only a NAME; the spawn-time fks-bot- prefix guard
/// stays authoritative when the backtest actually runs.
pub fn validate_edge(req: &EdgeRequest) -> Result<String, String> {
    let edge_id = validate_edge_id(&req.edge_id)?;

    if !EDGE_TYPES.contains(&req.edge_type.trim()) {
        return Err(format!(
            "unknown edge_type '{}' (supported: adaptive, rule)",
            req.edge_type
        ));
    }

    if !EDGE_STATUSES.contains(&req.status.trim()) {
        return Err(format!(
            "unknown status '{}' (supported: research, paper, live, retired)",
            req.status
        ));
    }

    if let Some(scope) = &req.asset_scope {
        let ok = scope
            .as_array()
            .is_some_and(|a| a.iter().all(serde_json::Value::is_string));
        if !ok {
            return Err(
                "asset_scope must be a JSON array of symbol strings (empty = all assets)"
                    .to_string(),
            );
        }
    }

    if let Some(v) = &req.validation_record
        && !v.is_object()
    {
        return Err("validation_record must be a JSON object".to_string());
    }

    if let Some(image) = &req.backtest_image
        && image.trim().is_empty()
    {
        return Err(
            "backtest_image must be a non-empty image name (omit it for 'not yet containerized')"
                .to_string(),
        );
    }

    Ok(edge_id)
}

/// Validate the `params` of a `POST /edges/{id}/backtest` request: a JSON
/// object (absent = `{}`), bounded in serialized size (it rides into the
/// container as one env var). Returns the normalised object.
pub fn validate_backtest_params(
    params: Option<&serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let params = params.cloned().unwrap_or_else(|| serde_json::json!({}));
    if !params.is_object() {
        return Err("params must be a JSON object".to_string());
    }
    if params.to_string().len() > MAX_PARAMS_LEN {
        return Err(format!(
            "params too large (max {MAX_PARAMS_LEN} bytes serialized)"
        ));
    }
    Ok(params)
}

// ─────────────────────────────────────────────────────────────────────────────
// Spawn-request shaping — the bridge onto the existing /spawn machinery
// ─────────────────────────────────────────────────────────────────────────────

/// The bot_id (and thus `fks-bot-` container-name suffix) of one backtest run.
pub fn backtest_bot_id(edge_id: &str, run_id: i64) -> String {
    format!("bt-{edge_id}-{run_id}")
}

/// Build the [`SpawnRequest`] for one backtest run. This deliberately goes
/// through the SAME `DockerOps::spawn` path as POST /spawn, so every guard
/// applies unchanged: fks-bot- image prefix, concurrent-bot cap, forced
/// fks_network, cap_drop ALL + no-new-privileges, resource defaults. The
/// container env carries the run contract (see the module docs); explicit
/// mode `backtest` labels it apart from paper/live bots.
pub fn build_backtest_spawn_request(
    edge_id: &str,
    run_id: i64,
    image: &str,
    params: &serde_json::Value,
    db_url: &str,
) -> SpawnRequest {
    let env: HashMap<String, String> = HashMap::from([
        ("BACKTEST_RUN_ID".to_string(), run_id.to_string()),
        ("BACKTEST_EDGE_ID".to_string(), edge_id.to_string()),
        ("BACKTEST_PARAMS".to_string(), params.to_string()),
        ("BACKTEST_DB_URL".to_string(), db_url.to_string()),
    ]);
    SpawnRequest {
        image: image.to_string(),
        bot_id: Some(backtest_bot_id(edge_id, run_id)),
        mode: "backtest".to_string(),
        env,
        labels: HashMap::from([("fks.edge_id".to_string(), edge_id.to_string())]),
        cpu_limit: None,
        memory_limit_mb: None,
        cmd: None,
        entrypoint: None,
        secrets: Vec::new(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no HTTP, no Docker)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn edge_req(json: &str) -> EdgeRequest {
        serde_json::from_str(json).expect("valid EdgeRequest JSON")
    }

    // ── edge validation ──────────────────────────────────────────────────────

    #[test]
    fn edge_minimal_validates_with_defaults() {
        let req = edge_req(r#"{"edge_id":" funding-reversion ","edge_type":"rule"}"#);
        assert_eq!(
            validate_edge(&req).expect("valid"),
            "funding-reversion",
            "edge_id is trimmed"
        );
        assert_eq!(req.status, "research", "status defaults to research");
        assert!(req.active, "active defaults to true");
        assert!(req.asset_scope.is_none() && req.backtest_image.is_none());
    }

    #[test]
    fn edge_accepts_full_doctrine_seeds() {
        // The three doctrine seeds all validate: janus-adaptive (all assets),
        // orb (GC/NQ/ES), funding-reversion (KuCoin perps).
        for json in [
            r#"{"edge_id":"janus-adaptive","edge_type":"adaptive","asset_scope":[]}"#,
            r#"{"edge_id":"orb","edge_type":"rule","asset_scope":["GC","NQ","ES"],
                "status":"research"}"#,
            r#"{"edge_id":"funding-reversion","edge_type":"rule",
                "asset_scope":["ETHUSDTM"],"status":"paper",
                "backtest_image":"fks-bot-backtest-crypto-futures:latest"}"#,
        ] {
            assert!(validate_edge(&edge_req(json)).is_ok(), "seed: {json}");
        }
    }

    #[test]
    fn edge_rejects_unknown_type_and_status() {
        let bad_type = edge_req(r#"{"edge_id":"x","edge_type":"vibes"}"#);
        assert!(validate_edge(&bad_type).is_err());

        let bad_status = edge_req(r#"{"edge_id":"x","edge_type":"rule","status":"moon"}"#);
        assert!(validate_edge(&bad_status).is_err());
    }

    #[test]
    fn edge_rejects_non_identifier_ids() {
        // The edge id becomes the container's bot_id suffix, so the Docker
        // name charset is enforced at registration time.
        for bad in ["", "   ", "has space", "a/b", "naïve", "x;rm"] {
            let req = edge_req(&format!(
                r#"{{"edge_id":{},"edge_type":"rule"}}"#,
                serde_json::json!(bad)
            ));
            assert!(validate_edge(&req).is_err(), "edge_id {bad:?} rejected");
        }
        let long = "x".repeat(MAX_EDGE_ID_LEN + 1);
        assert!(validate_edge_id(&long).is_err(), "over-length rejected");
        let max = "x".repeat(MAX_EDGE_ID_LEN);
        assert!(validate_edge_id(&max).is_ok(), "at-length accepted");
    }

    #[test]
    fn edge_rejects_malformed_scope_and_record() {
        let bad_scope = edge_req(r#"{"edge_id":"x","edge_type":"rule","asset_scope":"ETH"}"#);
        assert!(validate_edge(&bad_scope).is_err(), "scope must be an array");

        let bad_items = edge_req(r#"{"edge_id":"x","edge_type":"rule","asset_scope":[1,2]}"#);
        assert!(
            validate_edge(&bad_items).is_err(),
            "scope items must be strings"
        );

        let bad_record = edge_req(r#"{"edge_id":"x","edge_type":"rule","validation_record":[1]}"#);
        assert!(
            validate_edge(&bad_record).is_err(),
            "validation_record must be an object"
        );
    }

    #[test]
    fn edge_rejects_blank_backtest_image() {
        // Explicit-but-blank is a mistake; "not containerized" is expressed by
        // OMITTING the field (NULL in the registry).
        let req = edge_req(r#"{"edge_id":"x","edge_type":"rule","backtest_image":"  "}"#);
        assert!(validate_edge(&req).is_err());
    }

    // ── backtest params ──────────────────────────────────────────────────────

    #[test]
    fn backtest_params_default_to_empty_object() {
        assert_eq!(
            validate_backtest_params(None).expect("valid"),
            serde_json::json!({})
        );
    }

    #[test]
    fn backtest_params_must_be_an_object() {
        for bad in [serde_json::json!([1, 2]), serde_json::json!("days=90")] {
            assert!(validate_backtest_params(Some(&bad)).is_err(), "{bad}");
        }
        let ok = serde_json::json!({"days": 180, "symbols": ["ETHUSDTM"]});
        assert_eq!(validate_backtest_params(Some(&ok)).unwrap(), ok);
    }

    #[test]
    fn backtest_params_bounded_in_size() {
        let big = serde_json::json!({"blob": "x".repeat(MAX_PARAMS_LEN)});
        assert!(validate_backtest_params(Some(&big)).is_err());
    }

    // ── spawn-request shaping ────────────────────────────────────────────────

    #[test]
    fn backtest_bot_id_shape_and_docker_charset() {
        let id = backtest_bot_id("funding-reversion", 42);
        assert_eq!(id, "bt-funding-reversion-42");
        // Must satisfy the spawn path's identifier rules (≤64, docker charset)
        // even at the maximum edge-id length and a huge run id.
        let max = backtest_bot_id(&"x".repeat(MAX_EDGE_ID_LEN), i64::MAX);
        assert!(max.len() <= 64, "bot_id {} chars", max.len());
        assert!(
            max.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        );
    }

    #[test]
    fn backtest_spawn_request_carries_the_env_contract() {
        let params = serde_json::json!({"days": 365, "symbols": ["ETHUSDTM"]});
        let req = build_backtest_spawn_request(
            "funding-reversion",
            7,
            "fks-bot-backtest-crypto-futures:latest",
            &params,
            "postgres://fks_user:pw@postgres:5432/ruby_db",
        );
        assert_eq!(req.image, "fks-bot-backtest-crypto-futures:latest");
        assert_eq!(req.bot_id.as_deref(), Some("bt-funding-reversion-7"));
        assert_eq!(req.mode, "backtest");
        assert_eq!(
            req.env.get("BACKTEST_RUN_ID").map(String::as_str),
            Some("7")
        );
        assert_eq!(
            req.env.get("BACKTEST_EDGE_ID").map(String::as_str),
            Some("funding-reversion")
        );
        let sent: serde_json::Value =
            serde_json::from_str(req.env.get("BACKTEST_PARAMS").unwrap()).unwrap();
        assert_eq!(sent, params, "params round-trip as a JSON string");
        assert_eq!(
            req.env.get("BACKTEST_DB_URL").map(String::as_str),
            Some("postgres://fks_user:pw@postgres:5432/ruby_db")
        );
        assert_eq!(
            req.labels.get("fks.edge_id").map(String::as_str),
            Some("funding-reversion")
        );
        assert!(req.secrets.is_empty(), "backtests get no exchange keys");
        assert!(req.cmd.is_none() && req.entrypoint.is_none());
    }

    // ── backtests query plan ─────────────────────────────────────────────────

    #[test]
    fn backtests_query_plan_defaults_and_clamps() {
        assert_eq!(backtests_query_plan(None), BACKTESTS_DEFAULT_LIMIT);
        assert_eq!(backtests_query_plan(Some(10)), 10);
        assert_eq!(backtests_query_plan(Some(0)), 1);
        assert_eq!(backtests_query_plan(Some(-3)), 1);
        assert_eq!(backtests_query_plan(Some(i64::MAX)), BACKTESTS_MAX_LIMIT);
    }
}
