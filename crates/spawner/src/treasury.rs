// =============================================================================
// treasury.rs — pure logic for the treasury layer (P0.4 transfers ledger +
// P0.5 accounts registry + the /profit decomposition read)
//
// WHY: net_worth_snapshots (006) lets the platform SEE net worth, but drift
// still conflates deposits with trading profit — the spot bot's own status
// code documents "later deposits show up as PnL — no deposit ledger yet".
// The `transfers` ledger records signed cash flows (positive = into the
// account, negative = out), so the decomposition becomes:
//
//     delta        = end_net_worth − start_net_worth
//     net_inflows  = deposits_in − withdrawals_out
//     profit       = delta − net_inflows
//
// The `accounts` registry (src/sql/spawner/007_treasury.sql) is the source of
// truth for the multi-account topology (tiers: 0 cold-BTC backbone /
// 1 personal-crypto / 2 rithmic-main / 3 prop-copy-target) later phases key
// off. It carries NO credentials — those stay in the encrypted
// exchange_secrets store.
//
// Everything in this module is PURE (validation, request shaping, arithmetic)
// and always compiled, mirroring net_worth.rs: the handlers + sqlx queries it
// feeds are db-gated in api.rs / db.rs, but the request-shaping logic stays
// unit-testable without a live database.
// =============================================================================

use chrono::{DateTime, Utc};

use crate::models::{AccountRequest, TransferRequest};

// ─────────────────────────────────────────────────────────────────────────────
// Allowlists — mirror the CHECK constraints in 007_treasury.sql so a typo'd
// submission is a 400 at the API edge, not a Postgres constraint error.
// ─────────────────────────────────────────────────────────────────────────────

/// Valid `transfers.kind` values (mirrors the SQL CHECK constraint).
pub const TRANSFER_KINDS: &[&str] = &["deposit", "withdrawal", "payout", "sweep"];

/// Valid `transfers.source` values: 'manual' (operator entry) |
/// 'bot_detected' (a bot noticing an unexplained balance jump).
pub const TRANSFER_SOURCES: &[&str] = &["manual", "bot_detected"];

/// Valid `accounts.role` values (mirrors the SQL CHECK constraint).
pub const ACCOUNT_ROLES: &[&str] = &["watch", "bot-trade", "human-trade-source", "copy-target"];

/// Valid `accounts.compliance_flag` values (mirrors the SQL CHECK constraint).
/// 'manual-mirror' = a human confirms every mirrored fill (the platform-wide
/// no-autonomous-execution default); 'auto-fill' only where firm rules allow.
pub const COMPLIANCE_FLAGS: &[&str] = &["manual-mirror", "auto-fill"];

/// Highest valid `accounts.tier` (0 = cold-BTC backbone … 3 = prop-copy-target).
pub const ACCOUNT_TIER_MAX: i16 = 3;

/// Generous-but-bounded cap on identifier-ish text fields so obviously-bogus
/// input is rejected before it hits the DB.
const MAX_ID_LEN: usize = 128;

// ─────────────────────────────────────────────────────────────────────────────
// GET /transfers request shaping (mirrors net_worth_query_plan)
// ─────────────────────────────────────────────────────────────────────────────

/// Default number of ledger rows returned by `GET /transfers`.
pub const TRANSFERS_DEFAULT_LIMIT: i64 = 500;
/// Hard cap on the number of rows `GET /transfers` will return.
pub const TRANSFERS_MAX_LIMIT: i64 = 5000;

/// Pure request-shaping for `GET /transfers`. Clamps `limit` into
/// `1..=TRANSFERS_MAX_LIMIT` (defaulting to [`TRANSFERS_DEFAULT_LIMIT`] when
/// absent) and normalises the optional `account_id` filter (trimmed; blank →
/// no filter). Returns `(account_id_filter, limit)`.
pub fn transfers_query_plan(account_id: Option<&str>, limit: Option<i64>) -> (Option<String>, i64) {
    let limit = limit
        .unwrap_or(TRANSFERS_DEFAULT_LIMIT)
        .clamp(1, TRANSFERS_MAX_LIMIT);
    let account_id = account_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    (account_id, limit)
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /transfers validation
// ─────────────────────────────────────────────────────────────────────────────

/// A validated + normalised transfer, ready for `BotRunStore::insert_transfer`.
/// `ts: None` lets the DB default the row to NOW(); a manual backfill entry
/// (the operator recording a past DCA deposit) may carry an explicit time.
#[derive(Debug, Clone, PartialEq)]
pub struct NewTransfer {
    pub account_id: String,
    /// Signed flow: positive = into the account (deposit), negative = out
    /// (withdrawal). Finite and non-zero — enforced by [`validate_transfer`].
    pub amount: f64,
    pub currency: String,
    pub kind: String,
    pub source: String,
    pub note: Option<String>,
    pub ts: Option<DateTime<Utc>>,
}

/// Validate + normalise a `POST /transfers` submission. Pure so the request
/// shaping is unit-testable without a database. Errors are operator-facing
/// 400 messages.
pub fn validate_transfer(req: &TransferRequest) -> Result<NewTransfer, String> {
    let account_id = req.account_id.trim();
    if account_id.is_empty() {
        return Err("account_id is required".to_string());
    }
    if account_id.len() > MAX_ID_LEN {
        return Err(format!("account_id too long (max {MAX_ID_LEN} chars)"));
    }

    // Serde already rejects JSON NaN/Infinity, but the guard is cheap and the
    // ledger must never hold a non-finite or zero flow.
    if !req.amount.is_finite() {
        return Err("amount must be a finite number".to_string());
    }
    if req.amount == 0.0 {
        return Err(
            "amount must be non-zero (positive = deposit in, negative = withdrawal out)"
                .to_string(),
        );
    }

    let kind = req.kind.trim().to_lowercase();
    if !TRANSFER_KINDS.contains(&kind.as_str()) {
        return Err(format!(
            "unknown kind '{kind}' (supported: deposit, withdrawal, payout, sweep)"
        ));
    }

    let source = req.source.trim().to_lowercase();
    if !TRANSFER_SOURCES.contains(&source.as_str()) {
        return Err(format!(
            "unknown source '{source}' (supported: manual, bot_detected)"
        ));
    }

    let currency = req.currency.trim().to_uppercase();
    if currency.is_empty() || currency.len() > 16 {
        return Err("currency must be 1-16 chars".to_string());
    }

    let note = req
        .note
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    Ok(NewTransfer {
        account_id: account_id.to_string(),
        amount: req.amount,
        currency,
        kind,
        source,
        note,
        ts: req.ts,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /accounts validation
// ─────────────────────────────────────────────────────────────────────────────

/// Validate a `POST /accounts` submission against the registry's allowlists
/// (tier range, role, compliance_flag; risk_caps/sizing must be JSON objects
/// when supplied). Returns the trimmed `account_id` on success. NO credential
/// fields exist on the request by design — keys live in exchange_secrets.
pub fn validate_account(req: &AccountRequest) -> Result<String, String> {
    let account_id = req.account_id.trim();
    if account_id.is_empty() {
        return Err("account_id is required".to_string());
    }
    if account_id.len() > MAX_ID_LEN {
        return Err(format!("account_id too long (max {MAX_ID_LEN} chars)"));
    }

    if !(0..=ACCOUNT_TIER_MAX).contains(&req.tier) {
        return Err(format!(
            "tier must be 0..={ACCOUNT_TIER_MAX} \
             (0=backbone, 1=personal-crypto, 2=rithmic-main, 3=prop-copy-target)"
        ));
    }

    if req.account_class.trim().is_empty() || req.account_class.len() > MAX_ID_LEN {
        return Err("account_class is required (1-128 chars)".to_string());
    }

    if !ACCOUNT_ROLES.contains(&req.role.trim()) {
        return Err(format!(
            "unknown role '{}' (supported: watch, bot-trade, human-trade-source, copy-target)",
            req.role
        ));
    }

    if !COMPLIANCE_FLAGS.contains(&req.compliance_flag.trim()) {
        return Err(format!(
            "unknown compliance_flag '{}' (supported: manual-mirror, auto-fill)",
            req.compliance_flag
        ));
    }

    for (name, v) in [("risk_caps", &req.risk_caps), ("sizing", &req.sizing)] {
        if let Some(v) = v
            && !v.is_object()
        {
            return Err(format!("{name} must be a JSON object"));
        }
    }

    Ok(account_id.to_string())
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /profit — the decomposition itself
// ─────────────────────────────────────────────────────────────────────────────

/// The deposits-vs-profit decomposition of one account's net-worth drift over
/// a window. All figures are in the account's snapshot currency (USD for the
/// bot totals). Serialises straight into the `GET /profit` response.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ProfitDecomposition {
    /// First net-worth snapshot in the window.
    pub start_net_worth: f64,
    /// Last net-worth snapshot in the window.
    pub end_net_worth: f64,
    /// `end_net_worth - start_net_worth` — the raw drift.
    pub delta: f64,
    /// Sum of positive transfer amounts in the window (money in).
    pub deposits_in: f64,
    /// Sum of |negative| transfer amounts in the window (money out, reported
    /// as a positive magnitude).
    pub withdrawals_out: f64,
    /// `deposits_in - withdrawals_out` — the signed external cash flow.
    pub net_inflows: f64,
    /// `delta - net_inflows` — the drift the account actually EARNED (trading
    /// profit), with deposits/withdrawals stripped out.
    pub profit: f64,
}

/// Decompose net-worth drift into deposits vs profit. Pure — the handler
/// feeds it the first/last snapshot in the window plus the signed transfer
/// amounts BETWEEN those snapshots (a flow already reflected in the first
/// snapshot must not be double-counted).
///
/// Example (deposit mid-window): net worth 100 → 160 with a 50 deposit in
/// between → delta 60, net inflows 50, profit +10.
pub fn decompose_profit(
    start_net_worth: f64,
    end_net_worth: f64,
    transfer_amounts: &[f64],
) -> ProfitDecomposition {
    let deposits_in: f64 = transfer_amounts.iter().filter(|a| **a > 0.0).sum();
    let withdrawals_out: f64 = transfer_amounts
        .iter()
        .filter(|a| **a < 0.0)
        .map(|a| -a)
        .sum();
    let delta = end_net_worth - start_net_worth;
    let net_inflows = deposits_in - withdrawals_out;
    ProfitDecomposition {
        start_net_worth,
        end_net_worth,
        delta,
        deposits_in,
        withdrawals_out,
        net_inflows,
        profit: delta - net_inflows,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure logic (no DB, no HTTP)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer_req(json: &str) -> TransferRequest {
        serde_json::from_str(json).expect("valid TransferRequest JSON")
    }

    fn account_req(json: &str) -> AccountRequest {
        serde_json::from_str(json).expect("valid AccountRequest JSON")
    }

    // ── profit decomposition ─────────────────────────────────────────────────

    #[test]
    fn profit_strips_a_mid_window_deposit() {
        // The canonical P0.4 scenario: NW 100 → 160 with a 50 deposit in the
        // window. Naive drift says +60; the true trading profit is +10.
        let d = decompose_profit(100.0, 160.0, &[50.0]);
        assert_eq!(d.delta, 60.0);
        assert_eq!(d.deposits_in, 50.0);
        assert_eq!(d.withdrawals_out, 0.0);
        assert_eq!(d.net_inflows, 50.0);
        assert_eq!(d.profit, 10.0);
    }

    #[test]
    fn profit_adds_back_a_withdrawal() {
        // NW 100 → 60 with 50 withdrawn: the account LOST 40 on paper but
        // actually earned +10 (the withdrawal explains -50 of the drift).
        let d = decompose_profit(100.0, 60.0, &[-50.0]);
        assert_eq!(d.delta, -40.0);
        assert_eq!(d.deposits_in, 0.0);
        assert_eq!(d.withdrawals_out, 50.0);
        assert_eq!(d.net_inflows, -50.0);
        assert_eq!(d.profit, 10.0);
    }

    #[test]
    fn profit_with_no_transfers_is_raw_delta() {
        let d = decompose_profit(100.0, 130.0, &[]);
        assert_eq!(d.delta, 30.0);
        assert_eq!(d.net_inflows, 0.0);
        assert_eq!(d.profit, 30.0);
    }

    #[test]
    fn profit_nets_mixed_flows() {
        // Two DCA deposits (+50, +25) and one withdrawal (-30) while NW went
        // 200 → 260: net inflows +45, so only +15 was earned.
        let d = decompose_profit(200.0, 260.0, &[50.0, -30.0, 25.0]);
        assert_eq!(d.deposits_in, 75.0);
        assert_eq!(d.withdrawals_out, 30.0);
        assert_eq!(d.net_inflows, 45.0);
        assert_eq!(d.delta, 60.0);
        assert_eq!(d.profit, 15.0);
    }

    #[test]
    fn profit_can_be_negative() {
        // Deposits masked a loss: NW 100 → 120 but 50 was deposited → -30.
        let d = decompose_profit(100.0, 120.0, &[50.0]);
        assert_eq!(d.profit, -30.0);
    }

    // ── transfers query plan ─────────────────────────────────────────────────

    #[test]
    fn transfers_query_plan_defaults_and_clamps_limit() {
        assert_eq!(transfers_query_plan(None, None).1, TRANSFERS_DEFAULT_LIMIT);
        assert_eq!(transfers_query_plan(None, Some(10)).1, 10);
        assert_eq!(transfers_query_plan(None, Some(0)).1, 1);
        assert_eq!(transfers_query_plan(None, Some(-5)).1, 1);
        assert_eq!(
            transfers_query_plan(None, Some(i64::MAX)).1,
            TRANSFERS_MAX_LIMIT
        );
    }

    #[test]
    fn transfers_query_plan_normalises_account_filter() {
        assert_eq!(transfers_query_plan(None, None).0, None);
        assert_eq!(transfers_query_plan(Some(""), None).0, None);
        assert_eq!(transfers_query_plan(Some("   "), None).0, None);
        assert_eq!(
            transfers_query_plan(Some("  spot-portfolio "), None).0,
            Some("spot-portfolio".to_string())
        );
    }

    // ── transfer validation ──────────────────────────────────────────────────

    #[test]
    fn transfer_minimal_deposit_validates_with_defaults() {
        let req =
            transfer_req(r#"{"account_id":" spot-portfolio ","amount":250.0,"kind":"deposit"}"#);
        let t = validate_transfer(&req).expect("valid transfer");
        assert_eq!(t.account_id, "spot-portfolio", "account_id is trimmed");
        assert_eq!(t.amount, 250.0);
        assert_eq!(t.kind, "deposit");
        assert_eq!(t.source, "manual", "source defaults to manual");
        assert_eq!(t.currency, "USD", "currency defaults to USD");
        assert!(t.note.is_none());
        assert!(t.ts.is_none(), "ts left to the DB default");
    }

    #[test]
    fn transfer_normalises_kind_source_currency_case() {
        let req = transfer_req(
            r#"{"account_id":"a","amount":-10.0,"kind":" Withdrawal ",
                "source":"BOT_DETECTED","currency":"usd","note":"  sweep out  "}"#,
        );
        let t = validate_transfer(&req).expect("valid transfer");
        assert_eq!(t.kind, "withdrawal");
        assert_eq!(t.source, "bot_detected");
        assert_eq!(t.currency, "USD");
        assert_eq!(t.note.as_deref(), Some("sweep out"));
    }

    #[test]
    fn transfer_accepts_explicit_backfill_ts() {
        let req = transfer_req(
            r#"{"account_id":"a","amount":100.0,"kind":"deposit",
                "ts":"2026-01-15T00:00:00Z"}"#,
        );
        let t = validate_transfer(&req).expect("valid transfer");
        assert_eq!(t.ts.unwrap().to_rfc3339(), "2026-01-15T00:00:00+00:00");
    }

    #[test]
    fn transfer_rejects_zero_and_non_finite_amounts() {
        let zero = transfer_req(r#"{"account_id":"a","amount":0.0,"kind":"deposit"}"#);
        assert!(validate_transfer(&zero).is_err(), "zero amount rejected");

        let mut nan = transfer_req(r#"{"account_id":"a","amount":1.0,"kind":"deposit"}"#);
        nan.amount = f64::NAN;
        assert!(validate_transfer(&nan).is_err(), "NaN rejected");
        nan.amount = f64::INFINITY;
        assert!(validate_transfer(&nan).is_err(), "Infinity rejected");
    }

    #[test]
    fn transfer_rejects_unknown_kind_and_source() {
        let bad_kind = transfer_req(r#"{"account_id":"a","amount":1.0,"kind":"donation"}"#);
        assert!(validate_transfer(&bad_kind).is_err());

        let bad_source = transfer_req(
            r#"{"account_id":"a","amount":1.0,"kind":"deposit","source":"telepathy"}"#,
        );
        assert!(validate_transfer(&bad_source).is_err());
    }

    #[test]
    fn transfer_rejects_blank_account_id() {
        let req = transfer_req(r#"{"account_id":"   ","amount":1.0,"kind":"deposit"}"#);
        assert!(validate_transfer(&req).is_err());
    }

    // ── account validation ───────────────────────────────────────────────────

    #[test]
    fn account_minimal_validates_with_defaults() {
        let req = account_req(
            r#"{"account_id":"kraken-main","tier":1,
                "account_class":"personal-crypto","role":"bot-trade"}"#,
        );
        assert_eq!(validate_account(&req).expect("valid"), "kraken-main");
        assert_eq!(req.compliance_flag, "manual-mirror", "safe default");
        assert!(req.active, "active defaults to true");
        assert!(req.risk_caps.is_none() && req.sizing.is_none());
    }

    #[test]
    fn account_rejects_out_of_range_tier() {
        for tier in [-1i16, 4] {
            let req = account_req(&format!(
                r#"{{"account_id":"x","tier":{tier},"account_class":"prop","role":"watch"}}"#
            ));
            assert!(validate_account(&req).is_err(), "tier {tier} rejected");
        }
    }

    #[test]
    fn account_rejects_unknown_role_and_compliance_flag() {
        let bad_role = account_req(
            r#"{"account_id":"x","tier":3,"account_class":"prop","role":"yolo-trade"}"#,
        );
        assert!(validate_account(&bad_role).is_err());

        let bad_flag = account_req(
            r#"{"account_id":"x","tier":3,"account_class":"prop",
                "role":"copy-target","compliance_flag":"full-auto"}"#,
        );
        assert!(validate_account(&bad_flag).is_err());
    }

    #[test]
    fn account_rejects_non_object_policy_json() {
        let req = account_req(
            r#"{"account_id":"x","tier":2,"account_class":"prop",
                "role":"human-trade-source","risk_caps":[1,2,3]}"#,
        );
        assert!(
            validate_account(&req).is_err(),
            "risk_caps must be an object"
        );
    }

    #[test]
    fn account_rejects_blank_id_and_class() {
        let no_id = account_req(
            r#"{"account_id":" ","tier":0,"account_class":"cold-storage","role":"watch"}"#,
        );
        assert!(validate_account(&no_id).is_err());

        let no_class = account_req(
            r#"{"account_id":"btc-cold","tier":0,"account_class":"  ","role":"watch"}"#,
        );
        assert!(validate_account(&no_class).is_err());
    }
}
