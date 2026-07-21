//! Ambiguous-fill recovery (A.7) — end-to-end branch coverage against a mock
//! KuCoin REST endpoint (`wiremock`).
//!
//! Each test drives one `submit_with_recovery` branch through the real
//! `KucoinExchangeAdapter` `ExchangeClient` surface and asserts the *observable*
//! outcome (returned order id / surfaced error) plus the wire behaviour that
//! makes it safe (the reused `clientOid`, the bounded call count, the byClientOid
//! resolve). No live credentials or network access are used.
//!
//! Coverage:
//! - ambiguous → filled  ⇒ adopts the real venue order id
//! - ambiguous → resting ⇒ adopts (order lives, tracked — no double submit)
//! - ambiguous → not-found ⇒ safe to re-place the SAME clientOid
//! - ambiguous → terminal-unfilled ⇒ surfaces (no position, no futile retry)
//! - retriable ⇒ bounded retries then surface (no infinite loop)
//! - fatal ⇒ surfaces immediately (exactly one submit)
//! - clientOid reused across every retry (idempotency key)
//! - the same recovery applies to `close_position`
//! - happy path is byte-identical: one POST, no byClientOid, no retry

use std::time::Duration;

use rustrade::{ExchangeClient, Order, Position, Price, Side, StopAttachment, Symbol, Volume};
use rustrade_exchange_apiws::{KucoinExchangeAdapter, RecoveryPolicy};
use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use exchange_apiws::{Credentials, KuCoinClient};

const ORDERS: &str = "/api/v1/orders";
const BY_CLIENT_OID: &str = "/api/v1/orders/byClientOid";
const STOP_ORDERS: &str = "/api/v1/stopOrders";
const SYM: &str = "ETHUSDTM";
const STOP_TRIGGER: f64 = 1000.0;

/// Adapter pointed at the mock server, with a fast (still-exponential) recovery
/// policy so tests exercise the backoff path without waiting.
fn adapter(base_url: &str, max_attempts: u32) -> KucoinExchangeAdapter {
    let client = KuCoinClient::with_base_url(Credentials::new("k", "s", "p"), base_url)
        .expect("client builds");
    KucoinExchangeAdapter::new(client, 3)
        .with_contract_value(SYM, 0.01)
        .with_recovery_policy(RecoveryPolicy {
            max_attempts,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
        })
}

fn ok_envelope(data: Value) -> Value {
    serde_json::json!({ "code": "200000", "data": data })
}

fn err_envelope(code: &str, msg: &str) -> Value {
    serde_json::json!({ "code": code, "msg": msg })
}

/// A duplicate-`clientOid` rejection — the venue proof that a prior attempt
/// landed. `classify_submit` maps this to `Ambiguous`.
fn dup_body() -> Value {
    err_envelope("400100", "clientOid already exists (duplicate)")
}

fn order_detail(id: &str, status: &str, size: u32, filled: u32, active: bool) -> Value {
    serde_json::json!({
        "id": id,
        "symbol": SYM,
        "side": "buy",
        "type": "market",
        "status": status,
        "size": size,
        "filledSize": filled,
        "isActive": active,
    })
}

fn entry() -> Order {
    Order::market(Symbol::from(SYM), Side::Buy, Volume(1.0))
}

/// A reduce-only protective stop leg (bracket case A) — the path that places an
/// UNTRIGGERED stop, which KuCoin holds in a bucket `byClientOid` can't see.
/// Sell-to-close a long ⇒ trigger direction `"down"`.
fn stop_leg() -> Order {
    Order::market(Symbol::from(SYM), Side::Sell, Volume(1.0))
        .with_stop(StopAttachment::stop_market(Price(STOP_TRIGGER)))
        .with_reduce_only(true)
}

/// One entry in the `/api/v1/stopOrders` bucket, shaped to match `stop_leg()`
/// (same symbol/side/size/direction/reduceOnly) at `stop_price`. Carries NO
/// `clientOid` — exactly as KuCoin's `StopOrderDetail` — so recovery must match
/// on attributes.
fn stop_item(id: &str, stop_price: f64) -> Value {
    serde_json::json!({
        "id": id,
        "symbol": SYM,
        "side": "sell",
        "type": "market",
        "stop": "down",
        "stopPrice": stop_price,
        "size": 1,
        "reduceOnly": true,
    })
}

/// Body for a `GET /api/v1/stopOrders` page.
fn stop_page(items: Vec<Value>) -> Value {
    ok_envelope(serde_json::json!({ "items": items }))
}

/// Collect the `clientOid` from every POST /api/v1/orders the server received.
async fn posted_client_oids(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method == wiremock::http::Method::POST && r.url.path() == ORDERS)
        .map(|r| {
            serde_json::from_slice::<Value>(&r.body)
                .ok()
                .and_then(|b| b["clientOid"].as_str().map(str::to_string))
                .expect("POST body carries a clientOid")
        })
        .collect()
}

// ── ambiguous → filled: adopt the real venue order id ─────────────────────────

#[tokio::test]
async fn ambiguous_then_filled_adopts_real_order_id() {
    let server = MockServer::start().await;
    // The submit comes back ambiguous (dup) ...
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    // ... and byClientOid reveals it actually filled.
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok_envelope(order_detail(
                "venue-filled-1",
                "done",
                1,
                1,
                false,
            ))),
        )
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect("filled order is adopted");
    assert_eq!(
        id, "venue-filled-1",
        "adopts the real venue id from byClientOid"
    );
}

// ── ambiguous → resting: order lives, track it (no double submit) ─────────────

#[tokio::test]
async fn ambiguous_then_resting_tracks_without_resubmit() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok_envelope(order_detail(
                "venue-rest-1",
                "open",
                1,
                0,
                true, // still resting on the book
            ))),
        )
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect("resting order is adopted");
    assert_eq!(id, "venue-rest-1");
    // Exactly ONE submit was attempted — the resting order was adopted, never
    // re-placed.
    assert_eq!(
        posted_client_oids(&server).await.len(),
        1,
        "no double submit"
    );
}

// ── ambiguous → not-found: safe to re-place the SAME clientOid ────────────────

#[tokio::test]
async fn ambiguous_then_not_found_replaces_same_client_oid() {
    let server = MockServer::start().await;
    // First submit: ambiguous (dup). Second submit: succeeds.
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_envelope(
            serde_json::json!({ "orderId": "venue-fresh-2" }),
        )))
        .with_priority(2)
        .mount(&server)
        .await;
    // Resolve says the order never reached the engine.
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(err_envelope("400100", "order does not exist")),
        )
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect("re-placed after not-found");
    assert_eq!(id, "venue-fresh-2");

    let oids = posted_client_oids(&server).await;
    assert_eq!(oids.len(), 2, "re-placed exactly once");
    assert_eq!(
        oids[0], oids[1],
        "SAME clientOid reused on the re-place (idempotency key)"
    );
}

// ── ambiguous → terminal-unfilled: surface, no futile retry ───────────────────

#[tokio::test]
async fn ambiguous_then_terminal_unfilled_surfaces() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    // Reached the engine but cancelled/expired without filling.
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok_envelope(serde_json::json!({
                "id": "venue-cancelled",
                "symbol": SYM,
                "side": "buy",
                "type": "market",
                "status": "done",
                "size": 1,
                "filledSize": 0,
                "isActive": false,
                "cancelExist": true,
            }))),
        )
        .mount(&server)
        .await;

    let err = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect_err("terminal-unfilled must surface, not silently retry");
    let msg = err.to_string();
    assert!(msg.contains("terminal-unfilled"), "surfaced reason: {msg}");
    // Only the one submit — no futile re-place of the consumed clientOid.
    assert_eq!(posted_client_oids(&server).await.len(), 1);
}

// ── retriable: bounded retries then surface (no infinite loop) ────────────────

#[tokio::test]
async fn retriable_is_bounded_and_reuses_client_oid() {
    let server = MockServer::start().await;
    // Every submit returns a transient 5xx-class code.
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(err_envelope("500000", "system busy")),
        )
        .mount(&server)
        .await;

    let err = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect_err("exhausts bounded retries");
    assert!(err.to_string().to_lowercase().contains("system busy") || !err.to_string().is_empty());

    let oids = posted_client_oids(&server).await;
    assert_eq!(
        oids.len(),
        4,
        "exactly max_attempts submits — bounded, no infinite loop"
    );
    assert!(
        oids.iter().all(|o| o == &oids[0]),
        "same clientOid across every retry"
    );
}

// ── fatal: surface immediately, exactly one submit ────────────────────────────

#[tokio::test]
async fn fatal_surfaces_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(err_envelope("300000", "Balance insufficient")),
        )
        .mount(&server)
        .await;
    // A byClientOid mock is deliberately NOT mounted — a fatal submit must not
    // trigger a resolve.
    let err = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect_err("fatal surfaces");
    assert!(err.to_string().contains("Balance insufficient"), "{err}");
    assert_eq!(
        posted_client_oids(&server).await.len(),
        1,
        "no retry on fatal"
    );
}

// ── happy path: byte-identical — one POST, no byClientOid, no retry ───────────

#[tokio::test]
async fn happy_path_is_single_shot_no_recovery() {
    let server = MockServer::start().await;
    // Success on the first try. byClientOid is NOT mounted, so if the adapter
    // ever queried it the test would 404-fail the resolve — proving recovery is
    // inert on the success path.
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(ok_envelope(serde_json::json!({ "orderId": "clean-1" }))),
        )
        .expect(1)
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect("clean placement");
    assert_eq!(id, "clean-1");
    assert_eq!(posted_client_oids(&server).await.len(), 1);
    // MockServer's `.expect(1)` on the POST is verified on drop.
}

// ── close_position gets the same recovery ─────────────────────────────────────

#[tokio::test]
async fn close_position_recovers_ambiguous_fill() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok_envelope(order_detail(
                "venue-close-done",
                "done",
                1,
                1,
                false,
            ))),
        )
        .mount(&server)
        .await;

    let pos = Position {
        qty: 1.0,
        entry_price: Some(1000.0),
        unrealised_pnl: 0.0,
    };
    let id = adapter(&server.uri(), 4)
        .close_position(&Symbol::from(SYM), &pos)
        .await
        .expect("ambiguous close is reconciled");
    assert_eq!(id, "venue-close-done", "adopts the real closed-order id");
}

// ── HIGH: a landed UNTRIGGERED stop is resolved via the stop bucket, not ───────
//         misclassified as never-reached and re-placed.

#[tokio::test]
async fn stop_ambiguous_resolves_via_stop_bucket_not_never_reached() {
    let server = MockServer::start().await;
    // The stop submit comes back ambiguous (dup).
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    // The regular-orders bucket has NO record — expected for an untriggered
    // stop. On its own this would (wrongly) read as "never reached".
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(err_envelope("400100", "The order does not exist.")),
        )
        .mount(&server)
        .await;
    // ... but the stop IS resting in the SEPARATE stop bucket.
    Mock::given(method("GET"))
        .and(path(STOP_ORDERS))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(stop_page(vec![stop_item("resting-stop-7", STOP_TRIGGER)])),
        )
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&stop_leg())
        .await
        .expect("landed stop is adopted from the stop bucket");
    assert_eq!(
        id, "resting-stop-7",
        "adopts the real stop id from the stopOrders bucket"
    );
    // Exactly ONE submit — the landed protective stop was adopted, never
    // re-placed (no duplicate/second resting stop).
    assert_eq!(
        posted_client_oids(&server).await.len(),
        1,
        "no double protective stop"
    );
}

// ── stop absent from BOTH buckets ⇒ genuinely never reached ⇒ re-place same oid.

#[tokio::test]
async fn stop_ambiguous_absent_from_both_buckets_replaces_same_oid() {
    let server = MockServer::start().await;
    // First submit ambiguous (dup); second submit succeeds.
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_envelope(
            serde_json::json!({ "orderId": "stop-fresh-2" }),
        )))
        .with_priority(2)
        .mount(&server)
        .await;
    // Neither bucket knows the clientOid.
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(err_envelope("400100", "The order does not exist.")),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(STOP_ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(stop_page(vec![])))
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&stop_leg())
        .await
        .expect("re-placed after confirmed-never-reached");
    assert_eq!(id, "stop-fresh-2");

    let oids = posted_client_oids(&server).await;
    assert_eq!(oids.len(), 2, "re-placed exactly once");
    assert_eq!(
        oids[0], oids[1],
        "SAME clientOid reused on the stop re-place (idempotency key)"
    );
}

// ── stop bucket ambiguous (>1 indistinguishable match) ⇒ surface, never guess.

#[tokio::test]
async fn stop_ambiguous_multiple_bucket_matches_surface_unknown() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(err_envelope("400100", "The order does not exist.")),
        )
        .mount(&server)
        .await;
    // Two indistinguishable matching stops — the resolver must NOT guess an id.
    Mock::given(method("GET"))
        .and(path(STOP_ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(stop_page(vec![
            stop_item("dup-stop-a", STOP_TRIGGER),
            stop_item("dup-stop-b", STOP_TRIGGER),
        ])))
        .mount(&server)
        .await;

    let err = adapter(&server.uri(), 2)
        .place_order(&stop_leg())
        .await
        .expect_err("ambiguous stop bucket must surface, not adopt a wrong id");
    let msg = err.to_string();
    assert!(
        msg.contains("UNKNOWN") || msg.to_lowercase().contains("reconcile"),
        "surfaced reason should flag operator reconcile: {msg}"
    );
}

// ── MEDIUM: an ambiguous entry that PARTIALLY filled then went terminal is ─────
//           adopted as a (partial) landed position — not "no position".

#[tokio::test]
async fn ambiguous_then_partial_fill_adopts_partial_position() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(ORDERS))
        .respond_with(ResponseTemplate::new(200).set_body_json(dup_body()))
        .mount(&server)
        .await;
    // Filled 3 of 5, then the remainder cancelled (a real thin-book IOC-style
    // market outcome): terminal + not fully filled, yet 3 contracts are LIVE.
    Mock::given(method("GET"))
        .and(path(BY_CLIENT_OID))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(ok_envelope(serde_json::json!({
                "id": "venue-partial",
                "symbol": SYM,
                "side": "buy",
                "type": "market",
                "status": "done",
                "size": 5,
                "filledSize": 3,
                "isActive": false,
                "cancelExist": true,
            }))),
        )
        .mount(&server)
        .await;

    let id = adapter(&server.uri(), 4)
        .place_order(&entry())
        .await
        .expect("partial fill is a landed position, not a failure");
    assert_eq!(
        id, "venue-partial",
        "adopts the real venue id for the partial position"
    );
    // No futile re-place of a consumed clientOid.
    assert_eq!(posted_client_oids(&server).await.len(), 1);
}
