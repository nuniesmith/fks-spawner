//! Bot status + metrics HTTP server — the FKS bot contract.
//!
//! Serves three endpoints on `BOT_STATUS_PORT` (default **9091**, the port the
//! FKS stack's Prometheus scrapes; set `0`/`off` to disable):
//!
//! - `GET /health`  — liveness (`200 {"status":"ok"}`).
//! - `GET /metrics` — Prometheus text: the five series the FKS spawner harvests
//!   (`fks_bot_pnl_dollars`, `fks_bot_signals_total`, `fks_bot_trades_total`,
//!   `fks_bot_win_rate`, `fks_bot_uptime_seconds`) plus per-exchange balance /
//!   net-worth / position gauges.
//! - `GET /status`  — one JSON document with everything the web UI needs:
//!   mode, per-exchange balances + holdings, open positions, recent trades.
//!
//! The state lives in a process-global [`StatusState`] (one bot per process),
//! initialised by the binary's `main` via [`init`] and updated from anywhere via
//! [`get`] — so deep call sites (brains, fill observers) don't need a handle
//! threaded through every constructor. All updates are best-effort and cheap;
//! nothing here can block or fail a trade.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use tracing::{info, warn};

/// Recent trade events kept for `/status` (a small rolling window).
const MAX_EVENTS: usize = 50;

/// An `f64` stored as atomic bits (no locking for hot-path counters).
struct AtomicF64(AtomicU64);

impl AtomicF64 {
    fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }
    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed);
    }
    fn add(&self, delta: f64) {
        // CAS loop — contention here is a few updates/minute, not a hot path.
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(cur) + delta).to_bits();
            match self
                .0
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }
}

/// One asset slice inside a venue snapshot.
#[derive(Clone, Serialize)]
pub struct HoldingStatus {
    pub asset: String,
    pub qty: f64,
    pub price: f64,
    pub value: f64,
    /// Current weight of the portfolio (fraction of total value).
    pub weight: f64,
    /// Target weight after the reserve carve-out (fraction of total value).
    pub target_weight: f64,
}

/// A point-in-time snapshot of one exchange/venue.
#[derive(Clone, Serialize)]
pub struct VenueStatus {
    pub exchange: String,
    /// `paper` | `dry-run` | `live`.
    pub mode: String,
    pub cash_asset: String,
    /// Cash on hand, in the venue's cash currency (reported as USD-equivalent).
    pub cash: f64,
    /// Total portfolio value (holdings + cash), in the cash currency.
    pub total_value: f64,
    pub max_drift: f64,
    pub triggered: bool,
    pub last_rebalance: Option<u64>,
    pub updated: u64,
    pub holdings: Vec<HoldingStatus>,
}

/// A brain-tracked open futures position.
#[derive(Clone, Serialize)]
pub struct PositionStatus {
    pub symbol: String,
    /// +1 long / −1 short.
    pub dir: i8,
    pub entry_px: f64,
    pub entry_ts_ms: i64,
    pub mark_px: f64,
    /// Direction-signed open return, in percent.
    pub ret_pct: f64,
    pub updated: u64,
}

/// Process-wide bot status: counters + latest snapshots, shared by the HTTP
/// server and every update site.
pub struct StatusState {
    bot: String,
    /// `spot` | `futures` — lets the web UI group bots without guessing.
    market: &'static str,
    started: Instant,
    mode: RwLock<String>,
    signals: AtomicU64,
    trades: AtomicU64,
    wins: AtomicU64,
    losses: AtomicU64,
    /// Realized PnL from completed round trips (futures paper/live).
    realized_pnl: AtomicF64,
    /// Per-round-trip notional used to turn a paper return-% into dollars
    /// (futures: `margin_per_trade × leverage`, set by the engine).
    paper_notional: AtomicF64,
    /// Net-worth baseline, captured once every expected venue has reported —
    /// `pnl = realized + (net_worth − baseline)`. NAN until set. NB: for the
    /// spot bot, later deposits show up as PnL (no deposit ledger yet).
    baseline: AtomicF64,
    expected_venues: usize,
    venues: RwLock<BTreeMap<String, VenueStatus>>,
    positions: RwLock<BTreeMap<String, PositionStatus>>,
    events: RwLock<Vec<Value>>,
}

static STATUS: OnceLock<Arc<StatusState>> = OnceLock::new();

/// Create the process-global state (call once from `main`, before the engine).
/// Returns the existing state if already initialised.
pub fn init(bot: &str, market: &'static str, expected_venues: usize) -> Arc<StatusState> {
    STATUS
        .get_or_init(|| {
            Arc::new(StatusState {
                bot: bot.to_string(),
                market,
                started: Instant::now(),
                mode: RwLock::new("paper".to_string()),
                signals: AtomicU64::new(0),
                trades: AtomicU64::new(0),
                wins: AtomicU64::new(0),
                losses: AtomicU64::new(0),
                realized_pnl: AtomicF64::new(0.0),
                paper_notional: AtomicF64::new(0.0),
                baseline: AtomicF64::new(f64::NAN),
                expected_venues,
                venues: RwLock::new(BTreeMap::new()),
                positions: RwLock::new(BTreeMap::new()),
                events: RwLock::new(Vec::new()),
            })
        })
        .clone()
}

/// The global state, if `init` has run (deep call sites: a no-op `None` in
/// binaries that don't serve status — backtests, research bins).
pub fn get() -> Option<Arc<StatusState>> {
    STATUS.get().cloned()
}

impl StatusState {
    pub fn set_mode(&self, mode: &str) {
        if let Ok(mut m) = self.mode.write() {
            *m = mode.to_string();
        }
    }

    pub fn record_signal(&self) {
        self.signals.fetch_add(1, Ordering::Relaxed);
    }

    /// One executed order (entry, exit, scale-out, rebalance leg…).
    pub fn record_trade(&self) {
        self.trades.fetch_add(1, Ordering::Relaxed);
    }

    /// A completed round trip. `ret_frac` is the direction-signed **gross**
    /// price return (fraction, not %) and drives the dollar conversion via the
    /// paper notional. `net_pnl` — the trade's honest fees-included PnL (the
    /// ledger's `net_pnl_usdt` = gross − fees + funding) — decides the W/L
    /// classification when known: a trade whose gross return is positive but
    /// nets a loss after fees counts as a **LOSS**, matching the ledger's
    /// fees-decide-the-honest-W/L contract (the stat the Gate-A soak judges).
    /// Falls back to the gross sign only when `net_pnl` is `None` (legacy
    /// records that predate net booking).
    pub fn record_round_trip(&self, ret_frac: f64, net_pnl: Option<f64>) {
        // Win on the honest (fees-included) result when the net is known; the
        // gross sign is only a fallback. Both are compared by sign, so the
        // unit difference (net = dollars, ret_frac = fraction) is irrelevant.
        if net_pnl.unwrap_or(ret_frac) > 0.0 {
            self.wins.fetch_add(1, Ordering::Relaxed);
        } else {
            self.losses.fetch_add(1, Ordering::Relaxed);
        }
        let notional = self.paper_notional.get();
        if notional > 0.0 {
            self.realized_pnl.add(ret_frac * notional);
        }
    }

    /// Set the per-trade notional used to turn paper return-% into dollars.
    pub fn set_paper_notional(&self, usd: f64) {
        self.paper_notional.set(usd.max(0.0));
    }

    /// Upsert a venue snapshot; captures the PnL baseline once all expected
    /// venues have reported.
    pub fn update_venue(&self, v: VenueStatus) {
        let (count, net) = {
            let Ok(mut map) = self.venues.write() else {
                return;
            };
            map.insert(v.exchange.clone(), v);
            (map.len(), map.values().map(|v| v.total_value).sum::<f64>())
        };
        if self.baseline.get().is_nan() && count >= self.expected_venues.max(1) && net > 0.0 {
            self.baseline.set(net);
        }
    }

    /// Set (`Some`) or clear (`None`) an open position for a symbol.
    pub fn set_position(&self, symbol: &str, pos: Option<PositionStatus>) {
        let Ok(mut map) = self.positions.write() else {
            return;
        };
        match pos {
            Some(p) => {
                map.insert(symbol.to_string(), p);
            }
            None => {
                map.remove(symbol);
            }
        }
    }

    /// Refresh an open position's mark price + open return (no-op when flat).
    pub fn mark_position(&self, symbol: &str, mark_px: f64) {
        let Ok(mut map) = self.positions.write() else {
            return;
        };
        if let Some(p) = map.get_mut(symbol)
            && p.entry_px > 0.0
        {
            p.mark_px = mark_px;
            p.ret_pct = p.dir as f64 * (mark_px / p.entry_px - 1.0) * 100.0;
            p.updated = now_secs();
        }
    }

    /// Append a trade event to the rolling `/status` window.
    pub fn push_event(&self, mut event: Value) {
        if let Value::Object(map) = &mut event {
            map.entry("ts").or_insert(json!(now_secs()));
        }
        let Ok(mut ev) = self.events.write() else {
            return;
        };
        ev.push(event);
        let len = ev.len();
        if len > MAX_EVENTS {
            ev.drain(..len - MAX_EVENTS);
        }
    }

    /// Sum of the venue totals (the "all exchanges" number).
    pub fn net_worth(&self) -> f64 {
        self.venues
            .read()
            .map(|m| m.values().map(|v| v.total_value).sum())
            .unwrap_or(0.0)
    }

    /// Realized round-trip PnL + net-worth change since the baseline snapshot.
    pub fn pnl_dollars(&self) -> f64 {
        let baseline = self.baseline.get();
        let drift = if baseline.is_nan() {
            0.0
        } else {
            self.net_worth() - baseline
        };
        self.realized_pnl.get() + drift
    }

    fn win_rate(&self) -> f64 {
        let w = self.wins.load(Ordering::Relaxed) as f64;
        let l = self.losses.load(Ordering::Relaxed) as f64;
        if w + l > 0.0 { w / (w + l) } else { 0.0 }
    }

    /// Render the Prometheus exposition text.
    fn render_metrics(&self) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(2048);
        let bot = &self.bot;
        let market = self.market;
        let mut gauge = |name: &str, labels: &str, v: f64| {
            let sep = if labels.is_empty() {
                String::new()
            } else {
                format!(",{labels}")
            };
            let _ = writeln!(out, "{name}{{bot=\"{bot}\",market=\"{market}\"{sep}}} {v}");
        };

        // The five series the FKS spawner/Prometheus contract requires.
        gauge(
            "fks_bot_uptime_seconds",
            "",
            self.started.elapsed().as_secs_f64(),
        );
        gauge("fks_bot_pnl_dollars", "", self.pnl_dollars());
        gauge(
            "fks_bot_signals_total",
            "",
            self.signals.load(Ordering::Relaxed) as f64,
        );
        gauge(
            "fks_bot_trades_total",
            "",
            self.trades.load(Ordering::Relaxed) as f64,
        );
        gauge("fks_bot_win_rate", "", self.win_rate());

        // Balances / net worth (values in each venue's cash currency, treated
        // as USD-equivalent).
        gauge("fks_bot_net_worth_usd", "", self.net_worth());
        if let Ok(venues) = self.venues.read() {
            for v in venues.values() {
                let ex = format!("exchange=\"{}\"", v.exchange);
                gauge("fks_bot_exchange_total_usd", &ex, v.total_value);
                gauge("fks_bot_exchange_cash_usd", &ex, v.cash);
                for h in &v.holdings {
                    let labels = format!("exchange=\"{}\",asset=\"{}\"", v.exchange, h.asset);
                    gauge("fks_bot_asset_value_usd", &labels, h.value);
                    gauge("fks_bot_asset_qty", &labels, h.qty);
                }
            }
        }

        // Open futures positions.
        if let Ok(positions) = self.positions.read() {
            for p in positions.values() {
                let sym = format!("symbol=\"{}\"", p.symbol);
                gauge("fks_bot_position_dir", &sym, p.dir as f64);
                gauge("fks_bot_position_return_pct", &sym, p.ret_pct);
                gauge("fks_bot_position_entry_px", &sym, p.entry_px);
            }
        }
        out
    }

    /// The `/status` JSON document.
    fn status_json(&self) -> Value {
        let venues: Vec<VenueStatus> = self
            .venues
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        let positions: Vec<PositionStatus> = self
            .positions
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        let events = self.events.read().map(|e| e.clone()).unwrap_or_default();
        json!({
            "bot": self.bot,
            "market": self.market,
            "mode": self.mode.read().map(|m| m.clone()).unwrap_or_default(),
            "uptime_secs": self.started.elapsed().as_secs(),
            "net_worth_usd": self.net_worth(),
            "pnl_usd": self.pnl_dollars(),
            "signals_total": self.signals.load(Ordering::Relaxed),
            "trades_total": self.trades.load(Ordering::Relaxed),
            "wins": self.wins.load(Ordering::Relaxed),
            "losses": self.losses.load(Ordering::Relaxed),
            "win_rate": self.win_rate(),
            "exchanges": venues,
            "positions": positions,
            "recent_events": events,
        })
    }
}

/// Hook for the funding brain's self-contained paper-trade records: parse the
/// `action` and keep counters + the open-position snapshot in sync. A no-op
/// when [`init`] hasn't run (backtests, research bins).
pub fn observe_paper_event(v: &Value) {
    let Some(status) = get() else { return };
    status.push_event(v.clone());
    // In live mode real fills flow through `observe_fill`, which counts the
    // trades — counting them here too would double them. In paper there is no
    // fill source, so this hook is the only counter.
    let live = status.mode.read().map(|m| *m == "live").unwrap_or(false);
    let sym = v["sym"].as_str().unwrap_or("");
    match v["action"].as_str() {
        Some("entry") => {
            status.record_signal();
            if !live {
                status.record_trade();
            }
            let entry_px = v["entry_px"].as_f64().unwrap_or(0.0);
            status.set_position(
                sym,
                Some(PositionStatus {
                    symbol: sym.to_string(),
                    dir: v["dir"].as_i64().unwrap_or(0) as i8,
                    entry_px,
                    entry_ts_ms: v["t"].as_i64().unwrap_or(0),
                    mark_px: entry_px,
                    ret_pct: 0.0,
                    updated: now_secs(),
                }),
            );
        }
        Some("exit") | Some("stop_exit") => {
            if !live {
                status.record_trade();
            }
            // W/L is decided by the honest net PnL the brain booked
            // (`net_pnl_usdt`); the gross `ret_pct` still drives the dollar
            // conversion via the paper notional.
            let ret_frac = v["ret_pct"].as_f64().unwrap_or(0.0) / 100.0;
            status.record_round_trip(ret_frac, v["net_pnl_usdt"].as_f64());
            status.set_position(sym, None);
        }
        _ => {}
    }
}

/// Start the status HTTP server on `BOT_STATUS_PORT` (fallback
/// `BOT_METRICS_PORT`; default 9091; `0`/`off` disables). Never fails the bot:
/// a bind error is logged and the bot runs without the endpoint.
pub fn serve(state: Arc<StatusState>) {
    let port = std::env::var("BOT_STATUS_PORT")
        .or_else(|_| std::env::var("BOT_METRICS_PORT"))
        .unwrap_or_else(|_| "9091".to_string());
    if port == "0" || port.eq_ignore_ascii_case("off") {
        info!("status server disabled (BOT_STATUS_PORT=off)");
        return;
    }
    let Ok(port) = port.parse::<u16>() else {
        warn!(port, "status server: invalid BOT_STATUS_PORT — disabled");
        return;
    };

    tokio::spawn(async move {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => {
                info!(%addr, "status server listening (/health /metrics /status)");
                l
            }
            Err(e) => {
                warn!(error = %e, %addr, "status server: bind failed — running without it");
                return;
            }
        };
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let state = state.clone();
                    async move { Ok::<_, std::convert::Infallible>(respond(&state, &req)) }
                });
                // Serve one connection; errors (client hangups) are non-fatal.
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
}

/// Route one request (GET only; anything unknown → 404).
fn respond(state: &StatusState, req: &Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
    let (status, content_type, body) = match (req.method().as_str(), req.uri().path()) {
        ("GET", "/health") => (
            StatusCode::OK,
            "application/json",
            r#"{"status":"ok"}"#.to_string(),
        ),
        ("GET", "/metrics") => (
            StatusCode::OK,
            "text/plain; version=0.0.4",
            state.render_metrics(),
        ),
        ("GET", "/status") => (
            StatusCode::OK,
            "application/json",
            state.status_json().to_string(),
        ),
        _ => (
            StatusCode::NOT_FOUND,
            "application/json",
            r#"{"error":"not found"}"#.to_string(),
        ),
    };
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_default()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> StatusState {
        StatusState {
            bot: "test-bot".into(),
            market: "spot",
            started: Instant::now(),
            mode: RwLock::new("dry-run".into()),
            signals: AtomicU64::new(0),
            trades: AtomicU64::new(0),
            wins: AtomicU64::new(0),
            losses: AtomicU64::new(0),
            realized_pnl: AtomicF64::new(0.0),
            paper_notional: AtomicF64::new(0.0),
            baseline: AtomicF64::new(f64::NAN),
            expected_venues: 2,
            venues: RwLock::new(BTreeMap::new()),
            positions: RwLock::new(BTreeMap::new()),
            events: RwLock::new(Vec::new()),
        }
    }

    fn venue(name: &str, total: f64) -> VenueStatus {
        VenueStatus {
            exchange: name.into(),
            mode: "dry-run".into(),
            cash_asset: "USD".into(),
            cash: total / 10.0,
            total_value: total,
            max_drift: 0.01,
            triggered: false,
            last_rebalance: None,
            updated: 0,
            holdings: vec![HoldingStatus {
                asset: "BTC".into(),
                qty: 0.5,
                price: 100.0,
                value: 50.0,
                weight: 0.5,
                target_weight: 0.45,
            }],
        }
    }

    #[test]
    fn net_worth_sums_venues_and_baseline_waits_for_all() {
        let s = fresh();
        s.update_venue(venue("kraken", 100.0));
        // Only 1 of 2 venues reported → no baseline, PnL is realized-only.
        assert_eq!(s.net_worth(), 100.0);
        assert!(s.baseline.get().is_nan());
        assert_eq!(s.pnl_dollars(), 0.0);

        s.update_venue(venue("kucoin", 50.0));
        assert_eq!(s.net_worth(), 150.0);
        assert_eq!(s.baseline.get(), 150.0);

        // The venue's value moves → PnL is the drift from the baseline.
        s.update_venue(venue("kucoin", 60.0));
        assert!((s.pnl_dollars() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn round_trips_drive_win_rate_and_dollar_pnl() {
        let s = fresh();
        s.set_paper_notional(1000.0);
        // Gross +2% nets a win after fees; gross −1% nets a loss.
        s.record_round_trip(0.02, Some(19.4)); // +$20 gross realized, net WIN
        s.record_round_trip(-0.01, Some(-10.6)); // −$10 gross realized, net LOSS
        assert_eq!(s.wins.load(Ordering::Relaxed), 1);
        assert_eq!(s.losses.load(Ordering::Relaxed), 1);
        assert!((s.win_rate() - 0.5).abs() < 1e-9);
        // Dollar realized PnL stays gross-of-fees (rides the notional): +20 − 10.
        assert!((s.realized_pnl.get() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn gross_positive_but_net_negative_counts_as_a_loss() {
        // The Gate-A honest-W/L contract: a close whose GROSS price return is
        // positive but whose NET (fees-included) PnL is negative must count as
        // a LOSS. +0.1% gross on 3000 notional = +$3.00 gross, but −$0.60 net
        // after 12bps fees — the ledger books a net loss, so /status must too.
        let s = fresh();
        s.record_round_trip(0.001, Some(-0.60));
        assert_eq!(s.wins.load(Ordering::Relaxed), 0, "gross-win/net-loss is a loss");
        assert_eq!(s.losses.load(Ordering::Relaxed), 1);
        assert_eq!(s.win_rate(), 0.0);

        // Mirror: a gross loser that somehow nets positive is a WIN.
        s.record_round_trip(-0.001, Some(0.60));
        assert_eq!(s.wins.load(Ordering::Relaxed), 1);
        assert_eq!(s.losses.load(Ordering::Relaxed), 1);

        // No net booked (legacy record) → fall back to the gross sign.
        s.record_round_trip(0.02, None);
        assert_eq!(s.wins.load(Ordering::Relaxed), 2);
        assert_eq!(s.losses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn metrics_render_the_required_series() {
        let s = fresh();
        s.update_venue(venue("kraken", 100.0));
        let m = s.render_metrics();
        for required in [
            "fks_bot_uptime_seconds",
            "fks_bot_pnl_dollars",
            "fks_bot_signals_total",
            "fks_bot_trades_total",
            "fks_bot_win_rate",
            "fks_bot_net_worth_usd",
        ] {
            assert!(m.contains(required), "missing {required} in:\n{m}");
        }
        assert!(m.contains(
            r#"fks_bot_exchange_total_usd{bot="test-bot",market="spot",exchange="kraken"} 100"#
        ));
        assert!(m.contains(r#"asset="BTC""#));
    }

    #[test]
    fn positions_are_marked_and_cleared() {
        let s = fresh();
        s.set_position(
            "ETHUSDTM",
            Some(PositionStatus {
                symbol: "ETHUSDTM".into(),
                dir: -1,
                entry_px: 2000.0,
                entry_ts_ms: 0,
                mark_px: 2000.0,
                ret_pct: 0.0,
                updated: 0,
            }),
        );
        // Short from 2000, mark 1900 → +5% signed return.
        s.mark_position("ETHUSDTM", 1900.0);
        {
            let map = s.positions.read().unwrap();
            let p = map.get("ETHUSDTM").unwrap();
            assert!((p.ret_pct - 5.0).abs() < 1e-9);
        }
        s.set_position("ETHUSDTM", None);
        assert!(s.positions.read().unwrap().is_empty());
    }

    #[test]
    fn event_window_is_capped() {
        let s = fresh();
        for i in 0..(MAX_EVENTS + 10) {
            s.push_event(json!({ "n": i }));
        }
        let ev = s.events.read().unwrap();
        assert_eq!(ev.len(), MAX_EVENTS);
        assert_eq!(ev[0]["n"], 10_u64); // oldest were dropped
    }
}
