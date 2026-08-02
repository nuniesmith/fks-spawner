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
    /// USD notional the position was opened at, when the brain supplies one
    /// (the funding brain's `book.notional_usdt()`, also stamped into its
    /// ledger records as `notional_usdt`).
    ///
    /// `ret_pct` alone is a RATIO — it cannot be added to a dollar balance.
    /// Without a notional the position's dollar value is unknowable from this
    /// document, so [`StatusState::net_worth`] reports the total as INCOMPLETE
    /// rather than quietly publishing pre-trade cash as net worth.
    pub notional_usd: Option<f64>,
}

/// A bot's net worth, DECOMPOSED so no consumer has to guess what the headline
/// figure means — and so two bots that mean different things can never be
/// silently summed.
///
/// Before this existed, `/status.net_worth_usd` was `Σ exchanges[].total_value`
/// for every bot. For the SPOT bot that is a genuine mark-to-market portfolio
/// value (`cash + Σ holdings[].value`). For the FUTURES/funding bot the venue
/// snapshot books REALIZED equity — it moves only when a round trip closes —
/// and the open trade lives in a separate `positions[]` array that the total
/// never touched. Two different quantities under one field name, summed
/// together by the spawner's treasury roll-up.
///
/// Observed 2026-08-02 on the live paper funding bot: `net_worth_usd`
/// 11288.31 with an open AVAXUSDTM long at +6.89% on 3000 USDT notional —
/// about +207 USDT (1.8%) of the book absent from the "net worth". The sign
/// is the point: an ADVERSE open position makes the treasury show NO DRAWDOWN
/// AT ALL until the position closes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetWorth {
    /// The headline figure published as `net_worth_usd`. Equal to
    /// `venue_total_usd` unless open positions had to be folded in.
    pub total_usd: f64,
    /// `Σ exchanges[].total_value` — the venue-reported component.
    pub venue_total_usd: f64,
    /// Mark-to-market of the open positions. `None` when at least one open
    /// position carries no notional, i.e. the book CANNOT be valued.
    ///
    /// NB: this is a decomposition, not an addend. When `venue_total_usd`
    /// already marks positions to market (a live exchange's account equity
    /// does), this value is already inside it — which is exactly why
    /// [`StatusState::set_venue_total_marks_positions`] exists.
    pub unrealized_pnl_usd: Option<f64>,
    /// How many positions are open.
    pub open_positions: usize,
    /// Does `total_usd` account for every open position? `false` means the
    /// figure is NOT this bot's net worth and must not be recorded as one.
    pub complete: bool,
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
    /// Does the venue-reported total ALREADY mark open positions to market?
    /// Only the bot knows: a live futures venue reports exchange account
    /// equity (which includes unrealised PnL), while a paper ledger books
    /// realized cash. `None` = undeclared, and an undeclared basis with an
    /// open position is not a net worth — see [`StatusState::net_worth`].
    venue_total_marks_positions: RwLock<Option<bool>>,
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
                venue_total_marks_positions: RwLock::new(None),
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

    /// Declare whether the venue-reported total ALREADY marks open positions
    /// to market.
    ///
    /// - `true`  — the venue figure is an exchange account equity (or any
    ///   mark-to-market total). Open positions are already inside it; folding
    ///   the unrealised PnL in again would DOUBLE-COUNT it.
    /// - `false` — the venue figure is realized cash (a paper ledger's booked
    ///   equity). Open positions must be folded in to get a net worth.
    ///
    /// Undeclared is neither: a bot that has not called this and is holding an
    /// open position reports `complete: false`, because nothing in this
    /// process can tell the two bases apart from outside.
    pub fn set_venue_total_marks_positions(&self, marks: bool) {
        if let Ok(mut m) = self.venue_total_marks_positions.write() {
            *m = Some(marks);
        }
    }

    /// Sum of the venue totals (the "all exchanges" number). For a spot bot
    /// this is `cash + Σ holdings`; for a futures bot booking a realized
    /// ledger it is CASH ONLY — see [`StatusState::net_worth`].
    pub fn venue_total_usd(&self) -> f64 {
        self.venues
            .read()
            .map(|m| m.values().map(|v| v.total_value).sum())
            .unwrap_or(0.0)
    }

    /// The bot's net worth, decomposed. See [`NetWorth`] for why a single
    /// `f64` here was a number that meant two different things.
    ///
    /// The truth table, by design conservative — every branch either publishes
    /// a figure that accounts for the whole book, or says it does not:
    ///
    /// | open positions | venue basis   | positions valued | result                  |
    /// |----------------|---------------|------------------|-------------------------|
    /// | none           | any           | n/a              | venue total, COMPLETE   |
    /// | some           | marks (`true`)| n/a              | venue total, COMPLETE   |
    /// | some           | cash (`false`)| yes              | venue + unrealised, ✔   |
    /// | some           | cash (`false`)| no               | venue total, INCOMPLETE |
    /// | some           | undeclared    | any              | venue total, INCOMPLETE |
    pub fn net_worth(&self) -> NetWorth {
        let venue_total_usd = self.venue_total_usd();

        // ONE read of the position map, so the count and the valuation can
        // never disagree. `None` = the book is unreadable (a poisoned lock),
        // which is emphatically not the same as "flat".
        let book: Option<(usize, Option<f64>)> = self.positions.read().ok().map(|map| {
            let valued = map.values().try_fold(0.0f64, |acc, p| {
                let notional = p.notional_usd?;
                (notional.is_finite() && p.ret_pct.is_finite())
                    .then(|| acc + notional * p.ret_pct / 100.0)
            });
            (map.len(), valued.filter(|v| v.is_finite()))
        });
        let (open_positions, unrealized_pnl_usd) = book.unwrap_or((0, None));
        let marks = self
            .venue_total_marks_positions
            .read()
            .ok()
            .and_then(|m| *m);

        let (total_usd, complete) = match (book.is_some(), open_positions, marks) {
            // Book unreadable — nothing may be claimed about it.
            (false, _, _) => (venue_total_usd, false),
            // Flat: the venue total IS the net worth, exactly as before.
            (true, 0, _) => (venue_total_usd, true),
            // The venue figure already marks the position to market.
            (true, _, Some(true)) => (venue_total_usd, true),
            // Realized-cash venue figure + a fully valued book: fold it in, so
            // an adverse open position shows up as a DRAWDOWN immediately
            // instead of hiding until the trade closes.
            (true, _, Some(false)) => match unrealized_pnl_usd {
                Some(u) => (venue_total_usd + u, true),
                None => (venue_total_usd, false),
            },
            // Undeclared basis with an open position: unknowable. Say so.
            (true, _, None) => (venue_total_usd, false),
        };

        NetWorth {
            total_usd,
            venue_total_usd,
            unrealized_pnl_usd,
            open_positions,
            complete,
        }
    }

    /// Realized round-trip PnL + net-worth change since the baseline snapshot.
    ///
    /// Uses the same figure `/status` publishes as `net_worth_usd`, so the two
    /// numbers in one document can never disagree about whether an open
    /// position counts. Identical to the previous behaviour whenever the total
    /// is the bare venue sum (flat book, or a book that cannot be valued).
    pub fn pnl_dollars(&self) -> f64 {
        let baseline = self.baseline.get();
        let drift = if baseline.is_nan() {
            0.0
        } else {
            self.net_worth().total_usd - baseline
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
        let nw = self.net_worth();
        gauge("fks_bot_net_worth_usd", "", nw.total_usd);
        // Whether the gauge above accounts for every open position. A scraper
        // that sums net worth across bots needs this to know the figures are
        // the same quantity; 0 means `fks_bot_net_worth_usd` is NOT a net
        // worth, it is the venue-reported component of one.
        gauge(
            "fks_bot_net_worth_complete",
            "",
            if nw.complete { 1.0 } else { 0.0 },
        );
        // Emitted ONLY when the book can actually be valued. A gauge that
        // freezes at a stale number reads as healthy and un-fires the alerts
        // it feeds, so an absent series is the honest signal here.
        if let Some(u) = nw.unrealized_pnl_usd {
            gauge("fks_bot_unrealized_pnl_usd", "", u);
        }
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
        let nw = self.net_worth();
        json!({
            "bot": self.bot,
            "market": self.market,
            "mode": self.mode.read().map(|m| m.clone()).unwrap_or_default(),
            "uptime_secs": self.started.elapsed().as_secs(),
            // How many venues this bot is CONFIGURED with, so a consumer can
            // tell "all venues reported" from "some venue never checked in".
            // The venues map only gains an entry after a venue's first
            // SUCCESSFUL cycle, so a venue that is down at startup is simply
            // absent from `exchanges[]` — and net_worth_usd is then a partial
            // sum that looks entirely healthy. Publishing the expected count
            // is what makes that difference detectable from outside.
            "expected_venues": self.expected_venues,
            "net_worth_usd": nw.total_usd,
            // Does `net_worth_usd` account for every open position? A futures
            // bot whose venue snapshot books REALIZED cash publishes the same
            // field name for a completely different quantity, and the
            // spawner's treasury roll-up sums them together. `false` means
            // "this is not a net worth" — do not record it as one, and do not
            // add it to anybody else's.
            "net_worth_usd_complete": nw.complete,
            // The decomposition, so the headline number is checkable from
            // outside rather than merely asserted. NB: when the venue total
            // already marks positions to market, `unrealized_pnl_usd` is
            // already INSIDE `venue_total_usd` — never add these blindly.
            "venue_total_usd": nw.venue_total_usd,
            "unrealized_pnl_usd": nw.unrealized_pnl_usd,
            "open_positions": nw.open_positions,
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

/// True if `action` is a trade-CLOSING ledger action (`exit`, `stop_exit`, or
/// `kill_exit`). Mirrors fks-state's `funding_brain::is_close_action`: the
/// kill-switch close (`kill_exit`) MUST be recognised here too, or after a kill
/// drill `/status` keeps a phantom open position and never books the round trip.
pub fn is_close_action(action: &str) -> bool {
    matches!(action, "exit" | "stop_exit" | "kill_exit")
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
                    // The brain already stamps `notional_usdt` on its CLOSE
                    // records; when it stamps the ENTRY too, the open position
                    // becomes valuable in dollars and the bot's net worth can
                    // account for it. Absent → `None`, and the net worth
                    // honestly reports itself incomplete.
                    notional_usd: v["notional_usdt"]
                        .as_f64()
                        .filter(|n| n.is_finite() && *n > 0.0),
                }),
            );
        }
        Some(a) if is_close_action(a) => {
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
            venue_total_marks_positions: RwLock::new(None),
            events: RwLock::new(Vec::new()),
        }
    }

    /// A futures venue that books REALIZED equity: cash only, no holdings.
    /// This is the live funding bot's actual shape (observed 2026-08-02:
    /// `cash == total_value == 11288.306994792156`, `holdings: []`).
    fn cash_venue(name: &str, equity: f64) -> VenueStatus {
        VenueStatus {
            exchange: name.into(),
            mode: "paper".into(),
            cash_asset: "USDT".into(),
            cash: equity,
            total_value: equity,
            max_drift: 0.0,
            triggered: false,
            last_rebalance: None,
            updated: 1_785_603_708,
            holdings: vec![],
        }
    }

    /// Serialises the tests that drive the PROCESS-GLOBAL [`StatusState`] via
    /// [`observe_paper_event`]. They share one counter set, so running them
    /// concurrently makes `trades_before + 1` assertions race (observed: 4
    /// spurious failures in 25 runs). Not a lock the bot ever takes.
    static GLOBAL_STATUS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_global() -> std::sync::MutexGuard<'static, ()> {
        GLOBAL_STATUS.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn open_pos(symbol: &str, ret_pct: f64, notional_usd: Option<f64>) -> PositionStatus {
        PositionStatus {
            symbol: symbol.into(),
            dir: 1,
            entry_px: 6.184,
            entry_ts_ms: 1_785_628_855_628,
            mark_px: 6.61,
            ret_pct,
            updated: 1_785_654_051,
            notional_usd,
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
        assert_eq!(s.net_worth().total_usd, 100.0);
        assert!(s.baseline.get().is_nan());
        assert_eq!(s.pnl_dollars(), 0.0);

        s.update_venue(venue("kucoin", 50.0));
        assert_eq!(s.net_worth().total_usd, 150.0);
        assert_eq!(s.baseline.get(), 150.0);

        // The venue's value moves → PnL is the drift from the baseline.
        s.update_venue(venue("kucoin", 60.0));
        assert!((s.pnl_dollars() - 10.0).abs() < 1e-9);
    }

    // ── net worth means ONE thing (2026-08-02, gap #11) ─────────────────────
    /// A flat bot — the live SPOT bot's shape (`positions: []`). The venue sum
    /// IS the net worth, byte-for-byte as before, and it is COMPLETE.
    #[test]
    fn a_flat_book_is_complete_and_unchanged() {
        let s = fresh();
        s.update_venue(venue("kraken", 100.0));
        s.update_venue(venue("kucoin", 78.9053809138768));
        let nw = s.net_worth();
        assert_eq!(nw.total_usd, nw.venue_total_usd, "no positions to fold");
        assert_eq!(nw.open_positions, 0);
        assert_eq!(nw.unrealized_pnl_usd, Some(0.0));
        assert!(nw.complete, "a flat book is fully accounted for");
    }

    /// The live PAPER FUNDING bot, exactly as observed 2026-08-02: venue
    /// snapshot 11288.306994792156 (realized ledger equity, `holdings: []`),
    /// one open AVAXUSDTM long at +6.888745% — and NO notional, because the
    /// brain does not stamp one on its entry record.
    ///
    /// `ret_pct` is a ratio; it cannot be added to a balance. So the position
    /// is unvaluable and the total must NOT claim to be a net worth.
    #[test]
    fn an_unvaluable_open_position_makes_the_total_incomplete() {
        let s = fresh();
        s.update_venue(cash_venue("kucoin-futures", 11_288.306_994_792_156));
        s.set_position(
            "AVAXUSDTM",
            Some(open_pos("AVAXUSDTM", 6.888_745_148_771_025, None)),
        );

        let nw = s.net_worth();
        assert_eq!(nw.open_positions, 1);
        assert_eq!(
            nw.unrealized_pnl_usd, None,
            "a ret_pct with no notional cannot be valued in dollars"
        );
        assert_eq!(
            nw.total_usd, 11_288.306_994_792_156,
            "the figure is unchanged — what changes is that it stops CLAIMING to be net worth"
        );
        assert!(
            !nw.complete,
            "realized cash with an unaccounted open position is not a net worth"
        );
    }

    /// The repair: once the brain supplies the notional (`book.notional_usdt()`
    /// — 3000.0 on every one of this bot's ledger records) and declares that
    /// its venue figure is realized CASH, the open position is marked to
    /// market and the published net worth is the whole book.
    #[test]
    fn a_valued_open_position_is_marked_into_the_net_worth() {
        let s = fresh();
        s.set_venue_total_marks_positions(false); // paper ledger = realized cash
        s.update_venue(cash_venue("kucoin-futures", 11_288.306_994_792_156));
        s.set_position(
            "AVAXUSDTM",
            Some(open_pos("AVAXUSDTM", 6.888_745_148_771_025, Some(3000.0))),
        );

        let nw = s.net_worth();
        let expected_unrealized = 3000.0 * 6.888_745_148_771_025 / 100.0; // +206.66 USDT
        assert!((nw.unrealized_pnl_usd.expect("valued") - expected_unrealized).abs() < 1e-9);
        assert!((nw.total_usd - (11_288.306_994_792_156 + expected_unrealized)).abs() < 1e-9);
        assert!(nw.complete);
        assert!(
            nw.total_usd > nw.venue_total_usd,
            "the +207 USDT that was silently absent from the treasury is now in it"
        );
    }

    /// THE operator consequence. The audit's own observation: AVAXUSDTM long,
    /// entry 6.479, mark 6.446, −0.509% on 3000 notional. Before this change
    /// the treasury recorded pre-trade cash and showed NO DRAWDOWN AT ALL
    /// while the position bled — a flat line that is indistinguishable from a
    /// bot that is doing fine.
    #[test]
    fn an_adverse_open_position_shows_a_drawdown_immediately() {
        let s = fresh();
        s.set_venue_total_marks_positions(false);
        s.update_venue(cash_venue("kucoin-futures", 11_251.55));
        s.set_position(
            "AVAXUSDTM",
            Some(open_pos("AVAXUSDTM", -0.509, Some(3000.0))),
        );

        let nw = s.net_worth();
        assert!(nw.complete);
        assert!(
            nw.total_usd < 11_251.55,
            "an adverse open position must show as a drawdown, not as flat cash"
        );
        assert!((nw.total_usd - (11_251.55 - 15.27)).abs() < 1e-9);
        // And `pnl_usd` in the same document must agree with `net_worth_usd`.
        s.baseline.set(11_251.55);
        assert!((s.pnl_dollars() - (-15.27)).abs() < 1e-9);
    }

    /// A LIVE futures venue reports exchange account equity, which ALREADY
    /// includes unrealised PnL. Folding the position in again would
    /// double-count it — a wrong number recorded as authoritative, which is
    /// the very bug this change exists to stop.
    #[test]
    fn a_mark_to_market_venue_total_is_not_double_counted() {
        let s = fresh();
        s.set_venue_total_marks_positions(true); // live exchange account equity
        s.update_venue(cash_venue("kucoin-futures", 11_495.0));
        s.set_position(
            "AVAXUSDTM",
            Some(open_pos("AVAXUSDTM", 6.888_745, Some(3000.0))),
        );

        let nw = s.net_worth();
        assert_eq!(
            nw.total_usd, 11_495.0,
            "the exchange figure already marks the position — do not add it twice"
        );
        assert!(nw.complete, "and it IS a complete net worth");
        assert!(
            nw.unrealized_pnl_usd.is_some(),
            "still published as a decomposition, just not as an addend"
        );
    }

    /// The declaration is required, not assumed. An undeclared basis with an
    /// open position could be either shape, so neither may be claimed — even
    /// when the position happens to carry a notional.
    #[test]
    fn an_undeclared_basis_with_an_open_position_is_incomplete() {
        let s = fresh(); // venue_total_marks_positions: None
        s.update_venue(cash_venue("kucoin-futures", 11_288.31));
        s.set_position(
            "AVAXUSDTM",
            Some(open_pos("AVAXUSDTM", 6.888_745, Some(3000.0))),
        );
        let nw = s.net_worth();
        assert!(
            !nw.complete,
            "guessing the accounting basis is how a number acquires false authority"
        );
        assert_eq!(nw.total_usd, 11_288.31, "and nothing is silently folded in");
    }

    /// One unvaluable position poisons the whole book: a partial sum of the
    /// open positions is a WRONG total, not a smaller one.
    #[test]
    fn one_unvaluable_position_makes_the_whole_book_unvaluable() {
        let s = fresh();
        s.set_venue_total_marks_positions(false);
        s.update_venue(cash_venue("kucoin-futures", 10_000.0));
        s.set_position("AVAXUSDTM", Some(open_pos("AVAXUSDTM", 2.0, Some(3000.0))));
        s.set_position("DOTUSDTM", Some(open_pos("DOTUSDTM", -5.0, None)));
        let nw = s.net_worth();
        assert_eq!(nw.open_positions, 2);
        assert_eq!(nw.unrealized_pnl_usd, None);
        assert!(!nw.complete);
        assert_eq!(nw.total_usd, 10_000.0);
    }

    #[test]
    fn status_json_publishes_the_decomposition() {
        let s = fresh();
        s.set_venue_total_marks_positions(false);
        s.update_venue(cash_venue("kucoin-futures", 10_000.0));
        s.set_position("AVAXUSDTM", Some(open_pos("AVAXUSDTM", 2.0, None)));
        let j = s.status_json();
        assert_eq!(j["net_worth_usd"], 10_000.0);
        assert_eq!(j["venue_total_usd"], 10_000.0);
        assert_eq!(j["net_worth_usd_complete"], false);
        assert!(j["unrealized_pnl_usd"].is_null(), "unknown is null, not 0");
        assert_eq!(j["open_positions"], 1);

        // Valued → the same document reports a complete, marked-to-market total.
        s.set_position("AVAXUSDTM", Some(open_pos("AVAXUSDTM", 2.0, Some(3000.0))));
        let j = s.status_json();
        assert_eq!(j["net_worth_usd_complete"], true);
        assert_eq!(j["net_worth_usd"], 10_060.0);
        assert_eq!(j["unrealized_pnl_usd"], 60.0);
    }

    #[test]
    fn metrics_expose_whether_the_net_worth_is_complete() {
        let s = fresh();
        s.set_venue_total_marks_positions(false);
        s.update_venue(cash_venue("kucoin-futures", 10_000.0));
        s.set_position("AVAXUSDTM", Some(open_pos("AVAXUSDTM", 2.0, None)));
        let m = s.render_metrics();
        assert!(
            m.contains(r#"fks_bot_net_worth_complete{bot="test-bot",market="spot"} 0"#),
            "an incomplete net worth must say so on the money channel:\n{m}"
        );
        assert!(
            !m.contains("fks_bot_unrealized_pnl_usd"),
            "an unvaluable book exports NO unrealized gauge rather than a fake 0:\n{m}"
        );

        s.set_position("AVAXUSDTM", Some(open_pos("AVAXUSDTM", 2.0, Some(3000.0))));
        let m = s.render_metrics();
        assert!(m.contains(r#"fks_bot_net_worth_complete{bot="test-bot",market="spot"} 1"#));
        assert!(m.contains(r#"fks_bot_unrealized_pnl_usd{bot="test-bot",market="spot"} 60"#));
        assert!(m.contains(r#"fks_bot_net_worth_usd{bot="test-bot",market="spot"} 10060"#));
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
        assert_eq!(
            s.wins.load(Ordering::Relaxed),
            0,
            "gross-win/net-loss is a loss"
        );
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
                notional_usd: Some(3000.0),
            }),
        );
        // Short from 2000, mark 1900 → +5% signed return.
        s.mark_position("ETHUSDTM", 1900.0);
        {
            let map = s.positions.read().unwrap();
            let p = map.get("ETHUSDTM").unwrap();
            assert!((p.ret_pct - 5.0).abs() < 1e-9);
            assert_eq!(
                p.notional_usd,
                Some(3000.0),
                "re-marking must not drop the notional — losing it would silently \
                 make the whole book unvaluable on every price tick"
            );
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

    #[test]
    fn is_close_action_recognizes_kill_exit() {
        assert!(is_close_action("exit"));
        assert!(is_close_action("stop_exit"));
        assert!(is_close_action("kill_exit"));
        assert!(!is_close_action("entry"));
        assert!(!is_close_action(""));
    }

    #[test]
    fn observe_paper_event_takes_the_notional_from_the_entry_record() {
        // The brain already stamps `notional_usdt` on its CLOSE records; when
        // it stamps the ENTRY too, the open position becomes valuable and the
        // bot's net worth stops hiding it. Absent → None, never a guess.
        let _guard = lock_global();
        let s = init("notional-entry-status-test", "futures", 1);
        observe_paper_event(&json!({
            "sym": "NTLUSDTM", "action": "entry", "dir": 1, "entry_px": 6.184,
            "t": 1785628855628_i64, "notional_usdt": 3000.0
        }));
        assert_eq!(
            s.positions.read().unwrap()["NTLUSDTM"].notional_usd,
            Some(3000.0)
        );

        observe_paper_event(&json!({
            "sym": "NONOTIONAL", "action": "entry", "dir": 1, "entry_px": 6.184, "t": 1
        }));
        assert_eq!(
            s.positions.read().unwrap()["NONOTIONAL"].notional_usd,
            None,
            "an entry record without a notional must not invent one"
        );

        // Junk is not a notional either.
        observe_paper_event(&json!({
            "sym": "ZERONOTIONAL", "action": "entry", "dir": 1,
            "entry_px": 1.0, "t": 1, "notional_usdt": 0.0
        }));
        assert_eq!(
            s.positions.read().unwrap()["ZERONOTIONAL"].notional_usd,
            None
        );

        s.set_position("NTLUSDTM", None);
        s.set_position("NONOTIONAL", None);
        s.set_position("ZERONOTIONAL", None);
    }

    #[test]
    fn observe_paper_event_kill_exit_clears_phantom_position_and_books_the_round_trip() {
        // The H1 symptom on the spawner side: after a kill drill `/status` kept a
        // phantom open position and skipped the round-trip counters because
        // `kill_exit` was not in the close-action match. init() is the process
        // global; assert on the specific symbol so parallel tests can't perturb.
        let _guard = lock_global();
        let s = init("kill-exit-status-test", "futures", 1);
        observe_paper_event(&json!({
            "sym": "KEXITUSDTM", "action": "entry", "dir": 1, "entry_px": 100.0, "t": 1
        }));
        assert!(
            s.positions.read().unwrap().contains_key("KEXITUSDTM"),
            "entry opened the position"
        );
        let trades_before = s.trades.load(Ordering::Relaxed);
        observe_paper_event(&json!({
            "sym": "KEXITUSDTM", "action": "kill_exit", "ret_pct": -1.0, "net_pnl_usdt": -5.0
        }));
        assert!(
            !s.positions.read().unwrap().contains_key("KEXITUSDTM"),
            "kill_exit clears the position (no phantom open)"
        );
        assert_eq!(
            s.trades.load(Ordering::Relaxed),
            trades_before + 1,
            "kill_exit books the round trip (paper mode)"
        );
    }
}
