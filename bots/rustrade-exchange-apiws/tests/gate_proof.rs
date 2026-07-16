//! Gate-proof harness — Phase 1 of routing live order placement through
//! rustrade's execution/risk gates (see `RISK_GATE_ROUTING_DESIGN.md` §4,
//! "Phase 1 — Prove the pattern in paper with the gates actually blocking").
//!
//! This is the foundational, zero-live-money proof the whole gate-routing
//! effort rests on: a real rustrade [`Bot`] wired to a **recording** mock
//! [`ExchangeClient`] that records every `place_order` call, driven by a
//! [`FillSource`] that feeds synthetic fills to move PnL. Each test trips one
//! gate and asserts the **observable** outcome — that `place_order` was *not*
//! called on the exchange (`orders_for(sym).is_empty()`), proving the gate
//! prevents the placement end-to-end rather than merely returning `true` from
//! some predicate.
//!
//! Every "blocked" assertion is fenced by a following order the gates *cannot*
//! block (an exit / reduce-only `Close`, which bypasses all entry gates and is
//! processed after the blocked entry by the single per-brain execution loop).
//! When the fence's `place_order` is observed, the blocked entry has provably
//! already been processed — so "no entry order recorded" is a real negative,
//! not a race we didn't wait long enough for.
//!
//! The gates proven here map onto `crates/rustrade/src/execution.rs`:
//!
//! - Gate 1 — session-PnL halt (`SessionPnl::is_session_halted`, :227)
//! - Gate 2 — circuit breaker (`CircuitBreaker::is_tripped`, :241)
//! - Gate 3 — portfolio risk (`PortfolioRisk::check_entry`, :361)
//! - build_order structural blocks (sizer=0 :421, min-notional :432, capability :451)
//! - kill switch (supervisor cancel / `BotHandle::shutdown`)
//! - exits are exempt from *every* de-risking gate: a reduce-only `Close`
//!   against a non-flat position bypasses Gates 1 & 2 via the
//!   `is_reduce_only_exit` predicate (:221) and never reaches the portfolio
//!   (Gate 3) check, which only runs in `build_order`'s Buy/Sell arm (the
//!   `Close` arm at :382 builds a reduce-only order and returns). This is the
//!   rustrade 0.5.1 fix — 0.5.0 ran Gates 1 & 2 for Close too, blocking exits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{
    Bot, BotConfig, BotConfigBuilder, BotHandle, Brain, Candle, Capability, CircuitBreakerConfig,
    Decision, Exchange, ExchangeClient, Fill, FillSource, InstrumentSpec, MarketDataBus,
    MarketDataEvent, Order, OrderKind, PortfolioRiskConfig, Position, Price, Result,
    SessionPnlConfig, Side, SizeHint, Symbol, Volume,
};
use tokio::sync::{Mutex, mpsc};

// ─────────────────────────────────────────────────────────────────────────
// Recording mock ExchangeClient
// ─────────────────────────────────────────────────────────────────────────

/// A mock [`ExchangeClient`] that records every `place_order` call so a test
/// can assert an order was — or was not — placed. Positions are served from a
/// shared book that the [`ScriptedFillSource`] keeps consistent with the
/// synthetic fills it feeds, so `get_position` reflects the fills that move
/// PnL through Gates 1–2.
struct RecordingExchange {
    /// Every order handed to `place_order`, in call order.
    placed: Mutex<Vec<Order>>,
    /// Fires once per `place_order` so a test can await a specific placement.
    place_tx: mpsc::UnboundedSender<Order>,
    /// Position book shared with the fill source (post-fill state).
    book: Arc<Mutex<HashMap<Symbol, Position>>>,
    /// Per-symbol instrument metadata (min-notional / contract value).
    specs: HashMap<Symbol, InstrumentSpec>,
    /// Capabilities this adapter advertises. Anything absent → `supports` false.
    caps: HashSet<Capability>,
}

impl RecordingExchange {
    fn spec_for(&self, symbol: &Symbol) -> InstrumentSpec {
        self.specs
            .get(symbol)
            .copied()
            .unwrap_or_else(|| InstrumentSpec::from_contract_value(1.0))
    }

    /// All recorded orders for `symbol`.
    async fn orders_for(&self, symbol: &str) -> Vec<Order> {
        let want = Symbol::from(symbol);
        self.placed
            .lock()
            .await
            .iter()
            .filter(|o| o.symbol == want)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl ExchangeClient for RecordingExchange {
    fn name(&self) -> &str {
        "recording-mock"
    }

    async fn place_order(&self, order: &Order) -> Result<String> {
        let mut placed = self.placed.lock().await;
        placed.push(order.clone());
        let id = format!("mock-{}", placed.len());
        // Notify after recording so a receiver that wakes on the send sees the
        // order already in `placed`.
        let _ = self.place_tx.send(order.clone());
        Ok(id)
    }

    async fn cancel_all(&self, _symbol: &Symbol) -> Result<usize> {
        Ok(0)
    }

    async fn close_position(&self, _symbol: &Symbol, _position: &Position) -> Result<String> {
        // Only reached by close_positions_on_shutdown, which no test enables.
        Ok("mock-close".into())
    }

    async fn get_position(&self, symbol: &Symbol) -> Result<Position> {
        Ok(self
            .book
            .lock()
            .await
            .get(symbol)
            .copied()
            .unwrap_or(Position::FLAT))
    }

    async fn get_balance(&self, _currency: &str) -> Result<f64> {
        Ok(1_000_000_000.0)
    }

    fn supports(&self, capability: Capability) -> bool {
        self.caps.contains(&capability)
    }

    fn contract_value(&self, symbol: &Symbol) -> f64 {
        self.spec_for(symbol).contract_value
    }

    fn instrument_spec(&self, symbol: &Symbol) -> InstrumentSpec {
        self.spec_for(symbol)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Scripted brain — a decision closure over (event, position)
// ─────────────────────────────────────────────────────────────────────────

type Decider = Box<dyn Fn(&MarketDataEvent, &Position) -> Decision + Send + Sync>;

/// A [`Brain`] that defers every decision to a test-supplied closure. Most
/// tests script one fixed decision per symbol; the exits test scripts Buy-vs-
/// Close on the *same* symbol by inspecting the event.
struct ScriptedBrain {
    decider: Decider,
}

#[async_trait]
impl Brain for ScriptedBrain {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> Result<Decision> {
        Ok((self.decider)(event, position))
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Scripted fill source — feeds synthetic fills, keeps the book consistent
// ─────────────────────────────────────────────────────────────────────────

/// A [`FillSource`] backed by an unbounded channel. Each fill is applied to
/// the shared position book (weighted-average entry) *before* it is returned,
/// so by the time the framework's `FillRoutingService` refreshes the position
/// from `get_position`, the book already reflects the fill — exactly the
/// pre-fill-cache / post-fill-book relationship the realised-PnL accounting
/// (which drives Gates 1–2) depends on.
struct ScriptedFillSource {
    rx: Mutex<mpsc::UnboundedReceiver<Fill>>,
    book: Arc<Mutex<HashMap<Symbol, Position>>>,
}

#[async_trait]
impl FillSource for ScriptedFillSource {
    async fn next_fill(&self) -> Option<Fill> {
        let fill = self.rx.lock().await.recv().await?;
        apply_fill_to_book(&self.book, &fill).await;
        Some(fill)
    }
}

/// Apply a fill to the shared book using the same weighted-average model the
/// framework's `FillRoutingService` uses, so entry prices line up. Tests only
/// script open→close round trips, so flips never occur, but the flip arm is
/// handled for safety.
async fn apply_fill_to_book(book: &Arc<Mutex<HashMap<Symbol, Position>>>, fill: &Fill) {
    let mut b = book.lock().await;
    let pos = b.get(&fill.symbol).copied().unwrap_or(Position::FLAT);
    let signed = match fill.side {
        Side::Buy => fill.size.value(),
        Side::Sell => -fill.size.value(),
    };
    let new_qty = pos.qty + signed;
    let entry = if pos.qty == 0.0 {
        Some(fill.price.value())
    } else if pos.qty.signum() == signed.signum() {
        // Adding to the position — weighted-average the entry.
        let prev = pos.entry_price.unwrap_or(fill.price.value());
        let w = (pos.qty.abs() * prev + signed.abs() * fill.price.value())
            / (pos.qty.abs() + signed.abs());
        Some(w)
    } else if new_qty == 0.0 {
        None // fully closed
    } else if new_qty.signum() == pos.qty.signum() {
        pos.entry_price // partial reduce — keep the entry
    } else {
        Some(fill.price.value()) // flip
    };
    let new_pos = if new_qty == 0.0 {
        Position::FLAT
    } else {
        Position {
            qty: new_qty,
            entry_price: entry,
            unrealised_pnl: 0.0,
        }
    };
    b.insert(fill.symbol.clone(), new_pos);
}

// ─────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────

struct Harness {
    exchange: Arc<RecordingExchange>,
    bus: MarketDataBus,
    handle: BotHandle,
    place_rx: mpsc::UnboundedReceiver<Order>,
    fill_tx: Option<mpsc::UnboundedSender<Fill>>,
    run: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Everything a test needs to stand up one paper `Bot`.
struct HarnessSpec<'a> {
    symbols: &'a [&'a str],
    decisions: Vec<(&'a str, Decision)>,
    decider: Option<Decider>,
    specs: Vec<(&'a str, InstrumentSpec)>,
    caps: HashSet<Capability>,
    with_fills: bool,
    initial_book: Vec<(&'a str, Position)>,
    configure: Box<dyn FnOnce(BotConfigBuilder) -> BotConfigBuilder>,
}

impl<'a> HarnessSpec<'a> {
    fn new(symbols: &'a [&'a str]) -> Self {
        Self {
            symbols,
            decisions: Vec::new(),
            decider: None,
            specs: Vec::new(),
            caps: default_caps(),
            with_fills: false,
            initial_book: Vec::new(),
            configure: Box::new(|b| b),
        }
    }
    /// Script one fixed decision for a symbol (mutually exclusive with `decider`).
    fn decide(mut self, symbol: &'a str, decision: Decision) -> Self {
        self.decisions.push((symbol, decision));
        self
    }
    /// Script an arbitrary decision closure over (event, position).
    fn decider(
        mut self,
        f: impl Fn(&MarketDataEvent, &Position) -> Decision + Send + Sync + 'static,
    ) -> Self {
        self.decider = Some(Box::new(f));
        self
    }
    fn spec(mut self, symbol: &'a str, spec: InstrumentSpec) -> Self {
        self.specs.push((symbol, spec));
        self
    }
    fn with_fills(mut self) -> Self {
        self.with_fills = true;
        self
    }
    fn open(mut self, symbol: &'a str, qty: f64, entry: f64) -> Self {
        self.initial_book.push((
            symbol,
            Position {
                qty,
                entry_price: Some(entry),
                unrealised_pnl: 0.0,
            },
        ));
        self
    }
    fn configure(mut self, f: impl FnOnce(BotConfigBuilder) -> BotConfigBuilder + 'static) -> Self {
        self.configure = Box::new(f);
        self
    }
}

/// Default capabilities: the adapter honours reduce-only exits but advertises
/// no advanced time-in-force kinds — so a `PostOnly` entry is blocked by the
/// capability gate (the structural capability proof).
fn default_caps() -> HashSet<Capability> {
    HashSet::from([Capability::ReduceOnly])
}

impl Harness {
    async fn start(spec: HarnessSpec<'_>) -> Harness {
        let (place_tx, place_rx) = mpsc::unbounded_channel();
        let book = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut b = book.lock().await;
            for (s, p) in spec.initial_book {
                b.insert(Symbol::from(s), p);
            }
        }
        let mut spec_map = HashMap::new();
        for (s, sp) in spec.specs {
            spec_map.insert(Symbol::from(s), sp);
        }
        let exchange = Arc::new(RecordingExchange {
            placed: Mutex::new(Vec::new()),
            place_tx,
            book: book.clone(),
            specs: spec_map,
            caps: spec.caps,
        });

        let decider: Decider = match spec.decider {
            Some(d) => d,
            None => {
                let mut dmap = HashMap::new();
                for (s, d) in spec.decisions {
                    dmap.insert(Symbol::from(s), d);
                }
                Box::new(move |ev, _pos| {
                    dmap.get(ev.symbol())
                        .cloned()
                        .unwrap_or_else(Decision::hold)
                })
            }
        };
        let brain = Arc::new(ScriptedBrain { decider });

        let base = BotConfig::builder()
            .name("gate-proof")
            .symbols(spec.symbols.iter().copied())
            .without_signal_handler();
        let config = (spec.configure)(base).build().expect("config builds");

        let mut bot = Bot::new(config, exchange.clone(), vec![brain]).expect("bot builds");
        let fill_tx = if spec.with_fills {
            let (ftx, frx) = mpsc::unbounded_channel();
            bot = bot.with_fill_source(Arc::new(ScriptedFillSource {
                rx: Mutex::new(frx),
                book: book.clone(),
            }));
            Some(ftx)
        } else {
            None
        };

        let bus = bot.market_data_bus().clone();
        let handle = bot.handle();
        let run = tokio::spawn(async move { bot.run_until_shutdown().await });

        let h = Harness {
            exchange,
            bus,
            handle,
            place_rx,
            fill_tx,
            run,
        };
        h.wait_ready().await;
        h
    }

    /// Wait until the per-brain execution service has subscribed to the market
    /// bus — publishing before then would be dropped (no receivers).
    async fn wait_ready(&self) {
        for _ in 0..400 {
            if self.bus.subscriber_count() >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("execution service never subscribed to the market bus");
    }

    fn publish_candle(&self, symbol: &str, close: f64) {
        self.bus.publish(candle_event(symbol, close));
    }

    fn send_fill(&self, symbol: &str, side: Side, price: f64, size: f64) {
        self.fill_tx
            .as_ref()
            .expect("harness built with_fills")
            .send(make_fill(symbol, side, price, size))
            .expect("fill routing service alive");
    }

    /// Await the next `place_order` whose symbol matches `symbol`, skipping
    /// (but not discarding — the exchange already recorded them) any others.
    /// Panics on timeout so a test that expected a placement fails loudly.
    async fn await_placement(&mut self, symbol: &str) -> Order {
        let want = Symbol::from(symbol);
        loop {
            match tokio::time::timeout(Duration::from_secs(5), self.place_rx.recv()).await {
                Ok(Some(o)) if o.symbol == want => return o,
                Ok(Some(_)) => continue,
                Ok(None) => panic!("place channel closed before a {symbol} placement"),
                Err(_) => panic!("timed out waiting for a {symbol} placement"),
            }
        }
    }

    /// Wait until the position cache reports `symbol` as non-flat. Because the
    /// `FillRoutingService` drains fills strictly in order, an observably-open
    /// fence symbol proves every earlier fill (and its realised-PnL recording)
    /// has been processed.
    async fn wait_position_open(&self, symbol: &str) {
        let sym = Symbol::from(symbol);
        for _ in 0..400 {
            if !self.handle.position(&sym).await.is_flat() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("position for {symbol} never became non-flat");
    }

    async fn orders_for(&self, symbol: &str) -> Vec<Order> {
        self.exchange.orders_for(symbol).await
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        self.handle.shutdown();
        tokio::time::timeout(Duration::from_secs(5), self.run)
            .await
            .expect("bot did not shut down within 5s")
            .expect("run task panicked")
    }
}

fn candle_event(symbol: &str, close: f64) -> MarketDataEvent {
    MarketDataEvent::Candle {
        exchange: Exchange::from("mock"),
        symbol: Symbol::from(symbol),
        candle: Candle {
            time: 0,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
        },
    }
}

fn candle_close(event: &MarketDataEvent) -> f64 {
    match event {
        MarketDataEvent::Candle { candle, .. } => candle.close,
        _ => f64::NAN,
    }
}

fn make_fill(symbol: &str, side: Side, price: f64, size: f64) -> Fill {
    Fill {
        symbol: Symbol::from(symbol),
        order_id: format!("fill-{symbol}-{side:?}-{price}"),
        client_id: None,
        side,
        price: Price(price),
        size: Volume(size),
        fee: 0.0,
        fee_currency: "USDT".into(),
        timestamp: Utc::now(),
    }
}

/// Feed one losing long round trip on `symbol`: open `qty` @ `open_px`, then
/// close it @ `close_px` (`close_px < open_px` ⇒ a realised loss recorded by
/// the framework's fill router).
fn feed_losing_round_trip(h: &Harness, symbol: &str, qty: f64, open_px: f64, close_px: f64) {
    h.send_fill(symbol, Side::Buy, open_px, qty);
    h.send_fill(symbol, Side::Sell, close_px, qty);
}

// ─────────────────────────────────────────────────────────────────────────
// Positive control — the harness DOES place an un-gated entry
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_unblocked_entry_is_placed() {
    // With no gate tripped, a Buy must reach the exchange. This makes every
    // "not placed" assertion elsewhere meaningful — the pipeline can place.
    let mut h =
        Harness::start(HarnessSpec::new(&["ENTRY"]).decide("ENTRY", Decision::buy(1.0))).await;

    h.publish_candle("ENTRY", 100.0);
    let order = h.await_placement("ENTRY").await;

    assert_eq!(order.side, Side::Buy);
    assert!(!order.reduce_only, "an entry is not reduce-only");
    assert_eq!(h.orders_for("ENTRY").await.len(), 1);
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Gate 1 — session-PnL halt (driven by synthetic losing fills)
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_pnl_halt_blocks_entry_placement() {
    // Session cap -100; breaker effectively off (limit 100) so the *session*
    // halt — not the breaker — is what blocks.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "FENCE"])
            .with_fills()
            .decide("ENTRY", Decision::buy(1.0))
            .decide("FENCE", Decision::close())
            .configure(|b| {
                b.session_pnl_config(SessionPnlConfig { loss_limit: -100.0 })
                    .circuit_breaker_config(CircuitBreakerConfig {
                        loss_limit: 100,
                        window_secs: 86_400,
                        cooldown_secs: 86_400,
                    })
            }),
    )
    .await;

    // One losing round trip: 3 @ 100 → close 3 @ 50 = gross -150 ≤ -100 → halt.
    feed_losing_round_trip(&h, "ENTRY", 3.0, 100.0, 50.0);
    // Open the fence via a fill; once it's visible every prior fill is applied,
    // so the session for ENTRY is now halted.
    h.send_fill("FENCE", Side::Buy, 100.0, 1.0);
    h.wait_position_open("FENCE").await;

    // Attempt the entry (must be blocked by Gate 1), then the fence exit.
    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("FENCE", 100.0);
    let fence = h.await_placement("FENCE").await;

    assert!(fence.reduce_only, "fence must be a reduce-only exit");
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "session-PnL halt must block the entry: place_order was called for ENTRY"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Gate 2 — circuit breaker (driven by a synthetic loss streak)
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn circuit_breaker_trip_blocks_entry_placement() {
    // Breaker trips at 2 losses; session halt disabled (−∞) so the *breaker*
    // is the only thing that can block.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "FENCE"])
            .with_fills()
            .decide("ENTRY", Decision::buy(1.0))
            .decide("FENCE", Decision::close())
            .configure(|b| {
                b.session_pnl_config(SessionPnlConfig {
                    loss_limit: f64::NEG_INFINITY,
                })
                .circuit_breaker_config(CircuitBreakerConfig {
                    loss_limit: 2,
                    window_secs: 86_400,
                    cooldown_secs: 86_400,
                })
            }),
    )
    .await;

    // Two small losing round trips → 2 recorded losses → breaker tripped,
    // but session net (−2) never reaches −∞.
    feed_losing_round_trip(&h, "ENTRY", 1.0, 100.0, 99.0);
    feed_losing_round_trip(&h, "ENTRY", 1.0, 100.0, 99.0);
    h.send_fill("FENCE", Side::Buy, 100.0, 1.0);
    h.wait_position_open("FENCE").await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("FENCE", 100.0);
    let fence = h.await_placement("FENCE").await;

    assert!(fence.reduce_only);
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "circuit breaker must block the entry: place_order was called for ENTRY"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Gate 3 — portfolio: max concurrent positions
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn portfolio_max_concurrent_blocks_new_symbol_entry() {
    // One position already open (OTHER); cap is 1, so a new symbol's entry is
    // blocked by PortfolioRisk::check_entry. OTHER doubles as the exit fence.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "OTHER"])
            .decide("ENTRY", Decision::buy(1.0))
            .decide("OTHER", Decision::close())
            .open("OTHER", 5.0, 100.0)
            .configure(|b| {
                b.portfolio_config(PortfolioRiskConfig {
                    max_daily_loss: f64::NEG_INFINITY,
                    max_concurrent_positions: 1,
                    max_gross_exposure: f64::INFINITY,
                })
            }),
    )
    .await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("OTHER", 100.0);
    let fence = h.await_placement("OTHER").await;

    assert!(fence.reduce_only, "OTHER close is the reduce-only fence");
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "max_concurrent_positions must block the new-symbol entry"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Gate 3 — portfolio: gross-exposure cap
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn portfolio_gross_exposure_cap_blocks_entry() {
    // OTHER carries 1_000 of gross exposure (10 @ 100); cap 2_000. The ENTRY
    // sizes to 500 × 5 = 2_500 notional → 1_000 + 2_500 > 2_000 → blocked.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "OTHER"])
            .decide("ENTRY", Decision::buy(1.0))
            .decide("OTHER", Decision::close())
            .open("OTHER", 10.0, 100.0)
            .configure(|b| {
                b.portfolio_config(PortfolioRiskConfig {
                    max_daily_loss: f64::NEG_INFINITY,
                    max_concurrent_positions: 0,
                    max_gross_exposure: 2_000.0,
                })
            }),
    )
    .await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("OTHER", 100.0);
    let fence = h.await_placement("OTHER").await;

    assert!(fence.reduce_only);
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "gross-exposure cap must block the entry"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Structural (build_order) — sizer returns 0 contracts
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structural_zero_size_blocks_entry() {
    // A Buy that explicitly requests 0 quantity → sizer returns 0 → blocked
    // before any order is built.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "FENCE"])
            .decide(
                "ENTRY",
                Decision::buy(1.0).with_size_hint(SizeHint::Quantity(Volume(0.0))),
            )
            .decide("FENCE", Decision::close())
            .open("FENCE", 1.0, 100.0),
    )
    .await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("FENCE", 100.0);
    let fence = h.await_placement("FENCE").await;

    assert!(fence.reduce_only);
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "sizer=0 must block the entry"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Structural (build_order) — sub-min-notional order
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structural_sub_min_notional_blocks_entry() {
    // ENTRY's instrument demands a 1e9 min notional; a normally-sized order is
    // far below it → blocked by the min-notional gate.
    let big_min = InstrumentSpec {
        min_notional: 1_000_000_000.0,
        ..InstrumentSpec::from_contract_value(1.0)
    };
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "FENCE"])
            .decide("ENTRY", Decision::buy(1.0))
            .decide("FENCE", Decision::close())
            .spec("ENTRY", big_min)
            .open("FENCE", 1.0, 100.0),
    )
    .await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("FENCE", 100.0);
    let fence = h.await_placement("FENCE").await;

    assert!(fence.reduce_only);
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "sub-min-notional order must be blocked"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Structural (build_order) — unsupported order-kind capability
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn structural_unsupported_capability_blocks_entry() {
    // The adapter advertises only ReduceOnly (default caps), so a PostOnly
    // entry is blocked by the capability gate rather than silently downgraded.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "FENCE"])
            .decide(
                "ENTRY",
                Decision::buy(1.0)
                    .with_limit_price(Price(100.0))
                    .with_order_kind(OrderKind::PostOnly),
            )
            .decide("FENCE", Decision::close())
            .open("FENCE", 1.0, 100.0),
    )
    .await;

    h.publish_candle("ENTRY", 100.0);
    h.publish_candle("FENCE", 100.0);
    let fence = h.await_placement("FENCE").await;

    assert!(fence.reduce_only);
    assert!(
        h.orders_for("ENTRY").await.is_empty(),
        "unsupported PostOnly capability must block the entry"
    );
    h.shutdown().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────
// Kill switch — shutdown halts further placement
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_halts_further_placement() {
    // The exact same pipeline that places a valid entry places nothing once
    // the kill switch (BotHandle::shutdown → supervisor cancel) has fired.
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY", "ENTRY2"])
            .decide("ENTRY", Decision::buy(1.0))
            .decide("ENTRY2", Decision::buy(1.0)),
    )
    .await;

    // Control: an entry places before the kill.
    h.publish_candle("ENTRY", 100.0);
    let placed = h.await_placement("ENTRY").await;
    assert_eq!(placed.side, Side::Buy);

    // Fire the kill switch and wait for the bot to fully drain.
    let Harness {
        exchange,
        bus,
        handle,
        run,
        ..
    } = h;
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("kill switch did not stop the bot within 5s")
        .expect("run task panicked")
        .expect("run returned an error");

    // A post-kill entry must not be placed.
    bus.publish(candle_event("ENTRY2", 100.0));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        exchange.orders_for("ENTRY2").await.is_empty(),
        "no entry may be placed after the kill switch fires"
    );
    // The pre-kill control entry is still the only recorded order.
    assert_eq!(exchange.orders_for("ENTRY").await.len(), 1);
}

// ─────────────────────────────────────────────────────────────────────────
// Exit exemption (rustrade 0.5.1) — a reduce-only Close de-risks even under an
// active session halt AND a tripped circuit breaker AND a portfolio halt
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_reaches_exchange_under_session_halt_and_circuit_breaker() {
    // Every de-risking gate on ENTRY is armed by a single realised loss of
    // −200:
    //   • Gate 1 — session-PnL halt: net −200 ≤ the −10 cap → halted.
    //   • Gate 2 — circuit breaker: 1 recorded loss ≥ the 1-loss limit → tripped.
    //   • Gate 3 — portfolio daily-loss halt: account net −200 ≤ the −10 cap.
    //
    // Under all three, a *new entry* Buy stays fully gated — blocked by Gate 1,
    // the first check. But a reduce-only *Close* against the open position must
    // still REACH place_order, because the bot must always be able to de-risk:
    //   • Gates 1 & 2 exempt it — rustrade 0.5.1 skips both checks when the
    //     signal is a `Close` against a non-flat position (execution.rs :221,
    //     `is_reduce_only_exit`; the :227/:241 checks sit inside `if
    //     !is_reduce_only_exit`).
    //   • Gate 3 never runs for it — the portfolio check lives in build_order's
    //     Buy/Sell arm; the Close arm (:382) just builds a reduce-only order.
    //
    // This is the 0.5.1 de-risking fix. In 0.5.0 Gates 1 & 2 ran for *every*
    // non-Hold signal, Close included, so this exact Close would have been
    // blocked under the session halt — the bug this test now pins shut. (On
    // 0.5.0 the assertion below would fail: no order would reach the exchange
    // and `await_placement` would time out.)
    let mut h = Harness::start(
        HarnessSpec::new(&["ENTRY"])
            .decider(|ev, _pos| {
                if candle_close(ev) == 200.0 {
                    Decision::close()
                } else {
                    Decision::buy(1.0)
                }
            })
            .open("ENTRY", 4.0, 100.0)
            .configure(|b| {
                b.session_pnl_config(SessionPnlConfig { loss_limit: -10.0 })
                    .circuit_breaker_config(CircuitBreakerConfig {
                        loss_limit: 1,
                        window_secs: 86_400,
                        cooldown_secs: 86_400,
                    })
                    .portfolio_config(PortfolioRiskConfig {
                        max_daily_loss: -10.0,
                        max_concurrent_positions: 0,
                        max_gross_exposure: f64::INFINITY,
                    })
            }),
    )
    .await;

    // One realised loss arms all three de-risking gates on ENTRY at once:
    // session halt (net −200 ≤ −10), circuit breaker (1 loss ≥ 1), and the
    // portfolio daily-loss halt (account net −200 ≤ −10).
    h.handle
        .record_trade_outcome(&Symbol::from("ENTRY"), -200.0, 0.0)
        .await;

    // First a Buy (blocked by the session halt), then a Close (exempt — it must
    // go through, and serves as the fence proving both events were processed).
    h.publish_candle("ENTRY", 100.0); // → Buy, blocked by Gate 1
    h.publish_candle("ENTRY", 200.0); // → Close, exempt and placed
    let close = h.await_placement("ENTRY").await;

    assert!(
        close.reduce_only && close.side == Side::Sell,
        "the reduce-only exit must reach the exchange as a sell even under an \
         active session halt AND a tripped circuit breaker"
    );
    let orders = h.orders_for("ENTRY").await;
    assert_eq!(
        orders.len(),
        1,
        "exactly one order — the exit — should reach the exchange (the Buy was blocked)"
    );
    h.shutdown().await.unwrap();
}
