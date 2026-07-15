//! [`KucoinFillSource`] — a [`rustrade::FillSource`] that delivers **real**
//! KuCoin fills into the bot.
//!
//! Wiring this into a `Bot` (via `Bot::with_fill_source`) replaces paper-
//! simulated fills with the exchange's own executions, and — because the
//! framework gates bracket/OCO management on `fill_source.is_some()` — it's
//! what turns on real SL/TP bracket handling.
//!
//! # How it gets fills
//!
//! KuCoin's private `/contractMarket/tradeOrders` WS feed announces order
//! transitions promptly, but exchange-apiws's `OrderUpdate` carries only the
//! *order* price (which is `0.0` for market orders) — not the per-execution
//! match price. So this source treats the WS event as a **low-latency
//! trigger** and reads the authoritative price / size / fee from the
//! `/recentFills` REST endpoint ([`KuCoinClient::get_recent_fills`]), deduping
//! by trade id. A periodic safety poll covers any WS gap, and if the private
//! WS token can't be obtained the source degrades to **poll-only** (still
//! correct, just higher latency).
//!
//! Startup is baselined: fills that already existed when the source connected
//! are recorded-as-seen but **not** replayed into the bot.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rustrade::{Fill, FillSource, Price, Side, Symbol, Volume};
use tokio::sync::{Mutex, mpsc, watch};
use tracing::{debug, info, warn};

use exchange_apiws::actors::{DataMessage, ExchangeConnector};
use exchange_apiws::rest::Fill as EaFill;
use exchange_apiws::ws::{KucoinConnector, SupervisedConfig, WsFeedEndpoint, run_feed_supervised};
use exchange_apiws::{KuCoinClient, KucoinEnv};

use crate::ms_to_dt;

/// Default safety-poll cadence — the WS trigger normally fires first; this
/// just bounds worst-case latency if the WS is down or misses an event.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound on remembered trade ids (FIFO eviction). `/recentFills` returns
/// at most ~1000 rows, so this comfortably covers the dedup window while
/// keeping memory bounded on long runs.
const SEEN_CAPACITY: usize = 10_000;

// ── Dedup ────────────────────────────────────────────────────────────────────

/// Bounded set of already-emitted trade ids, FIFO-evicted past its cap.
#[derive(Debug)]
struct SeenFills {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl SeenFills {
    fn new(cap: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap: cap.max(1),
        }
    }

    /// Record `id`; returns `true` if it was **not** seen before.
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        if self.order.len() >= self.cap
            && let Some(evicted) = self.order.pop_front()
        {
            self.set.remove(&evicted);
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        true
    }
}

// ── Conversion ─────────────────────────────────────────────────────────────

/// Stable id for a `/recentFills` row: prefer the exchange trade id, else fall
/// back to `order_id:created_at` (good enough to dedupe consecutive polls).
fn fill_id(f: &EaFill) -> String {
    f.trade_id
        .clone()
        .unwrap_or_else(|| format!("{}:{}", f.order_id, f.created_at.unwrap_or(0)))
}

/// Convert an exchange-apiws `/recentFills` row into a framework [`Fill`].
fn ea_fill_to_fill(symbol: &str, f: &EaFill) -> Fill {
    Fill {
        symbol: Symbol::from(symbol),
        order_id: f.order_id.clone(),
        client_id: None,
        side: if f.side.eq_ignore_ascii_case("sell") {
            Side::Sell
        } else {
            Side::Buy
        },
        price: Price(f.price),
        size: Volume(f64::from(f.size)),
        fee: f.fee,
        fee_currency: f.fee_currency.clone().unwrap_or_else(|| "USDT".to_string()),
        timestamp: f.created_at.and_then(ms_to_dt).unwrap_or_else(Utc::now),
    }
}

// ── Source ───────────────────────────────────────────────────────────────────

/// A [`FillSource`] backed by KuCoin Futures `/recentFills`, triggered by the
/// private `tradeOrders` WS feed. See this module's documentation for the
/// WS-trigger + `/recentFills` design.
///
/// Construct with [`connect`](Self::connect); hand the result to
/// `Bot::with_fill_source`. Cloning is not provided — the background task owns
/// the receiver; wrap in `Arc` (as `Bot::with_fill_source` requires) to share.
pub struct KucoinFillSource {
    rx: Mutex<mpsc::UnboundedReceiver<Fill>>,
    /// Dropped when the source is dropped, which signals the driver +
    /// WS tasks to stop (keeps the source drop-safe).
    _shutdown: watch::Sender<bool>,
}

impl KucoinFillSource {
    /// Connect a fill source over `symbols`, polling `/recentFills` every
    /// `poll_interval` as a safety net behind the WS trigger.
    ///
    /// Spawns the driver task on the current Tokio runtime and returns
    /// immediately; the baseline (don't-replay-history) snapshot happens inside
    /// the task, so this never blocks on the network.
    #[must_use]
    pub fn connect(
        client: KuCoinClient,
        env: KucoinEnv,
        symbols: Vec<String>,
        poll_interval: Duration,
    ) -> Self {
        let (fill_tx, fill_rx) = mpsc::unbounded_channel::<Fill>();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::spawn(drive(
            client,
            env,
            symbols,
            poll_interval.max(Duration::from_secs(1)),
            fill_tx,
            shutdown_rx,
        ));

        Self {
            rx: Mutex::new(fill_rx),
            _shutdown: shutdown_tx,
        }
    }

    /// Connect with the default safety-poll cadence (5 s).
    #[must_use]
    pub fn connect_default(client: KuCoinClient, env: KucoinEnv, symbols: Vec<String>) -> Self {
        Self::connect(client, env, symbols, DEFAULT_POLL_INTERVAL)
    }
}

#[async_trait]
impl FillSource for KucoinFillSource {
    async fn next_fill(&self) -> Option<Fill> {
        self.rx.lock().await.recv().await
    }
}

/// Fetch `/recentFills` for each symbol and emit rows not yet seen. When
/// `baseline` is true, rows are only recorded-as-seen (startup history is not
/// replayed into the bot).
async fn hydrate(
    client: &KuCoinClient,
    symbols: &[String],
    seen: &mut SeenFills,
    baseline: bool,
    fill_tx: &mpsc::UnboundedSender<Fill>,
) {
    for symbol in symbols {
        let fills = match client.get_recent_fills(symbol).await {
            Ok(f) => f,
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "recentFills poll failed");
                continue;
            }
        };
        for f in &fills {
            let is_new = seen.insert(&fill_id(f));
            if is_new && !baseline && fill_tx.send(ea_fill_to_fill(symbol, f)).is_err() {
                // Receiver gone — the source was dropped; stop hydrating.
                return;
            }
        }
    }
}

/// One "check for new fills now" action, abstracted so the driver loop
/// ([`run_loop`]) can be exercised without a live exchange in tests. The
/// production implementation is [`HydratePoller`]; both the WS trigger and the
/// safety poll invoke the exact same action.
#[async_trait]
trait Poller: Send {
    async fn poll(&mut self);
}

/// Production poller: fetch `/recentFills` and emit any not-yet-seen rows.
struct HydratePoller {
    client: KuCoinClient,
    symbols: Vec<String>,
    seen: SeenFills,
    fill_tx: mpsc::UnboundedSender<Fill>,
}

#[async_trait]
impl Poller for HydratePoller {
    async fn poll(&mut self) {
        hydrate(
            &self.client,
            &self.symbols,
            &mut self.seen,
            false,
            &self.fill_tx,
        )
        .await;
    }
}

/// The background driver: baseline, then loop on (WS trigger | safety poll |
/// shutdown), hydrating real fills each time.
async fn drive(
    client: KuCoinClient,
    env: KucoinEnv,
    symbols: Vec<String>,
    poll_interval: Duration,
    fill_tx: mpsc::UnboundedSender<Fill>,
    shutdown: watch::Receiver<bool>,
) {
    let symbol_count = symbols.len();
    let mut poller = HydratePoller {
        client: client.clone(),
        symbols,
        seen: SeenFills::new(SEEN_CAPACITY),
        fill_tx,
    };

    // Baseline: don't replay fills that predate this source.
    hydrate(
        &poller.client,
        &poller.symbols,
        &mut poller.seen,
        true,
        &poller.fill_tx,
    )
    .await;
    info!(symbols = symbol_count, "kucoin fill source baselined; live");

    // Private-WS trigger (graceful: poll-only if it can't start).
    let (ws_tx, ws_rx) = mpsc::channel::<DataMessage>(256);
    tokio::spawn(run_ws(client, env, ws_tx, shutdown.clone()));

    let mut tick = tokio::time::interval(poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    run_loop(ws_rx, tick, shutdown, poller).await;
}

/// Drive `poller` off the WS trigger and the safety-poll `tick` until shutdown.
///
/// The WS branch is a *low-latency trigger* only; the `tick` branch is the
/// authoritative safety net that must keep firing even when the WS trigger is
/// down (poll-only mode). Because the trigger is `biased` ahead of `tick`, a
/// closed `ws_rx` (its sender dropped when `run_ws` degrades to poll-only)
/// resolves `recv()` to `Ready(None)` *permanently* and would starve `tick`
/// into a 100%-CPU hot-spin that delivers zero fills. Guard against that: on
/// the first `None`, disable the trigger branch (`ws_alive = false`) so the
/// select falls through to `tick` and poll-only delivery continues.
async fn run_loop<P: Poller>(
    mut ws_rx: mpsc::Receiver<DataMessage>,
    mut tick: tokio::time::Interval,
    mut shutdown: watch::Receiver<bool>,
    mut poller: P,
) {
    let mut ws_alive = true;
    loop {
        tokio::select! {
            biased;
            // Sender dropped (source dropped) or set to true ⇒ stop.
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            maybe = ws_rx.recv(), if ws_alive => {
                match maybe {
                    // An order transition ⇒ check for new fills now.
                    Some(_) => poller.poll().await,
                    // The WS task ended (poll-only). Disable this branch so it
                    // stops resolving Ready(None) and starving the safety poll;
                    // `tick` now drives delivery until shutdown.
                    None => {
                        ws_alive = false;
                        debug!("kucoin fills: WS trigger closed; safety poll only");
                    }
                }
            }
            _ = tick.tick() => {
                poller.poll().await;
            }
        }
    }
    debug!("kucoin fill source driver stopped");
}

/// Run the private `tradeOrders` WS feed, forwarding every parsed message as a
/// trigger. Returns (logging) if the private token can't be obtained, leaving
/// the driver on its safety poll.
async fn run_ws(
    client: KuCoinClient,
    env: KucoinEnv,
    ws_tx: mpsc::Sender<DataMessage>,
    shutdown: watch::Receiver<bool>,
) {
    // Initial connector (used by the runner for parse/ping; its URL is replaced
    // by the refresh closure's endpoint on every connect).
    let connector: Arc<dyn ExchangeConnector> = match client.get_ws_token_private().await {
        Ok(token) => match KucoinConnector::new(&token, env) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                warn!(error = %e, "private WS connector build failed; fills poll-only");
                return;
            }
        },
        Err(e) => {
            warn!(error = %e, "private WS token unavailable; fills poll-only");
            return;
        }
    };

    // Re-negotiate a fresh private token + the tradeOrders subscription on every
    // (re)connect cycle.
    let refresh = move || {
        let client = client.clone();
        async move {
            let token = client.get_ws_token_private().await?;
            let conn = KucoinConnector::new(&token, env)?;
            let subscriptions = conn.order_updates_subscription().into_iter().collect();
            Ok(WsFeedEndpoint {
                url: conn.negotiated_url,
                subscriptions,
            })
        }
    };

    info!("kucoin private WS (tradeOrders) starting — low-latency fill trigger");
    if let Err(e) = run_feed_supervised(
        connector,
        ws_tx,
        SupervisedConfig::default(),
        shutdown,
        refresh,
    )
    .await
    {
        warn!(error = %e, "private WS feed ended; fills fall back to polling");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts `poll()` calls so a test can prove the safety poll keeps firing.
    struct CountingPoller {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Poller for CountingPoller {
        async fn poll(&mut self) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Regression for the `biased` select starving the safety poll: when the WS
    /// trigger dies (its sender is dropped, as `run_ws` does on poll-only
    /// degrade), `ws_rx.recv()` resolves `Ready(None)` forever. Before the fix
    /// the `biased` loop hot-spun on that closed branch and `tick.tick()` never
    /// ran, so ZERO fills were delivered for the rest of the process. After the
    /// fix the trigger branch is disabled on `None` and the safety poll keeps
    /// firing — which is what this test asserts.
    #[tokio::test(start_paused = true)]
    async fn safety_poll_survives_ws_trigger_death() {
        let (ws_tx, ws_rx) = mpsc::channel::<DataMessage>(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let count = Arc::new(AtomicUsize::new(0));
        let poller = CountingPoller {
            count: Arc::clone(&count),
        };

        let handle = tokio::spawn(run_loop(ws_rx, tick, shutdown_rx, poller));

        // Simulate the WS trigger dying (poll-only degrade): drop the sender so
        // ws_rx closes and recv() resolves Ready(None) permanently.
        drop(ws_tx);

        // With the clock paused, tokio auto-advances to pending timers while the
        // loop is idle in `select!`. If the fix regressed to a hot-spin the loop
        // would never go idle, time would not advance, and this sleep would hang
        // (the test times out) with `count == 0`.
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Safety poll must have fired repeatedly despite the dead WS trigger.
        assert!(
            count.load(Ordering::SeqCst) >= 3,
            "safety poll starved after WS death (count = {})",
            count.load(Ordering::SeqCst)
        );

        // And shutdown still works — the loop is genuinely alive, not wedged.
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("driver did not stop on shutdown")
            .expect("driver task panicked");
    }

    fn ea_fill(trade_id: Option<&str>, side: &str, price: f64, size: u32) -> EaFill {
        EaFill {
            symbol: "XBTUSDTM".into(),
            order_id: "order-1".into(),
            side: side.into(),
            price,
            size,
            fee: 0.12,
            fee_currency: Some("USDT".into()),
            liquidity: Some("taker".into()),
            trade_id: trade_id.map(str::to_string),
            created_at: Some(1_700_000_000_000),
        }
    }

    #[test]
    fn seen_dedupes_and_evicts() {
        let mut seen = SeenFills::new(2);
        assert!(seen.insert("a"));
        assert!(seen.insert("b"));
        assert!(!seen.insert("a")); // already seen
        // Inserting a third evicts "a" (the oldest).
        assert!(seen.insert("c"));
        // "a" was evicted, so it now counts as new again.
        assert!(seen.insert("a"));
        // "b"/"c" still remembered.
        assert!(!seen.insert("c"));
    }

    #[test]
    fn fill_id_prefers_trade_id_then_falls_back() {
        assert_eq!(fill_id(&ea_fill(Some("t-42"), "buy", 1.0, 1)), "t-42");
        let no_tid = ea_fill(None, "buy", 1.0, 1);
        assert_eq!(fill_id(&no_tid), "order-1:1700000000000");
    }

    #[test]
    fn converts_recentfill_to_framework_fill() {
        let f = ea_fill(Some("t-1"), "sell", 65_000.5, 3);
        let out = ea_fill_to_fill("XBTUSDTM", &f);
        assert_eq!(out.symbol, Symbol::from("XBTUSDTM"));
        assert_eq!(out.order_id, "order-1");
        assert_eq!(out.side, Side::Sell);
        assert_eq!(out.price, Price(65_000.5));
        assert_eq!(out.size, Volume(3.0));
        assert!((out.fee - 0.12).abs() < 1e-9);
        assert_eq!(out.fee_currency, "USDT");
        assert_eq!(out.timestamp.timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn fill_side_defaults_to_buy_on_unknown() {
        let f = ea_fill(Some("t"), "weird", 1.0, 1);
        assert_eq!(ea_fill_to_fill("X", &f).side, Side::Buy);
    }

    #[test]
    fn missing_fee_currency_defaults_to_usdt() {
        let mut f = ea_fill(Some("t"), "buy", 1.0, 1);
        f.fee_currency = None;
        assert_eq!(ea_fill_to_fill("X", &f).fee_currency, "USDT");
    }
}
