//! The live portfolio engine: build venues from config, then poll → plan →
//! rebalance each one. Generalizes the standalone Kraken bot's loop over the
//! [`SpotExchange`] trait, multiple venues, and a cash reserve.
//!
//! Per-venue mode (independently decided): **paper** (no usable keys → simulated
//! book, real prices, no orders), **dry-run** (real balances, logs would-be
//! trades, no orders — when `live = false`), or **live** (real balances + real
//! orders). A venue with no usable keys can never place orders, even if the
//! global `live` flag is set.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tracing::{info, warn};

use exchange_apiws::client::Credentials;
use exchange_apiws::cryptocom::CryptocomCredentials;
use exchange_apiws::kraken::KrakenCredentials;

use crypto_bot_core::alerts::Alerter;
use crypto_bot_core::events::EventClient;
use crypto_bot_core::journal::Journal;
use crypto_bot_core::status::{self, HoldingStatus, VenueStatus};

use super::config::{AiMode, ExchangeConfig, PortfolioConfig};
use super::cryptocom::CryptocomSpot;
use super::exchange::{Fill, Side, SpotExchange};
use super::kraken::KrakenSpot;
use super::kucoin::KucoinSpot;
use super::rebalance::{self, Holding, Target, Trade};
use super::signals::{self, AiTilt, SignalSource, TiltParams};

/// Consecutive "triggered but no executable trade" cycles before the stuck-venue
/// alert fires (≈ poll_secs × this; 12 ≈ 1h at the default 5-min poll).
const STUCK_ALERT_CYCLES: u32 = 12;
/// Re-alert cadence (cycles) while a venue stays stuck (≈ 12h at a 5-min poll).
const STUCK_REALERT_CYCLES: u32 = 144;

/// Simulated holdings for a keyless paper venue (real prices, fake fills).
struct PaperBook {
    qty: HashMap<String, f64>,
    cash: f64,
}

/// One configured exchange + its rebalancing settings + per-venue runtime state.
struct Venue {
    ex: Box<dyn SpotExchange>,
    targets: Vec<Target>,
    reserve_pct: f64,
    band: f64,
    cooldown_secs: u64,
    min_trade_usd: f64,
    deposit_trigger_usd: f64,
    last_rebalance: Option<u64>,
    /// Cash seen at the start of the previous cycle (for deposit detection).
    last_cash: Option<f64>,
    /// Whether the previous cycle traded (so its cash change isn't read as a deposit).
    traded_last: bool,
    /// Consecutive cycles where the venue was triggered but produced no executable
    /// trade (can't self-correct) — drives the stuck-venue alert.
    stuck_cycles: u32,
    /// `Some` when the venue runs on a simulated book (no usable keys).
    paper: Option<PaperBook>,
    /// Whether THIS venue may place real orders — the global `live` switch AND
    /// the per-venue allowlist (`live_venues`). A non-allowlisted venue stays
    /// dry-run even when the portfolio is globally live.
    live: bool,
    /// Per traded asset after the previous cycle's LIVE fills:
    /// `(expected_total_qty, traded_delta)`. The next cycle checks the balance
    /// moved by ~`traded_delta` (divergence is measured against the DELTA, not the
    /// whole holding, so a fully-unsettled small trade on a large position is still
    /// caught). `None` when nothing is awaiting verification.
    pending_reconcile: Option<HashMap<String, (f64, f64)>>,
    /// Set when a submission's outcome could not be established. While this is
    /// `Some`, the venue places NO further orders — not after the cooldown
    /// expires, not on a deposit, and not for the rest of the batch the
    /// ambiguity occurred in.
    ///
    /// A cooldown is a TIMER; this is a HOLD. The distinction is the whole
    /// point: a timer expires on its own and hands the venue back without
    /// anyone having established what happened, which is precisely the
    /// condition under which a duplicate gets placed.
    unresolved: Option<UnresolvedSubmission>,
    /// Latest total value this venue reported (last successful cycle). Feeds the
    /// portfolio drawdown breaker so a transient per-venue error doesn't blind it.
    last_value: Option<f64>,
}

/// A submission whose outcome was never established.
///
/// Carries what was attempted so the alert and the journal can name it, and so
/// a human can look it up on the exchange.
#[derive(Clone, Debug)]
struct UnresolvedSubmission {
    asset: String,
    side: Side,
    usd: f64,
    at_secs: u64,
}

/// What actually happened to one rebalance trade — distinguishes a real fill
/// (reconcilable), a simulated dry-run/paper trade (acted, nothing to reconcile),
/// and a submission whose outcome was never established, so the cooldown and
/// reconciliation are armed on what is actually known rather than on a guess.
enum TradeOutcome {
    /// A real order filled — carries the fill for reconciliation.
    Filled(Fill),
    /// A dry-run or paper trade was simulated (no real balance moved).
    Simulated,
    /// A real order was submitted and the outcome is UNKNOWN.
    ///
    /// The adapter returned an error, which is not the same as the venue
    /// returning a rejection. A connection reset, a read timeout, or a 5xx can
    /// all arrive AFTER the broker has accepted the order — so the order may be
    /// working, may be filled, or may never have existed.
    ///
    /// Distinguished from `Failed` because the two demand opposite responses.
    /// `Failed` means nothing happened, so retrying next cycle is correct.
    /// Treating an unknown outcome the same way is what allows a second
    /// submission of an order that already filled — the balance has not caught
    /// up yet, so the next cycle sees the same drift and places the same trade
    /// again.
    ///
    /// This variant does NOT make the outcome known. It stops the platform from
    /// ASSERTING that nothing happened, and holds new risk on that venue until
    /// a human has looked.
    ///
    /// THERE IS DELIBERATELY NO `Failed` VARIANT. One existed, documented as
    /// "nothing happened", and every adapter error was mapped to it — including
    /// timeouts, which is the bug this replaces. Adding `Unknown` left `Failed`
    /// with no constructor at all, which is the honest answer: the adapters
    /// return `anyhow::Error` with no structured kind, so THIS LAYER CANNOT TELL
    /// a rejection from an ambiguity. A variant nothing can construct is a
    /// distinction the code does not actually make, and keeping one around
    /// invites a future reader to map errors into it by guesswork.
    ///
    /// Restoring a definite-rejection path is real work, not a variant: the
    /// adapters must surface a typed error that separates "the venue said no"
    /// from "we never heard back". Until they do, every submission error is
    /// treated as unknown, which is the conservative direction.
    Unknown,
}

/// The multi-venue portfolio engine.
pub struct Engine {
    venues: Vec<Venue>,
    poll_secs: u64,
    alerter: Alerter,
    /// Fire-and-forget spawner `/events` ingest for platform `risk_halt` events
    /// (trade-cap trip). A no-op unless the spawner injected `SPAWNER_EVENTS_*`
    /// (i.e. its `EVENTS_TOKEN` is set) — additive alongside the Discord alert.
    event_client: EventClient,
    /// Bot handle carried on emitted `risk_halt` events (container id / name;
    /// falls back to the bot's own name).
    event_bot_id: String,
    journal: Journal,
    // ── Risk guardrails (live-money) ─────────────────────────────────────────
    /// Drawdown breaker threshold; `None` = disabled.
    max_drawdown_pct: Option<f64>,
    /// Single-trade notional cap as a fraction of venue value; `None` = disabled.
    max_trade_pct: Option<f64>,
    /// Slippage alert threshold (fraction); `0` = disabled.
    max_slippage_pct: f64,
    /// Post-trade reconciliation tolerance (fraction); `None` = disabled.
    reconcile_tolerance_pct: Option<f64>,
    // ── janus AI signal bridge ───────────────────────────────────────────────
    /// Coupling mode: `Off` skips the bridge, `Shadow` logs-only, `On` trades the tilt.
    ai_mode: AiMode,
    /// Tilt gating/magnitude knobs (from the `ai_*` config).
    ai_params: TiltParams,
    /// Redis signal reader; `None` when AI is off or Redis was unreachable at boot
    /// (in which case the bot runs on config weights — fail-safe).
    ai_source: Option<SignalSource>,
    /// Portfolio net-worth high-water mark, updated each full poll.
    high_water: f64,
    /// Sticky halt flag: once a drawdown breach sets it, live trading stays OFF
    /// (all venues forced to dry-run) until an operator restart — no auto-resume
    /// into a falling market.
    halted: bool,
}

impl Engine {
    /// Build the engine from config: construct each venue's adapter, validate any
    /// API keys with a read-only balance probe, and pick paper vs real mode.
    pub async fn build(cfg: PortfolioConfig) -> Result<Self> {
        // Discord alerts: config `alert_webhook` if set, else the DISCORD_WEBHOOK
        // env var (no-op when neither is present).
        let alerter = Alerter::new(
            cfg.alert_webhook
                .clone()
                .or_else(|| env_str("DISCORD_WEBHOOK")),
        );
        let journal = Journal::new(cfg.journal.clone());

        let mut venues = Vec::new();
        for ec in &cfg.exchanges {
            let targets: Vec<Target> = ec
                .targets
                .iter()
                .map(|(n, w)| Target {
                    name: n.clone(),
                    weight: *w,
                })
                .collect();
            let ex = build_adapter(ec)?;

            // Validate keys (if any) before trusting them; fall back to paper.
            let paper = if ex.has_keys() {
                match ex.balances().await {
                    Ok(_) => {
                        info!(
                            venue = ex.name(),
                            "API keys validated — using real balances"
                        );
                        None
                    }
                    Err(e) => {
                        warn!(
                            venue = ex.name(),
                            error = format!("{e:#}"),
                            "key check failed — PAPER mode (simulated, no orders). Use a trade+query key and restart."
                        );
                        Some(new_paper(&targets, cfg.paper_usd))
                    }
                }
            } else {
                warn!(
                    venue = ex.name(),
                    paper_usd = cfg.paper_usd,
                    "no API keys — PAPER mode (fake cash, real prices)"
                );
                Some(new_paper(&targets, cfg.paper_usd))
            };

            // Per-venue live gate: real orders only when the global switch is on
            // AND this venue is on the allowlist (or no allowlist is set). A
            // keyless/paper venue can never place orders regardless.
            let venue_live = cfg.venue_is_live(&ec.name);
            info!(
                venue = ex.name(),
                cash = %ec.cash,
                reserve = format!("{:.0}%", ec.reserve_pct * 100.0),
                band = format!("±{:.0}%", ec.band * 100.0),
                basket = %targets.iter().map(|t| format!("{} {:.0}%", t.name, t.weight * 100.0)).collect::<Vec<_>>().join("/"),
                mode = if paper.is_some() { "paper" } else if venue_live { "LIVE" } else { "dry-run" },
                "venue ready"
            );
            venues.push(Venue {
                ex,
                targets,
                reserve_pct: ec.reserve_pct,
                band: ec.band,
                cooldown_secs: ec.cooldown_secs,
                min_trade_usd: ec.min_trade_usd,
                deposit_trigger_usd: ec.deposit_trigger_usd,
                last_rebalance: None,
                last_cash: None,
                traded_last: false,
                stuck_cycles: 0,
                paper,
                live: venue_live,
                pending_reconcile: None,
                unresolved: None,
                last_value: None,
            });
        }

        // janus AI bridge: wire it only when enabled. Connect to Redis once; if
        // it's unreachable the bot still runs on config weights — the AI is an
        // enhancement, never a hard dependency.
        let ai_mode = cfg.ai_signals;
        let ai_params = TiltParams {
            min_confidence: cfg.ai_min_confidence,
            max_age_secs: cfg.ai_max_signal_age_secs,
            tilt_max: cfg.ai_tilt_max,
        };
        let ai_source = if ai_mode == AiMode::Off {
            None
        } else {
            let assets: Vec<String> = venues
                .iter()
                .flat_map(|v| v.targets.iter().map(|t| t.name.clone()))
                .collect();
            let symbol_map = signals::default_symbol_map(&assets);
            if symbol_map.is_empty() {
                warn!(
                    "AI signals enabled but no basket asset maps to a janus symbol — bridge inert"
                );
                None
            } else {
                let mut mapped: Vec<String> = symbol_map.keys().cloned().collect();
                mapped.sort();
                let url = cfg.resolved_ai_redis_url();
                match SignalSource::connect(&url, symbol_map).await {
                    Ok(src) => {
                        info!(
                            mode = ?ai_mode,
                            tilt_max = format!("±{:.0}%", cfg.ai_tilt_max * 100.0),
                            min_confidence = cfg.ai_min_confidence,
                            assets = %mapped.join(","),
                            "janus AI signal bridge connected"
                        );
                        Some(src)
                    }
                    Err(e) => {
                        warn!(
                            error = format!("{e:#}"),
                            "AI signal Redis unreachable at boot — running on config weights"
                        );
                        None
                    }
                }
            }
        };

        // Spawner event ingest for risk_halt (no-op unless SPAWNER_EVENTS_* were
        // injected). The bot handle prefers the container id/name the spawner
        // exposes as HOSTNAME, then the bot's own name.
        let event_client = EventClient::from_env();
        let event_bot_id = env_str("FKS_BOT_ID")
            .or_else(|| env_str("HOSTNAME"))
            .unwrap_or_else(|| "spot-portfolio".to_string());

        Ok(Self {
            venues,
            poll_secs: cfg.poll_secs,
            alerter,
            event_client,
            event_bot_id,
            journal,
            max_drawdown_pct: cfg.max_drawdown_pct,
            max_trade_pct: cfg.max_trade_pct,
            max_slippage_pct: cfg.max_slippage_pct,
            reconcile_tolerance_pct: cfg.reconcile_tolerance_pct,
            ai_mode,
            ai_params,
            ai_source,
            high_water: 0.0,
            halted: false,
        })
    }

    /// Run the poll-and-rebalance loop until Ctrl-C / SIGTERM.
    pub async fn run(mut self) -> Result<()> {
        let live_venues: Vec<&str> = self
            .venues
            .iter()
            .filter(|v| v.live)
            .map(|v| v.ex.name())
            .collect();
        self.alerter.notify(format!(
            "🟢 spot-portfolio started — {} venue(s); LIVE: {}",
            self.venues.len(),
            if live_venues.is_empty() {
                "none (all dry-run/paper)".to_string()
            } else {
                live_venues.join(", ")
            }
        ));

        let mut poll = tokio::time::interval(Duration::from_secs(self.poll_secs.max(5)));
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("installing SIGTERM handler")?;
        info!(
            poll_secs = self.poll_secs,
            venues = self.venues.len(),
            "spot-portfolio entering main loop — Ctrl-C to stop"
        );

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = sigterm.recv() => break,
                _ = poll.tick() => {
                    let halted = self.halted;
                    let (mtp, msp, rtp) =
                        (self.max_trade_pct, self.max_slippage_pct, self.reconcile_tolerance_pct);
                    // Read janus's latest signals once per poll — shared by every
                    // venue. Fail-safe: fetch never errors (missing reads are just
                    // omitted), so a Redis blip leaves the bot on config weights.
                    let ai = match &self.ai_source {
                        Some(src) if self.ai_mode != AiMode::Off => Some(AiTilt {
                            mode: self.ai_mode,
                            params: self.ai_params,
                            signals: src.fetch(now_millis()).await,
                        }),
                        _ => None,
                    };
                    let ai_ref = ai.as_ref();
                    for venue in self.venues.iter_mut() {
                        // Per-venue live gate AND the sticky drawdown halt: an
                        // unverified venue (absent from `live_venues`) or a tripped
                        // breaker forces dry-run this cycle.
                        let vlive = venue.live && !halted;
                        match run_cycle(venue, vlive, mtp, msp, rtp, ai_ref, &self.alerter, &self.event_client, &self.event_bot_id, &self.journal).await {
                            Ok(value) => venue.last_value = Some(value),
                            Err(e) => {
                                warn!(venue = venue.ex.name(), error = format!("{e:#}"), "rebalance cycle failed");
                            }
                        }
                    }
                    // Drawdown breaker net worth = LIVE, REAL (non-paper) venues only,
                    // using each venue's latest known value. This keeps the breaker
                    // working during a partial outage (a transient per-venue error
                    // reuses that venue's last value instead of suppressing the whole
                    // check) and stops a paper venue's fake capital from diluting it.
                    let breaker_nw: f64 = self
                        .venues
                        .iter()
                        .filter(|v| v.live && v.paper.is_none())
                        .filter_map(|v| v.last_value)
                        .sum();
                    self.check_drawdown(breaker_nw).await;
                }
            }
        }

        info!("shutdown requested — leaving baskets as-is");
        self.alerter
            .notify_blocking("🛑 spot-portfolio stopped")
            .await;
        info!("stopped");
        Ok(())
    }

    /// Drawdown circuit breaker. Updates the net-worth high-water mark and, when
    /// armed (`max_drawdown_pct`), HALTS live trading if net worth has fallen more
    /// than the limit below the peak. The halt is **sticky** — once tripped, every
    /// venue runs dry-run until the operator restarts the bot, so it can never
    /// auto-resume live into a falling market. Halting means *stop trading*, not
    /// liquidate: existing positions are left exactly as they are.
    async fn check_drawdown(&mut self, net_worth: f64) {
        if !net_worth.is_finite() || net_worth <= 0.0 {
            return; // no usable reading (e.g. paper-only / prices missing)
        }
        let Some(dd) = self.max_drawdown_pct else {
            return; // breaker disabled
        };
        if self.halted {
            return; // already tripped — sticky
        }
        let (hw, tripped) = drawdown_decision(net_worth, self.high_water, dd);
        self.high_water = hw;
        if tripped {
            self.halted = true;
            let drop_pct = (1.0 - net_worth / self.high_water) * 100.0;
            let msg = format!(
                "🛑 DRAWDOWN BREAKER TRIPPED — net worth ${:.2} is {:.1}% below the ${:.2} \
                 high-water mark (limit {:.0}%). LIVE TRADING HALTED — every venue is now \
                 dry-run. Positions are untouched. Restart the bot to resume live.",
                net_worth,
                drop_pct,
                self.high_water,
                dd * 100.0,
            );
            warn!(
                net_worth = format!("{net_worth:.2}"),
                high_water = format!("{:.2}", self.high_water),
                drop_pct = format!("{drop_pct:.1}"),
                "drawdown breaker tripped — live trading halted"
            );
            self.alerter.notify_blocking(msg).await;
        }
    }
}

/// One rebalance check for a single venue: prices → holdings → plan → maybe
/// trade. Returns the venue's total value (for the portfolio drawdown breaker).
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    venue: &mut Venue,
    live: bool,
    max_trade_pct: Option<f64>,
    max_slippage_pct: f64,
    reconcile_tolerance_pct: Option<f64>,
    ai: Option<&AiTilt>,
    alerter: &Alerter,
    event_client: &EventClient,
    bot_id: &str,
    journal: &Journal,
) -> Result<f64> {
    // 1. Live prices for each target asset.
    let mut prices: HashMap<String, f64> = HashMap::new();
    for t in &venue.targets {
        let px = venue
            .ex
            .price(&t.name)
            .await
            .with_context(|| format!("{}: fetching price for {}", venue.ex.name(), t.name))?;
        prices.insert(t.name.clone(), px);
    }

    // 2. Holdings + cash: simulated (paper) or real balances.
    let (holdings, cash) = match &venue.paper {
        Some(book) => {
            let hs = venue
                .targets
                .iter()
                .map(|t| Holding {
                    name: t.name.clone(),
                    qty: *book.qty.get(&t.name).unwrap_or(&0.0),
                    price: *prices.get(&t.name).unwrap_or(&0.0),
                })
                .collect::<Vec<_>>();
            (hs, book.cash)
        }
        None => {
            let bals = venue
                .ex
                .balances()
                .await
                .with_context(|| format!("{}: reading balances", venue.ex.name()))?;
            let free = |asset: &str| {
                bals.iter()
                    .find(|b| b.asset == asset)
                    .map_or(0.0, |b| b.free)
            };
            let hs = venue
                .targets
                .iter()
                .map(|t| Holding {
                    name: t.name.clone(),
                    qty: free(&t.name),
                    price: *prices.get(&t.name).unwrap_or(&0.0),
                })
                .collect::<Vec<_>>();
            (hs, free(venue.ex.cash_asset()))
        }
    };

    // 2b. Reconciliation: verify the previous cycle's LIVE fills actually settled
    // on the exchange — each traded asset's balance should now match the expected
    // post-fill quantity. A divergence beyond tolerance means the exchange didn't
    // apply an order as reported; alert so it surfaces instead of the bot silently
    // re-planning around a stale balance.
    if let Some(expected) = venue.pending_reconcile.take()
        && let Some(tol) = reconcile_tolerance_pct
    {
        let actual: HashMap<String, f64> =
            holdings.iter().map(|h| (h.name.clone(), h.qty)).collect();
        let mismatches = reconcile_check(&expected, &actual, tol);

        // CLEARING THE HOLD. Reconciliation is the only automatic path out:
        // the balances agreed with what we expected, so the book is describable
        // again. A hold is never cleared by time passing, because nothing about
        // waiting establishes what happened to the order.
        //
        // Note this can only fire when there was something to reconcile. A hold
        // taken on the FIRST trade of a batch leaves no expectation behind, so
        // it persists until an operator acts — which is the correct asymmetry:
        // the less we know, the longer we hold.
        if mismatches.is_empty() && venue.unresolved.is_some() {
            let u = venue.unresolved.take();
            if let Some(u) = u {
                info!(
                    venue = venue.ex.name(), asset = %u.asset,
                    "hold CLEARED — balances reconciled to expectation"
                );
                alerter.notify(format!(
                    "✅ {} hold cleared — balances reconciled after the unconfirmed {:?} {}. Trading resumes.",
                    venue.ex.name(), u.side, u.asset
                ));
            }
        }

        for (asset, exp_qty, act_qty, div) in mismatches {
            warn!(
                venue = venue.ex.name(), asset = %asset,
                expected = format!("{exp_qty:.8}"), actual = format!("{act_qty:.8}"),
                divergence = format!("{:.1}%", div * 100.0),
                "reconciliation mismatch — live fill may not have settled"
            );
            alerter.notify(format!(
                "🔎 {} reconciliation mismatch on {}: expected {:.6} after fills, balance shows {:.6} ({:.1}% off) — an order may not have settled.",
                venue.ex.name(), asset, exp_qty, act_qty, div * 100.0
            ));
        }
    }

    // 3. Targets — optionally tilted by the janus AI bridge. In `Shadow` the tilt
    //    is computed and LOGGED but the plan still runs on config weights; in `On`
    //    it re-splits the invested sleeve (bounded by `tilt_max`, reserve + asset
    //    set untouched). Absent/off ⇒ plain config targets.
    let effective_targets: Vec<Target> = match ai {
        Some(ai) => {
            let tilted = signals::apply_tilt(&venue.targets, &ai.signals, &ai.params);
            let applied = ai.mode == AiMode::On;
            // Log the AI evaluation EVERY poll (not only when a tilt fires) so the
            // shadow trial is analyzable in Loki: each signal + why it did/didn't
            // tilt, and the net would-be tilt.
            info!(
                venue = venue.ex.name(),
                mode = ?ai.mode,
                applied,
                eval = %signals::eval_summary(&ai.signals, &ai.params),
                tilt = %signals::tilt_summary(&venue.targets, &tilted)
                    .unwrap_or_else(|| "none".to_string()),
                "AI signal eval"
            );
            if applied {
                tilted
            } else {
                venue.targets.clone()
            }
        }
        None => venue.targets.clone(),
    };

    // 3b. Plan.
    let p = rebalance::plan(
        &holdings,
        cash,
        &effective_targets,
        venue.reserve_pct,
        venue.band,
        venue.min_trade_usd,
    );

    // 4. Snapshot log.
    let weights = p
        .weights
        .iter()
        .map(|(n, w)| format!("{n} {:.1}%", w * 100.0))
        .collect::<Vec<_>>()
        .join("  ");
    info!(
        venue = venue.ex.name(),
        total = format!("{:.2}", p.total_value),
        cash = format!("{:.2}", cash),
        weights = %weights,
        max_drift = format!("{:.1}%", p.max_drift * 100.0),
        triggered = p.triggered,
        "portfolio"
    );

    // 4b. Publish the venue snapshot to the status server (/status, /metrics).
    if let Some(status) = status::get() {
        let reserve = venue.reserve_pct.clamp(0.0, 1.0);
        let weight_of = |name: &str| {
            p.weights
                .iter()
                .find(|(n, _)| n == name)
                .map_or(0.0, |(_, w)| *w)
        };
        let target_of = |name: &str| {
            venue
                .targets
                .iter()
                .find(|t| t.name == name)
                .map_or(0.0, |t| t.weight * (1.0 - reserve))
        };
        status.update_venue(VenueStatus {
            exchange: venue.ex.name().to_string(),
            mode: if venue.paper.is_some() {
                "paper".to_string()
            } else if live {
                "live".to_string()
            } else {
                "dry-run".to_string()
            },
            cash_asset: venue.ex.cash_asset().to_string(),
            cash,
            total_value: p.total_value,
            max_drift: p.max_drift,
            triggered: p.triggered,
            last_rebalance: venue.last_rebalance,
            updated: now_secs(),
            holdings: holdings
                .iter()
                .map(|h| HoldingStatus {
                    asset: h.name.clone(),
                    qty: h.qty,
                    price: h.price,
                    value: h.qty * h.price,
                    weight: weight_of(&h.name),
                    target_weight: target_of(&h.name),
                })
                .collect(),
        });
    }

    // 4b. THE UNRESOLVED HOLD.
    //
    // Deliberately ahead of BOTH deposit detection and the cooldown check,
    // because those were the two routes around it. A deposit explicitly
    // "bypasses the cooldown", and the cooldown expires on its own — so a hold
    // expressed as a cooldown is not a hold at all. Placing it here means no
    // timer and no cash event can hand the venue back.
    //
    // Clearing is deliberately NOT automatic on a timer: it happens when
    // reconciliation shows the balance matches expectation (§2b), or when an
    // operator acts. Uncertainty does not resolve by waiting.
    if let Some(u) = venue.unresolved.clone() {
        let age = now_secs().saturating_sub(u.at_secs);
        warn!(
            venue = venue.ex.name(), asset = %u.asset, side = ?u.side,
            usd = format!("{:.2}", u.usd), held_for_secs = age,
            "VENUE HELD — a submission's outcome was never established; placing no orders"
        );
        // Re-alert on a slow cadence so a hold cannot be forgotten, without
        // paging every poll.
        if age.is_multiple_of(3600) {
            alerter.notify(format!(
                "⛔ {} STILL HELD after {}m — {:?} {} (~${:.2}) was never confirmed. \
                 No orders are being placed on this venue. Reconcile it against the \
                 exchange, then restart the bot to clear the hold.",
                venue.ex.name(),
                age / 60,
                u.side,
                u.asset,
                u.usd
            ));
        }
        // The venue's value is still a true reading — only ORDERS are held.
        return Ok(p.total_value);
    }

    // 5. Deposit detection: a cash jump since the previous (non-trading) cycle
    //    means funds were added — rebalance now, bypassing the cooldown.
    let deposit = venue.deposit_trigger_usd > 0.0
        && !venue.traded_last
        && venue
            .last_cash
            .is_some_and(|prev| cash - prev > venue.deposit_trigger_usd);
    if deposit {
        info!(
            venue = venue.ex.name(),
            added = format!("{:.2}", cash - venue.last_cash.unwrap_or(cash)),
            "deposit detected — rebalancing now (bypassing cooldown)"
        );
    }

    // 6. Rebalance if triggered + has trades + (past cooldown OR a deposit landed).
    let mut traded = false;
    if p.triggered && !p.trades.is_empty() {
        let now = now_secs();
        let within_cooldown = venue
            .last_rebalance
            .is_some_and(|last| now.saturating_sub(last) < venue.cooldown_secs);
        if within_cooldown && !deposit {
            let wait =
                venue.cooldown_secs - now.saturating_sub(venue.last_rebalance.unwrap_or(now));
            info!(
                venue = venue.ex.name(),
                wait_secs = wait,
                "drift past band but within the rebalance cooldown — skipping"
            );
        } else {
            let trigger = if deposit { "deposit" } else { "drift" };
            info!(
                venue = venue.ex.name(),
                trades = p.trades.len(),
                max_drift = format!("{:.1}%", p.max_drift * 100.0),
                trigger,
                "REBALANCE"
            );
            alerter.notify(format!(
                "⚖️ {} rebalance ({trigger}): {} trades, drift {:.0}%, ${:.0}",
                venue.ex.name(),
                p.trades.len(),
                p.max_drift * 100.0,
                p.total_value
            ));
            // Per traded asset: (expected_total_qty, traded_delta) after real
            // fills — armed for the next cycle's reconciliation.
            let mut expected: HashMap<String, (f64, f64)> = HashMap::new();
            // Count trades that actually went through (real fill OR simulated).
            // Cap-skips and failed orders don't count — so a cycle where nothing
            // executed doesn't arm the cooldown / deposit-suppression on a non-event.
            let mut acted = 0usize;
            // Set when a submission errored: the outcome was never established.
            let mut unknown_outcome = false;
            for tr in &p.trades {
                // Trade-size cap: one rebalance trade should never move a large
                // fraction of the venue. A plan that does is a mispricing/bug —
                // skip it (and alert) rather than send a huge real order.
                if let Some(cap_pct) = max_trade_pct
                    && trade_exceeds_cap(tr.usd, p.total_value, cap_pct)
                {
                    let cap = cap_pct * p.total_value;
                    warn!(
                        venue = venue.ex.name(), asset = %tr.name,
                        usd = format!("{:.2}", tr.usd), cap = format!("{:.2}", cap),
                        "trade exceeds max_trade_pct cap — SKIPPING"
                    );
                    alerter.notify(format!(
                        "🚫 {} skipped a {} trade of ${:.0} — exceeds the {:.0}% cap (${:.0}) of the ${:.0} venue value. Likely a mispriced/buggy plan.",
                        venue.ex.name(), tr.name, tr.usd, cap_pct * 100.0, cap, p.total_value
                    ));
                    // Raise a platform `risk_halt` through the spawner ingest IN
                    // ADDITION to the Discord alert (no-op unless SPAWNER_EVENTS_*
                    // were injected). The TRADE-CAP guard is a halt-class guard;
                    // the drawdown breaker deliberately is NOT emitted here (the
                    // live spot bot runs breaker-off by HODL policy).
                    let mode = if venue.paper.is_some() {
                        "paper"
                    } else if live {
                        "live"
                    } else {
                        "dry-run"
                    };
                    event_client.fire_risk_halt(
                        bot_id,
                        mode,
                        format!(
                            "trade-cap guard: {} {} trade ${:.0} exceeds the {:.0}% cap (${:.0}) of ${:.0} venue value — order SKIPPED",
                            venue.ex.name(),
                            tr.name,
                            tr.usd,
                            cap_pct * 100.0,
                            cap,
                            p.total_value
                        ),
                    );
                    continue;
                }
                match execute_trade(venue, tr, live, max_slippage_pct, alerter, journal).await {
                    TradeOutcome::Filled(fill) => {
                        acted += 1;
                        // expected total = current holding ± filled base qty; the
                        // traded delta is the settlement amount to reconcile against.
                        let start = holdings
                            .iter()
                            .find(|h| h.name == fill.asset)
                            .map_or(0.0, |h| h.qty);
                        let delta = match fill.side {
                            Side::Buy => fill.base_qty,
                            Side::Sell => -fill.base_qty,
                        };
                        let entry = expected.entry(fill.asset.clone()).or_insert((start, 0.0));
                        entry.0 += delta;
                        entry.1 += delta;
                    }
                    TradeOutcome::Simulated => acted += 1,
                    // Not counted as `acted` — nothing is KNOWN to have moved,
                    // so there is no delta to reconcile against.
                    //
                    // BREAKS the batch. The remaining trades in this plan were
                    // sized against a book we can no longer describe: if the
                    // ambiguous order did fill, every subsequent trade is
                    // computed from a stale balance. Continuing the loop was
                    // the first of the three gaps in this design.
                    TradeOutcome::Unknown => {
                        unknown_outcome = true;
                        venue.unresolved = Some(UnresolvedSubmission {
                            asset: tr.name.clone(),
                            side: tr.side,
                            usd: tr.usd,
                            at_secs: now_secs(),
                        });
                        warn!(
                            venue = venue.ex.name(), asset = %tr.name,
                            remaining = p.trades.len(),
                            "HOLDING the venue — abandoning the rest of this batch"
                        );
                        break;
                    }
                }
            }
            // Only arm the cooldown + reconciliation when a trade genuinely went
            // through. If every trade was cap-skipped or every order was REJECTED,
            // leave the venue free to retry next cycle (and don't suppress deposit
            // detect).
            //
            // An UNKNOWN outcome is the exception, and that distinction is the whole
            // point of the variant: "the venue rejected it" and "we never found out"
            // are not the same fact. Freeing the venue after an unknown submission is
            // what lets the next cycle re-place an order that may already have filled,
            // because the balance has not caught up to say otherwise.
            if unknown_outcome {
                venue.last_rebalance = Some(now);
            }
            if acted > 0 {
                venue.last_rebalance = Some(now);
                traded = true;
                if !expected.is_empty() && reconcile_tolerance_pct.is_some() {
                    venue.pending_reconcile = Some(expected);
                }
            }
        }
    }

    // 7. Stuck-venue watchdog: a venue that is triggered every cycle but produces NO
    //    executable trade can't self-correct (e.g. every corrective trade is dust-
    //    filtered — the Crypto.com SOL wedge that sat silent for ~340 cycles). Count
    //    consecutive such cycles and alert, so it surfaces instead of failing quietly.
    if p.triggered && p.trades.is_empty() {
        venue.stuck_cycles += 1;
        let n = venue.stuck_cycles;
        if n == STUCK_ALERT_CYCLES
            || (n > STUCK_ALERT_CYCLES && n.is_multiple_of(STUCK_REALERT_CYCLES))
        {
            warn!(
                venue = venue.ex.name(),
                cycles = n,
                max_drift = format!("{:.0}%", p.max_drift * 100.0),
                "venue STUCK — triggered but no executable trade"
            );
            alerter.notify(format!(
                "⚠️ {} stuck: {:.0}% drift but no executable trade for {} cycles — a target slice is likely under min_trade_usd",
                venue.ex.name(),
                p.max_drift * 100.0,
                n
            ));
        }
    } else {
        venue.stuck_cycles = 0;
    }

    // 8. Remember this cycle's cash + whether we traded, for next-cycle deposit detection.
    venue.last_cash = Some(cash);
    venue.traded_last = traded;
    Ok(p.total_value)
}

/// Place (or simulate) one rebalancing trade and record/alert it. Returns a
/// [`TradeOutcome`]: `Filled` (real order, carries the fill for reconciliation),
/// `Simulated` (dry-run/paper), or `Failed` (real order rejected).
async fn execute_trade(
    venue: &mut Venue,
    tr: &Trade,
    live: bool,
    max_slippage_pct: f64,
    alerter: &Alerter,
    journal: &Journal,
) -> TradeOutcome {
    let real = live && venue.paper.is_none();
    let tag = if real { "" } else { "[DRY] " };

    // The realized fill: a real order's resolved fill, or the planned trade as a
    // stand-in for dry-run / paper.
    let fill = if real {
        let res = match tr.side {
            Side::Buy => venue.ex.market_buy(&tr.name, tr.usd).await,
            Side::Sell => venue.ex.market_sell(&tr.name, tr.volume).await,
        };
        match res {
            Ok(f) => {
                info!(
                    venue = venue.ex.name(), asset = %tr.name, side = ?tr.side,
                    usd = format!("{:.2}", f.quote_usd), qty = format!("{:.8}", f.base_qty),
                    price = format!("{:.2}", f.avg_price),
                    "LIVE: market order filled"
                );
                f
            }
            Err(e) => {
                // The adapter errored. That is NOT the venue rejecting the
                // order — a timeout or reset can arrive after the broker has
                // accepted it, so the order may be working, filled, or absent.
                warn!(
                    venue = venue.ex.name(), asset = %tr.name, side = ?tr.side,
                    error = format!("{e:#}"),
                    "order submission FAILED OR UNKNOWN — outcome not established"
                );
                alerter.notify(format!(
                    "⚠️ UNKNOWN ORDER OUTCOME on {} — {:?} {} (~${:.2}) errored during \
                     submission. It may have filled. The venue is held rather than retried; \
                     check the exchange before trading it by hand.",
                    venue.ex.name(),
                    tr.side,
                    tr.name,
                    tr.usd
                ));
                return TradeOutcome::Unknown;
            }
        }
    } else {
        info!(
            venue = venue.ex.name(), asset = %tr.name, side = ?tr.side,
            usd = format!("{:.2}", tr.usd), price = format!("{:.2}", tr.price),
            "DRY-RUN: would place market order"
        );
        Fill {
            asset: tr.name.clone(),
            side: tr.side,
            base_qty: tr.volume,
            avg_price: tr.price,
            quote_usd: tr.usd,
            // dry-run stand-in: nothing was submitted, so nothing is confirmed
            resolved: false,
        }
    };

    // UNCONFIRMED EXECUTION. The adapter could not resolve this order against
    // the venue and returned the values we asked for. Something may have
    // filled, partially filled, or not filled at all; the numbers below are an
    // assumption, not a measurement, and the position is now unknown.
    //
    // Alert-only for now, deliberately: refusing to journal it would lose the
    // record entirely, and halting here is a bigger behavioural change than
    // belongs in a detection fix. The full repair is an order lifecycle with an
    // explicit unknown state and reconciliation before new risk is taken.
    if real && !fill.resolved {
        warn!(
            venue = venue.ex.name(), asset = %tr.name, side = ?tr.side,
            assumed_usd = format!("{:.2}", fill.quote_usd),
            assumed_qty = format!("{:.8}", fill.base_qty),
            "UNCONFIRMED FILL — venue lookup failed; journalled values are REQUESTED, not executed"
        );
        alerter.notify(format!(
            "⚠️ UNCONFIRMED FILL on {} — {:?} {} could not be confirmed with the venue. \
             Journalled ~${:.2} @ {:.2} are the values we ASKED for, not a measured execution. \
             Reconcile against the exchange before trusting the position.",
            venue.ex.name(),
            tr.side,
            tr.name,
            fill.quote_usd,
            fill.avg_price
        ));
    }

    // Slippage / bad-fill anomaly alert: a real fill whose average price strayed
    // far from the planned price signals thin liquidity, a stale quote, or a
    // mispriced order. Alert-only — the fill already happened; this surfaces it.
    //
    // Gated on `fill.resolved` because slippage is `planned vs filled`, and on
    // an unresolved fill those are literally the same number — the check would
    // report a reassuring 0.00% at exactly the moment the fill is least
    // trustworthy. A vacuous pass is worse than no check: it is evidence of
    // nothing that reads as evidence of correctness.
    if real
        && fill.resolved
        && let Some(slip) = slippage_exceeds(tr.price, fill.avg_price, max_slippage_pct)
    {
        warn!(
            venue = venue.ex.name(), asset = %tr.name,
            planned = format!("{:.2}", tr.price), filled = format!("{:.2}", fill.avg_price),
            slippage = format!("{:.2}%", slip * 100.0),
            "LIVE fill slippage exceeded threshold"
        );
        alerter.notify(format!(
            "⚠️ {} {} fill slipped {:.1}% — planned ${:.2}, filled ${:.2} (threshold {:.1}%)",
            venue.ex.name(),
            tr.name,
            slip * 100.0,
            tr.price,
            fill.avg_price,
            max_slippage_pct * 100.0
        ));
    }

    journal.record(serde_json::json!({
        "event": "rebalance_trade", "venue": venue.ex.name(), "live": real,
        "asset": fill.asset, "side": format!("{:?}", fill.side),
        "volume": fill.base_qty, "usd": fill.quote_usd, "price": fill.avg_price,
        // Provenance of the three numbers above. `false` means the venue could
        // not confirm the order and these are the REQUESTED values — the row is
        // an intent record, not an execution record. Anything computing
        // performance or reconciliation from this journal must filter on it.
        "fill_confirmed": fill.resolved,
    }));
    if let Some(status) = status::get() {
        status.record_trade();
        status.push_event(serde_json::json!({
            "event": "rebalance_trade", "venue": venue.ex.name(), "live": real,
            "asset": fill.asset, "side": format!("{:?}", fill.side),
            "volume": fill.base_qty, "usd": fill.quote_usd, "price": fill.avg_price,
        }));
    }
    let mark = if tr.side == Side::Buy {
        "🟩 BUY"
    } else {
        "🟥 SELL"
    };
    alerter.notify(format!(
        "{tag}{mark} {:.6} {} (~${:.2} @ {:.2}) on {}",
        fill.base_qty,
        fill.asset,
        fill.quote_usd,
        fill.avg_price,
        venue.ex.name()
    ));

    // Keep the paper book in sync so later cycles see the simulated basket.
    if let Some(book) = &mut venue.paper {
        let q = book.qty.entry(tr.name.clone()).or_insert(0.0);
        match tr.side {
            Side::Buy => {
                *q += tr.volume;
                book.cash -= tr.usd;
            }
            Side::Sell => {
                *q -= tr.volume;
                book.cash += tr.usd;
            }
        }
    }

    // Hand the real fill back for reconciliation; dry-run/paper trades don't
    // move a real balance, so there's nothing to reconcile.
    if real {
        TradeOutcome::Filled(fill)
    } else {
        TradeOutcome::Simulated
    }
}

fn build_adapter(c: &ExchangeConfig) -> Result<Box<dyn SpotExchange>> {
    match c.name.as_str() {
        "kraken" => {
            let creds = match (env_str("KRAKEN_API_KEY"), env_str("KRAKEN_API_SECRET")) {
                (Some(_), Some(_)) => KrakenCredentials::from_env().ok(),
                _ => None,
            };
            Ok(Box::new(KrakenSpot::new(c.cash.clone(), creds)?))
        }
        "cryptocom" => {
            let creds = match (
                env_str("CRYPTOCOM_API_KEY"),
                env_str("CRYPTOCOM_API_SECRET"),
            ) {
                (Some(_), Some(_)) => CryptocomCredentials::from_env().ok(),
                _ => None,
            };
            Ok(Box::new(CryptocomSpot::new(c.cash.clone(), creds)?))
        }
        // KuCoin SPOT — same exchange as the futures bot, separate binary. Prefers
        // a dedicated spot key (KUCOIN_API_*); falls back to the futures bot's KC_*
        // creds, since a KuCoin key is account-wide and can carry both spot +
        // futures permissions ("kucoin for futures and spot").
        "kucoin" => {
            let creds = match (
                env_str("KUCOIN_API_KEY"),
                env_str("KUCOIN_API_SECRET"),
                env_str("KUCOIN_API_PASSPHRASE"),
            ) {
                (Some(k), Some(s), Some(p)) => Some(Credentials::new(k, s, p)),
                _ => Credentials::from_env().ok(), // KC_KEY / KC_SECRET / KC_PASSPHRASE
            };
            Ok(Box::new(KucoinSpot::new(c.cash.clone(), creds)?))
        }
        other => bail!("unknown exchange '{other}' (supported: kraken, cryptocom, kucoin)"),
    }
}

fn new_paper(targets: &[Target], cash: f64) -> PaperBook {
    PaperBook {
        qty: targets.iter().map(|t| (t.name.clone(), 0.0)).collect(),
        cash,
    }
}

fn env_str(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Unix time in milliseconds — matches janus's signal sorted-set score, so the
/// AI bridge can age a signal without parsing its timestamp string.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Risk guardrail helpers (pure — unit-tested without exchanges/runtime)
// ─────────────────────────────────────────────────────────────────────────────

/// Drawdown decision: given the current `net_worth`, the prior `high_water`, and
/// the drawdown limit `dd` (fraction), return `(new_high_water, tripped)`.
/// `tripped` is true when net worth has fallen more than `dd` below the peak.
fn drawdown_decision(net_worth: f64, high_water: f64, dd: f64) -> (f64, bool) {
    let hw = high_water.max(net_worth);
    let tripped = hw > 0.0 && net_worth < hw * (1.0 - dd);
    (hw, tripped)
}

/// True if a single trade's notional exceeds `cap_pct` of the venue's value.
/// A non-positive value or cap is treated as "no cap" (never exceeds).
fn trade_exceeds_cap(trade_usd: f64, total_value: f64, cap_pct: f64) -> bool {
    total_value > 0.0 && cap_pct > 0.0 && trade_usd > cap_pct * total_value
}

/// Slippage fraction when a fill's `filled` price strays from the `planned`
/// price by more than `threshold`; `None` if within threshold or disabled.
fn slippage_exceeds(planned: f64, filled: f64, threshold: f64) -> Option<f64> {
    if threshold <= 0.0 || planned <= 0.0 {
        return None;
    }
    let slip = (filled - planned).abs() / planned;
    (slip > threshold).then_some(slip)
}

/// Post-trade reconciliation: for each traded asset, `expected` holds
/// `(expected_total_qty, traded_delta)`. Compare the `actual` balance to the
/// expected total, but measure divergence **against the traded delta** (the
/// amount that was supposed to move) — so a fully-unsettled small trade on a
/// large existing position still shows ~100% divergence and is flagged, instead
/// of being diluted below tolerance by the untouched holding. Returns the assets
/// whose divergence exceeds `tol`, as `(asset, expected_total, actual, div)`.
fn reconcile_check(
    expected: &HashMap<String, (f64, f64)>,
    actual: &HashMap<String, f64>,
    tol: f64,
) -> Vec<(String, f64, f64, f64)> {
    let mut out = Vec::new();
    for (asset, &(exp_total, delta)) in expected {
        let denom = delta.abs();
        if denom < 1e-12 {
            continue; // nothing was supposed to move → nothing to reconcile
        }
        let act = actual.get(asset).copied().unwrap_or(0.0);
        let div = (act - exp_total).abs() / denom;
        if div > tol {
            out.push((asset.clone(), exp_total, act, div));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    /// WHY THE SLIPPAGE CHECK IS GATED ON `fill.resolved`.
    ///
    /// An unresolved fill carries the REQUESTED price as its average price, so
    /// `planned` and `filled` are the same number and slippage is exactly zero
    /// — no matter how badly the real order actually executed, or whether it
    /// executed at all. Ungated, the check would report a reassuring 0.00%
    /// precisely when the fill is least trustworthy.
    ///
    /// This pins that arithmetic so nobody "simplifies" the gate away later.
    #[test]
    fn an_unresolved_fill_makes_the_slippage_check_vacuous() {
        let planned = 100.0;
        // What an unresolved fill looks like: avg_price IS the planned price,
        // because the adapter had nothing better to report.
        let unresolved_filled = planned;
        assert_eq!(
            slippage_exceeds(planned, unresolved_filled, 0.001),
            None,
            "an unresolved fill can never trip the slippage alert — that is why \
             the caller must gate on fill.resolved rather than trusting a pass"
        );

        // A genuinely resolved fill at the same threshold DOES trip it, so the
        // check is not simply inert.
        assert!(slippage_exceeds(planned, 105.0, 0.001).is_some());
    }

    /// A fill's provenance must survive into the record, because the journal is
    /// what every later performance and reconciliation claim is computed from.
    #[test]
    fn fill_provenance_distinguishes_measured_from_assumed() {
        let confirmed = Fill {
            asset: "BTC".into(),
            side: Side::Buy,
            base_qty: 0.5,
            avg_price: 100.0,
            quote_usd: 50.0,
            resolved: true,
        };
        let assumed = Fill {
            resolved: false,
            ..confirmed.clone()
        };
        // Identical numbers, different epistemic status — which is exactly the
        // failure mode: they are indistinguishable without this flag.
        assert_eq!(confirmed.base_qty, assumed.base_qty);
        assert_eq!(confirmed.avg_price, assumed.avg_price);
        assert!(confirmed.resolved && !assumed.resolved);
    }
    use super::*;

    #[test]
    fn drawdown_updates_high_water_and_never_trips_on_new_peak() {
        let (hw, tripped) = drawdown_decision(120.0, 100.0, 0.15);
        assert_eq!(hw, 120.0, "high-water rises to the new peak");
        assert!(!tripped, "a new peak never trips");
    }

    #[test]
    fn drawdown_within_limit_does_not_trip() {
        // 10% below the 100 peak, limit 15% → no trip; peak unchanged.
        let (hw, tripped) = drawdown_decision(90.0, 100.0, 0.15);
        assert_eq!(hw, 100.0);
        assert!(!tripped);
    }

    #[test]
    fn drawdown_beyond_limit_trips() {
        // 20% below the 100 peak, limit 15% → trips.
        let (hw, tripped) = drawdown_decision(80.0, 100.0, 0.15);
        assert_eq!(hw, 100.0);
        assert!(tripped);
    }

    #[test]
    fn drawdown_first_reading_sets_peak_no_trip() {
        let (hw, tripped) = drawdown_decision(50.0, 0.0, 0.15);
        assert_eq!(hw, 50.0);
        assert!(
            !tripped,
            "the first reading is the peak — can't be a drawdown"
        );
    }

    #[test]
    fn trade_cap_blocks_only_oversized_trades() {
        // 50% cap of a $100 venue = $50.
        assert!(trade_exceeds_cap(60.0, 100.0, 0.5), "over cap");
        assert!(!trade_exceeds_cap(40.0, 100.0, 0.5), "under cap");
        assert!(
            !trade_exceeds_cap(50.0, 100.0, 0.5),
            "exactly at cap is allowed"
        );
        assert!(!trade_exceeds_cap(60.0, 0.0, 0.5), "no value → no cap");
    }

    #[test]
    fn reconcile_flags_only_diverging_assets() {
        // (expected_total, traded_delta). BTC bought 0.5 → 1.0, settled exactly.
        // ETH bought 2.0 → 10.0, but only 1.6 settled (actual 9.6) = 20% of the
        // traded delta short.
        let expected: HashMap<String, (f64, f64)> =
            [("BTC".into(), (1.0, 0.5)), ("ETH".into(), (10.0, 2.0))]
                .into_iter()
                .collect();
        let actual: HashMap<String, f64> = [("BTC".into(), 1.0), ("ETH".into(), 9.6)]
            .into_iter()
            .collect();
        let flagged = reconcile_check(&expected, &actual, 0.03);
        assert_eq!(flagged.len(), 1, "only the diverging asset is flagged");
        assert_eq!(flagged[0].0, "ETH");
        assert!((flagged[0].3 - 0.2).abs() < 1e-9, "20% of the traded delta");
    }

    #[test]
    fn reconcile_within_tolerance_is_silent() {
        // Bought 1.0 (from 0 → total 1.0); 1% fee/rounding off — within tolerance.
        let expected: HashMap<String, (f64, f64)> =
            [("BTC".into(), (1.0, 1.0))].into_iter().collect();
        let actual: HashMap<String, f64> = [("BTC".into(), 0.99)].into_iter().collect();
        assert!(reconcile_check(&expected, &actual, 0.03).is_empty());
    }

    #[test]
    fn reconcile_missing_balance_flags_full_divergence() {
        // Bought 5.0 (from 0 → total 5.0) but the balance shows nothing → 100%.
        let expected: HashMap<String, (f64, f64)> =
            [("SOL".into(), (5.0, 5.0))].into_iter().collect();
        let actual: HashMap<String, f64> = HashMap::new();
        let flagged = reconcile_check(&expected, &actual, 0.03);
        assert_eq!(flagged.len(), 1);
        assert!((flagged[0].3 - 1.0).abs() < 1e-9);
    }

    #[test]
    fn reconcile_small_trade_on_large_holding_is_caught() {
        // The regression: hold 10 BTC, BUY 0.1 (→ expected total 10.1), but the
        // fill never settled (actual still 10.0). Measured against the 0.1 DELTA
        // this is 100% divergence and IS flagged — the old total-based denominator
        // diluted it to 0.99% and stayed silent.
        let expected: HashMap<String, (f64, f64)> =
            [("BTC".into(), (10.1, 0.1))].into_iter().collect();
        let actual: HashMap<String, f64> = [("BTC".into(), 10.0)].into_iter().collect();
        let flagged = reconcile_check(&expected, &actual, 0.03);
        assert_eq!(
            flagged.len(),
            1,
            "unsettled small trade on a big position must flag"
        );
        assert!(
            (flagged[0].3 - 1.0).abs() < 1e-9,
            "100% of the traded delta unsettled"
        );
    }

    #[test]
    fn slippage_flags_only_beyond_threshold() {
        // planned 100, filled 103 = 3% slip, threshold 2% → flagged.
        assert!(slippage_exceeds(100.0, 103.0, 0.02).is_some());
        // filled 101 = 1% slip → within threshold.
        assert!(slippage_exceeds(100.0, 101.0, 0.02).is_none());
        // symmetric (a better-than-planned fill still "deviates").
        assert!(slippage_exceeds(100.0, 97.0, 0.02).is_some());
        // disabled (threshold 0) or bad planned price → never flags.
        assert!(slippage_exceeds(100.0, 200.0, 0.0).is_none());
        assert!(slippage_exceeds(0.0, 200.0, 0.02).is_none());
    }

    // ── The unresolved hold, exercised through the real `run_cycle` ─────────
    //
    // The previous version of this test asserted a locally computed boolean,
    // which the external reviewer correctly pointed out proves nothing about
    // the production transitions. These drive `run_cycle` against a fake
    // exchange and count actual submissions.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Counts submissions and can be told to fail them (producing `Unknown`).
    struct FakeExchange {
        submissions: Arc<AtomicUsize>,
        fail_submissions: bool,
        cash: f64,
    }

    #[async_trait::async_trait]
    impl SpotExchange for FakeExchange {
        fn name(&self) -> &str {
            "fake"
        }
        fn has_keys(&self) -> bool {
            true
        }
        fn cash_asset(&self) -> &str {
            "USD"
        }
        async fn balances(&self) -> Result<Vec<crate::spot::exchange::Balance>> {
            Ok(vec![
                crate::spot::exchange::Balance {
                    asset: "USD".into(),
                    free: self.cash,
                },
                crate::spot::exchange::Balance {
                    asset: "BTC".into(),
                    free: 0.0,
                },
                crate::spot::exchange::Balance {
                    asset: "ETH".into(),
                    free: 0.0,
                },
            ])
        }
        async fn price(&self, _asset: &str) -> Result<f64> {
            Ok(100.0)
        }
        async fn market_buy(&self, _asset: &str, _quote_usd: f64) -> Result<Fill> {
            self.submissions.fetch_add(1, AtomicOrdering::SeqCst);
            if self.fail_submissions {
                anyhow::bail!("simulated transport timeout after broker acceptance");
            }
            Ok(Fill {
                asset: "BTC".into(),
                side: Side::Buy,
                base_qty: 1.0,
                avg_price: 100.0,
                quote_usd: 100.0,
                resolved: true,
            })
        }
        async fn market_sell(&self, _asset: &str, _base_qty: f64) -> Result<Fill> {
            self.submissions.fetch_add(1, AtomicOrdering::SeqCst);
            anyhow::bail!("simulated transport timeout after broker acceptance")
        }
    }

    fn held_venue(submissions: Arc<AtomicUsize>, cash: f64, fail: bool) -> Venue {
        Venue {
            ex: Box::new(FakeExchange {
                submissions,
                fail_submissions: fail,
                cash,
            }),
            targets: vec![
                Target {
                    name: "BTC".into(),
                    weight: 0.5,
                },
                Target {
                    name: "ETH".into(),
                    weight: 0.5,
                },
            ],
            reserve_pct: 0.0,
            band: 0.01,
            cooldown_secs: 300,
            min_trade_usd: 1.0,
            deposit_trigger_usd: 10.0,
            last_rebalance: None,
            last_cash: None,
            traded_last: false,
            stuck_cycles: 0,
            paper: None,
            live: true,
            pending_reconcile: None,
            unresolved: None,
            last_value: None,
        }
    }

    async fn cycle(v: &mut Venue) {
        let _ = run_cycle(
            v,
            true,
            None,
            0.0,
            None,
            None,
            &Alerter::new(None),
            &EventClient::new(None, None),
            "test",
            &Journal::new(None),
        )
        .await;
    }

    /// GAP 1 — the rest of the batch must not be submitted.
    #[tokio::test]
    async fn an_unknown_outcome_abandons_the_rest_of_the_batch() {
        let n = Arc::new(AtomicUsize::new(0));
        let mut v = held_venue(n.clone(), 10_000.0, true);
        cycle(&mut v).await;
        assert_eq!(
            n.load(AtomicOrdering::SeqCst),
            1,
            "two targets are adrift, so the plan holds two trades; only the FIRST \
             may be submitted once its outcome is unknown"
        );
        assert!(v.unresolved.is_some(), "the venue must be held");
    }

    /// GAP 2 — a deposit explicitly bypasses the cooldown; it must not bypass the hold.
    #[tokio::test]
    async fn a_deposit_does_not_release_the_hold() {
        let n = Arc::new(AtomicUsize::new(0));
        let mut v = held_venue(n.clone(), 10_000.0, true);
        cycle(&mut v).await;
        let after_first = n.load(AtomicOrdering::SeqCst);
        assert!(v.unresolved.is_some());

        // Simulate cash arriving: the deposit path fires on a jump since the
        // previous non-trading cycle.
        v.traded_last = false;
        v.last_cash = Some(10.0);
        v.ex = Box::new(FakeExchange {
            submissions: n.clone(),
            fail_submissions: true,
            cash: 500_000.0,
        });
        cycle(&mut v).await;
        assert_eq!(
            n.load(AtomicOrdering::SeqCst),
            after_first,
            "a deposit must not place an order while the venue is held"
        );
    }

    /// GAP 3 — the cooldown expires on its own; the hold must not.
    #[tokio::test]
    async fn cooldown_expiry_does_not_release_the_hold() {
        let n = Arc::new(AtomicUsize::new(0));
        let mut v = held_venue(n.clone(), 10_000.0, true);
        cycle(&mut v).await;
        let after_first = n.load(AtomicOrdering::SeqCst);
        assert!(v.unresolved.is_some());

        // Push the cooldown far into the past — as if hours had elapsed.
        v.last_rebalance = Some(now_secs().saturating_sub(86_400));
        cycle(&mut v).await;
        assert_eq!(
            n.load(AtomicOrdering::SeqCst),
            after_first,
            "an expired cooldown must not place an order while the venue is held; \
             a timer running out establishes nothing about the earlier order"
        );
    }
}
