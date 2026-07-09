# rustrade-exchange-apiws

[`rustrade`](https://crates.io/crates/rustrade-framework) `ExchangeClient`
adapters backed by [`exchange-apiws`](https://crates.io/crates/exchange-apiws)'s
signed REST surfaces:

| adapter | venue | shape |
|---|---|---|
| `KucoinExchangeAdapter` (+ `KucoinFillSource`) | **KuCoin Futures** | contracts, leverage, SL/TP brackets, real fills → `CryptoPerp` |
| `KrakenSpotAdapter` (+ `KrakenFillSource`) | **Kraken spot** | long-only, base-asset units, `position` = balance, real fills → `CryptoSpot` |
| `RoutingExchange` (+ `CompositeFillSource`) | **both at once** | per-symbol dispatch into the venues above, so one bot's `class_risk` diverges across classes |

The framework speaks `Order` / `Position` / `Capability`; `exchange-apiws`
speaks each venue's signed HTTP API. These adapters are the bridge — the
thing in the FKS bots that turns a framework `Order` into a **real** order on a
real exchange. Point them at sandbox/test credentials and the exact same code
path paper-trades.

## Kraken spot

Spot is a different shape and the adapter models it honestly: long-only (no
shorts, no leverage), orders in **base-asset units** (not contracts), and a
"position" is your **balance** of the base asset (closing it is a market sell).
Kraken keys balances by its own asset codes (`XXBT`, `XETH`, …), so
`get_position` needs a `symbol → base-asset-code` map at construction:

```rust
use rustrade_exchange_apiws::KrakenSpotAdapter;
let exchange = KrakenSpotAdapter::from_env(&[("XBTUSD", "XXBT"), ("ETHUSD", "XETH")])?;
```

It advertises `AssetClass::CryptoSpot`, so `class_risk(AssetClass::CryptoSpot, RiskConfig::crypto_spot())`
applies the spot rules (1× leverage) automatically. *(Bybit is unused — not
tradeable from Canada.)*

It is **Track 1** of
[`docs/MULTI_ASSET_BRAIN_ROADMAP.md`](../../docs/MULTI_ASSET_BRAIN_ROADMAP.md):
until now every bot under `bots/` traded against `MockExchange`, so nothing
actually executed through the framework.

## Multi-venue — `RoutingExchange`

A `rustrade::Bot` holds **one** `ExchangeClient`, but per-asset-class risk
(`class_risk`) only earns its keep when one bot trades **more than one** class at
once. `RoutingExchange` is a single `ExchangeClient` that dispatches each call to
a **per-symbol** venue, so KuCoin perps and Kraken spot run side by side in one
bot — and because each symbol's `instrument_spec` (and thus `AssetClass`) comes
from its own venue, the framework's `resolve_risk` applies the right preset to
each automatically (`CryptoPerp` → 5×, `CryptoSpot` → 1×):

```rust
use std::sync::Arc;
use rustrade_exchange_apiws::{KrakenSpotAdapter, KucoinExchangeAdapter, RoutingExchange};

let kucoin = Arc::new(KucoinExchangeAdapter::from_env(5, &["XBTUSDTM"]).await?);
let kraken = Arc::new(KrakenSpotAdapter::from_env(&[("XBTUSD", "XXBT")])?);
let exchange = Arc::new(
    RoutingExchange::builder()
        .route(["XBTUSDTM"], kucoin) // → CryptoPerp risk
        .route(["XBTUSD"], kraken)   // → CryptoSpot risk
        .build()?,
);
```

The two symbol-less calls answer conservatively: `supports` is the **intersection**
across venues (a capability only if *every* venue has it), and `get_balance` is the
**sum** for the given currency. `CompositeFillSource` merges the venues' fill
sources into one stream, and `RoutingCandleSource` does the same on the market-data
side — KuCoin klines for the perp symbols, `KrakenCandleSource` (Kraken public OHLC)
for the spot symbols. `crypto-demo` wires all this behind `DEMO_EXCHANGE=multi`.

## Mapping

| framework call | KuCoin (via exchange-apiws) |
|---|---|
| `place_order(Order)` plain | `place_order` (market / limit / IOC / FOK) |
| `place_order(Order)` with `stop` + `reduce_only` | `place_stop_order` — a bracket leg |
| `place_order(Order)` with `stop`, not reduce-only | `place_order` (entry) **+** a reduce-only `place_stop_order` (protection) |
| `close_position` | `close_position` (market, signed qty) |
| `get_position` / `get_balance` | `get_position` / `get_balance` |
| `cancel_all` | `cancel_all_orders` + `cancel_all_stop_orders` |
| `get_open_orders` / `cancel_order` | `get_open_orders` / `cancel_order` |
| `contract_value` | cached `get_contract().multiplier` |

Stop-trigger direction (`"up"` / `"down"`) is derived purely from the closing
side and stop kind — a stop-loss sits the correct side of the market and a
take-profit the other — so no mark-price lookup is needed to place a bracket.

## Capabilities (advertised truthfully)

| capability | supported | why |
|---|---|---|
| `StopOrders` | ✅ | `place_stop_order` |
| `ReduceOnly` | ✅ | `reduce_only` order field |
| `Ioc` / `Fok` | ✅ | `TimeInForce::IOC` / `FOK` |
| `OrderTracking` | ✅ | `get_open_orders` + `cancel_order` |
| `PostOnly` | ❌ | the `place_order` surface exposes no post-only flag |
| `PublicFeed` / `PrivateFeed` | ❌ | trading-only; a bot wires its own feeds |

## Usage

```rust
use rustrade_exchange_apiws::KucoinExchangeAdapter;
use std::sync::Arc;

// KC_KEY / KC_SECRET / KC_PASSPHRASE from the environment, 5× leverage,
// pre-fetching contract multipliers for the symbols we'll trade.
let exchange = Arc::new(
    KucoinExchangeAdapter::from_env(5, &["XBTUSDTM", "ETHUSDTM"]).await?
);

// `exchange` is a `dyn ExchangeClient` — hand it to `Bot::new(config, exchange, brains)`.
```

For tests or hard-coded multipliers, build it without touching the network:

```rust
use rustrade_exchange_apiws::KucoinExchangeAdapter;
use exchange_apiws::{Credentials, KuCoinClient, KucoinEnv};

let client = KuCoinClient::new(Credentials::from_env()?, KucoinEnv::LiveFutures)?;
let exchange = KucoinExchangeAdapter::new(client, 5)
    .with_contract_value("XBTUSDTM", 0.001);
```

## Safety

Placing orders through this adapter is **live trading** when pointed at live
credentials. The FKS stack defaults to paper everywhere for a reason — see the
"no autonomous execution" principle in the root `CLAUDE.md`. The `crypto-demo`
bot keeps `MockExchange` as its default and only constructs this adapter behind
an explicit `DEMO_EXCHANGE=kucoin` opt-in.

## Real fills

Each venue ships a `rustrade::FillSource` that streams the exchange's actual
executions into the bot, replacing paper-simulated fills. Because the framework
gates bracket/OCO handling on a fill source being present, wiring one also turns
on real SL/TP management.

### KuCoin — `KucoinFillSource`

```rust
use rustrade_exchange_apiws::KucoinFillSource;
use exchange_apiws::KucoinEnv;
use std::sync::Arc;

let fills = Arc::new(KucoinFillSource::connect(
    adapter.client().clone(),
    KucoinEnv::LiveFutures,
    vec!["XBTUSDTM".into(), "ETHUSDTM".into()],
    std::time::Duration::from_secs(5),
));
// bot.with_fill_source(fills)
```

It uses the private `tradeOrders` WS as a **low-latency trigger** and reads the
authoritative price/size/fee from `/recentFills` (exchange-apiws's `OrderUpdate`
omits the per-execution match price — and reports `0.0` for market orders), so
fills carry true prices. Deduped by trade id; baselined at startup so history
isn't replayed; degrades to poll-only if the private WS token is unavailable.

### Kraken — `KrakenFillSource`

Kraken has no private own-trades WS through `exchange-apiws`, so this source is
poll-based: it reads `/0/private/TradesHistory` on a cadence (5 s default) and
emits trades the framework hasn't seen. Trades are in **base-asset units** with
the fee in the quote currency (which `TradesHistory` doesn't name per row, so
you pass it — e.g. `"USD"`). Deduped by trade id (bounded FIFO) and baselined at
startup so pre-existing history isn't replayed.

```rust
use rustrade_exchange_apiws::KrakenFillSource;
use std::sync::Arc;

let fills = Arc::new(KrakenFillSource::connect_default(adapter.client().clone(), "USD"));
// bot.with_fill_source(fills)
```

## Status & roadmap

- ✅ KuCoin Futures `ExchangeClient` (orders, brackets, positions, balance, order tracking).
- ✅ `KucoinFillSource` — real fills via the private `tradeOrders` WS trigger + `/recentFills`.
- ✅ Kraken **spot** `ExchangeClient` over `exchange-apiws`'s `KrakenPrivateClient`
  (long-only, `position` = base-asset balance, market/limit, `AssetClass::CryptoSpot`).
- ✅ `KrakenFillSource` — real fills by polling `/private/TradesHistory` (Kraken has
  no private own-trades WS here), deduped by trade id + baselined at startup.
- ✅ `RoutingExchange` + `CompositeFillSource` — compose KuCoin + Kraken into one
  symbol-routed `ExchangeClient` so a single bot's `class_risk` diverges across
  `CryptoPerp` (5×) and `CryptoSpot` (1×); wired in `crypto-demo` as `DEMO_EXCHANGE=multi`.
- ✅ `KrakenCandleSource` + `RoutingCandleSource` — market-data side: Kraken public
  OHLC candles, and a per-symbol candle router so each venue's symbols get their own
  candles (the `multi` mode pulls KuCoin klines for perps, Kraken OHLC for spot).
- ⏳ Expose per-execution `matchPrice`/`matchSize` on exchange-apiws's `OrderUpdate`
  so the WS feed can carry fill prices directly (drop the `/recentFills` hydration).
  *(Landed in exchange-apiws; the `KucoinFillSource` simplification follows a publish.)*

KuCoin (futures) + Kraken (spot) are the target venues; Bybit is unused (N/A in Canada).

## License

MIT OR Apache-2.0.
