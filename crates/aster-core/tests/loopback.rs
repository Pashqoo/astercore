//! The contract test: our server, and the terminal's own client of the same
//! `moonproto` rev, on loopback — the Init spine (M0) and the market data (M1).
//!
//! This is the observation `PLAN.md` asks M0 to end with, minus the GUI: the
//! terminal is `moonproto::MoonClient` plus a window, and `MoonClient` walks
//! exactly the Init spine the terminal walks and fails it the same way. So
//! "`connect_blocking` returned" IS "the terminal reached `Ready`", and
//! everything asserted after it is read out of the client's own state — what
//! the terminal would be showing, not what our encoder thinks it sent.
//!
//! Hermetic on purpose: the catalog is a fixture, not a live `exchangeInfo`.
//! The live exchange is covered by running the core (`AGENTS.md`, "Канал
//! наблюдения"), and a contract test that needed the network would stop being
//! run.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aster_core::account::{Account, Position};
use aster_core::aster::json::{BookTicker, ExchangeInfo, Kline, PremiumIndex};
use aster_core::book::{Diff, Snapshot};
use aster_core::engine::{
    CoreHandler, FeedLink, ACCOUNT_PLACEHOLDER, EXCHANGE_CODE, EXCHANGE_NAME, SERVER_NAME,
};
use aster_core::feed::{FeedCommand, FeedEvent};
use aster_core::load::Load;
use aster_core::model::Catalog;
use aster_core::order_store::OrderStore;
use aster_core::orders::Action;
use aster_core::strategies::Strategies;
use aster_core::stream_health::StreamHealth;
use aster_core::trading::{ExecStatus, OrderUpdate, TradeCommand, TradingEvent};
use moonproto::server::codec::market_data::{delphi_days, Candle};
use moonproto::server::key_export::ServerKey;
use moonproto::server::Server;
use moonproto::state::{AccountEvent, BalanceEvent, MarketBalancePosition};
use moonproto::{
    BaseCurrency, ClientConfig, ConnectConfig, Event, ExchangeTypeMask, InitConfig,
    InitialStrategies, MoonClient, TransportMode,
};

/// Two markets, both real shapes measured on 01.10: a vanilla perpetual that
/// trades, and a dated one that is settling — which is the market the price
/// rows must carry as zeros, because the exchange quotes no book for it.
const EXCHANGE_INFO: &str = r#"{
  "serverTime": 1790864169500,
  "timezone": "UTC",
  "rateLimits": [],
  "symbols": [
    {
      "symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT",
      "marginAsset": "USDT", "status": "TRADING", "contractType": "PERPETUAL",
      "pricePrecision": 1, "quantityPrecision": 3, "channel": "{}",
      "deliveryDate": 4133404800000,
      "maintMarginPercent": "2.5", "requiredMarginPercent": "5.0",
      "liquidationFee": "0.025", "marketTakeBound": "0.02",
      "triggerProtect": "0.02",
      "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.1", "minPrice": "1", "maxPrice": "1000000"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "120"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.02", "multiplierDown": "0.98"},
        {"filterType": "MAX_NUM_ORDERS", "limit": 200},
        {"filterType": "MAX_NUM_ALGO_ORDERS", "limit": 10}
      ]
    },
    {
      "symbol": "TONUSDT", "baseAsset": "TON", "quoteAsset": "USDT",
      "marginAsset": "USDT", "status": "SETTLING", "contractType": "PERPETUAL",
      "pricePrecision": 4, "quantityPrecision": 1, "channel": "{}",
      "deliveryDate": 1781740800000,
      "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.0001", "minPrice": "0.01", "maxPrice": "1000"},
        {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "10000"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.1", "multiplierDown": "0.9"}
      ]
    }
  ]
}"#;

const BTC_BID: f64 = 83_772.0;
const BTC_ASK: f64 = 83_772.1;
const BTC_MARK: f64 = 83_775.5;
/// A fraction on the wire from Aster, a percent on the wire to the terminal.
const BTC_FUNDING_RATE: f64 = 0.000_089_79;
const BTC_FUNDING_MS: i64 = 1_790_870_400_000;

fn catalog() -> Catalog {
    let info: ExchangeInfo = serde_json::from_str(EXCHANGE_INFO).expect("fixture parses");
    let mut cat = Catalog::build(&info);
    assert_eq!(cat.markets().len(), 2, "both markets are carried");
    cat.apply_premium_index(&[PremiumIndex {
        symbol: "BTCUSDT".into(),
        last_funding_rate: BTC_FUNDING_RATE,
        next_funding_time_ms: BTC_FUNDING_MS,
        mark_price: BTC_MARK,
    }]);
    // Only the trading market is quoted, as the live answer does it.
    assert_eq!(
        cat.apply_book(&[BookTicker {
            symbol: "BTCUSDT".into(),
            bid_price: BTC_BID,
            ask_price: BTC_ASK,
        }]),
        1
    );
    cat
}

/// Our server on an ephemeral port, in its own thread, stopped on drop.
struct Core {
    key: ServerKey,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Core {
    fn start() -> Self {
        let mut key = ServerKey::generate(None, 0, TransportMode::V2);
        let handler = CoreHandler::new(
            1,
            ACCOUNT_PLACEHOLDER.into(),
            catalog(),
            Strategies::new(None, 0),
        );
        let mut server = Server::bind(&key, handler).expect("bind");
        key.port = server.local_addr().expect("local_addr").port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::spawn(move || server.run(&flag));
        Self {
            key,
            stop,
            thread: Some(thread),
        }
    }

    fn connect(&self) -> MoonClient {
        connect(&self.key)
    }
}

/// The terminal's own client, walked to `Ready` against the core behind `key`.
fn connect(key: &ServerKey) -> MoonClient {
    let cfg = ClientConfig::new("127.0.0.1", key.port, key.master_key, key.mac_key)
        .with_transport_mode(key.transport_mode);
    let init = InitConfig {
        // The terminal arrives with its own strategy list; an empty one is
        // the first-run case and the one M0 has to survive.
        initial_strategies: Some(InitialStrategies::new(0, Vec::new())),
        ..Default::default()
    };
    MoonClient::connect_blocking(
        cfg,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(20)),
        Duration::from_secs(30),
    )
    .expect("the client must reach Ready against this core")
}

impl Drop for Core {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn wait_until(timeout: Duration, mut tick: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if tick() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

/// The whole Init spine (M0) in one test: the client walks BaseCheck, AuthCheck,
/// GetMarketsList, UpdateMarketsList and the strategy schema, and what it ends
/// up holding is what the terminal would draw.
#[test]
fn the_client_reaches_ready_and_holds_the_catalog_we_sent() {
    let core = Core::start();
    let client = core.connect();

    // --- BaseCheck: who the core says it is ---------------------------------
    let info = client.server_info().expect("server info after BaseCheck");
    assert_eq!(info.server_name.as_deref(), Some(SERVER_NAME));
    assert_eq!(info.exchange_name.as_deref(), Some(EXCHANGE_NAME));
    assert_eq!(
        info.exchange_code.map(|c| c.stable_id()),
        Some(EXCHANGE_CODE),
        "out of MoonBot's own ExchangeCode range, so the terminal names the \
         venue by EXCHANGE_NAME"
    );
    assert!(
        info.supports(ExchangeTypeMask::FUTURES),
        "a perpetual venue reported as spot shows positions as a coin wallet"
    );
    assert!(!info.supports(ExchangeTypeMask::SPOT));
    assert_eq!(info.base_currency_name.as_deref(), Some("USDT"));

    // --- AuthCheck ----------------------------------------------------------
    let auth = client.auth_info().expect("auth info after AuthCheck");
    assert_eq!(auth.account_id, ACCOUNT_PLACEHOLDER);
    assert_eq!(auth.recvd_max_payload, Some(4 * 1024 * 1024));

    // --- GetMarketsList / GetMarketsIndexes ---------------------------------
    let snap = client.snapshot().expect("snapshot");
    let markets = snap.markets();
    assert_eq!(markets.market_count(), 2);
    assert!(
        markets.indexes_synchronized(),
        "the index map must agree with the list, or every indexed packet lands \
         on the wrong market"
    );
    let btc = markets.market_snapshot("BTCUSDT").expect("BTCUSDT");
    // The settlement currency, and the field that read as SPOT for as long as
    // it was inherited from a MOEX core: the terminal's `listed_type()` is
    // derived from it and an EMPTY one means spot.
    assert_eq!(btc.futures_type, BaseCurrency::USDT, "NOT spot");
    assert!(btc.is_btc_market, "Delta_BTC_* has a market to read again");
    assert_eq!(btc.market_currency_canonic, "BTC");
    assert_eq!(btc.k1000, 1);
    assert_eq!((btc.tick_size(), btc.step_size()), (0.1, 0.001));
    assert_eq!(btc.min_notional(), 5.0);
    assert_eq!((btc.min_price(), btc.max_price()), (1.0, 1_000_000.0));
    assert_eq!(
        (btc.multiplier_up(), btc.multiplier_down()),
        (1.02, 0.98),
        "the price band is per symbol and the terminal has to know it"
    );
    assert_eq!(
        btc.max_leverage, 20,
        "100 / requiredMarginPercent, the instrument ceiling"
    );
    assert_eq!(btc.delivery_time_ms(), 0, "a perpetual has no settlement");
    // Fraction in, percent out: the one unit conversion this core performs.
    assert!(
        (btc.funding_rate - BTC_FUNDING_RATE * 100.0).abs() < 1e-9,
        "funding arrived as {} %",
        btc.funding_rate
    );
    // The instant is written in UTC and the upstream reader moves it onto this
    // machine's wall clock while parsing (`apply_delphi_local_funding_shift`) —
    // which is exactly what the terminal undoes before counting down to the
    // charge. The zone shift is not nameable from here (the helper is private
    // to the vendored crate), so what is asserted is what a wrong write would
    // break: a real instant within a day of the one sent, and NOT the 1970
    // sentinel that an unguarded Delphi conversion of "no funding" produces.
    let charged_at = btc.funding_time().unix_millis();
    assert!(
        (charged_at - BTC_FUNDING_MS).abs() <= 14 * 3_600_000,
        "funding instant came out as {charged_at}, sent {BTC_FUNDING_MS}"
    );

    let ton = markets.market_snapshot("TONUSDT").expect("TONUSDT");
    // The exchange published no funding row for this market, and that must
    // arrive as nothing at all rather than as a charge due in 1970: the
    // terminal's own absence test is the TIME, not the rate.
    assert_eq!(ton.funding_rate, 0.0);
    // The wire carries 0.0 for "no funding" and the terminal's own absence test
    // is that zero (`funding_time > 0`). Read through this accessor a zero
    // becomes Delphi day zero — 1899-12-30 — while the defect it guards against
    // (writing `delphi_days(0)` for an absent charge) would read as
    // 1970-01-01, i.e. a unix time of about zero. So the sign is the test, and
    // the two cases are 2.2 trillion milliseconds apart.
    let absent = ton.funding_time().unix_millis();
    assert!(
        absent < -2_000_000_000_000,
        "an absent funding time must stay absent, not become 1970: {absent}"
    );
    assert!(
        !ton.status_trading,
        "a settling market is carried, not traded"
    );
    assert_eq!(
        ton.delivery_time_ms(),
        1_781_740_800_000,
        "the date PanicSellDelisted is keyed on"
    );

    // --- UpdateMarketsList --------------------------------------------------
    let price = markets.price("BTCUSDT").expect("BTCUSDT price");
    assert_eq!((price.bid, price.ask), (BTC_BID, BTC_ASK));
    assert_eq!(price.mark_price, BTC_MARK);
    let ton_price = markets.price("TONUSDT").expect("TONUSDT price");
    assert_eq!((ton_price.bid, ton_price.ask), (0.0, 0.0));
    // The zeros alone prove nothing — an untouched market's price is zero
    // everywhere by default. What says the ROW ARRIVED is `min_lot_size`: the
    // client recomputes it as `max(step × mid, min_notional)` on every row it
    // applies, so an applied zero row leaves this market's `MIN_NOTIONAL` here
    // and a market the core never mentioned would still read 0. That is the
    // difference between "the terminal was told there is no quote" and "the
    // terminal was told nothing and is free to keep an old one".
    assert_eq!(
        ton_price.min_lot_size, 5.0,
        "an unquoted market must still get a row, so the terminal cannot keep \
         a stale quote: {ton_price:?}"
    );

    // --- QueryHedgeMode: one-way until M5 -----------------------------------
    // Asked for, not assumed: hedge mode is not an Init step, so the client
    // holds nothing until something requests it — which is also the path the
    // terminal's own toolbar takes.
    assert_eq!(
        snap.account().hedge_mode(),
        None,
        "hedge mode is not part of the Init spine"
    );
    client
        .account()
        .refresh_hedge_mode()
        .expect("the request is sent");
    assert!(
        wait_until(Duration::from_secs(5), || {
            client
                .drain_events()
                .into_iter()
                .any(|e| matches!(e, Event::Account(AccountEvent::HedgeModeUpdated { .. })))
        }),
        "no answer to QueryHedgeMode"
    );
    assert_eq!(
        client.snapshot().expect("snapshot").account().hedge_mode(),
        Some(false),
        "M2's order model is written against one-way positions"
    );

    let _ = client.disconnect();
}

/// The unhappy branch: a method this core does not implement must be REFUSED,
/// not ignored. Silence costs the client a 12 s timeout and then its own
/// error; a refusal lands at once and names itself.
#[test]
fn an_unimplemented_engine_method_is_refused_rather_than_ignored() {
    let core = Core::start();
    let client = core.connect();

    let sent = Instant::now();
    client
        .account()
        .set_hedge_mode(true)
        .expect("the request itself is sent");
    let mut refusal = None;
    let answered = wait_until(Duration::from_secs(5), || {
        for event in client.drain_events() {
            if let Event::EngineAction(action) = event {
                refusal = Some(action);
                return true;
            }
        }
        false
    });
    assert!(answered, "no answer at all to SetHedgeMode");
    let action = refusal.expect("engine action event");
    assert!(!action.success, "this core cannot set hedge mode yet");
    assert_eq!(action.error_msg, "not implemented");
    assert!(
        sent.elapsed() < Duration::from_secs(5),
        "a refusal must not take a step timeout: {:?}",
        sent.elapsed()
    );

    let _ = client.disconnect();
}

// ----- M1: market data --------------------------------------------------------

/// The core with a market feed the test plays: the commands the handler sends
/// come out of `cmd_rx`, and whatever the test puts into `ev_tx` is what the
/// exchange said. The loop is the core's own — `step`, then `pump` — so what
/// the client sees is what a terminal would.
struct FedCore {
    key: ServerKey,
    cmd_rx: Receiver<FeedCommand>,
    ev_tx: Sender<FeedEvent>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FedCore {
    fn start() -> Self {
        Self::start_with(|h| h)
    }

    /// The core with an account: the order worker is the returned receiver,
    /// and the order store a fresh file of its own.
    fn trading() -> (Self, Receiver<TradeCommand>) {
        let (tx, rx) = mpsc::channel();
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "astercore-loopback-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let (store, saved) = OrderStore::open(dir.join("orders.json"));
        let core = Self::start_with(move |h| h.with_orders(store, saved).with_trading(tx));
        (core, rx)
    }

    fn start_with(setup: impl FnOnce(CoreHandler) -> CoreHandler) -> Self {
        let mut key = ServerKey::generate(None, 0, TransportMode::V2);
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = mpsc::channel();
        let handler = setup(
            CoreHandler::new(
                1,
                ACCOUNT_PLACEHOLDER.into(),
                catalog(),
                Strategies::new(None, 0),
            )
            .with_feed(FeedLink {
                tx: cmd_tx,
                health: StreamHealth::default(),
                load: Arc::new(Load::default()),
            }),
        );
        let mut server = Server::bind(&key, handler).expect("bind");
        key.port = server.local_addr().expect("local_addr").port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            // Built here: its receiver stays with the loop that drains it.
            let control = aster_core::control::Control::new();
            let mut stopping = false;
            while !flag.load(Ordering::Relaxed) {
                server.step();
                let (h, sessions) = server.split();
                h.pump(sessions, &ev_rx);
                h.run_control(&control);
                // What `main` does once a halt is asked for (the terminal's
                // shutdown among them): the stop sweeps the entries (and
                // keeps serving here, so the test can watch it).
                if control.halted().is_some() && !stopping {
                    stopping = true;
                    h.begin_stop(now_ms());
                }
            }
        });
        Self {
            key,
            cmd_rx,
            ev_tx,
            stop,
            thread: Some(thread),
        }
    }

    fn connect(&self) -> MoonClient {
        connect(&self.key)
    }

    /// The next feed command matching `pick`, skipping the rest.
    fn expect_cmd<T>(&self, what: &str, mut pick: impl FnMut(FeedCommand) -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let cmd = self
                .cmd_rx
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("no feed command: {what}"));
            if let Some(v) = pick(cmd) {
                return v;
            }
        }
    }
}

impl Drop for FedCore {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn now_ms() -> i64 {
    aster_core::engine::now_ms()
}

fn diff(u0: i64, u1: i64, pu: i64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> Diff {
    Diff {
        first_id: u0,
        last_id: u1,
        prev_id: pu,
        bids: bids.to_vec(),
        asks: asks.to_vec(),
    }
}

/// The client's applied BTCUSDT book is exactly these levels, best first.
fn book_is(
    snap: &moonproto::events::MoonStateSnapshot,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> bool {
    let Some(book) = snap.order_book("BTCUSDT", moonproto::state::OrderBookKind::Futures) else {
        return false;
    };
    let same = |side: &[moonproto::state::OrderBookLevel], want: &[(f64, f64)]| {
        side.len() == want.len()
            && side
                .iter()
                .zip(want)
                .all(|(l, (p, q))| (l.rate - p).abs() < 0.01 && (l.quantity - q).abs() < 1e-6)
    };
    same(&book.buys, bids) && same(&book.sells, asks)
}

/// M1 in one test: the terminal's subscriptions reach the feed as commands,
/// and what the feed says — a trade, a book stitched from its snapshot, a
/// CoinCard history, a new funding pair — reaches the client's own state.
#[test]
fn the_tape_the_book_the_chart_and_live_funding_reach_the_client() {
    let core = FedCore::start();
    let client = core.connect();
    client
        .streams()
        .subscribe_all_trades(moonproto::TradesStreamMode::TradesOnly)
        .unwrap();
    client.streams().subscribe_orderbook("BTCUSDT").unwrap();
    let card = client
        .candles()
        .request_coin_card("BTCUSDT", moonproto::DeepHistoryKind::Min5)
        .unwrap();

    core.expect_cmd("the book subscription", |c| match c {
        FeedCommand::SetBooks(s) if s == ["BTCUSDT"] => Some(()),
        _ => None,
    });
    let (client_id, request_uid) = core.expect_cmd("the CoinCard request", |c| match c {
        FeedCommand::Candles {
            symbol,
            minutes,
            client_id,
            request_uid,
        } => {
            assert_eq!((symbol.as_str(), minutes), ("BTCUSDT", 5));
            Some((client_id, request_uid))
        }
        _ => None,
    });
    // The book's first event asks for its snapshot; it is older than the
    // snapshot and is dropped by the stitch.
    core.ev_tx
        .send(FeedEvent::BookDiff {
            symbol: "BTCUSDT".into(),
            diff: diff(90, 95, 89, &[(83_700.0, 9.0)], &[]),
        })
        .unwrap();
    core.expect_cmd("the book snapshot", |c| match c {
        FeedCommand::BookSnapshot(s) if s == "BTCUSDT" => Some(()),
        _ => None,
    });
    core.ev_tx
        .send(FeedEvent::BookSnapshot {
            symbol: "BTCUSDT".into(),
            result: Ok(Snapshot {
                last_id: 100,
                bids: vec![(83_771.9, 0.5), (83_771.0, 2.0)],
                asks: vec![(83_772.1, 1.25), (83_773.0, 3.0)],
            }),
        })
        .unwrap();
    let bar_ms = now_ms() / 300_000 * 300_000 - 300_000;
    core.ev_tx
        .send(FeedEvent::CandlesReply {
            client_id,
            request_uid,
            result: Ok(vec![Candle {
                open: 83_700.0,
                high: 83_800.0,
                low: 83_650.0,
                close: 83_772.0,
                volume: 12.5,
                time: delphi_days(bar_ms),
            }]),
        })
        .unwrap();
    // Funding moved on: the next charge is eight hours later and the rate is
    // another one. The catalog row the client holds still says the old pair,
    // so only the price rows can carry this.
    let next_charge = BTC_FUNDING_MS + 8 * 3_600_000;
    core.ev_tx
        .send(FeedEvent::Marks(vec![PremiumIndex {
            symbol: "BTCUSDT".into(),
            last_funding_rate: 0.000_2,
            next_funding_time_ms: next_charge,
            mark_price: BTC_MARK,
        }]))
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Trades are applied only once the client's indexes are synchronized;
        // keep sending until one lands.
        core.ev_tx
            .send(FeedEvent::Trade {
                symbol: "BTCUSDT".into(),
                price: 83_772.0,
                qty: -0.004,
                time_ms: now_ms(),
            })
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        let Some(snap) = client.snapshot() else {
            continue;
        };
        let last = snap
            .markets()
            .get("BTCUSDT")
            .map_or(0.0, |m| m.trade_state().last_trade_price);
        let top = snap.top_of_book("BTCUSDT", moonproto::state::OrderBookKind::Futures);
        let book_ok = book_is(
            &snap,
            &[(83_771.9, 0.5), (83_771.0, 2.0)],
            &[(83_772.1, 1.25), (83_773.0, 3.0)],
        );
        let bars = snap
            .markets()
            .get("BTCUSDT")
            .and_then(|m| snap.coin_card_candles_for(&m, card.kind).map(<[_]>::len));
        let funding = snap.markets().price("BTCUSDT").map(|p| p.funding_rate);
        // Percent on the wire: 0.0002 is 0.02 %.
        let funded = funding.is_some_and(|r| (r - 0.02).abs() < 1e-12);
        if (last - 83_772.0).abs() < 0.01 && book_ok && bars == Some(1) && funded {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "not applied: last={last} top={top:?} bars={bars:?} funding={funding:?}"
        );
    }

    // The price runs up through the asks: the ask at 83772.1 is taken and bids
    // appear at and above it. The exchange's diff, sent as it is, would cut
    // the client's new bids at the removed ask; the client must end up with
    // exactly the book the core holds.
    core.ev_tx
        .send(FeedEvent::BookDiff {
            symbol: "BTCUSDT".into(),
            diff: diff(
                99,
                101,
                98,
                &[(83_772.1, 0.7), (83_772.5, 0.1)],
                &[(83_772.1, 0.0), (83_774.0, 1.0)],
            ),
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        thread::sleep(Duration::from_millis(20));
        let crossed = client.snapshot().is_some_and(|snap| {
            book_is(
                &snap,
                &[
                    (83_772.5, 0.1),
                    (83_772.1, 0.7),
                    (83_771.9, 0.5),
                    (83_771.0, 2.0),
                ],
                &[(83_773.0, 3.0), (83_774.0, 1.0)],
            )
        });
        if crossed {
            break;
        }
        let book = client.snapshot().and_then(|snap| {
            snap.order_book("BTCUSDT", moonproto::state::OrderBookKind::Futures)
                .map(|b| format!("{:?} / {:?}", b.buys, b.sells))
        });
        assert!(
            Instant::now() < deadline,
            "crossing diff not mirrored: {book:?}"
        );
    }

    // A second terminal opens the same book: the core already keeps it live,
    // so no stitch will send it whole — the subscription itself must, or this
    // client builds its book from changed levels alone.
    let late = core.connect();
    late.streams().subscribe_orderbook("BTCUSDT").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        thread::sleep(Duration::from_millis(20));
        let whole = late.snapshot().is_some_and(|snap| {
            book_is(
                &snap,
                &[
                    (83_772.5, 0.1),
                    (83_772.1, 0.7),
                    (83_771.9, 0.5),
                    (83_771.0, 2.0),
                ],
                &[(83_773.0, 3.0), (83_774.0, 1.0)],
            )
        });
        if whole {
            break;
        }
        let book = late.snapshot().and_then(|snap| {
            snap.order_book("BTCUSDT", moonproto::state::OrderBookKind::Futures)
                .map(|b| format!("{:?} / {:?}", b.buys, b.sells))
        });
        assert!(
            Instant::now() < deadline,
            "a late subscriber got no whole book: {book:?} snapshot={}",
            late.snapshot().is_some()
        );
    }
    // Released explicitly: a client that only disconnects keeps its books
    // subscribed until the server times its session out, a minute later.
    // The request is queued, not sent: give the client a moment to send it
    // before the disconnect tears the session down.
    late.streams().unsubscribe_orderbook("BTCUSDT").unwrap();
    thread::sleep(Duration::from_millis(300));
    let _ = late.disconnect();

    // Closing the book releases its session: one the feed would otherwise
    // keep open for nobody. (A client that just goes away is released too,
    // but only once the server times its session out — the client sends no
    // close on `disconnect` — so that path is a minute long and not tested
    // here.)
    client.streams().unsubscribe_orderbook("BTCUSDT").unwrap();
    core.expect_cmd("the book released", |c| match c {
        FeedCommand::SetBooks(s) if s.is_empty() => Some(()),
        _ => None,
    });
    let _ = client.disconnect();
}

/// The unhappy branch: the exchange refuses the REST call behind a chart. The
/// refusal must reach the client as a failure it can show — not as silence,
/// which the client would wait out, and not as an empty chart, which reads as
/// a market with no history.
#[test]
fn a_refused_history_call_reaches_the_client_as_a_failure() {
    let core = FedCore::start();
    let client = core.connect();
    client
        .streams()
        .subscribe_all_trades(moonproto::TradesStreamMode::TradesOnly)
        .unwrap();
    client
        .candles()
        .request_coin_card("BTCUSDT", moonproto::DeepHistoryKind::Hour1)
        .unwrap();
    let (client_id, request_uid) = core.expect_cmd("the CoinCard request", |c| match c {
        FeedCommand::Candles {
            client_id,
            request_uid,
            ..
        } => Some((client_id, request_uid)),
        _ => None,
    });
    let refusal = "api 429/-1003: Too many requests";
    core.ev_tx
        .send(FeedEvent::CandlesReply {
            client_id,
            request_uid,
            result: Err(refusal.into()),
        })
        .unwrap();
    let mut failed = None;
    assert!(
        wait_until(Duration::from_secs(5), || {
            for event in client.drain_events() {
                if let Event::CoinCardCandles(moonproto::CoinCardCandlesEvent::UpdateFailed {
                    error,
                    ..
                }) = event
                {
                    failed = Some(error);
                    return true;
                }
            }
            false
        }),
        "the client never heard the CoinCard refusal"
    );
    assert!(failed.unwrap().contains("-1003"));

    // The same for the tape of the last hour.
    let ticket = client.history().request_chart("BTCUSDT").unwrap();
    let (client_id, request_uid) = core.expect_cmd("the history request", |c| match c {
        FeedCommand::History {
            symbol,
            client_id,
            request_uid,
        } => {
            assert_eq!(symbol, "BTCUSDT");
            Some((client_id, request_uid))
        }
        _ => None,
    });
    core.ev_tx
        .send(FeedEvent::HistoryReply {
            client_id,
            request_uid,
            result: Err(refusal.into()),
        })
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || {
            client.drain_events().into_iter().any(|e| {
                matches!(
                    e,
                    Event::MarketHistory(moonproto::MarketHistoryEvent::Failed { ticket: t, .. })
                        if t == ticket
                )
            })
        }),
        "the client never heard the history refusal"
    );
    let _ = client.disconnect();
}

/// The screener's window columns: the terminal's one-shot `RequestCandlesData`
/// is held until the warm-up ends, and the answer is the sealed 5m bars in
/// quote turnover — the bar still in progress stays out, the client builds
/// that one from its own tape.
#[test]
fn the_held_candle_snapshot_fills_the_hourly_volume_after_the_warmup() {
    let core = FedCore::start();
    let client = core.connect();
    // Subscribing to the tape is what makes the client ask for the snapshot.
    client
        .streams()
        .subscribe_all_trades(moonproto::TradesStreamMode::TradesOnly)
        .unwrap();
    let one_hour = || {
        client
            .snapshot()
            .and_then(|s| s.market_history_derived_snapshot_now("BTCUSDT"))
            .map_or(0.0, |d| d.candle_volumes.one_hour)
    };
    thread::sleep(Duration::from_secs(1));
    assert_eq!(one_hour(), 0.0, "answered before the warm-up");

    let period = 300_000;
    let current = now_ms() / period * period;
    let bar = |open_ms: i64, quote_volume: f64| Kline {
        open_ms,
        interval: "5m".into(),
        open: 83_700.0,
        high: 83_800.0,
        low: 83_600.0,
        close: 83_750.0,
        volume: quote_volume / 83_700.0,
        quote_volume,
    };
    // Three sealed bars inside the hour (the client derives nothing from
    // fewer), one older than it, and the one in progress.
    let bars = vec![
        bar(current - 20 * period, 9_000_000.0),
        bar(current - 3 * period, 1_000_000.0),
        bar(current - 2 * period, 2_000_000.0),
        bar(current - period, 3_000_000.0),
        bar(current, 50_000_000.0),
    ];
    core.ev_tx
        .send(FeedEvent::Warmup {
            symbol: "BTCUSDT".into(),
            bars,
        })
        .unwrap();
    core.ev_tx.send(FeedEvent::WarmupDone).unwrap();

    // The client's retry of the held request lands within its 15 s timeout.
    assert!(
        wait_until(Duration::from_secs(20), || one_hour() > 0.0),
        "the snapshot never reached the client's windows"
    );
    let v = one_hour();
    assert!(
        (v - 6_000_000.0).abs() < 1.0,
        "the hour is the three sealed bars, not the one in progress: {v}"
    );
    let _ = client.disconnect();
}

/// The balance and position the client holds for `market` once a full
/// snapshot lands: the globals in USDT and that market's row.
fn next_balance(client: &MoonClient, market: &str) -> ((f64, f64, f64), MarketBalancePosition) {
    assert!(
        wait_until(Duration::from_secs(5), || {
            client
                .drain_events()
                .into_iter()
                .any(|e| matches!(e, Event::Balance(BalanceEvent::SnapshotApplied { .. })))
        }),
        "no balance snapshot reached the client"
    );
    let snap = client.snapshot().expect("snapshot");
    let g = snap.balances().global().clone();
    let pos = snap
        .markets()
        .iter()
        .find(|h| h.with(|m| m.symbol() == market))
        .expect("market")
        .balance_position();
    (
        (
            g.btc_balance_total,
            g.btc_balance_locked,
            g.btc_balance_full,
        ),
        pos,
    )
}

/// M2: the account. A core without a key answers the client's balance refresh
/// with the empty snapshot; an account read reaches every session as a full
/// snapshot — money in USDT, a short with its entry price — the next read
/// without the position leaves the market flat on the client, and a withdrawn
/// (stale) account is the empty snapshot again.
#[test]
fn the_account_reaches_the_client_and_a_closed_position_goes_flat() {
    let core = FedCore::start();
    let client = core.connect();
    client.balances().refresh().unwrap();
    let (money, btc) = next_balance(&client, "BTCUSDT");
    assert_eq!(money, (0.0, 0.0, 0.0), "no key: no money claimed");
    assert_eq!(btc.pos_size, 0.0);

    core.ev_tx
        .send(FeedEvent::Account(Some(Account {
            free: 700.0,
            equity: 1009.5,
            positions: vec![Position {
                symbol: "BTCUSDT".into(),
                size: -0.002,
                entry: 83_000.0,
            }],
        })))
        .unwrap();
    let (money, btc) = next_balance(&client, "BTCUSDT");
    assert_eq!(money, (700.0, 309.5, 1009.5), "free, locked, equity");
    assert_eq!((btc.pos_size, btc.pos_price), (0.002, 83_000.0));
    assert_eq!(
        btc.pos_dir,
        moonproto::OrderType::Sell,
        "a short is a size with a sell direction"
    );

    core.ev_tx
        .send(FeedEvent::Account(Some(Account {
            free: 1000.0,
            equity: 1000.0,
            positions: Vec::new(),
        })))
        .unwrap();
    let (money, btc) = next_balance(&client, "BTCUSDT");
    assert_eq!(money, (1000.0, 0.0, 1000.0));
    assert_eq!(
        (btc.pos_size, btc.pos_price),
        (0.0, 0.0),
        "a position left out of a full snapshot is closed on the client"
    );

    // Reads failing past `STALE_AFTER`: the money is withdrawn, not frozen,
    // and a refresh the client asks for afterwards gets the same answer.
    core.ev_tx.send(FeedEvent::Account(None)).unwrap();
    let (money, _) = next_balance(&client, "BTCUSDT");
    assert_eq!(
        money,
        (0.0, 0.0, 0.0),
        "a stale balance is not shown as live"
    );
    client.balances().refresh().unwrap();
    let (money, _) = next_balance(&client, "BTCUSDT");
    assert_eq!(money, (0.0, 0.0, 0.0));
    let _ = client.disconnect();
}

// ----- M2: orders ---------------------------------------------------------------

/// A Start from the terminal on a core without an account is refused before
/// anything reaches an exchange — and the terminal hears it: the order comes
/// back as BuyFail, and the reason as a line of its core log, not silence.
#[test]
fn a_start_without_an_account_comes_back_as_buy_fail_with_its_reason() {
    // The core's own loop, `step` then `pump`: order images and log lines
    // leave from `pump`, as in `main`.
    let core = FedCore::start();
    let client = core.connect();

    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            80_000.0,
            50.0,
        ))
        .expect("the order request itself is sent");
    let (mut failed, mut reason) = (false, None);
    let answered = wait_until(Duration::from_secs(5), || {
        for event in client.drain_events() {
            match event {
                Event::Order(
                    moonproto::state::OrderEvent::Created(o)
                    | moonproto::state::OrderEvent::Updated(o),
                ) if o.market_name == "BTCUSDT"
                    && o.status == moonproto::OrderWorkerStatus::BuyFail =>
                {
                    failed = true;
                }
                Event::ServerLog(log) if log.msg.contains("trading is off") => {
                    reason = Some(log.msg.clone());
                }
                _ => {}
            }
        }
        failed && reason.is_some()
    });
    assert!(
        answered,
        "BuyFail image: {failed}, log line: {reason:?} — a refused Start must reach the terminal"
    );

    let _ = client.disconnect();
}

/// The next order call the core hands its worker.
fn next_action(rx: &Receiver<TradeCommand>) -> Action {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left).expect("an order call") {
            TradeCommand::Exchange { action, .. } => return action,
            TradeCommand::OpenOrders => {}
        }
    }
}

/// A Start carrying its own stops (the `StopSettings` tail): the entry goes
/// to the exchange, and the order the terminal is shown has those stops — the
/// position is not opened unprotected.
#[test]
fn a_start_with_its_own_stops_keeps_them() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let stops = moonproto::StopSettings::disabled()
        .with_stop_loss_percent(2.5, 0.5)
        .with_trailing_percent(1.2, 0.3);
    client
        .trade()
        .new_order(
            moonproto::NewOrderParams::new("BTCUSDT", moonproto::OrderSide::Long, 83_000.0, 100.0)
                .with_stops(stops),
        )
        .expect("the order request itself is sent");
    let action = next_action(&orders);
    assert!(
        matches!(&action, Action::Post { price: Some(p), lots: 1, sell: false, .. } if *p == 83_000.0),
        "{action:?}"
    );
    let mut seen = None;
    let shown = wait_until(Duration::from_secs(5), || {
        for event in client.drain_events() {
            if let Event::Order(
                moonproto::state::OrderEvent::Created(o) | moonproto::state::OrderEvent::Updated(o),
            ) = event
            {
                if o.stops.stop_loss_enabled() && o.stops.trailing_enabled() {
                    seen = Some((o.stops.stop_loss_level(), o.stops.trailing_level()));
                    return true;
                }
            }
        }
        false
    });
    assert!(shown, "no order image with the stops it was placed with");
    // The stop-loss shows as its price, 2.5 % under the entry (83000 →
    // 80925); the trailing as the percent it trails by.
    assert_eq!(seen, Some((80_925.0, 1.2)));
    let _ = client.disconnect();
}

/// The terminal's emulator mode: the entry is answered by the core's emulator
/// — it rests as a live order would, under the market — and nothing reaches
/// the exchange: a paper trade must never become a real one.
#[test]
fn an_entry_in_emulator_mode_is_answered_by_the_emulator_not_sent() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let mut settings = moonproto::ClientSettingsCommand::default();
    settings.emu_mode = true;
    send_settings(&client, settings);
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            80_000.0,
            100.0,
        ))
        .expect("the order request itself is sent");
    let mut status = None;
    let answered = wait_until(Duration::from_secs(5), || {
        for event in client.drain_events() {
            if let Event::Order(
                moonproto::state::OrderEvent::Created(o) | moonproto::state::OrderEvent::Updated(o),
            ) = event
            {
                status = Some(o.status);
            }
        }
        status == Some(moonproto::OrderWorkerStatus::BuySet)
    });
    assert!(answered, "the emulated entry is not resting: {status:?}");
    let call = std::iter::from_fn(|| orders.try_recv().ok())
        .find(|c| matches!(c, TradeCommand::Exchange { .. }));
    assert!(call.is_none(), "an emulated entry reached the order worker");
    let _ = client.disconnect();
}

/// Send the terminal's settings and wait until the core has answered them,
/// so whatever the test sends next is read after them.
fn send_settings(client: &MoonClient, settings: moonproto::ClientSettingsCommand) {
    let _ = client.drain_events();
    client.settings().send(settings).expect("settings sent");
    let echoed = wait_until(Duration::from_secs(5), || {
        client.drain_events().into_iter().any(|e| {
            matches!(
                e,
                Event::Settings(moonproto::state::SettingsEvent::ClientSettingsUpdated)
            )
        })
    });
    assert!(echoed, "the core did not answer the settings");
}

/// A manual order without its own stops takes the toolbar's: the take
/// profit as its planned exit (+2 % of 83000 = 84660) and the stop, 1.5 %
/// under the entry (81755).
#[test]
fn a_manual_order_takes_the_toolbars_take_profit_and_stop() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let mut settings = moonproto::ClientSettingsCommand::default();
    settings.x_sell = 2;
    settings.panic_if_price_drop = true;
    settings.price_drop_level = -1.5;
    send_settings(&client, settings);
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            83_000.0,
            100.0,
        ))
        .expect("the order request itself is sent");
    assert!(matches!(next_action(&orders), Action::Post { .. }));
    let mut seen = None;
    let shown = wait_until(Duration::from_secs(5), || {
        for event in client.drain_events() {
            if let Event::Order(
                moonproto::state::OrderEvent::Created(o) | moonproto::state::OrderEvent::Updated(o),
            ) = event
            {
                if o.stops.stop_loss_enabled() {
                    seen = Some((o.planned_sell_price, o.stops.stop_loss_level()));
                    return true;
                }
            }
        }
        false
    });
    assert!(shown, "no order image with the toolbar's stop");
    assert_eq!(seen, Some((84_660.0, 81_755.0)));
    let _ = client.disconnect();
}

/// A pending order's own stops are set when it is armed, not when it
/// triggers: nothing reaches the exchange yet, and the image has them — the
/// 3 % stop priced from the trigger (85000 → 82450).
#[test]
fn a_pending_order_keeps_its_own_stops_while_it_waits() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    // A pending order needs the market's price to tell its trigger's side.
    core.ev_tx
        .send(FeedEvent::Trade {
            symbol: "BTCUSDT".into(),
            price: 83_700.0,
            qty: 0.01,
            time_ms: now_ms(),
        })
        .unwrap();
    let stops = moonproto::StopSettings::disabled().with_stop_loss_percent(3.0, 0.0);
    // The trade and the order reach the core by different roads; until the
    // trade is in, the core refuses the order for want of a price, which is
    // its right answer — the order is then sent again.
    let mut shown = false;
    for _ in 0..5 {
        client
            .trade()
            .new_pending_order(
                moonproto::PendingOrderParams::new(
                    "BTCUSDT",
                    moonproto::OrderSide::Long,
                    85_000.0,
                    100.0,
                )
                .with_stops(stops),
            )
            .expect("the order request itself is sent");
        let mut refused = false;
        wait_until(Duration::from_secs(5), || {
            for e in client.drain_events() {
                match e {
                    Event::Order(
                        moonproto::state::OrderEvent::Created(o)
                        | moonproto::state::OrderEvent::Updated(o),
                    ) if o.stops.stop_loss_enabled() && o.stops.stop_loss_level() == 82_450.0 => {
                        shown = true;
                    }
                    Event::ServerLog(l) if l.msg.contains("no price yet") => refused = true,
                    _ => {}
                }
            }
            shown || refused
        });
        if shown {
            break;
        }
    }
    assert!(shown, "no image of the pending order with its stop");
    let call = std::iter::from_fn(|| orders.try_recv().ok())
        .find(|c| matches!(c, TradeCommand::Exchange { .. }));
    assert!(
        call.is_none(),
        "a pending order reached the exchange before its trigger"
    );
    let _ = client.disconnect();
}

/// A report of the exchange for the order the core just posted under `key`.
fn report(key: &str, id: &str, status: ExecStatus, lots: i64, filled: i64) -> FeedEvent {
    FeedEvent::Trading(TradingEvent::Order(OrderUpdate {
        exchange_id: id.into(),
        request_id: key.into(),
        uid: "BTCUSDT".into(),
        status,
        sell: false,
        is_market: false,
        lots_requested: lots,
        lots_executed: filled,
        price: 83_000.0,
        avg_price: if filled > 0 { 83_000.0 } else { 0.0 },
        unary: true,
        time_ms: now_ms(),
        message: String::new(),
    }))
}

fn log_line(client: &MoonClient, needle: &str) -> bool {
    wait_until(Duration::from_secs(5), || {
        client
            .drain_events()
            .into_iter()
            .any(|e| matches!(e, Event::ServerLog(l) if l.msg.contains(needle)))
    })
}

fn post_entry(client: &MoonClient, orders: &Receiver<TradeCommand>) -> String {
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            83_000.0,
            100.0,
        ))
        .expect("the order request itself is sent");
    match next_action(orders) {
        Action::Post { key, .. } => key,
        other => panic!("{other:?}"),
    }
}

/// MoonBot's guarded shutdown: refused while the core holds a position.
#[test]
fn a_shutdown_is_refused_while_a_position_is_open() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let key = post_entry(&client, &orders);
    core.ev_tx
        .send(report(&key, "901", ExecStatus::Filled, 1, 1))
        .unwrap();
    // The entry filled: the core now holds a position.
    assert!(wait_until(Duration::from_secs(5), || {
        client.drain_events().into_iter().any(|e| {
            matches!(e, Event::Order(moonproto::state::OrderEvent::Updated(o))
                if o.status == moonproto::OrderWorkerStatus::BuyDone)
        })
    }));
    client.settings().request_core_shutdown().expect("sent");
    assert!(
        log_line(&client, "shutdown refused: 1 position(s)"),
        "no refusal while a position is open"
    );
    let _ = client.disconnect();
}

/// Without a position the core agrees, and the stop withdraws the live entry:
/// its cancel reaches the order worker. Nothing new may be entered after.
#[test]
fn a_shutdown_withdraws_the_live_entries() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let key = post_entry(&client, &orders);
    core.ev_tx
        .send(report(&key, "902", ExecStatus::New, 1, 0))
        .unwrap();
    // Let the core take the report before the shutdown is judged.
    assert!(wait_until(Duration::from_secs(5), || {
        client.drain_events().into_iter().any(|e| {
            matches!(e, Event::Order(moonproto::state::OrderEvent::Updated(o))
                if o.status == moonproto::OrderWorkerStatus::BuySet)
        })
    }));
    client.settings().request_core_shutdown().expect("sent");
    assert!(log_line(&client, "the core is leaving"));
    let cancel = next_action(&orders);
    assert!(
        matches!(&cancel, Action::Cancel { exchange_id, leg: aster_core::orders::Leg::Buy, .. } if exchange_id == "902"),
        "{cancel:?}"
    );
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            83_000.0,
            100.0,
        ))
        .expect("sent");
    assert!(
        log_line(&client, "the core is stopping"),
        "an entry after the stop was not refused"
    );
    let post = std::iter::from_fn(|| orders.try_recv().ok()).find(|c| {
        matches!(
            c,
            TradeCommand::Exchange {
                action: Action::Post { .. },
                ..
            }
        )
    });
    assert!(
        post.is_none(),
        "an entry after the stop reached the order worker"
    );
    let _ = client.disconnect();
}

/// «Move all» of the buys: every resting entry of the market moves to the
/// price the terminal names — as a Replace each, at a tick, in the band.
#[test]
fn move_all_moves_every_resting_entry_to_the_price() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    let mut keys = Vec::new();
    for (i, id) in ["911", "912"].into_iter().enumerate() {
        let key = post_entry(&client, &orders);
        core.ev_tx
            .send(report(&key, id, ExecStatus::New, 1, 0))
            .unwrap();
        keys.push(key);
        // Each acknowledged before the next, so the client holds both.
        let n = i + 1;
        assert!(wait_until(Duration::from_secs(5), || {
            let _ = client.drain_events();
            client.snapshot().is_some_and(|s| {
                s.orders()
                    .iter()
                    .filter(|o| o.status == moonproto::OrderWorkerStatus::BuySet)
                    .count()
                    >= n
            })
        }));
    }
    client
        .trade()
        .move_all_buys(
            "BTCUSDT",
            moonproto::MoveAllBuysParams::replace_kind(
                moonproto::BulkMoveKind::All,
                82_500.0,
                moonproto::PositionFilter::Both,
            ),
        )
        .expect("sent");
    let mut moved = Vec::new();
    for _ in 0..2 {
        match next_action(&orders) {
            Action::Replace {
                exchange_id, price, ..
            } => moved.push((exchange_id, price)),
            other => panic!("{other:?}"),
        }
    }
    moved.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        moved,
        vec![("911".to_string(), 82_500.0), ("912".to_string(), 82_500.0)]
    );
    let _ = client.disconnect();
}

/// `-4141` on an entry: the next entry on that market is refused by the core,
/// before the exchange.
#[test]
fn an_entry_refused_for_a_closed_market_closes_it_to_entries() {
    let (core, orders) = FedCore::trading();
    let client = core.connect();
    // The exchange refuses the entry the core posted.
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            83_000.0,
            100.0,
        ))
        .expect("sent");
    let posted = next_action(&orders);
    core.ev_tx
        .send(FeedEvent::Trading(TradingEvent::Failed {
            action: posted,
            definitive: true,
            msg: "api 400/-4141: Symbol is closed for new positions.".into(),
        }))
        .unwrap();
    assert!(log_line(&client, "takes no new positions"));
    client
        .trade()
        .new_order(moonproto::NewOrderParams::new(
            "BTCUSDT",
            moonproto::OrderSide::Long,
            83_000.0,
            100.0,
        ))
        .expect("sent");
    assert!(log_line(&client, "takes no new positions on it (until"));
    let post = std::iter::from_fn(|| orders.try_recv().ok()).find(|c| {
        matches!(
            c,
            TradeCommand::Exchange {
                action: Action::Post { .. },
                ..
            }
        )
    });
    assert!(
        post.is_none(),
        "an entry on a closed market reached the worker"
    );
    let _ = client.disconnect();
}

// ----- M3: strategies ----------------------------------------------------------

/// The terminal's own client, to `Ready` with `strategies` as its local list.
fn connect_with(key: &ServerKey, strategies: Vec<moonproto::StrategySnapshot>) -> MoonClient {
    let cfg = ClientConfig::new("127.0.0.1", key.port, key.master_key, key.mac_key)
        .with_transport_mode(key.transport_mode);
    let init = InitConfig {
        initial_strategies: Some(InitialStrategies::new(1, strategies)),
        ..Default::default()
    };
    MoonClient::connect_blocking(
        cfg,
        ConnectConfig::new(init).with_connect_timeout(Duration::from_secs(20)),
        Duration::from_secs(30),
    )
    .expect("the client must reach Ready against this core")
}

/// One trade of BTCUSDT on the fed tape.
fn btc_trade(core: &FedCore, price: f64) {
    core.ev_tx
        .send(FeedEvent::Trade {
            symbol: "BTCUSDT".into(),
            price,
            qty: 0.01,
            time_ms: now_ms(),
        })
        .unwrap();
}

/// M3's first observation, on loopback: a MoonShot strategy the terminal
/// brings in its list and starts runs in the emulator — its entry rests under
/// the market, the tape fills it, its exit goes out at `SellPrice` and the
/// tape fills that too — and not one call reaches the order worker. A
/// strategy at zero money risk, with no exchange behind it.
#[test]
fn an_emulated_moonshot_enters_and_exits_on_the_tape_and_never_reaches_the_exchange() {
    use moonproto::{FieldValue, OrderWorkerStatus, StrategyFields, StrategyKind};
    let (core, orders) = FedCore::trading();
    let mut fields = StrategyFields::new();
    for (k, v) in [
        ("StrategyName", FieldValue::String("emu".into())),
        ("CoinsWhiteList", FieldValue::String("BTCUSDT".into())),
        ("EmulatorMode", FieldValue::Bool(true)),
        ("OrderSize", FieldValue::Double(100.0)),
        ("MShotPrice", FieldValue::Double(0.5)),
        ("MShotPriceMin", FieldValue::Double(0.3)),
        ("MShotAdd15minDelta", FieldValue::Double(0.0)),
        ("MShotAddHourlyDelta", FieldValue::Double(0.0)),
        ("SellPrice", FieldValue::Double(1.0)),
        ("PriceDownTimer", FieldValue::Double(0.0)),
        ("UseStopLoss", FieldValue::Bool(false)),
    ] {
        fields.insert(k, v);
    }
    let strategy = moonproto::StrategySnapshot::new(
        11,
        1,
        now_ms() as u64,
        true,
        StrategyKind::MOON_SHOT,
        "",
        fields,
    );
    let client = connect_with(&core.key, vec![strategy]);
    // A price for the market, then the start button.
    btc_trade(&core, BTC_BID);
    client.strategies().start().expect("start sent");

    let mut seen: Vec<(OrderWorkerStatus, f64, f64)> = Vec::new();
    let watch = |seen: &mut Vec<(OrderWorkerStatus, f64, f64)>, want: OrderWorkerStatus| {
        wait_until(Duration::from_secs(10), || {
            for event in client.drain_events() {
                if let Event::Order(
                    moonproto::state::OrderEvent::Created(o)
                    | moonproto::state::OrderEvent::Updated(o),
                ) = event
                {
                    seen.push((o.status, o.buy_price, o.sell_price));
                }
            }
            seen.iter().any(|(s, ..)| *s == want)
        })
    };
    assert!(
        watch(&mut seen, OrderWorkerStatus::BuySet),
        "no entry from the strategy: {seen:?}"
    );
    // The entry rests 0.5 % under the bid.
    let entry = seen
        .iter()
        .find(|(s, ..)| *s == OrderWorkerStatus::BuySet)
        .map(|(_, b, _)| *b)
        .unwrap();
    assert!((entry - BTC_BID * 0.995).abs() < 1.0, "entry at {entry}");
    // The tape trades through it: filled at its own limit, the exit goes out.
    btc_trade(&core, entry - 10.0);
    assert!(
        watch(&mut seen, OrderWorkerStatus::SellSet),
        "no exit after the fill: {seen:?}"
    );
    let exit = seen
        .iter()
        .rev()
        .find(|(s, ..)| *s == OrderWorkerStatus::SellSet)
        .map(|(_, _, sp)| *sp)
        .unwrap();
    assert!(
        (exit - entry * 1.01).abs() < 1.0,
        "exit at {exit} for {entry}"
    );
    btc_trade(&core, exit + 10.0);
    assert!(
        watch(&mut seen, OrderWorkerStatus::SellDone),
        "the exit never filled: {seen:?}"
    );

    let call = std::iter::from_fn(|| orders.try_recv().ok())
        .find(|c| matches!(c, TradeCommand::Exchange { .. }));
    assert!(call.is_none(), "an emulated order reached the order worker");
    let _ = client.disconnect();
}
