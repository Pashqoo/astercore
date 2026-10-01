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

use aster_core::aster::json::{BookTicker, ExchangeInfo, Kline, PremiumIndex};
use aster_core::book::{Diff, Snapshot};
use aster_core::engine::{
    CoreHandler, FeedLink, ACCOUNT_PLACEHOLDER, EXCHANGE_CODE, EXCHANGE_NAME, SERVER_NAME,
};
use aster_core::feed::{FeedCommand, FeedEvent};
use aster_core::load::Load;
use aster_core::model::Catalog;
use aster_core::strategies::Strategies;
use aster_core::stream_health::StreamHealth;
use moonproto::server::codec::market_data::{delphi_days, Candle};
use moonproto::server::key_export::ServerKey;
use moonproto::server::Server;
use moonproto::state::AccountEvent;
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
        let handler = CoreHandler::new(1, ACCOUNT_PLACEHOLDER.into(), catalog(), Strategies::new());
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
        let mut key = ServerKey::generate(None, 0, TransportMode::V2);
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = mpsc::channel();
        let handler = CoreHandler::new(1, ACCOUNT_PLACEHOLDER.into(), catalog(), Strategies::new())
            .with_feed(FeedLink {
                tx: cmd_tx,
                health: StreamHealth::default(),
                load: Arc::new(Load::default()),
            });
        let mut server = Server::bind(&key, handler).expect("bind");
        key.port = server.local_addr().expect("local_addr").port();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.step();
                let (h, sessions) = server.split();
                h.pump(sessions, &ev_rx);
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
