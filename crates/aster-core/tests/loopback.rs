//! The M0 contract test: our server, and the terminal's own client of the same
//! `moonproto` rev, on loopback.
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
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aster_core::aster::json::{BookTicker, ExchangeInfo, PremiumIndex};
use aster_core::engine::{
    CoreHandler, ACCOUNT_PLACEHOLDER, EXCHANGE_CODE, EXCHANGE_NAME, SERVER_NAME,
};
use aster_core::model::Catalog;
use aster_core::strategies::Strategies;
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
        let cfg = ClientConfig::new(
            "127.0.0.1",
            self.key.port,
            self.key.master_key,
            self.key.mac_key,
        )
        .with_transport_mode(self.key.transport_mode);
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

/// The whole of M0 in one test: the client walks BaseCheck, AuthCheck,
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
