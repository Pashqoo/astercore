//! The core: read Aster's catalog, then speak MoonProto to the terminal.
//!
//! The lines it prints at startup are the observation that the REST path and
//! the catalog mapping work against the real exchange — `clock:`, `limits:`,
//! `catalog:` and `wire:`, each number of them meant to be read against an
//! independent parse of `/fapi/v1/exchangeInfo` (`AGENTS.md`, "Канал
//! наблюдения"), then `account:` — the first read of the balance and
//! positions, when there is a key to sign it with; the terminal gets every
//! later one. After them it opens the market streams (`feed.rs`), binds the
//! UDP socket and serves: the catalog goes to the terminal once per session,
//! the prices every couple of seconds, the tape, books and candles live.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aster_core::account;
use aster_core::api_meter::{self, ApiMeter};
use aster_core::aster::rest::Rest;
use aster_core::aster::sign::{Credentials, LoadError, Network, Signer};
use aster_core::control::{self, Control};
use aster_core::engine::{self, CoreHandler, FeedLink};
use aster_core::feed;
use aster_core::key_store;
use aster_core::levman;
use aster_core::load::Load;
use aster_core::model::{Catalog, QUOTE};
use aster_core::order_store::OrderStore;
use aster_core::prices;
use aster_core::reports::Reports;
use aster_core::settings;
use aster_core::stderr_log;
use aster_core::strategies::Strategies;
use aster_core::stream_health::StreamHealth;
use aster_core::trading;
use aster_core::{telegram, web};
use moonproto::server::codec::engine as engine_codec;
use moonproto::server::codec::ui;
use moonproto::server::Server;

/// One past TInvestCore's 3100: the two cores are expected to run on the same
/// machine, and a port collision would show up as a terminal that cannot
/// connect rather than as an error.
const DEFAULT_PORT: u16 = 3101;
/// Feed snapshots applied per UDP-loop iteration, so receiving is never starved
/// by a backlog the loop built up itself.
const APPLY_BATCH: usize = 16;
/// The API wallet's key file, unless `ASTER_API_KEY_FILE` names another. The
/// name is the one `.gitignore` already closes (`asterkey*`).
const API_KEY_FILE: &str = "asterkey";
/// The order store (`order_store.rs`), in the working directory, as
/// TInvestCore keeps it.
const ORDERS_FILE: &str = "data/orders.json";
/// A core without an account keeps its (emulated) orders apart: the real ones
/// of a keyed run must not be restored where nothing can work them and
/// refused as «trading is off».
const EMULATOR_ORDERS_FILE: &str = "data/orders-emulator.json";
/// The strategy list as MoonBot text (`strategy_file.rs`) and the trade
/// reports (`reports.rs`), beside it.
const STRATEGIES_FILE: &str = "data/strategies.txt";
/// The last leverage-management settings, from the terminal or the page (`levman`).
const LEV_MANAGE_FILE: &str = "data/lev_manage.bin";
/// A day of request counters and round trips for the page's API tab.
/// Telemetry and nothing else: losing it costs a chart.
const API_METER_FILE: &str = "data/api_meter.json";
/// How often the counters go to disk.
const METER_SAVE: Duration = Duration::from_secs(60);
/// The link line's heartbeat: `/fapi/v1/time` (weight 1) this often, whatever
/// else the core is doing — that call *is* the line (`api_meter`), and ten
/// seconds keeps its connection warm (TInvestCore measured the cold one as a
/// different number).
const PING_HEARTBEAT: Duration = Duration::from_secs(10);
const REPORTS_FILE: &str = "data/reports.jsonl";

fn main() -> ExitCode {
    // First of all, before the journal, the settings or anything that writes: it only reads the
    // key file, and run as another user than the service it must leave nothing of its own in
    // the working directory.
    if std::env::args().any(|a| a == "--print-key") {
        return match key_store::load(key_store::DEFAULT_PATH) {
            Ok(key) => {
                println!("{}", key.export());
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("key: {}: {e}", key_store::DEFAULT_PATH);
                ExitCode::FAILURE
            }
        };
    }
    stderr_log::init();
    // `data/config.json`: the page, the chat, the journal. A file that exists
    // and does not read is a refusal to start, not an empty start — its
    // fields decide whether the page asks for a password.
    let settings = match settings::load_or_create(settings::DEFAULT_PATH, &settings::from_env()) {
        Ok(loaded) => {
            stderr_log::set_level(&loaded.settings.log_level);
            stderr_log::set_keep_days(loaded.settings.log_keep_days);
            if loaded.created {
                log::info!("config: {} created", settings::DEFAULT_PATH);
            }
            for var in &loaded.ignored_env {
                log::warn!(
                    "config: {var} differs from {} and is ignored; change it there",
                    settings::DEFAULT_PATH
                );
            }
            loaded.settings
        }
        Err(e) => {
            eprintln!("config: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Every REST call of the process counts into it from the first one on.
    let meter = ApiMeter::new(Some(API_METER_FILE.into()));
    // Aster prices one call by count: `ORDERS` 1200 a minute, the new orders
    // (`exchangeInfo.rateLimits`, measured 01.10); the weight and the 10 s
    // order window are the exchange's own gauges on the page.
    meter.set_tariff(vec![api_meter::TariffGroup {
        methods: vec!["POST /fapi/v3/order".into()],
        per_minute: 1200,
        per_second: None,
    }]);
    api_meter::set_global(Arc::clone(&meter));

    let port = std::env::var("ASTER_CORE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let address = match std::env::var("ASTER_CORE_ADDR") {
        Ok(a) if !a.trim().is_empty() => match a.trim().parse() {
            Ok(ip) => Some(ip),
            // The address is only ever used to MINT a key, so a typo must not
            // take the core off the market when a key already exists — but it
            // must not silently mint a key for the wrong endpoint either.
            Err(_) if !std::path::Path::new(key_store::DEFAULT_PATH).exists() => {
                eprintln!("key: ASTER_CORE_ADDR is not an IP address: {a}");
                return ExitCode::FAILURE;
            }
            Err(_) => {
                log::warn!(
                    "key: ASTER_CORE_ADDR is not an IP address ({a}); \
                     the existing {} decides where the terminal dials",
                    key_store::DEFAULT_PATH
                );
                None
            }
        },
        _ => None,
    };

    let key = match key_store::load_or_create(key_store::DEFAULT_PATH, address, port) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("key: {e}");
            return ExitCode::FAILURE;
        }
    };
    // An existing key keeps its own address, so a changed variable is a no-op
    // the operator has to hear about — it looks applied otherwise.
    if let Some(requested) = address {
        if key.address != Some(requested) {
            log::warn!(
                "key {}: ASTER_CORE_ADDR is {requested}, but the existing key advertises {}; \
                 remove {} and restart to issue a key for {requested}, then re-import it",
                key.rnd,
                key.address
                    .map_or_else(|| "no address".to_string(), |a| a.to_string()),
                key_store::DEFAULT_PATH
            );
        }
    }
    // The export IS the private key, so it goes to a console and nowhere else: not to the log
    // file, and not to a pipe either — under systemd stdout is the journal, which keeps it for
    // good and shows it to the groups that read the journal (`AGENTS.md`, `## Secrets`). The
    // line is read once off the console and pasted into the terminal; `--print-key` (handled first
    // thing in `main`) prints it again from the key file, for a core that runs as a service.
    if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        println!(
            "Astercore key {} port {} mode {}\n{}",
            key.rnd,
            key.port,
            key.transport_mode.name(),
            key.export()
        );
    } else {
        println!(
            "Astercore key {} port {} mode {}: not printed, stdout is not a console; \
             run `aster-core --print-key` in the working directory",
            key.rnd,
            key.port,
            key.transport_mode.name()
        );
    }
    log::info!(
        "key {}: terminal dials {}:{} over {}",
        key.rnd,
        key.address.map_or_else(
            || "<no address — the terminal will dial 127.0.0.1>".to_string(),
            |a| a.to_string()
        ),
        key.port,
        key.transport_mode.name()
    );

    let mut rest = Rest::new();
    // The clock first: every signed call adds this delta to its nonce, and the
    // gateway refuses a nonce outside ±60 s of its own clock (`aster/sign.rs`).
    match rest.sync_clock() {
        Ok(delta) => println!(
            "clock: delta {delta} ms, rtt {} ms",
            rest.last_rtt().map(|d| d.as_millis()).unwrap_or(0)
        ),
        // Fatal, unlike the merges below: a core whose clock is unchecked
        // cannot sign anything, and starting anyway would only move the failure
        // to the first order.
        Err(e) => {
            eprintln!("clock: {e}");
            return ExitCode::FAILURE;
        }
    }

    let info = match rest.exchange_info() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("exchangeInfo: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "limits: {}",
        info.rate_limits
            .iter()
            .map(|l| format!("{} {}/{}{}", l.kind, l.limit, l.interval_num, l.interval))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut cat = Catalog::build(&info);
    // All three merges are best-effort on purpose: the catalog itself is what
    // the terminal cannot start without, while a missing turnover, funding or
    // quote degrades one column. Every failure is printed and counted in the
    // `catalog:` line, not fatal.
    match rest.ticker_24h_all() {
        Ok(rows) => {
            let filled = cat.apply_tickers(&rows);
            log::info!("tickers: {} rows -> {filled} markets", rows.len());
        }
        Err(e) => eprintln!("ticker/24hr: {e}"),
    }
    match rest.premium_index_all() {
        Ok(rows) => {
            let filled = cat.apply_premium_index(&rows);
            log::info!("premiumIndex: {} rows -> {filled} funded", rows.len());
        }
        Err(e) => eprintln!("premiumIndex: {e}"),
    }
    match rest.book_ticker_all() {
        Ok(rows) => {
            let filled = cat.apply_book(&rows);
            log::info!("bookTicker: {} rows -> {filled} quoted", rows.len());
        }
        Err(e) => eprintln!("ticker/bookTicker: {e}"),
    }
    println!("{}", cat.summary());
    // The catalog as the terminal will actually receive it. One line rather
    // than the per-symbol dump of the first milestone: the fields that were
    // inherited as MOEX zeros are checked in `PLAN.md` ("Находка M0") and in
    // the encoder's own tests, while the size is what says the whole catalog
    // encoded here and now.
    let specs = cat.specs();
    println!(
        "wire: {} rows, GetMarketsList {} bytes, {} price rows",
        specs.len(),
        engine_codec::write_markets_list(&specs).len(),
        cat.prices().len()
    );
    // The account, when there is a key. No key is a market-data core, as M1
    // was; a key that is there and does not sign is fatal, because the trader
    // put it there to trade and would otherwise learn it on the first order.
    // A path named explicitly is a key the operator means to trade with, so its
    // absence is fatal; only the default name may be absent.
    //
    // After the catalog, because a position counts only on one of its markets
    // (`account.rs`); before the socket, so the first terminal gets the money.
    let named_key = std::env::var("ASTER_API_KEY_FILE")
        .ok()
        .filter(|p| !p.trim().is_empty());
    let key_path = named_key.clone().unwrap_or_else(|| API_KEY_FILE.into());
    let network = match std::env::var("ASTER_NET") {
        Ok(n) if !n.trim().is_empty() => match Network::parse(&n) {
            Some(net) => net,
            None => {
                eprintln!("account: ASTER_NET is neither mainnet nor testnet: {n}");
                return ExitCode::FAILURE;
            }
        },
        _ => Network::Mainnet,
    };
    let symbols_of_account = account::Symbols {
        rows: cat.symbols().iter().map(|s| s.to_string()).collect(),
        usdt: info
            .symbols
            .iter()
            .filter(|s| {
                let margin = if s.margin_asset.is_empty() {
                    &s.quote_asset
                } else {
                    &s.margin_asset
                };
                margin == QUOTE
            })
            .map(|s| s.symbol.clone())
            .collect(),
    };
    let account = match Credentials::load(&key_path) {
        Err(LoadError::Io(e))
            if e.kind() == std::io::ErrorKind::NotFound && named_key.is_none() =>
        {
            println!(
                "account: none ({key_path} absent, {}) — market data only",
                network.name()
            );
            None
        }
        Err(e) => {
            eprintln!("account: {key_path}: {e}");
            return ExitCode::FAILURE;
        }
        Ok(creds) => {
            warn_if_shared(&key_path);
            let mut signer = Signer::new(creds, network);
            let who = format!(
                "{} signer {} user {}",
                network.name(),
                short(signer.credentials().signer()),
                signer.credentials().user().map_or("none".into(), short)
            );
            // The account's own client, on the signer's network: its nonces
            // carry a clock measured against the gateway that checks them.
            let mut account_rest = Rest::on(network);
            if let Err(e) = account_rest.sync_clock() {
                eprintln!("account: {who}: clock: {e}");
                return ExitCode::FAILURE;
            }
            match account_rest.dual_side_position(&mut signer) {
                Ok(false) => {}
                Ok(true) => {
                    eprintln!(
                        "account: {who}: the account is in hedge mode; the core's order model is \
                         one-way (every entry would be refused with -4061). Switch the account to \
                         one-way mode and start again"
                    );
                    return ExitCode::FAILURE;
                }
                Err(e) => {
                    eprintln!("account: {who}: position mode: {e}");
                    return ExitCode::FAILURE;
                }
            }
            match account::read(&mut account_rest, &mut signer, &symbols_of_account) {
                Ok(a) => {
                    println!(
                        "account: {who}, clock delta {} ms, USDT free {:.2} equity {:.2}, \
                         {} positions{}",
                        account_rest.clock_delta_ms(),
                        a.free,
                        a.equity,
                        a.positions.len(),
                        a.positions
                            .iter()
                            .map(|p| format!(" {} {}@{}", p.symbol, p.size, p.entry))
                            .collect::<String>()
                    );
                    // The leverage the account may ask for, per market. Not fatal: without it
                    // the catalog keeps the instrument's own ceiling (`Market::max_leverage`),
                    // which never promises more than the exchange allows.
                    match account_rest
                        .leverage_brackets(&mut signer, |s| symbols_of_account.rows.contains(s))
                    {
                        Ok(rows) => {
                            let taken = cat.apply_leverage_brackets(&rows);
                            println!(
                                "leverage: {taken} of {} markets from the account's brackets",
                                symbols_of_account.rows.len()
                            );
                        }
                        Err(e) => {
                            eprintln!("leverage: brackets: {e} — the instrument's ceiling stands")
                        }
                    }
                    Some((account_rest, signer, a))
                }
                Err(e) => {
                    eprintln!("account: {who}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    if let Some(usage) = rest.usage().weight_1m {
        log::info!("usage: weight-1m {usage}");
    }

    // From here on every call to the exchange is made off the UDP loop: the
    // top-of-book refresher (`prices.rs`) and the market feed (`feed.rs`) each
    // on their own threads with their own clients. The refresher's first call
    // is one period away, because the answers above are as fresh as a call made
    // now would be; the mark stream replaces the startup `premiumIndex` within
    // its first 3 s frame.
    let feeds = prices::start();
    // The streams open before the socket binds, so the first terminal to
    // connect finds the tape already flowing rather than the handshakes still
    // in flight.
    let (ev_tx, ev_rx) = mpsc::channel();
    let mut health = StreamHealth::default();
    let load = Arc::new(Load::default());
    let symbols: Vec<String> = cat.symbols().iter().map(|s| s.to_string()).collect();
    // The grid of every market, for the order worker to read open orders on.
    let grids: std::collections::HashMap<String, trading::Grid> = cat
        .markets()
        .iter()
        .map(|m| {
            let grid = trading::Grid {
                step: m.step_size,
                tick: m.tick_size,
            };
            (m.symbol.clone(), grid)
        })
        .collect();
    let now = engine::now_ms();
    // The account as the page and the chat name it.
    let account_label = account.as_ref().map_or_else(
        || "none (emulator only)".to_string(),
        |(_, s, _)| format!("{} {}", s.network().name(), short(s.credentials().signer())),
    );
    // The control queue is built before the handler: the Telegram poller and
    // the page each hold a sender of it and nothing else of the core's.
    let control = Control::new();
    let reporter = telegram::start(&settings, control.sender());
    web::start(&settings, control.sender(), Some(Arc::clone(&meter)));
    start_meter(Arc::clone(&meter));
    let handler = CoreHandler::new(
        1,
        engine::ACCOUNT_PLACEHOLDER.to_string(),
        cat,
        Strategies::new(Some(PathBuf::from(STRATEGIES_FILE)), now),
    )
    .with_settings(settings, PathBuf::from(settings::DEFAULT_PATH))
    .with_telegram(reporter)
    .with_account_label(account_label)
    .with_reports(Reports::open(Some(PathBuf::from(REPORTS_FILE)), now));
    // The order store with or without an account: a core without one runs
    // its strategies in the emulator, and a restart must resume them.
    let orders_file = if account.is_some() {
        ORDERS_FILE
    } else {
        EMULATOR_ORDERS_FILE
    };
    let (store, saved) = OrderStore::open(PathBuf::from(orders_file));
    let mut handler = handler.with_orders(store, saved);
    // The terminal's leverage management survives a restart with or without an account; only
    // an account acts on it.
    let lev_saved = levman::load(std::path::Path::new(LEV_MANAGE_FILE));
    handler = handler.with_levman(PathBuf::from(LEV_MANAGE_FILE), lev_saved.clone(), None);
    if let Some((account_rest, signer, first)) = account {
        // The order workers sign with clones of the account's signer: one
        // wallet, one nonce sequence (`Signer`). `orders_rest` is the first
        // worker's client — on the same network, measured against the same
        // gateway; `trading::start` builds the others the same way.
        let mut orders_rest = Rest::on(signer.network());
        orders_rest.set_clock_delta_ms(account_rest.clock_delta_ms());
        let trading = trading::start(orders_rest, signer.clone(), grids, ev_tx.clone());
        // The leverage management of the terminal: its own client and thread, the wallet's
        // signer (one nonce sequence), the settings of the previous run acted on at once.
        let mut lev_rest = Rest::on(signer.network());
        lev_rest.set_clock_delta_ms(account_rest.clock_delta_ms());
        let lev_config = lev_saved
            .as_deref()
            .and_then(ui::lev_manage)
            .map(|l| levman::Config::from_wire(&l));
        let lev_worker = levman::start(lev_rest, signer.clone(), lev_config, handler.lev_status());
        handler = handler.with_levman(PathBuf::from(LEV_MANAGE_FILE), lev_saved, Some(lev_worker));
        account::start(
            account_rest,
            signer,
            symbols_of_account,
            first.clone(),
            ev_tx.clone(),
        );
        handler = handler.with_account(first).with_trading(trading);
    }
    let feed_tx = feed::start(&symbols, ev_tx, &mut health, Arc::clone(&load));
    let handler = handler.with_feed(FeedLink {
        tx: feed_tx,
        health,
        load,
    });
    let mut server = match Server::bind(&key, handler) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("bind: {e}");
            return ExitCode::FAILURE;
        }
    };
    log::info!(
        "serving MoonProto on udp/{} as {} (exchange code {})",
        key.port,
        engine::SERVER_NAME,
        engine::EXCHANGE_CODE
    );
    // The signal handlers go up here, not earlier: until the loop below runs
    // nobody would notice the flag, and a `systemctl stop` during the start
    // would be swallowed instead of killing the process. That death by SIGTERM is exit 143
    // to a shell; to systemd it is a clean stop (`SIGTERM` is among the signals a unit
    // does not count as a failure), so `Restart=on-failure` leaves it stopped, as for 0.
    control::install_signals();
    {
        let (handler, _) = server.split();
        handler.announce();
    }

    // Every way out of THIS LOOP goes through the stop below: the entries are
    // withdrawn, the terminals told, the orders written. The exit code is the
    // supervisor's to read: 0 a stop that was asked for (the page, the
    // terminal, a signal), 70 a restart it is meant to undo, 1 a failure.
    let code = loop {
        server.step();
        // A dead refresher would leave its prices frozen, and nothing else in
        // the process would notice. Checked before the snapshots, because a
        // panicked refresher has nothing left to send and the loop must not
        // keep serving what it last sent.
        if let Some(name) = feeds.dead() {
            log::error!("prices: the {name} refresher is gone — its prices would freeze");
            break ExitCode::FAILURE;
        }
        let (handler, sessions) = server.split();
        handler.pump(sessions, &ev_rx);
        if handler.feed_lost() {
            log::error!("feed: the market feed is gone — the tape and books would freeze");
            break ExitCode::FAILURE;
        }
        handler.run_control(&control);
        if let Some(halt) = control.halted() {
            log::info!("exit {} ({})", halt.code(), halt.name());
            break ExitCode::from(halt.code());
        }
        let mut gone = false;
        for _ in 0..APPLY_BATCH {
            match feeds.try_recv() {
                Ok(snap) => handler.apply(snap),
                Err(TryRecvError::Empty) => break,
                // The refresher's sender dropped, which the check above
                // normally sees first. Kept because this is the end of the
                // exhaustive match, not a case that cannot happen; either way
                // the core leaves rather than serving prices that will never
                // move again, and the exit code is what brings it back.
                Err(TryRecvError::Disconnected) => {
                    log::error!("prices: the refresher is gone — prices would freeze");
                    gone = true;
                    break;
                }
            }
        }
        if gone {
            break ExitCode::FAILURE;
        }
    };
    // Whatever the reason for leaving, the entries go first and the orders
    // reach the disk last.
    let clean = withdraw_entries(&mut server, &ev_rx);
    flush_sessions(&mut server, &ev_rx);
    let (handler, _) = server.split();
    handler.finish();
    // The last minute of counters, after everything that matters is on disk.
    meter.save();
    // A shutdown that left orders with the exchange did not end cleanly.
    if clean {
        code
    } else {
        ExitCode::FAILURE
    }
}

/// The counters' own thread: the link line's heartbeat and the window to
/// disk. Neither belongs in the trading loop — one is a request, one a file.
fn start_meter(meter: Arc<ApiMeter>) {
    let spawned = std::thread::Builder::new()
        .name("api-meter".into())
        .spawn(move || {
            let mut rest = Rest::new();
            let (mut next_save, mut next_ping) = (Instant::now() + METER_SAVE, Instant::now());
            loop {
                // A panic in one pass is said and the thread goes on: it is telemetry, and
                // a heartbeat that died silently would leave the link line frozen for good.
                let pass = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if Instant::now() >= next_ping {
                        // The answer is thrown away: the round trip is what was
                        // asked for, and the call has counted itself.
                        let _ = rest.server_time();
                        // From the answer, not from the deadline: a beat that
                        // waited for a dead gateway must not be followed by a
                        // burst catching up on it.
                        next_ping = Instant::now() + PING_HEARTBEAT;
                    }
                    if Instant::now() >= next_save {
                        meter.save();
                        next_save = Instant::now() + METER_SAVE;
                    }
                }));
                if pass.is_err() {
                    log::error!("api meter: a pass panicked; going on");
                    next_ping = Instant::now() + PING_HEARTBEAT;
                    next_save = Instant::now() + METER_SAVE;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        });
    if let Err(e) = spawned {
        log::warn!("api meter: the thread did not start: {e}; no ping line, no history on disk");
    }
}

/// How long a leaving core waits for its last messages to reach the
/// terminals and be acknowledged. On a LAN an ACK takes milliseconds; a second
/// covers a reliable message's first retries; a terminal that is gone never
/// answers, and costs the whole second.
const FLUSH_WAIT: Duration = Duration::from_secs(1);

/// Send what is still queued — the images of the orders the stop withdrew,
/// its log lines — and wait for their ACKs, up to [`FLUSH_WAIT`]. Without it
/// the process ends with them in the sessions' queues: they are sent only by
/// `Server::step`, and the terminal keeps showing an order the exchange no
/// longer has (01.10: a withdrawn entry stayed on the chart).
fn flush_sessions(server: &mut Server<CoreHandler>, rx: &mpsc::Receiver<feed::FeedEvent>) {
    let deadline = Instant::now() + FLUSH_WAIT;
    loop {
        // The outbox into the sessions first, then a step: it puts them on the
        // wire and takes the ACKs in. Judged after both, so nothing the pump
        // just queued is left behind.
        {
            let (handler, sessions) = server.split();
            handler.pump(sessions, rx);
        }
        server.step();
        let (handler, sessions) = server.split();
        let unsent = sessions.filter(|s| !s.quiet()).count();
        if unsent == 0 && handler.outbox_empty() {
            return;
        }
        if Instant::now() >= deadline {
            log::warn!(
                "exit: {unsent} terminal session(s) did not acknowledge the last messages in {}s",
                FLUSH_WAIT.as_secs()
            );
            return;
        }
    }
}

/// How long the stop waits for the exchange to confirm the withdrawals.
///
/// A cancel answers in about 0.3–0.45 s (measured 01.10 and 02.10) and takes no place in the
/// order budget. The calls of one market go one after another on its worker, the markets side by
/// side on twelve (`trading::start`): 65 entries over some forty markets were withdrawn in about
/// 30 s by the single worker of 02.10 and now take a few seconds, but a market with many entries
/// still goes one at a time, so the budget stays: 30 s withdraws some 75 entries on one market.
/// A stop that runs out of it still ends, and names what it left.
const STOP_WITHDRAW: Duration = Duration::from_secs(30);
/// How often the drain asks again about entries still live.
const SWEEP_EVERY: Duration = Duration::from_secs(1);

/// The rule the core leaves by (TInvestCore): no order that can open or grow
/// a position is left resting with no core to watch it. The exits stay, the
/// terminal's `Start` is shut before the first sweep, and the sweep runs on
/// every pass, since a pending entry may trigger meanwhile. A stop that cannot
/// reach the exchange still ends, and says what it leaves behind. The drain
/// also waits for exits still on their way — an entry that filled while it was
/// being withdrawn gets one, and leaving before it is out would leave the
/// position bare. `false` when the stop left orders with the exchange.
fn withdraw_entries(
    server: &mut Server<CoreHandler>,
    rx: &mpsc::Receiver<feed::FeedEvent>,
) -> bool {
    let deadline = Instant::now() + STOP_WITHDRAW;
    let mut held = {
        let (handler, _) = server.split();
        handler.begin_stop(engine::now_ms());
        let left = handler.live_entries();
        if left == 0 && handler.exits_in_flight() == 0 {
            return true;
        }
        left
    };
    // Once a second, not every pass: a cancel the exchange keeps refusing
    // must not turn into a call every few milliseconds for the whole budget.
    let mut swept = Instant::now();
    loop {
        server.step();
        let (handler, sessions) = server.split();
        handler.pump(sessions, rx);
        if !handler.can_withdraw() {
            let (left, exits) = (handler.live_entries(), handler.exits_in_flight());
            log::warn!(
                "stop: the order worker is gone — {left} entry order(s) stay with the \
                 exchange, {exits} exit(s) not confirmed: check them by hand"
            );
            return left == 0 && exits == 0;
        }
        if swept.elapsed() >= SWEEP_EVERY {
            swept = Instant::now();
            handler.sweep_entries(engine::now_ms());
        }
        let (left, exits) = (handler.live_entries(), handler.exits_in_flight());
        if left == 0 && exits == 0 {
            log::info!("stop: every entry order withdrawn");
            return true;
        }
        held = held.max(left);
        if Instant::now() >= deadline {
            log::warn!(
                "stop: {left} of {held} entry order(s) still with the exchange, {exits} exit(s) \
                 not confirmed after {}s — check them by hand",
                STOP_WITHDRAW.as_secs()
            );
            return false;
        }
    }
}

/// `0x21cF…1bb0`: enough of an address to tell wallets apart in a log line.
fn short(addr: &str) -> String {
    match (addr.get(..6), addr.get(addr.len().saturating_sub(4)..)) {
        (Some(head), Some(tail)) if addr.len() > 10 => format!("{head}…{tail}"),
        _ => addr.to_string(),
    }
}

/// The key file is the account: a group- or world-readable one is said out
/// loud. Not fatal — the file is the operator's, and so is the `chmod 600`.
#[cfg(unix)]
fn warn_if_shared(path: &str) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            eprintln!("account: {path} is mode {mode:o}, readable beyond its owner — chmod 600");
        }
    }
}

#[cfg(not(unix))]
fn warn_if_shared(_: &str) {}
