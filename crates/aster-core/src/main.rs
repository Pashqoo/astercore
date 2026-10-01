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
use aster_core::aster::rest::Rest;
use aster_core::aster::sign::{Credentials, LoadError, Network, Signer};
use aster_core::engine::{self, CoreHandler, FeedLink};
use aster_core::feed;
use aster_core::key_store;
use aster_core::load::Load;
use aster_core::model::{Catalog, QUOTE};
use aster_core::order_store::OrderStore;
use aster_core::prices;
use aster_core::reports::Reports;
use aster_core::stderr_log;
use aster_core::strategies::Strategies;
use aster_core::stream_health::StreamHealth;
use aster_core::trading;
use moonproto::server::codec::engine as engine_codec;
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
const REPORTS_FILE: &str = "data/reports.jsonl";

fn main() -> ExitCode {
    stderr_log::init();

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
    // The export IS the private key, so it goes to stdout and never to the log
    // file: this line is read once, off the console, and pasted into the
    // terminal (`AGENTS.md`, `## Secrets`).
    println!(
        "Astercore key {} port {} mode {}\n{}",
        key.rnd,
        key.port,
        key.transport_mode.name(),
        key.export()
    );
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
    let handler = CoreHandler::new(
        1,
        engine::ACCOUNT_PLACEHOLDER.to_string(),
        cat,
        Strategies::new(Some(PathBuf::from(STRATEGIES_FILE)), now),
    )
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
    if let Some((account_rest, signer, first)) = account {
        // The order worker signs with a clone of the account's signer: one
        // wallet, one nonce sequence (`Signer`). Its own client, on the same
        // network, measured against the same gateway.
        let mut orders_rest = Rest::on(signer.network());
        orders_rest.set_clock_delta_ms(account_rest.clock_delta_ms());
        let trading = trading::start(orders_rest, signer.clone(), grids, ev_tx.clone());
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

    // Every way out of THIS LOOP goes through the stop below: the entries are
    // withdrawn, the terminals told, the orders written. A signal does not
    // come through here at all yet — SIGINT/SIGTERM end the process at once,
    // and the entries stay with the exchange (`PLAN.md`, M4: signals).
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
        if handler.shutdown_requested() {
            break ExitCode::SUCCESS;
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
    // A shutdown that left orders with the exchange did not end cleanly.
    if clean {
        code
    } else {
        ExitCode::FAILURE
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
/// The worker paces its calls 100 ms apart and a cancel answers in about
/// 0.3 s (measured 01.10: `/fapi/v1/time` 0.30 s; the cancels of the live test
/// of 01.10 came back within a second), so some 2.5 a second: 30 s withdraws
/// about 75 entries, more than this core has had at once. A stop that runs out
/// of it still ends, and names what it left.
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
