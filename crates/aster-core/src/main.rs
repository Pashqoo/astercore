//! The core: read Aster's catalog, then speak MoonProto to the terminal.
//!
//! The lines it prints at startup are the observation that the REST path and
//! the catalog mapping work against the real exchange — `clock:`, `limits:`,
//! `catalog:` and `wire:`, each number of them meant to be read against an
//! independent parse of `/fapi/v1/exchangeInfo` (`AGENTS.md`, "Канал
//! наблюдения"). After them it binds the UDP socket and serves: the catalog
//! goes to the terminal once per session, the prices every couple of seconds.

use std::process::ExitCode;
use std::sync::mpsc::TryRecvError;

use aster_core::aster::rest::Rest;
use aster_core::engine::{self, CoreHandler};
use aster_core::key_store;
use aster_core::model::Catalog;
use aster_core::prices;
use aster_core::stderr_log;
use aster_core::strategies::Strategies;
use moonproto::server::codec::engine as engine_codec;
use moonproto::server::Server;

/// One past TInvestCore's 3100: the two cores are expected to run on the same
/// machine, and a port collision would show up as a terminal that cannot
/// connect rather than as an error.
const DEFAULT_PORT: u16 = 3101;
/// Feed snapshots applied per UDP-loop iteration, so receiving is never starved
/// by a backlog the loop built up itself.
const APPLY_BATCH: usize = 16;

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
    // The clock first: every signed call from M2 on adds this delta, and
    // `-1021 INVALID_TIMESTAMP` is what skipping it costs.
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
    if let Some(usage) = rest.usage().weight_1m {
        log::info!("usage: weight-1m {usage}");
    }

    // From here on every call to the exchange is made in the refresher's own
    // threads, off the UDP loop — the whole reason that module exists
    // (`prices.rs`). They carry their own clients and their first calls are one
    // period away, because the three answers above are as fresh as a call made
    // now would be.
    let feeds = prices::start();
    let handler = CoreHandler::new(
        1,
        engine::ACCOUNT_PLACEHOLDER.to_string(),
        cat,
        Strategies::new(),
    );
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

    // No stop path yet: there is nothing to put away on the way out — no
    // orders, no positions, no file the core writes while it runs. A SIGINT
    // ends it, and M4 brings the signal handling along with the state that
    // will need saving first (`PLAN.md`, M4).
    loop {
        server.step();
        // One dead refresher is as bad as both: the prices it fed would never
        // move again, and nothing else in the process would notice. Checked
        // before the snapshots, because a panicked feed has nothing left to
        // send and the loop must not keep serving what it last sent.
        if let Some(name) = feeds.dead() {
            log::error!("prices: the {name} refresher is gone — its prices would freeze");
            return ExitCode::FAILURE;
        }
        let (handler, _) = server.split();
        for _ in 0..APPLY_BATCH {
            match feeds.try_recv() {
                Ok(snap) => handler.apply(snap),
                Err(TryRecvError::Empty) => break,
                // The channel closes only when the LAST sender drops, so this
                // is the both-gone case and the check above catches the first
                // one. Kept because the two are different facts and this one is
                // the end of the exhaustive match, not a case that cannot
                // happen. Either way the core leaves rather than serving prices
                // that will never move again, and the exit code is what brings
                // it back. An outage that leaves the threads alive is the
                // refresher's own to report, and it clears the prices rather
                // than freezing them.
                Err(TryRecvError::Disconnected) => {
                    log::error!("prices: both refreshers are gone — prices would freeze");
                    return ExitCode::FAILURE;
                }
            }
        }
    }
}
