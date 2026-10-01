//! M0 step 1: read Aster's catalog and say what is in it.
//!
//! The lines this prints are the observation that the REST path, the catalog
//! mapping and the `GetMarketsList` encoding work against the real exchange —
//! not a test's idea of it. The `wire:` lines are the ones to read against an
//! independent parse of `/fapi/v1/exchangeInfo`: every number there is what the
//! terminal will be told.

use aster_core::aster::rest::Rest;
use aster_core::model::Catalog;
use moonproto::server::codec::engine;

fn main() -> std::process::ExitCode {
    let mut rest = Rest::new();

    let delta = match rest.sync_clock() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("clock: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let rtt = rest.last_rtt().map(|d| d.as_millis()).unwrap_or(0);
    println!("clock: delta {delta} ms, rtt {rtt} ms");

    let info = match rest.exchange_info() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("exchangeInfo: {e}");
            return std::process::ExitCode::FAILURE;
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

    // Both merges are best-effort on purpose: the catalog itself is what the
    // terminal cannot start without, while a missing turnover or funding figure
    // degrades one column and one pool ranking. A failure here is printed and
    // counted in the `catalog:` line (`funding n/596`), not fatal.
    match rest.ticker_24h_all() {
        Ok(rows) => {
            let filled = cat.apply_tickers(&rows);
            println!("tickers: {} rows -> {filled} markets", rows.len());
        }
        Err(e) => eprintln!("ticker/24hr: {e}"),
    }
    match rest.premium_index_all() {
        Ok(rows) => {
            let filled = cat.apply_funding(&rows);
            println!("funding: {} rows -> {filled} markets", rows.len());
        }
        Err(e) => eprintln!("premiumIndex: {e}"),
    }

    println!("{}", cat.summary());

    // The wire rows themselves, encoded exactly as `GetMarketsList` sends them.
    // Encoding the whole catalog here is the observation that the encoder runs
    // over all 596 real rows, and the sample lines are the fields that were
    // inherited as MOEX zeros — they are read off the built rows, not off the
    // catalog, so the line says what the terminal would receive.
    let specs = cat.specs();
    let payload = engine::write_markets_list(&specs);
    println!(
        "wire: {} rows, GetMarketsList {} bytes",
        specs.len(),
        payload.len()
    );
    for sample in ["BTCUSDT", "1000SHIBUSDT", "TONUSDT", "MBLUSDT"] {
        let Some(s) = specs.iter().find(|s| s.symbol == sample) else {
            println!("wire: {sample} not in catalog");
            continue;
        };
        println!(
            "wire: {} coin {} canonic {} futures_type {} lev {}x band {}..{} price {}..{} \
             k1000 {} delivery {} funding {} vol {:.0}",
            s.symbol,
            s.currency,
            s.currency_canonic,
            s.futures_type.name(),
            s.max_leverage,
            s.multiplier_down,
            s.multiplier_up,
            s.min_price,
            s.max_price,
            s.k1000,
            s.delivery_time_ms
                .map_or_else(|| "none".to_string(), |ms| ms.to_string()),
            s.funding.map_or_else(
                || "none".to_string(),
                |f| format!("{:+.6}% at {}", f.rate_pct, f.time_ms)
            ),
            s.volume,
        );
    }

    if let Some(usage) = rest.usage().weight_1m {
        println!("usage: weight-1m {usage}");
    }
    std::process::ExitCode::SUCCESS
}
