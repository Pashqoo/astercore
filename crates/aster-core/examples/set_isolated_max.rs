//! Sets every trading market of the account to ISOLATED margin at the highest leverage its
//! brackets allow. A dry run unless `--apply` is given: it reads the account (signed reads only)
//! and prints what it would send.
//!
//! Run from a directory that has the key file (or name it with `ASTER_API_KEY_FILE`):
//!   cargo run --release -p aster-core --example set_isolated_max            # dry run
//!   cargo run --release -p aster-core --example set_isolated_max -- --apply
//!
//! `ASTER_NET` is `mainnet` (default) or `testnet`, as for the core. The key file is read by the
//! library and never printed.
//!
//! A symbol with an open position or a resting order is left alone and listed (the exchange
//! refuses the margin change there, and a leverage change would move a live position's
//! liquidation); run it again when those are flat — it only sends what is still different. A
//! rate limit or a ban stops the run. After an apply the account is read again and what is
//! still to do is printed, which is also how a call whose outcome was unknown (a timeout) shows.
//!
//! Exit code of an `--apply` (a dry run exits 0): 0 all set, 2 only blocked symbols are left,
//! 1 something that could be set is not, the run stopped on a limit, or the read-back failed.
//!
//! The core may keep running on the same wallet. This tool has its own nonce sequence, as a
//! second process must; both draw nonces from the microsecond clock, so a collision needs the
//! same microsecond. The core shows the new leverage on its next account read (within 15 s, or
//! at once on a stream event) — a terminal that already knows the markets keeps the maximum it
//! was told before (see PLAN.md, "Плечо").

use std::collections::HashSet;
use std::process::ExitCode;

use aster_core::aster::rest::{Error, Rest};
use aster_core::aster::sign::{Credentials, Network, Signer};
use aster_core::model::Catalog;
use aster_core::setup::{self, Plan};

fn read_plans(
    rest: &mut Rest,
    signer: &mut Signer,
    symbols: &[String],
) -> Result<Vec<Plan>, (&'static str, Error)> {
    let keep = |s: &str| symbols.iter().any(|x| x == s);
    let rows = rest
        .position_risk(signer, keep)
        .map_err(|e| ("positionRisk", e))?;
    let brackets = rest
        .leverage_brackets(signer, keep)
        .map_err(|e| ("leverageBracket", e))?;
    let orders = rest.open_orders(signer).map_err(|e| ("openOrders", e))?;
    // A short answer is not an account: a symbol missing from it would read as flat.
    if rows.is_empty() || brackets.is_empty() {
        return Err((
            "answers",
            Error::Decode(format!(
                "{} position rows, {} bracket rows",
                rows.len(),
                brackets.len()
            )),
        ));
    }
    let ordered: HashSet<String> = orders.into_iter().map(|o| o.symbol).collect();
    Ok(setup::plans(symbols, &rows, &brackets, &ordered))
}

fn summary(plans: &[Plan]) -> String {
    let n = |f: &dyn Fn(&Plan) -> bool| plans.iter().filter(|p| f(p)).count();
    format!(
        "{} markets: margin to set {}, leverage to set {}, already right {}, blocked {}",
        plans.len(),
        n(&|p| p.margin.is_some()),
        n(&|p| p.leverage.is_some()),
        n(&|p| p.is_noop()),
        n(&|p| p.block.is_some()),
    )
}

fn main() -> ExitCode {
    let apply = std::env::args().any(|a| a == "--apply");
    let key_path = std::env::var("ASTER_API_KEY_FILE")
        .ok()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| "asterkey".into());
    let network = match std::env::var("ASTER_NET") {
        Ok(n) if !n.trim().is_empty() => match Network::parse(&n) {
            Some(net) => net,
            None => {
                eprintln!("ASTER_NET is neither mainnet nor testnet: {n}");
                return ExitCode::FAILURE;
            }
        },
        _ => Network::Mainnet,
    };
    let creds = match Credentials::load(&key_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{key_path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut signer = Signer::new(creds, network);
    let mut rest = Rest::on(network);
    if let Err(e) = rest.sync_clock() {
        eprintln!("clock: {e}");
        return ExitCode::FAILURE;
    }
    let info = match rest.exchange_info() {
        Ok(i) => i,
        Err(e) => {
            eprintln!("exchangeInfo: {e}");
            return ExitCode::FAILURE;
        }
    };
    let cat = Catalog::build(&info);
    let symbols: Vec<String> = cat.trading().map(|m| m.symbol.clone()).collect();
    let plans = match read_plans(&mut rest, &mut signer, &symbols) {
        Ok(p) => p,
        Err((what, e)) => {
            eprintln!("{what}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("{}", summary(&plans));
    for p in plans.iter().filter(|p| !p.is_noop()) {
        match p.block {
            Some(b) => println!("  {}: left alone, {}", p.symbol, b.text()),
            None => println!(
                "  {}: margin {}, leverage {}",
                p.symbol,
                p.margin.map_or("ok".into(), |k| format!("-> {k}")),
                p.leverage.map_or("ok".into(), |l| format!("-> {l}x")),
            ),
        }
    }
    if !apply {
        println!("dry run: nothing was sent; add --apply to send");
        return ExitCode::SUCCESS;
    }
    let report = setup::apply(&mut rest, &mut signer, &plans);
    println!(
        "sent: margin set {}, leverage set {}, skipped {}, failed {}",
        report.margin_set,
        report.leverage_set,
        report.skipped.len(),
        report.failed.len()
    );
    for (s, why) in &report.skipped {
        println!("  skipped {s}: {why}");
    }
    for (s, why) in &report.failed {
        println!("  FAILED {s}: {why}");
    }
    if let Some(why) = &report.aborted {
        println!("STOPPED on a limit, the rest was not sent: {why}");
    }
    // After a rate limit or a ban nothing more is asked of the exchange: the run is over.
    if report.aborted.is_some() {
        return ExitCode::FAILURE;
    }
    // The state as the exchange now has it: what a timed-out call did, and what is left. The
    // exit code is read from it, not from the calls' outcomes.
    let after = match read_plans(&mut rest, &mut signer, &symbols) {
        Ok(after) => after,
        Err((what, e)) => {
            println!("after: could not read back ({what}: {e})");
            return ExitCode::FAILURE;
        }
    };
    println!("after: {}", summary(&after));
    let mut undone = false;
    for p in after.iter().filter(|p| !p.is_noop()) {
        match p.block {
            Some(b) => println!("  left: {}: {}", p.symbol, b.text()),
            None => {
                undone = true;
                println!("  STILL TO DO: {}", p.symbol);
            }
        }
    }
    if undone {
        ExitCode::FAILURE
    } else if after.iter().any(|p| p.block.is_some()) {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}
