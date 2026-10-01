//! The account the terminal shows: USDT money and open positions, read by a
//! thread of its own and handed to the UDP loop as [`FeedEvent::Account`].
//!
//! Two signed calls a refresh, `/fapi/v3/balance` and `/fapi/v3/positionRisk`
//! (weight 5 each by the docs: 40 a minute at [`REFRESH`], against 2400), on a
//! client of the signer's own network, whose clock is measured against that
//! same gateway on start and every [`CLOCK_EVERY`] — a nonce drifts out of the
//! gateway's ±60 s otherwise, on a Mac that sleeps.
//!
//! The period is TInvestCore's `POSITIONS_PERIOD` (`trading.rs`), ported as
//! is: it is what MoonTerminal was shown by that core. A user-data stream
//! (`ACCOUNT_UPDATE`) replaces the polling for timeliness — the next step of
//! M2, `PLAN.md`.

use std::collections::{BTreeMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use crate::aster::json::{Balance, PositionRisk};
use crate::aster::rest::{self, Rest};
use crate::aster::sign::Signer;
use crate::feed::FeedEvent;
use crate::model::QUOTE;

/// How often balance and positions are re-read.
pub const REFRESH: Duration = Duration::from_secs(15);
/// How often the clock is re-measured against the signer's gateway.
pub const CLOCK_EVERY: Duration = Duration::from_secs(600);
/// How long reads may keep failing before the last snapshot is withdrawn,
/// counted from the first failure: the fifth failed read in a row, some 75 s
/// after the last good one (more when the calls run into their timeout). One
/// dropped call or a gateway hiccup does not blank the terminal's money; a
/// revoked key or a changed answer does in about a minute.
pub const STALE_AFTER: Duration = Duration::from_secs(60);

/// One read of the account, in USDT: what `TBalanceFull` carries.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Account {
    /// `availableBalance` of the USDT row: margin free for a new order.
    pub free: f64,
    /// Wallet balance plus the unrealized PnL of every USDT position, isolated
    /// ones included — the equity. `crossUnPnl` is not used: it leaves out the
    /// isolated positions, whose wallets the balance does include.
    pub equity: f64,
    /// Open positions on this core's markets, one per symbol, by symbol.
    pub positions: Vec<Position>,
}

/// A symbol's net open position.
#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub symbol: String,
    /// Signed base quantity: long > 0.
    pub size: f64,
    /// Entry price, or 0 when there is no single one (both hedge legs open).
    pub entry: f64,
}

/// Which symbols count for what. The two differ: a position becomes a row
/// only on one of this core's markets, the catalog's, which the terminal can
/// show; its unrealized PnL counts toward the USDT equity whenever the symbol is
/// margined in USDT — a settling or delisted perpetual still carries margin in
/// the wallet, and its PnL belongs beside it.
#[derive(Debug, Clone, Default)]
pub struct Symbols {
    /// The catalog's markets.
    pub rows: HashSet<String>,
    /// Every symbol of `exchangeInfo` margined in USDT, whatever its status. A
    /// listing after the start joins it on the next start.
    pub usdt: HashSet<String>,
}

impl Account {
    /// The account from one pair of answers: the USDT row of the balance, if
    /// there was one, and every position row.
    ///
    /// No USDT row is an error, not an empty wallet: a figure of 0 shown to
    /// the trader is a claim about the money the exchange did not make.
    pub fn from_rows(
        usdt: Option<&Balance>,
        positions: &[PositionRisk],
        symbols: &Symbols,
    ) -> Result<Self, String> {
        let usdt = usdt.ok_or_else(|| format!("no {QUOTE} row in the balance"))?;
        let unrealized: f64 = positions
            .iter()
            .filter(|p| symbols.usdt.contains(&p.symbol))
            .map(|p| p.unrealized)
            .sum();
        // Net per symbol. One-way mode gives one row a symbol; hedge mode two,
        // each signed and with its own entry. The wire carries one position a
        // market, so two open legs are netted and have no entry price to
        // report — the terminal then shows the size without a live PnL rather
        // than a PnL against a price neither leg was opened at. Legs that
        // cancel out are no row; their PnL is real and stays in the equity.
        let mut net: BTreeMap<&str, (f64, f64, usize)> = BTreeMap::new();
        for p in positions
            .iter()
            .filter(|p| p.amount != 0.0 && symbols.rows.contains(&p.symbol))
        {
            let (size, entry, legs) = net.entry(p.symbol.as_str()).or_default();
            *size += p.amount;
            *entry = p.entry_price;
            *legs += 1;
        }
        let positions = net
            .into_iter()
            .filter(|(_, (size, _, _))| *size != 0.0)
            .map(|(symbol, (size, entry, legs))| Position {
                symbol: symbol.to_string(),
                size,
                entry: if legs == 1 { entry } else { 0.0 },
            })
            .collect();
        Ok(Self {
            free: usdt.available,
            equity: usdt.balance + unrealized,
            positions,
        })
    }

    /// Margin that is not free: the equity less the available balance, never
    /// below zero, so that free + locked is the equity the terminal shows.
    pub fn locked(&self) -> f64 {
        (self.equity - self.free).max(0.0)
    }
}

/// Why a read failed: the call that failed, or what in its answer was wrong.
#[derive(Debug)]
pub enum ReadError {
    Rest(rest::Error),
    Shape(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rest(e) => write!(f, "{e}"),
            Self::Shape(e) => f.write_str(e),
        }
    }
}

impl From<rest::Error> for ReadError {
    fn from(e: rest::Error) -> Self {
        Self::Rest(e)
    }
}

/// One read: balance, then positions.
pub fn read(rest: &mut Rest, signer: &mut Signer, symbols: &Symbols) -> Result<Account, ReadError> {
    let usdt = rest.balance(signer, QUOTE)?;
    let positions = rest.position_risk(signer, |s| symbols.usdt.contains(s))?;
    Account::from_rows(usdt.as_ref(), &positions, symbols).map_err(ReadError::Shape)
}

/// Re-read the account every [`REFRESH`] on a thread named `aster-account`,
/// sending each read that differs from the last one sent. `first` is the read
/// `main` made at startup, already in the engine.
///
/// A failed read is logged, and the next tick re-measures the clock first — a
/// nonce outside the gateway's window is the failure a sleeping Mac makes. A
/// short outage leaves the last snapshot in place, stale but true; once the
/// reads have failed for [`STALE_AFTER`] the snapshot is withdrawn (`None`),
/// and the terminal shows the money as unknown rather than a balance that
/// silently stopped moving. The first good read brings it back. A panic is
/// sent as [`FeedEvent::Lost`], and the core leaves on it like on any dead
/// feed.
pub fn start(
    mut rest: Rest,
    mut signer: Signer,
    symbols: Symbols,
    first: Account,
    tx: Sender<FeedEvent>,
) {
    thread::Builder::new()
        .name("aster-account".into())
        .spawn(move || {
            let lost = tx.clone();
            let run = panic::catch_unwind(AssertUnwindSafe(move || {
                let mut last = Some(first);
                let mut failing_since: Option<Instant> = None;
                let mut clock_due = Instant::now() + CLOCK_EVERY;
                loop {
                    thread::sleep(REFRESH);
                    if Instant::now() >= clock_due {
                        // A failed measurement keeps the old delta and is
                        // retried on the next tick, not in ten minutes.
                        match rest.sync_clock() {
                            Ok(delta) => {
                                log::debug!("account: clock delta {delta} ms");
                                clock_due = Instant::now() + CLOCK_EVERY;
                            }
                            Err(e) => log::warn!("account: clock: {e}"),
                        }
                    }
                    let send = match read(&mut rest, &mut signer, &symbols) {
                        Ok(now) => {
                            if let Some(since) = failing_since.take() {
                                log::info!(
                                    "account: read again after {} s of failures",
                                    since.elapsed().as_secs()
                                );
                            }
                            (last.as_ref() != Some(&now)).then_some(Some(now))
                        }
                        Err(e) => {
                            log::warn!("account: {e}");
                            clock_due = Instant::now();
                            let since = *failing_since.get_or_insert_with(Instant::now);
                            (last.is_some() && since.elapsed() >= STALE_AFTER).then(|| {
                                log::error!(
                                    "account: no read for {} s — the terminal is told the \
                                     money is unknown until the next one",
                                    since.elapsed().as_secs()
                                );
                                None
                            })
                        }
                    };
                    if let Some(next) = send {
                        last = next.clone();
                        if tx.send(FeedEvent::Account(next)).is_err() {
                            return;
                        }
                    }
                }
            }));
            if run.is_err() {
                let _ = lost.send(FeedEvent::Lost("the account reader"));
            }
        })
        .expect("spawn");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bal(asset: &str, balance: f64, available: f64) -> Balance {
        Balance {
            asset: asset.into(),
            balance,
            available,
        }
    }

    fn pos(symbol: &str, amount: f64, entry: f64, unrealized: f64) -> PositionRisk {
        PositionRisk {
            symbol: symbol.into(),
            amount,
            entry_price: entry,
            unrealized,
        }
    }

    fn symbols(rows: &[&str], usdt: &[&str]) -> Symbols {
        Symbols {
            rows: rows.iter().map(|s| s.to_string()).collect(),
            usdt: usdt.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn equity_adds_every_usdt_position_and_rows_are_the_open_ones() {
        let syms = symbols(
            &["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            &["BTCUSDT", "ETHUSDT", "SOLUSDT", "OLDUSDT"],
        );
        let a = Account::from_rows(
            Some(&bal("USDT", 1000.0, 700.0)),
            &[
                pos("BTCUSDT", 0.01, 60000.0, 12.5),
                pos("ETHUSDT", 0.0, 0.0, 0.0),
                pos("SOLUSDT", -2.0, 150.0, -3.0),
                // Margined in USD1: neither a row nor in the USDT equity.
                pos("BTCUSD1", 1.0, 60000.0, 99.0),
                // Settling, out of the catalog: no row, but its PnL is USDT.
                pos("OLDUSDT", 5.0, 1.0, 0.5),
            ],
            &syms,
        )
        .unwrap();
        assert_eq!((a.free, a.equity), (700.0, 1010.0));
        assert_eq!(a.locked(), 310.0);
        assert_eq!(
            a.positions,
            vec![
                Position {
                    symbol: "BTCUSDT".into(),
                    size: 0.01,
                    entry: 60000.0
                },
                Position {
                    symbol: "SOLUSDT".into(),
                    size: -2.0,
                    entry: 150.0
                },
            ]
        );
    }

    #[test]
    fn hedge_legs_are_netted_and_two_open_legs_have_no_entry() {
        let all = ["BTCUSDT", "ETHUSDT", "XRPUSDT"];
        let a = Account::from_rows(
            Some(&bal("USDT", 100.0, 100.0)),
            &[
                pos("BTCUSDT", 0.0, 0.0, 0.0), // LONG leg, flat
                pos("BTCUSDT", -0.5, 61000.0, 1.0),
                pos("ETHUSDT", 2.0, 3000.0, 0.0),
                pos("ETHUSDT", -0.5, 3100.0, 0.0),
                pos("XRPUSDT", 1.0, 2.0, 0.0),
                pos("XRPUSDT", -1.0, 2.1, 0.0),
            ],
            &symbols(&all, &all),
        )
        .unwrap();
        assert_eq!(a.equity, 101.0, "cancelled-out legs keep their PnL");
        assert_eq!(
            a.positions,
            vec![
                Position {
                    symbol: "BTCUSDT".into(),
                    size: -0.5,
                    entry: 61000.0
                },
                Position {
                    symbol: "ETHUSDT".into(),
                    size: 1.5,
                    entry: 0.0
                },
            ],
            "a flat leg leaves the other's entry; two open legs have none; \
             legs that cancel out are no row"
        );
    }

    #[test]
    fn no_usdt_row_is_an_error_not_an_empty_wallet() {
        let e = Account::from_rows(None, &[], &Symbols::default()).unwrap_err();
        assert!(e.contains("no USDT row"), "{e}");
    }

    #[test]
    fn equity_below_free_locks_nothing() {
        let a = Account {
            free: 100.0,
            equity: 90.0,
            positions: Vec::new(),
        };
        assert_eq!(a.locked(), 0.0);
    }
}
