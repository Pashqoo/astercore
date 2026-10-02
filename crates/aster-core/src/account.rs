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
//! is: it is what MoonTerminal was shown by that core. Between two periods the
//! account's user-data stream wakes the reader: any event on it — a fill, an
//! order placed or cancelled, a funding fee — is a reason to read now
//! ([`user_stream`]). The events themselves are not applied: `ACCOUNT_UPDATE`
//! carries no available balance, and an order that only rests moves the free
//! margin without one. The REST pair stays the one source of the snapshot,
//! the stream only makes it timely, and the period stays because the
//! unrealized PnL inside the equity moves with every mark price, which no
//! account event reports.

use std::collections::{BTreeMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use crate::aster::json::{Balance, PositionRisk, UserEvent};
use crate::aster::rest::{self, Rest};
use crate::aster::sign::{Network, Signer};
use crate::aster::ws;
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
/// How often the stream key is renewed: half of its 60 minutes (docs), so one
/// failed renewal still leaves the key half an hour to be renewed in.
pub const KEY_EVERY: Duration = Duration::from_secs(30 * 60);
/// How soon a failed key call is retried. The polling carries the account
/// meanwhile, so this is about the stream's timeliness, not the money.
pub const KEY_RETRY: Duration = Duration::from_secs(60);
/// A read woken by the stream waits this long after the event, so that the
/// burst one fill makes (an order update, then the account's) is read once.
pub const SETTLE: Duration = Duration::from_millis(250);
/// Woken reads are at least this far apart: 30 reads a minute at weight 10
/// is 300 of the 2400 the IP may spend, however busy the account gets.
pub const MIN_GAP: Duration = Duration::from_secs(2);
/// How long after a session's end the account is read: the key call and the
/// handshake of the next session, measured 01.10 at 0.3 s and 0.83 s, with
/// room to spare.
pub const REOPEN: Duration = Duration::from_secs(3);
/// The longest the stream thread waits before reopening a failed session.
const BACKOFF_MAX: Duration = Duration::from_secs(60);

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
    // The answer carries a row per symbol, flat ones included: none at all is a short answer, not
    // a flat account, and a flat account would close every position the core keeps.
    if positions.is_empty() {
        return Err(ReadError::Shape("positionRisk: no rows".into()));
    }
    Account::from_rows(usdt.as_ref(), &positions, symbols).map_err(ReadError::Shape)
}

/// Re-read the account every [`REFRESH`], and soon after any event of the
/// account's user-data stream, on a thread named `aster-account`, sending each
/// read that differs from the last one sent. `first` is the read `main` made
/// at startup, already in the engine.
///
/// A failed read is logged, and the next tick re-measures the clock first — a
/// nonce outside the gateway's window is the failure a sleeping Mac makes. A
/// short outage leaves the last snapshot in place, stale but true; once the
/// reads have failed for [`STALE_AFTER`] the snapshot is withdrawn (`None`),
/// and the terminal shows the money as unknown rather than a balance that
/// silently stopped moving. The first good read brings it back. A panic is
/// sent as [`FeedEvent::Lost`], and the core leaves on it like on any dead
/// feed.
///
/// Every signed call stays on this thread, the stream key's included: one
/// signer, one nonce sequence. The stream itself is read on its own thread
/// ([`user_stream`]), which this one hands the key to.
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
                let (wake_tx, wake_rx) = mpsc::channel();
                let slot = Arc::new(Slot::default());
                user_stream(signer.network(), Arc::clone(&slot), wake_tx, tx.clone());
                let mut stream = Stream {
                    wake: Some(wake_rx),
                    slot,
                    key: None,
                    key_due: Instant::now(),
                };
                let mut last = Some(first);
                let mut failing_since: Option<Instant> = None;
                let mut clock_due = Instant::now() + CLOCK_EVERY;
                let mut last_read = Instant::now();
                let mut read_due = last_read + REFRESH;
                loop {
                    let due = read_due.min(stream.key_due);
                    if let Some(woken) = stream.wait(due) {
                        let after = match woken {
                            // The first event of a burst sets the read; the
                            // rest of the burst lands inside its settle time.
                            Wake::Event => SETTLE,
                            // Read once the next session is likely open, so
                            // that the read covers what the gap missed. After
                            // a failed session the reopen waits its backoff
                            // too; what lands after this read then waits for
                            // the period.
                            Wake::Ended => {
                                stream.key_due = Instant::now();
                                REOPEN
                            }
                        };
                        // While the reads fail, the stream does not make them
                        // more frequent: the period's pace is the backoff.
                        if failing_since.is_none() {
                            let soon = (Instant::now() + after).max(last_read + MIN_GAP);
                            read_due = read_due.min(soon);
                        }
                        continue;
                    }
                    if Instant::now() >= stream.key_due && !stream.renew(&mut rest, &mut signer) {
                        // A nonce outside the window is the likeliest reason,
                        // as for a read.
                        clock_due = Instant::now();
                    }
                    if Instant::now() < read_due {
                        continue;
                    }
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
                    last_read = Instant::now();
                    read_due = last_read + REFRESH;
                    let send = match read(&mut rest, &mut signer, &symbols) {
                        Ok(now) => {
                            if let Some(since) = failing_since.take() {
                                log::info!(
                                    "account: read again after {} s of failures",
                                    since.elapsed().as_secs()
                                );
                            }
                            if last.as_ref() == Some(&now) {
                                if tx.send(FeedEvent::AccountRead(now)).is_err() {
                                    return;
                                }
                                None
                            } else {
                                Some(Some(now))
                            }
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

/// What the stream thread tells the account thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// An event arrived: read the account soon.
    Event,
    /// The session ended — rotated, failed, expired or told to stop. The
    /// thread has emptied the slot and opens the next session on the key the
    /// next `POST` puts there; the account is read once that session is
    /// likely open, since events may have been missed while it was down.
    Ended,
}

/// The key the stream thread opens its next session on, and the stop of the
/// session it has open. One slot, not a queue: only the newest key matters.
/// Both halves change under the one lock, so a key placed with a stop either
/// ends the session that was open when it was placed, or is the key the next
/// session takes with the stop cleared — never a stop that lands on the
/// session of the very key it came with.
#[derive(Default)]
struct Slot {
    key: Mutex<Option<String>>,
    placed: Condvar,
    stop: AtomicBool,
}

impl Slot {
    fn lock(&self) -> MutexGuard<'_, Option<String>> {
        self.key.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The account thread's side of the user-data stream: the key it last got
/// from the exchange and when to ask again.
struct Stream {
    /// `None` once the stream thread is gone; the account is then polled.
    wake: Option<Receiver<Wake>>,
    slot: Arc<Slot>,
    key: Option<String>,
    key_due: Instant,
}

impl Stream {
    /// Wait for a wake-up until `due`: `Some` if one came first.
    fn wait(&mut self, due: Instant) -> Option<Wake> {
        let left = due.saturating_duration_since(Instant::now());
        let Some(rx) = &self.wake else {
            thread::sleep(left);
            return None;
        };
        match rx.recv_timeout(left) {
            Ok(w) => Some(w),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                log::error!("user: the stream thread is gone — the account is polled only");
                self.wake = None;
                None
            }
        }
    }

    /// Ask for the account's active key and place it in the slot. A changed
    /// key under an open session ends that session too: its own key is dead,
    /// and the stream would never say so (`Rest::listen_key`). `false` when
    /// the call failed; it is retried in [`KEY_RETRY`]. Nothing is asked once
    /// the stream thread is gone: nobody would read the key.
    fn renew(&mut self, rest: &mut Rest, signer: &mut Signer) -> bool {
        if self.wake.is_none() {
            self.key_due = Instant::now() + KEY_EVERY;
            return true;
        }
        match rest.listen_key(signer) {
            Ok(key) => {
                let changed = self.key.as_ref().is_some_and(|k| *k != key);
                if changed {
                    log::info!("user: the stream key changed");
                }
                {
                    let mut slot = self.slot.lock();
                    *slot = Some(key.clone());
                    if changed {
                        self.slot.stop.store(true, Ordering::Relaxed);
                    }
                }
                self.slot.placed.notify_one();
                self.key = Some(key);
                self.key_due = Instant::now() + KEY_EVERY;
                true
            }
            Err(e) => {
                log::warn!("user: stream key: {e}");
                self.key_due = Instant::now() + KEY_RETRY;
                false
            }
        }
    }
}

/// Read the account's user-data stream on a thread named `aster-user`, one
/// session per key placed in `slot`, and tell the account thread of every
/// event and of every session's end; each order's event also goes to the
/// engine (`feed`), for the order model.
///
/// The thread holds no credentials: the key is all the stream needs, and it
/// only reads. At every session's end it empties the slot before it says so,
/// so the next session opens on a key the exchange named no earlier than the
/// renewal in flight when this one ended — never on one placed during the
/// session, which may have expired since. A failed
/// session is reopened after a backoff that doubles to [`BACKOFF_MAX`] and
/// starts over after a session that lived longer than that; one that ended on
/// its own — rotated, stopped, its key expired — at once.
fn user_stream(network: Network, slot: Arc<Slot>, wake: Sender<Wake>, feed: Sender<FeedEvent>) {
    thread::Builder::new()
        .name("aster-user".into())
        .spawn(move || {
            let beat = ws::Beat::new();
            let mut backoff = Duration::from_secs(1);
            loop {
                let key = {
                    let mut placed = slot.lock();
                    loop {
                        if let Some(key) = placed.take() {
                            slot.stop.store(false, Ordering::Relaxed);
                            break key;
                        }
                        placed = slot.placed.wait(placed).unwrap_or_else(|e| e.into_inner());
                    }
                };
                let opened = Instant::now();
                let url = format!("wss://{}/ws/{key}", network.ws_host());
                let was_open = std::cell::Cell::new(false);
                let res = ws::guarded(|| {
                    ws::run_at(
                        network.ws_host(),
                        url,
                        ws::IDLE_LIMIT,
                        &slot.stop,
                        &beat,
                        // Said once the handshake is through, not before: a refused one would
                        // otherwise start the open-orders read (weight 40) on every attempt.
                        || {
                            log::info!(
                                "user: stream open on {} (session {})",
                                network.name(),
                                beat.sessions()
                            );
                            // Events between the last session and this one may be lost: the
                            // engine reads the open orders again (`TradeCommand::OpenOrders`).
                            was_open.set(true);
                            let _ = feed.send(FeedEvent::UserStreamOpen);
                        },
                        |text| {
                            let event = user_event(text);
                            log_event(&event);
                            // The order model's report of the order (`orders.rs`).
                            if let UserEvent::Order(o) = &event {
                                let _ = feed.send(FeedEvent::UserOrder(o.order.clone()));
                            }
                            if event == UserEvent::Expired {
                                slot.stop.store(true, Ordering::Relaxed);
                            }
                            let _ = wake.send(Wake::Event);
                        },
                    )
                });
                slot.lock().take();
                // Told even when the handshake was refused (`was_open` false): `Ended` is what
                // makes the account thread place a NEW listenKey, and a refused one is most
                // likely an expired one. The backoff below paces the attempts, and with them
                // this POST and read.
                if was_open.get() {
                    let _ = feed.send(FeedEvent::UserStreamClosed);
                }
                if wake.send(Wake::Ended).is_err() {
                    return;
                }
                if opened.elapsed() > BACKOFF_MAX {
                    backoff = Duration::from_secs(1);
                }
                match res {
                    Ok(()) => backoff = Duration::from_secs(1),
                    Err(e) => {
                        log::warn!("user: {e}; reopening in {} s", backoff.as_secs());
                        thread::sleep(backoff);
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                    }
                }
            }
        })
        .expect("spawn");
}

/// One frame of the user-data stream as an event. A frame that does not read
/// as the docs describe — fields of another shape, or not JSON at all — is
/// `Other`: it is still an event, and still wakes the reader.
fn user_event(text: &str) -> UserEvent {
    serde_json::from_str(text).unwrap_or_else(|e| {
        log::warn!(
            "user: unreadable event ({e}): {}",
            text.chars().take(160).collect::<String>()
        );
        UserEvent::Other
    })
}

fn log_event(event: &UserEvent) {
    match event {
        UserEvent::Account(a) => log::info!("account: update ({})", a.update.reason),
        UserEvent::Order(o) => {
            let o = &o.order;
            log::info!(
                "order: {} {} {} {} {}/{} {}@{} filled {} last {} client {}",
                o.symbol,
                o.id,
                o.side,
                o.kind,
                o.execution,
                o.status,
                o.qty,
                o.price,
                o.filled,
                o.last_price,
                o.client_id
            );
        }
        UserEvent::MarginCall => log::warn!("account: MARGIN CALL"),
        UserEvent::Expired => log::info!("user: the stream key expired"),
        UserEvent::Other => log::debug!("user: event"),
    }
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
    fn an_unreadable_event_still_wakes_and_known_ones_decode() {
        let odd = r#"{"e":"ACCOUNT_UPDATE","a":{"m":7}}"#;
        assert_eq!(user_event(odd), UserEvent::Other);
        let exp = r#"{"e":"listenKeyExpired","E":1}"#;
        assert_eq!(user_event(exp), UserEvent::Expired);
        assert_eq!(user_event(r#"{"result":null,"id":1}"#), UserEvent::Other);
        assert_eq!(user_event("not json"), UserEvent::Other);
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
