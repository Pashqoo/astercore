//! Aster order reports normalized to one shape, [`OrderUpdate`], whatever they
//! came from: the reply to a `POST`/`DELETE`/`GET /fapi/v3/order`, a row of
//! `openOrders`, or an `ORDER_TRADE_UPDATE` of the user-data stream. The order
//! model (`orders.rs`, ported from TInvestCore) reads only this shape, so it
//! never sees which of them a report was.
//!
//! Quantities are in lots — whole `stepSize` steps of the market — the unit the
//! model counts in (`Market::lot`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::aster::json::{OrderEvent, OrderReply};
use crate::aster::rest::{self, OrderRef, Rest};
use crate::aster::sign::Signer;
use crate::feed::FeedEvent;
use crate::model::on_grid;
use crate::orders::{Action, Leg, Op};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl ExecStatus {
    /// An Aster order status (`X` of the stream, `status` of a reply). An
    /// order the exchange expired (an IOC/GTX limit that did not rest, a
    /// market order's unfilled rest) ended without more fills, which is what
    /// a cancel means to the model. `NEW_INSURANCE` and `NEW_ADL` are the
    /// exchange's liquidation orders, never one of the core's; `None`.
    pub fn from_aster(status: &str) -> Option<Self> {
        Some(match status {
            "NEW" => Self::New,
            "PARTIALLY_FILLED" => Self::PartiallyFilled,
            "FILLED" => Self::Filled,
            "CANCELED" | "EXPIRED" => Self::Cancelled,
            "REJECTED" => Self::Rejected,
            _ => return None,
        })
    }

    pub fn is_final(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    /// `orderId`, as a string.
    pub exchange_id: String,
    /// `clientOrderId`: the key the core generated; may be another client's.
    pub request_id: String,
    /// The symbol.
    pub uid: String,
    pub status: ExecStatus,
    pub sell: bool,
    pub is_market: bool,
    pub lots_requested: i64,
    pub lots_executed: i64,
    /// Order price and average fill price (0 when unknown).
    pub price: f64,
    pub avg_price: f64,
    /// The reply to the core's own call, not a stream report: a stream
    /// rejection may precede the reply that settles the call.
    pub unary: bool,
    pub time_ms: i64,
    pub message: String,
}

/// A failure that may be the clock's: a nonce outside the window, or no answer at all.
fn clock_suspect(e: &rest::Error) -> bool {
    !matches!(e, rest::Error::Api { code, .. } if *code != CODE_NONCE_EXPIRED)
}

/// The same for a finished action's refusal code: the exchange's own refusal of the order (-2011
/// on a filled order, -2019) says nothing about the clock, and measuring it first would cost the
/// next order a round trip on the critical path.
fn clock_suspect_code(code: Option<i64>) -> bool {
    code.is_none() || code == Some(CODE_NONCE_EXPIRED)
}

/// The exchange refused the call for good: the request was read and turned
/// down, so it did not and will not take effect. A 5xx, a timeout or a
/// transport error leaves its fate unknown.
pub(crate) fn definitive(e: &rest::Error) -> bool {
    matches!(e, rest::Error::Api { status, .. } if (400..500).contains(status) && *status != 408)
}

/// The order worker's name in `FeedEvent::Lost`.
pub const WORKER: &str = "the order worker";
/// The thread that reads the account's open orders (`FeedEvent::Lost`): not the order path.
pub const READER: &str = "the open-orders reader";

/// What the entries may spend of the exchange's order budget (`ORDERS` of `exchangeInfo`: 300 per
/// 10 s, 1200 per minute): two thirds of it, the rest is the exits'. Only the calls that make an
/// order count (a post, and a replace for two); a cancel or a read does not.
const ENTRY_CAP_10S: usize = 200;
const ENTRY_CAP_1M: usize = 800;
/// Order workers. A market belongs to one of them (`shard`), so the calls on one market keep their
/// order — the exchange's own rules are per symbol: a resting entry that stands against an exit
/// must be gone before the reduce-only exit is placed (`-2022`) — and the calls of different
/// markets go out side by side. More workers than the markets a call can be stuck behind.
const WORKERS: usize = 12;
/// A clock measure that failed is tried again after this, not in front of every call.
const CLOCK_RETRY: Duration = Duration::from_secs(5);
/// A worker with nothing to do wakes this often to look again: a push wakes it at once, this is
/// only the floor under a lost wake-up.
const IDLE_WAIT: Duration = Duration::from_secs(1);

/// The market grid a call is written on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grid {
    /// `stepSize`: one lot.
    pub step: f64,
    /// `tickSize`.
    pub tick: f64,
}

pub enum TradeCommand {
    /// Exchange work of the order model on market `uid` (the symbol).
    Exchange {
        action: Action,
        uid: String,
        grid: Grid,
    },
    /// Read the account's live orders → `TradingEvent::OpenOrders`.
    OpenOrders,
}

pub enum TradingEvent {
    Order(OrderUpdate),
    Failed {
        action: Action,
        definitive: bool,
        msg: String,
    },
    /// The account's live orders: on start and after every reopening of the
    /// user-data stream, whose events in between may be lost.
    OpenOrders(Vec<OrderUpdate>),
    /// The read of the account's live orders failed: it is asked again (the engine plans it),
    /// or the restored orders would stay unchecked until the stream reopens, hours away.
    OpenOrdersFailed,
    /// Round trip of the last call of an action, ms; a call that never
    /// answered reads as the call timeout.
    Ping(i64),
}

/// The order workers: threads that make the model's exchange calls and report each outcome as
/// `FeedEvent::Trading`. There is no queue in front of the exchange: a router thread
/// (`aster-orders`) only hands a command to the mailbox of the worker its market belongs to
/// ([`shard`]) and never waits for a call. A worker takes an exit before the entries waiting
/// beside it — a stop, a panic, an exit moved or cancelled never stands behind the entry work of
/// other markets, only behind the call already in flight — but the calls on one market keep their
/// order ([`take_next`]). The open orders are read on a thread of their own. What is left to wait
/// for is the exchange's own budget ([`Limiter`]): an entry that does not fit it stays in the
/// mailbox and the worker goes on with what does; the exits are counted but never held.
///
/// Every worker has its own client and a clone of the account's signer (one nonce sequence for the
/// wallet, shared atomically). The gateway's clock is measured by a thread of its own
/// ([`Clock`]) and handed to every worker, so no call — an exit's least of all — waits for a
/// `/time`; only the retry of a call the gateway refused for its nonce measures it in place. A
/// panic of a worker is sent as [`FeedEvent::Lost`] and what waited in its mailbox is reported
/// failed: exits that never go out are worse than a core that leaves.
pub fn start(
    rest: Rest,
    signer: Signer,
    grids: HashMap<String, Grid>,
    ev: Sender<FeedEvent>,
) -> Sender<TradeCommand> {
    let (tx, rx) = mpsc::channel::<TradeCommand>();
    let grids = Arc::new(grids);
    let limiter = Arc::new(Limiter::default());
    let clock = Arc::new(Clock::new(rest.clock_delta_ms()));
    let network = signer.network();
    let mut first = Some(rest);
    let mut client = || {
        // The first takes the client the caller measured; the rest get their own, on the same
        // network, with the same clock.
        first.take().unwrap_or_else(|| {
            let mut r = Rest::on(network);
            r.set_clock_delta_ms(clock.delta());
            r
        })
    };
    let ticker = Arc::clone(&clock);
    let measurer = client();
    thread::Builder::new()
        .name("aster-order-clock".into())
        .spawn(move || ticker.run(measurer))
        .expect("spawn");
    let boxes: Vec<Arc<Mailbox>> = (0..WORKERS)
        .map(|i| {
            let mailbox = Arc::new(Mailbox::default());
            let worker = Worker {
                rest: client(),
                signer: signer.clone(),
                ev: ev.clone(),
                limiter: Arc::clone(&limiter),
                clock: Arc::clone(&clock),
                mailbox: Arc::clone(&mailbox),
            };
            thread::Builder::new()
                .name(format!("aster-order-{i}"))
                .spawn(move || worker.run())
                .expect("spawn");
            mailbox
        })
        .collect();
    let (reads, reader_rx) = mpsc::channel::<()>();
    let reader = Reader {
        rest: client(),
        signer,
        grids,
        ev: ev.clone(),
        clock,
    };
    thread::Builder::new()
        .name("aster-order-read".into())
        .spawn(move || reader.run(&reader_rx))
        .expect("spawn");
    let router = Router { boxes, reads, ev };
    thread::Builder::new()
        .name("aster-orders".into())
        .spawn(move || {
            while let Ok(cmd) = rx.recv() {
                router.route(cmd);
            }
            router.close();
        })
        .expect("spawn");
    tx
}

/// The gateway's clock for every order thread: `server_time - local_time`, measured here every
/// [`CLOCK_EVERY`](crate::account::CLOCK_EVERY) and when a thread asks ([`Clock::kick`]) after a
/// failure that may be the clock's (a nonce outside the gateway's ±60 s is the failure a sleeping
/// Mac makes). Whoever makes a call reads the figure; nobody waits for the measure.
struct Clock {
    delta_ms: AtomicI64,
    asked: Mutex<bool>,
    wake: Condvar,
}

impl Clock {
    fn new(delta_ms: i64) -> Self {
        Self {
            delta_ms: AtomicI64::new(delta_ms),
            asked: Mutex::new(false),
            wake: Condvar::new(),
        }
    }

    fn delta(&self) -> i64 {
        self.delta_ms.load(Ordering::Relaxed)
    }

    fn set(&self, delta_ms: i64) {
        self.delta_ms.store(delta_ms, Ordering::Relaxed);
    }

    /// Asks for a measure now; does not wait for it.
    fn kick(&self) {
        *self.asked.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.wake.notify_one();
    }

    /// Measures at once, then every [`CLOCK_EVERY`](crate::account::CLOCK_EVERY) or when asked.
    fn run(&self, mut rest: Rest) {
        loop {
            // A panic in the measure is a failed measure: the clock thread must outlive it, or
            // every later kick would ask nobody.
            let measured = panic::catch_unwind(AssertUnwindSafe(|| rest.sync_clock()))
                .unwrap_or_else(|_| {
                    Err(rest::Error::Transport("the clock measure panicked".into()))
                });
            let wait = match measured {
                Ok(delta) => {
                    log::debug!("orders: clock delta {delta} ms");
                    self.set(delta);
                    crate::account::CLOCK_EVERY
                }
                Err(e) => {
                    log::warn!("orders: clock: {e}");
                    CLOCK_RETRY
                }
            };
            let mut asked = self.asked.lock().unwrap_or_else(PoisonError::into_inner);
            if !*asked {
                asked = self
                    .wake
                    .wait_timeout(asked, wait)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            *asked = false;
        }
    }
}

/// One exchange call waiting for its worker.
struct Call {
    action: Action,
    uid: String,
    grid: Grid,
}

impl Call {
    /// An exit is the `Sell` leg: a stop, a panic, an exit moved or cancelled.
    fn is_exit(&self) -> bool {
        describe(&self.action).1 == Leg::Sell
    }

    /// A call the exchange's budget holds: an entry that makes an order.
    fn is_held_by(&self, entries_fit: bool) -> bool {
        !entries_fit && !self.is_exit() && weight_of(&self.action) > 0
    }

    /// The call as the model is told it never went out.
    fn failed(self, why: &str) -> TradingEvent {
        TradingEvent::Failed {
            action: self.action,
            definitive: true,
            msg: why.to_string(),
        }
    }
}

/// Order-making calls an action costs of the exchange's budget: a post one, a replace (a cancel
/// and a post) two, a cancel or a read none.
fn weight_of(action: &Action) -> usize {
    match action {
        Action::Post { .. } => 1,
        Action::Replace { .. } => 2,
        _ => 0,
    }
}

/// The market a call is written on belongs to one worker, always the same one: FNV-1a of the
/// symbol, which unlike `RandomState` is the same on every run.
fn shard(uid: &str) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in uid.bytes() {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
    (h % WORKERS as u64) as usize
}

/// The next call of a worker: the first exit whose market has no call waiting before it that is
/// not a held entry, else the first call that has nothing of its market before it and that the
/// budget does not hold (`entries_fit`: false while the entries are over it). So an exit passes
/// the entries of other markets and the entries held by the budget, and never another call of its
/// own market that was asked for earlier — the cancel of a rival entry stays ahead of the exit it
/// clears the way for; the cancel of a held entry stays behind the entry.
fn take_next(queue: &mut VecDeque<Call>, entries_fit: bool) -> Option<Call> {
    // Every earlier call of a market, and those of them the exchange has a hand in: an entry held
    // by the budget has not been placed, so an exit may pass it (what it would stand against is
    // not on the exchange yet), where the cancel or the replace of that entry may not.
    let mut earlier: HashSet<&str> = HashSet::new();
    let mut placed: HashSet<&str> = HashSet::new();
    let mut first = None;
    let mut exit = None;
    for (i, c) in queue.iter().enumerate() {
        let held = c.is_held_by(entries_fit);
        if c.is_exit() && !placed.contains(c.uid.as_str()) {
            exit = Some(i);
            break;
        }
        if first.is_none() && !held && !earlier.contains(c.uid.as_str()) {
            first = Some(i);
        }
        earlier.insert(c.uid.as_str());
        if !held {
            placed.insert(c.uid.as_str());
        }
    }
    queue.remove(exit.or(first)?)
}

/// What a worker is waiting on.
#[derive(Default)]
struct Mailbox {
    queue: Mutex<VecDeque<Call>>,
    wake: Condvar,
    /// The router is done: the worker ends once nothing is left that it may do.
    closed: AtomicBool,
    /// The worker is gone (a panic). Set under the queue's lock, so a call is either in the queue
    /// when the worker's last act drains it, or handed back by [`Mailbox::push`].
    dead: AtomicBool,
}

enum Next {
    Call(Call),
    Closed,
}

impl Mailbox {
    /// `Err` gives the call back when the worker is gone.
    fn push(&self, call: Call) -> Result<(), Box<Call>> {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        if self.dead.load(Ordering::SeqCst) {
            return Err(Box::new(call));
        }
        queue.push_back(call);
        drop(queue);
        self.wake.notify_one();
        Ok(())
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.wake.notify_all();
    }

    /// The worker is gone: what waited is handed back, and nothing is taken after.
    fn bury(&self) -> Vec<Call> {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        self.dead.store(true, Ordering::SeqCst);
        queue.drain(..).collect()
    }

    /// The next call, once there is one the worker may make: it waits for a push, and for the
    /// budget when only entries it holds are waiting. `Closed` once closed with nothing to do.
    fn next(&self, limiter: &Limiter) -> Next {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            let wait = limiter.wait();
            if let Some(call) = take_next(&mut queue, wait.is_none()) {
                return Next::Call(call);
            }
            if self.closed.load(Ordering::SeqCst) {
                return Next::Closed;
            }
            // Held entries only: wake when the budget has room for them. Nothing waiting: for a push.
            let held = wait.filter(|_| !queue.is_empty());
            queue = match held {
                Some(wait) => self.wake.wait_timeout(queue, wait),
                None => self.wake.wait_timeout(queue, IDLE_WAIT),
            }
            .unwrap_or_else(PoisonError::into_inner)
            .0;
        }
    }
}

/// Hands each command to the worker of its market. It makes no exchange call, so it is never
/// behind one: the only thing it waits on is the engine's next command.
struct Router {
    boxes: Vec<Arc<Mailbox>>,
    reads: Sender<()>,
    ev: Sender<FeedEvent>,
}

impl Router {
    /// A market whose worker is gone is told so — its exits fail, which the model answers — and
    /// the other markets are served as before: one worker lost must not take the exits of the
    /// rest with it.
    fn route(&self, cmd: TradeCommand) {
        match cmd {
            TradeCommand::Exchange { action, uid, grid } => {
                if let Err(call) = self.boxes[shard(&uid)].push(Call { action, uid, grid }) {
                    let _ = self.ev.send(FeedEvent::Trading(call.failed(GONE)));
                }
            }
            TradeCommand::OpenOrders => {
                if self.reads.send(()).is_err() {
                    let _ = self
                        .ev
                        .send(FeedEvent::Trading(TradingEvent::OpenOrdersFailed));
                }
            }
        }
    }

    fn close(&self) {
        for mailbox in &self.boxes {
            mailbox.close();
        }
    }
}

/// What a call whose worker is gone is told.
const GONE: &str = "the order worker is gone";

/// The exchange's order budget as the entries are held to it: the times of the recent order-making
/// calls, the exits' too. An exit takes its place at once; an entry waits until it fits both windows.
#[derive(Default)]
struct Limiter {
    calls: Mutex<VecDeque<Instant>>,
}

impl Limiter {
    /// Drops the calls that have left the minute window.
    fn prune(calls: &mut VecDeque<Instant>, now: Instant) {
        while calls
            .front()
            .is_some_and(|&t| now.saturating_duration_since(t) >= Duration::from_secs(60))
        {
            calls.pop_front();
        }
    }

    /// How long until an entry fits, `None` when it fits now.
    fn wait_for(calls: &mut VecDeque<Instant>, now: Instant) -> Option<Duration> {
        let minute = Duration::from_secs(60);
        let ten = Duration::from_secs(10);
        Self::prune(calls, now);
        let in_ten = calls
            .iter()
            .rev()
            .take_while(|&&t| now.saturating_duration_since(t) < ten)
            .count();
        // The call that falls out of the window first frees the place.
        let mut wait = Duration::ZERO;
        if in_ten >= ENTRY_CAP_10S {
            let t = calls[calls.len() - ENTRY_CAP_10S];
            wait = wait.max(ten.saturating_sub(now.saturating_duration_since(t)));
        }
        if calls.len() >= ENTRY_CAP_1M {
            let t = calls[calls.len() - ENTRY_CAP_1M];
            wait = wait.max(minute.saturating_sub(now.saturating_duration_since(t)));
        }
        (!wait.is_zero()).then(|| wait.max(Duration::from_millis(1)))
    }

    /// Takes a place for a call now, or says how long to wait before asking again; an exit never
    /// waits. What the workers do in two steps ([`Limiter::wait`], then [`Limiter::spend`]).
    #[cfg(test)]
    fn take(calls: &mut VecDeque<Instant>, exit: bool, now: Instant) -> Option<Duration> {
        if !exit {
            if let Some(wait) = Self::wait_for(calls, now) {
                return Some(wait);
            }
        }
        Self::prune(calls, now);
        calls.push_back(now);
        None
    }

    /// How long the entries have to wait, `None` when they fit.
    fn wait(&self) -> Option<Duration> {
        let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        Self::wait_for(&mut calls, Instant::now())
    }

    /// Counts `n` order-making calls just made or about to be: an entry is let through by
    /// [`Limiter::wait`] and counted here, so several workers can pass it together and overshoot
    /// the cap by a few — the cap is two thirds of the exchange's, not all of it. The time is
    /// read under the lock, so the deque stays in the order of its times.
    fn spend(&self, n: usize) {
        let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        Self::prune(&mut calls, now);
        calls.extend(std::iter::repeat_n(now, n));
    }
}

/// One order worker: its client, its signer clone and its mailbox.
struct Worker {
    rest: Rest,
    signer: Signer,
    ev: Sender<FeedEvent>,
    limiter: Arc<Limiter>,
    clock: Arc<Clock>,
    mailbox: Arc<Mailbox>,
}

impl Worker {
    fn run(self) {
        let ev = self.ev.clone();
        let mailbox = Arc::clone(&self.mailbox);
        let run = panic::catch_unwind(AssertUnwindSafe(|| self.work()));
        if run.is_err() {
            // What waited will never be made: told to the model, which answers a failed exit.
            for call in mailbox.bury() {
                let _ = ev.send(FeedEvent::Trading(call.failed(GONE)));
            }
            let _ = ev.send(FeedEvent::Lost(WORKER));
        }
    }

    fn work(self) {
        let Self {
            mut rest,
            mut signer,
            ev,
            limiter,
            clock,
            mailbox,
        } = self;
        let send = |e: TradingEvent| ev.send(FeedEvent::Trading(e)).is_ok();
        while let Next::Call(call) = mailbox.next(&limiter) {
            let Call { action, uid, grid } = call;
            let (order, leg, op) = describe(&action);
            log::debug!("{action:?}");
            let cost = weight_of(&action);
            if cost > 0 {
                limiter.spend(cost);
            }
            rest.set_clock_delta_ms(clock.delta());
            let mut done = execute_guarded(&mut rest, &mut signer, &action, &uid, grid, &ev);
            if done.code == Some(CODE_NONCE_EXPIRED) && done.reports.is_empty() {
                // The one place that measures the clock in front of a call: the gateway refused
                // this one for its nonce, and it goes again with the right one.
                log::warn!("orders: request outside the time window, clock measured again");
                match rest.sync_clock() {
                    Ok(delta) => {
                        clock.set(delta);
                        done = execute_guarded(&mut rest, &mut signer, &action, &uid, grid, &ev);
                    }
                    Err(e) => {
                        log::warn!("orders: clock: {e}");
                        clock.kick();
                    }
                }
            }
            let rtt = rest.last_rtt().unwrap_or(rest::CALL_TIMEOUT).as_millis() as i64;
            send(TradingEvent::Ping(rtt));
            for u in done.reports {
                log::debug!("{op:?} order {order:#x} {leg:?}: {u:?}");
                send(TradingEvent::Order(u));
            }
            if let Some((definitive, msg)) = done.failed {
                if done.code == Some(CODE_REDUCE_ONLY) {
                    log::error!(
                        "{op:?} order {order:#x} {leg:?}: {msg} — the exit's reduce-only \
                         was refused: the core and the account disagree about the position"
                    );
                } else {
                    log::warn!("{op:?} order {order:#x} {leg:?}: {msg}");
                }
                if clock_suspect_code(done.code) {
                    clock.kick();
                }
                let failed = TradingEvent::Failed {
                    action,
                    definitive,
                    msg,
                };
                if !send(failed) {
                    return;
                }
            }
        }
    }
}

/// The thread that reads the account's open orders: not a call of any market, so not behind the
/// workers' calls (nor they behind it). A snapshot can meet an outcome of a worker out of order;
/// the engine takes it as it takes the user-data stream's own race with a reply.
struct Reader {
    rest: Rest,
    signer: Signer,
    grids: Arc<HashMap<String, Grid>>,
    ev: Sender<FeedEvent>,
    clock: Arc<Clock>,
}

impl Reader {
    fn run(self, rx: &Receiver<()>) {
        let lost = self.ev.clone();
        let run = panic::catch_unwind(AssertUnwindSafe(|| self.read(rx)));
        if run.is_err() {
            let _ = lost.send(FeedEvent::Lost(READER));
        }
    }

    fn read(self, rx: &Receiver<()>) {
        let Self {
            mut rest,
            mut signer,
            grids,
            ev,
            clock,
        } = self;
        let send = |e: TradingEvent| ev.send(FeedEvent::Trading(e)).is_ok();
        while rx.recv().is_ok() {
            rest.set_clock_delta_ms(clock.delta());
            match rest.open_orders(&mut signer) {
                Ok(rows) => {
                    let list = rows
                        .iter()
                        .filter_map(|r| {
                            let step = grids.get(&r.symbol)?.step;
                            OrderUpdate::from_reply(r, step, true)
                        })
                        .collect();
                    if !send(TradingEvent::OpenOrders(list)) {
                        return;
                    }
                }
                Err(e) => {
                    log::warn!("orders: open orders: {e}");
                    if clock_suspect(&e) {
                        clock.kick();
                    }
                    if !send(TradingEvent::OpenOrdersFailed) {
                        return;
                    }
                }
            }
        }
    }
}

/// [`execute`], with a panic inside it told to the model before the worker goes down with it: the
/// call may or may not have gone out, so it fails as unknown, and its order does not wait for an
/// answer that never comes (what still waited in the mailbox is told by [`Mailbox::bury`]).
fn execute_guarded(
    rest: &mut Rest,
    signer: &mut Signer,
    action: &Action,
    uid: &str,
    grid: Grid,
    ev: &Sender<FeedEvent>,
) -> Done {
    match panic::catch_unwind(AssertUnwindSafe(|| {
        execute(rest, signer, action, uid, grid)
    })) {
        Ok(done) => done,
        Err(panicked) => {
            let _ = ev.send(FeedEvent::Trading(TradingEvent::Failed {
                action: action.clone(),
                definitive: false,
                msg: "the order worker panicked in this call".into(),
            }));
            panic::resume_unwind(panicked)
        }
    }
}

fn describe(a: &Action) -> (u64, Leg, Op) {
    match a {
        Action::Post { order, leg, .. } => (*order, *leg, Op::Post),
        Action::Cancel { order, leg, .. } => (*order, *leg, Op::Cancel),
        Action::Replace { order, leg, .. } => (*order, *leg, Op::Replace),
        Action::Query { order, leg, .. } | Action::QueryRequest { order, leg, .. } => {
            (*order, *leg, Op::Query)
        }
    }
}

/// What one action's calls brought back: the reports, in order, and the
/// failure that ended it, if one did — `(definitive, message)`. Both can be
/// there: a `Replace` whose cancel went through and whose post did not.
#[derive(Debug, Default)]
struct Done {
    reports: Vec<OrderUpdate>,
    failed: Option<(bool, String)>,
    /// The exchange's code of that failure, when it gave one.
    code: Option<i64>,
}

impl Done {
    fn fail(mut self, e: &rest::Error) -> Self {
        self.failed = Some((definitive(e), e.to_string()));
        if let rest::Error::Api { code, .. } = e {
            self.code = Some(*code);
        }
        self
    }
}

/// `-4225 Nonce Expired`: the request's nonce was outside the gateway's
/// window (docs, v3 «Nonce Mechanism» and its example answer). It was not
/// carried out, so it is made again once the clock is measured anew. That
/// helps a clock that fell behind; a sequence pushed ahead of the gateway
/// stays ahead (`Signer::next_nonce` never steps back), and the retry then
/// fails the same way and is reported.
const CODE_NONCE_EXPIRED: i64 = -4225;
/// `-2022 REDUCE_ONLY_REJECT`: an exit refused for its reduce-only flag — the
/// core's model and the account disagree about the position, which is a
/// defect to look into, not a market condition (`PLAN.md`, error codes).
const CODE_REDUCE_ONLY: i64 = -2022;

/// One action's exchange calls.
///
/// A `Replace` is a cancel and a post under the new key: Aster has no
/// replace that takes a new key. The cancel's report goes first, so fills of
/// the old order are counted. When the cancel's answer shows fills the model
/// had not counted (`filled`), the new order is not posted: its lots were
/// sized without them, and posting would buy past the budget. The `Replace`
/// then fails, and the model falls back to the old order, whose final report
/// it has (`Orders::failed`).
fn execute(rest: &mut Rest, signer: &mut Signer, a: &Action, uid: &str, grid: Grid) -> Done {
    let mut done = Done::default();
    let report = |done: &mut Done, r: &OrderReply, key: Option<&str>| -> bool {
        match OrderUpdate::from_reply(r, grid.step, true) {
            Some(mut u) => {
                if let (true, Some(key)) = (u.request_id.is_empty(), key) {
                    u.request_id = key.to_owned();
                }
                done.reports.push(u);
                true
            }
            None => {
                done.failed = Some((false, format!("unreadable order status {:?}", r.status)));
                false
            }
        }
    };
    match a {
        Action::Post {
            key,
            leg,
            lots,
            price,
            sell,
            ..
        } => {
            let params = post_params(uid, *leg, key, *lots, *price, *sell, grid);
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match rest.new_order(signer, &params) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Replace {
            leg,
            exchange_id,
            key,
            lots,
            price,
            filled,
            ..
        } => {
            let old = match rest.cancel_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(old) => old,
                Err(e) => return done.fail(&e),
            };
            if !report(&mut done, &old, None) {
                return done;
            }
            let old_filled = done.reports[0].lots_executed;
            if old_filled > *filled {
                done.failed = Some((
                    true,
                    format!(
                        "{} lot(s) filled while it was being replaced; the new order is not placed",
                        old_filled - filled
                    ),
                ));
                return done;
            }
            let sell = old.side == "SELL";
            let params = post_params(uid, *leg, key, *lots, Some(*price), sell, grid);
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match rest.new_order(signer, &params) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Cancel { exchange_id, .. } => {
            match rest.cancel_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(r) => {
                    report(&mut done, &r, None);
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::QueryRequest { key, .. } => {
            match rest.query_order(signer, uid, OrderRef::Key(key)) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Query { exchange_id, .. } => {
            match rest.query_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(r) => {
                    report(&mut done, &r, None);
                }
                Err(e) => return done.fail(&e),
            }
        }
    }
    done
}

/// The parameters of a new order: a GTC limit at `price`, or MARKET without
/// one. Every exit is `reduceOnly` (`PLAN.md`, «Ордера»): one-way mode, so an
/// exit cannot turn the position over, whatever fills before it.
fn post_params(
    uid: &str,
    leg: Leg,
    key: &str,
    lots: i64,
    price: Option<f64>,
    sell: bool,
    grid: Grid,
) -> Vec<(&'static str, String)> {
    let mut p = vec![
        ("symbol", uid.to_owned()),
        ("side", if sell { "SELL" } else { "BUY" }.to_owned()),
        (
            "type",
            if price.is_some() { "LIMIT" } else { "MARKET" }.to_owned(),
        ),
        ("quantity", on_grid(lots as f64 * grid.step, grid.step)),
    ];
    if let Some(price) = price {
        p.push(("timeInForce", "GTC".to_owned()));
        p.push(("price", on_grid(price, grid.tick)));
    }
    if leg == Leg::Sell {
        p.push(("reduceOnly", "true".to_owned()));
    }
    p.push(("newClientOrderId", key.to_owned()));
    // The final state of a MARKET order, not only its acceptance: the model
    // learns the fill from the reply, not seconds later from the stream.
    p.push(("newOrderRespType", "RESULT".to_owned()));
    p
}

impl OrderUpdate {
    /// From an `ORDER_TRADE_UPDATE` of the user-data stream; quantities in
    /// lots of `step`. `None` for a status the model does not model (the
    /// exchange's own liquidation orders), without a grid to count lots on,
    /// and for a quantity or price that does not read: a fill read as zero
    /// would be a fill never counted, and the next reply or reconciliation
    /// carries the order's true state.
    pub fn from_event(o: &OrderEvent, step: f64) -> Option<Self> {
        if step.is_nan() || step <= 0.0 {
            return None;
        }
        let num = |s: &str| s.parse::<f64>().ok().filter(|v| v.is_finite());
        let lots = |q: f64| (q / step).round() as i64;
        Some(Self {
            exchange_id: o.id.to_string(),
            request_id: o.client_id.clone(),
            uid: o.symbol.clone(),
            status: ExecStatus::from_aster(&o.status)?,
            sell: o.side == "SELL",
            is_market: o.kind == "MARKET",
            lots_requested: lots(num(&o.qty)?),
            lots_executed: lots(num(&o.filled)?),
            price: num(&o.price)?,
            avg_price: if o.avg_price.is_empty() {
                0.0
            } else {
                num(&o.avg_price)?
            },
            unary: false,
            time_ms: o.time_ms,
            message: String::new(),
        })
    }

    /// From an order reply; quantities in lots of `step`. `None` for a status
    /// the model does not model and without a grid: an order read as live
    /// that is not would be kept and acted on.
    pub fn from_reply(r: &OrderReply, step: f64, unary: bool) -> Option<Self> {
        if step.is_nan() || step <= 0.0 {
            return None;
        }
        let lots = |q: f64| (q / step).round() as i64;
        Some(Self {
            exchange_id: r.order_id.to_string(),
            request_id: r.client_order_id.clone(),
            uid: r.symbol.clone(),
            status: ExecStatus::from_aster(&r.status)?,
            sell: r.side == "SELL",
            is_market: r.kind == "MARKET",
            lots_requested: lots(r.orig_qty),
            lots_executed: lots(r.executed_qty),
            price: r.price,
            avg_price: r.avg_price,
            unary,
            time_ms: r.update_ms,
            message: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_silent_or_nonce_failure_makes_the_clock_suspect() {
        assert!(clock_suspect_code(None));
        assert!(clock_suspect_code(Some(CODE_NONCE_EXPIRED)));
        // -2011 (cancel of a filled order), -2019 (margin), -2022 (reduce-only refused).
        for code in [-2011, -2019, -2022] {
            assert!(!clock_suspect_code(Some(code)), "{code}");
        }
    }

    const GRID: Grid = Grid {
        step: 0.001,
        tick: 0.1,
    };

    fn params(p: &[(&'static str, String)]) -> String {
        p.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// What the worker and the engine make of each refusal the exchange can give, from the real
    /// `rest::Error` (no hand-written message): refused for good or of unknown fate, whether the
    /// clock is suspected, whether it counts as a rate limit.
    #[test]
    fn exchange_refusals_are_classified_by_status_and_code() {
        use crate::moonshot::rate_limited;
        let api = |status: u16, code: i64| rest::Error::Api {
            status,
            code,
            msg: "x".into(),
        };
        //            error                    definitive  clock?  rate-limited
        let table = [
            (api(400, -4225), true, true, false), // nonce outside the window
            (api(400, -2022), true, false, false), // reduce-only refused
            (api(400, -2019), true, false, false), // margin is insufficient
            (api(400, -2011), true, false, false), // cancel of a finished order
            (api(400, -1111), true, false, false), // precision
            (api(400, -4164), true, false, false), // below the minimum notional
            (api(400, -4141), true, false, false), // symbol closed
            (api(429, -1003), true, false, true), // too many requests
            (api(418, -1003), true, false, true), // banned
            (api(400, -1015), true, false, true), // too many new orders
            (api(503, -1001), false, false, false), // gateway trouble: fate unknown
            (api(408, 0), false, false, false),   // timed out upstream
            (rest::Error::Transport("reset".into()), false, true, false),
        ];
        for (e, definitive_, clock, limited) in table {
            let text = e.to_string();
            assert_eq!(definitive(&e), definitive_, "definitive: {text}");
            assert_eq!(clock_suspect(&e), clock, "clock: {text}");
            assert_eq!(rate_limited(&text), limited, "rate limit: {text}");
        }
    }

    #[test]
    fn an_entry_limit_and_a_market_exit_are_written_on_the_grid() {
        let entry = post_params(
            "BTCUSDT",
            Leg::Buy,
            "k1",
            16,
            Some(84_249.349_9),
            false,
            GRID,
        );
        assert_eq!(
            params(&entry),
            "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=0.016&timeInForce=GTC&\
             price=84249.3&newClientOrderId=k1&newOrderRespType=RESULT"
        );
        let exit = post_params("BTCUSDT", Leg::Sell, "k2", 7, None, true, GRID);
        assert_eq!(
            params(&exit),
            "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.007&reduceOnly=true&\
             newClientOrderId=k2&newOrderRespType=RESULT"
        );
        // A short's exit buys back, and is reduce-only all the same.
        let cover = post_params("BTCUSDT", Leg::Sell, "k3", 1, Some(80_000.0), false, GRID);
        assert!(
            params(&cover).contains("side=BUY&type=LIMIT")
                && params(&cover).contains("reduceOnly=true")
        );
    }

    #[test]
    fn a_reply_becomes_a_report_in_lots() {
        // The docs' reply shape, with the order's numbers.
        let r: OrderReply = serde_json::from_str(
            r#"{"clientOrderId":"k1","cumQty":"0","cumQuote":"0","executedQty":"0.007",
                "orderId":22542179,"avgPrice":"84250.1","origQty":"0.016","price":"84249.3",
                "reduceOnly":false,"side":"BUY","positionSide":"BOTH","status":"PARTIALLY_FILLED",
                "stopPrice":"0","closePosition":false,"symbol":"BTCUSDT","timeInForce":"GTC",
                "type":"LIMIT","origType":"LIMIT","updateTime":1566818724722,
                "workingType":"CONTRACT_PRICE","priceProtect":false}"#,
        )
        .unwrap();
        let u = OrderUpdate::from_reply(&r, GRID.step, true).unwrap();
        assert_eq!(
            (
                u.exchange_id.as_str(),
                u.request_id.as_str(),
                u.uid.as_str()
            ),
            ("22542179", "k1", "BTCUSDT")
        );
        assert_eq!(
            (
                u.status,
                u.lots_requested,
                u.lots_executed,
                u.sell,
                u.is_market
            ),
            (ExecStatus::PartiallyFilled, 16, 7, false, false)
        );
        assert_eq!((u.price, u.avg_price), (84_249.3, 84_250.1));
    }

    #[test]
    fn a_stream_report_becomes_a_report_and_an_unreadable_one_does_not() {
        let mut o = OrderEvent {
            symbol: "BTCUSDT".into(),
            client_id: "k1".into(),
            id: 8886774,
            side: "SELL".into(),
            kind: "MARKET".into(),
            execution: "TRADE".into(),
            status: "FILLED".into(),
            qty: "0.016".into(),
            price: "0".into(),
            filled: "0.016".into(),
            last_price: "84250.1".into(),
            avg_price: "84250.1".into(),
            time_ms: 1568879465651,
            commission: "0.53".into(),
            commission_asset: "USDT".into(),
            trade_id: 7,
        };
        let u = OrderUpdate::from_event(&o, GRID.step).unwrap();
        assert_eq!(
            (
                u.status,
                u.lots_requested,
                u.lots_executed,
                u.sell,
                u.is_market,
                u.unary
            ),
            (ExecStatus::Filled, 16, 16, true, true, false)
        );
        assert_eq!((u.exchange_id.as_str(), u.avg_price), ("8886774", 84_250.1));
        o.filled = "abc".into();
        assert!(OrderUpdate::from_event(&o, GRID.step).is_none());
        o.filled = "0.016".into();
        o.status = "NEW_ADL".into();
        assert!(OrderUpdate::from_event(&o, GRID.step).is_none());
    }

    #[test]
    fn an_expired_order_is_a_cancel_to_the_model() {
        assert_eq!(
            ExecStatus::from_aster("EXPIRED"),
            Some(ExecStatus::Cancelled)
        );
        assert_eq!(ExecStatus::from_aster("NEW_ADL"), None);
    }

    #[test]
    fn a_refusal_is_definitive_and_a_timeout_is_not() {
        let api = |status| rest::Error::Api {
            status,
            code: -2019,
            msg: String::new(),
        };
        assert!(definitive(&api(400)));
        assert!(definitive(&api(429)));
        assert!(!definitive(&api(408)));
        assert!(!definitive(&api(503)));
        assert!(!definitive(&rest::Error::Transport("timeout".into())));
    }

    fn call(uid: &str, order: u64, leg: Leg) -> Call {
        Call {
            action: Action::Cancel {
                order,
                leg,
                exchange_id: "1".into(),
            },
            uid: uid.into(),
            grid: GRID,
        }
    }

    fn queue(calls: Vec<Call>) -> VecDeque<Call> {
        calls.into_iter().collect()
    }

    fn who(c: Call) -> (String, u64) {
        (c.uid, describe(&c.action).0)
    }

    fn post(uid: &str, order: u64) -> Call {
        Call {
            action: Action::Post {
                order,
                leg: Leg::Buy,
                key: "k".into(),
                uid: uid.into(),
                lots: 1,
                price: Some(1.0),
                sell: false,
            },
            uid: uid.into(),
            grid: GRID,
        }
    }

    /// An exit passes the entry work of other markets: it does not stand behind it.
    #[test]
    fn an_exit_passes_the_entries_of_other_markets() {
        let mut q = queue(vec![
            call("AAAUSDT", 1, Leg::Buy),
            call("BBBUSDT", 2, Leg::Buy),
            call("CCCUSDT", 3, Leg::Sell),
        ]);
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("CCCUSDT".into(), 3));
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("AAAUSDT".into(), 1));
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("BBBUSDT".into(), 2));
        assert!(take_next(&mut q, true).is_none());
    }

    /// On one market the calls keep the order they were asked in: the cancel of a rival entry
    /// stays ahead of the exit it clears the way for (a reduce-only exit against a resting entry
    /// is refused, `-2022`) — but an exit of another market still passes them both.
    #[test]
    fn the_calls_of_one_market_keep_their_order() {
        let mut q = queue(vec![
            call("AAAUSDT", 10, Leg::Buy),  // the rival entry's cancel
            call("AAAUSDT", 11, Leg::Sell), // the exit it clears the way for
            call("BBBUSDT", 20, Leg::Sell),
        ]);
        assert_eq!(
            who(take_next(&mut q, true).unwrap()),
            ("BBBUSDT".into(), 20)
        );
        assert_eq!(
            who(take_next(&mut q, true).unwrap()),
            ("AAAUSDT".into(), 10)
        );
        assert_eq!(
            who(take_next(&mut q, true).unwrap()),
            ("AAAUSDT".into(), 11)
        );
    }

    /// Without an exit, or with exits only behind their own market's earlier calls, it is plain
    /// first come first served; exits of one market also keep their order among themselves.
    #[test]
    fn without_a_passing_exit_it_is_first_come_first_served() {
        let mut q = queue(vec![
            call("AAAUSDT", 1, Leg::Buy),
            call("AAAUSDT", 2, Leg::Buy),
            call("AAAUSDT", 3, Leg::Sell),
            call("AAAUSDT", 4, Leg::Sell),
        ]);
        let order: Vec<u64> = std::iter::from_fn(|| take_next(&mut q, true))
            .map(|c| who(c).1)
            .collect();
        assert_eq!(order, [1, 2, 3, 4]);
    }

    /// Over the budget an entry stays where it is and the worker goes on with what the budget
    /// does not hold: the exits — those of the held entry's own market too, since what it would
    /// stand against is not on the exchange yet — the cancels, the entries that fit.
    #[test]
    fn an_entry_over_the_budget_is_held_and_the_rest_goes_on() {
        let mut q = queue(vec![
            post("AAAUSDT", 1),
            call("AAAUSDT", 2, Leg::Sell), // passes the held entry of its own market
            call("BBBUSDT", 3, Leg::Buy),  // a cancel: costs no budget
            call("CCCUSDT", 4, Leg::Sell),
            post("DDDUSDT", 5),
        ]);
        assert_eq!(
            who(take_next(&mut q, false).unwrap()),
            ("AAAUSDT".into(), 2)
        );
        assert_eq!(
            who(take_next(&mut q, false).unwrap()),
            ("CCCUSDT".into(), 4)
        );
        assert_eq!(
            who(take_next(&mut q, false).unwrap()),
            ("BBBUSDT".into(), 3)
        );
        assert!(
            take_next(&mut q, false).is_none(),
            "only the held entries are left"
        );
        assert_eq!(q.len(), 2);
        // The budget has room again: first come first served, the held entry first.
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("AAAUSDT".into(), 1));
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("DDDUSDT".into(), 5));
    }

    /// What is asked about a held entry stays behind it: its cancel or its replace cannot reach
    /// the exchange before the order it is about.
    #[test]
    fn the_cancel_of_a_held_entry_stays_behind_it() {
        let mut q = queue(vec![post("AAAUSDT", 1), call("AAAUSDT", 1, Leg::Buy)]);
        assert!(take_next(&mut q, false).is_none());
        assert_eq!(who(take_next(&mut q, true).unwrap()), ("AAAUSDT".into(), 1));
        assert_eq!(q.len(), 1);
        assert!(matches!(q[0].action, Action::Cancel { .. }));
    }

    #[test]
    fn a_market_always_has_the_same_worker_and_markets_spread() {
        assert_eq!(shard("BTCUSDT"), shard("BTCUSDT"));
        assert_eq!(shard("龙虾USDT"), shard("龙虾USDT"));
        let used: HashSet<usize> = [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "DOGEUSDT",
            "XRPUSDT",
            "ADAUSDT",
            "LINKUSDT",
            "AVAXUSDT",
            "SUIUSDT",
            "TAOUSDT",
            "ENAUSDT",
            "WLDUSDT",
            "UNIUSDT",
            "LTCUSDT",
            "BCHUSDT",
            "NEARUSDT",
            "ARBUSDT",
            "ONDOUSDT",
            "TRUMPUSDT",
            "AAVEUSDT",
        ]
        .into_iter()
        .map(shard)
        .collect();
        assert!(used.iter().all(|&i| i < WORKERS));
        assert!(
            used.len() >= WORKERS / 2,
            "twenty markets use {} workers",
            used.len()
        );
    }

    fn router() -> (Router, Receiver<()>, Receiver<FeedEvent>) {
        let boxes = (0..WORKERS).map(|_| Arc::new(Mailbox::default())).collect();
        let (reads, reader_rx) = mpsc::channel();
        let (ev, ev_rx) = mpsc::channel();
        (Router { boxes, reads, ev }, reader_rx, ev_rx)
    }

    fn exchange(uid: &str, order: u64, leg: Leg) -> TradeCommand {
        TradeCommand::Exchange {
            action: call(uid, order, leg).action,
            uid: uid.into(),
            grid: GRID,
        }
    }

    #[test]
    fn the_router_hands_a_market_to_its_worker_and_reads_to_the_reader() {
        let (router, reads, _) = router();
        router.route(exchange("BTCUSDT", 1, Leg::Buy));
        router.route(exchange("BTCUSDT", 2, Leg::Sell));
        router.route(TradeCommand::OpenOrders);
        let waiting: Vec<usize> = router
            .boxes
            .iter()
            .map(|m| m.queue.lock().unwrap().len())
            .collect();
        assert_eq!(waiting[shard("BTCUSDT")], 2);
        assert_eq!(waiting.iter().sum::<usize>(), 2);
        assert!(reads.try_recv().is_ok());
    }

    /// A market whose worker is gone is told so — its calls fail, which the model answers — and the
    /// other markets are served as before: one lost worker does not take the rest's exits with it.
    #[test]
    fn a_dead_worker_fails_its_market_and_the_others_go_on() {
        let (router, reads, ev) = router();
        let victim = router.boxes[shard("BTCUSDT")].clone();
        victim.push(call("BTCUSDT", 5, Leg::Sell)).ok().unwrap();
        // The worker's last act: what waited comes back, and nothing is taken after.
        let left = victim.bury();
        assert_eq!(left.len(), 1);
        router.route(exchange("BTCUSDT", 1, Leg::Sell));
        match ev.try_recv() {
            Ok(FeedEvent::Trading(TradingEvent::Failed {
                action, definitive, ..
            })) => {
                assert_eq!(describe(&action).0, 1);
                assert!(definitive, "it never went out");
            }
            _ => panic!("the call of a dead worker was not told failed"),
        }
        let other = ["ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"]
            .into_iter()
            .find(|u| shard(u) != shard("BTCUSDT"))
            .expect("some market has another worker");
        router.route(exchange(other, 2, Leg::Sell));
        assert!(ev.try_recv().is_err(), "a healthy market is not failed");
        assert_eq!(router.boxes[shard(other)].queue.lock().unwrap().len(), 1);
        drop(reads);
        router.route(TradeCommand::OpenOrders);
        assert!(matches!(
            ev.try_recv(),
            Ok(FeedEvent::Trading(TradingEvent::OpenOrdersFailed))
        ));
    }

    #[test]
    fn a_mailbox_gives_its_calls_in_order_and_closes_when_there_is_nothing_to_do() {
        let (m, limiter) = (Mailbox::default(), Limiter::default());
        m.push(call("AAAUSDT", 1, Leg::Buy)).ok().unwrap();
        m.push(call("AAAUSDT", 2, Leg::Buy)).ok().unwrap();
        m.close();
        // Closed, but what is waiting is still done.
        let Next::Call(first) = m.next(&limiter) else {
            panic!("a call")
        };
        assert_eq!(describe(&first.action).0, 1);
        assert!(matches!(m.next(&limiter), Next::Call(_)));
        assert!(matches!(m.next(&limiter), Next::Closed));
    }

    /// A worker over the budget does not sleep through the window with an exit in its mailbox: it
    /// wakes at the push, whichever way the thread and the push race. Bounded by two seconds
    /// where a lost wake-up would take the ten of the window.
    #[test]
    fn an_exit_arriving_behind_a_held_entry_is_not_made_to_wait() {
        let (m, limiter) = (Arc::new(Mailbox::default()), Arc::new(Limiter::default()));
        limiter.spend(ENTRY_CAP_10S); // the entries are at their cap for ten seconds
        m.push(post("AAAUSDT", 1)).ok().unwrap();
        let (m2, l2) = (Arc::clone(&m), Arc::clone(&limiter));
        let (done, got) = mpsc::channel();
        thread::spawn(move || {
            let id = match m2.next(&l2) {
                Next::Call(c) => Some(describe(&c.action).0),
                Next::Closed => None,
            };
            let _ = done.send(id);
        });
        assert!(
            got.recv_timeout(Duration::from_millis(50)).is_err(),
            "the held entry does not run"
        );
        m.push(call("BBBUSDT", 9, Leg::Sell)).ok().unwrap();
        assert_eq!(
            got.recv_timeout(Duration::from_secs(2))
                .expect("the exit woke the worker"),
            Some(9)
        );
    }

    #[test]
    fn only_the_calls_that_make_an_order_cost_the_budget() {
        let replace = Action::Replace {
            order: 1,
            leg: Leg::Buy,
            exchange_id: "1".into(),
            key: "k".into(),
            uid: "AAAUSDT".into(),
            lots: 1,
            price: 1.0,
            filled: 0,
        };
        assert_eq!(weight_of(&post("AAAUSDT", 1).action), 1);
        assert_eq!(weight_of(&replace), 2);
        assert_eq!(weight_of(&call("AAAUSDT", 1, Leg::Buy).action), 0);
    }

    /// The clock the workers read is the one the measuring thread sets; a kick only asks.
    #[test]
    fn the_clock_is_read_by_everyone_and_kicked_not_waited_for() {
        let clock = Clock::new(-40);
        assert_eq!(clock.delta(), -40);
        clock.set(125);
        assert_eq!(clock.delta(), 125);
        clock.kick(); // returns at once; the figure stays until the measuring thread sets it
        assert_eq!(clock.delta(), 125);
        assert!(*clock.asked.lock().unwrap());
    }

    /// Fill the windows with `n` calls spread over the last `span`.
    fn calls(now: Instant, n: usize, span: Duration) -> VecDeque<Instant> {
        (0..n)
            .map(|i| now - span + span.mul_f64(i as f64 / n.max(1) as f64))
            .collect()
    }

    #[test]
    fn an_entry_fits_while_the_windows_have_room_and_waits_when_they_do_not() {
        let now = Instant::now() + Duration::from_secs(120);
        let mut q = calls(now, ENTRY_CAP_10S - 1, Duration::from_secs(9));
        assert_eq!(Limiter::take(&mut q, false, now), None);
        assert_eq!(q.len(), ENTRY_CAP_10S, "it took its place");
        // Full for ten seconds: the next entry waits for the oldest call to leave the window.
        let wait = Limiter::take(&mut q, false, now).expect("the window is full");
        assert!(
            wait > Duration::ZERO && wait <= Duration::from_secs(10),
            "{wait:?}"
        );
        assert_eq!(q.len(), ENTRY_CAP_10S, "a refusal takes no place");
        // After that wait it fits.
        assert_eq!(Limiter::take(&mut q, false, now + wait), None);
    }

    #[test]
    fn the_minute_window_holds_the_entries_too() {
        let now = Instant::now() + Duration::from_secs(120);
        // 400 calls over the last 59 s, none in the last 10 s: the ten-second window is empty.
        let mut q = calls(
            now - Duration::from_secs(10),
            ENTRY_CAP_1M,
            Duration::from_secs(49),
        );
        let wait = Limiter::take(&mut q, false, now).expect("the minute is full");
        assert!(wait <= Duration::from_secs(50), "{wait:?}");
        assert_eq!(Limiter::take(&mut q, false, now + wait), None);
    }

    /// An exit never waits, and what it spends is counted, so the entries wait for it.
    #[test]
    fn an_exit_never_waits_and_the_entries_pay_for_it() {
        let now = Instant::now() + Duration::from_secs(120);
        let mut q = calls(now, ENTRY_CAP_10S + 50, Duration::from_secs(5));
        assert_eq!(Limiter::take(&mut q, true, now), None);
        assert_eq!(q.len(), ENTRY_CAP_10S + 51);
        let wait = Limiter::take(&mut q, false, now).expect("over the cap");
        // With 151 in the window an entry needs 52 of them gone, not just one.
        assert_eq!(
            Limiter::take(&mut q, false, now + wait - Duration::from_millis(1)),
            Some(Duration::from_millis(1))
        );
        assert_eq!(Limiter::take(&mut q, false, now + wait), None);
    }

    #[test]
    fn old_calls_leave_the_minute_window() {
        let now = Instant::now() + Duration::from_secs(300);
        let mut q = calls(now - Duration::from_secs(61), 500, Duration::from_secs(10));
        assert_eq!(Limiter::take(&mut q, false, now), None);
        assert_eq!(q.len(), 1, "only the new call is left");
    }
}
