//! Core orders: the MoonBot worker model (entry leg + exit leg, `is_short`
//! flips both) over Aster exchange orders, ported from TInvestCore. Pure
//! state: terminal commands and exchange reports go in, exchange actions and
//! changed images come out.
//!
//! Panic sell, stop-loss, trailing stop and a «market» `ClosePosition` close
//! with a MARKET order (`PLAN.md`, «Открытые решения» п. 2): a limit through
//! the book cannot cross Aster's `PERCENT_PRICE` band of a few percent, and
//! TInvestCore's deep limit would sit pinned to it. A `ClosePosition` by limit
//! moves the exit to a limit `PANIC_SPREAD` through the book and follows it,
//! as TInvestCore did. The emulator takes the same MARKET and fills it at the
//! best quote (since 03.10; before, an emulated panic was a limit pinned to the
//! band, so the emulator showed another execution than the real account would
//! get). Quantities are lots: whole `stepSize` steps.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hash, Hasher};

use moonproto::server::codec::trade::{status, LegState, OrderRecord, StartOrder};
use serde::{Deserialize, Serialize};

use crate::model::{round_tick, Catalog, Market, OrderKind};
use crate::reports::ExitSource;

/// A filled exchange order to ask `GetOrderState` about: `id` is the broker
/// id, or our idempotency key when `by_request`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilledOrder {
    pub id: String,
    pub by_request: bool,
}

/// One exchange order of an emulated core order, as `emulator` answers for it.
#[derive(Debug, Clone, PartialEq)]
pub struct EmuOrder {
    pub order: u64,
    pub leg: Leg,
    pub key: String,
    /// Empty until the emulator placed it.
    pub exchange_id: String,
    pub uid: String,
    pub sell: bool,
    pub lots: i64,
    pub filled: i64,
    /// Limit price and mean fill price, quoted units.
    pub price: f64,
    pub mean: f64,
    pub status: Option<ExecStatus>,
}

impl EmuOrder {
    fn of(o: &CoreOrder, leg: Leg, e: &Execution) -> Self {
        Self {
            order: o.id,
            leg,
            key: e.key.clone(),
            exchange_id: e.exchange_id().unwrap_or_default().to_owned(),
            uid: o.uid.clone(),
            sell: o.sells(leg),
            lots: e.lots,
            filled: e.filled,
            price: e.price,
            mean: e.mean,
            status: e.status,
        }
    }
}

/// Keys of the latest generations kept in the order store for late reports.
const KEEP_KEYS: usize = 3;

/// MoonBot `SellReason` codes (`moonproto::state::SellReason`) the core sets.
pub mod reason {
    pub const SELL_PRICE: u8 = 1;
    pub const AUTO_PRICE_DOWN: u8 = 2;
    pub const PANIC_SELL: u8 = 6;
    pub const STOP_LOSS: u8 = 7;
    pub const TRAILING: u8 = 8;
    pub const MANUAL_SELL: u8 = 10;
    pub const BV_SV_STOP: u8 = 13;
}

/// Reason of a new core exit generation: the pending decision (consumed by
/// this generation), else the live exit's (a chase, a manual drag, a resize
/// keep it), else a plain sell at the target price.
fn exit_reason_of(next: u8, sell: &ExLeg) -> u8 {
    let live = sell
        .executions
        .iter()
        .rev()
        .find(|e| e.is_live())
        .map_or(0, |e| e.reason);
    match (next, live) {
        (0, 0) => reason::SELL_PRICE,
        (0, live) => live,
        (next, _) => next,
    }
}
use crate::trading::{ExecStatus, OrderUpdate};

/// Terminal orders stay listed this long so late status requests still resolve.
const KEEP_DONE_MS: i64 = 120_000;
/// `-2013 NO_SUCH_ORDER`: the exchange does not know the order.
pub(crate) const CODE_ORDER_NOT_FOUND: i64 = -2013;
/// How long after the first «not found» to one request key a further one
/// proves the request never reached the exchange: Aster v3 takes a request's
/// nonce only within 60 s of its own clock (API docs, «V3 Nonce Mechanism»),
/// so a request older than that can no longer be executed; 5 s more cover the
/// core's clock offset. The pause between the asks doubles from
/// `RESOLVE_PERIOD_MS` (asks at 0, 10, 30, 70 s). Until 03.10 it was six asks
/// over ~5 min — TInvestCore's, for T-Invest's minute error budget and its
/// stream aliases.
const REQUEST_GIVE_UP_MS: i64 = 65_000;
/// Pause between reconciliations of an uncertain request.
const RESOLVE_PERIOD_MS: i64 = 10_000;
/// Spread a panic / close limit crosses the book when the order carries none
/// (MoonBot's 1.5 %).
pub const PANIC_SPREAD: f64 = 0.015;
/// A panic exit follows the book no more often than this.
const PANIC_CHASE_MS: i64 = 2_000;
/// Retry pause after the exchange refused a panic exit call.
const PANIC_RETRY_MS: i64 = 10_000;

/// The wait before a refused panic exit is tried again: 10 s, doubling to 160 s. An exit the
/// exchange keeps refusing (a dust position under the minimum notional, a reduce-only mismatch)
/// is a thing to look at, not to ask for every ten seconds all night.
fn panic_retry_ms(fails: u32) -> i64 {
    PANIC_RETRY_MS << fails.saturating_sub(1).min(4)
}
/// A fresh fill's units may still be missing from `positionRisk` this long.
const POSITION_GRACE_MS: i64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    /// Entry: BUY for a long, SELL for a short.
    Buy,
    /// Exit.
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Post,
    Cancel,
    Replace,
    Query,
}

/// Exchange work for `trading`.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Post {
        order: u64,
        leg: Leg,
        key: String,
        uid: String,
        lots: i64,
        /// `None` = market order.
        price: Option<f64>,
        sell: bool,
    },
    Cancel {
        order: u64,
        leg: Leg,
        exchange_id: String,
    },
    /// Move the leg's live order: Aster's amend (`PUT /fapi/v3/order`), the
    /// same order under the same `key`, to `price`, `lots` its whole size —
    /// fills included.
    Replace {
        order: u64,
        leg: Leg,
        exchange_id: String,
        key: String,
        uid: String,
        lots: i64,
        price: f64,
    },
    /// Resolve a request whose outcome is unknown, using its idempotency key.
    QueryRequest { order: u64, leg: Leg, key: String },
    /// Re-read a leg whose state is in doubt.
    Query {
        order: u64,
        leg: Leg,
        exchange_id: String,
    },
}

impl Action {
    /// The core order the work is for.
    pub fn order(&self) -> u64 {
        match self {
            Self::Post { order, .. }
            | Self::Replace { order, .. }
            | Self::Cancel { order, .. }
            | Self::Query { order, .. }
            | Self::QueryRequest { order, .. } => *order,
        }
    }
}

/// Outcome of one input.
#[derive(Debug, Default, PartialEq)]
pub struct Effects {
    /// Orders whose image must be pushed.
    pub changed: Vec<u64>,
    /// Lines for the terminal's core log.
    pub logs: Vec<String>,
    pub actions: Vec<Action>,
}

impl Effects {
    fn changed(&mut self, id: u64) {
        if !self.changed.contains(&id) {
            self.changed.push(id);
        }
    }

    pub fn extend(&mut self, other: Effects) {
        for id in other.changed {
            self.changed(id);
        }
        self.logs.extend(other.logs);
        self.actions.extend(other.actions);
    }
}

/// One exchange order. An amend (`Action::Replace`) moves it in place; a leg
/// re-posted after a cancel (an exit moved to MARKET, a refused exit tried
/// again) is a new execution, and late reports of retired executions may add
/// fills but must never change the active order. Orders saved before 03.10
/// may still hold the generations TInvestCore's cancel-and-post replace made.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct Execution {
    key: String,
    ids: HashSet<String>,
    lots: i64,
    filled: i64,
    mean: f64,
    price: f64,
    status: Option<ExecStatus>,
    awaiting_reply: bool,
    /// Historical fills still count, but this order cannot reserve exit lots.
    retired: bool,
    /// Exit generations: the `reason::*` it was placed for (0 = unknown).
    reason: u8,
}

impl Execution {
    fn is_live(&self) -> bool {
        !self.retired && !self.status.is_some_and(ExecStatus::is_final)
    }

    fn exchange_id(&self) -> Option<&str> {
        self.ids
            .iter()
            .find(|id| id.parse::<i64>().is_ok())
            .or_else(|| self.ids.iter().next())
            .map(String::as_str)
    }

    fn matches_key(&self, key: &str) -> bool {
        !key.is_empty() && key == self.key
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Target {
    price: f64,
    size: Option<f64>,
    resize: bool,
    /// An exit at market: `price` is only the reference it is shown at.
    #[serde(default)]
    market: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct ExLeg {
    key: String,
    exchange_id: String,
    replacing: bool,
    /// The core may resize its own exit to the remaining position.
    owned: bool,
    /// Sum of all executions' fills, including retired exchange orders.
    filled_lots: i64,
    /// Filled retired lots plus requested lots of the active execution.
    lots: i64,
    executions: Vec<Execution>,
    deferred: Option<Target>,
    cancel_requested: bool,
    /// A Cancel is queued at the worker; repeated requests (MoonShot retries
    /// every 5 s, stream reports) wait for its outcome instead of duplicating it.
    /// Not persisted: the worker queue does not survive a restart.
    #[serde(skip)]
    cancel_sent: bool,
    /// An uncertain request is queried, never reposted with a fresh key.
    uncertain: bool,
    #[serde(skip)]
    resolve_at: i64,
    resolve_key: String,
    /// The amend in flight (`replacing`): what it asked for, and what the order was before, to
    /// go back to when the exchange refuses it. Not persisted: a restart reads the order
    /// (`restore` makes the leg uncertain), and that is the order's truth.
    #[serde(skip)]
    amend: Option<Amend>,
    #[serde(with = "LegStateDef")]
    state: LegState,
}

/// One amend of a leg's live order: what it asked for, and what the order was before.
#[derive(Debug, Clone, Copy)]
struct Amend {
    price: f64,
    /// The order's whole size it asked for, fills included.
    lots: i64,
    was_price: f64,
    was_lots: i64,
    was_reason: u8,
    /// Its call failed with no answer: whether it landed is for the next read of the order to
    /// say (`apply`), not for the amend's own answer, which will not come.
    unsure: bool,
}

impl Amend {
    /// A report of the order shows something else than this amend asked for — a price or a
    /// size it carries (0 = the report says nothing of it).
    fn differs(&self, price: f64, lots: i64, tick: f64) -> bool {
        (price > 0.0 && (price - self.price).abs() >= tick.max(f64::EPSILON) * 0.5)
            || (lots > 0 && lots != self.lots)
    }
}

/// `LegState` (vendored side, no serde) as persisted by the order store.
#[derive(Serialize, Deserialize)]
#[serde(remote = "LegState")]
struct LegStateDef {
    exchange_id: i64,
    price: f64,
    quantity: f64,
    filled: f64,
    mean_price: f64,
    notional: f64,
    spent: f64,
    create_ms: i64,
    open_ms: i64,
    close_ms: i64,
    is_market: bool,
    opened: bool,
    closed: bool,
    canceled: bool,
}

impl ExLeg {
    fn live_ids(&self, leg: Leg) -> Vec<&str> {
        if leg == Leg::Sell && !self.owned {
            self.executions
                .iter()
                .filter(|e| e.is_live())
                .filter_map(Execution::exchange_id)
                .collect()
        } else {
            (!self.exchange_id.is_empty())
                .then_some(self.exchange_id.as_str())
                .into_iter()
                .collect()
        }
    }

    /// Someone is already going to learn this leg's outcome, so a second call
    /// asking for it is waste: a Cancel sits at the worker (its own `Cancel`
    /// ends in a state read), a Replace is in flight (its failure queries),
    /// or a reconciliation is scheduled.
    fn awaiting_outcome(&self) -> bool {
        self.replacing || self.uncertain || self.cancel_sent
    }

    /// Cancels for the live exchange orders, once per outcome: nothing while
    /// another call already owns the leg's outcome (`awaiting_outcome`).
    fn cancellations(&mut self, order: u64, leg: Leg) -> Vec<Action> {
        if self.awaiting_outcome() {
            return Vec::new();
        }
        let actions: Vec<Action> = self
            .live_ids(leg)
            .into_iter()
            .map(|id| Action::Cancel {
                order,
                leg,
                exchange_id: id.to_owned(),
            })
            .collect();
        self.cancel_sent = !actions.is_empty();
        actions
    }

    fn ensure_execution(&mut self) {
        if self.executions.is_empty() {
            let mut ids = HashSet::new();
            if !self.exchange_id.is_empty() {
                ids.insert(self.exchange_id.clone());
            }
            self.executions.push(Execution {
                key: self.key.clone(),
                ids,
                lots: self.lots,
                filled: self.filled_lots,
                mean: self.state.mean_price,
                price: self.state.price,
                status: None,
                awaiting_reply: !self.state.opened,
                retired: false,
                // A leg without a ledger has no recorded reason.
                reason: 0,
            });
        }
    }

    fn remaining_lots(&self) -> i64 {
        if !self.owned && !self.executions.is_empty() {
            return self
                .executions
                .iter()
                .filter(|e| e.is_live())
                .map(|e| (e.lots - e.filled).max(0))
                .sum();
        }
        self.executions
            .last()
            .map_or((self.lots - self.filled_lots).max(0), |e| {
                (e.lots - e.filled).max(0)
            })
    }

    fn aggregate(&mut self, lot: f64) {
        self.filled_lots = self.executions.iter().map(|e| e.filled).sum();
        self.lots = self.filled_lots + self.remaining_lots();
        self.state.filled = self.filled_lots as f64 * lot;
        self.state.quantity = self.lots as f64 * lot;
        self.state.notional = self.state.quantity * self.state.price;
        self.state.spent = self
            .executions
            .iter()
            .map(|e| e.filled as f64 * e.mean * lot)
            .sum();
        if self.state.filled > 0.0 {
            self.state.mean_price = self.state.spent / self.state.filled;
        }
    }
}

/// One rest of a leg: the moment it came to sit at `price`.
///
/// The line the order actually drew while it waited, which is what a deal's
/// picture has to show — the level it happened to fill at says nothing about
/// how it got there. Not the ledger: `Execution` is that, and it keeps
/// generations rather than positions.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Move {
    /// Unix ms, the clock the picture's time axis is drawn on.
    pub at: i64,
    pub price: f64,
}

/// Rests one leg's line remembers. A picture spans the whole position, so a
/// long-held exit legitimately wants every one of them; past this the oldest
/// go, and the line then starts where the memory does instead of pretending
/// the order was placed there.
const MOVES_MAX: usize = 64;

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CoreOrder {
    pub id: u64,
    pub uid: String,
    pub market: String,
    pub is_short: bool,
    pub status: u8,
    lot: f64,
    tick: f64,
    pub strategy_id: u64,
    /// Last accepted manual entry move; independent of creation/repeat ordering.
    pub entry_moved_at: i64,
    /// Where each leg has rested, oldest first (`note_moves`). An order from
    /// before this was recorded carries none, and its picture falls back to
    /// the flat level it always drew.
    buy_moves: Vec<Move>,
    sell_moves: Vec<Move>,
    buy: ExLeg,
    sell: ExLeg,
    /// The terminal's planned exit (its chart line) and its ratio to the entry
    /// price: the exit is placed at `mean_price × ratio` once the entry fills
    /// (MoonBot's «Try to sell for [actual buy]+N%»).
    planned_sell: f64,
    planned_ratio: f64,
    use_market_stop: bool,
    /// Stop-loss trigger price (0 = off) and the % its exit crosses the book;
    /// a stop set as a % of the entry keeps it to follow the actual fill price.
    stop_price: f64,
    stop_pct: f64,
    stop_spread: f64,
    /// Trailing stop from the terminal's stop editor: the line trails the
    /// best price by `trail_pct` % (0 = off) and its exit crosses the book by
    /// `trail_spread` %; with a `take_profit` price the trailing starts only
    /// once the price has reached it. `trail_peak` is the best price since
    /// it started (0 = not yet).
    trail_pct: f64,
    trail_spread: f64,
    take_profit: f64,
    trail_peak: f64,
    /// The trailing stop fired: the exit crosses the book by `trail_spread`
    /// (0 included), not the stop-loss spread.
    trail_fired: bool,
    /// The exit goes at market: panic, stop-loss, trailing, or the
    /// terminal's «market» close — the emulator's as well as the account's.
    market_exit: bool,
    /// The strategy's own stop as `(price, spread %)`, shown in the image only
    /// (SL:ON and the chart line): the strategy watches it itself, `watch`
    /// never fires on it.
    bot_stop: (f64, f64),
    /// Panic sell: the exit follows the book until it fills.
    panic: bool,
    /// An emulator order (MoonBot `EmulatorMode` / core `emu_mode`): its
    /// exchange work is answered by `emulator`, never sent to the broker,
    /// and the account's orders and positions never reconcile it.
    pub emulator: bool,
    /// A pending order (MoonBot's Pending): the trigger price it waits for,
    /// and whether the price must rise to it (a stop) or fall to it (a
    /// limit). Nothing is placed until then; the image shows status None.
    pending: f64,
    pending_up: bool,
    /// «Immune for clicks»: «Move all» by kind leaves the order alone.
    pub immune: bool,
    /// The trader's hand trade routed to a Manual strategy: kept when that
    /// strategy is deleted, counted as manual (`PenaltyTime`).
    pub hand: bool,
    /// Pending decision for the next exit generation (`reason::*`, 0 = none):
    /// the generation takes and clears it; each keeps its own copy and the
    /// fill decides which counts.
    next_reason: u8,
    /// Earliest time of the next panic exit call.
    #[serde(skip)]
    panic_next: i64,
    /// Panic/stop exits refused, or MARKET ones expired unfilled, in a row: the retry waits
    /// longer each time (`panic_retry_ms`) and starts over when an exit is live (a MARKET one
    /// once it fills) or a panic is asked for again.
    #[serde(skip)]
    panic_fails: u32,
    /// The planned exit was refused by the exchange (`-2022` with a rival order resting, a
    /// margin or price refusal): the position stands without its exit, and the core asks again
    /// (`watch`) until one is live. Persisted — a restart must not forget a position that is
    /// unprotected. `None` is a file from before the field existed (`restore` then judges by the
    /// shape of the leg); `Some(false)` is a decision that stands — an exit that is live, or
    /// cancelled by hand — and is not second-guessed after a restart.
    exit_refused: Option<bool>,
    /// When the next attempt is due (0 = none pending), and how many were refused in a row.
    #[serde(skip)]
    exit_retry_at: i64,
    #[serde(skip)]
    exit_fails: u32,
    rev: u64,
    done_ms: i64,
}

impl CoreOrder {
    /// Blank order on `market` (legs, status and sizes set by the caller).
    fn on(id: u64, market: &Market) -> Self {
        Self {
            id,
            uid: market.symbol.clone(),
            market: market.symbol.clone(),
            lot: market.lot(),
            tick: market.tick_size,
            rev: 1,
            ..Self::default()
        }
    }

    /// Waiting for its trigger: the core holds it and the exchange has never
    /// seen it (`start_pending`).
    pub(crate) fn is_pending(&self) -> bool {
        self.pending > 0.0
    }

    /// Its exit is on the way and not yet resting confirmed: posted with no
    /// answer, being moved (cancelled, its replacement deferred to the
    /// cancel's final report), replaced or reconciled — or, for a filled
    /// entry, an exit the order is still to place (a panic waiting for its
    /// retry, a planned exit). An order with no exit planned has none coming.
    pub(crate) fn exit_unsettled(&self) -> bool {
        let ex = &self.sell;
        match self.status {
            status::SELL_SET => {
                ex.cancel_requested
                    || ex.deferred.is_some()
                    || ex.replacing
                    || ex.uncertain
                    || ex.executions.last().is_some_and(|e| e.awaiting_reply)
            }
            status::BUY_DONE => self.panic || self.planned_ratio > 0.0,
            _ => false,
        }
    }

    /// Filled entry, whole or part: the order holds a position to close.
    pub(crate) fn holds_position(&self) -> bool {
        matches!(self.status, status::BUY_DONE | status::SELL_SET)
            || (self.status == status::BUY_SET && self.buy.filled_lots > 0)
    }

    /// The line a leg drew, oldest first: where it rested and when.
    pub fn moves(&self, leg: Leg) -> &[Move] {
        match leg {
            Leg::Buy => &self.buy_moves,
            Leg::Sell => &self.sell_moves,
        }
    }

    /// Record where each leg rests now.
    ///
    /// Asked, not told. The price is assigned in eight places — a placement, a
    /// replace, a deferred target, a reprice into the band, a stream report, a
    /// fall-back after a refusal — and three of those deliberately do not mark
    /// the order changed, because nobody needed telling: `target`'s deferred
    /// branch and both `fall_back` sites move the resting price and return. A
    /// line built off the changed list alone therefore missed exactly the
    /// moves it exists to draw. So this is called where the core LOOKS at an
    /// order instead: on every `watch` pass (once a second, and after every
    /// exchange report), and on the changed list too, which is what dates a
    /// move to the moment it happened rather than to the next second.
    ///
    /// `now` is the wall clock, because the moment is drawn on a time axis
    /// beside the fill's own — not a duration, which is where the monotonic
    /// clock belongs.
    fn note_moves(&mut self, now: i64) {
        // Half a tick is the same threshold `target` uses to decide a replace
        // is worth sending: below it the order did not move, it was rounded.
        let apart = self.tick.max(f64::EPSILON) * 0.5;
        for (price, line) in [
            (self.buy.state.price, &mut self.buy_moves),
            (self.sell.state.price, &mut self.sell_moves),
        ] {
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let last = line.last();
            if last.is_some_and(|m| (m.price - price).abs() < apart) {
                continue;
            }
            // The wall clock can step back (NTP, a hand on the machine). The
            // line is read as a sorted step function, so a moment before the
            // one before it would cut the drawing short; it costs a `max` here
            // and buys the reader that guarantee.
            let at = last.map_or(now, |m| now.max(m.at));
            line.push(Move { at, price });
            if line.len() > MOVES_MAX {
                line.remove(0);
            }
        }
    }

    fn leg(&mut self, leg: Leg) -> &mut ExLeg {
        match leg {
            Leg::Buy => &mut self.buy,
            Leg::Sell => &mut self.sell,
        }
    }

    /// Exchange direction of a leg.
    pub fn sells(&self, leg: Leg) -> bool {
        (leg == Leg::Sell) != self.is_short
    }

    fn residual_lots(&self) -> i64 {
        (self.buy.filled_lots - self.sell.filled_lots).max(0)
    }

    /// Price the position was opened at (the entry limit until a fill reports).
    fn entry(&self) -> f64 {
        if self.buy.state.mean_price > 0.0 {
            self.buy.state.mean_price
        } else {
            self.buy.state.price
        }
    }

    /// Stop `stop_pct` % beyond the entry, at a tick.
    fn pct_stop(&self) -> f64 {
        let k = if self.is_short { 1.0 } else { -1.0 };
        round_tick(self.entry() * (1.0 + k * self.stop_pct / 100.0), self.tick)
    }

    /// Spread of this order's panic exit.
    fn panic_spread(&self) -> f64 {
        if self.trail_fired {
            self.trail_spread / 100.0
        } else if self.stop_spread > 0.0 {
            self.stop_spread / 100.0
        } else {
            PANIC_SPREAD
        }
    }

    /// Planned exit over the entry price (0 = none): `Orders` places the exit
    /// at `entry × ratio` once the entry fills.
    pub fn planned_ratio(&self) -> f64 {
        self.planned_ratio
    }

    /// Count this order's lots in `step` units instead of its own `lot`: exact or not at all.
    /// `false` (and nothing changed) when some count is not a whole number of new lots.
    fn rescale_lot(&mut self, step: f64) -> bool {
        let ratio = self.lot / step;
        let conv = |v: i64| {
            let x = v as f64 * ratio;
            // Absolute: a relative tolerance would pass a half lot at half a million lots.
            ((x - x.round()).abs() <= 1e-6).then(|| x.round() as i64)
        };
        let mut all = vec![
            self.buy.lots,
            self.buy.filled_lots,
            self.sell.lots,
            self.sell.filled_lots,
        ];
        for ex in [&self.buy, &self.sell] {
            for e in &ex.executions {
                all.push(e.lots);
                all.push(e.filled);
            }
        }
        if all.iter().any(|v| conv(*v).is_none()) {
            return false;
        }
        for ex in [&mut self.buy, &mut self.sell] {
            ex.lots = conv(ex.lots).unwrap_or(ex.lots);
            ex.filled_lots = conv(ex.filled_lots).unwrap_or(ex.filled_lots);
            for e in &mut ex.executions {
                e.lots = conv(e.lots).unwrap_or(e.lots);
                e.filled = conv(e.filled).unwrap_or(e.filled);
            }
        }
        self.lot = step;
        true
    }

    /// The strategy's stop shown in the image as `(price, spread %)`
    /// (price 0 = none).
    pub fn bot_stop(&self) -> (f64, f64) {
        self.bot_stop
    }

    /// Price a live leg is heading to: a move deferred behind an entry
    /// Replace or an exit Cancel in flight, a reconciliation or a
    /// foreign-exit takeover, else the order's
    /// price. `None` while a plain Cancel is in flight: `target` drops a move
    /// then (a takeover keeps taking them into `deferred`).
    pub fn heading(&self, leg: Leg) -> Option<f64> {
        let ex = match leg {
            Leg::Buy => &self.buy,
            Leg::Sell => &self.sell,
        };
        match (&ex.deferred, ex.cancel_requested) {
            (Some(t), _) => Some(t.price),
            (None, true) => None,
            (None, false) => Some(ex.state.price),
        }
    }

    /// Who closed (or is closing) the position: the core's own exit, a
    /// foreign exchange order, or nobody the core saw (`close_outside`).
    pub fn exit_source(&self) -> ExitSource {
        match (self.sell.owned, self.sell.executions.is_empty()) {
            (true, _) => ExitSource::Core,
            (false, false) => ExitSource::Foreign,
            (false, true) if self.sell.filled_lots > 0 => ExitSource::Outside,
            (false, true) => ExitSource::Core,
        }
    }

    /// `SellReason` code of the exit: once closed, of the last generation that
    /// filled (the one that actually sold); before, of the live generation.
    /// A position closed outside the core has no generation: a manual sell.
    pub fn exit_reason(&self) -> u8 {
        let ex = &self.sell.executions;
        let pick = if status::is_terminal(self.status) {
            ex.iter().rev().find(|e| e.filled > 0)
        } else {
            ex.iter().rev().find(|e| e.is_live()).or(ex.last())
        };
        match pick {
            Some(e) => e.reason,
            None if self.exit_source() == ExitSource::Outside => reason::MANUAL_SELL,
            None => 0,
        }
    }

    /// Every exchange order that filled, entry and exit, as a query can find
    /// it: by the exchange's id, else (an id never learned) by our key.
    pub fn filled_orders(&self) -> Vec<FilledOrder> {
        let find = |ids: &mut dyn Iterator<Item = &String>, key: &str| {
            let ids: Vec<&String> = ids.collect();
            match ids.iter().find(|id| broker_id(id)) {
                Some(id) => Some(FilledOrder {
                    id: (*id).clone(),
                    by_request: false,
                }),
                None if key.is_empty() => {
                    log::warn!(
                        "{}: filled order {ids:?} has no broker id or key, no commission",
                        self.market
                    );
                    None
                }
                None => Some(FilledOrder {
                    id: key.to_owned(),
                    by_request: true,
                }),
            }
        };
        let mut out = Vec::new();
        for ex in [&self.buy, &self.sell] {
            if ex.executions.is_empty() {
                if ex.filled_lots > 0 {
                    out.extend(find(&mut std::iter::once(&ex.exchange_id), &ex.key));
                }
                continue;
            }
            for e in ex.executions.iter().filter(|e| e.filled > 0) {
                out.extend(find(&mut e.ids.iter(), &e.key));
            }
        }
        out
    }

    /// Exchange id of the exit, else of the entry.
    pub fn exchange_id(&self) -> &str {
        if self.sell.exchange_id.is_empty() {
            &self.buy.exchange_id
        } else {
            &self.sell.exchange_id
        }
    }

    /// The manual stop takes the image slot: it is the one `watch` fires on.
    pub fn record(&self) -> OrderRecord<'_> {
        let stop = if self.stop_price > 0.0 {
            Some((self.stop_price, self.stop_spread))
        } else {
            (self.bot_stop.0 > 0.0).then_some(self.bot_stop)
        };
        // The chart draws the sell line at the leg price: once the exit filled
        // that is the fill, not a stop limit posted a spread beyond the book.
        let mut sell = self.sell.state;
        if sell.closed && sell.filled > 0.0 && sell.mean_price > 0.0 {
            sell.price = sell.mean_price;
        }
        OrderRecord {
            id: self.id,
            rev: self.rev,
            market: &self.market,
            is_short: self.is_short,
            status: self.status,
            strategy_id: self.strategy_id,
            buy: self.buy.state,
            sell,
            planned_sell: self.planned_sell,
            use_market_stop: self.use_market_stop,
            stop,
            immune: self.immune,
            trailing: (self.trail_pct > 0.0).then_some((self.trail_pct, self.trail_spread)),
            take_profit: (self.take_profit > 0.0).then_some(self.take_profit),
            panic: self.panic,
            sell_reason: self.exit_reason(),
            emulator: self.emulator,
        }
    }
}

/// One leg resting on a side of the book (`Orders::resting`).
#[derive(Debug, Clone, Copy)]
pub struct Resting {
    pub id: u64,
    pub leg: Leg,
    pub price: f64,
    /// USDT the order carries (price × lots × lot).
    pub value: f64,
    pub set_at: i64,
    pub immune: bool,
}

/// «Not found» answers to one request key being reconciled: how many, and when the first came.
#[derive(Debug, Clone, Copy)]
struct RequestMisses {
    count: u32,
    first_ms: i64,
}

#[derive(Default)]
pub struct Orders {
    map: HashMap<u64, CoreOrder>,
    /// Idempotency key / exchange order id -> leg.
    by_key: HashMap<String, (u64, Leg)>,
    by_exchange: HashMap<String, (u64, Leg)>,
    next_id: u64,
    keys: RandomState,
    /// «Not found» answers so far per request key being reconciled.
    request_misses: HashMap<String, RequestMisses>,
    /// Exchange ids and request keys of account orders left to the account
    /// (`adopt`): their later reports stay theirs whatever the positions say.
    /// Kept across restarts (`left_ids`), so bounded to the newest
    /// `LEFT_CAP`: past it the oldest goes (`left_order`).
    left: HashSet<String>,
    left_order: VecDeque<String>,
    /// Orders the last `reconcile` found missing from the account: one read is not enough to
    /// close a position (a lagging or short answer looks the same), the second in a row is.
    gone_once: HashMap<u64, i64>,
    /// Exchange ids whose `Query` answered «not found» once: the second answer, after
    /// `REQUERY_MS`, closes the leg; one alone does not (a read can lag behind the matching).
    query_missed: HashMap<String, i64>,
    /// Second reads due: (at ms, order, leg, exchange id).
    requery: Vec<(i64, u64, Leg, String)>,
}

/// How long a first «not found» of a numeric `Query` is remembered.
const QUERY_MISS_MEMORY_MS: i64 = 60_000;
/// The wait before a numeric `Query` that found nothing is asked again.
const REQUERY_MS: i64 = 2_000;

/// Ids remembered as left to the account, at most.
const LEFT_CAP: usize = 4096;

impl Orders {
    pub fn new() -> Self {
        Self::starting_at(1)
    }

    /// Ids grow from `base`. The terminal keys its retained order lines by id
    /// for the whole session, so ids must not repeat across core restarts:
    /// a reused id revives the old (closed) line under its old market and the
    /// new order never appears on its own chart. The engine passes the start
    /// time in ms; manual orders keep the terminal's random request uid.
    pub fn starting_at(base: u64) -> Self {
        Self {
            next_id: base.max(1),
            ..Self::default()
        }
    }

    pub fn get(&self, id: u64) -> Option<&CoreOrder> {
        self.map.get(&id)
    }

    /// Record where the legs of the orders that changed now rest — the line
    /// a deal's picture is drawn from, dated to the moment it moved. This is
    /// the sharp half; `watch` sweeps the rest, including the repricings that
    /// never mark an order changed at all.
    pub fn note_moves(&mut self, ids: &[u64], now: i64) {
        for id in ids {
            if let Some(o) = self.map.get_mut(id) {
                o.note_moves(now);
            }
        }
    }

    /// The live orders (not finished) for the order store, each leg's
    /// ledger trimmed to what a restart needs: generations with fills, the
    /// live ones of a foreign exit, and the last few keys (the live one and
    /// late reports of its predecessors).
    pub fn persisted(&self) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for o in self.map.values().filter(|o| !status::is_terminal(o.status)) {
            let mut v = match serde_json::to_value(o) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("orders: {} ({:#x}) not saved: {e}", o.market, o.id);
                    continue;
                }
            };
            for leg in ["buy", "sell"] {
                // Only the last generation of an own leg is live (see
                // `remaining_lots`); foreign exits may hold several.
                let owned = v[leg]["owned"].as_bool().unwrap_or(true);
                if let Some(list) = v[leg]["executions"].as_array_mut() {
                    let n = list.len();
                    let mut i = 0;
                    list.retain(|e| {
                        i += 1;
                        let filled = e["filled"].as_i64().unwrap_or(0) > 0;
                        let final_ = matches!(
                            e["status"].as_str(),
                            Some("Filled" | "Cancelled" | "Rejected")
                        );
                        let live = !owned && !e["retired"].as_bool().unwrap_or(false) && !final_;
                        filled || live || i + KEEP_KEYS > n
                    });
                }
            }
            out.push(v);
        }
        out
    }

    /// Ids of the account orders left to the account, for the snapshot,
    /// oldest first (an unchanged set writes the same text).
    pub fn left_ids(&self) -> Vec<String> {
        self.left_order.iter().cloned().collect()
    }

    /// The previous run's `left_ids`.
    pub fn restore_left(&mut self, ids: Vec<String>) {
        for id in ids {
            self.remember_left(id);
        }
    }

    /// Remember `id` as left to the account; past `LEFT_CAP` the oldest goes.
    fn remember_left(&mut self, id: String) {
        if id.is_empty() || !self.left.insert(id.clone()) {
            return;
        }
        self.left_order.push_back(id);
        while self.left_order.len() > LEFT_CAP {
            if let Some(old) = self.left_order.pop_front() {
                self.left.remove(&old);
            }
        }
    }

    /// Take `tick` and the lot from today's catalog: a restored order carries the previous
    /// run's copy, and the exchange may move a market's tick or step between runs. The lot
    /// counts of the order are re-expressed in today's lot when that is exact (every count a
    /// whole number of new lots); when it is not, the order keeps its old lot, which is said
    /// at once — a quantity that cannot be written on the new grid is a refusal to look at,
    /// not a size to guess.
    /// Returns `(refreshed, missing)`: orders on a market today's catalog lacks.
    pub fn respec<'a>(&mut self, market_of: impl Fn(&str) -> Option<&'a Market>) -> (usize, usize) {
        let (mut refreshed, mut missing) = (0, 0);
        for o in self.map.values_mut() {
            let Some(m) = market_of(&o.uid) else {
                missing += 1;
                continue;
            };
            o.tick = m.tick_size;
            // (`lot` 0 is a file without the field: nothing to convert from.)
            if m.step_size > 0.0 && o.lot > 0.0 && (o.lot - m.step_size).abs() > o.lot * 1e-9 {
                let old = o.lot;
                if o.rescale_lot(m.step_size) {
                    log::warn!(
                        "orders: {}: the lot changed from {old} to {}, the order's lots follow",
                        o.market,
                        m.step_size
                    );
                } else {
                    log::error!(
                        "orders: {}: the lot changed from {old} to {} and the order's quantities \
                         are not whole new lots; its orders go out on the NEW grid with the OLD \
                         counts (off by {}×) — close this position by hand",
                        o.market,
                        m.step_size,
                        old / m.step_size
                    );
                }
            }
            refreshed += 1;
        }
        (refreshed, missing)
    }

    /// Orders of the previous run (see `persisted`). Whatever was in flight
    /// at the stop — a Post or Replace without its reply, a leg without an
    /// exchange id — becomes uncertain and is asked by key on the first
    /// `watch`; queued Cancels are forgotten (asked again when needed).
    pub fn restore(&mut self, orders: Vec<CoreOrder>) -> usize {
        let mut n = 0;
        for mut o in orders {
            if status::is_terminal(o.status) || self.map.contains_key(&o.id) {
                continue;
            }
            let id = o.id;
            // A position whose planned exit was refused before it opened stands unprotected:
            // from a file written before the flag existed it is recognised by the shape
            // `retry_exits` looks for (a flag that IS in the file is believed as it is).
            if o.exit_refused.is_none()
                && o.status == status::BUY_DONE
                && o.planned_ratio > 0.0
                && !o.panic
                && o.sell.state.canceled
                && !o.sell.state.opened
            {
                o.exit_refused = Some(true);
            }
            if o.exit_refused == Some(true) {
                // Due at once: it stood unprotected across the restart.
                o.exit_retry_at = 1;
            }
            for leg in [Leg::Buy, Leg::Sell] {
                let live = matches!(
                    (leg, o.status),
                    (Leg::Buy, status::BUY_SET) | (Leg::Sell, status::SELL_SET)
                );
                let ex = o.leg(leg);
                // A stream report can settle a leg before its REST reply, so a
                // stale `awaiting_reply` counts only on a leg still working.
                let in_flight = ex.replacing
                    || (live
                        && (ex.executions.iter().any(|e| e.awaiting_reply)
                            || (!ex.key.is_empty() && ex.exchange_id.is_empty())));
                for e in &mut ex.executions {
                    e.awaiting_reply = false;
                }
                ex.replacing = false;
                if in_flight {
                    ex.uncertain = true;
                    ex.resolve_key = ex
                        .executions
                        .last()
                        .map_or_else(|| ex.key.clone(), |e| e.key.clone());
                }
                let mut keys: Vec<String> = ex.executions.iter().map(|e| e.key.clone()).collect();
                keys.push(ex.key.clone());
                let mut ids: Vec<String> = ex
                    .executions
                    .iter()
                    .flat_map(|e| e.ids.iter().cloned())
                    .collect();
                ids.push(ex.exchange_id.clone());
                for key in keys.into_iter().filter(|k| !k.is_empty()) {
                    self.bind(&key, id, leg);
                }
                for x in ids.into_iter().filter(|x| !x.is_empty()) {
                    self.by_exchange.insert(x, (id, leg));
                }
            }
            // `next_id` stays: it starts at this run's start time, above the
            // generated ids of any earlier run, and `new_id` skips taken ones
            // (a manual order keeps the terminal's random request uid).
            o.rev = o.rev.max(1);
            self.map.insert(id, o);
            n += 1;
        }
        n
    }

    /// A position of the opposite side to `short` is open on `market` in the same mode
    /// (`emulator`): in one-way mode an entry against it would net it on the exchange.
    pub fn holds_against(&self, market: &str, short: bool, emulator: bool) -> bool {
        self.map.values().any(|o| {
            o.uid == market && o.is_short != short && o.emulator == emulator && o.holds_position()
        })
    }

    /// Every order still listed (finished ones until `records` prunes them).
    pub fn iter(&self) -> impl Iterator<Item = &CoreOrder> {
        self.map.values()
    }

    /// Live images plus recently finished ones; older finished orders are dropped.
    pub fn records(&mut self, now_ms: i64) -> Vec<OrderRecord<'_>> {
        let stale: Vec<u64> = self
            .map
            .values()
            .filter(|o| status::is_terminal(o.status) && now_ms - o.done_ms > KEEP_DONE_MS)
            .map(|o| o.id)
            .collect();
        for id in stale {
            self.remove(id);
        }
        let mut recs: Vec<OrderRecord<'_>> = self.map.values().map(CoreOrder::record).collect();
        recs.sort_by_key(|r| r.id);
        recs
    }

    /// Whether an exchange report belongs to a known leg.
    pub fn knows(&self, u: &OrderUpdate) -> bool {
        self.locate(u).is_some()
    }

    /// Active legs whose exchange ids are not in `open` (the exchange's live
    /// list): their final report may have been missed.
    ///
    /// A leg whose outcome another call already owns is skipped. Absence from
    /// the snapshot is the normal look of a Cancel in flight — the exchange
    /// drops the order the moment it takes the cancel — so a sweep that lands
    /// on a session open made this net ask `GetOrderState` a second time for
    /// every entry it was itself withdrawing: 01.10 a filter pass halted 76
    /// strategy/market pairs as the session rolled, 58 cancels became 111
    /// state reads against the group's ceiling of 100/min, and the 11 calls
    /// past it came back `429`. The net is for a leg nobody is asking about.
    pub fn missing_from(&self, open: &[&str]) -> Vec<Action> {
        self.map
            .values()
            .filter(|o| !o.emulator && matches!(o.status, status::BUY_SET | status::SELL_SET))
            .flat_map(|o| {
                let leg = if o.status == status::BUY_SET {
                    Leg::Buy
                } else {
                    Leg::Sell
                };
                let ex = match leg {
                    Leg::Buy => &o.buy,
                    Leg::Sell => &o.sell,
                };
                if ex.awaiting_outcome() {
                    return Vec::new();
                }
                ex.live_ids(leg)
                    .into_iter()
                    .filter(|id| !open.contains(id))
                    .map(|id| Action::Query {
                        order: o.id,
                        leg,
                        exchange_id: id.to_owned(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // ----- emulator ----------------------------------------------------------

    /// «Immune for clicks» on/off (on the image as `OFL_IMMUNE`).
    pub fn set_immune(&mut self, id: u64, on: bool) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        o.immune = on;
        fx.logs.push(format!(
            "{}: immune for clicks {}",
            o.market,
            if on { "on" } else { "off" }
        ));
        self.bump(id, &mut fx);
        fx
    }

    /// Orders of a market with a resting exit (`sells`: SellSet, MoonBot's
    /// «move all sells») or entry (BuySet) — the leg, as the terminal's own
    /// candidate check reads it, not the side of the book a short trades on.
    pub fn resting(&self, uid: &str, sells: bool) -> Vec<Resting> {
        let (want, leg) = if sells {
            (status::SELL_SET, Leg::Sell)
        } else {
            (status::BUY_SET, Leg::Buy)
        };
        self.map
            .values()
            .filter(|o| o.uid == uid && o.status == want)
            .filter_map(|o| {
                let price = o.heading(leg)?;
                let ex = if sells { &o.sell } else { &o.buy };
                let lots = (ex.lots - ex.filled_lots).max(0) as f64;
                Some(Resting {
                    id: o.id,
                    leg,
                    price,
                    // MoonBot's TopVol / LowVol: the money still resting.
                    value: price * lots * o.lot,
                    // An entry moved by hand was set then.
                    set_at: if sells {
                        ex.state.create_ms
                    } else {
                        ex.state.create_ms.max(o.entry_moved_at)
                    },
                    immune: o.immune,
                })
            })
            .collect()
    }

    pub fn set_hand(&mut self, id: u64) {
        if let Some(o) = self.map.get_mut(&id) {
            o.hand = true;
        }
    }

    /// Mark a new order as the emulator's, before its first action leaves.
    pub fn set_emulator(&mut self, id: u64) {
        if let Some(o) = self.map.get_mut(&id) {
            o.emulator = true;
        }
    }

    /// The exchange order of emulated `order`'s `leg` known by `id`: a
    /// request key or an exchange id.
    pub fn emu_order(&self, order: u64, leg: Leg, id: &str) -> Option<EmuOrder> {
        let o = self.map.get(&order).filter(|o| o.emulator)?;
        let ex = match leg {
            Leg::Buy => &o.buy,
            Leg::Sell => &o.sell,
        };
        if let Some(e) = ex
            .executions
            .iter()
            .rev()
            .find(|e| e.matches_key(id) || e.ids.contains(id))
        {
            return Some(EmuOrder::of(o, leg, e));
        }
        // A request no report reached yet has no ledger: the leg is its order.
        let pending =
            ex.executions.is_empty() && !id.is_empty() && (id == ex.key || id == ex.exchange_id);
        pending.then(|| EmuOrder {
            order,
            leg,
            key: ex.key.clone(),
            exchange_id: ex.exchange_id.clone(),
            uid: o.uid.clone(),
            sell: o.sells(leg),
            lots: ex.lots,
            filled: ex.filled_lots,
            price: ex.state.price,
            mean: ex.state.mean_price,
            status: None,
        })
    }

    /// Emulated exchange orders resting on `uid`: each leg's current order,
    /// placed and not final. An amend moves the order in place; a re-post
    /// starts a new generation, so an older one never rests.
    pub fn emu_resting(&self, uid: &str) -> Vec<EmuOrder> {
        let mut out = Vec::new();
        for o in self.map.values().filter(|o| o.emulator && o.uid == uid) {
            for (leg, ex) in [(Leg::Buy, &o.buy), (Leg::Sell, &o.sell)] {
                let Some(e) = ex.executions.last() else {
                    continue;
                };
                if !e.ids.is_empty() && !e.awaiting_reply && e.is_live() && e.filled < e.lots {
                    out.push(EmuOrder::of(o, leg, e));
                }
            }
        }
        out
    }

    /// Markets with a live emulated order.
    pub fn emu_markets(&self) -> HashSet<String> {
        self.map
            .values()
            .filter(|o| o.emulator && !status::is_terminal(o.status))
            .map(|o| o.uid.clone())
            .collect()
    }

    // ----- terminal commands -------------------------------------------------

    /// `Start`: size the entry and place it.
    pub fn start(&mut self, req_uid: u64, s: &StartOrder, market: &Market, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let price = if s.price > 0.0 {
            market.nearest(s.price)
        } else {
            market.last()
        };
        let id = self.new_id(req_uid);
        let order = CoreOrder {
            market: s.market.clone(),
            is_short: s.is_short,
            status: status::BUY_SET,
            strategy_id: s.strategy_id,
            buy: ExLeg {
                owned: true,
                state: LegState {
                    price,
                    create_ms: now_ms,
                    ..LegState::default()
                },
                ..ExLeg::default()
            },
            planned_sell: s.planned_sell,
            // The ratio comes from the price the TERMINAL priced the exit
            // against (the raw chart click), not the tick-snapped entry:
            // dividing by the snapped one moved the take profit off the
            // percent the toolbar shows.
            planned_ratio: match (s.planned_sell > 0.0, s.price > 0.0, price > 0.0) {
                (true, true, _) => s.planned_sell / s.price,
                (true, false, true) => s.planned_sell / price,
                _ => 0.0,
            },
            use_market_stop: s.use_market_stop,
            ..CoreOrder::on(id, market)
        };
        self.map.insert(id, order);
        self.place_entry(id, market, price, s.size, s.price > 0.0, now_ms, &mut fx);
        fx.changed(id);
        fx
    }

    /// Size the entry of `id` to `size` USDT at `price` and post it (a limit
    /// with `limit`, else at market): whole lots, the leg keyed and bound.
    /// Below one lot the order fails instead (`BUY_FAIL`). Shared by `start`
    /// and a pending order's trigger (`pending_due`), so both size and refuse
    /// an entry by the same rules.
    #[allow(clippy::too_many_arguments)]
    fn place_entry(
        &mut self,
        id: u64,
        market: &Market,
        price: f64,
        size: f64,
        limit: bool,
        now_ms: i64,
        fx: &mut Effects,
    ) {
        let lot_value = market.lot_value(price);
        // The exchange's own filters, the order's kind deciding the ceiling:
        // a size the exchange would refuse fails here, with its reason.
        let kind = if limit {
            OrderKind::Limit
        } else {
            OrderKind::Market
        };
        let lots = match market.size_from_notional(size, price, kind) {
            Ok(qty) => (qty / market.lot()).round() as i64,
            Err(e) => {
                let o = self.map.get_mut(&id).expect("order");
                o.status = status::BUY_FAIL;
                o.done_ms = now_ms;
                // The `Refused` gate of the strategies reads this for its cooldown; left at 0 the
                // strategy asks again every pass.
                o.buy.state.close_ms = now_ms;
                fx.logs.push(format!(
                    "{}: {}size {size:.0} USDT at {price}: {e}",
                    o.market,
                    if o.is_pending() { "pending " } else { "" }
                ));
                return;
            }
        };
        let key = self.new_key();
        let o = self.map.get_mut(&id).expect("order");
        o.status = status::BUY_SET;
        o.buy.lots = lots;
        o.buy.key = key.clone();
        o.buy.state.price = price;
        o.buy.state.quantity = lots as f64 * market.lot();
        o.buy.state.notional = lots as f64 * lot_value;
        o.buy.state.create_ms = now_ms;
        let sell = o.is_short;
        self.bind(&key, id, Leg::Buy);
        fx.actions.push(Action::Post {
            order: id,
            leg: Leg::Buy,
            key,
            uid: market.symbol.clone(),
            lots,
            price: limit.then_some(price),
            sell,
        });
    }

    /// A pending order: the core keeps it and places the entry once the
    /// price crosses the trigger (above the market it waits for a rise,
    /// below it for a fall). Nothing reaches the broker meanwhile.
    pub fn start_pending(
        &mut self,
        req_uid: u64,
        s: &StartOrder,
        market: &Market,
        now_ms: i64,
    ) -> Effects {
        let mut fx = Effects::default();
        let trigger = market.nearest(s.price);
        if trigger <= 0.0 {
            return self.fail_start(req_uid, s, "a pending order needs a price", now_ms);
        }
        // The market's own price tells which side of it the trigger sits on,
        // and so whether the order waits for a rise or for a fall. Without a
        // price there is no side to wait for: the order is refused, not armed
        // in a guessed direction.
        if !market.live_price() {
            let reason = format!(
                "{}: no price yet, a pending order needs one to tell the trigger's side",
                s.market
            );
            return self.fail_start(req_uid, s, &reason, now_ms);
        }
        let reference = market.last();
        let id = self.new_id(req_uid);
        let order = CoreOrder {
            market: s.market.clone(),
            is_short: s.is_short,
            status: status::NONE,
            strategy_id: s.strategy_id,
            buy: ExLeg {
                owned: true,
                state: LegState {
                    price: trigger,
                    notional: s.size,
                    create_ms: now_ms,
                    ..LegState::default()
                },
                ..ExLeg::default()
            },
            planned_sell: s.planned_sell,
            planned_ratio: if s.planned_sell > 0.0 && s.price > 0.0 {
                s.planned_sell / s.price
            } else {
                0.0
            },
            use_market_stop: s.use_market_stop,
            pending: trigger,
            pending_up: trigger > reference,
            ..CoreOrder::on(id, market)
        };
        fx.logs.push(format!(
            "{}: pending order at {trigger} ({} the market {reference})",
            s.market,
            if order.pending_up { "above" } else { "below" }
        ));
        fx.changed(id);
        self.map.insert(id, order);
        fx
    }

    /// A pending order the terminal took back.
    pub fn cancel_pending(&mut self, id: u64, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id).filter(|o| o.pending > 0.0) else {
            return fx;
        };
        o.pending = 0.0;
        o.status = status::BUY_CANCEL;
        o.done_ms = now_ms;
        o.buy.state.close_ms = now_ms;
        fx.logs
            .push(format!("{}: pending order cancelled", o.market));
        fx.changed(id);
        fx
    }

    /// Pending orders whose trigger the price has crossed: their entries go
    /// out now, at the trigger price.
    fn pending_due(&mut self, model: &Catalog, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let due: Vec<u64> = self
            .map
            .values()
            .filter(|o| o.pending > 0.0)
            .filter(|o| {
                let Some(m) = model.get(&o.uid) else {
                    return false;
                };
                // A stale feed's last price is not a crossing: the trigger
                // waits until the stream carries the market again, the way an
                // immediate entry is held (`start_order`).
                if !m.fresh() {
                    return false;
                }
                let last = if m.live_price() { m.last() } else { 0.0 };
                last > 0.0
                    && if o.pending_up {
                        last >= o.pending
                    } else {
                        last <= o.pending
                    }
            })
            .map(|o| o.id)
            .collect();
        for id in due {
            let (uid, price, size) = {
                let o = &self.map[&id];
                (o.uid.clone(), o.pending, o.buy.state.notional)
            };
            let Some(m) = model.get(&uid) else {
                continue;
            };
            // A hand entry armed while flat, or before the opposite position opened, is
            // judged again at the trigger, as `start_order` judges an immediate one.
            let (hand, short, emulator) = {
                let o = &self.map[&id];
                (o.strategy_id == 0 || o.hand, o.is_short, o.emulator)
            };
            if hand && self.holds_against(&uid, short, emulator) {
                let o = self.map.get_mut(&id).expect("pending order");
                o.pending = 0.0;
                o.status = status::BUY_FAIL;
                o.done_ms = now_ms;
                o.buy.state.close_ms = now_ms;
                fx.logs.push(format!(
                    "{}: pending order not placed: a {} position is open on it",
                    o.market,
                    if short { "long" } else { "short" }
                ));
                fx.changed(id);
                continue;
            }
            // The pending order carried its USDT budget in `notional`; the
            // entry is sized from it now, at the trigger price.
            self.place_entry(id, m, price, size, true, now_ms, &mut fx);
            let o = self.map.get_mut(&id).expect("pending order");
            o.pending = 0.0;
            if o.status == status::BUY_SET {
                fx.logs
                    .push(format!("{}: pending order triggered at {price}", o.market));
            }
            fx.changed(id);
        }
        fx
    }

    /// A `Start` the core refuses before touching the exchange: the terminal
    /// still gets an order image (BuyFail) and the reason.
    pub fn fail_start(
        &mut self,
        req_uid: u64,
        s: &StartOrder,
        reason: &str,
        now_ms: i64,
    ) -> Effects {
        let id = self.new_id(req_uid);
        self.map.insert(
            id,
            CoreOrder {
                id,
                market: s.market.clone(),
                is_short: s.is_short,
                status: status::BUY_FAIL,
                strategy_id: s.strategy_id,
                buy: ExLeg {
                    state: LegState {
                        price: s.price,
                        notional: s.size,
                        create_ms: now_ms,
                        close_ms: now_ms,
                        ..LegState::default()
                    },
                    ..ExLeg::default()
                },
                planned_sell: s.planned_sell,
                use_market_stop: s.use_market_stop,
                rev: 1,
                done_ms: now_ms,
                ..CoreOrder::default()
            },
        );
        Effects {
            changed: vec![id],
            logs: vec![format!("{}: {reason}", s.market)],
            actions: Vec::new(),
        }
    }

    /// `TargetBuy`: move the entry, `size` (USDT notional) resizes it as well;
    /// `TargetSell`: move or place the exit.
    pub fn target(&mut self, id: u64, leg: Leg, price: f64, size: Option<f64>) -> Effects {
        self.target_impl(
            id,
            leg,
            Target {
                price,
                size,
                resize: false,
                market: false,
            },
        )
    }

    /// Move the exit of `id` to a MARKET order; `price` is the reference it is
    /// shown at until it fills.
    fn target_market(&mut self, id: u64, price: f64) -> Effects {
        self.target_impl(
            id,
            Leg::Sell,
            Target {
                price,
                size: None,
                resize: false,
                market: true,
            },
        )
    }

    /// Strategy reprices preserve the configured USDT budget, even when it
    /// equals the old image notional. Manual chart moves preserve whole lots.
    pub fn target_entry(&mut self, id: u64, price: f64, size: f64) -> Effects {
        self.target_impl(
            id,
            Leg::Buy,
            Target {
                price,
                size: Some(size),
                resize: true,
                market: false,
            },
        )
    }

    /// Re-plans the exit of a pending entry at an absolute price. The ratio
    /// `place_exit` works from is taken against `entry` — the price the move
    /// beside it ASKED for, not the one the order carries: a move deferred
    /// behind a Replace in flight has not changed that one yet, and a ratio
    /// off the old price would come back as a wrong exit the moment the
    /// deferred move lands. `sell` 0 keeps the ratio the entry already has.
    pub fn replan_exit(&mut self, id: u64, entry: f64, sell: f64) -> Effects {
        let mut fx = Effects::default();
        if !entry.is_finite() || !sell.is_finite() || entry <= 0.0 || sell <= 0.0 {
            return fx;
        }
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        // `target_impl` snaps the entry to the tick before it replays the
        // ratio, so the divisor is the snapped price.
        let entry = round_tick(entry, o.tick);
        if o.status != status::BUY_SET || entry <= 0.0 {
            return fx;
        }
        o.planned_sell = round_tick(sell, o.tick);
        o.planned_ratio = o.planned_sell / entry;
        self.bump(id, &mut fx);
        fx
    }

    pub fn target_manual(
        &mut self,
        id: u64,
        leg: Leg,
        price: f64,
        size: Option<f64>,
        now: i64,
    ) -> Effects {
        let fx = self.target(id, leg, price, size);
        if leg == Leg::Buy && fx.changed.contains(&id) {
            self.map.get_mut(&id).expect("changed").entry_moved_at = now;
        }
        fx
    }

    fn target_impl(&mut self, id: u64, leg: Leg, target: Target) -> Effects {
        let mut fx = Effects::default();
        if !target.price.is_finite() || target.price <= 0.0 || !self.map.contains_key(&id) {
            return fx;
        }
        let o = self.map.get_mut(&id).expect("checked");
        let price = round_tick(target.price, o.tick);
        match (leg, o.status) {
            (Leg::Buy, status::BUY_SET) | (Leg::Sell, status::SELL_SET) => {
                let (market, uid) = (o.market.clone(), o.uid.clone());
                let (lot, lot_value) = (o.lot, price * o.lot);
                let (tick, ratio, residual) = (o.tick, o.planned_ratio, o.residual_lots());
                // Taken now: the repost comes after the live exit is final.
                let why = exit_reason_of(o.next_reason, &o.sell);
                let ex = o.leg(leg);
                if ex.cancel_requested {
                    if ex.deferred.is_some() {
                        ex.deferred = Some(target);
                        if leg == Leg::Sell && ex.owned {
                            ex.state.price = price;
                            ex.state.notional = ex.state.quantity * price;
                        }
                        // A Cancel lost to the transport goes out again.
                        fx.actions = ex.cancellations(id, leg);
                    }
                    return fx;
                }
                // Several independent exits placed outside the core may cover
                // this position.
                // Cancel them all before replacing them with one core-owned exit.
                if leg == Leg::Sell && !ex.owned {
                    fx.extend(self.cancel(id, leg));
                    self.map.get_mut(&id).expect("order").sell.deferred = Some(target);
                    self.bump(id, &mut fx);
                    return fx;
                }
                // A Cancel names the exchange's order id, which the reply to
                // the Post or a stream report brings.
                if ex.replacing || ex.uncertain || ex.exchange_id.is_empty() {
                    ex.deferred = Some(target);
                    self.bump(id, &mut fx);
                    return fx;
                }
                ex.ensure_execution();
                let mut lots = if leg == Leg::Sell {
                    residual
                } else {
                    ex.remaining_lots()
                };
                let resized = target
                    .size
                    .filter(|s| target.resize || (s - ex.state.notional).abs() > 1e-6);
                if let (Leg::Buy, Some(size)) = (leg, resized) {
                    if !size.is_finite() || size <= 0.0 || lot_value <= 0.0 {
                        return fx;
                    }
                    // A size of exactly k lots must not floor to k−1 over a last binary digit.
                    let requested = (size / lot_value + 1e-9).floor() as i64;
                    if !target.resize && requested < 1 && ex.filled_lots == 0 {
                        fx.logs.push(format!(
                            "{market}: size {size:.0} USDT is below one lot ({lot_value:.2} USDT)"
                        ));
                        return fx;
                    }
                    lots = if target.resize {
                        requested - ex.filled_lots
                    } else {
                        (requested - ex.filled_lots).max(1)
                    };
                }
                if lots < 1 {
                    // No new lot fits the budget (or the position is already
                    // closed). Remove the old live order instead of leaving it
                    // able to execute above the budget / beyond the position.
                    ex.cancel_requested = true;
                    fx.actions = ex.cancellations(id, leg);
                    fx.logs.push(format!(
                        "{market}: no remaining lots at target {price}, cancelling"
                    ));
                    self.bump(id, &mut fx);
                    return fx;
                }
                if !target.market
                    && (price - ex.state.price).abs() < tick.max(f64::EPSILON) * 0.5
                    && lots == ex.remaining_lots()
                {
                    return fx;
                }
                // An exit going to MARKET is not an amend (LIMIT only): it is
                // cancelled, and its final report posts the remainder.
                if leg == Leg::Sell && target.market {
                    ex.cancel_requested = true;
                    ex.deferred = Some(target);
                    ex.state.price = price;
                    ex.state.notional = ex.state.quantity * price;
                    fx.actions = ex.cancellations(id, leg);
                    o.next_reason = why;
                    self.bump(id, &mut fx);
                    return fx;
                }
                // Aster's amend: the same order, its size the whole order's —
                // its own fills and what is left to buy or to close. Fills that
                // land while the amend is on its way count against that size,
                // so an exit never sells past the position: the reason
                // TInvestCore never moved one (its replace re-placed the full
                // lots while the old order kept filling, TMB SHU6 14.08, ETLN
                // 17.08) and why until 03.10 an exit move was a cancel, a window
                // without an exit and a new post.
                let e = ex.executions.last_mut().expect("ensured");
                let amend = Amend {
                    price,
                    lots: e.filled + lots,
                    was_price: e.price,
                    was_lots: e.lots,
                    was_reason: e.reason,
                    unsure: false,
                };
                e.lots = e.filled + lots;
                e.price = price;
                if leg == Leg::Sell {
                    e.reason = why;
                }
                let (key, total) = (e.key.clone(), e.lots);
                if leg == Leg::Sell {
                    o.next_reason = 0;
                }
                let ex = o.leg(leg);
                ex.amend = Some(amend);
                ex.state.price = price;
                ex.state.closed = false;
                ex.state.canceled = false;
                ex.replacing = true;
                ex.deferred = None;
                ex.aggregate(lot);
                fx.actions.push(Action::Replace {
                    order: id,
                    leg,
                    exchange_id: ex.exchange_id.clone(),
                    key,
                    uid,
                    lots: total,
                    price,
                });
                if leg == Leg::Buy && ratio > 0.0 {
                    o.planned_sell = round_tick(price * ratio, tick);
                }
            }
            (Leg::Sell, status::BUY_DONE) => {
                fx.actions.extend(self.place_exit(id, price, target.market));
            }
            _ => return fx,
        }
        self.bump(id, &mut fx);
        fx
    }

    /// The engine pinned a leg's price to the `PERCENT_PRICE` bound of its side before the call.
    pub fn reprice(&mut self, id: u64, leg: Leg, price: f64) -> bool {
        let Some(o) = self.map.get_mut(&id) else {
            return false;
        };
        let ex = o.leg(leg);
        ex.state.price = price;
        if let Some(e) = ex.executions.last_mut() {
            e.price = price;
        }
        if let Some(a) = ex.amend.as_mut() {
            a.price = price;
        }
        ex.state.notional = ex.state.quantity * price;
        o.rev += 1;
        true
    }

    /// Planned exits the exchange refused before they opened get one more
    /// try. Not wired in this core (only tests call it): it is the MOEX port's answer to a band that arrived late.
    pub fn retry_exits(&mut self, uid: &str) -> Effects {
        let mut fx = Effects::default();
        let ids: Vec<u64> = self
            .map
            .values()
            .filter(|o| o.uid == uid && o.status == status::BUY_DONE && o.planned_ratio > 0.0)
            .filter(|o| o.sell.state.canceled && !o.sell.state.opened)
            .map(|o| o.id)
            .collect();
        for id in ids {
            let o = &self.map[&id];
            let price = round_tick(o.entry() * o.planned_ratio, o.tick);
            fx.logs
                .push(format!("{}: retrying the sell order at {price}", o.market));
            fx.actions.extend(self.place_exit(id, price, false));
            self.bump(id, &mut fx);
        }
        fx
    }

    /// `CancelBuy` on an entry that may still be pending: a pending order
    /// never reached the exchange, so it is taken back in the core, the way
    /// opcode 9 takes it back. Every path that cancels an entry — the
    /// terminal's button, «Cancel ALL», a strategy's own timeout — goes
    /// through here, or a pending order would quietly stay armed.
    pub fn cancel_buy(&mut self, id: u64, now_ms: i64) -> Effects {
        if self.map.get(&id).is_some_and(|o| o.is_pending()) {
            return self.cancel_pending(id, now_ms);
        }
        self.cancel(id, Leg::Buy)
    }

    /// `CancelBuy` / `CancelSell`.
    pub fn cancel(&mut self, id: u64, leg: Leg) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        let active = matches!(
            (leg, o.status),
            (Leg::Buy, status::BUY_SET) | (Leg::Sell, status::SELL_SET)
        );
        if leg == Leg::Sell {
            // A hand cancel of the exit is a decision: no retry of a refused one behind it.
            (o.exit_refused, o.exit_retry_at) = (Some(false), 0);
        }
        let ex = o.leg(leg);
        if active {
            ex.cancel_requested = true;
            ex.deferred = None;
            fx.actions = ex.cancellations(id, leg);
        }
        fx
    }

    /// `ClosePosition`: every open entry on the market goes into panic exit
    /// (one limit through the book, see `panic_exit`), each for its own lots.
    /// Units on the account without a core order are a hold (`adopt`) and
    /// are never sold. `emulator`: the mode the core trades in (the terminal's
    /// emulator mode, or no account) — the emulator closes its own orders only.
    /// `side`: only the long (`false`) or short (`true`) orders; `at_market`:
    /// the exits go at market instead of a limit at the panic spread.
    pub fn close_position(
        &mut self,
        market: &Market,
        emulator: bool,
        side: Option<bool>,
        at_market: bool,
        now_ms: i64,
    ) -> Effects {
        let mut fx = Effects::default();
        let ids: Vec<u64> = self
            .map
            .values()
            .filter(|o| o.uid == market.symbol && o.emulator == emulator)
            .filter(|o| side.is_none_or(|short| o.is_short == short))
            .filter(|o| o.holds_position())
            .map(|o| o.id)
            .collect();
        for id in &ids {
            self.map.get_mut(id).expect("order").market_exit = at_market;
        }
        if ids.is_empty() {
            fx.logs.push(format!(
                "{}: no position of the core to close; the account's units are left alone",
                market.symbol
            ));
        }
        for id in &ids {
            self.set_sell_reason(*id, reason::MANUAL_SELL);
            fx.extend(self.panic_exit(*id, market, now_ms));
        }
        fx
    }

    /// `PanicSellAll`: every position of the core — emulated or not, any
    /// strategy or manual — goes to a panic exit that follows the book
    /// (`panic_exit`); orders already in panic keep their chase.
    pub fn panic_all(&mut self, model: &Catalog, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let ids: Vec<u64> = self
            .map
            .values()
            .filter(|o| o.holds_position() && !o.panic)
            .map(|o| o.id)
            .collect();
        let mut moved = 0;
        for id in ids {
            let uid = &self.map[&id].uid;
            let Some(m) = model.get(uid) else {
                fx.logs.push(format!(
                    "{}: Panic Sell skipped, market not in the catalog",
                    self.map[&id].market
                ));
                continue;
            };
            self.set_sell_reason(id, reason::PANIC_SELL);
            self.at_market(id);
            fx.extend(self.panic_exit(id, m, now_ms));
            moved += 1;
        }
        fx.logs.push(format!(
            "Panic Sell ALL: {moved} position(s) of the core go to panic exits"
        ));
        fx
    }

    /// `Stops`: the stop-loss of an order — a price when `fixed`, else
    /// `level` % beyond the entry; `spread` is what its exit crosses the book.
    pub fn set_stops(
        &mut self,
        id: u64,
        on: bool,
        fixed: bool,
        level: f64,
        spread: f64,
    ) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        let was = (o.stop_pct, o.stop_price, o.stop_spread);
        // A percent is a distance from the entry, whatever its sign on the
        // wire; a stop «on» at 0 % or at no price at all is no stop — at the
        // entry it would fire on the first tick against it.
        let on = on && level.is_finite() && if fixed { level > 0.0 } else { level != 0.0 };
        let level = level.abs();
        let spread = if spread.is_finite() {
            spread.abs()
        } else {
            0.0
        };
        o.stop_pct = if on && !fixed { level } else { 0.0 };
        o.stop_price = match (on, fixed) {
            (false, _) => 0.0,
            (true, true) => level,
            (true, false) => o.pct_stop(),
        };
        // A percent that prices the stop at or below zero (a long's 100 % and
        // more) is no stop either — once the entry is known; before it (a
        // market or pending entry) the percent waits for the fill.
        if on && !fixed && o.stop_price <= 0.0 && o.entry() > 0.0 {
            o.stop_pct = 0.0;
            o.stop_price = 0.0;
        }
        o.stop_spread = if on { spread } else { 0.0 };
        // The terminal sends the whole stop group when any part changes.
        if (o.stop_pct, o.stop_price, o.stop_spread) == was {
            return fx;
        }
        fx.logs.push(if o.stop_price > 0.0 {
            format!(
                "{}: manually turned StopLoss:ON StopPrice: {} spread: {:.2}%",
                o.market,
                o.stop_price,
                o.panic_spread() * 100.0
            )
        } else {
            format!("{}: manually turned StopLoss:OFF", o.market)
        });
        self.bump(id, &mut fx);
        fx
    }

    /// `Stops`, trailing part: a % trailing stop (a fixed one is refused) and
    /// the take-profit price that starts it. A changed level or start re-arms
    /// the trailing from the next price.
    #[allow(clippy::too_many_arguments)]
    pub fn set_trailing(
        &mut self,
        id: u64,
        on: bool,
        fixed: bool,
        level: f64,
        spread: f64,
        tp_on: bool,
        tp: f64,
    ) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        if on && fixed {
            fx.logs.push(format!(
                "{}: a fixed trailing stop is not supported by this core, set it in %",
                o.market
            ));
        }
        let pct = if on && !fixed { level.abs() } else { 0.0 };
        if pct.is_nan() || pct >= 100.0 {
            fx.logs.push(format!(
                "{}: trailing level {level}% is out of range, trailing not changed",
                o.market
            ));
            return fx;
        }
        let spread = if pct > 0.0 { spread.max(0.0) } else { 0.0 };
        let take = if tp_on && tp > 0.0 { tp } else { 0.0 };
        if (pct, spread, take) == (o.trail_pct, o.trail_spread, o.take_profit) {
            return fx;
        }
        if pct != o.trail_pct || take != o.take_profit {
            o.trail_peak = 0.0;
            o.trail_fired = false;
        }
        (o.trail_pct, o.trail_spread, o.take_profit) = (pct, spread, take);
        fx.logs.push(match (pct > 0.0, take > 0.0) {
            (true, true) => format!(
                "{}: manually turned Trailing:ON {pct}% spread: {spread:.2}% from TakeProfit: {take}",
                o.market
            ),
            (true, false) => format!(
                "{}: manually turned Trailing:ON {pct}% spread: {spread:.2}%",
                o.market
            ),
            (false, true) => format!(
                "{}: TakeProfit {take} only starts a trailing stop, and trailing is off",
                o.market
            ),
            (false, false) => format!("{}: manually turned Trailing:OFF", o.market),
        });
        self.bump(id, &mut fx);
        fx
    }

    /// The strategy's stop for the image (`price` 0 = none); a new image
    /// only when it changed, the strategy calls this on every pass.
    pub fn set_bot_stop(&mut self, id: u64, price: f64, spread: f64) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        if o.bot_stop != (price, spread) {
            o.bot_stop = (price, spread);
            self.bump(id, &mut fx);
        }
        fx
    }

    /// A position whose strategy is gone takes the strategy's last bot stop as its own stop
    /// (`Orders::watch` fires on that one, never on the picture). A stop the trader set stays.
    pub fn adopt_bot_stop(&mut self, id: u64) -> Effects {
        let Some(o) = self.map.get(&id) else {
            return Effects::default();
        };
        let (price, spread) = o.bot_stop;
        if o.stop_price > 0.0 || price <= 0.0 {
            return Effects::default();
        }
        let mut fx = self.set_stops(id, true, true, price, spread);
        // The stop is the order's own now; the trader may turn it off, and nothing re-arms it.
        fx.extend(self.set_bot_stop(id, 0.0, 0.0));
        if let Some(o) = self.map.get(&id) {
            fx.logs.push(format!(
                "{}: the strategy is gone, its stop {price} now guards the position",
                o.market
            ));
        }
        fx
    }

    /// Why the exit is (re)placed, for the image and the trade report: set by
    /// whoever moves it before the move itself (a strategy's PriceDown or
    /// stop, a manual stop, panic, close position). The next exit generation
    /// takes it; a deferred or refused move leaves the live one's reason.
    pub fn set_sell_reason(&mut self, id: u64, code: u8) {
        if let Some(o) = self.map.get_mut(&id) {
            o.next_reason = code;
        }
    }

    /// `Panic`: on = sell out now (`panic_exit`) and keep following the book;
    /// off = leave the exit where it is.
    pub fn set_panic(&mut self, id: u64, on: bool, market: &Market, now_ms: i64) -> Effects {
        if on {
            // A panic asked for now is not held back by the refusals of an earlier one.
            if let Some(o) = self.map.get_mut(&id) {
                o.panic_fails = 0;
            }
            self.set_sell_reason(id, reason::PANIC_SELL);
            self.at_market(id);
            return self.panic_exit(id, market, now_ms);
        }
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        o.panic = false;
        o.trail_fired = false;
        o.market_exit = false;
        // The live exit stays a panic one; the next move is a plain sell.
        o.next_reason = reason::SELL_PRICE;
        fx.logs.push(format!("{}: Panic Sell off", o.market));
        self.bump(id, &mut fx);
        fx
    }

    /// Stops and panic exits of open positions; called once a second and
    /// after every exchange report. An armed stop fires when the side we must
    /// hit (bid for a sale, ask for a buy-back) crosses it; a panic exit that
    /// fell behind the book by a tick is moved again after `PANIC_CHASE_MS`.
    pub fn watch(&mut self, model: &Catalog, now_ms: i64) -> Effects {
        let mut fx = self.pending_due(model, now_ms);
        // A position whose planned exit the exchange refused is asked for it again.
        let again: Vec<(u64, f64)> = self
            .map
            .values()
            .filter(|o| {
                o.status == status::BUY_DONE
                    && o.exit_refused == Some(true)
                    && !o.panic
                    && o.planned_ratio > 0.0
                    && o.exit_retry_at > 0
                    && now_ms >= o.exit_retry_at
                    // Not while an exit is already out or being asked about.
                    && !o.sell.uncertain
                    && !o.sell.executions.last().is_some_and(Execution::is_live)
            })
            .map(|o| (o.id, round_tick(o.entry() * o.planned_ratio, o.tick)))
            .collect();
        for (id, price) in again {
            let tries = self.map.get_mut(&id).map_or(0, |o| {
                o.exit_retry_at = 0;
                o.exit_fails + 1
            });
            let placed = self.place_exit(id, price, false);
            if let Some(o) = self.map.get_mut(&id) {
                if placed.is_empty() {
                    // Nothing left to place (no units, no price): no retry behind it.
                    o.exit_refused = Some(false);
                } else {
                    fx.logs.push(format!(
                        "{}: the exit was refused; placing it again at {price} (try {tries})",
                        o.market
                    ));
                }
            }
            if !placed.is_empty() {
                fx.actions.extend(placed);
                self.bump(id, &mut fx);
            }
        }
        for o in self.map.values_mut() {
            let id = o.id;
            // Where the legs rest, taken off the walk that is already here: a
            // reprice that never marked the order changed (`target`'s deferred
            // branch, either `fall_back`) is invisible to the changed list, and
            // the line a deal's picture draws would have lost it.
            o.note_moves(now_ms);
            for leg in [Leg::Buy, Leg::Sell] {
                let ex = o.leg(leg);
                if ex.uncertain && now_ms >= ex.resolve_at {
                    ex.resolve_at = now_ms + RESOLVE_PERIOD_MS;
                    fx.actions.push(Action::QueryRequest {
                        order: id,
                        leg,
                        key: ex.resolve_key.clone(),
                    });
                }
            }
        }
        let (due, wait): (Vec<_>, Vec<_>) = std::mem::take(&mut self.requery)
            .into_iter()
            .partition(|(at, ..)| now_ms >= *at);
        self.requery = wait;
        for (_, id, leg, exchange_id) in due {
            if self
                .map
                .get(&id)
                .is_some_and(|o| !status::is_terminal(o.status))
            {
                fx.actions.push(Action::Query {
                    order: id,
                    leg,
                    exchange_id,
                });
            }
        }
        // Miss counts of requests no longer being reconciled.
        if !self.request_misses.is_empty() {
            let asked: HashSet<&str> = self
                .map
                .values()
                .flat_map(|o| [&o.buy, &o.sell])
                .filter(|ex| ex.uncertain)
                .map(|ex| ex.resolve_key.as_str())
                .collect();
            self.request_misses
                .retain(|k, _| asked.contains(k.as_str()));
        }

        let ids: Vec<u64> = self
            .map
            .values()
            .filter(|o| o.holds_position())
            .filter(|o| o.panic || o.stop_price > 0.0 || o.trail_pct > 0.0)
            .map(|o| o.id)
            .collect();
        for id in ids {
            let o = &self.map[&id];
            let Some(m) = model.get(&o.uid) else {
                continue;
            };
            if o.panic {
                if now_ms >= o.panic_next {
                    fx.extend(self.panic_exit(id, m, now_ms));
                }
                continue;
            }
            let sells = o.sells(Leg::Sell);
            let side = m.quote(sells, now_ms);
            // A stop is decided by the live book, else by a trade of this run: the startup
            // ticker's price is neither.
            let px = if side > 0.0 {
                side
            } else if m.live_price() {
                m.last()
            } else {
                0.0
            };
            // The trailing follows the live book only: a stale `last` would
            // ratchet its best price.
            let hit = o.stop_price > 0.0
                && px > 0.0
                && if sells {
                    px <= o.stop_price
                } else {
                    px >= o.stop_price
                };
            if !hit {
                if o.trail_pct > 0.0 && side > 0.0 && o.entry() > 0.0 {
                    fx.extend(self.trail(id, sells, side, m, now_ms));
                }
                continue;
            }
            fx.logs.push(format!(
                "{}: StopLoss activated on price drop: {} = {px} BuyPrice = {} StopPrice: {} spread: {:.2}%",
                o.market,
                if sells { "BID" } else { "ASK" },
                o.entry(),
                o.stop_price,
                o.panic_spread() * 100.0
            ));
            self.set_sell_reason(id, reason::STOP_LOSS);
            self.at_market(id);
            fx.extend(self.panic_exit(id, m, now_ms));
        }
        fx
    }

    /// Trailing stop of a position at the price it would exit at (`px`: the
    /// bid for a sale, the ask for a buy-back): starts at the take profit (or
    /// at once), follows the best price, and fires when the price falls
    /// `trail_pct` % back from it — a panic exit with the trailing spread.
    fn trail(&mut self, id: u64, sells: bool, px: f64, m: &Market, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let o = self.map.get_mut(&id).expect("order");
        let beyond = |a: f64, b: f64| if sells { a >= b } else { a <= b };
        if o.trail_peak <= 0.0 {
            if o.take_profit > 0.0 && !beyond(px, o.take_profit) {
                return fx;
            }
            o.trail_peak = px;
            fx.logs
                .push(format!("{}: Trailing started at {px}", o.market));
            return fx;
        }
        if beyond(px, o.trail_peak) {
            o.trail_peak = px;
            return fx;
        }
        let k = if sells { -1.0 } else { 1.0 };
        let line = o.trail_peak * (1.0 + k * o.trail_pct / 100.0);
        if !beyond(line, px) {
            return fx;
        }
        fx.logs.push(format!(
            "{}: Trailing stop activated: {} = {px} best price: {} line: {line:.6} ({}%)",
            o.market,
            if sells { "BID" } else { "ASK" },
            o.trail_peak,
            o.trail_pct
        ));
        o.trail_fired = true;
        self.set_sell_reason(id, reason::TRAILING);
        self.at_market(id);
        fx.extend(self.panic_exit(id, m, now_ms));
        fx
    }

    /// A strategy's fired stop (`moonshot::Cmd::Move` with `market`): the exit goes
    /// at MARKET, emulated or not, and `watch` follows it as a panic exit from
    /// here on.
    pub fn stop_out(&mut self, id: u64, code: u8, m: &Market, now_ms: i64) -> Effects {
        self.set_sell_reason(id, code);
        self.at_market(id);
        self.panic_exit(id, m, now_ms)
    }

    /// The exit of `id` goes at market, emulated or not.
    fn at_market(&mut self, id: u64) {
        if let Some(o) = self.map.get_mut(&id) {
            o.market_exit = true;
        }
    }

    /// Move the exit to MARKET (`market_exit`), or else to a limit
    /// `panic_spread` through the book: placed from BuyDone, a limit replaced
    /// from SellSet when the current exit sits at least a
    /// tick behind it. Marks the order panic so `watch` keeps following.
    fn panic_exit(&mut self, id: u64, m: &Market, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        let armed = std::mem::replace(&mut o.panic, true);
        if !armed {
            self.bump(id, &mut fx);
        }
        // The core's own resting entries that would stand against this exit (a long entry beside
        // a short's buy-back, the other way round for a long): on a one-way account the
        // exchange refuses a reduce-only order that such an order could flip (`-2022`), and
        // a panic that cannot be placed is no panic. Taken off first, and said.
        let (uid, short, emulated) = {
            let o = &self.map[&id];
            (o.uid.clone(), o.is_short, o.emulator)
        };
        if !emulated {
            let rivals: Vec<u64> = self
                .map
                .values()
                .filter(|r| {
                    r.id != id
                        && r.uid == uid
                        && !r.emulator
                        && r.is_short != short
                        && r.status == status::BUY_SET
                })
                .map(|r| r.id)
                .collect();
            for rival in rivals {
                fx.logs.push(format!(
                    "{uid}: cancelling the resting entry {rival:#x} that stands against this exit"
                ));
                fx.extend(self.cancel(rival, Leg::Buy));
            }
        }
        let o = &self.map[&id];
        if o.status == status::BUY_SET && o.buy.filled_lots > 0 {
            self.map.get_mut(&id).expect("order").panic_next = now_ms + PANIC_CHASE_MS;
            fx.extend(self.cancel(id, Leg::Buy));
            return fx;
        }
        let sells = o.sells(Leg::Sell);
        let at_market = o.market_exit;
        let spread = o.panic_spread();
        // For a MARKET exit, the price it is shown at until it fills.
        let Some(price) = m
            .marketable_at(sells, spread, now_ms)
            .map(|p| m.within_limits(p, !sells))
        else {
            if !armed {
                fx.logs
                    .push(format!("{}: no price to sell at yet", o.market));
            }
            return fx;
        };
        let behind = if sells {
            o.sell.state.price - price
        } else {
            price - o.sell.state.price
        };
        let act = match o.status {
            status::BUY_DONE => true,
            // A MARKET exit is not chased: it takes the book as it stands.
            status::SELL_SET if at_market => !(o.sell.owned && o.sell.state.is_market),
            status::SELL_SET => !o.sell.owned || behind >= o.tick.max(f64::EPSILON),
            _ => false, // not filled yet: the flag waits for the fill
        };
        if !act {
            return fx;
        }
        let market = o.market.clone();
        let call = match (o.status == status::SELL_SET, at_market) {
            (true, true) => self.target_market(id, price),
            (true, false) => self.target(id, Leg::Sell, price, None),
            (false, _) => Effects {
                actions: self.place_exit(id, price, at_market),
                ..Effects::default()
            },
        };
        // Deferred behind an in-flight Replace / reconciliation: nothing to report
        // (EUTR 17.09: ~30 identical lines per order while the leg waited).
        if !call.actions.is_empty() {
            fx.logs.push(if at_market {
                format!("{market}: Panic Sell at MARKET (~{price})")
            } else {
                format!(
                    "{market}: Panic Sell Replacing <Spread: {:.2}%> => {price}",
                    spread * 100.0
                )
            });
        }
        fx.extend(call);
        let o = self.map.get_mut(&id).expect("listed");
        o.panic_next = now_ms + PANIC_CHASE_MS;
        self.bump(id, &mut fx);
        fx
    }

    // ----- exchange reports --------------------------------------------------

    /// Apply a report for a known leg (see `knows`).
    pub fn apply(&mut self, u: &OrderUpdate, now_ms: i64) -> Effects {
        let mut fx = Effects::default();
        let Some((id, leg)) = self.locate(u) else {
            return fx;
        };
        let o = self.map.get_mut(&id).expect("located");
        let (lot, tick) = (o.lot, o.tick);
        let ex = o.leg(leg);
        ex.ensure_execution();
        let foreign_exit = leg == Leg::Sell && !ex.owned;
        // An exit move waits for the cancelled order's final report.
        let reprice = leg == Leg::Sell && ex.cancel_requested && ex.deferred.is_some();
        // An end the core asked for is not the exchange giving up on the order.
        let cancel_asked = ex.cancel_requested;
        let which = ex
            .executions
            .iter()
            .rposition(|e| e.matches_key(&u.request_id))
            .or_else(|| {
                ex.executions
                    .iter()
                    .rposition(|e| e.ids.contains(&u.exchange_id))
            })
            .unwrap_or(ex.executions.len() - 1);
        let current = which == ex.executions.len() - 1;
        // An amend on its way: a report of the order as it stood before — the
        // stream racing the amend, the answer to an earlier amend or to a read
        // sent before it — brings its fills, but must not put the old price and
        // size back or end the move. A report of what the amend asked for, the
        // order's end, or, after the amend's call failed with no answer, the
        // read of the order settle it.
        let stale_amend = current
            && !u.status.is_final()
            && ex.amend.is_some_and(|a| {
                a.differs(u.price, u.lots_requested, tick) && !(a.unsure && u.unary)
            });
        let patched;
        let u = if stale_amend {
            patched = OrderUpdate {
                price: 0.0,
                lots_requested: 0,
                ..u.clone()
            };
            &patched
        } else {
            u
        };
        let e = &mut ex.executions[which];
        // A stream rejection while the call is out: the call's own reply
        // settles the request. Wait for it before making the position retryable.
        if e.awaiting_reply && !u.unary && u.status == ExecStatus::Rejected {
            return fx;
        }
        if u.unary {
            e.awaiting_reply = false;
        }
        if !u.exchange_id.is_empty() {
            e.ids.insert(u.exchange_id.clone());
            self.by_exchange.insert(u.exchange_id.clone(), (id, leg));
        }
        let (known, known_mean) = (e.filled, e.mean);
        let was_final = e.status.is_some_and(ExecStatus::is_final);
        if u.price > 0.0 && u.lots_executed >= known {
            e.price = u.price;
        }
        e.filled = e.filled.max(u.lots_executed).max(0);
        if u.avg_price > 0.0 && u.lots_executed > 0 && u.lots_executed >= known {
            e.mean = u.avg_price;
        }
        if u.lots_requested > 0 {
            e.lots = u.lots_requested.max(e.filled);
        }
        // Finality and fills are monotonic within one exchange order. A late
        // NEW snapshot cannot undo a partial fill or a confirmed cancellation.
        if !e.status.is_some_and(ExecStatus::is_final)
            || e.filled > known
            || u.status == ExecStatus::Filled
        {
            e.status = Some(if !u.status.is_final() && e.filled > 0 {
                ExecStatus::PartiallyFilled
            } else {
                u.status
            });
        }
        let current_status = e.status.unwrap_or(u.status);
        let exec_filled = e.filled;
        if !current && !foreign_exit && e.filled == known && e.mean == known_mean {
            return fx;
        }
        if current {
            let fresh = ex.replacing || ex.exchange_id.is_empty();
            if !u.exchange_id.is_empty()
                && (fresh || (broker_id(&u.exchange_id) && !broker_id(&ex.exchange_id)))
            {
                ex.exchange_id = u.exchange_id.clone();
                ex.state.exchange_id = wire_id(&u.exchange_id);
            }
            if !stale_amend {
                // A read showing the amend never landed: the exit keeps the
                // reason it was placed for.
                if let Some(a) = ex.amend.take() {
                    if a.differs(u.price, u.lots_requested, tick) {
                        if let Some(e) = ex.executions.last_mut() {
                            e.reason = a.was_reason;
                        }
                    }
                }
                ex.replacing = false;
                ex.uncertain = false;
            }
            if u.price > 0.0 && !u.is_market && u.lots_executed >= known {
                ex.state.price = u.price;
            }
            ex.state.is_market = u.is_market;
            ex.state.opened |= current_status != ExecStatus::Rejected;
            if ex.state.open_ms == 0 && ex.state.opened {
                ex.state.open_ms = if u.time_ms > 0 { u.time_ms } else { now_ms };
            }
            if current_status.is_final() {
                if ex.state.close_ms == 0 {
                    ex.state.close_ms = now_ms;
                }
                ex.state.closed = current_status == ExecStatus::Filled;
                ex.state.canceled = current_status != ExecStatus::Filled;
                if !foreign_exit {
                    ex.cancel_requested = false;
                    ex.cancel_sent = false;
                    if !reprice {
                        ex.deferred = None;
                    }
                }
            }
        }
        if foreign_exit {
            // The displayed leg stays active until every independent exit ends.
            if let Some(i) = ex.executions.iter().rposition(Execution::is_live) {
                let last = ex.executions.len() - 1;
                ex.executions.swap(i, last);
                let live = &ex.executions[last];
                ex.key = live.key.clone();
                ex.exchange_id = live.exchange_id().unwrap_or_default().to_owned();
                ex.state.exchange_id = wire_id(&ex.exchange_id);
                ex.state.price = live.price;
                ex.state.closed = false;
                ex.state.canceled = false;
                ex.state.close_ms = 0;
            } else {
                ex.cancel_requested = false;
                ex.cancel_sent = false;
            }
        }
        ex.aggregate(lot);
        let final_status = ex
            .executions
            .last()
            .and_then(|e| e.status)
            .filter(|st| st.is_final());
        let filled = ex.filled_lots > 0;
        let previous = o.status;
        let market = o.market.clone();
        let next = match leg {
            Leg::Buy if final_status.is_none() => status::BUY_SET,
            Leg::Buy
                if filled
                    && o.sell
                        .executions
                        .last()
                        .is_some_and(|e| !e.status.is_some_and(ExecStatus::is_final)) =>
            {
                status::SELL_SET
            }
            Leg::Buy if filled && o.residual_lots() == 0 => status::SELL_DONE,
            Leg::Buy if filled => status::BUY_DONE,
            Leg::Buy if final_status == Some(ExecStatus::Rejected) => status::BUY_FAIL,
            Leg::Buy => status::BUY_CANCEL,
            Leg::Sell if final_status.is_none() => status::SELL_SET,
            Leg::Sell if o.residual_lots() > 0 => status::BUY_DONE,
            Leg::Sell => status::SELL_DONE,
        };
        if current && u.status == ExecStatus::Rejected {
            fx.logs
                .push(format!("{market}: order rejected {}", u.message));
            if leg == Leg::Sell && o.panic {
                o.panic_fails = o.panic_fails.saturating_add(1);
                o.panic_next = now_ms + panic_retry_ms(o.panic_fails);
            } else if leg == Leg::Sell
                && o.planned_ratio > 0.0
                && !o.sell.state.opened
                && o.exit_retry_at == 0
            {
                // The planned exit, refused before it ever stood: asked again, less and less
                // often. (Not an exit the user or a strategy moved — it has stood, and what
                // they asked for is not ours to override with the plan; and not the same
                // refusal reported twice, by the reply and by the stream.)
                o.exit_refused = Some(true);
                o.exit_fails = o.exit_fails.saturating_add(1);
                o.exit_retry_at = now_ms + panic_retry_ms(o.exit_fails);
            }
        } else if current
            && leg == Leg::Sell
            && !foreign_exit
            && o.panic
            && u.is_market
            && current_status == ExecStatus::Cancelled
            && !was_final
            && !cancel_asked
        {
            // A MARKET exit the exchange expired: off-session a stock perp fills only within
            // 5 % of the mark. Some lots sold — the rest goes again after PANIC_CHASE_MS. None
            // — nothing stands inside the cap, and asking every 2 s all night is a burst of
            // orders against the exchange's limit (a panic over 40 stocks): it waits like a
            // refusal does.
            if exec_filled == 0 {
                o.panic_fails = o.panic_fails.saturating_add(1);
                let wait = panic_retry_ms(o.panic_fails);
                o.panic_next = now_ms + wait;
                fx.logs.push(format!(
                    "{market}: the MARKET exit expired unfilled (nothing inside the exchange's \
                     price cap); next try in {} s",
                    wait / 1000
                ));
            } else {
                o.panic_fails = 0;
            }
        } else if current
            && leg == Leg::Sell
            && matches!(
                u.status,
                ExecStatus::New | ExecStatus::PartiallyFilled | ExecStatus::Filled
            )
        {
            // The exit is live: the refusals before it are over. A MARKET one counts for the
            // panic only once it fills — it may still expire unfilled (above).
            if u.status != ExecStatus::New || !u.is_market {
                o.panic_fails = 0;
            }
            (o.exit_refused, o.exit_fails, o.exit_retry_at) = (Some(false), 0, 0);
        }
        self.set_status(id, next, now_ms);
        let o = self.map.get_mut(&id).expect("order");
        if leg == Leg::Buy && filled && o.stop_pct > 0.0 {
            o.stop_price = o.pct_stop();
        }
        if leg == Leg::Buy && next == status::BUY_DONE && o.panic {
            o.panic_next = now_ms;
        }
        if next == status::BUY_DONE
            && leg == Leg::Buy
            && previous != status::BUY_DONE
            && o.planned_ratio > 0.0
            && !o.panic
        {
            let price = round_tick(o.entry() * o.planned_ratio, o.tick);
            fx.logs.push(format!("{market}: Buy order DONE! Avg.Price: {} Try to sell for [actual buy]{:+.2}% = {price}", o.entry(), (o.planned_ratio - 1.0) * 100.0));
            fx.actions.extend(self.place_exit(id, price, false));
        }
        // Cancellation has priority over deferred repricing. In particular a
        // partial entry filled while Replace was in flight must stop entering.
        let o = self.map.get_mut(&id).expect("order");
        let ex = o.leg(leg);
        if (foreign_exit || reprice) && final_status.is_some() {
            if let Some(target) = ex.deferred.take() {
                fx.extend(self.target_impl(id, leg, target));
            }
        } else if (current || foreign_exit) && final_status.is_none() {
            if ex.cancel_requested {
                fx.actions.extend(ex.cancellations(id, leg));
            } else if let Some(target) = ex.deferred.take() {
                fx.extend(self.target_impl(id, leg, target));
            }
        }
        // A late fill of a retired order changes the remaining position. The
        // current exit must only cover that remainder, even at the same price.
        // A pending exit move posts the remainder anyway, at its own target.
        let o = &self.map[&id];
        if o.status == status::SELL_SET
            && o.sell.owned
            && !o.sell.replacing
            && !o.sell.uncertain
            && !o.sell.cancel_requested
            && o.sell.remaining_lots() != o.residual_lots()
        {
            let price = o.sell.state.price;
            fx.extend(self.target(id, Leg::Sell, price, None));
        }
        self.bump(id, &mut fx);
        fx
    }

    /// Exchange order not known to the core (found on the account). The core
    /// manages only what it opened: units the account holds beyond its
    /// positions are a hold bought outside it. A foreign order is taken as
    /// the exit of a core position (a sale from the exchange's own app)
    /// when it closes one and, for a sale, the hold cannot cover it; any
    /// other — a purchase, a sale of the hold — is left to the account with
    /// all its later reports. `held` is the account's units of the instrument
    /// (the last `GetPositions`, blocked included); `None` before the first
    /// one, when a sale cannot be told from the hold's: that report is
    /// skipped, not remembered, and the order's next one decides.
    pub fn adopt(
        &mut self,
        u: &OrderUpdate,
        market: &Market,
        held: Option<f64>,
        now_ms: i64,
    ) -> Effects {
        if u.status == ExecStatus::Rejected {
            return Effects::default(); // never opened, nothing to close or track
        }
        if self.knows(u) {
            return self.apply(u, now_ms);
        }
        if self.left.contains(&u.exchange_id)
            || (!u.request_id.is_empty() && self.left.contains(&u.request_id))
        {
            self.leave(u);
            return Effects::default();
        }
        let lots = u.lots_requested.max(u.lots_executed);
        let hold_lots = match held {
            _ if !u.sell => 0,
            Some(held) if market.lot() > 0.0 => {
                ((held - self.core_long_units(&market.symbol)) / market.lot() + 1e-9).floor() as i64
            }
            Some(_) => i64::MAX,
            // Not remembered: the next report is judged once the hold is known.
            None => {
                return Effects {
                    logs: vec![format!(
                        "{}: sale {} placed outside the core waits for the account's positions",
                        market.symbol, u.exchange_id
                    )],
                    ..Effects::default()
                }
            }
        };
        if hold_lots < lots {
            if let Some(id) = self.closable_by(u, market) {
                return self.attach_exit(id, u, now_ms);
            }
        }
        self.leave(u);
        let side = if u.sell { "sale" } else { "purchase" };
        Effects {
            logs: vec![format!(
                "{}: {side} {} of {lots} lot(s) placed outside the core is left to the account",
                market.symbol, u.exchange_id
            )],
            ..Effects::default()
        }
    }

    /// Remember `u`'s ids as an order left to the account.
    fn leave(&mut self, u: &OrderUpdate) {
        self.remember_left(u.exchange_id.clone());
        if !u.request_id.is_empty() {
            self.remember_left(u.request_id.clone());
        }
    }

    /// Units of the real long positions the core holds on `uid`.
    fn core_long_units(&self, uid: &str) -> f64 {
        self.map
            .values()
            .filter(|o| !o.emulator && !o.is_short && o.uid == uid)
            .filter(|o| matches!(o.status, status::BUY_DONE | status::SELL_SET))
            .map(|o| o.residual_lots() as f64 * o.lot)
            .sum()
    }

    /// Oldest order in position on `market` that a foreign report of `u`'s
    /// direction closes: one of its size, else any (`reconcile` squares the rest).
    fn closable_by(&self, u: &OrderUpdate, market: &Market) -> Option<u64> {
        let lots = u.lots_requested.max(u.lots_executed);
        let mut held: Vec<&CoreOrder> = self
            .map
            .values()
            .filter(|o| {
                let reserved = if o.status == status::SELL_SET {
                    o.sell.remaining_lots()
                } else {
                    0
                };
                !o.emulator
                    && o.uid == market.symbol
                    && o.sells(Leg::Sell) == u.sell
                    && (o.status == status::BUY_DONE
                        || (o.status == status::SELL_SET
                            && !o.sell.owned
                            && o.residual_lots() - reserved >= lots))
            })
            .collect();
        held.sort_by_key(|o| (o.buy.state.close_ms, o.id));
        held.iter()
            .find(|o| o.residual_lots() == lots)
            .or(held.first())
            .map(|o| o.id)
    }

    /// Track a foreign exchange order as the exit leg of `id`.
    fn attach_exit(&mut self, id: u64, u: &OrderUpdate, now_ms: i64) -> Effects {
        let o = self.map.get_mut(&id).expect("closable");
        let continuing = o.status == status::SELL_SET;
        let lots = u.lots_requested.max(u.lots_executed);
        let ex = &mut o.sell;
        if !ex.key.is_empty() || ex.filled_lots > 0 {
            ex.ensure_execution();
        }
        if !continuing {
            for e in &mut ex.executions {
                e.retired = true;
            }
        }
        ex.key = u.request_id.clone();
        ex.exchange_id.clear();
        ex.executions.push(Execution {
            key: u.request_id.clone(),
            lots,
            price: u.price,
            reason: reason::MANUAL_SELL,
            ..Execution::default()
        });
        ex.replacing = false;
        ex.uncertain = false;
        ex.owned = false;
        ex.cancel_sent = false; // a new exchange id: any pending Cancel targets it
        if !continuing {
            ex.deferred = None;
            ex.cancel_requested = false;
        }
        ex.state = LegState {
            price: u.price,
            notional: (ex.filled_lots + lots) as f64 * o.lot * u.price,
            create_ms: now_ms,
            ..LegState::default()
        };
        ex.aggregate(o.lot);
        o.status = status::SELL_SET;
        let mut fx = Effects::default();
        fx.logs.push(format!(
            "{}: order {} placed outside the core closes this position",
            o.market, u.exchange_id
        ));
        if !u.request_id.is_empty() {
            self.by_key.insert(u.request_id.clone(), (id, Leg::Sell));
        }
        self.by_exchange
            .insert(u.exchange_id.clone(), (id, Leg::Sell));
        fx.extend(self.apply(u, now_ms));
        fx
    }

    /// Account positions against orders in position: units that left the
    /// account outside the core (a sale from the exchange's app, a report missed
    /// while the stream was down) close their BuyDone orders as SellDone at
    /// the market price, oldest first. Orders with a live exit of their own
    /// are left to their reports — the exchange blocks their units anyway; a
    /// foreign (keyless) exit may have filled under an id the core never
    /// matched, so it counts as closable too. So does an exit whose outcome is
    /// being reconciled (`uncertain`): a live exit keeps its units blocked, and
    /// blocked units count as held (EUTR 18.09: the refused re-posted exit
    /// queried its request key every 10 s while the account held nothing).
    pub fn reconcile(
        &mut self,
        held: &HashMap<String, f64>,
        model: &Catalog,
        now_ms: i64,
    ) -> Effects {
        let mut by_uid: HashMap<&str, Vec<&CoreOrder>> = HashMap::new();
        for o in self.map.values().filter(|o| !o.emulator) {
            if matches!(o.status, status::BUY_DONE | status::SELL_SET) {
                by_uid.entry(o.uid.as_str()).or_default().push(o);
            }
        }
        let mut gone = Vec::new();
        for (uid, orders) in by_uid {
            // The exchange may not have booked a recent fill yet: such an
            // order is left out, whether or not its exit is already placed
            // (MAGE 22.09: an exit known by its stream alias only closed the
            // entry 30 ms after it filled). Its units may still be in the
            // snapshot, which only makes the rest look less short.
            let settled = |o: &&CoreOrder| now_ms - o.buy.state.close_ms >= POSITION_GRACE_MS;
            let units = held.get(uid).copied().unwrap_or(0.0);
            for short in [false, true] {
                let mut side: Vec<&CoreOrder> = orders
                    .iter()
                    .copied()
                    .filter(|o| o.is_short == short)
                    .filter(settled)
                    .collect();
                let avail = if short { -units } else { units }.max(0.0);
                let need: f64 = side.iter().map(|o| o.residual_lots() as f64 * o.lot).sum();
                let mut excess = need - avail;
                side.sort_by_key(|o| o.buy.state.close_ms);
                // An exit the exchange never confirmed cannot hold units the
                // account lacks.
                let closable = |o: &CoreOrder| {
                    o.status == status::BUY_DONE || o.sell.key.is_empty() || o.sell.uncertain
                };
                for o in side.iter().filter(|o| closable(o)) {
                    let remaining = o.residual_lots() as f64 * o.lot;
                    if remaining > excess + 1e-9 {
                        break;
                    }
                    excess -= remaining;
                    gone.push((o.id, avail, need));
                }
            }
        }
        // First seen missing at one read, closed when a read at least `POSITION_GRACE_MS`
        // later still lacks it: reads a couple of seconds apart (the stream wakes them) can
        // share one lagging answer.
        let first: HashMap<u64, i64> = gone
            .iter()
            .map(|(id, _, _)| (*id, self.gone_once.get(id).copied().unwrap_or(now_ms)))
            .collect();
        let confirmed: Vec<_> = gone
            .into_iter()
            .filter(|(id, _, _)| now_ms - first[id] >= POSITION_GRACE_MS)
            .collect();
        self.gone_once = first;
        let mut fx = Effects::default();
        for (id, avail, need) in confirmed {
            fx.extend(self.close_outside(id, model, avail, need, now_ms));
            self.gone_once.remove(&id);
        }
        fx
    }

    /// What the last reads saw missing is forgotten (the account became unknown): the next
    /// sighting is a first one.
    pub fn forget_gone(&mut self) {
        self.gone_once.clear();
    }

    /// The position of `id` is gone from the account: book its exit as filled
    /// at the current market price.
    fn close_outside(
        &mut self,
        id: u64,
        model: &Catalog,
        avail: f64,
        need: f64,
        now_ms: i64,
    ) -> Effects {
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        let market = model.get(&o.uid);
        let quote = market.map_or(0.0, |m| {
            let side = m.quote(o.sells(Leg::Sell), now_ms);
            if side > 0.0 {
                side
            } else {
                m.last()
            }
        });
        let price = if quote > 0.0 { quote } else { o.entry() };
        let units = o.buy.state.filled;
        let spent = o.sell.state.spent + o.residual_lots() as f64 * o.lot * price;
        let mean = if units > 0.0 { spent / units } else { price };
        let old = std::mem::take(&mut o.sell.key);
        o.sell = ExLeg {
            lots: o.buy.filled_lots,
            filled_lots: o.buy.filled_lots,
            state: LegState {
                price,
                mean_price: mean,
                quantity: units,
                filled: units,
                notional: spent,
                spent,
                create_ms: now_ms,
                open_ms: now_ms,
                close_ms: now_ms,
                opened: true,
                closed: true,
                ..LegState::default()
            },
            ..ExLeg::default()
        };
        o.panic = false;
        o.stop_price = 0.0;
        (o.trail_pct, o.take_profit, o.trail_peak) = (0.0, 0.0, 0.0);
        o.bot_stop = (0.0, 0.0);
        fx.logs.push(format!(
            "{}: position sold outside the core (account holds {avail} of {need}), closed at {price}",
            o.market
        ));
        self.unbind(&old);
        self.set_status(id, status::SELL_DONE, now_ms);
        self.bump(id, &mut fx);
        fx
    }

    /// A failed request carries its identity so a late REST error cannot
    /// undo a newer request or stream-confirmed execution.
    pub fn failed(&mut self, action: &Action, definitive: bool, msg: &str, now_ms: i64) -> Effects {
        let (id, leg, op, request) = match action {
            Action::Post {
                order, leg, key, ..
            } => (*order, *leg, Op::Post, key.as_str()),
            Action::Replace {
                order, leg, key, ..
            } => (*order, *leg, Op::Replace, key.as_str()),
            Action::Cancel {
                order,
                leg,
                exchange_id,
            } => (*order, *leg, Op::Cancel, exchange_id.as_str()),
            Action::Query {
                order,
                leg,
                exchange_id,
            } => (*order, *leg, Op::Query, exchange_id.as_str()),
            Action::QueryRequest { order, leg, key } => (*order, *leg, Op::Query, key.as_str()),
        };
        let mut fx = Effects::default();
        let Some(o) = self.map.get_mut(&id) else {
            return fx;
        };
        let (market, uid, sells, lot, tick) =
            (o.market.clone(), o.uid.clone(), o.sells(leg), o.lot, o.tick);
        let ex = o.leg(leg);
        ex.ensure_execution();
        if op == Op::Cancel {
            ex.cancel_sent = false; // the outcome is known; a retry may go out
        }
        if (matches!(op, Op::Post | Op::Replace) || matches!(action, Action::QueryRequest { .. }))
            && !ex.executions.last().unwrap().matches_key(request)
        {
            return fx;
        }
        if op == Op::Cancel
            && !ex
                .executions
                .iter()
                .any(|e| e.ids.contains(request) && e.is_live())
        {
            return fx;
        }
        if let Action::Replace { price, lots, .. } = action {
            // Every amend of an order goes under its one key: a late failure of
            // an earlier one says nothing of the amend out now.
            if ex.amend.is_some_and(|a| a.differs(*price, *lots, tick)) {
                return fx;
            }
        }
        fx.logs.push(format!("{market}: {op:?} failed: {msg}"));
        if op == Op::Replace {
            // A refused amend leaves the order as it was (API docs, «Modify
            // Order») — or the order is gone, filled or cancelled: the model
            // goes back to it, and the Query below reads which. A call of
            // unknown fate is reconciled by our key, again and again, until a
            // read settles it (`apply`); the move stays in flight till then.
            if !definitive {
                if let Some(a) = ex.amend.as_mut() {
                    a.unsure = true;
                }
                ex.uncertain = true;
                ex.resolve_key = request.to_owned();
                ex.resolve_at = now_ms + 5_000;
                fx.actions.push(Action::QueryRequest {
                    order: id,
                    leg,
                    key: request.to_owned(),
                });
            } else {
                if let Some(a) = ex.amend.take() {
                    let e = ex.executions.last_mut().unwrap();
                    e.price = a.was_price;
                    e.lots = a.was_lots.max(e.filled);
                    e.reason = a.was_reason;
                    ex.state.price = a.was_price;
                    ex.aggregate(lot);
                }
                ex.replacing = false;
            }
        } else if op == Op::Post {
            let last = ex.executions.last_mut().unwrap();
            // The worker is done with this request: a stream REJECTED from
            // now on is the outcome.
            last.awaiting_reply = false;
            // A 4xx on the Post refuses the request under this key. A stream
            // NEW without fills that raced ahead of the reply is that same
            // refused request (TInvestCore 21.09), unless the exchange already
            // gave it a number. Exits keep the stream's word until the account
            // decides (`reconcile`, TInvestCore EUTR 18.09).
            let refused = definitive
                && op == Op::Post
                && leg == Leg::Buy
                && last.filled == 0
                && !last.ids.iter().any(|id| broker_id(id))
                && matches!(
                    last.status,
                    None | Some(ExecStatus::New | ExecStatus::Rejected)
                );
            if refused {
                ex.uncertain = false;
                let u = rejected_post(ex, request, uid, sells, msg, now_ms);
                fx.extend(self.apply(&u, now_ms));
                return fx;
            }
            if ex.executions.last().unwrap().status.is_some() {
                // Stream evidence wins over the failed REST call.
                return Effects::default();
            }
            if !definitive || ex.executions.last().unwrap().status.is_some() {
                ex.uncertain = true;
                ex.resolve_key = request.to_owned();
                ex.resolve_at = now_ms + 5_000;
                fx.actions.push(Action::QueryRequest {
                    order: id,
                    leg,
                    key: request.to_owned(),
                });
                fx.logs.push(format!(
                    "{market}: request outcome unknown, reconciling before any retry"
                ));
                return fx;
            }
            let u = rejected_post(ex, request, uid, sells, msg, now_ms);
            fx.extend(self.apply(&u, now_ms));
            return fx;
        }
        if matches!(action, Action::QueryRequest { .. }) {
            // One «not found» cannot prove an in-flight submission never arrived:
            // hold the identity and ask again with backoff. A fill, an
            // exchange id or a later status from the stream proves the order
            // exists.
            // A miss `REQUEST_GIVE_UP_MS` after the first one: the request is
            // not on the exchange. Exits hold on any id or status, as before: a wrong
            // give-up there posts a second exit.
            ex.resolve_at = now_ms + 10_000;
            let last = ex.executions.last().unwrap();
            if !crate::aster::rest::msg_has_code(msg, CODE_ORDER_NOT_FOUND)
                || last.filled > 0
                || last.ids.iter().any(|id| broker_id(id))
                || last.status.is_some_and(|s| s != ExecStatus::New)
                || (leg == Leg::Sell && (!last.ids.is_empty() || last.status.is_some()))
            {
                return fx;
            }
            let misses = self
                .request_misses
                .entry(request.to_owned())
                .or_insert(RequestMisses {
                    count: 0,
                    first_ms: now_ms,
                });
            misses.count += 1;
            if now_ms - misses.first_ms < REQUEST_GIVE_UP_MS {
                ex.resolve_at = now_ms + (RESOLVE_PERIOD_MS << (misses.count - 1).min(4));
                return fx;
            }
            let count = misses.count;
            self.request_misses.remove(request);
            fx.logs.push(format!(
                "{market}: request {request} not found {count} times over \
                 {} s: it never reached the exchange",
                REQUEST_GIVE_UP_MS / 1000
            ));
            if ex.executions.len() >= 2 {
                fall_back(ex, now_ms);
                return fx;
            }
            ex.uncertain = false;
            let u = rejected_post(ex, request, uid, sells, msg, now_ms);
            fx.extend(self.apply(&u, now_ms));
            return fx;
        }
        if op == Op::Query && crate::aster::rest::msg_has_code(msg, CODE_ORDER_NOT_FOUND) {
            if let Some(e) = ex.executions.iter().find(|e| e.ids.contains(request)) {
                if !broker_id(request) {
                    // REST resolves broker ids only: «not found» for a stream
                    // alias says nothing about the order (EUTR 17.09: a live
                    // exit turned BuyDone and a second exit went out). Hold the
                    // current generation and reconcile by its request key.
                    if ex
                        .executions
                        .last()
                        .is_some_and(|l| l.ids.contains(request))
                    {
                        let key = e.key.clone();
                        ex.uncertain = true;
                        ex.resolve_key = key.clone();
                        ex.resolve_at = now_ms + 5_000;
                        fx.actions.push(Action::QueryRequest {
                            order: id,
                            leg,
                            key,
                        });
                    }
                    return fx;
                }
                if self.query_missed.len() > 256 {
                    self.query_missed.clear();
                }
                // A first miss counts for a minute; one older than that is forgotten (the
                // order was found or finished meanwhile) and this one is a first again.
                let again = self
                    .query_missed
                    .remove(request)
                    .is_some_and(|at| now_ms - at < QUERY_MISS_MEMORY_MS);
                if !again {
                    self.query_missed.insert(request.to_owned(), now_ms);
                    self.requery
                        .push((now_ms + REQUERY_MS, id, leg, request.to_owned()));
                    fx.logs.push(format!(
                        "{market}: order {request} not found once, asking again"
                    ));
                    return fx;
                }
                let u = OrderUpdate {
                    exchange_id: request.to_owned(),
                    request_id: e.key.clone(),
                    uid,
                    status: ExecStatus::Cancelled,
                    sell: sells,
                    is_market: ex.state.is_market,
                    lots_requested: e.lots,
                    lots_executed: e.filled,
                    price: e.price,
                    avg_price: e.mean,
                    unary: true,
                    time_ms: now_ms,
                    message: String::new(),
                };
                fx.extend(self.apply(&u, now_ms));
            }
            return fx;
        }
        // A refused Cancel/Replace may mean the order already finished: read
        // its state. A transport failure tells nothing and the caller retries
        // the Cancel itself, so a Query over the same dead link is only noise.
        if matches!(op, Op::Cancel | Op::Replace) && !ex.exchange_id.is_empty() && definitive {
            fx.actions.push(Action::Query {
                order: id,
                leg,
                exchange_id: if op == Op::Cancel {
                    request.to_owned()
                } else {
                    ex.exchange_id.clone()
                },
            });
        }
        if o.panic {
            if leg == Leg::Sell && matches!(op, Op::Post | Op::Replace) && definitive {
                // The exchange refused the exit itself: look at it less and less often.
                o.panic_fails = o.panic_fails.saturating_add(1);
                o.panic_next = now_ms + panic_retry_ms(o.panic_fails);
            } else {
                // Anything else (a cancel or a query refused, the other leg): the plain retry.
                o.panic_next = now_ms + PANIC_RETRY_MS;
            }
        }
        fx
    }

    #[cfg(test)]
    pub(crate) fn fail(&mut self, id: u64, leg: Leg, op: Op, msg: &str, now: i64) -> Effects {
        let Some(o) = self.map.get_mut(&id) else {
            return Effects::default();
        };
        let ex = o.leg(leg);
        let action = match op {
            Op::Post => Action::Post {
                order: id,
                leg,
                key: ex.key.clone(),
                uid: String::new(),
                lots: 0,
                price: None,
                sell: false,
            },
            Op::Replace => Action::Replace {
                order: id,
                leg,
                key: ex.key.clone(),
                uid: String::new(),
                exchange_id: ex.exchange_id.clone(),
                lots: 0,
                price: 0.0,
            },
            Op::Cancel => Action::Cancel {
                order: id,
                leg,
                exchange_id: ex.exchange_id.clone(),
            },
            Op::Query => Action::Query {
                order: id,
                leg,
                exchange_id: ex.exchange_id.clone(),
            },
        };
        self.failed(&action, true, msg, now)
    }

    // ----- internals -----------------------------------------------------------

    fn locate(&self, u: &OrderUpdate) -> Option<(u64, Leg)> {
        self.by_key
            .get(&u.request_id)
            .or_else(|| self.by_exchange.get(&u.exchange_id))
            .copied()
    }

    /// Post the exit leg for the whole filled entry as a limit at `price`.
    /// Post the exit of `id` for its residual lots: a limit at `price`, or a
    /// MARKET order shown at `price` until it fills.
    fn place_exit(&mut self, id: u64, price: f64, market: bool) -> Vec<Action> {
        let key = self.new_key();
        let Some(o) = self.map.get_mut(&id) else {
            return Vec::new();
        };
        let lots = o.residual_lots();
        if lots < 1 || price <= 0.0 {
            return Vec::new();
        }
        let why = exit_reason_of(o.next_reason, &o.sell);
        o.next_reason = 0;
        let sell = o.sells(Leg::Sell);
        let ex = &mut o.sell;
        if !ex.key.is_empty() || ex.filled_lots > 0 {
            ex.ensure_execution();
        }
        ex.key = key.clone();
        ex.owned = true;
        ex.exchange_id.clear();
        ex.executions.push(Execution {
            key: key.clone(),
            lots,
            price,
            awaiting_reply: true,
            reason: why,
            ..Execution::default()
        });
        ex.replacing = false;
        ex.uncertain = false;
        ex.deferred = None;
        ex.cancel_requested = false;
        ex.cancel_sent = false;
        ex.state = LegState {
            price,
            notional: (ex.filled_lots + lots) as f64 * o.lot * price,
            create_ms: o.buy.state.close_ms.max(o.buy.state.create_ms),
            is_market: market,
            ..LegState::default()
        };
        ex.aggregate(o.lot);
        o.status = status::SELL_SET;
        let uid = o.uid.clone();
        self.bind(&key, id, Leg::Sell);
        vec![Action::Post {
            order: id,
            leg: Leg::Sell,
            key,
            uid,
            lots,
            price: (!market).then_some(price),
            sell,
        }]
    }

    fn set_status(&mut self, id: u64, next: u8, now_ms: i64) {
        let o = self.map.get_mut(&id).expect("order");
        o.status = next;
        if status::is_terminal(next) {
            o.done_ms = now_ms;
        }
    }

    fn bump(&mut self, id: u64, fx: &mut Effects) {
        if let Some(o) = self.map.get_mut(&id) {
            o.rev += 1;
            fx.changed(id);
        }
    }

    fn remove(&mut self, id: u64) {
        if let Some(o) = self.map.remove(&id) {
            // A report of this order's exit that comes AFTER it is forgotten would be a foreign
            // one: `adopt` would let it close some other position of the market. Its ids are
            // remembered as settled (the `left` set: reports of those stay unattached).
            let mut ids: Vec<String> = Vec::new();
            for ex in [&o.buy, &o.sell] {
                ids.push(ex.key.clone());
                ids.extend(
                    ex.executions
                        .iter()
                        .flat_map(|e| std::iter::once(e.key.clone()).chain(e.ids.iter().cloned())),
                );
            }
            for settled in ids {
                self.remember_left(settled);
            }
            for ex in [&o.buy, &o.sell] {
                self.unbind(&ex.key);
            }
            self.by_key.retain(|_, v| v.0 != id);
            self.by_exchange.retain(|_, v| v.0 != id);
        }
    }

    /// Reports under `key` resolve to the leg.
    fn bind(&mut self, key: &str, id: u64, leg: Leg) {
        self.by_key.insert(key.to_owned(), (id, leg));
    }

    fn unbind(&mut self, key: &str) {
        if !key.is_empty() {
            self.by_key.remove(key);
        }
    }

    /// The terminal's request uid becomes the order id when free (so a
    /// replayed `Start` cannot open a second order); otherwise the next id.
    fn new_id(&mut self, req_uid: u64) -> u64 {
        if req_uid != 0 && !self.map.contains_key(&req_uid) {
            return req_uid;
        }
        while self.map.contains_key(&self.next_id) || self.next_id == 0 {
            self.next_id += 1;
        }
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// UUID-shaped idempotency key from the process-random hasher: 36
    /// characters of hex digits and dashes, inside what Aster's
    /// `newClientOrderId` admits (`^[.A-Z:/a-z0-9_-]{1,36}$`).
    fn new_key(&mut self) -> String {
        let mut h = self.keys.build_hasher();
        (
            self.next_id,
            self.by_key.len(),
            crate::aster::rest::now_ms(),
        )
            .hash(&mut h);
        let a = h.finish();
        (a, self.map.len()).hash(&mut h);
        let b = h.finish();
        self.next_id += 1;
        format!(
            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
            a >> 32,
            (a >> 16) & 0xffff,
            a & 0xfff,
            0x8000 | ((b >> 48) & 0x3fff),
            b & 0xffff_ffff_ffff
        )
    }
}

/// The exchange's own order id (`orderId`, numeric), as opposed to our
/// request key (`clientOrderId`).
pub(crate) fn broker_id(exchange_id: &str) -> bool {
    exchange_id.parse::<i64>().is_ok()
}

/// Exchange order id on the wire (`int_id`): Aster's `orderId`, 0 if it is
/// not yet known.
fn wire_id(exchange_id: &str) -> i64 {
    exchange_id.parse::<i64>().unwrap_or(0)
}

/// The last request of `ex` was refused or never arrived: it stays in the
/// ledger as rejected, the generation before it is current again and is
/// reconciled by its key from `resolve_at`.
fn fall_back(ex: &mut ExLeg, resolve_at: i64) {
    let n = ex.executions.len();
    ex.executions[n - 1].status = Some(ExecStatus::Rejected);
    ex.executions.swap(n - 1, n - 2);
    let old = ex.executions.last().unwrap();
    ex.key = old.key.clone();
    // The old generation may have acquired its exchange id while this
    // request was pending.
    if let Some(numeric) = old.ids.iter().find(|id| broker_id(id)) {
        ex.exchange_id = numeric.clone();
        ex.state.exchange_id = wire_id(numeric);
    }
    ex.state.price = old.price;
    ex.replacing = false;
    ex.uncertain = true;
    ex.resolve_key = ex.key.clone();
    ex.resolve_at = resolve_at;
}

/// A REST refusal of the Post `request` as the report `apply` takes.
fn rejected_post(
    ex: &ExLeg,
    request: &str,
    uid: String,
    sell: bool,
    msg: &str,
    now_ms: i64,
) -> OrderUpdate {
    OrderUpdate {
        exchange_id: String::new(),
        request_id: request.to_owned(),
        uid,
        status: ExecStatus::Rejected,
        sell,
        is_market: ex.state.is_market,
        lots_requested: ex.executions.last().map_or(0, |e| e.lots),
        lots_executed: 0,
        price: ex.state.price,
        avg_price: 0.0,
        unary: true,
        time_ms: now_ms,
        message: msg.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Tag;

    /// TInvestCore's SBER fixture on Aster's model: a lot of 10, a tick of
    /// 0.01, last 300, no band. The market's key is `u-sber` (Aster's key is
    /// the symbol) and the terminal names it `SBER`, so the ported tests
    /// keep telling the two apart.
    fn sber_model() -> Catalog {
        Catalog::of(vec![Market {
            symbol: "u-sber".into(),
            base: "SBER".into(),
            quote: "USDT".into(),
            long_name: String::new(),
            price_precision: 2,
            quantity_precision: 0,
            tick_size: 0.01,
            step_size: 10.0,
            min_qty: 10.0,
            max_qty: 1_000_000.0,
            market_max_qty: 1_000_000.0,
            min_notional: 5.0,
            min_price: 0.01,
            max_price: 1_000_000.0,
            multiplier_up: 0.0,
            multiplier_down: 0.0,
            max_num_orders: 200,
            max_num_algo_orders: 10,
            trigger_protect: 0.02,
            market_take_bound: 0.02,
            maint_margin_percent: 2.5,
            required_margin_percent: 5.0,
            bracket_leverage: None,
            liquidation_fee: 0.025,
            trading: true,
            has_sessions: false,
            delivery_ms: None,
            tags: vec![Tag::Crypto],
            quote_volume_24h: None,
            last_price: Some(300.0),
            price_seeded: false,
            bid: None,
            ask: None,
            mark_price: None,
            funding: None,
            feed_fresh: true,
            book_ms: 0,
        }])
    }

    fn start(size: f64, price: f64, planned: f64) -> StartOrder {
        StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 3,
            size,
            price,
            planned_sell: planned,
            stops: None,
        }
    }

    fn update(key: &str, ex: &str, st: ExecStatus, req: i64, done: i64, avg: f64) -> OrderUpdate {
        OrderUpdate {
            exchange_id: ex.into(),
            request_id: key.into(),
            uid: "u-sber".into(),
            status: st,
            sell: false,
            is_market: false,
            lots_requested: req,
            lots_executed: done,
            price: 0.0,
            avg_price: avg,
            unary: true,
            time_ms: 5,
            message: String::new(),
        }
    }

    fn post_key(fx: &Effects) -> String {
        match &fx.actions[0] {
            Action::Post { key, .. } => key.clone(),
            other => panic!("{other:?}"),
        }
    }

    /// A stop «on» at 0 % would sit at the entry and fire on the first tick
    /// against it; one at 100 % of a long prices below zero. Neither is a stop.
    /// A real percent is a distance, whatever its sign on the wire.
    #[test]
    fn a_stop_with_no_real_distance_is_off() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let id = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1).changed[0];
        orders.set_stops(id, true, false, 0.0, 0.0);
        assert_eq!(orders.get(id).unwrap().record().stop, None);
        orders.set_stops(id, true, false, 100.0, 0.0);
        assert_eq!(orders.get(id).unwrap().record().stop, None);
        orders.set_stops(id, true, false, f64::NAN, 0.0);
        assert_eq!(orders.get(id).unwrap().record().stop, None);
        orders.set_stops(id, true, false, -2.0, 0.5);
        assert_eq!(orders.get(id).unwrap().record().stop, Some((294.0, 0.5)));
    }

    /// A restored order is re-expressed in today's lot when the exchange changed the step: every
    /// count a whole number of new lots, or the order keeps its old lot (and says so).
    #[test]
    fn a_restored_orders_lots_follow_a_changed_step_when_exact() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 0.0), sber, 1);
        let key = post_key(&fx);
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        let before = orders.get(1).unwrap();
        assert_eq!((before.lot, before.buy.filled_lots), (10.0, 2));

        // The step halves: the same 20 units are 4 lots.
        let mut finer = sber.clone();
        finer.step_size = 5.0;
        orders.respec(|_| Some(&finer));
        let o = orders.get(1).unwrap();
        assert_eq!((o.lot, o.buy.filled_lots, o.buy.lots), (5.0, 4, 4));

        // The step grows to 30: 20 units are not a whole lot — the order keeps its lot.
        let mut coarse = sber.clone();
        coarse.step_size = 30.0;
        orders.respec(|_| Some(&coarse));
        let o = orders.get(1).unwrap();
        assert_eq!((o.lot, o.buy.filled_lots), (5.0, 4));
    }

    /// A planned exit the exchange refused (here `-2022`, a reduce-only order beside a rival
    /// resting order) is asked for again, less and less often, until one is live; a position
    /// must not stand without its exit while one can be placed. A hand cancel of the exit ends
    /// the retries, and the flag survives a restart.
    #[test]
    fn a_refused_planned_exit_is_placed_again_until_it_is_live() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 320.0), sber, 1);
        let key = post_key(&fx);
        let fx = orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        let exit_key = post_key(&fx);
        // The exchange refuses the exit.
        let mut refused = update(&exit_key, "", ExecStatus::Rejected, 2, 0, 0.0);
        refused.sell = true;
        orders.apply(&refused, 30);
        let o = orders.get(1).unwrap();
        assert_eq!(o.status, status::BUY_DONE);
        assert_eq!(o.exit_refused, Some(true));
        // Nothing before the pause, a new Post after it.
        assert!(orders.watch(&model, 5_000).actions.is_empty());
        let fx = orders.watch(&model, 10_100);
        let retry = fx
            .actions
            .iter()
            .find(|a| matches!(a, Action::Post { sell: true, .. }))
            .expect("the exit is asked for again");
        let Action::Post { key: retry_key, .. } = retry else {
            unreachable!()
        };
        assert_ne!(retry_key, &exit_key, "under a fresh key");
        // Refused again: the pause doubles (20 s).
        let mut refused = update(retry_key, "", ExecStatus::Rejected, 2, 0, 0.0);
        refused.sell = true;
        orders.apply(&refused, 10_200);
        assert!(orders.watch(&model, 25_000).actions.is_empty());
        let fx = orders.watch(&model, 30_300);
        let Some(Action::Post { key: live_key, .. }) = fx
            .actions
            .iter()
            .find(|a| matches!(a, Action::Post { sell: true, .. }))
            .cloned()
        else {
            panic!("a third try");
        };
        // This one is accepted: the flag and the retries end.
        let mut accepted = update(&live_key, "9002", ExecStatus::New, 2, 0, 0.0);
        accepted.sell = true;
        orders.apply(&accepted, 30_400);
        let o = orders.get(1).unwrap();
        assert_eq!(o.status, status::SELL_SET);
        assert_ne!(o.exit_refused, Some(true));
        assert!(orders.watch(&model, 600_000).actions.is_empty());
    }

    /// The decision not to have the exit stands: cancelling it by hand stops the retries.
    #[test]
    fn a_hand_cancel_of_the_exit_ends_the_retries() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 320.0), sber, 1);
        let key = post_key(&fx);
        let fx = orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        let mut refused = update(&post_key(&fx), "", ExecStatus::Rejected, 2, 0, 0.0);
        refused.sell = true;
        orders.apply(&refused, 30);
        assert_eq!(orders.get(1).unwrap().exit_refused, Some(true));
        orders.cancel(1, Leg::Sell);
        assert_ne!(orders.get(1).unwrap().exit_refused, Some(true));
        assert!(orders.watch(&model, 60_000).actions.is_empty());
    }

    /// A panic exit that a rival resting entry would make the exchange refuse (`-2022` on a
    /// one-way account) takes the rival off first: a short's buy-back beside a resting long.
    #[test]
    fn a_panic_exit_cancels_the_resting_entry_that_stands_against_it() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // A short in position.
        let mut short = start(3000.0, 300.0, 0.0);
        short.is_short = true;
        let fx = orders.start(1, &short, sber, 1);
        let key = post_key(&fx);
        let mut filled = update(&key, "9001", ExecStatus::Filled, 1, 1, 300.0);
        filled.sell = true;
        orders.apply(&filled, 2);
        assert_eq!(orders.get(1).unwrap().status, status::BUY_DONE);
        // A long entry resting beside it, acknowledged by the exchange.
        let fx = orders.start(2, &start(3000.0, 290.0, 0.0), sber, 3);
        let rival_key = post_key(&fx);
        orders.apply(&update(&rival_key, "9100", ExecStatus::New, 1, 0, 0.0), 4);
        assert_eq!(orders.get(2).unwrap().status, status::BUY_SET);

        let fx = orders.set_panic(1, true, sber, 10);
        assert!(
            fx.actions.iter().any(
                |a| matches!(a, Action::Cancel { order: 2, leg: Leg::Buy, exchange_id, .. } if exchange_id == "9100")
            ),
            "{:?}",
            fx.actions
        );
        assert!(fx
            .logs
            .iter()
            .any(|l| l.contains("stands against this exit")));
        // A rival of the SAME direction as the position is not one.
        let fx = orders.set_panic(2, true, sber, 11);
        assert!(!fx
            .actions
            .iter()
            .any(|a| matches!(a, Action::Cancel { order: 1, .. })));
    }

    /// The flag in the file is believed: a cancelled exit stays cancelled across a restart, and
    /// only a file from before the flag existed is judged by the shape of the leg.
    #[test]
    fn a_restored_flag_is_believed_and_a_missing_one_is_judged_by_shape() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let build = |flag: Option<bool>| {
            let mut orders = Orders::new();
            let fx = orders.start(1, &start(7000.0, 310.0, 320.0), sber, 1);
            let key = post_key(&fx);
            let fx = orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
            let mut refused = update(&post_key(&fx), "", ExecStatus::Rejected, 2, 0, 0.0);
            refused.sell = true;
            orders.apply(&refused, 30);
            let mut persisted = orders.persisted();
            // What the file says about the flag.
            match flag {
                Some(v) => persisted[0]["exit_refused"] = serde_json::json!(v),
                None => {
                    persisted[0].as_object_mut().unwrap().remove("exit_refused");
                }
            }
            let list: Vec<CoreOrder> = persisted
                .into_iter()
                .map(|v| serde_json::from_value(v).unwrap())
                .collect();
            let mut back = Orders::new();
            back.restore(list);
            back
        };
        // Missing (an old file): the shape says «refused before it opened» → retried at once.
        let back = build(None);
        assert_eq!(back.get(1).unwrap().exit_refused, Some(true));
        assert!(!back.clone_actions_after_watch(&model).is_empty());
        // Explicitly false (the user cancelled it): left alone.
        let back = build(Some(false));
        assert_eq!(back.get(1).unwrap().exit_refused, Some(false));
        assert!(back.clone_actions_after_watch(&model).is_empty());
        // Explicitly true: retried at once.
        let back = build(Some(true));
        assert!(!back.clone_actions_after_watch(&model).is_empty());
    }

    impl Orders {
        /// What the first `watch` after a restore would send (the orders are consumed).
        fn clone_actions_after_watch(&self, model: &Catalog) -> Vec<Action> {
            let mut me = Orders::new();
            let list: Vec<CoreOrder> = self
                .persisted()
                .into_iter()
                .map(|v| serde_json::from_value(v).unwrap())
                .collect();
            me.restore(list);
            me.watch(model, 1_000_000).actions
        }
    }

    /// The retry of a refused panic exit backs off from 10 s to 160 s.
    #[test]
    fn a_refused_panic_exit_is_retried_less_and_less_often() {
        let secs: Vec<i64> = (0..8).map(|n| panic_retry_ms(n) / 1000).collect();
        assert_eq!(secs, [10, 10, 20, 40, 80, 160, 160, 160]);
    }

    /// An order forgotten after its time is not forgotten by the exchange's late reports: its
    /// ids are remembered as settled, so a report of its exit cannot be taken for a foreign one
    /// that closes another position.
    #[test]
    fn a_pruned_orders_ids_stay_settled() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 0.0), sber, 1);
        let key = post_key(&fx);
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        assert!(!orders.left.contains("9001"));
        orders.remove(1);
        assert!(orders.left.contains("9001") && orders.left.contains(&key));
        assert!(orders.get(1).is_none());
    }

    /// A strategy's fired stop on a LIVE order leaves at MARKET: a reduce-only market sale with
    /// no price, not a limit through the book (`PLAN.md`, п. 2).
    #[test]
    fn a_fired_stop_of_a_live_order_exits_at_market() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 0.0), sber, 1);
        let key = post_key(&fx);
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        assert_eq!(orders.get(1).unwrap().status, status::BUY_DONE);
        let fx = orders.stop_out(1, reason::STOP_LOSS, sber, 30);
        assert!(
            matches!(
                &fx.actions[..],
                [Action::Post {
                    sell: true,
                    price: None,
                    lots: 2,
                    ..
                }]
            ),
            "{:?}",
            fx.actions
        );
        // The emulated twin leaves at MARKET too: the emulator fills it at the best quote.
        let fx = orders.start(2, &start(7000.0, 310.0, 0.0), sber, 40);
        let key = post_key(&fx);
        orders.set_emulator(2);
        orders.apply(&update(&key, "9002", ExecStatus::Filled, 2, 2, 309.5), 50);
        let fx = orders.stop_out(2, reason::STOP_LOSS, sber, 60);
        assert!(
            matches!(
                &fx.actions[..],
                [Action::Post {
                    sell: true,
                    price: None,
                    ..
                }]
            ),
            "{:?}",
            fx.actions
        );
    }

    /// A hand entry against an open position of the other side is not placed — at the start
    /// (`holds_against`, asked by the engine) and when a pending one fires.
    #[test]
    fn a_hand_entry_against_an_open_position_is_not_placed() {
        let mut model = sber_model();
        let sber = model.get("u-sber").unwrap().clone();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 0.0), &sber, 1);
        let key = post_key(&fx);
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        assert!(orders.get(1).unwrap().holds_position());
        assert!(orders.holds_against("u-sber", true, false));
        assert!(!orders.holds_against("u-sber", false, false));
        assert!(!orders.holds_against("u-sber", true, true));
        assert!(!orders.holds_against("other", true, false));

        // A hand short armed at 301 (above the last 300) fires when the price gets there.
        let mut pending = start(3000.0, 301.0, 0.0);
        pending.is_short = true;
        pending.strategy_id = 0;
        let id = orders.start_pending(2, &pending, &sber, 30).changed[0];
        model.set_last("u-sber", 302.0);
        let fx = orders.watch(&model, 40);
        assert!(fx.actions.is_empty(), "{:?}", fx.actions);
        let o = orders.get(id).unwrap();
        assert_eq!((o.status, o.pending), (status::BUY_FAIL, 0.0));
        assert_eq!(o.buy.state.close_ms, 40);
    }

    /// The stop a deleted strategy had drawn becomes the order's own, and a stop is decided by a
    /// live quote: an old one is left for the last trade.
    #[test]
    fn an_orphans_bot_stop_fires_and_an_old_quote_does_not_decide() {
        let mut model = sber_model();
        let sber = model.get("u-sber").unwrap().clone();
        let mut orders = Orders::new();
        let fx = orders.start(1, &start(7000.0, 310.0, 0.0), &sber, 1);
        let key = post_key(&fx);
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        orders.set_bot_stop(1, 295.0, 0.5);
        // Only a stop the order does not have yet is taken, and only once.
        assert!(!orders.adopt_bot_stop(1).logs.is_empty());
        assert!(orders.adopt_bot_stop(1).logs.is_empty());
        assert_eq!(orders.get(1).unwrap().bot_stop(), (0.0, 0.0));
        {
            let m = model.get_mut("u-sber").unwrap();
            m.bid = Some(294.0);
            m.book_ms = 1_000;
        }
        // The book is a minute old: the last trade (300) decides, and it holds.
        assert!(orders.watch(&model, 61_000).logs.is_empty());
        // A fresh quote under the stop fires it.
        model.get_mut("u-sber").unwrap().book_ms = 60_500;
        let fx = orders.watch(&model, 61_000);
        assert!(
            fx.logs.iter().any(|l| l.contains("StopLoss activated")),
            "{:?}",
            fx.logs
        );
    }

    #[test]
    fn long_order_walks_buy_set_done_sell_set_done() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // 7000 USDT at 310 -> 2 lots of 10.
        let fx = orders.start(0xAB, &start(7000.0, 310.0, 320.0), sber, 1);
        assert_eq!(fx.changed, [0xAB]);
        assert!(matches!(
            &fx.actions[0],
            Action::Post { order: 0xAB, leg: Leg::Buy, uid, lots: 2, price: Some(p), sell: false, .. }
                if uid == "u-sber" && *p == 310.0
        ));
        let key = post_key(&fx);
        assert_eq!(key.len(), 36);
        let rec = orders.get(0xAB).unwrap().record();
        assert_eq!(
            (rec.status, rec.buy.quantity, rec.buy.notional),
            (status::BUY_SET, 20.0, 6200.0)
        );

        let u = update(&key, "9001", ExecStatus::New, 2, 0, 0.0);
        assert!(orders.knows(&u));
        let fx = orders.apply(&u, 10);
        assert_eq!(orders.get(0xAB).unwrap().status, status::BUY_SET);
        assert!(fx.actions.is_empty());
        assert_eq!(
            orders.get(0xAB).unwrap().record().buy.exchange_id,
            wire_id("9001")
        );

        // Fill -> BuyDone and the planned exit is posted at once, keeping the
        // planned distance (+3.2258 %) from the actual fill: 309.5 -> 319.48.
        let fx = orders.apply(&update("", "9001", ExecStatus::Filled, 2, 2, 309.5), 20);
        let o = orders.get(0xAB).unwrap();
        assert_eq!(o.status, status::SELL_SET);
        let rec = o.record();
        assert_eq!(
            (rec.buy.filled, rec.buy.mean_price, rec.buy.closed),
            (20.0, 309.5, true)
        );
        assert!(matches!(
            &fx.actions[0],
            Action::Post { leg: Leg::Sell, lots: 2, price: Some(p), sell: true, .. } if *p == 319.48
        ));
        assert_eq!(rec.sell.price, 319.48);
        assert!(fx.logs[0].contains("[actual buy]+3.23%"), "{:?}", fx.logs);
        let sell_key = post_key(&fx);

        // Move the exit: an amend of the same order under the same key, then
        // it fills.
        let fx = orders.apply(&update(&sell_key, "9002", ExecStatus::New, 2, 0, 0.0), 30);
        assert!(fx.actions.is_empty());
        let fx = orders.target(0xAB, Leg::Sell, 325.0, None);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { exchange_id, key, lots: 2, price, .. }]
                if exchange_id == "9002" && *key == sell_key && *price == 325.0),
            "{fx:?}"
        );
        assert_eq!(orders.get(0xAB).unwrap().record().sell.price, 325.0);
        // The order's stream report from before the amend raced it: the move stands.
        let mut before = update(&sell_key, "9002", ExecStatus::New, 2, 0, 0.0);
        (before.price, before.unary) = (319.48, false);
        assert!(orders.apply(&before, 31).actions.is_empty());
        assert_eq!(orders.get(0xAB).unwrap().record().sell.price, 325.0);
        let mut amended = update(&sell_key, "9002", ExecStatus::New, 2, 0, 0.0);
        amended.price = 325.0;
        orders.apply(&amended, 34);
        assert!(!orders.get(0xAB).unwrap().sell.replacing);
        let fx = orders.apply(
            &update(&sell_key, "9002", ExecStatus::Filled, 2, 2, 325.0),
            40,
        );
        assert_eq!(fx.changed, [0xAB]);
        let rec = orders.get(0xAB).unwrap().record();
        assert_eq!(
            (rec.status, rec.sell.filled, rec.sell.exchange_id),
            (status::SELL_DONE, 20.0, wire_id("9002"))
        );
        assert_eq!(wire_id("84320505953"), 84320505953);
        assert_eq!(rec.sell.spent, 20.0 * 325.0);
        assert_eq!(orders.records(40).len(), 1);
        assert!(orders.records(40 + KEEP_DONE_MS + 1).is_empty());
    }

    #[test]
    fn target_buy_resizes_the_entry() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // 2 lots of 10 at 300.
        let fx = orders.start(0, &start(6000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9001", ExecStatus::New, 2, 0, 0.0), 2);

        // Same size at a new price: lots unchanged, the same order amended.
        let fx = orders.target(id, Leg::Buy, 290.0, Some(6000.0));
        assert!(matches!(&fx.actions[0],
            Action::Replace { lots: 2, price, key: k, exchange_id, .. }
                if *price == 290.0 && *k == key && exchange_id == "9001"));
        orders.apply(&update(&key, "9001", ExecStatus::New, 2, 0, 0.0), 3);
        // 9000 USDT at 290 -> 3 lots; image follows.
        let fx = orders.target(id, Leg::Buy, 290.0, Some(9000.0));
        assert!(matches!(&fx.actions[0], Action::Replace { lots: 3, .. }));
        let rec = orders.get(id).unwrap().record();
        assert_eq!((rec.buy.quantity, rec.buy.notional), (30.0, 8700.0));
        orders.apply(&update(&key, "9001", ExecStatus::New, 3, 0, 0.0), 4);
        // Below one lot: refused, no exchange call.
        let fx = orders.target(id, Leg::Buy, 290.0, Some(100.0));
        assert!(fx.actions.is_empty());
        assert!(fx.logs[0].contains("below"), "{:?}", fx.logs);

        // Partial fill: the remainder can shrink to one lot but not below. The
        // amend names the order's whole size: its 2 filled lots and the 1 left.
        orders.apply(
            &update(&key, "9001", ExecStatus::PartiallyFilled, 3, 2, 290.0),
            5,
        );
        let fx = orders.target(id, Leg::Buy, 285.0, Some(100.0));
        assert!(matches!(&fx.actions[0], Action::Replace { lots: 3, .. }));
        assert_eq!(orders.get(id).unwrap().record().buy.quantity, 30.0);
    }

    #[test]
    fn core_ids_start_at_the_base_and_yield_to_request_uids() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::starting_at(1_789_000_000_000);
        let a = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1).changed[0];
        let b = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1).changed[0];
        assert_eq!(a, 1_789_000_000_000);
        assert!(b > a);
        // The terminal's own request uid still wins when free.
        let c = orders
            .start(0xCAFE, &start(3000.0, 300.0, 0.0), sber, 1)
            .changed[0];
        assert_eq!(c, 0xCAFE);
    }

    #[test]
    fn cancel_paths_and_failures() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // Too small: BuyFail without touching the exchange.
        let fx = orders.start(1, &start(100.0, 300.0, 0.0), sber, 1);
        assert!(fx.actions.is_empty());
        assert_eq!(orders.get(1).unwrap().status, status::BUY_FAIL);
        assert!(fx.logs[0].contains("below"), "{:?}", fx.logs);
        let fx = orders.fail_start(0, &start(100.0, 300.0, 0.0), "no account", 1);
        assert_eq!(fx.changed, [2]);
        assert_eq!(fx.logs, ["SBER: no account"]);
        assert_eq!(orders.get(2).unwrap().status, status::BUY_FAIL);

        // Cancel before the exchange id is known does nothing; after it, cancels.
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let id = fx.changed[0];
        let key = post_key(&fx);
        assert!(orders.cancel(id, Leg::Buy).actions.is_empty());
        assert!(orders.target(id, Leg::Buy, 299.0, None).actions.is_empty());
        let fx = orders.apply(&update(&key, "9009", ExecStatus::New, 1, 0, 0.0), 2);
        assert!(
            matches!(&fx.actions[0], Action::Cancel { exchange_id, leg: Leg::Buy, .. } if exchange_id == "9009")
        );
        assert!(orders.cancel(id, Leg::Buy).actions.is_empty(), "queued");
        orders.apply(&update("", "9009", ExecStatus::Cancelled, 1, 0, 0.0), 3);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_CANCEL);
        assert!(orders.cancel(id, Leg::Buy).actions.is_empty(), "terminal");

        // Partial fill then cancel keeps the position: BuyDone.
        let fx = orders.start(0, &start(6000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(
            &update(&key, "9010", ExecStatus::PartiallyFilled, 2, 1, 300.0),
            2,
        );
        orders.apply(&update(&key, "9010", ExecStatus::Cancelled, 2, 1, 300.0), 3);
        let rec = orders.get(id).unwrap().record();
        assert_eq!((rec.status, rec.buy.filled), (status::BUY_DONE, 10.0));
        // TargetSell from BuyDone places the exit for the filled part only.
        let fx = orders.target(id, Leg::Sell, 305.0, None);
        assert!(
            matches!(&fx.actions[0], Action::Post { leg: Leg::Sell, lots: 1, price: Some(p), .. } if *p == 305.0)
        );
        // Exit post fails at the gateway: back to BuyDone with a log line.
        let fx = orders.fail(id, Leg::Sell, Op::Post, "30079 not available", 4);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        assert!(fx.logs[0].contains("Post failed"));
        // ClosePosition sells the remainder with a limit 1.5 % through the
        // last price (no book): 300 -> 295.5; the order turns panic.
        let fx = orders.close_position(sber, false, None, false, 5);
        assert!(matches!(
            &fx.actions[0],
            Action::Post { leg: Leg::Sell, lots: 1, price: Some(p), sell: true, .. } if *p == 295.5
        ));
        let rec = orders.get(id).unwrap().record();
        assert!((rec.status, rec.panic) == (status::SELL_SET, true));
        let sell_key = post_key(&fx);
        let fill = update(&sell_key, "9011", ExecStatus::Filled, 1, 1, 301.0);
        assert!(orders.knows(&fill));
        orders.apply(&fill, 6);
        assert_eq!(orders.get(id).unwrap().status, status::SELL_DONE);

        // Rejected entry -> BuyFail; Post failure -> BuyFail.
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        let fx = orders.apply(&update(&key, "9012", ExecStatus::Rejected, 1, 0, 0.0), 2);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_FAIL);
        assert!(fx.logs[0].contains("rejected"));
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let id = fx.changed[0];
        orders.fail(
            id,
            Leg::Buy,
            Op::Post,
            "api 400/-2019: Margin is insufficient.",
            2,
        );
        assert_eq!(orders.get(id).unwrap().status, status::BUY_FAIL);
    }

    /// A pending order rests in the core, triggers when the price crosses
    /// its level, and is taken back by the terminal without an exchange call.
    #[test]
    fn pending_order_triggers_and_cancels() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().last_price = Some(250.0);
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // Below the market: it waits for a fall.
        let fx = orders.start_pending(11, &start(5000.0, 240.0, 0.0), sber, 1_000);
        let id = fx.changed[0];
        assert!(fx.actions.is_empty() && fx.logs[0].contains("pending order at 240"));
        assert_eq!(orders.get(id).unwrap().status, status::NONE);
        model.get_mut("u-sber").unwrap().last_price = Some(245.0);
        assert!(orders.watch(&model, 2_000).actions.is_empty());
        model.get_mut("u-sber").unwrap().last_price = Some(239.5);
        let fx = orders.watch(&model, 3_000);
        assert!(
            matches!(&fx.actions[..], [Action::Post { leg: Leg::Buy, lots: 2, price: Some(p), .. }] if *p == 240.0),
            "{:?}",
            fx.actions
        );
        assert_eq!(orders.get(id).unwrap().status, status::BUY_SET);

        // Another one, taken back before it triggers.
        let sber = model.get("u-sber").unwrap();
        let fx = orders.start_pending(12, &start(5000.0, 230.0, 0.0), sber, 4_000);
        let second = fx.changed[0];
        // The terminal's own «cancel buy» takes a pending order back too, not
        // only the dedicated opcode.
        let fx = orders.cancel_buy(second, 5_000);
        assert!(fx.logs[0].contains("pending order cancelled"));
        assert_eq!(orders.get(second).unwrap().status, status::BUY_CANCEL);
        model.get_mut("u-sber").unwrap().last_price = Some(229.0);
        assert!(orders.watch(&model, 6_000).actions.is_empty());
    }

    /// A pending order lives in the core, so a restart must not cost it its
    /// USDT budget (nothing is priced until the trigger crosses), and a stale
    /// feed is not a crossing: the trigger waits for the stream to come back.
    #[test]
    fn pending_order_survives_a_restart_and_waits_for_a_fresh_feed() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().last_price = Some(250.0);
        let mut orders = Orders::new();
        let fx = orders.start_pending(
            11,
            &start(5000.0, 240.0, 0.0),
            model.get("u-sber").unwrap(),
            1_000,
        );
        let id = fx.changed[0];
        let text = serde_json::to_string(&orders.persisted()).unwrap();
        let saved: Vec<CoreOrder> = serde_json::from_str(&text).unwrap();
        let mut back = Orders::starting_at(1_000_000);
        assert_eq!(back.restore(saved), 1);
        assert_eq!(back.respec(|uid| model.get(uid)), (1, 0));
        model.get_mut("u-sber").unwrap().last_price = Some(239.0);
        model.get_mut("u-sber").unwrap().feed_fresh = false;
        assert!(back.watch(&model, 2_000).actions.is_empty(), "stale feed");
        model.get_mut("u-sber").unwrap().feed_fresh = true;
        let fx = back.watch(&model, 3_000);
        // The budget came through the restart: 5000 USDT is 2 lots of 10 at 240.
        assert!(
            matches!(&fx.actions[..], [Action::Post { leg: Leg::Buy, lots: 2, price: Some(p), .. }] if *p == 240.0),
            "{:?}",
            fx.actions
        );
        assert_eq!(back.get(id).unwrap().status, status::BUY_SET);
    }

    /// «Move all» reads the resting legs; a confirmed move leaves its price
    /// on the image; «immune» is on the order.
    #[test]
    fn resting_legs_move_and_immune() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9001", ExecStatus::New, 1, 0, 300.0), 2);
        let resting = orders.resting(&sber.symbol, false);
        assert_eq!(
            (
                resting.len(),
                resting[0].id,
                resting[0].price,
                resting[0].value
            ),
            (1, id, 300.0, 3000.0)
        );
        assert!(orders.resting(&sber.symbol, true).is_empty());
        orders.set_immune(id, true);
        assert!(orders.resting(&sber.symbol, false)[0].immune);
        assert!(orders.get(id).unwrap().record().immune);
        let fx = orders.target_manual(id, Leg::Buy, 297.0, None, 3);
        let key2 = match &fx.actions[0] {
            Action::Replace { key, price, .. } if *price == 297.0 => key.clone(),
            other => panic!("{other:?}"),
        };
        orders.apply(&update(&key2, "9002", ExecStatus::New, 1, 0, 297.0), 4);
        assert_eq!(orders.get(id).unwrap().record().buy.price, 297.0);
    }

    /// Force-market close: the exit goes as a MARKET order (`PLAN.md`,
    /// «Открытые решения» п. 2), shown at the panic price until it fills; a
    /// side close leaves the other side alone.
    #[test]
    fn market_close_goes_at_market_and_side_close_filters() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().bid = Some(299.0);
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9001", ExecStatus::Filled, 1, 1, 300.0), 2);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        // Closing the shorts: a long position stays.
        assert!(orders
            .close_position(sber, false, Some(true), false, 3)
            .actions
            .is_empty());
        let fx = orders.close_position(sber, false, None, true, 4);
        assert!(
            matches!(
                &fx.actions[..],
                [Action::Post {
                    leg: Leg::Sell,
                    lots: 1,
                    price: None,
                    sell: true,
                    ..
                }]
            ),
            "{:?}",
            fx.actions
        );
        let rec = orders.get(id).unwrap().record();
        assert!(
            rec.sell.is_market && rec.sell.price == 294.51,
            "{:?}",
            rec.sell
        );
        assert!(
            fx.logs.iter().any(|l| l.contains("at MARKET")),
            "{:?}",
            fx.logs
        );
    }

    #[test]
    fn close_position_replaces_pending_exit_and_adopts_positions() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().bid = Some(299.0);
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 310.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        let fx = orders.apply(&update(&key, "9001", ExecStatus::Filled, 1, 1, 300.0), 2);
        let sell_key = post_key(&fx);
        orders.apply(&update(&sell_key, "9002", ExecStatus::New, 1, 0, 0.0), 3);
        assert_eq!(orders.get(id).unwrap().status, status::SELL_SET);
        // Close: the limit exit is amended to 1.5 % below the bid — the same
        // order, never off the book — and carries the close's reason.
        let fx = orders.close_position(sber, false, None, false, 4);
        assert!(matches!(
            &fx.actions[..],
            [Action::Replace { exchange_id, price, .. }] if exchange_id == "9002" && *price == 294.51
        ));
        assert_eq!(orders.get(id).unwrap().heading(Leg::Sell), Some(294.51));
        let rec = orders.get(id).unwrap().record();
        assert_eq!(
            (rec.status, rec.sell.price, rec.panic, rec.sell_reason),
            (status::SELL_SET, 294.51, true, reason::MANUAL_SELL)
        );
        let mut amended = update(&sell_key, "9002", ExecStatus::New, 1, 0, 0.0);
        amended.price = 294.51;
        orders.apply(&amended, 4);
        // Already through the book: a second close does nothing.
        assert!(orders
            .close_position(sber, false, None, false, 5)
            .actions
            .is_empty());

        // No core order, but units on the account: a hold, nothing is sold.
        let mut fresh = Orders::new();
        let fx = fresh.close_position(sber, false, None, false, 1);
        assert!(fx.actions.is_empty() && fx.changed.is_empty(), "{fx:?}");
        assert!(fx.logs[0].contains("left alone"), "{:?}", fx.logs);
        assert_eq!(fresh.iter().count(), 0);

        // Live core orders the exchange may have lost: a live entry and an
        // exit filled 1 of 2 lots.
        let mut boot = Orders::new();
        let fx = boot.start(0, &start(3000.0, 295.0, 0.0), sber, 1);
        let a_id = fx.changed[0];
        boot.apply(
            &update(&post_key(&fx), "7001", ExecStatus::New, 1, 0, 0.0),
            1,
        );
        let fx = boot.start(0, &start(6000.0, 300.0, 305.0), sber, 1);
        let b_id = fx.changed[0];
        let fx = boot.apply(
            &update(&post_key(&fx), "7000", ExecStatus::Filled, 2, 2, 300.0),
            1,
        );
        let exit = post_key(&fx);
        boot.apply(
            &update(&exit, "7002", ExecStatus::PartiallyFilled, 2, 1, 305.0),
            1,
        );
        let b = boot.get(b_id).unwrap();
        assert_eq!(
            (b.status, b.record().buy.filled, b.record().sell.filled),
            (status::SELL_SET, 20.0, 10.0)
        );
        let missing = boot.missing_from(&["7001"]);
        assert_eq!(
            missing,
            [Action::Query {
                order: b_id,
                leg: Leg::Sell,
                exchange_id: "7002".into()
            }]
        );
        // Replace failure asks the exchange for the truth.
        boot.target(a_id, Leg::Buy, 294.0, None);
        let fx = boot.fail(a_id, Leg::Buy, Op::Replace, "30010", 2);
        assert!(
            matches!(&fx.actions[0], Action::Query { exchange_id, .. } if exchange_id == "7001")
        );
        // The exchange no longer knows the order: the leg closes with its fills.
        let gone = "api 400/-2013: Order does not exist.";
        // One «not found» is a second read, not a verdict.
        let fx = boot.fail(b_id, Leg::Sell, Op::Query, gone, 3);
        assert!(fx.changed.is_empty());
        let fx = boot.fail(b_id, Leg::Sell, Op::Query, gone, 3);
        assert_eq!(fx.changed, [b_id]);
        assert!(fx.actions.is_empty());
        let b = boot.get(b_id).unwrap();
        assert_eq!((b.status, b.record().sell.filled), (status::BUY_DONE, 10.0));
        boot.fail(a_id, Leg::Buy, Op::Query, gone, 3);
        boot.fail(a_id, Leg::Buy, Op::Query, gone, 3);
        assert_eq!(boot.get(a_id).unwrap().status, status::BUY_CANCEL);
        assert!(boot.missing_from(&[]).is_empty());
    }

    /// `replan_exit` prices the plan against the entry the move ASKED for.
    /// A move issued behind a Replace still in flight is deferred — the
    /// order's own price is still the old one — so a ratio taken from it
    /// would come back as a wrong exit the moment the deferred move lands.
    #[test]
    fn replan_exit_prices_the_plan_against_the_requested_entry() {
        let model = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 306.0),
            model.get("u-sber").unwrap(),
            1,
        );
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9001", ExecStatus::New, 1, 0, 0.0), 2);
        orders.target_entry(id, 295.0, 3000.0);
        orders.target_entry(id, 290.0, 3000.0);
        assert_eq!(
            orders.get(id).unwrap().record().buy.price,
            295.0,
            "the second move is deferred behind the Replace in flight"
        );
        orders.replan_exit(id, 290.0, 299.4);
        let o = orders.get(id).unwrap();
        assert_eq!(o.record().planned_sell, 299.4);
        assert!(
            (o.planned_ratio() - 299.4 / 290.0).abs() < 1e-12,
            "the ratio must not come off the stale 295.00"
        );
    }

    /// Manual order the MoonBot way: the exit keeps the planned distance from
    /// the actual fill, a stop-loss fires on the bid and turns into a panic
    /// exit that follows the book, panic from BuyDone posts the exit.
    #[test]
    fn trailing_from_the_stop_editor_starts_at_take_profit_and_fires_on_the_pullback() {
        let mut model = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 306.0),
            model.get("u-sber").unwrap(),
            1,
        );
        let (id, key) = (fx.changed[0], post_key(&fx));
        let fx = orders.apply(&update(&key, "9001", ExecStatus::Filled, 1, 1, 300.0), 2);
        let sell_key = post_key(&fx);
        orders.apply(&update(&sell_key, "9002", ExecStatus::New, 1, 0, 0.0), 3);

        // A fixed trailing is refused out loud and arms nothing.
        let fx = orders.set_trailing(id, true, true, 2.0, 0.0, false, 0.0);
        assert!(fx.logs[0].contains("not supported"), "{:?}", fx.logs);
        assert_eq!(orders.get(id).unwrap().record().trailing, None);

        // An out-of-range level is refused; a stop-loss sits beside the trailing.
        let fx = orders.set_trailing(id, true, false, 150.0, 0.0, false, 0.0);
        assert!(fx.logs[0].contains("out of range"), "{:?}", fx.logs);
        orders.set_stops(id, true, true, 290.0, 1.0);
        // 1 % trailing with a 0.5 % exit spread, started by the take profit 303.
        let fx = orders.set_trailing(id, true, false, 1.0, 0.5, true, 303.0);
        assert!(fx.logs[0].contains("Trailing:ON"), "{:?}", fx.logs);
        let rec = orders.get(id).unwrap().record();
        assert_eq!(
            (rec.trailing, rec.take_profit),
            (Some((1.0, 0.5)), Some(303.0))
        );
        // The same settings again (the terminal re-sends the whole group): silent.
        let fx = orders.set_trailing(id, true, false, 1.0, 0.5, true, 303.0);
        assert!(fx.logs.is_empty() && fx.changed.is_empty());

        // Below the take profit nothing trails; a dip there does not fire.
        model.get_mut("u-sber").unwrap().bid = Some(302.0);
        assert!(orders.watch(&model, 5_000).logs.is_empty());
        model.get_mut("u-sber").unwrap().bid = Some(299.0);
        assert!(orders.watch(&model, 5_500).actions.is_empty());
        model.get_mut("u-sber").unwrap().bid = Some(303.5);
        let fx = orders.watch(&model, 6_000);
        assert!(
            fx.logs[0].contains("Trailing started at 303.5"),
            "{:?}",
            fx.logs
        );
        // The best price climbs to 305: the line is 301.95.
        model.get_mut("u-sber").unwrap().bid = Some(305.0);
        assert!(orders.watch(&model, 7_000).actions.is_empty());
        model.get_mut("u-sber").unwrap().bid = Some(302.0);
        assert!(orders.watch(&model, 8_000).actions.is_empty());
        model.get_mut("u-sber").unwrap().bid = Some(301.9);
        let fx = orders.watch(&model, 9_000);
        assert!(
            fx.logs[0].contains("Trailing stop activated"),
            "{:?}",
            fx.logs
        );
        assert!(matches!(
            &fx.actions[..],
            [Action::Cancel { exchange_id, .. }] if exchange_id == "9002"
        ));
        let o = orders.get(id).unwrap();
        assert!(o.record().panic);
        // The exit crosses the bid by the trailing spread; the stop-loss keeps its own.
        assert_eq!(o.record().stop, Some((290.0, 1.0)));
        assert!((o.panic_spread() - 0.005).abs() < 1e-12);
        assert!(o.heading(Leg::Sell).is_some_and(|p| p < 301.9 && p > 300.0));
        let mut done = update(&sell_key, "9002", ExecStatus::Cancelled, 1, 0, 306.0);
        done.unary = true;
        orders.apply(&done, 9_001);
        assert_eq!(
            orders.get(id).unwrap().record().sell_reason,
            reason::TRAILING
        );

        // Off clears the image.
        orders.set_trailing(id, false, false, 0.0, 0.0, false, 0.0);
        let rec = orders.get(id).unwrap().record();
        assert_eq!((rec.trailing, rec.take_profit), (None, None));
    }

    #[test]
    fn auto_sell_from_fill_stop_loss_and_panic() {
        let mut model = sber_model();
        let mut orders = Orders::new();
        // Planned +2 %: 300 -> 306; moving the entry moves the plan (290 -> 295.8).
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 306.0),
            model.get("u-sber").unwrap(),
            1,
        );
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9001", ExecStatus::New, 1, 0, 0.0), 2);
        let fx = orders.target(id, Leg::Buy, 290.0, None);
        let key2 = match &fx.actions[0] {
            Action::Replace { key, .. } => key.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(orders.get(id).unwrap().record().planned_sell, 295.8);
        // Filled better than the limit: the exit is +2 % from 289.5 = 295.29.
        let fx = orders.apply(&update(&key2, "9002", ExecStatus::Filled, 1, 1, 289.5), 3);
        assert!(matches!(
            &fx.actions[0],
            Action::Post { leg: Leg::Sell, price: Some(p), .. } if *p == 295.29
        ));
        let sell_key = post_key(&fx);
        orders.apply(&update(&sell_key, "9003", ExecStatus::New, 1, 0, 0.0), 4);
        assert_eq!(
            orders.get(id).unwrap().record().sell_reason,
            reason::SELL_PRICE
        );

        // Fixed stop at 288 with a 1 % spread: shown on the image, armed on the bid.
        let fx = orders.set_stops(id, true, true, 288.0, 1.0);
        assert!(fx.logs[0].contains("StopLoss:ON"));
        assert_eq!(orders.get(id).unwrap().record().stop, Some((288.0, 1.0)));
        model.get_mut("u-sber").unwrap().bid = Some(288.5);
        assert!(orders.watch(&model, 5_000).actions.is_empty());
        model.get_mut("u-sber").unwrap().bid = Some(287.9);
        let fx = orders.watch(&model, 6_000);
        assert!(fx.logs[0].contains("StopLoss activated"), "{:?}", fx.logs);
        assert!(matches!(
            &fx.actions[..],
            [Action::Cancel { exchange_id, .. }] if exchange_id == "9003"
        ));
        let rec = orders.get(id).unwrap().record();
        assert!(rec.panic && rec.sell.price == 285.02);
        // The stop's exit goes out at MARKET (`PLAN.md`, «Открытые решения»
        // п. 2) from the cancelled order's final report.
        let mut done = update(&sell_key, "9003", ExecStatus::Cancelled, 1, 0, 295.29);
        done.unary = true;
        let fx = orders.apply(&done, 6_001);
        assert!(
            matches!(
                &fx.actions[..],
                [Action::Post {
                    price: None,
                    sell: true,
                    ..
                }]
            ),
            "{fx:?}"
        );
        assert_eq!(
            orders.get(id).unwrap().record().sell_reason,
            reason::STOP_LOSS
        );
        let mut live = update(&post_key(&fx), "9004", ExecStatus::New, 1, 0, 0.0);
        live.is_market = true;
        orders.apply(&live, 6_002);
        // The book runs away: a MARKET exit takes it as it stands, no chase.
        model.get_mut("u-sber").unwrap().bid = Some(280.0);
        assert!(orders.watch(&model, 8_000).actions.is_empty());
        assert!(orders.watch(&model, 12_000).actions.is_empty());

        // Panic from BuyDone posts a MARKET exit, shown 1.5 % under the bid;
        // off keeps it.
        let sber = model.get("u-sber").unwrap();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 20);
        let (id2, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "9005", ExecStatus::Filled, 1, 1, 300.0), 21);
        assert_eq!(orders.get(id2).unwrap().status, status::BUY_DONE);
        // Percent stop for a long: 2 % under the fill.
        orders.set_stops(id2, true, false, 2.0, 0.0);
        assert_eq!(orders.get(id2).unwrap().record().stop, Some((294.0, 0.0)));
        let fx = orders.set_panic(id2, true, sber, 22_000);
        assert!(matches!(
            &fx.actions[0],
            Action::Post {
                leg: Leg::Sell,
                price: None,
                sell: true,
                ..
            }
        ));
        assert_eq!(orders.get(id2).unwrap().record().sell.price, 275.8);
        assert_eq!(
            orders.get(id2).unwrap().record().sell_reason,
            reason::PANIC_SELL
        );
        // A manual move of the exit keeps the reason.
        orders.target(id2, Leg::Sell, 276.0, None);
        assert_eq!(
            orders.get(id2).unwrap().record().sell_reason,
            reason::PANIC_SELL
        );
        let fx = orders.set_panic(id2, false, sber, 23_000);
        assert!(fx.actions.is_empty() && !orders.get(id2).unwrap().record().panic);
        // Panic on an unfilled entry only sets the flag.
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 30);
        let id3 = fx.changed[0];
        let fx = orders.set_panic(id3, true, sber, 31_000);
        assert!(fx.actions.is_empty() && orders.get(id3).unwrap().record().panic);
    }

    /// A restart: live orders come back with their legs, stops and ids; a
    /// request in flight is asked by key, finished orders are not kept, and
    /// reports under the old keys still find their order.
    #[test]
    fn persisted_orders_restore_after_a_restart() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // A: filled entry, acked exit, a stop.
        let fx = orders.start(0, &start(3000.0, 300.0, 306.0), sber, 1);
        let (a, key) = (fx.changed[0], post_key(&fx));
        let fx = orders.apply(&update(&key, "b1", ExecStatus::Filled, 1, 1, 300.0), 2);
        let exit_key = post_key(&fx);
        orders.apply(&update(&exit_key, "s1", ExecStatus::New, 1, 0, 0.0), 3);
        orders.set_stops(a, true, true, 290.0, 1.0);
        // B: an entry re-placed many times, the last Replace without a reply.
        let fx = orders.start(0, &start(3000.0, 299.0, 0.0), sber, 4);
        let (b, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "e0", ExecStatus::New, 1, 0, 0.0), 5);
        let mut last_key = key;
        for i in 1..=10 {
            let fx = orders.target(b, Leg::Buy, 299.0 - f64::from(i) * 0.1, None);
            let Action::Replace { key, .. } = &fx.actions[0] else {
                panic!("{:?}", fx.actions)
            };
            last_key = key.clone();
            if i < 10 {
                orders.apply(
                    &update(key, &format!("e{i}"), ExecStatus::New, 1, 0, 0.0),
                    6 + i64::from(i),
                );
            }
        }
        // C: finished — not kept.
        let fx = orders.start(0, &start(3000.0, 298.0, 0.0), sber, 30);
        let (c, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "c1", ExecStatus::Cancelled, 1, 0, 0.0), 31);

        let text = serde_json::to_string(&orders.persisted()).unwrap();
        let saved: Vec<CoreOrder> = serde_json::from_str(&text).unwrap();
        let mut back = Orders::starting_at(1_000_000);
        assert_eq!(back.restore(saved), 2);
        assert!(back.get(c).is_none());

        let (was, now) = (
            orders.get(a).unwrap().record(),
            back.get(a).unwrap().record(),
        );
        assert_eq!(
            (now.status, now.stop, now.sell.price),
            (was.status, was.stop, was.sell.price)
        );
        assert_eq!(
            (now.buy, now.sell.exchange_id),
            (was.buy, was.sell.exchange_id)
        );
        assert_eq!(back.get(a).unwrap().exit_reason(), reason::SELL_PRICE);
        // The ledger of B keeps the live generations and the last keys only.
        assert!(back.get(b).unwrap().buy.executions.len() <= KEEP_KEYS + 1);
        // B's unanswered Replace is asked by its key on the first watch.
        let fx = back.watch(&model, 100);
        assert!(
            fx.actions.iter().any(|x| matches!(x, Action::QueryRequest { order, key, .. } if *order == b && *key == last_key)),
            "{:?}",
            fx.actions
        );
        // A's exit fill under its exchange id closes the restored order.
        back.apply(&update("", "s1", ExecStatus::Filled, 1, 1, 306.0), 101);
        assert_eq!(back.get(a).unwrap().status, status::SELL_DONE);
        // New ids never collide with restored ones.
        let fx = back.start(0, &start(3000.0, 300.0, 0.0), sber, 102);
        assert!(![a, b].contains(&fx.changed[0]));
    }

    /// The line a leg drew is the picture's, so it must hold what the order
    /// did and nothing else: one rest per place it stood, none for a price
    /// that only got rounded, and it must survive the file.
    #[test]
    fn a_replaced_entry_keeps_the_line_it_drew() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.note_moves(&fx.changed, 1_000);
        // Asked again with nothing moved: still one rest, not two.
        orders.note_moves(&[id], 2_000);
        // The exchange acknowledges it; only then is there an order to move.
        orders.apply(&update(&key, "e0", ExecStatus::New, 1, 0, 0.0), 2_100);
        let fx = orders.target(id, Leg::Buy, 299.0, None);
        orders.note_moves(&fx.changed, 3_000);
        let line: Vec<(i64, f64)> = orders
            .get(id)
            .unwrap()
            .moves(Leg::Buy)
            .iter()
            .map(|m| (m.at, m.price))
            .collect();
        assert_eq!(line, vec![(1_000, 300.0), (3_000, 299.0)]);
        // A clock that stepped back must not put the line out of order: it is
        // read as a step function, and a moment before its predecessor would
        // cut the drawing short.
        let Some(Action::Replace { key, .. }) = fx.actions.first() else {
            panic!("{:?}", fx.actions)
        };
        orders.apply(&update(key, "e1", ExecStatus::New, 1, 0, 0.0), 3_100);
        orders.target(id, Leg::Buy, 298.0, None);
        orders.note_moves(&[id], 2_500);
        let ats: Vec<i64> = orders
            .get(id)
            .unwrap()
            .moves(Leg::Buy)
            .iter()
            .map(|m| m.at)
            .collect();
        assert!(ats.windows(2).all(|w| w[0] <= w[1]), "{ats:?}");

        let text = serde_json::to_string(&orders.persisted()).unwrap();
        let saved: Vec<CoreOrder> = serde_json::from_str(&text).unwrap();
        assert_eq!(saved[0].moves(Leg::Buy).len(), 3);
    }

    /// The persisted order grew two fields. An `orders.json` written before
    /// them is the file the live core restarts on, and it must still load —
    /// with no line, which is the flat fallback the picture always drew.
    #[test]
    fn an_orders_file_from_before_the_lines_still_loads() {
        // A leg as the store writes it, minus the two fields: `LegState` is a
        // vendored struct with no serde defaults, so every one of its own is
        // here, and a file missing one of THOSE would not have loaded before
        // this change either.
        let leg = r#""key":"k","lots":1,"state":{"exchange_id":9,"price":300.0,
            "quantity":1.0,"filled":0.0,"mean_price":0.0,"notional":300.0,"spent":0.0,
            "create_ms":1,"open_ms":1,"close_ms":0,"is_market":false,"opened":true,
            "closed":false,"canceled":false}"#;
        let old = format!(
            r#"[{{"id":7,"uid":"u-sber","market":"SBER","status":2,"lot":1.0,"tick":0.01,
            "buy":{{{leg}}}}}]"#
        );
        let saved: Vec<CoreOrder> = serde_json::from_str(&old).expect("an older orders.json");
        let o = saved.first().expect("the order");
        assert_eq!((o.id, o.buy.state.price), (7, 300.0));
        assert!(o.moves(Leg::Buy).is_empty() && o.moves(Leg::Sell).is_empty());
    }

    /// Commission lookups: the broker id when known, else our request key.
    #[test]
    fn filled_orders_fall_back_to_the_request_key() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 306.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        // An instant fill known only by its RIN0 alias.
        let fx = orders.apply(
            &update(&key, "RIN0AHPEZ", ExecStatus::Filled, 1, 1, 300.0),
            2,
        );
        let exit_key = post_key(&fx);
        orders.apply(
            &update(&exit_key, "8455", ExecStatus::Filled, 1, 1, 306.0),
            3,
        );
        let o = orders.get(id).unwrap();
        assert_eq!(o.status, status::SELL_DONE);
        assert_eq!(
            o.filled_orders(),
            [
                FilledOrder {
                    id: key,
                    by_request: true
                },
                FilledOrder {
                    id: "8455".into(),
                    by_request: false
                },
            ]
        );
    }

    /// The reason belongs to the exit generation: a refused move leaves the
    /// live exit's reason, and the fill of that exit is what the order reports.
    #[test]
    fn refused_move_keeps_the_live_exits_reason() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 306.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        let fx = orders.apply(&update(&key, "b1", ExecStatus::Filled, 1, 1, 300.0), 2);
        let exit_key = post_key(&fx);
        orders.apply(&update(&exit_key, "s1", ExecStatus::New, 1, 0, 0.0), 3);
        assert_eq!(orders.get(id).unwrap().exit_reason(), reason::SELL_PRICE);
        orders.set_sell_reason(id, reason::STOP_LOSS);
        let fx = orders.target(id, Leg::Sell, 297.0, None);
        assert!(matches!(&fx.actions[..], [Action::Replace { .. }]));
        assert_eq!(orders.get(id).unwrap().exit_reason(), reason::STOP_LOSS);
        // The exchange refuses the amend: the order stands as it was, price
        // and reason, and is read again.
        let refusal = "api 400/-4014: Price not increased by tick size.";
        let fx = orders.fail(id, Leg::Sell, Op::Replace, refusal, 4);
        assert!(matches!(&fx.actions[..], [Action::Query { .. }]));
        let o = orders.get(id).unwrap();
        assert_eq!(
            (o.exit_reason(), o.record().sell.price),
            (reason::SELL_PRICE, 306.0)
        );
        assert!(!o.sell.replacing);
        // The old exit had filled: the deferred move has nothing left to sell.
        let fx = orders.apply(&update(&exit_key, "s1", ExecStatus::Filled, 1, 1, 306.0), 5);
        assert!(fx.actions.is_empty());
        let o = orders.get(id).unwrap();
        assert_eq!(
            (o.status, o.exit_reason()),
            (status::SELL_DONE, reason::SELL_PRICE)
        );
    }

    /// The stream reports an order under a UUID before the broker's numeric
    /// id (the only one REST accepts); reports under either id are ours.
    #[test]
    fn numeric_exchange_id_wins_and_adopted_orders_follow_their_key() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        // Stream first (UUID), REST reply second (numeric).
        orders.apply(&update(&key, "01a0-uuid", ExecStatus::New, 1, 0, 0.0), 2);
        orders.apply(&update(&key, "8433", ExecStatus::New, 1, 0, 0.0), 3);
        let fx = orders.cancel(id, Leg::Buy);
        assert!(
            matches!(&fx.actions[0], Action::Cancel { exchange_id, .. } if exchange_id == "8433")
        );
        orders.apply(
            &update("", "01a0-uuid", ExecStatus::Cancelled, 1, 0, 0.0),
            4,
        );
        assert_eq!(orders.get(id).unwrap().status, status::BUY_CANCEL);

        // A foreign order left under the UUID stays left under the numeric
        // id: the shared request key ties its reports together.
        let mut boot = Orders::new();
        let fx = boot.adopt(
            &update("k-c", "01a1-uuid", ExecStatus::New, 1, 0, 0.0),
            sber,
            Some(0.0),
            1,
        );
        assert!(fx.changed.is_empty() && fx.logs.len() == 1, "{fx:?}");
        let fx = boot.adopt(
            &update("k-c", "8434", ExecStatus::Filled, 1, 1, 300.0),
            sber,
            Some(0.0),
            2,
        );
        assert_eq!(fx, Effects::default());
        assert_eq!(boot.iter().count(), 0);
    }

    /// SU26254 22.09: a bond bought in the broker's app for a hold turned
    /// into a manual order and the terminal drew a stop on it. The core
    /// manages only what it opened: an app purchase stays the account's, and
    /// an app sale the hold covers never closes the core's own position on
    /// the same instrument, not even when its fill lands after `GetPositions`
    /// has already shrunk the hold.
    #[test]
    fn orders_from_the_brokers_app_are_a_hold_left_alone() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let mut buy = update("app-buy", "9001", ExecStatus::New, 5, 0, 0.0);
        buy.price = 300.0;
        let fx = os.adopt(&buy, sber, Some(0.0), 1);
        assert!(fx.changed.is_empty() && fx.actions.is_empty(), "{fx:?}");
        assert!(fx.logs[0].contains("left to the account"), "{:?}", fx.logs);
        buy.exchange_id = "9001-uuid".into();
        buy.status = ExecStatus::Filled;
        buy.lots_executed = 5;
        assert_eq!(os.adopt(&buy, sber, Some(0.0), 2), Effects::default());
        assert_eq!(os.iter().count(), 0);

        // The terminal's own 2 lots beside a 5-lot hold (70 units held).
        let fx = os.start(0, &start(6000.0, 300.0, 0.0), sber, 10);
        let id = fx.changed[0];
        os.apply(
            &update(&post_key(&fx), "9100", ExecStatus::Filled, 2, 2, 300.0),
            11,
        );
        assert_eq!(os.get(id).unwrap().status, status::BUY_DONE);
        let mut sale = update("app-sale", "9002", ExecStatus::New, 3, 0, 0.0);
        sale.sell = true;
        sale.price = 305.0;
        assert!(os.adopt(&sale, sber, Some(70.0), 20).changed.is_empty());
        sale.status = ExecStatus::Filled;
        sale.lots_executed = 3;
        assert_eq!(os.adopt(&sale, sber, Some(40.0), 21), Effects::default());
        let o = os.get(id).unwrap();
        assert_eq!((o.status, o.record().sell.filled), (status::BUY_DONE, 0.0));

        // A sale beyond the hold (2 lots left, 3 sold) is the terminal's.
        let mut beyond = update("app-sale-2", "9003", ExecStatus::New, 3, 0, 0.0);
        beyond.sell = true;
        beyond.price = 306.0;
        assert_eq!(os.adopt(&beyond, sber, Some(40.0), 30).changed, [id]);
        assert_eq!(os.get(id).unwrap().status, status::SELL_SET);

        // Before the first `GetPositions` a sale is only skipped: its next
        // report, with the positions known, still closes the terminal's lots.
        let mut os = Orders::new();
        let fx = os.start(0, &start(6000.0, 300.0, 0.0), sber, 10);
        let id = fx.changed[0];
        os.apply(
            &update(&post_key(&fx), "9200", ExecStatus::Filled, 2, 2, 300.0),
            11,
        );
        let mut early = update("app-sale-0", "9004", ExecStatus::New, 2, 0, 0.0);
        early.sell = true;
        early.price = 306.0;
        let fx = os.adopt(&early, sber, None, 25);
        assert!(
            fx.changed.is_empty() && fx.logs[0].contains("waits"),
            "{fx:?}"
        );
        assert_eq!(os.get(id).unwrap().status, status::BUY_DONE);
        early.status = ExecStatus::Filled;
        early.lots_executed = 2;
        early.avg_price = 306.0;
        assert_eq!(os.adopt(&early, sber, Some(20.0), 26).changed, [id]);
        assert_eq!(os.get(id).unwrap().status, status::SELL_DONE);
    }

    /// An app sale the hold covered is left to the account for good: after a
    /// restart its fill lands with the hold already shrunk by it, and only the
    /// remembered ids keep it off the terminal's own position (SU26254 22.09).
    #[test]
    fn an_order_left_to_the_account_stays_left_across_a_restart() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(0, &start(6000.0, 300.0, 0.0), sber, 10);
        let id = fx.changed[0];
        os.apply(
            &update(&post_key(&fx), "9100", ExecStatus::Filled, 2, 2, 300.0),
            11,
        );
        // A 3-lot app sale beside a 5-lot hold (70 units held, 20 the core's).
        let mut sale = update("app-sale", "9002", ExecStatus::New, 3, 0, 0.0);
        sale.sell = true;
        sale.price = 305.0;
        assert!(os.adopt(&sale, sber, Some(70.0), 20).changed.is_empty());
        // The restart brings the orders and the left ids back from the snapshot.
        let saved: Vec<CoreOrder> =
            serde_json::from_value(serde_json::Value::Array(os.persisted())).unwrap();
        let mut boot = Orders::new();
        assert_eq!(boot.restore(saved), 1);
        boot.restore_left(os.left_ids());
        sale.status = ExecStatus::Filled;
        sale.lots_executed = 3;
        assert_eq!(boot.adopt(&sale, sber, Some(40.0), 30), Effects::default());
        assert_eq!(boot.get(id).unwrap().status, status::BUY_DONE);
    }

    /// A Cancel refused by the exchange reads the order back; a Cancel lost
    /// on the transport (DNS outage 17.09) is only logged — the caller retries
    /// the Cancel, a Query over the same dead link would just double the noise.
    #[test]
    fn cancel_failure_queries_only_on_a_definitive_refusal() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "8433", ExecStatus::New, 1, 0, 0.0), 2);
        let cancel = orders.cancel(id, Leg::Buy).actions.remove(0);
        let fx = orders.failed(&cancel, false, "transport: io: lookup failed", 3);
        assert_eq!(fx.logs.len(), 1);
        assert!(fx.actions.is_empty());
        assert!(orders.map[&id].buy.cancel_requested);
        let fx = orders.failed(&cancel, true, "HTTP 400 30059", 4);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Query { exchange_id, .. }] if exchange_id == "8433")
        );
    }

    /// A queued Cancel is not duplicated by the 5 s MoonShot retry or by a
    /// stream report while the worker still holds it (17.09: 275 × `30059`
    /// from second Cancels behind a burst of AutoCancelBuy). A failed Cancel
    /// or a new exchange id lets the next request go out.
    #[test]
    fn queued_cancel_is_sent_once_until_its_outcome() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "8433", ExecStatus::New, 1, 0, 0.0), 2);
        let cancel = orders.cancel(id, Leg::Buy).actions.remove(0);
        assert!(orders.cancel(id, Leg::Buy).actions.is_empty());
        let fx = orders.apply(&update(&key, "8433", ExecStatus::New, 1, 0, 0.0), 3);
        assert!(fx.actions.is_empty(), "{fx:?}");
        assert!(orders.map[&id].buy.cancel_requested);

        let fx = orders.failed(&cancel, false, "transport: io: lookup failed", 4);
        assert!(fx.actions.is_empty());
        assert_eq!(orders.cancel(id, Leg::Buy).actions.len(), 1);

        orders.apply(&update("", "8433", ExecStatus::Cancelled, 1, 0, 0.0), 5);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_CANCEL);
        assert!(!orders.map[&id].buy.cancel_sent);

        // Cancel requested while a Replace is in flight goes to the new id once.
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "b1", ExecStatus::New, 1, 0, 0.0), 2);
        let fx = orders.target(id, Leg::Buy, 299.0, None);
        let Action::Replace { key: k2, .. } = &fx.actions[0] else {
            panic!("{fx:?}");
        };
        let k2 = k2.clone();
        assert!(orders.cancel(id, Leg::Buy).actions.is_empty());
        let fx = orders.apply(&update(&k2, "b2", ExecStatus::New, 1, 0, 0.0), 3);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Cancel { exchange_id, .. }] if exchange_id == "b2"),
            "{fx:?}"
        );
        let fx = orders.apply(&update(&k2, "b2", ExecStatus::New, 1, 0, 0.0), 4);
        assert!(fx.actions.is_empty(), "{fx:?}");
    }

    /// An amend moves the entry at the tick, the same order under the same
    /// key; a refused one leaves the order as it was, and the read that
    /// follows decides — here, that the order had ended.
    #[test]
    fn an_amended_entry_keeps_its_order_and_a_refusal_puts_it_back() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 306.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "b1", ExecStatus::New, 1, 0, 0.0), 2);
        // The chart line comes unrounded; the order goes at the tick.
        let fx = orders.target(id, Leg::Buy, 297.0794, None);
        let Action::Replace {
            key: k2,
            price,
            exchange_id,
            ..
        } = &fx.actions[0]
        else {
            panic!("{fx:?}");
        };
        assert_eq!((*price, k2, exchange_id.as_str()), (297.08, &key, "b1"));
        let mut amended = update(&key, "b1", ExecStatus::New, 1, 0, 0.0);
        amended.price = 297.08;
        orders.apply(&amended, 5);
        let rec = orders.get(id).unwrap().record();
        assert_eq!(
            (rec.status, rec.buy.price, rec.buy.exchange_id),
            (status::BUY_SET, 297.08, wire_id("b1"))
        );

        // Amend refused: the order as it was, read again.
        let fx = orders.target(id, Leg::Buy, 296.0, None);
        assert!(matches!(&fx.actions[0], Action::Replace { .. }));
        let gone = "api 400/-2013: Order does not exist.";
        let fx = orders.fail(id, Leg::Buy, Op::Replace, gone, 6);
        assert!(matches!(&fx.actions[0], Action::Query { exchange_id, .. } if exchange_id == "b1"));
        assert_eq!(orders.get(id).unwrap().record().buy.price, 297.08);
        orders.apply(&update("", "b1", ExecStatus::Cancelled, 1, 0, 0.0), 7);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_CANCEL);
    }

    /// A planned exit the exchange refused before it opened is retried when
    /// the price band becomes known; a cancelled one is not.
    #[test]
    fn refused_planned_exit_is_retried_once() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 306.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.apply(&update(&key, "b1", ExecStatus::Filled, 1, 1, 300.0), 2);
        let fx = orders.fail(
            id,
            Leg::Sell,
            Op::Post,
            "api 400/-4024: Price over max price.",
            3,
        );
        assert!(fx.actions.is_empty());
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        let fx = orders.retry_exits("u-sber");
        assert!(matches!(
            &fx.actions[0],
            Action::Post { leg: Leg::Sell, price: Some(p), .. } if *p == 306.0
        ));
        assert_eq!(orders.get(id).unwrap().status, status::SELL_SET);
        let k = post_key(&fx);
        orders.apply(&update(&k, "s1", ExecStatus::New, 1, 0, 0.0), 4);
        orders.apply(&update("", "s1", ExecStatus::Cancelled, 1, 0, 0.0), 5);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        assert!(orders.retry_exits("u-sber").actions.is_empty());
    }

    /// A position sold from the broker's app: the foreign SELL becomes the
    /// order's exit when its report arrives, and `GetPositions` closes an order
    /// whose units are gone when no report came (stream down).
    #[test]
    fn foreign_sell_and_missing_units_close_the_position() {
        let mut model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        // Two long orders in position (1 lot each) plus a third with a live exit.
        let mut ids = Vec::new();
        for (t, planned) in [(1, 0.0), (2, 0.0), (3, 310.0)] {
            let fx = orders.start(0, &start(3000.0, 300.0, planned), sber, t);
            let (id, key) = (fx.changed[0], post_key(&fx));
            orders.apply(
                &update(&key, &format!("ex{t}"), ExecStatus::Filled, 1, 1, 300.0),
                t + 10,
            );
            ids.push(id);
        }
        assert_eq!(orders.get(ids[2]).unwrap().status, status::SELL_SET);
        // A percent stop is re-derived from the fill: 2 % under 300 -> 294.
        orders.set_stops(ids[0], true, false, 2.0, 0.0);
        assert_eq!(
            orders.get(ids[0]).unwrap().record().stop,
            Some((294.0, 0.0))
        );

        // Foreign SELL of 1 lot: the oldest matching order takes it as its exit.
        let mut sell = update("app-request", "app-1", ExecStatus::New, 1, 0, 0.0);
        sell.sell = true;
        sell.price = 305.0;
        let fx = orders.adopt(&sell, sber, Some(0.0), 1_000);
        assert_eq!(fx.changed, [ids[0]]);
        assert!(fx.logs[0].contains("outside the core"), "{:?}", fx.logs);
        let o = orders.get(ids[0]).unwrap();
        assert_eq!((o.status, o.record().sell.price), (status::SELL_SET, 305.0));
        // A rejected foreign order is nobody's exit.
        let mut refused = sell.clone();
        refused.exchange_id = "app-x".into();
        refused.status = ExecStatus::Rejected;
        assert_eq!(
            orders.adopt(&refused, sber, Some(0.0), 1_050),
            Effects::default()
        );
        // The shared request id proves the numeric report belongs to this exit.
        sell.exchange_id = "84372344326".into();
        sell.status = ExecStatus::Filled;
        sell.lots_executed = 1;
        sell.avg_price = 305.5;
        assert!(orders.knows(&sell));
        let fx = orders.adopt(&sell, sber, Some(0.0), 1_100);
        assert_eq!(fx.changed, [ids[0]]);
        let rec = orders.get(ids[0]).unwrap().record();
        assert_eq!(
            (rec.status, rec.sell.mean_price),
            (status::SELL_DONE, 305.5)
        );

        // Positions: the account holds only the blocked lot of the live exit.
        model.get_mut("u-sber").unwrap().bid = Some(299.0);
        let held: HashMap<String, f64> = [("u-sber".to_string(), 10.0)].into();
        // Within the grace after a fill nothing is decided.
        assert_eq!(orders.reconcile(&held, &model, 5_000), Effects::default());
        // One read that lacks the units decides nothing; the second in a row does.
        assert_eq!(orders.reconcile(&held, &model, 29_000), Effects::default());
        let fx = orders.reconcile(&held, &model, 39_000);
        assert_eq!(fx.changed, [ids[1]]);
        assert!(fx.logs[0].contains("holds 10 of 20"), "{:?}", fx.logs);
        let rec = orders.get(ids[1]).unwrap().record();
        assert_eq!(
            (
                rec.status,
                rec.sell.mean_price,
                rec.sell.filled,
                rec.sell.closed
            ),
            (status::SELL_DONE, 299.0, 10.0, true)
        );
        // The live exit is left to its own report; a matching account is quiet.
        assert_eq!(orders.get(ids[2]).unwrap().status, status::SELL_SET);
        assert_eq!(orders.reconcile(&held, &model, 41_000), Effects::default());
    }

    #[test]
    fn review_partial_exit_cancel_must_keep_residual_position() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        let fx = orders.apply(
            &update(&post_key(&fx), "b1", ExecStatus::Filled, 2, 2, 300.0),
            1_000_001,
        );
        let key = post_key(&fx);
        orders.apply(
            &update(&key, "s1", ExecStatus::PartiallyFilled, 2, 1, 306.0),
            1_000_002,
        );
        orders.apply(
            &update(&key, "s1", ExecStatus::Cancelled, 2, 1, 306.0),
            1_000_003,
        );
        assert_eq!(
            orders.get(id).unwrap().status,
            status::BUY_DONE,
            "10 units are still held"
        );
        let fx = orders.target(id, Leg::Sell, 305.0, None);
        assert!(matches!(fx.actions[0], Action::Post { lots: 1, .. }));
    }

    #[test]
    fn review_replace_must_preserve_previous_exit_fills() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        let fx = orders.apply(
            &update(&post_key(&fx), "b1", ExecStatus::Filled, 2, 2, 300.0),
            1_000_001,
        );
        orders.apply(
            &update(
                &post_key(&fx),
                "s1",
                ExecStatus::PartiallyFilled,
                2,
                1,
                306.0,
            ),
            1_000_002,
        );
        // The amend names the exit's whole size: the lot it sold and the one
        // still to sell — never two more.
        let fx = orders.target(id, Leg::Sell, 305.0, None);
        let [Action::Replace { lots, key, .. }] = &fx.actions[..] else {
            panic!("{fx:?}")
        };
        assert_eq!(*lots, 2);
        let mut amended = update(key, "s1", ExecStatus::PartiallyFilled, 2, 1, 306.0);
        amended.price = 305.0;
        orders.apply(&amended, 1_000_003);
        let rec = orders.get(id).unwrap().record();
        assert_eq!(
            (rec.sell.filled, rec.sell.quantity, rec.sell.price),
            (10.0, 20.0, 305.0),
            "the order already sold 10 units"
        );
    }

    #[test]
    fn review_pending_replace_must_not_lose_acknowledgement_key() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        orders.apply(
            &update(&post_key(&fx), "b1", ExecStatus::New, 1, 0, 0.0),
            1_000_001,
        );
        let fx = orders.target(id, Leg::Buy, 299.0, None);
        let Action::Replace { key, .. } = &fx.actions[0] else {
            panic!()
        };
        orders.target(id, Leg::Buy, 298.0, None);
        assert!(
            orders.knows(&update(key, "b2", ExecStatus::New, 1, 0, 0.0)),
            "first replace reply is now mistaken for a foreign order"
        );
    }

    #[test]
    fn review_late_post_failure_must_not_overwrite_confirmed_fill() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        orders.apply(
            &update(&post_key(&fx), "b1", ExecStatus::Filled, 1, 1, 300.0),
            1_000_001,
        );
        orders.fail(
            id,
            Leg::Buy,
            Op::Post,
            "timeout after exchange accepted order",
            1_000_002,
        );
        assert_eq!(
            orders.get(id).unwrap().status,
            status::BUY_DONE,
            "confirmed position must remain managed"
        );
    }

    #[test]
    fn review_late_new_report_must_not_erase_partial_fill() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(6000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        let key = post_key(&fx);
        orders.apply(
            &update(&key, "b1", ExecStatus::PartiallyFilled, 2, 1, 300.0),
            1_000_002,
        );
        orders.apply(&update(&key, "b1", ExecStatus::New, 2, 0, 0.0), 1_000_003);
        assert_eq!(
            orders.get(id).unwrap().record().buy.filled,
            10.0,
            "REST NEW may arrive after stream fill"
        );
    }

    #[test]
    fn review_rejected_exit_must_leave_position_manageable() {
        let m = sber_model();
        let mut orders = Orders::new();
        let fx = orders.start(
            0,
            &start(3000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1_000_000,
        );
        let id = fx.changed[0];
        let fx = orders.apply(
            &update(&post_key(&fx), "b1", ExecStatus::Filled, 1, 1, 300.0),
            1_000_001,
        );
        orders.apply(
            &update(&post_key(&fx), "s1", ExecStatus::Rejected, 1, 0, 0.0),
            1_000_002,
        );
        assert_eq!(
            orders.get(id).unwrap().status,
            status::BUY_DONE,
            "rejected exit has not closed the position"
        );
    }

    /// An exit cancelled by hand after one lot sold, a new one placed for the
    /// rest: a fill of the old order reported late cuts the new one to the
    /// remainder (an amend), and the position closes at the mean of both.
    #[test]
    fn late_retired_exit_fill_resizes_the_live_remainder_and_keeps_mean() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(9000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b", ExecStatus::Filled, 3, 3, 300.0),
            1001,
        );
        let old = post_key(&fx);
        os.apply(
            &update(&old, "s1", ExecStatus::PartiallyFilled, 3, 1, 306.0),
            1002,
        );
        let fx = os.cancel(id, Leg::Sell);
        assert!(matches!(&fx.actions[..], [Action::Cancel { .. }]), "{fx:?}");
        // TMB ASTR 17.08: the state read after the Cancel lags the exchange.
        let mut stale = update(&old, "s1", ExecStatus::Cancelled, 3, 1, 306.0);
        stale.unary = true;
        os.apply(&stale, 1003);
        let fx = os.target(id, Leg::Sell, 305.0, None);
        let [Action::Post { key, lots: 2, .. }] = &fx.actions[..] else {
            panic!("{fx:?}");
        };
        let key = key.clone();
        os.apply(&update(&key, "s2", ExecStatus::New, 2, 0, 0.0), 1003);
        // Another lot executed on s1 just before cancellation; its report
        // reaches us after the new exit: that one is amended to the remainder.
        let fx = os.apply(
            &update(&old, "s1", ExecStatus::Cancelled, 3, 2, 306.0),
            1004,
        );
        assert_eq!(os.get(id).unwrap().record().sell.filled, 20.0);
        let [Action::Replace {
            exchange_id,
            lots: 1,
            ..
        }] = &fx.actions[..]
        else {
            panic!("{fx:?}");
        };
        assert_eq!(exchange_id, "s2");
        let mut amended = update(&key, "s2", ExecStatus::New, 1, 0, 0.0);
        amended.price = 305.0;
        os.apply(&amended, 1004);
        os.apply(&update(&key, "s2", ExecStatus::Filled, 1, 1, 305.0), 1005);
        let r = os.get(id).unwrap().record();
        assert_eq!((r.status, r.sell.filled), (status::SELL_DONE, 30.0));
        assert!((r.sell.mean_price - (306.0 * 2.0 + 305.0) / 3.0).abs() < 1e-9);
        let fx = os.apply(&update(&old, "s1", ExecStatus::New, 3, 0, 0.0), 1006);
        assert!(fx.actions.is_empty());
        assert_eq!(os.get(id).unwrap().status, status::SELL_DONE);
    }

    /// TMB SHU6 14.08 / ETLN 17.08: TInvestCore's `ReplaceOrder` re-placed the
    /// full lots while the old exit kept filling, and its fills reached the
    /// core seconds late — the move oversold into a short. Aster's amend names
    /// the order's whole size, fills included: a lot sold that the core has
    /// not seen yet counts against it, and nothing sells past the position.
    #[test]
    fn an_exit_move_is_an_amend_of_its_whole_size() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let buy = post_key(&fx);
        let fx = os.apply(&update(&buy, "b", ExecStatus::Filled, 2, 2, 300.0), 1001);
        let old = post_key(&fx);
        os.apply(&update(&old, "s1", ExecStatus::New, 2, 0, 0.0), 1002);
        // s1 already sold one lot on the exchange; the core has not seen it.
        let fx = os.target(id, Leg::Sell, 304.0, None);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { exchange_id, lots: 2, .. }] if exchange_id == "s1"),
            "{fx:?}"
        );
        assert_eq!(os.get(id).unwrap().heading(Leg::Sell), Some(304.0));
        // A second step while the amend is out only moves the target.
        assert!(os.target(id, Leg::Sell, 303.0, None).actions.is_empty());
        let mut amended = update(&old, "s1", ExecStatus::PartiallyFilled, 2, 1, 306.0);
        amended.price = 304.0;
        let fx = os.apply(&amended, 1003);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { lots: 2, price, .. }] if *price == 303.0),
            "{fx:?}"
        );
        let o = os.get(id).unwrap();
        assert_eq!((o.status, o.record().sell.filled), (status::SELL_SET, 10.0));
    }

    /// A late entry fill while an exit amend is out: the next amend covers the
    /// grown position at the latest target, not the older one.
    #[test]
    fn late_entry_fill_during_exit_move_keeps_the_latest_target() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let buy = post_key(&fx);
        os.apply(&update(&buy, "b1", ExecStatus::New, 2, 0, 0.0), 1001);
        // The entry ends with one of its two lots: the exit goes for that one.
        let fx = os.apply(
            &update(&buy, "b1", ExecStatus::Cancelled, 2, 1, 300.0),
            1002,
        );
        let exit = post_key(&fx);
        os.apply(&update(&exit, "s1", ExecStatus::New, 1, 0, 0.0), 1003);
        os.target(id, Leg::Sell, 304.0, None);
        assert!(os.target(id, Leg::Sell, 303.0, None).actions.is_empty());
        // The second lot's fill, reported late.
        os.apply(
            &update(&buy, "b1", ExecStatus::Cancelled, 2, 2, 300.0),
            1004,
        );
        assert_eq!(os.get(id).unwrap().heading(Leg::Sell), Some(303.0));
        let mut amended = update(&exit, "s1", ExecStatus::New, 1, 0, 0.0);
        amended.price = 304.0;
        let fx = os.apply(&amended, 1005);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { lots: 2, price, .. }] if *price == 303.0),
            "{fx:?}"
        );
    }

    /// An amend of unknown fate (the transport failed) is read by our key
    /// before the next move goes, again if the read fails too; the read
    /// settles it — here, that it never landed, so the exit keeps its reason.
    #[test]
    fn an_amend_of_unknown_fate_is_read_before_the_next_move() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b", ExecStatus::Filled, 2, 2, 300.0),
            1001,
        );
        let exit = post_key(&fx);
        os.apply(&update(&exit, "s1", ExecStatus::New, 2, 0, 0.0), 1002);
        os.set_sell_reason(id, reason::STOP_LOSS);
        let fx = os.target(id, Leg::Sell, 304.0, None);
        let fx = os.failed(&fx.actions[0], false, "transport", 1003);
        let [read @ Action::QueryRequest { key, .. }] = &fx.actions[..] else {
            panic!("{fx:?}");
        };
        assert_eq!(*key, exit);
        assert!(os.target(id, Leg::Sell, 303.0, None).actions.is_empty());
        // The read fails as well: asked again, not forgotten.
        assert!(os.failed(read, false, "transport", 1004).actions.is_empty());
        assert!(os
            .watch(&m, 1004 + RESOLVE_PERIOD_MS)
            .actions
            .iter()
            .any(|a| matches!(a, Action::QueryRequest { key, .. } if *key == exit)));
        // The read: the amend never landed.
        let mut read = update(&exit, "s1", ExecStatus::New, 2, 0, 0.0);
        read.price = 306.0;
        let fx = os.apply(&read, 15_000);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { price, .. }] if *price == 303.0),
            "{fx:?}"
        );
        let mut answer = update(&exit, "s1", ExecStatus::New, 2, 0, 0.0);
        answer.price = 303.0;
        os.apply(&answer, 15_001);
        let o = os.get(id).unwrap();
        assert_eq!((o.record().sell.price, o.sell.uncertain), (303.0, false));
    }

    /// Every amend of an order goes under its one key, so only a report of
    /// what the amend asked for settles it: not the stream racing it with the
    /// order as it stood (a size-only amend included), not the late answer or
    /// the late refusal of an earlier amend.
    #[test]
    fn an_amend_is_settled_only_by_what_it_asked_for() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let (id, key) = (fx.changed[0], post_key(&fx));
        os.apply(&update(&key, "b1", ExecStatus::New, 2, 0, 0.0), 1001);
        let report = |lots: i64, price: f64, unary: bool| {
            let mut u = update(&key, "b1", ExecStatus::New, lots, 0, 0.0);
            (u.price, u.unary) = (price, unary);
            u
        };
        // A resize at the same price.
        let fx = os.target(id, Leg::Buy, 300.0, Some(9000.0));
        assert!(
            matches!(&fx.actions[..], [Action::Replace { lots: 3, .. }]),
            "{fx:?}"
        );
        os.apply(&report(2, 300.0, false), 1002);
        let o = os.get(id).unwrap();
        assert!(o.buy.replacing);
        assert_eq!(o.record().buy.quantity, 30.0);
        os.apply(&report(3, 300.0, true), 1003);
        assert!(!os.get(id).unwrap().buy.replacing);
        // A1 to 299, settled by the stream first; A2 to 298 goes out.
        let a1 = os.target(id, Leg::Buy, 299.0, None).actions[0].clone();
        os.apply(&report(3, 299.0, false), 1004);
        let fx = os.target(id, Leg::Buy, 298.0, None);
        assert!(matches!(&fx.actions[..], [Action::Replace { price, .. }] if *price == 298.0));
        // A1's own answer and a refusal of A1 come late: A2 stands.
        os.apply(&report(3, 299.0, true), 1005);
        let refusal = "api 400/-4014: Price not increased by tick size.";
        assert!(os.failed(&a1, true, refusal, 1006).actions.is_empty());
        let o = os.get(id).unwrap();
        assert!(o.buy.replacing);
        assert_eq!(o.record().buy.price, 298.0);
        os.apply(&report(3, 298.0, true), 1007);
        assert!(!os.get(id).unwrap().buy.replacing);
    }

    #[test]
    fn cancelled_partial_exit_reposts_only_remaining_and_finishes_cumulatively() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let buy = post_key(&fx);
        let fx = os.apply(&update(&buy, "b", ExecStatus::Filled, 2, 2, 300.0), 1001);
        let old = post_key(&fx);
        os.apply(
            &update(&old, "s1", ExecStatus::Cancelled, 2, 1, 306.0),
            1002,
        );
        let fx = os.target(id, Leg::Sell, 304.0, None);
        assert!(matches!(fx.actions[0], Action::Post { lots: 1, .. }));
        let current = post_key(&fx);
        // A duplicate entry fill cannot create a second exit.
        assert!(os
            .apply(&update(&buy, "b", ExecStatus::Filled, 2, 2, 300.0), 1003)
            .actions
            .is_empty());
        os.apply(
            &update(&current, "s2", ExecStatus::Filled, 1, 1, 304.0),
            1004,
        );
        let r = os.get(id).unwrap().record();
        assert_eq!(
            (r.status, r.sell.filled, r.sell.mean_price),
            (status::SELL_DONE, 20.0, 305.0)
        );
        assert!(os
            .apply(
                &update(&old, "s1", ExecStatus::Cancelled, 2, 1, 306.0),
                1005
            )
            .actions
            .is_empty());
    }

    /// Amends go one at a time: a move asked while one is out waits for its
    /// answer, and a cancel asked meanwhile wins over the move still waiting.
    #[test]
    fn amends_are_serialized_and_cancellation_wins_over_deferred_target() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let key = post_key(&fx);
        os.apply(&update(&key, "b1", ExecStatus::New, 1, 0, 0.0), 1001);
        let fx = os.target(id, Leg::Buy, 299.0, None);
        assert!(matches!(&fx.actions[..], [Action::Replace { .. }]));
        assert!(os.target(id, Leg::Buy, 298.0, None).actions.is_empty());
        let answer = |price: f64| {
            let mut u = update(&key, "b1", ExecStatus::New, 1, 0, 0.0);
            u.price = price;
            u
        };
        let fx = os.apply(&answer(299.0), 1002);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { exchange_id, price, .. }]
                if exchange_id == "b1" && *price == 298.0),
            "{fx:?}"
        );
        assert!(os.target(id, Leg::Buy, 297.0, None).actions.is_empty());
        assert!(os.cancel(id, Leg::Buy).actions.is_empty());
        let fx = os.apply(&answer(298.0), 1004);
        assert!(
            matches!(&fx.actions[..], [Action::Cancel { exchange_id, .. }] if exchange_id == "b1"),
            "{fx:?}"
        );
    }

    #[test]
    fn timeout_before_fill_is_reconciled_without_a_new_post() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let action = fx.actions[0].clone();
        let key = post_key(&fx);
        let fx = os.failed(&action, false, "transport timeout", 1001);
        assert!(matches!(&fx.actions[0], Action::QueryRequest { key: k, .. } if k == &key));
        assert_eq!(os.get(id).unwrap().status, status::BUY_SET);
        assert!(!os.get(id).unwrap().record().buy.canceled);
        assert!(os.target(id, Leg::Buy, 299.0, None).actions.is_empty());
        let fx = os.watch(&m, 12_000);
        assert!(fx
            .actions
            .iter()
            .all(|a| matches!(a, Action::QueryRequest { .. })));
        os.apply(&update(&key, "b", ExecStatus::Filled, 1, 1, 300.0), 12_001);
        assert_eq!(os.get(id).unwrap().status, status::BUY_DONE);
        assert!(os.watch(&m, 25_000).actions.is_empty());
    }

    /// An amend of unknown fate on a partly filled entry: the order is read,
    /// its fills stay counted, and a cancel asked meanwhile goes once the read
    /// has settled the amend.
    #[test]
    fn timeout_during_amend_keeps_the_fills_until_the_read_settles() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let key = post_key(&fx);
        os.apply(
            &update(&key, "b1", ExecStatus::PartiallyFilled, 2, 1, 300.0),
            1001,
        );
        // The whole order: its filled lot and the one still to buy.
        let fx = os.target(id, Leg::Buy, 299.0, None);
        let action = fx.actions[0].clone();
        assert!(
            matches!(&action, Action::Replace { lots: 2, .. }),
            "{action:?}"
        );
        let fx = os.failed(&action, false, "transport timeout", 1002);
        assert!(
            matches!(&fx.actions[..], [Action::QueryRequest { key: k, .. }] if *k == key),
            "{fx:?}"
        );
        assert_eq!(os.get(id).unwrap().status, status::BUY_SET);
        assert!(os.cancel(id, Leg::Buy).actions.is_empty());
        let mut read = update(&key, "b1", ExecStatus::PartiallyFilled, 2, 1, 300.0);
        read.price = 300.0;
        let fx = os.apply(&read, 1003);
        assert!(
            matches!(&fx.actions[..], [Action::Cancel { exchange_id, .. }] if exchange_id == "b1"),
            "{fx:?}"
        );
        os.apply(
            &update(&key, "b1", ExecStatus::Cancelled, 2, 1, 300.0),
            1005,
        );
        let r = os.get(id).unwrap().record();
        assert_eq!(
            (r.status, r.buy.filled, r.buy.mean_price),
            (status::BUY_DONE, 10.0, 300.0)
        );
    }

    #[test]
    fn reconcile_counts_residual_position_after_a_partial_exit() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b", ExecStatus::Filled, 2, 2, 300.0),
            1001,
        );
        os.apply(
            &update(&post_key(&fx), "s", ExecStatus::Cancelled, 2, 1, 306.0),
            1002,
        );
        let held = [("u-sber".to_string(), 10.0)].into_iter().collect();
        assert!(os.reconcile(&held, &m, 30_000).changed.is_empty());
        assert_eq!(os.get(id).unwrap().status, status::BUY_DONE);
        // The rest leaves the account: closed outside, a manual sell.
        assert!(os.reconcile(&HashMap::new(), &m, 31_000).changed.is_empty());
        assert_eq!(os.get(id).unwrap().status, status::BUY_DONE);
        os.reconcile(&HashMap::new(), &m, 41_000);
        let rec = os.get(id).unwrap().record();
        assert_eq!(
            (rec.status, rec.sell_reason),
            (status::SELL_DONE, reason::MANUAL_SELL)
        );
    }

    /// MAGE 22.09 (TInvestCore): two entries filled 130 ms apart and both
    /// placed their exits at once; a positions read taken before the second
    /// fill must not close either.
    #[test]
    fn reconcile_waits_after_a_fill_even_with_the_exit_placed() {
        let m = sber_model();
        let mut os = Orders::new();
        let mut ids = Vec::new();
        for t in [1000, 1010] {
            let fx = os.start(0, &start(3000.0, 300.0, 306.0), m.get("u-sber").unwrap(), t);
            ids.push(fx.changed[0]);
            let fx = os.apply(
                &update(
                    &post_key(&fx),
                    &format!("b{t}"),
                    ExecStatus::Filled,
                    1,
                    1,
                    300.0,
                ),
                t + 1,
            );
            let mut exit = update(
                &post_key(&fx),
                &format!("0b7e1a2c-1111-2222-3333-44445555{t}"),
                ExecStatus::New,
                1,
                0,
                0.0,
            );
            exit.sell = true;
            os.apply(&exit, t + 2);
        }
        assert_eq!(os.get(ids[0]).unwrap().status, status::SELL_SET);
        let held = [("u-sber".to_string(), 10.0)].into_iter().collect();
        assert!(os.reconcile(&held, &m, 1150).changed.is_empty());
        assert_eq!(os.get(ids[0]).unwrap().status, status::SELL_SET);
        // A second read once the fills are settled is only their FIRST sighting as missing:
        // had the grace filter been dropped, the first read at 1150 would count and this
        // one would close them.
        assert!(os.reconcile(&held, &m, 11_200).changed.is_empty());
        assert_eq!(os.get(ids[0]).unwrap().status, status::SELL_SET);
    }

    #[test]
    fn uncertain_exit_is_not_reposted_before_its_fill_arrives() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b", ExecStatus::Filled, 1, 1, 300.0),
            1001,
        );
        let exit = fx.actions[0].clone();
        let key = post_key(&fx);
        os.failed(&exit, false, "timeout", 1002);
        assert_eq!(os.get(id).unwrap().status, status::SELL_SET);
        assert!(os.target(id, Leg::Sell, 304.0, None).actions.is_empty());
        assert!(os.retry_exits("u-sber").actions.is_empty());
        let fx = os.apply(&update(&key, "s", ExecStatus::Filled, 1, 1, 305.0), 1003);
        assert!(fx.actions.is_empty());
        assert_eq!(os.get(id).unwrap().status, status::SELL_DONE);
        assert_eq!(
            os.failed(&exit, true, "late REST error", 1004),
            Effects::default()
        );
    }

    /// A broker number from the stream proves the order: a later 4xx on the
    /// Post does not close the entry, and misses by request do not either.
    #[test]
    fn entry_with_a_broker_number_survives_a_post_refusal() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let post = fx.actions[0].clone();
        let key = post_key(&fx);
        let mut new = update(&key, "555", ExecStatus::New, 1, 0, 0.0);
        new.unary = false;
        os.apply(&new, 1001);
        os.failed(&post, true, "api 400/-2019: Margin is insufficient.", 1002);
        assert_eq!(os.get(id).unwrap().status, status::BUY_SET);
        let query = Action::QueryRequest {
            order: id,
            leg: Leg::Buy,
            key,
        };
        for i in 0..6 {
            os.failed(
                &query,
                true,
                "api 400/-2013: Order does not exist.",
                2000 + i * 20_000,
            );
        }
        assert_eq!(os.get(id).unwrap().status, status::BUY_SET);
    }

    /// The same ghost in memory: BuySet, a stream NEW, the request unknown to
    /// REST. It gives up once a miss comes `REQUEST_GIVE_UP_MS` after the
    /// first — past the nonce window — instead of asking every 10 s forever.
    #[test]
    fn restored_new_entry_unknown_by_request_gives_up() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let key = post_key(&fx);
        let alias = "0b6c1f2e-6d7a-4c1e-9f3b-2a4d5e6f7a8b";
        let mut new = update(&key, alias, ExecStatus::New, 1, 0, 0.0);
        new.unary = false;
        os.apply(&new, 1001);
        let query = Action::QueryRequest {
            order: id,
            leg: Leg::Buy,
            key: key.clone(),
        };
        // The asks `watch` makes: 0, 10, 30 and 70 s after the first.
        for at in [2_000, 12_000, 32_000, 72_000] {
            assert_eq!(os.get(id).unwrap().status, status::BUY_SET);
            os.failed(&query, true, "api 400/-2013: Order does not exist.", at);
        }
        assert_eq!(os.get(id).unwrap().status, status::BUY_FAIL);
    }

    /// A real 21.09 ghost (NNSB) as the old core saved it: uncertain, a stream
    /// NEW under two UUID aliases, no broker number. Restored, it is asked
    /// with backoff until a miss lands past the nonce window, and fails.
    #[test]
    fn phantom_restored_from_orders_json_gives_up() {
        let saved: Vec<CoreOrder> =
            serde_json::from_str(include_str!("../tests/fixtures/phantom_30042_entry.json"))
                .unwrap();
        let id = saved[0].id;
        let m = sber_model();
        let mut os = Orders::new();
        assert_eq!(os.restore(saved), 1);
        let mut asked = 0;
        for t in 0..80 {
            let now = 1_000 + t * 5_000;
            for a in os.watch(&m, now).actions {
                if matches!(a, Action::QueryRequest { .. }) {
                    asked += 1;
                    os.failed(&a, true, "api 400/-2013: Order does not exist.", now);
                }
            }
        }
        assert_eq!(asked, 4);
        assert_eq!(os.get(id).unwrap().status, status::BUY_FAIL);
    }

    /// An exit restored with only its stream alias and no execution ledger
    /// (`ensure_execution`: status unknown) keeps holding on `50005`: a wrong
    /// give-up would post a second exit.
    #[test]
    fn alias_only_exit_holds_on_request_misses() {
        let model = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            model.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b1", ExecStatus::Filled, 2, 2, 300.0),
            1001,
        );
        post_key(&fx);
        let mut saved = os.persisted();
        let sell = &mut saved[0]["sell"];
        sell["executions"] = serde_json::json!([]);
        sell["exchange_id"] = "0b6c1f2e-6d7a-4c1e-9f3b-2a4d5e6f7a8b".into();
        sell["uncertain"] = true.into();
        sell["resolve_key"] = sell["key"].clone();
        let saved: Vec<CoreOrder> = serde_json::from_value(saved.into()).unwrap();
        let before = saved[0].status;
        let mut back = Orders::new();
        assert_eq!(back.restore(saved), 1);
        let mut asked = 0;
        for t in 0..80 {
            let now = 2_000 + t * 5_000;
            for a in back.watch(&model, now).actions {
                assert!(!matches!(a, Action::Post { .. }), "{a:?}");
                if matches!(a, Action::QueryRequest { .. }) {
                    asked += 1;
                    let fx = back.failed(&a, true, "api 400/-2013: Order does not exist.", now);
                    assert!(fx.actions.iter().all(|a| !matches!(a, Action::Post { .. })));
                }
            }
        }
        // An entry gives up on its fourth ask; this exit is past that and still asking.
        assert!(asked > 6, "keeps asking: {asked}");
        assert_eq!(back.get(id).unwrap().status, before);
    }

    /// The entry's last fill reported after its end: the exit already out is
    /// amended to the grown position — no second exit.
    #[test]
    fn late_entry_fill_expands_existing_exit_without_posting_another() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(6000.0, 300.0, 306.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let buy = post_key(&fx);
        os.apply(&update(&buy, "b1", ExecStatus::New, 2, 0, 0.0), 1001);
        let fx = os.apply(
            &update(&buy, "b1", ExecStatus::Cancelled, 2, 1, 300.0),
            1002,
        );
        let exit = post_key(&fx);
        os.apply(&update(&exit, "s1", ExecStatus::New, 1, 0, 0.0), 1003);
        let fx = os.apply(
            &update(&buy, "b1", ExecStatus::Cancelled, 2, 2, 300.0),
            1004,
        );
        assert!(
            matches!(&fx.actions[..], [Action::Replace { exchange_id, lots: 2, .. }] if exchange_id == "s1"),
            "{fx:?}"
        );
        assert_eq!(os.get(id).unwrap().record().buy.filled, 20.0);
    }

    #[test]
    fn foreign_exit_after_partial_cancel_keeps_previous_fills() {
        let model = sber_model();
        let m = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(0, &start(6000.0, 300.0, 306.0), m, 1000);
        let id = fx.changed[0];
        let fx = os.apply(
            &update(&post_key(&fx), "b", ExecStatus::Filled, 2, 2, 300.0),
            1001,
        );
        os.apply(
            &update(&post_key(&fx), "s1", ExecStatus::Cancelled, 2, 1, 306.0),
            1002,
        );
        let mut foreign = update("", "manual-exit", ExecStatus::New, 1, 0, 0.0);
        foreign.sell = true;
        foreign.price = 304.0;
        let fx = os.adopt(&foreign, m, Some(0.0), 1003);
        assert!(fx.actions.is_empty());
        assert_eq!(os.get(id).unwrap().record().sell.filled, 10.0);
        foreign.status = ExecStatus::Filled;
        foreign.lots_executed = 1;
        foreign.avg_price = 304.0;
        os.apply(&foreign, 1004);
        let r = os.get(id).unwrap().record();
        assert_eq!(
            (r.status, r.sell.filled, r.sell.mean_price),
            (status::SELL_DONE, 20.0, 305.0)
        );
    }

    #[test]
    fn a_failed_amend_reads_the_order_by_its_numeric_id() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 0.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let old = post_key(&fx);
        let mut stream = update(&old, "uuid-alias", ExecStatus::New, 1, 0, 0.0);
        stream.unary = false;
        os.apply(&stream, 1001);
        // Known only by the alias: amended by our key.
        let fx = os.target(id, Leg::Buy, 299.0, None);
        let action = fx.actions[0].clone();
        assert!(
            matches!(&action, Action::Replace { exchange_id, key, .. } if exchange_id == "uuid-alias" && *key == old)
        );
        os.apply(&update(&old, "9876543", ExecStatus::New, 1, 0, 0.0), 1002);
        // A refusal reads the order by the number it has meanwhile.
        let fx = os.failed(&action, true, "api 400/-2013: Order does not exist.", 1003);
        assert!(
            matches!(&fx.actions[0], Action::Query { exchange_id, .. } if exchange_id == "9876543")
        );
    }

    /// CBOM 18.09: a stop exit is posted a spread under the book and fills at
    /// the bid; the chart draws the sell line from the image's leg price, so
    /// once filled it must show the fill, not the deep limit.
    #[test]
    fn filled_exit_shows_its_fill_price_in_the_image() {
        let m = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            0,
            &start(3000.0, 300.0, 310.0),
            m.get("u-sber").unwrap(),
            1000,
        );
        let id = fx.changed[0];
        let key = post_key(&fx);
        let fx = os.apply(&update(&key, "111", ExecStatus::Filled, 1, 1, 300.0), 1001);
        let exit = post_key(&fx);
        let mut new = update(&exit, "222", ExecStatus::New, 1, 0, 0.0);
        new.price = 310.0;
        os.apply(&new, 1002);
        let fx = os.target(id, Leg::Sell, 290.0, None);
        let mut replaced = update(&exit, "333", ExecStatus::New, 1, 0, 0.0);
        if let Action::Replace { key, .. } = &fx.actions[0] {
            replaced.request_id = key.clone();
        }
        replaced.price = 290.0;
        os.apply(&replaced, 1003);
        assert_eq!(os.get(id).unwrap().record().sell.price, 290.0);
        // A live exit keeps its line at the limit, even partly filled.
        let mut partial = replaced.clone();
        partial.lots_requested = 2;
        partial.status = ExecStatus::PartiallyFilled;
        partial.lots_executed = 1;
        partial.avg_price = 297.0;
        os.apply(&partial, 1004);
        let rec = os.get(id).unwrap().record();
        assert_eq!((rec.sell.price, rec.sell.mean_price), (290.0, 297.0));
        let mut filled = replaced.clone();
        filled.status = ExecStatus::Filled;
        filled.lots_executed = 1;
        filled.avg_price = 296.5;
        os.apply(&filled, 1004);
        let rec = os.get(id).unwrap().record();
        assert_eq!(rec.status, status::SELL_DONE);
        assert_eq!((rec.sell.price, rec.sell.mean_price), (296.5, 296.5));
    }

    #[test]
    fn partial_entry_stop_cancels_then_closes_actual_fills() {
        for short in [false, true] {
            let mut model = sber_model();
            let mut os = Orders::new();
            let mut req = start(9000.0, 300.0, 303.0);
            req.is_short = short;
            req.strategy_id = 0;
            let fx = os.start(42, &req, model.get("u-sber").unwrap(), 1000);
            let key = post_key(&fx);
            os.set_stops(42, true, false, 2.0, 0.4);
            os.apply(
                &update(&key, "1001", ExecStatus::PartiallyFilled, 3, 1, 300.0),
                1100,
            );
            model.get_mut("u-sber").unwrap().last_price = Some(if short { 310.0 } else { 290.0 });
            let fx = os.watch(&model, 1200);
            assert!(
                matches!(
                    fx.actions.as_slice(),
                    [Action::Cancel {
                        order: 42,
                        leg: Leg::Buy,
                        ..
                    }]
                ),
                "{fx:?}"
            );
            assert!(os.watch(&model, 1300).actions.is_empty());
            // One more lot fills while cancellation is in flight.
            let fx = os.apply(
                &update(&key, "1001", ExecStatus::Cancelled, 3, 2, 300.0),
                1400,
            );
            assert!(
                fx.actions.is_empty(),
                "panic must suppress the planned take profit"
            );
            let fx = os.watch(&model, 4000);
            assert!(
                matches!(fx.actions.as_slice(), [Action::Post { order: 42, leg: Leg::Sell, lots: 2, sell, .. }] if *sell != short),
                "{fx:?}"
            );
            let exit_key = post_key(&fx);
            os.apply(
                &update(&exit_key, "2001", ExecStatus::Filled, 2, 2, 290.0),
                4100,
            );
            assert_eq!(os.get(42).unwrap().status, status::SELL_DONE);
        }
    }

    /// The entry leg's `quantity` is what the order asked for, not what came
    /// of it — even once a cancellation cut it short. The chat's entry line
    /// divides the fill by it to say «(50%)», so this is a contract now and
    /// not an accident of `remaining_lots` (the entry leg is `owned`, and that
    /// branch counts the last execution's remainder whether it is live or not).
    #[test]
    fn an_entry_leg_keeps_the_size_it_asked_for() {
        let model = sber_model();
        let mut os = Orders::new();
        // 9000 USDT at 300 with a lot of ten: three lots, thirty units.
        let fx = os.start(
            42,
            &start(9000.0, 300.0, 303.0),
            model.get("u-sber").unwrap(),
            1000,
        );
        let key = post_key(&fx);
        assert_eq!(os.get(42).unwrap().record().buy.quantity, 30.0);
        os.apply(
            &update(&key, "1001", ExecStatus::PartiallyFilled, 3, 1, 300.0),
            1100,
        );
        assert_eq!(os.get(42).unwrap().record().buy.quantity, 30.0);
        // Cancelled with two of the three filled: twenty units are the
        // position, thirty are still what was asked for.
        os.apply(
            &update(&key, "1001", ExecStatus::Cancelled, 3, 2, 300.0),
            1400,
        );
        let rec = os.get(42).unwrap().record();
        assert_eq!(rec.buy.filled, 20.0);
        assert_eq!(rec.buy.quantity, 30.0);
        // And this is a status the chat's entry note fires on: the exit for
        // the two lots went out in the same breath.
        assert_eq!(os.get(42).unwrap().status, status::SELL_SET);
    }

    #[test]
    fn close_position_keeps_partial_entry_and_exit_in_one_order() {
        for short in [false, true] {
            let model = sber_model();
            let m = model.get("u-sber").unwrap();
            let mut os = Orders::new();
            let mut req = start(9000.0, 300.0, 303.0);
            req.is_short = short;
            let fx = os.start(42, &req, m, 1000);
            let key = post_key(&fx);
            os.apply(
                &update(&key, "1001", ExecStatus::PartiallyFilled, 3, 1, 300.0),
                1100,
            );
            let fx = os.close_position(m, false, None, false, 1200);
            assert!(
                matches!(
                    fx.actions.as_slice(),
                    [Action::Cancel {
                        order: 42,
                        leg: Leg::Buy,
                        ..
                    }]
                ),
                "{fx:?}"
            );
            assert_eq!(os.iter().count(), 1);
            let fx = os.apply(
                &update(&key, "1001", ExecStatus::Cancelled, 3, 1, 300.0),
                1300,
            );
            assert!(fx.actions.is_empty());
            let fx = os.watch(&model, 4000);
            assert!(
                matches!(
                    fx.actions.as_slice(),
                    [Action::Post {
                        order: 42,
                        leg: Leg::Sell,
                        lots: 1,
                        ..
                    }]
                ),
                "{fx:?}"
            );
            os.apply(
                &update(&post_key(&fx), "2001", ExecStatus::Filled, 1, 1, 300.0),
                4100,
            );
            assert!(os
                .apply(
                    &update(&key, "1001", ExecStatus::Cancelled, 3, 1, 300.0),
                    4200
                )
                .actions
                .is_empty());
            assert!(os.watch(&model, 7000).actions.is_empty());
            assert_eq!(os.get(42).unwrap().status, status::SELL_DONE);
        }
    }

    /// A panic is one MARKET exit, not chased while it is live — an emulated
    /// one as well (until 03.10 it was TInvestCore's limit pinned to the
    /// `PERCENT_PRICE` bound, 297 / 303 here, not what the account would get).
    #[test]
    fn panic_goes_at_market_in_the_emulator_as_well() {
        for (short, emulated) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut model = sber_model();
            {
                let m = model.get_mut("u-sber").unwrap();
                m.mark_price = Some(300.0);
                m.multiplier_down = 0.99;
                m.multiplier_up = 1.01;
            }
            let m = model.get("u-sber").unwrap();
            assert_eq!(m.band(), Some((297.0, 303.0)));
            let mut os = Orders::new();
            let mut req = start(3000.0, 300.0, 0.0);
            req.is_short = short;
            let fx = os.start(42, &req, m, 1000);
            if emulated {
                os.set_emulator(42);
            }
            os.apply(
                &update(&post_key(&fx), "1001", ExecStatus::Filled, 1, 1, 300.0),
                1100,
            );
            let fx = os.set_panic(42, true, m, 1200);
            let key = post_key(&fx);
            match fx.actions[0] {
                Action::Post { price: None, .. } => {}
                ref other => panic!("short {short} emulated {emulated}: {other:?}"),
            }
            let mut report = update(&key, "2001", ExecStatus::New, 1, 0, 0.0);
            report.price = if short { 303.0 } else { 297.0 };
            report.is_market = true;
            os.apply(&report, 1300);
            assert!(os.watch(&model, 4000).actions.is_empty());
            assert!(os.watch(&model, 7000).actions.is_empty());
        }
    }

    /// Off-session, a stock perp's MARKET fills only within 5 % of the mark and the rest
    /// EXPIRES (docs.asterdex.com, stock perpetuals). The panic sells the remainder at MARKET
    /// again after `PANIC_CHASE_MS`, and only the remainder, until the position is gone.
    #[test]
    fn a_panic_market_exit_cut_short_by_the_exchange_sells_the_rest_again() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().mark_price = Some(300.0);
        let m = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(42, &start(9000.0, 300.0, 0.0), m, 1000);
        os.apply(
            &update(&post_key(&fx), "1001", ExecStatus::Filled, 3, 3, 300.0),
            1100,
        );
        let fx = os.set_panic(42, true, m, 1200);
        let key = post_key(&fx);
        assert!(matches!(
            fx.actions[0],
            Action::Post {
                lots: 3,
                price: None,
                sell: true,
                ..
            }
        ));
        // One lot inside the cap, the other two expired.
        let mut cut = update(&key, "2001", ExecStatus::Cancelled, 3, 1, 285.0);
        cut.is_market = true;
        cut.sell = true;
        os.apply(&cut, 1300);
        assert_eq!(os.get(42).unwrap().status, status::BUY_DONE);
        let fx = os.watch(&model, 1200 + PANIC_CHASE_MS);
        let again = fx
            .actions
            .iter()
            .find(|a| matches!(a, Action::Post { leg: Leg::Sell, .. }))
            .unwrap_or_else(|| panic!("no second exit: {:?}", fx.actions));
        assert!(
            matches!(
                again,
                Action::Post {
                    lots: 2,
                    price: None,
                    sell: true,
                    ..
                }
            ),
            "{again:?}"
        );
        // The rest fills: the order is done, nothing more is sent.
        let mut rest = update(&post_key(&fx), "2002", ExecStatus::Filled, 2, 2, 284.0);
        rest.is_market = true;
        rest.sell = true;
        os.apply(&rest, 3500);
        assert_eq!(os.get(42).unwrap().status, status::SELL_DONE);
        assert!(os.watch(&model, 9000).actions.is_empty());
    }

    /// A panic MARKET exit that expires with nothing sold (an empty book inside the cap, off
    /// session) is tried again 10 s later, then 20 s — not every 2 s; its NEW report does not
    /// reset that, the same expiry reported twice counts once, and a fill does reset it.
    #[test]
    fn a_panic_market_exit_that_expires_unfilled_waits_longer_each_time() {
        let mut model = sber_model();
        model.get_mut("u-sber").unwrap().mark_price = Some(300.0);
        let m = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(42, &start(9000.0, 300.0, 0.0), m, 1000);
        os.apply(
            &update(&post_key(&fx), "1001", ExecStatus::Filled, 3, 3, 300.0),
            1100,
        );
        let market = |key: &str, ex: &str, st: ExecStatus, done: i64| {
            let mut u = update(key, ex, st, 3, done, 0.0);
            u.is_market = true;
            u.sell = true;
            u
        };
        let mut key = post_key(&os.set_panic(42, true, m, 1200));
        let mut at = 1200;
        for (n, wait) in [(1, 10_000), (2, 20_000)] {
            os.apply(&market(&key, &format!("e{n}"), ExecStatus::New, 0), at + 50);
            let mut gone = market(&key, &format!("e{n}"), ExecStatus::Cancelled, 0);
            let fx = os.apply(&gone, at + 100);
            assert!(
                fx.logs.iter().any(|l| l.contains("expired unfilled")),
                "{:?}",
                fx.logs
            );
            // The same end, by the stream after the reply.
            gone.unary = false;
            os.apply(&gone, at + 150);
            assert!(os
                .watch(&model, at + 100 + PANIC_CHASE_MS)
                .actions
                .is_empty());
            assert!(os.watch(&model, at + 100 + wait - 1).actions.is_empty());
            let fx = os.watch(&model, at + 100 + wait);
            key = post_key(&fx);
            at += 100 + wait;
        }
        // A panic asked for again starts its count afresh.
        assert_eq!(os.get(42).unwrap().panic_fails, 2);
        os.set_panic(42, true, m, at + 10);
        assert_eq!(os.get(42).unwrap().panic_fails, 0);
        // Two of the three lots sell, the last expires: the rest goes again 2 s after the post.
        let mut part = market(&key, "e3", ExecStatus::Cancelled, 2);
        part.avg_price = 290.0;
        os.apply(&part, at + 100);
        assert_eq!(os.get(42).unwrap().panic_fails, 0);
        let fx = os.watch(&model, at + PANIC_CHASE_MS);
        assert!(
            matches!(
                fx.actions[0],
                Action::Post {
                    lots: 1,
                    price: None,
                    ..
                }
            ),
            "{:?}",
            fx.actions
        );
    }

    #[test]
    fn independent_foreign_exits_preserve_each_fill_and_remaining_position() {
        for reverse in [false, true] {
            let model = sber_model();
            let m = model.get("u-sber").unwrap();
            let mut os = Orders::new();
            let fx = os.start(42, &start(9000.0, 300.0, 0.0), m, 1000);
            os.apply(
                &update(&post_key(&fx), "1001", ExecStatus::Filled, 3, 3, 300.0),
                1100,
            );
            let mut a = update("", "2001", ExecStatus::New, 1, 0, 0.0);
            a.sell = true;
            a.price = 305.0;
            let mut b = a.clone();
            b.exchange_id = "2002".into();
            // Identical prices and sizes still do not establish identity.
            os.adopt(&a, m, Some(0.0), 1200);
            os.adopt(&b, m, Some(0.0), 1300);
            let reports = if reverse {
                [&mut b, &mut a]
            } else {
                [&mut a, &mut b]
            };
            for (i, r) in reports.into_iter().enumerate() {
                r.status = ExecStatus::Filled;
                r.lots_executed = 1;
                r.avg_price = 305.0;
                os.apply(r, 1400 + i as i64);
                if i == 0 {
                    assert_eq!(os.get(42).unwrap().status, status::SELL_SET);
                }
            }
            let rec = os.get(42).unwrap().record();
            assert_eq!(rec.sell.filled, 20.0);
            assert_eq!(rec.sell_reason, reason::MANUAL_SELL);
            let fx = os.target(42, Leg::Sell, 311.0, None);
            assert!(
                matches!(fx.actions.as_slice(), [Action::Post { lots: 1, .. }]),
                "{fx:?}"
            );
        }
    }

    fn two_foreign_exits() -> (Catalog, Orders, OrderUpdate, OrderUpdate) {
        let model = sber_model();
        let m = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(42, &start(9000.0, 300.0, 0.0), m, 1000);
        os.apply(
            &update(&post_key(&fx), "1001", ExecStatus::Filled, 3, 3, 300.0),
            1100,
        );
        let mut a = update("", "2001", ExecStatus::New, 1, 0, 0.0);
        a.sell = true;
        a.price = 310.0;
        let mut b = a.clone();
        b.exchange_id = "2002".into();
        b.price = 295.5;
        os.adopt(&a, m, Some(0.0), 1200);
        os.adopt(&b, m, Some(0.0), 1300);
        (model, os, a, b)
    }

    #[test]
    fn foreign_exit_takeover_waits_for_every_cancel_and_counts_racing_fills() {
        for reverse in [false, true] {
            let (_, mut os, mut a, mut b) = two_foreign_exits();
            let fx = os.target(42, Leg::Sell, 299.0, None);
            assert_eq!(fx.actions.len(), 2);
            assert!(fx
                .actions
                .iter()
                .all(|a| matches!(a, Action::Cancel { .. })));
            a.status = ExecStatus::Filled;
            a.lots_executed = 1;
            a.avg_price = 310.0;
            b.status = ExecStatus::Cancelled;
            let reports = if reverse { [&b, &a] } else { [&a, &b] };
            let fx = os.apply(reports[0], 1400);
            assert!(
                fx.actions
                    .iter()
                    .all(|a| matches!(a, Action::Cancel { .. })),
                "{fx:?}"
            );
            assert_eq!(os.get(42).unwrap().status, status::SELL_SET);
            let fx = os.apply(reports[1], 1500);
            assert!(
                matches!(
                    fx.actions.as_slice(),
                    [Action::Post {
                        lots: 2,
                        price: Some(299.0),
                        ..
                    }]
                ),
                "{fx:?}"
            );
            os.apply(
                &update(&post_key(&fx), "3001", ExecStatus::New, 2, 0, 0.0),
                1600,
            );
            // Duplicates of both retired reports cannot repost or double fills.
            assert!(os.apply(&a, 1700).actions.is_empty());
            assert!(os.apply(&b, 1800).actions.is_empty());
            assert_eq!(os.get(42).unwrap().record().sell.filled, 10.0);
        }
    }

    #[test]
    fn explicit_foreign_exit_cancel_clears_deferred_takeover() {
        let (_, mut os, mut a, mut b) = two_foreign_exits();
        assert_eq!(os.target(42, Leg::Sell, 299.0, None).actions.len(), 2);
        // The Cancels are already queued; the explicit request only drops the takeover.
        assert!(os.cancel(42, Leg::Sell).actions.is_empty());
        assert!(os.map[&42].sell.deferred.is_none());
        a.status = ExecStatus::Cancelled;
        b.status = ExecStatus::Cancelled;
        os.apply(&a, 1400);
        assert!(os.apply(&b, 1500).actions.is_empty());
        assert_eq!(os.get(42).unwrap().status, status::BUY_DONE);
    }

    #[test]
    fn foreign_exit_reconciliation_queries_every_missing_id() {
        let (_, os, _, _) = two_foreign_exits();
        let actions = os.missing_from(&["2002"]);
        assert!(
            matches!(actions.as_slice(), [Action::Query { order: 42, leg: Leg::Sell, exchange_id }] if exchange_id == "2001")
        );
        assert_eq!(os.missing_from(&[]).len(), 2);
    }

    #[test]
    fn the_net_leaves_alone_a_leg_whose_cancel_is_already_in_flight() {
        let (_, mut os, mut a, mut b) = two_foreign_exits();
        // Both ids are missing from the snapshot while nobody asks: queried.
        assert_eq!(os.missing_from(&[]).len(), 2);
        // The takeover sends a Cancel for each; the exchange drops the order as
        // it takes the cancel, so the next snapshot will not carry them — and
        // the Cancel itself reads the final state. Asking again is one wasted
        // `GetOrderState` per entry, which is what spent the ceiling on 01.10.
        assert_eq!(os.target(42, Leg::Sell, 299.0, None).actions.len(), 2);
        assert!(os.missing_from(&[]).is_empty());
        // Outcome known: the net owns the leg again.
        a.status = ExecStatus::Cancelled;
        b.status = ExecStatus::Cancelled;
        os.apply(&a, 1400);
        os.apply(&b, 1500);
        assert!(os.missing_from(&[]).is_empty(), "the leg is no longer live");
    }

    #[test]
    fn failed_foreign_cancel_queries_its_own_execution_only_while_live() {
        let (_, mut os, mut a, _) = two_foreign_exits();
        let cancel = Action::Cancel {
            order: 42,
            leg: Leg::Sell,
            exchange_id: a.exchange_id.clone(),
        };
        os.cancel(42, Leg::Sell);
        let fx = os.failed(&cancel, true, "HTTP 400 30010", 1400);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Query { exchange_id, .. }] if exchange_id == &a.exchange_id)
        );
        a.status = ExecStatus::Cancelled;
        os.apply(&a, 1500);
        assert!(os
            .failed(&cancel, true, "30059 already cancelled", 1600)
            .actions
            .is_empty());
    }

    /// 01.10's shape: `cancel_order` went through and only the trailing state
    /// read was refused — `HTTP 429` with no code at all. The corrective Query
    /// is what then finds the cancelled order and closes the leg.
    ///
    /// The classification is taken from `trading::definitive`, not written in
    /// by hand: the whole chain is the point. Narrowing that function so a rate
    /// limit reads as «not the broker's verdict» silently drops this Query, and
    /// a hand-written `true` here would keep passing through exactly that.
    #[test]
    fn a_throttled_cancel_still_gets_the_read_that_finds_the_order() {
        let (_, mut os, a, _) = two_foreign_exits();
        let cancel = Action::Cancel {
            order: 42,
            leg: Leg::Sell,
            exchange_id: a.exchange_id.clone(),
        };
        os.cancel(42, Leg::Sell);
        let throttled = crate::aster::rest::Error::Api {
            status: 429,
            code: -1003,
            msg: "Too many requests".into(),
        };
        let fx = os.failed(
            &cancel,
            crate::trading::definitive(&throttled),
            &throttled.to_string(),
            1400,
        );
        assert!(
            matches!(fx.actions.as_slice(), [Action::Query { exchange_id, .. }] if exchange_id == &a.exchange_id),
            "{fx:?}"
        );
        // And the leg is free for a fresh Cancel: nothing latched it shut.
        assert!(!os.map[&42].sell.cancel_sent);
    }

    #[test]
    fn panic_takes_over_all_foreign_exits_even_if_one_is_marketable() {
        let (model, mut os, _, _) = two_foreign_exits();
        let fx = os.set_panic(42, true, model.get("u-sber").unwrap(), 1400);
        assert_eq!(fx.actions.len(), 2);
        assert!(fx
            .actions
            .iter()
            .all(|a| matches!(a, Action::Cancel { .. })));
    }

    /// A stop fires on a partly filled entry while its amend is out: the
    /// panic waits for the amend's answer, then cancels the entry and sells
    /// every lot it bought.
    #[test]
    fn partial_entry_stop_waits_for_pending_amend_then_cancels() {
        let mut model = sber_model();
        let mut os = Orders::new();
        let fx = os.start(
            42,
            &start(9000.0, 300.0, 303.0),
            model.get("u-sber").unwrap(),
            1000,
        );
        let key = post_key(&fx);
        os.apply(&update(&key, "1001", ExecStatus::New, 3, 0, 0.0), 1100);
        os.set_stops(42, true, false, 2.0, 0.0);
        let fx = os.target(42, Leg::Buy, 299.0, None);
        assert!(
            matches!(&fx.actions[..], [Action::Replace { exchange_id, lots: 3, .. }] if exchange_id == "1001"),
            "{fx:?}"
        );
        // A fill on the stream, from before the amend landed.
        let mut partial = update(&key, "1001", ExecStatus::PartiallyFilled, 3, 1, 299.0);
        (partial.unary, partial.price) = (false, 300.0);
        os.apply(&partial, 1200);
        assert_eq!(os.get(42).unwrap().record().stop, Some((293.02, 0.0)));
        model.get_mut("u-sber").unwrap().last_price = Some(290.0);
        assert!(os.watch(&model, 1300).actions.is_empty());
        assert!(os.get(42).unwrap().record().panic);
        let mut amended = update(&key, "1001", ExecStatus::PartiallyFilled, 3, 1, 299.0);
        amended.price = 299.0;
        let fx = os.apply(&amended, 1400);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Cancel { exchange_id, .. }] if exchange_id == "1001"),
            "{fx:?}"
        );
        os.apply(
            &update(&key, "1001", ExecStatus::Cancelled, 3, 2, 299.0),
            1500,
        );
        let fx = os.watch(&model, 1501);
        assert!(
            matches!(
                fx.actions.as_slice(),
                [Action::Post {
                    lots: 2,
                    leg: Leg::Sell,
                    ..
                }]
            ),
            "{fx:?}"
        );
    }

    #[test]
    fn foreign_exit_does_not_reactivate_retired_core_generations() {
        let model = sber_model();
        let m = model.get("u-sber").unwrap();
        let mut os = Orders::new();
        let fx = os.start(42, &start(9000.0, 300.0, 306.0), m, 1000);
        let fx = os.apply(
            &update(&post_key(&fx), "1001", ExecStatus::Filled, 3, 3, 300.0),
            1100,
        );
        let original = post_key(&fx);
        os.apply(&update(&original, "2001", ExecStatus::New, 3, 0, 0.0), 1200);
        // The exit is taken off and placed again: a second generation.
        let fx = os.cancel(42, Leg::Sell);
        assert!(matches!(&fx.actions[..], [Action::Cancel { .. }]));
        let mut done = update(&original, "2001", ExecStatus::Cancelled, 3, 0, 306.0);
        done.unary = true;
        os.apply(&done, 1250);
        let fx = os.target(42, Leg::Sell, 305.0, None);
        let replacement = &post_key(&fx);
        // The new exit is cancelled outside the core.
        os.apply(
            &update(replacement, "2002", ExecStatus::Cancelled, 3, 0, 0.0),
            1300,
        );
        let mut foreign = update("", "3001", ExecStatus::New, 1, 0, 0.0);
        foreign.sell = true;
        foreign.price = 304.0;
        os.adopt(&foreign, m, Some(0.0), 1400);
        // A late partial fill counts, but must not revive 2001 as a live exit.
        os.apply(
            &update(&original, "2001", ExecStatus::PartiallyFilled, 3, 1, 306.0),
            1500,
        );
        assert_eq!(os.missing_from(&[]).len(), 1);
        foreign.status = ExecStatus::Filled;
        foreign.lots_executed = 1;
        foreign.avg_price = 304.0;
        os.apply(&foreign, 1600);
        assert_eq!(os.get(42).unwrap().status, status::BUY_DONE);
        let fx = os.target(42, Leg::Sell, 303.0, None);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Post { lots: 1, .. }]),
            "{fx:?}"
        );
    }

    /// UNAC 18.09: a restart killed the core right after it sent Replaces;
    /// their keys never reached the exchange and were asked every 10 s with
    /// `50005` forever, spending the token's error budget (`80006` on the
    /// cancels at the session end) and holding a filled entry without exit.
    /// A request still not found `REQUEST_GIVE_UP_MS` after the first miss never arrived: the leg
    /// falls back to the generation before it and reads its real state.
    #[test]
    fn a_request_lost_in_a_restart_is_given_up() {
        let saved: Vec<CoreOrder> =
            serde_json::from_str(include_str!("../tests/fixtures/unac_lost_replace.json")).unwrap();
        let m = sber_model();
        let mut os = Orders::new();
        assert_eq!(os.restore(saved), 2);
        let (filled, cancelled) = (1_789_754_294_002, 1_789_754_294_004);
        let lost = [
            "07e842a1-01e7-470a-9075-9bf124bc4579",
            "9bcd66c5-c143-415f-9198-0d9d8a417b00",
        ];
        let mut asked: Vec<(i64, String)> = Vec::new();
        for t in 0..80 {
            let now = 1_000 + t * 5_000;
            for a in os.watch(&m, now).actions {
                if let Action::QueryRequest { key, .. } = &a {
                    asked.push((now, key.clone()));
                    if lost.contains(&key.as_str()) {
                        os.failed(&a, true, "api 400/-2013: Order does not exist.", now);
                    }
                }
            }
        }
        for key in lost {
            let times: Vec<i64> = asked.iter().filter(|a| a.1 == key).map(|a| a.0).collect();
            assert_eq!(times.len(), 4, "{times:?}");
            assert!(
                times[times.len() - 1] - times[0] >= REQUEST_GIVE_UP_MS,
                "past the nonce window: {times:?}"
            );
        }
        assert!(os.request_misses.is_empty());
        let asked: Vec<String> = asked.into_iter().map(|a| a.1).collect();
        // The generations before them are asked instead.
        let old_filled = "01baeddf-7b54-4cd0-b480-c29a56a4cb8a";
        let old_cancelled = "34fb4dfa-3828-42de-acab-1b3076c3828e";
        assert!(asked.iter().any(|k| k == old_filled));
        assert!(asked.iter().any(|k| k == old_cancelled));
        let uid = "43666aea-a4df-46e1-a815-e5eccbc1fb3f";
        let report = |key: &str, ex: &str, st, done, avg| OrderUpdate {
            uid: uid.into(),
            ..update(key, ex, st, 2, done, avg)
        };
        os.apply(
            &report(old_filled, "84589350210", ExecStatus::Filled, 2, 0.3765),
            300_000,
        );
        os.apply(
            &report(old_cancelled, "84589350267", ExecStatus::Cancelled, 0, 0.0),
            300_000,
        );
        let o = os.get(filled).unwrap();
        assert!(!o.buy.uncertain);
        assert_eq!(o.buy.filled_lots, 2);
        assert!(!os.get(cancelled).is_some_and(|o| o.buy.uncertain));
        assert!(os
            .watch(&m, 400_000)
            .actions
            .iter()
            .all(|a| !matches!(a, Action::QueryRequest { .. })));
    }

    #[test]
    fn emulated_orders_stay_out_of_the_account() {
        let model = sber_model();
        let sber = model.get("u-sber").unwrap();
        let mut orders = Orders::new();
        let fx = orders.start(0, &start(3000.0, 300.0, 0.0), sber, 1);
        let (id, key) = (fx.changed[0], post_key(&fx));
        orders.set_emulator(id);
        orders.apply(
            &update(&key, "7000000000000000001", ExecStatus::Filled, 1, 1, 300.0),
            10,
        );
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        assert!(orders.get(id).unwrap().record().emulator);
        // The account holds nothing: a real order would be closed outside the core.
        // Twice, a grace apart: the confirmation a real order would get.
        for at in [60_000, 71_000] {
            assert!(orders
                .reconcile(&HashMap::new(), &model, at)
                .changed
                .is_empty());
        }
        assert!(orders.missing_from(&[]).is_empty());
        // ClosePosition closes the orders of the terminal's mode only.
        assert!(orders
            .close_position(sber, false, None, false, 2_000)
            .changed
            .is_empty());
        let fx = orders.close_position(sber, true, None, false, 2_000);
        assert_eq!(fx.changed, [id]);
        assert!(orders.get(id).unwrap().panic);
        // A foreign sale on the account is a real position's, never the emulator's.
        let mut sell = update("app-request", "app-1", ExecStatus::New, 1, 0, 0.0);
        sell.sell = true;
        sell.price = 305.0;
        assert!(!orders
            .adopt(&sell, sber, Some(0.0), 1_000)
            .changed
            .contains(&id));
        assert!(orders.get(id).unwrap().panic);
    }
}
