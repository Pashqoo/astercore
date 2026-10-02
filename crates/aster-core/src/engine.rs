//! MoonProto `Handler`: the Init spine the terminal has to walk before it is
//! `Ready`, and the resync it asks for right after.
//!
//! The spine is not ours to choose. `moonproto`'s own client fails init unless
//! BaseCheck, AuthCheck, `GetMarketsList`, `UpdateMarketsList` and the strategy
//! schema all answer (`client::init::steps`), and it then sends an order
//! snapshot request, a settings request, its strategy list and a balance
//! refresh whose replies are *not* part of the barrier. So this file answers
//! the first group with real data, the balance with the account when the core
//! has a key (`account.rs`), and its orders (`orders.rs`, M2).
//!
//! Adapted from TInvestCore's `engine.rs`, which is the same spine over 6000
//! lines of trading on top. Market data (M1) is here: the tape, the books and
//! the candles the terminal subscribes to, fed from `feed.rs`; so is manual
//! trading (M2), and the strategies with their emulator and trade reports
//! (M3): one pass a second, a tenth of a second while a strategy order is live (`run_shots`),
//! whose commands go through `Orders`
//! as the terminal's do. Every Engine API method outside what is implemented
//! answers with a refusal naming itself, which is how the terminal shows a
//! missing feature instead of waiting out a timeout.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Instant;

use moonproto::server::codec::engine::{self, EngineMethod, EngineRequest, ServerInfo};
use moonproto::server::codec::log::log_msg;
use moonproto::server::codec::market_data::{self, DeepHistoryKind, BOOK_KIND_FUTURES};
use moonproto::server::codec::trade::{InitialStops, OrderCommand, StartOrder};
use moonproto::server::codec::{balance, report, strat, trade, ui, BaseHeader, BASE_HEADER_SIZE};
use moonproto::server::{Command, Handler, Session};
use moonproto::StrategyKind;

use crate::account::Account;
use crate::aster::ws::Stamp;
use crate::autostop;
use crate::book::{LocalBook, Out};
use crate::candles5m::Candles5m;
use crate::emulator::{self, Emulator};
use crate::feed::{FeedCommand, FeedEvent};
use crate::guards;
use crate::load::Load;
use crate::model::{Catalog, QUOTE, QUOTE_CODE};
use crate::moonshot::{self, Cmd, MoonShot, Params, WorkWindow, BTC_SYMBOL};
use crate::order_store::{self, OrderStore};
use crate::orders::{Action, CoreOrder, Effects, Leg, Orders, Resting};
use crate::prices::Snapshot;
use crate::reports::{self, Deal, Profit, Reports};
use crate::strategies::Strategies;
use crate::stream_health::{Scope, StreamHealth, SUMMARY_EVERY_MS};
use crate::tape::Tape;
use crate::trades_stream::TradesStream;
use crate::trading::{Grid, OrderUpdate, TradeCommand, TradingEvent};
use crate::windows::Windows;

#[path = "engine_ops.rs"]
mod ops;
use ops::{DealNote, EntryNote, Ops};

pub const SERVER_NAME: &str = "Astercore";
pub const EXCHANGE_NAME: &str = "Aster";
/// Out of the MoonBot `ExchangeCode` range on purpose, one past TInvestCore's
/// 220: the terminal then names the venue by [`EXCHANGE_NAME`] and draws the
/// core under `reported` without a logo, instead of borrowing a directory
/// entry's rules (`venue(221) == None`, verified in the terminal's source).
pub const EXCHANGE_CODE: u8 = 221;
pub const SERVER_VERSION: i32 = 1;
/// `ExchangeTypeMask` bit for a futures venue.
///
/// **Not SPOT, which is what TInvestCore reports.** With this bit the terminal
/// shows only open positions in Assets and reads `leverage_x` per market
/// (`moon-core/src/feed/types.rs`); with the spot bit a perpetual venue reads
/// as a wallet of coins.
const EXCHANGE_TYPE_FUTURES: u8 = 0x02;
/// Largest payload the core will accept from one client, reported in AuthCheck.
const MAX_PAYLOAD: i32 = 4 * 1024 * 1024;
/// What AuthCheck reports as the account: Aster has no account id to report
/// (`CoreHandler::account_id`).
///
/// Here rather than in `main.rs` because the contract test asserts the same
/// string: it is one fact about what the terminal is told, and two copies of it
/// drift the moment it changes.
pub const ACCOUNT_PLACEHOLDER: &str = "aster";

const API: u8 = Command::API.to_byte();
/// How long after a user-data session opens the open orders are read.
const OPEN_ORDERS_AFTER_MS: i64 = 3_000;
/// The wait before a failed read of the account's open orders is made again.
const OPEN_ORDERS_RETRY_MS: i64 = 10_000;
/// How long a market the exchange refused new positions on (`-4140`/`-4141`) stays closed to
/// the core's own entries.
const CLOSED_MARKET_MS: i64 = 60 * 60_000;
/// How often stops, trailing, pending triggers and panic exits are judged
/// (`Orders::watch`), as TInvestCore did.
const WATCH_EVERY_MS: i64 = 1_000;
const STRAT: u8 = Command::Strat.to_byte();
const UI: u8 = Command::UI.to_byte();
const ORDER: u8 = Command::Order.to_byte();
const BALANCE: u8 = Command::Balance.to_byte();
const LOG: u8 = Command::LogMsg.to_byte();
const TRADES: u8 = Command::TradesStream.to_byte();
const TRADES_RESEND: u8 = Command::TradesResendResponse.to_byte();
const ORDER_BOOK: u8 = Command::OrderBook.to_byte();
/// The Telegram state this core reports: no built-in Telegram reader.
const TELEGRAM_UNSUPPORTED: &str = r#"{"enabled":false,"service_online":false,"state_supported":false,"client_state":"unsupported","setup_error":"Astercore has no built-in Telegram reader"}"#;
/// Feed events applied per UDP-loop pass, so receiving is never starved by a
/// backlog the feed built up.
const PUMP_BATCH: usize = 256;
/// How often stream liveness is judged.
const HEALTH_EVERY_MS: i64 = 1_000;
/// The timer of the strategy pass, `idle` ms after the last one (which took `took_us`): every
/// [`SHOTS_PERIOD_MS`] whatever it cost; sooner only once [`SHOTS_DUTY`] times its duration has
/// passed — at once when `woken` (an order report, a strike-extending trade), and every
/// [`SHOTS_FAST_MS`] while `strategy_order_live` says an order of a strategy is live (a ladder to
/// follow, an exit to move). The order scan runs only when the rest holds.
fn pass_due(
    idle: i64,
    took_us: i64,
    woken: bool,
    strategy_order_live: impl FnOnce() -> bool,
) -> bool {
    if idle >= SHOTS_PERIOD_MS {
        return true;
    }
    idle.saturating_mul(1_000) >= took_us.saturating_mul(SHOTS_DUTY)
        && (woken || (idle >= SHOTS_FAST_MS && strategy_order_live()))
}

/// Whether the periodic orders snapshot is due: [`ORDERS_SNAPSHOT_EVERY_MS`] after the last, and
/// at once when the clock reads earlier than the last one (it stepped back: the snapshot is what
/// heals the terminal's table, it does not wait for the clock to catch up).
fn snapshot_due(now: i64, last: i64) -> bool {
    !(0..ORDERS_SNAPSHOT_EVERY_MS).contains(&(now - last))
}

/// What the strategy pass costs, reported with the `load:` line (`SUMMARY_EVERY_MS`): the number
/// that told how often it may run (MoonBot checks a MoonShot corridor every 16 ms). The timer
/// itself ([`pass_due`]) goes by the last pass alone, not by these figures. Timed on
/// the monotonic clock, as that line is: a stepped wall clock must not print a period that never
/// lasted.
#[derive(Default)]
struct ShotsCost {
    passes: u32,
    total: std::time::Duration,
    max: std::time::Duration,
    since: Option<Instant>,
}

impl ShotsCost {
    /// Adds one pass finished at `at`; the report line when the interval is up, and the count
    /// starts over.
    fn record(&mut self, took: std::time::Duration, at: Instant) -> Option<String> {
        let since = *self.since.get_or_insert(at);
        self.passes += 1;
        self.total += took;
        self.max = self.max.max(took);
        let lasted = at.saturating_duration_since(since);
        if lasted < std::time::Duration::from_millis(SUMMARY_EVERY_MS.unsigned_abs()) {
            return None;
        }
        let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
        let line = format!(
            "shots: {} passes in {} s, avg {:.2} ms, max {:.2} ms",
            self.passes,
            lasted.as_secs(),
            ms(self.total) / f64::from(self.passes),
            ms(self.max)
        );
        *self = Self {
            since: Some(at),
            ..Self::default()
        };
        Some(line)
    }
}
/// The strategy pass runs at least this often (TInvestCore's period) — the idle one: see
/// [`pass_due`] for the fast mode and the wake-ups after an order report or a trade that extends
/// a strike.
const SHOTS_PERIOD_MS: i64 = 1_000;
/// While a strategy order is live the pass runs this often (MoonBot checks a MoonShot corridor
/// every 16 ms; a replace here takes a round trip to the exchange anyway). The samplers of
/// Drops, Strike and Hook keep one sample per time slot, so more passes only refresh it.
const SHOTS_FAST_MS: i64 = 100;
/// A pass is never started sooner than this many times its own duration after the last one, a
/// wake-up included: below the second's period a pass takes at most one twentieth of a core.
const SHOTS_DUTY: i64 = 20;
/// The markets' mean hourly delta (`moonshot::market_delta`) is a sweep of
/// the whole catalog: taken this often, not on every pass.
const MARKET_DELTA_EVERY_MS: i64 = 10_000;
/// The report's profit counters are broadcast at most this often.
const PROFIT_PERIOD_MS: i64 = 60_000;
/// Exchange orders whose commission is kept at most (`note_commission`).
const COMMISSIONS_KEPT: usize = 20_000;
/// Emulator rounds per pass: an answer can bring more work (a fill places
/// the exit), and that work is answered in the same pass.
const EMU_ROUNDS: usize = 8;
/// After an order turns terminal, the whole snapshot is broadcast this much
/// later (the terminal drops a finished order only from a snapshot).
const ORDERS_REFRESH_MS: i64 = 1_000;
/// While a terminal is connected the whole orders snapshot is broadcast this often, even an empty
/// one, whatever happens to the orders. 02.10: 22 cancelled or refused entries stayed on the
/// terminal's table as live for half an hour (the snapshot a second after each terminal image did
/// not clear them; restarting the terminal did). Why they stayed is NOT established; the client
/// library asks about an order a snapshot lacks and drops it on `OrderNotFound` (not when its
/// mirror is terminal but not exact, `state/orders.rs` `apply_gone`), so a periodic snapshot is
/// the cheap try, not a proven cure. The snapshot is one reliable datagram per session and
/// the transport drops one over 256 slices (some 700 retained orders), `session.rs` `send_wire`.
const ORDERS_SNAPSHOT_EVERY_MS: i64 = 10_000;

/// A book the terminal shows, kept from the diff stream (`book.rs`), and the
/// seq of the last packet sent for it: what `RequestOrderBookFull` answers.
#[derive(Default)]
struct Book {
    seq: u16,
    local: LocalBook,
}

impl Book {
    /// The next packet's seq. Zero is skipped: the client reads a last
    /// applied seq of 0 as "no book yet".
    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1).max(1);
        self.seq
    }
}

/// What ties the handler to the market feed. Absent in the contract test that
/// only walks the Init spine, present in the core.
pub struct FeedLink {
    pub tx: Sender<FeedCommand>,
    pub health: StreamHealth,
    pub load: Arc<Load>,
}

pub struct CoreHandler {
    bot_id: i64,
    /// What AuthCheck reports as the account.
    ///
    /// An Aster account has no id of its own — it is the main wallet's address,
    /// which the key file may not even name — so this stays
    /// [`ACCOUNT_PLACEHOLDER`], also on a core that reads the account: a
    /// non-empty string, because an empty one reads in the terminal as "not
    /// authorized", and one the trader can recognise as a placeholder.
    account_id: String,
    catalog: Catalog,
    strategies: Strategies,
    /// Last `TClientSettings` the terminal sent, echoed back on request.
    ///
    /// The terminal is the owner of these settings and the core is their
    /// storage: it asks for them right after init and applies what comes back.
    /// Starting from the library's defaults rather than an empty blob is what
    /// keeps that first answer parseable before any terminal has sent one.
    client_settings: Vec<u8>,
    /// Last shared-config blob, same contract.
    shared_config: Vec<u8>,
    /// How many markets the last book snapshot quoted, so a change is a journal
    /// line and the steady state is not.
    quoted: usize,
    /// `TBalanceFull.epoch`. Fixed at 1, as in TInvestCore: the core sends
    /// full snapshots only, which the client applies whatever their epoch —
    /// the epoch orders INCREMENTAL updates, and there are none.
    balance_epoch: u16,
    /// The last read of the account; `None` on a core without a key, and while
    /// the reads keep failing (`account::STALE_AFTER`) — either way the money
    /// is unknown, and a balance request is answered with the empty snapshot.
    account: Option<Account>,
    feed: Option<FeedLink>,
    /// The core-wide tape packetizer, broadcast to `trade_subs`.
    trades: TradesStream,
    trade_subs: HashSet<u64>,
    /// Books each client shows, and their union as last sent to the feed.
    book_subs: HashMap<u64, BTreeSet<u16>>,
    feed_books: BTreeSet<u16>,
    books: HashMap<u16, Book>,
    /// Whole books owed to a client that just subscribed (client, market),
    /// sent by the next `pump` — after the subscription's reply.
    book_fulls_due: Vec<(u64, u16)>,
    /// Live candles each client follows (one timeframe per market), and their
    /// union as last sent to the feed.
    candle_subs: HashMap<u64, HashMap<u16, DeepHistoryKind>>,
    feed_candles: BTreeSet<(u16, i64)>,
    /// The screener's 5m candles, what `RequestCandlesData` answers.
    candles: Candles5m,
    /// The feed's 5m warm-up has ended (`FeedEvent::WarmupDone`); true
    /// without a feed, where there is nothing to wait for.
    warmup_done: bool,
    /// `RequestCandlesData` uids held until the warm-up ends (client → uid).
    candles_pending: HashMap<u64, u64>,
    health_at: i64,
    load_at: Stamp,
    feed_lost: bool,
    /// The core's orders (`orders.rs`).
    orders: Orders,
    /// The order worker; `None` on a core without an account, where every
    /// order is refused before it is made.
    trading: Option<Sender<TradeCommand>>,
    order_store: Option<OrderStore>,
    /// Why the previous run's orders could not be restored (`order_store::Saved::lost`), until
    /// the account has been compared with the empty list.
    orders_lost: Option<String>,
    /// Exchange work of the last effects, sent by `pump` after one snapshot
    /// of the whole batch (`flush_actions`), with the market it is on.
    pending_actions: Vec<(Action, String)>,
    /// Messages for every session (order images, log lines), sent by `pump`.
    outbox: Vec<(u8, Vec<u8>)>,
    /// When `Orders::watch` last ran.
    watch_at: i64,
    /// When to read the account's open orders (`TradeCommand::OpenOrders`).
    open_orders_due: Option<i64>,
    /// The toolbar's defaults for manual orders, from `TClientSettings`.
    manual: ui::ManualDefaults,
    /// The core is leaving (`begin_stop`): no entry may be placed.
    stopping: bool,
    /// The terminal asked the core to leave and it agreed (`TShutdownCommand`).
    shutdown_requested: bool,
    /// Markets the exchange takes no new positions on (`-4140`/`-4141` on an
    /// entry), for this run: an entry there is refused by the core.
    closed_markets: HashMap<String, i64>,
    /// The strategy engine, its tick windows and the start of its last pass (monotonic: a stepped
    /// wall clock must not hold the timer); `shots_due` wakes the next one ([`pass_due`]).
    shots: MoonShot,
    windows: Windows,
    shots_began: Instant,
    shots_due: bool,
    shots_cost: ShotsCost,
    /// What the last pass took, µs: the fast period keeps its duty to it ([`SHOTS_DUTY`]).
    shots_took_us: i64,
    /// The markets' mean hourly delta and when it was taken.
    market_delta: (Option<f64>, i64),
    /// When the core started: warm-up bars that end before it seed the
    /// windows, the live tape covers the rest.
    started_at: i64,
    /// The exchange of emulated orders, their work of this pass and the
    /// markets they rest on.
    emulator: Emulator,
    emu_actions: Vec<Action>,
    emu_markets: HashSet<String>,
    /// Trade reports and the profit counters last broadcast.
    reports: Reports,
    profit: Profit,
    profit_at: i64,
    /// When to broadcast the orders snapshot after an order turned terminal.
    orders_refresh_at: i64,
    /// When the orders snapshot last went out to the sessions, after a terminal image or on the
    /// period ([`ORDERS_SNAPSHOT_EVERY_MS`]); a session's own pull does not count.
    orders_snapshot_at: i64,
    /// Strategy loss guards (`TotalLoss`, Sessions), the rules each strategy
    /// counts under, and the `PenaltyTime` marks of the report.
    guards: guards::Guards,
    guard_rules: Option<Arc<GuardRules>>,
    #[allow(clippy::type_complexity)]
    penalty_marks: Option<(HashMap<(u64, String), i64>, HashMap<(String, bool), i64>)>,
    /// The terminal's auto-start rules, the start of the loss counter (Unix
    /// s), and the state of the market panic and the circuit breakers.
    auto_stop: autostop::Rules,
    loss_since: i64,
    market_stopped: bool,
    market_latched: bool,
    circuit_stopped: Option<(&'static str, Option<i64>)>,
    /// API errors of the last minute (the error circuit breaker).
    api_errors: VecDeque<i64>,
    /// The terminal's global black list: permanent symbols and temporary ones
    /// with their end (ms).
    black_list: (HashSet<String>, Vec<(String, i64)>),
    /// When the terminal sent the current `client_settings` (ms).
    settings_at: i64,
    /// Commission per exchange order (symbol, order id — ids are the
    /// symbol's), USDT, summed from the user stream's trade events (`n`), for
    /// the report; with the trade ids already summed, so an event heard twice
    /// is counted once.
    commissions: HashMap<(String, String), (f64, HashSet<i64>)>,
    /// The operator's side: settings, the chat, the deals on their way to it
    /// (`engine_ops.rs`).
    ops: Ops,
    /// The prints the core heard itself, for the deals' pictures.
    tape: Tape,
}

/// Per strategy: trades in the emulator, and its Sessions rule.
type GuardRules = HashMap<u64, (bool, Option<guards::SessionRule>)>;

impl CoreHandler {
    pub fn new(bot_id: i64, account_id: String, catalog: Catalog, strategies: Strategies) -> Self {
        // Counted off the catalog rather than started at zero: the startup read
        // has already quoted it (`main.rs`), and a zero here would make the
        // first refresh report a change that did not happen.
        let quoted = catalog.prices().iter().filter(|r| r.bid > 0.0).count();
        Self {
            bot_id,
            account_id,
            catalog,
            strategies,
            client_settings: ui::default_client_settings(0),
            shared_config: ui::default_shared_config_blob(),
            quoted,
            balance_epoch: 1,
            account: None,
            feed: None,
            trades: TradesStream::new(now_ms),
            trade_subs: HashSet::new(),
            book_subs: HashMap::new(),
            feed_books: BTreeSet::new(),
            books: HashMap::new(),
            book_fulls_due: Vec::new(),
            candle_subs: HashMap::new(),
            feed_candles: BTreeSet::new(),
            candles: Candles5m::default(),
            warmup_done: true,
            candles_pending: HashMap::new(),
            health_at: 0,
            load_at: Stamp::now(),
            feed_lost: false,
            // Ids from the start time, so a restart never reuses one (the
            // report keys its rows by them).
            orders: Orders::starting_at(now_ms() as u64),
            trading: None,
            order_store: None,
            orders_lost: None,
            pending_actions: Vec::new(),
            outbox: Vec::new(),
            watch_at: 0,
            open_orders_due: None,
            manual: ui::ManualDefaults::default(),
            stopping: false,
            shutdown_requested: false,
            closed_markets: HashMap::new(),
            shots: MoonShot::default(),
            windows: Windows::default(),
            shots_began: Instant::now(),
            shots_cost: ShotsCost::default(),
            shots_took_us: 0,
            shots_due: false,
            market_delta: (None, 0),
            started_at: now_ms(),
            emulator: Emulator::starting_at(now_ms()),
            emu_actions: Vec::new(),
            emu_markets: HashSet::new(),
            reports: Reports::open(None, now_ms()),
            profit: Profit::default(),
            profit_at: 0,
            orders_refresh_at: 0,
            orders_snapshot_at: now_ms(),
            guards: guards::Guards::default(),
            guard_rules: None,
            penalty_marks: None,
            auto_stop: autostop::Rules::default(),
            loss_since: 0,
            market_stopped: false,
            market_latched: false,
            circuit_stopped: None,
            api_errors: VecDeque::new(),
            black_list: (HashSet::new(), Vec::new()),
            settings_at: 0,
            commissions: HashMap::new(),
            ops: Ops::new(crate::settings::DEFAULT_PATH.into()),
            tape: Tape::default(),
        }
    }

    /// Keep trade reports in `reports` (in memory only without it).
    pub fn with_reports(mut self, reports: Reports) -> Self {
        self.profit = reports.profit(now_ms());
        self.reports = reports;
        self.replay_guards();
        self
    }

    /// Start from the account `main` read at startup, so the first terminal
    /// is answered with it rather than with an empty wallet for a period.
    pub fn with_account(mut self, account: Account) -> Self {
        // Orders that could not be restored leave their positions without stops and exits:
        // the account's open positions are the ones nobody manages now.
        if let Some(why) = self.orders_lost.take() {
            let open: Vec<String> = account
                .positions
                .iter()
                .map(|p| format!("{} {}", p.symbol, p.size))
                .collect();
            let text = if open.is_empty() {
                format!("orders not restored ({why}); the account holds no positions")
            } else {
                format!(
                    "orders not restored ({why}); the account HOLDS {} with no stop or exit in the core",
                    open.join(", ")
                )
            };
            if open.is_empty() {
                log::warn!("{text}");
            } else {
                log::error!("{text}");
            }
            self.tg(crate::telegram::Kind::Alarm, format!("⛔ {text}"));
        }
        self.account = Some(account);
        self
    }

    /// Keep the orders and the run state in `store`, resuming what the
    /// previous run left there: its orders, the terminal's settings, the
    /// strategies' running flag and the auto-stop state. A core without an
    /// account keeps them too — its strategies run in the emulator, and a
    /// restart must not forget them.
    pub fn with_orders(mut self, store: OrderStore, saved: order_store::Saved) -> Self {
        self.orders_lost = saved.lost.clone();
        let restored = self.orders.restore(saved.orders);
        self.orders.restore_left(saved.left);
        let (refreshed, missing) = self.orders.respec(|uid| self.catalog.get(uid));
        if restored > 0 {
            log::info!(
                "orders: {restored} restored, {refreshed} on today's catalog, {missing} on a market it lacks"
            );
        }
        let now = now_ms();
        self.shots.restore(&self.orders, now);
        self.emu_markets = self.orders.emu_markets();
        if !saved.client_settings.is_empty() {
            let at = if saved.settings_at > 0 {
                saved.settings_at
            } else {
                now
            };
            self.set_client_settings(saved.client_settings, at);
        }
        self.loss_since = saved.loss_since;
        self.market_stopped = saved.market_stopped;
        // The crash that stopped them already panicked.
        self.market_latched = saved.market_stopped;
        if saved.running {
            self.strategies.set_running(true);
        }
        log::info!(
            "strategies {} after the restart ({} listed)",
            if saved.running { "running" } else { "stopped" },
            self.strategies.list().len()
        );
        // The restored emulator mode decides which deals the sessions count.
        self.replay_guards();
        self.order_store = Some(store);
        self
    }

    /// Trade on the account through the order worker (`trading::start`).
    pub fn with_trading(mut self, tx: Sender<TradeCommand>) -> Self {
        // What the exchange holds now: the restored orders are read against
        // it before any terminal acts on them.
        let _ = tx.send(TradeCommand::OpenOrders);
        self.trading = Some(tx);
        // No fill is seen until the user-data stream opens: real entries wait
        // for it (`FeedEvent::UserStreamOpen`).
        self.shots.set_fills_seen(false);
        // With an account a strategy trades for real unless it says
        // otherwise: the rules cached while there was none called every
        // strategy emulated.
        self.shots.set_emulator(self.emu_mode());
        self.replay_guards();
        self
    }

    /// Connect the handler to the market feed (`feed::start`).
    pub fn with_feed(mut self, feed: FeedLink) -> Self {
        self.feed = Some(feed);
        self.warmup_done = false;
        self
    }

    /// Send to the feed; `false` when there is none or it is gone. A send that
    /// fails on a feed that WAS there means its coordinator thread died — the
    /// receiver drops only with it — and is latched in [`Self::feed_lost`].
    fn feed_send(&mut self, cmd: FeedCommand) -> bool {
        let Some(f) = &self.feed else {
            return false;
        };
        let sent = f.tx.send(cmd).is_ok();
        if !sent && !self.feed_lost {
            log::error!("feed: the coordinator is gone — subscriptions can no longer change");
            self.feed_lost = true;
        }
        sent
    }

    /// The market feed died under the core. Nothing in the process would
    /// notice otherwise: the terminal's subscriptions would be recorded as
    /// sent and never opened, and its chart requests never answered. `main`
    /// leaves on it, as it does on a dead price refresher, and the exit code
    /// is what brings the core back.
    pub fn feed_lost(&self) -> bool {
        self.feed_lost
    }

    /// Act on a book's step: ask the feed for a snapshot, or send the clients
    /// showing it the whole book or the diff, under the book's next seq.
    fn book_out(&mut self, idx: u16, symbol: String, out: Out, sessions: &mut [&mut Session]) {
        let (full, bids, asks) = match out {
            Out::Nothing => return,
            Out::AskSnapshot { gap } => {
                if let Some(f) = &self.feed {
                    f.load.book_snapshot(gap);
                }
                if gap {
                    log::debug!("book {symbol}: the update chain broke, asking a snapshot");
                }
                self.feed_send(FeedCommand::BookSnapshot(symbol));
                return;
            }
            Out::Full => {
                let Some(b) = self.books.get(&idx) else {
                    return;
                };
                let (bids, asks) = b.local.levels();
                (true, bids, asks)
            }
            Out::Diff { bids, asks } => (false, bids, asks),
        };
        let Some(book) = self.books.get_mut(&idx) else {
            return;
        };
        let seq = book.next_seq();
        let shown = |subs: &HashMap<u64, BTreeSet<u16>>, id: u64| {
            subs.get(&id).is_some_and(|set| set.contains(&idx))
        };
        if !sessions
            .iter()
            .any(|s| shown(&self.book_subs, s.client_id()))
        {
            return;
        }
        let packet =
            market_data::order_book_packet(idx, seq, full, BOOK_KIND_FUTURES, &bids, &asks);
        for s in sessions.iter_mut() {
            if shown(&self.book_subs, s.client_id()) {
                s.send(ORDER_BOOK, &packet);
            }
        }
    }

    fn indexes(&self, names: &[String]) -> Vec<u16> {
        names
            .iter()
            .filter_map(|n| self.catalog.index_of_symbol(n))
            .collect()
    }

    fn symbol_of(&self, idx: u16) -> Option<String> {
        self.catalog
            .markets()
            .get(usize::from(idx))
            .map(|m| m.symbol.clone())
    }

    /// Push the union of every client's book and candle subscriptions to the
    /// feed, when it changed. A book that leaves the union leaves the cache
    /// too: its session is about to stop, and a book the core no longer hears
    /// must not answer the next `RequestOrderBookFull` as if it were current.
    fn sync_feed_subscriptions(&mut self) {
        let books: BTreeSet<u16> = self.book_subs.values().flatten().copied().collect();
        if books != self.feed_books {
            for idx in self.feed_books.difference(&books) {
                self.books.remove(idx);
            }
            let symbols = books.iter().filter_map(|&i| self.symbol_of(i)).collect();
            self.feed_send(FeedCommand::SetBooks(symbols));
            self.feed_books = books;
        }
        let candles: BTreeSet<(u16, i64)> = self
            .candle_subs
            .values()
            .flatten()
            .map(|(&i, k)| (i, k.minutes()))
            .collect();
        if candles != self.feed_candles {
            let subs = candles
                .iter()
                .filter_map(|&(i, m)| Some((self.symbol_of(i)?, m)))
                .collect();
            self.feed_send(FeedCommand::SetCandles(subs));
            self.feed_candles = candles;
        }
    }

    /// Apply queued feed events, judge the streams, and flush the tape; called
    /// by the main loop between `Server::step`s with the authorized sessions.
    pub fn pump<'s>(
        &mut self,
        sessions: impl Iterator<Item = &'s mut Session>,
        rx: &Receiver<FeedEvent>,
    ) {
        let mut sessions: Vec<&mut Session> = sessions.collect();
        for (client, idx) in std::mem::take(&mut self.book_fulls_due) {
            // Unsubscribed again before this pump: owed nothing.
            if !self
                .book_subs
                .get(&client)
                .is_some_and(|set| set.contains(&idx))
            {
                continue;
            }
            let Some(b) = self.books.get(&idx).filter(|b| b.local.has_book()) else {
                // Not stitched yet: the stitch sends it to every client showing it.
                continue;
            };
            let Some(s) = sessions.iter_mut().find(|s| s.client_id() == client) else {
                continue;
            };
            let (bids, asks) = b.local.levels();
            let packet =
                market_data::order_book_packet(idx, b.seq, true, BOOK_KIND_FUTURES, &bids, &asks);
            s.send(ORDER_BOOK, &packet);
        }
        let mut applied = 0;
        for ev in rx.try_iter().take(PUMP_BATCH) {
            self.apply_feed(ev, &mut sessions);
            applied += 1;
        }
        if applied == PUMP_BATCH {
            if let Some(f) = &self.feed {
                f.load.batch_full();
            }
        }
        let now = now_ms();
        if now - self.health_at >= HEALTH_EVERY_MS {
            self.health_at = now;
            self.judge_streams();
        }
        if now - self.watch_at >= WATCH_EVERY_MS {
            self.watch_at = now;
            let fx = self.orders.watch(&self.catalog, now);
            self.effects(fx, now);
        }
        // Not while stopping: an entry placed now would be one more to
        // withdraw. `start_order` is the terminal's door, shut by the same flag.
        if !self.stopping && self.shots_pass_due() {
            let began = Instant::now();
            self.run_shots(now);
            let took = began.elapsed();
            self.shots_took_us = i64::try_from(took.as_micros()).unwrap_or(i64::MAX);
            if let Some(line) = self.shots_cost.record(took, Instant::now()) {
                log::info!("{line}");
            }
        }
        if self.open_orders_due.is_some_and(|due| now >= due) {
            self.open_orders_due = None;
            if let Some(tx) = &self.trading {
                let _ = tx.send(TradeCommand::OpenOrders);
            }
        }
        self.flush_actions(now);
        self.run_emulator(now);
        self.persist_orders(false, now);
        if now - self.profit_at >= PROFIT_PERIOD_MS {
            self.push_profit(now);
        }
        let refresh = self.orders_refresh_at != 0 && now >= self.orders_refresh_at;
        // Not built for nobody: a session that connects pulls the snapshot itself.
        if !sessions.is_empty() && (refresh || snapshot_due(now, self.orders_snapshot_at)) {
            self.orders_refresh_at = 0;
            self.orders_snapshot_at = now;
            let snapshot = trade::orders_snapshot(0, &self.orders.records(now));
            self.outbox.push((ORDER, snapshot));
        }
        for (channel, payload) in std::mem::take(&mut self.outbox) {
            for s in sessions.iter_mut() {
                s.send_encrypted(channel, &payload, true);
            }
        }
        if let Some(packet) = self.trades.poll(Instant::now()) {
            for s in sessions
                .iter_mut()
                .filter(|s| self.trade_subs.contains(&s.client_id()))
            {
                s.send(TRADES, &packet);
            }
        }
    }

    /// Stream liveness, and what a dead mark stream takes with it: the mark
    /// prices and the funding are CLEARED, the same rule the price refresher
    /// applies to an outage (`prices.rs`) — a terminal showing no funding is
    /// right, one counting down to a charge the core stopped hearing about is
    /// not. The next frame after the stream returns restores both.
    fn judge_streams(&mut self) {
        let Some(feed) = self.feed.as_mut() else {
            return;
        };
        let stamp = Stamp::now();
        let mut markets: Vec<(Vec<String>, bool)> = Vec::new();
        let mut marks_died = false;
        for (scope, alive) in feed.health.judge(stamp) {
            match scope {
                Scope::Marks if !alive => marks_died = true,
                Scope::Marks => {}
                Scope::Markets(symbols) => markets.push((symbols.clone(), alive)),
            }
        }
        if marks_died {
            self.catalog.apply_premium_index(&[]);
        }
        // A market whose tape is dead has no price to trade on: its orders
        // hold (`Market::fresh`).
        for (symbols, alive) in markets {
            for symbol in &symbols {
                self.catalog.set_feed_fresh(symbol, alive);
            }
        }
        // Monotonic, unlike the streams line: a stepped wall clock must not
        // print a load line for a period that never lasted.
        if stamp.mono - self.load_at.mono >= SUMMARY_EVERY_MS {
            self.load_at = stamp;
            log::info!("{}", feed.load.summary());
        }
    }

    fn apply_feed(&mut self, ev: FeedEvent, sessions: &mut [&mut Session]) {
        match ev {
            FeedEvent::Trade {
                symbol,
                price,
                qty,
                time_ms,
            } => {
                if let Some(idx) = self.catalog.index_of_symbol(&symbol) {
                    self.trades.push(idx, time_ms, price as f32, qty as f32);
                    self.candles.push(idx, time_ms, price, qty);
                    self.catalog.set_last(&symbol, price);
                    self.windows.push(idx, time_ms, price, qty);
                    let turnover = price * qty.abs();
                    let now = now_ms();
                    self.tape.push(idx, now, time_ms, price as f32, qty as f32);
                    if self.shots.on_trade(idx, now, price, turnover, qty > 0.0) {
                        self.shots_due = true;
                    }
                    self.emulate_fills(&symbol, Some(price));
                }
            }
            FeedEvent::Warmup { symbol, bars } => {
                if let Some(idx) = self.catalog.index_of_symbol(&symbol) {
                    for b in &bars {
                        self.candles
                            .seed(idx, b.open_ms, b.low, b.high, b.quote_volume);
                    }
                    self.seed_windows(idx, &bars);
                }
            }
            FeedEvent::WarmupDone => {
                self.warmup_done = true;
                let pending = std::mem::take(&mut self.candles_pending);
                for s in sessions.iter_mut() {
                    if let Some(&uid) = pending.get(&s.client_id()) {
                        self.send_candles_snapshot(s, uid);
                    }
                }
            }
            FeedEvent::Marks(rows) => {
                let funded = self.catalog.apply_premium_index(&rows);
                log::trace!("marks: funding on {funded} markets ({} rows)", rows.len());
            }
            FeedEvent::BookDiff { symbol, diff } => {
                let Some(idx) = self.catalog.index_of_symbol(&symbol) else {
                    return;
                };
                // A late event of a book nobody shows any more: its session
                // is closing, and keeping it would revive what the unsubscribe
                // just dropped.
                if !self.feed_books.contains(&idx) {
                    return;
                }
                let out = self
                    .books
                    .entry(idx)
                    .or_default()
                    .local
                    .on_diff(diff, now_ms());
                self.book_out(idx, symbol, out, sessions);
            }
            FeedEvent::BookSnapshot { symbol, result } => {
                let Some(idx) = self.catalog.index_of_symbol(&symbol) else {
                    return;
                };
                let Some(book) = self.books.get_mut(&idx) else {
                    // Asked for a book that has since been dropped.
                    return;
                };
                let out = book.local.on_snapshot(result, now_ms());
                self.book_out(idx, symbol, out, sessions);
            }
            // The session carrying these books ended on its own (or they left
            // the subscription). A client still showing one is sent an EMPTY
            // whole book: left alone, the terminal would keep drawing the last
            // one as live through the reconnect and its backoff — the same rule
            // as the price rows, where no quote beats an old one.
            FeedEvent::BooksUnavailable(symbols) => {
                let mut off = 0;
                for symbol in &symbols {
                    let Some(idx) = self.catalog.index_of_symbol(symbol) else {
                        continue;
                    };
                    let Some(book) = self.books.remove(&idx) else {
                        continue;
                    };
                    off += 1;
                    let packet = market_data::order_book_packet(
                        idx,
                        book.seq.wrapping_add(1).max(1),
                        true,
                        BOOK_KIND_FUTURES,
                        &[],
                        &[],
                    );
                    for s in sessions.iter_mut() {
                        let shown = self
                            .book_subs
                            .get(&s.client_id())
                            .is_some_and(|set| set.contains(&idx));
                        if shown {
                            s.send(ORDER_BOOK, &packet);
                        }
                    }
                }
                if off > 0 {
                    if let Some(f) = &self.feed {
                        f.load.books_off(off);
                    }
                }
            }
            FeedEvent::Candle {
                symbol,
                minutes,
                candle,
            } => {
                let Some(idx) = self.catalog.index_of_symbol(&symbol) else {
                    return;
                };
                for s in sessions.iter_mut() {
                    let kind = self
                        .candle_subs
                        .get(&s.client_id())
                        .and_then(|subs| subs.get(&idx))
                        .filter(|k| k.minutes() == minutes);
                    if let Some(&kind) = kind {
                        s.send_encrypted(
                            API,
                            &market_data::candle_update(rand_uid(), idx, kind, &candle),
                            false,
                        );
                    }
                }
            }
            FeedEvent::Lost(what) => {
                log::error!("feed: {what} is gone");
                self.feed_lost = true;
            }
            FeedEvent::Trading(ev) => self.on_trading(ev),
            FeedEvent::UserOrder(o) => {
                self.note_commission(&o);
                let step = self.catalog.get(&o.symbol).map_or(0.0, |m| m.step_size);
                match OrderUpdate::from_event(&o, step) {
                    Some(u) => self.on_trading(TradingEvent::Order(u)),
                    None if o.status.starts_with("NEW_") => {}
                    None => log::warn!("order: unreadable report of {} {}", o.symbol, o.id),
                }
            }
            // Read a moment after the session opened (`UserStreamOpen` comes once its
            // handshake is through): a read made before the subscription leaves a window that
            // neither the read nor the stream covers. Retries in between fold into the one
            // read.
            FeedEvent::UserStreamClosed => self.shots.set_fills_seen(false),
            FeedEvent::UserStreamOpen => {
                self.shots.set_fills_seen(true);
                if self.open_orders_due.is_none() {
                    self.open_orders_due = Some(now_ms() + OPEN_ORDERS_AFTER_MS);
                }
            }
            FeedEvent::AccountRead(a) => {
                // The same account again: not worth a terminal packet, but a position that is
                // still missing is now seen twice, and the budget starts from the exchange's
                // figure again.
                let now = now_ms();
                let fx = self.orders.reconcile(&Self::held(&a), &self.catalog, now);
                self.effects(fx, now);
                self.shots.set_free_balance(Some(a.free));
            }
            FeedEvent::Account(account) => {
                // Positions that left the account outside the core close
                // their orders (`Orders::reconcile`). An unknown account
                // closes nothing, and forgets what it had seen missing.
                if let Some(a) = &account {
                    let now = now_ms();
                    let fx = self.orders.reconcile(&Self::held(a), &self.catalog, now);
                    self.effects(fx, now);
                } else {
                    self.orders.forget_gone();
                }
                // A fresh read resets the free-money budget of the entries.
                self.shots
                    .set_free_balance(account.as_ref().map(|a| a.free));
                self.account = account;
                let payload = self.balance_payload(rand_uid());
                for s in sessions.iter_mut() {
                    s.send_encrypted(BALANCE, &payload, true);
                }
            }
            FeedEvent::CandlesReply {
                client_id,
                request_uid,
                result,
            } => {
                // The core's own request: the chat's `/chart` or a deal's
                // picture, never a terminal session.
                if client_id == crate::control::CLIENT_ID {
                    return self.on_control_candles(request_uid, result);
                }
                let Some(s) = sessions.iter_mut().find(|s| s.client_id() == client_id) else {
                    return;
                };
                let method = EngineMethod::GetCoinCardCandles;
                let resp = match result {
                    Ok(candles) => engine::response_ok(
                        request_uid,
                        method,
                        &market_data::coin_card_candles(&candles),
                    ),
                    Err(e) => {
                        log::warn!("CoinCard: {e}");
                        engine::response_err(request_uid, method, 0, &e)
                    }
                };
                s.send_encrypted(API, &resp, true);
            }
            FeedEvent::HistoryReply {
                client_id,
                request_uid,
                result,
            } => {
                if client_id == crate::control::CLIENT_ID {
                    return self.on_control_history(request_uid, result);
                }
                let Some(s) = sessions.iter_mut().find(|s| s.client_id() == client_id) else {
                    return;
                };
                let method = EngineMethod::RequestMarketHistory;
                match result {
                    Ok(trades) => {
                        for chunk in market_data::market_history(&trades) {
                            s.send_encrypted(
                                API,
                                &engine::response_ok(request_uid, method, &chunk),
                                true,
                            );
                        }
                    }
                    Err(e) => {
                        log::warn!("market history: {e}");
                        s.send_encrypted(
                            API,
                            &engine::response_err(request_uid, method, 0, &e),
                            true,
                        );
                    }
                }
            }
        }
    }

    /// `RequestCandlesData`: every market's sealed 5m candles in one chunked
    /// answer. The journal line is the size of what went out — the one number
    /// that says whether a slow first load is ours or the network's.
    fn send_candles_snapshot(&self, session: &mut Session, request_uid: u64) {
        let started = Instant::now();
        let now = now_ms();
        let per_market: Vec<(&str, Vec<market_data::Candle>)> = self
            .catalog
            .markets()
            .iter()
            .enumerate()
            .filter_map(|(i, m)| {
                let candles = self.candles.sealed(i as u16, now);
                (!candles.is_empty()).then_some((m.symbol.as_str(), candles))
            })
            .collect();
        let refs: Vec<(&str, &[market_data::Candle])> =
            per_market.iter().map(|(n, c)| (*n, c.as_slice())).collect();
        let chunks = market_data::candles_snapshot(&refs);
        log::info!(
            "candles: {} markets, {} candles, {} chunks of {} bytes in {} ms to client {:#x}",
            refs.len(),
            refs.iter().map(|(_, c)| c.len()).sum::<usize>(),
            chunks.len(),
            chunks.iter().map(Vec::len).sum::<usize>(),
            started.elapsed().as_millis(),
            session.client_id()
        );
        for chunk in chunks {
            session.send_encrypted(
                API,
                &engine::response_ok(request_uid, EngineMethod::RequestCandlesData, &chunk),
                true,
            );
        }
    }

    /// Apply one snapshot from the top-of-book refresher. Called from the UDP loop
    /// between receives, never from inside a request: the request answers
    /// whatever the last applied snapshot says.
    pub fn apply(&mut self, snap: Snapshot) {
        match snap {
            Snapshot::Book(rows) => {
                let quoted = self.catalog.apply_book(&rows);
                // The book moved: emulated orders it reached fill.
                for symbol in self.emu_markets.clone() {
                    self.emulate_fills(&symbol, None);
                }
                // A market that stops being quoted goes into the price rows as
                // a zero, and the terminal then shows nothing for it — that is
                // a fact about the venue, so it is said out loud when the count
                // changes and kept quiet when it does not.
                if quoted != self.quoted {
                    log::info!(
                        "prices: {quoted} of {} markets quoted ({} rows)",
                        self.catalog.markets().len(),
                        rows.len()
                    );
                    self.quoted = quoted;
                }
            }
        }
    }

    fn server_info(&self) -> Vec<u8> {
        engine::write_server_info(&ServerInfo {
            bot_id: self.bot_id,
            server_name: SERVER_NAME,
            exchange_code: EXCHANGE_CODE,
            exchange_name: EXCHANGE_NAME,
            exchange_type_mask: EXCHANGE_TYPE_FUTURES,
            base_currency_name: QUOTE,
            base_currency_code: QUOTE_CODE,
            server_version: SERVER_VERSION,
            moonproto_version: i32::from(moonproto::server::codec::PROTO_CMD_VER),
        })
    }

    fn on_api(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(req) = EngineRequest::parse(payload) else {
            log::warn!("API: unparsable request ({} bytes)", payload.len());
            return;
        };
        let client = session.client_id();
        let data = match req.method {
            EngineMethod::SubscribeAllTrades => {
                self.trade_subs.insert(client);
                Vec::new()
            }
            EngineMethod::UnsubscribeAllTrades => {
                self.trade_subs.remove(&client);
                Vec::new()
            }
            EngineMethod::TradesResend => {
                if let Some(nums) = market_data::parse_trades_resend_params(&req.params) {
                    session.send(TRADES_RESEND, &self.trades.resend(&nums));
                }
                Vec::new()
            }
            // A book another client already keeps live goes to this one whole,
            // but not from here: the client drops every book packet until the
            // reply to this request confirms the subscription (Delphi
            // `FSubscribedBookServerToken`), and the reply leaves after this
            // returns. `pump` sends it next (`book_fulls_due`).
            EngineMethod::SubscribeOrderBook => {
                let idx = self.indexes(&req.market_names);
                self.book_fulls_due.extend(idx.iter().map(|&i| (client, i)));
                self.book_subs.entry(client).or_default().extend(idx);
                self.sync_feed_subscriptions();
                Vec::new()
            }
            EngineMethod::UnsubscribeOrderBook => {
                let idx = self.indexes(&req.market_names);
                if let Some(set) = self.book_subs.get_mut(&client) {
                    set.retain(|i| !idx.contains(i));
                }
                self.sync_feed_subscriptions();
                Vec::new()
            }
            // Answered from the local book. One not stitched yet gets nothing
            // here, and needs nothing: the stitch sends every client showing
            // it a full book (`book_out`).
            EngineMethod::RequestOrderBookFull | EngineMethod::ReloadOrderBook => {
                let wanted: Vec<u16> = match market_data::parse_order_book_full_params(&req.params)
                {
                    Some((idx, _)) => vec![idx],
                    None => self
                        .book_subs
                        .get(&client)
                        .into_iter()
                        .flatten()
                        .copied()
                        .collect(),
                };
                for idx in wanted {
                    if let Some(b) = self.books.get(&idx).filter(|b| b.local.has_book()) {
                        let (bids, asks) = b.local.levels();
                        let packet = market_data::order_book_packet(
                            idx,
                            b.seq,
                            true,
                            BOOK_KIND_FUTURES,
                            &bids,
                            &asks,
                        );
                        session.send(ORDER_BOOK, &packet);
                    }
                }
                Vec::new()
            }
            EngineMethod::SubscribeCandles => {
                let Some(kind) = market_data::parse_kind_param(&req.params) else {
                    return self.reply_err(session, &req, "bad timeframe");
                };
                let idx = self.indexes(&req.market_names);
                let subs = self.candle_subs.entry(client).or_default();
                for i in &idx {
                    subs.insert(*i, kind);
                }
                for i in idx {
                    session.send_encrypted(
                        API,
                        &market_data::candle_tf_state(rand_uid(), i, Some(kind), 1),
                        true,
                    );
                }
                self.sync_feed_subscriptions();
                Vec::new()
            }
            EngineMethod::UnsubscribeCandles => {
                let idx = self.indexes(&req.market_names);
                if let Some(subs) = self.candle_subs.get_mut(&client) {
                    for i in &idx {
                        subs.remove(i);
                    }
                }
                self.sync_feed_subscriptions();
                Vec::new()
            }
            EngineMethod::GetCoinCardCandles => {
                let (Some(kind), Some(_)) = (
                    market_data::parse_kind_param(&req.params),
                    self.catalog.index_of_symbol(&req.market_name),
                ) else {
                    return self.reply_err(session, &req, "unknown market or timeframe");
                };
                let sent = self.feed_send(FeedCommand::Candles {
                    symbol: req.market_name.clone(),
                    minutes: kind.minutes(),
                    client_id: client,
                    request_uid: req.uid,
                });
                if !sent {
                    self.reply_err(session, &req, "no market data source");
                }
                return;
            }
            EngineMethod::RequestMarketHistory => {
                if self.catalog.index_of_symbol(&req.market_name).is_none() {
                    return self.reply_err(session, &req, "unknown market");
                }
                let sent = self.feed_send(FeedCommand::History {
                    symbol: req.market_name.clone(),
                    client_id: client,
                    request_uid: req.uid,
                });
                if !sent {
                    self.reply_err(session, &req, "no market data source");
                }
                return;
            }
            // Held until the warm-up: the client asks once per connection and
            // keeps what it got, so an early answer would leave the window
            // columns empty until a reconnect. Held, its request times out
            // after 15 s and is asked again; the newest uid is the one kept.
            EngineMethod::RequestCandlesData => {
                if self.warmup_done {
                    self.send_candles_snapshot(session, req.uid);
                } else {
                    self.candles_pending.insert(client, req.uid);
                }
                return;
            }
            EngineMethod::BaseCheck => self.server_info(),
            EngineMethod::AuthCheck => engine::write_auth_check(&self.account_id, MAX_PAYLOAD),
            EngineMethod::GetMarketsList => engine::write_markets_list(&self.catalog.specs()),
            // With funding: the catalog row carried it once per session, and
            // the next-charge time moves every few hours (`Catalog::funded_prices`).
            EngineMethod::UpdateMarketsList => {
                engine::write_markets_prices_funded(&self.catalog.funded_prices())
            }
            EngineMethod::GetMarketsIndexes => {
                engine::write_markets_indexes(&self.catalog.symbols())
            }
            // No token permissions to report: Aster has no such notion, and the
            // empty answer is what the terminal reads as "no tags".
            EngineMethod::CheckBinanceTags => Vec::new(),
            // One-way positions (`positionSide: BOTH`), which is the mode M2's
            // order model is written against; hedge mode is M5+ (`PLAN.md`).
            // (Startup refuses an account in hedge mode, `main.rs`: the answer is true to it.)
            EngineMethod::QueryHedgeMode => engine::write_hedge_mode(false),
            // «Cancel ALL orders»: ok at once with no data; the result reaches the terminal as
            // the order images of the cancelled entries.
            EngineMethod::CancelAllOrders => {
                self.cancel_all_orders(now_ms());
                Vec::new()
            }
            // The EIP-712 API wallet does not expire (`PLAN.md` §10.1).
            EngineMethod::CheckAPIExpirationTime => engine::write_no_api_expiration(),
            // Futures wallet only: no spot/margin wallets to transfer between.
            EngineMethod::UpdateTransferAssets => engine::write_no_transfer_assets(),
            other => {
                // A refusal, not silence. The client waits out a 12 s timeout
                // for a request nobody answers and then fails the whole step;
                // an error reply lands at once and names the method, which is
                // what makes "not implemented yet" visible in the terminal
                // rather than looking like a dead core.
                log::debug!("API: {} not implemented", other.name());
                return self.reply_err(session, &req, "not implemented");
            }
        };
        session.send_encrypted(API, &engine::response_ok(req.uid, req.method, &data), true);
    }

    fn reply_err(&self, session: &mut Session, req: &EngineRequest, msg: &str) {
        session.send_encrypted(
            API,
            &engine::response_err(req.uid, req.method, 0, msg),
            true,
        );
    }

    fn on_ui(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(hdr) = BaseHeader::parse(payload) else {
            return;
        };
        match hdr.cmd_id {
            ui::CMD_CLIENT_SETTINGS => {
                self.set_client_settings(payload.to_vec(), now_ms());
                session.send_encrypted(UI, &ui::with_uid(payload, rand_uid()), true);
            }
            // MoonBot's guarded shutdown: refused while the core holds a
            // position — its exit, or the want of one, needs a core to watch
            // it. Otherwise the core leaves (`main`), withdrawing its entries.
            ui::CMD_SHUTDOWN => {
                let open = self.open_positions();
                let text = if open > 0 {
                    format!(
                        "shutdown refused: {open} position(s) of the core are open — \
                         close them first"
                    )
                } else {
                    self.shutdown_requested = true;
                    "shutdown: the core is leaving".to_string()
                };
                log::info!("{text}");
                session.send_encrypted(LOG, &log_msg(now_ms(), &text), true);
            }
            ui::CMD_SETTINGS_REQUEST => {
                self.refresh_temp_black_list(now_ms());
                let resp = ui::with_uid(&self.client_settings, hdr.uid);
                session.send_encrypted(UI, &resp, true);
                // Post-init: the client applies the counters only after Ready.
                session.send_encrypted(UI, &profit_state(&self.profit), true);
            }
            ui::CMD_SHARED_CONFIG => {
                if let Some(blob) = ui::shared_config_blob(payload) {
                    self.shared_config = blob.to_vec();
                    self.read_terminal_shots();
                }
                let resp = ui::shared_config_payload(rand_uid(), &self.shared_config);
                session.send_encrypted(UI, &resp, true);
            }
            ui::CMD_SHARED_CONFIG_REQUEST => {
                let resp = ui::shared_config_payload(hdr.uid, &self.shared_config);
                session.send_encrypted(UI, &resp, true);
            }
            ui::CMD_KERNEL_LICENSE_STATE_REQUEST => {
                session.send_encrypted(UI, &ui::kernel_license_state(hdr.uid, true), true);
            }
            ui::CMD_STRAT_START_STOP | ui::CMD_STRAT_START_STOP_V2 => {
                let body = &payload[BASE_HEADER_SIZE..];
                let Some((start, items)) = ui::parse_strat_start_stop(hdr.cmd_id, body) else {
                    return;
                };
                if !items.is_empty() {
                    self.checked(session, &items);
                }
                // A start or stop by hand is the trader's: «Restart if» and
                // the circuit breakers' restarts no longer apply.
                self.set_strategies(start);
            }
            // The terminal's Telegram panel (moonproto 9fd0490) drives a
            // MoonBot core's built-in Telegram reader. This core has none, and
            // says so: every control is answered with a disabled, unsupported
            // state — silence would leave the panel waiting with no state at
            // all.
            ui::CMD_TELEGRAM_REFRESH..=ui::CMD_TELEGRAM_LOGOUT => {
                session.send_encrypted(
                    UI,
                    &ui::telegram_state(rand_uid(), TELEGRAM_UNSUPPORTED),
                    true,
                );
            }
            other => log::debug!("UI cmd {other} ignored"),
        }
    }

    /// Strategy list sync: the terminal owns edits, the core keeps the list,
    /// echoes accepted revisions (or its newer copy) to every session.
    fn on_strat(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(hdr) = BaseHeader::parse(payload) else {
            return;
        };
        let body = &payload[BASE_HEADER_SIZE..];
        let now = now_ms();
        match hdr.cmd_id {
            // The mandatory Init step: without this answer the client never
            // reaches `Ready`.
            strat::CMD_SCHEMA_REQUEST => {
                let resp = strat::schema_payload(hdr.uid, self.strategies.schema_blob());
                session.send_encrypted(STRAT, &resp, true);
            }
            strat::CMD_SNAPSHOT => {
                let Some(snap) = strat::parse_snapshot(body) else {
                    return;
                };
                let (touched, rejected) = self.strategies.apply_snapshot(&snap);
                self.replay_guards();
                for id in rejected {
                    self.outbox.push((STRAT, strat::delete(rand_uid(), id, "")));
                }
                let echo = if snap.full {
                    Some(self.strategies.full_payload(rand_uid()))
                } else {
                    self.strategies.partial_payload(rand_uid(), &touched)
                };
                if let Some(echo) = echo {
                    self.outbox.push((STRAT, echo));
                }
            }
            strat::CMD_DELETE => {
                let Some((id, folder)) = strat::parse_delete(body) else {
                    return;
                };
                if self.strategies.delete(id, &folder, now) {
                    self.replay_guards();
                    self.outbox
                        .push((STRAT, strat::delete(rand_uid(), id, &folder)));
                }
            }
            strat::CMD_SELL_PRICE_UPDATE => {
                let Some((id, price)) = strat::parse_sell_price(body) else {
                    return;
                };
                if self.strategies.set_sell_price(id, price, now) {
                    if let Some(p) = self.strategies.partial_payload(rand_uid(), &[id]) {
                        self.outbox.push((STRAT, p));
                    }
                }
            }
            strat::CMD_CHECKED_SYNC => {
                if let Some(items) = strat::parse_checked_sync(body) {
                    self.checked(session, &items);
                }
            }
            other => log::debug!("Strat cmd {other} ignored"),
        }
    }

    /// Checked flags: acknowledged to the sender, forwarded to every session.
    fn checked(&mut self, session: &mut Session, items: &[strat::CheckedItem]) {
        for it in items {
            log::info!("strategy {} checked={}", it.strategy_id, it.checked);
        }
        self.strategies.set_checked(items);
        session.send_encrypted(STRAT, &strat::checked_echo(rand_uid(), items), true);
        self.outbox
            .push((STRAT, strat::checked_sync(rand_uid(), items)));
    }

    /// Start or stop the strategies by hand, every terminal's button following.
    fn set_strategies(&mut self, start: bool) {
        self.strategies.set_running(start);
        self.market_stopped = false;
        self.circuit_stopped = None;
        let checked = self.strategies.list().iter().filter(|s| s.checked).count();
        log::info!(
            "strategies {} ({checked} of {} checked)",
            if start { "started" } else { "stopped" },
            self.strategies.list().len()
        );
        self.outbox
            .push((STRAT, strat::runtime_state(rand_uid(), start)));
    }

    /// `CancelAllOrders`: the strategies stop (or they would lay the ladders again within a
    /// second) and every live entry, hand ones and pending ones too, is cancelled. Exits stay;
    /// an entry that partly filled gets its exit as with any cancel (`trading.mdc`).
    fn cancel_all_orders(&mut self, now: i64) {
        let stopped = self.stop_strategies();
        self.market_stopped = false;
        let ids: Vec<u64> = self
            .orders
            .iter()
            .filter(|o| o.status == trade::status::BUY_SET || o.is_pending())
            .map(|o| o.id)
            .collect();
        let mut fx = Effects::default();
        for &id in &ids {
            fx.extend(self.orders.cancel_buy(id, now));
        }
        fx.logs.push(format!(
            "Cancel ALL orders: {}{} entries cancelled",
            if stopped { "strategies stopped, " } else { "" },
            ids.len()
        ));
        self.effects(fx, now);
    }

    /// Stop the running strategies, the terminals' flag following (their pass
    /// withdraws their entries); false when they were not running. What a
    /// stop clears or latches is the caller's.
    fn stop_strategies(&mut self) -> bool {
        if !self.strategies.running() {
            return false;
        }
        self.strategies.set_running(false);
        self.outbox
            .push((STRAT, strat::runtime_state(rand_uid(), false)));
        true
    }

    /// Rewrite the kept `TClientSettings` with the temporary black list as it
    /// stands now, so an echo does not hand a terminal the old remaining
    /// times to send back (which would extend every row).
    fn refresh_temp_black_list(&mut self, now: i64) {
        if self.black_list.1.is_empty() {
            return;
        }
        let rows = self
            .black_list
            .1
            .iter()
            .filter(|(_, until)| *until > now)
            .map(|(sym, until)| {
                (
                    sym.clone(),
                    std::time::Duration::from_millis((until - now) as u64),
                )
            })
            .collect();
        if let Some(payload) = ui::with_temp_black_list(&self.client_settings, rows) {
            self.client_settings = payload;
            self.settings_at = now;
            self.black_list.1.retain(|(_, until)| *until > now);
        }
    }

    /// The terminal's `TClientSettings`: echoed on request, kept across
    /// restarts, read for the manual defaults, the emulator mode, the global
    /// black list and the auto-start rules.
    fn set_client_settings(&mut self, payload: Vec<u8>, received_at: i64) {
        self.settings_at = received_at;
        self.client_settings = payload;
        let was = self.manual.emulator;
        self.read_manual_defaults();
        self.shots.set_emulator(self.emu_mode());
        self.guard_rules = None;
        if was != self.manual.emulator {
            self.replay_guards();
        }
        let payload = &self.client_settings;
        if let Some((permanent, temporary)) = ui::black_list(payload) {
            // The remaining time counts from when the terminal sent it.
            let temporary: Vec<(String, i64)> = temporary
                .into_iter()
                .filter(|(_, days)| days.is_finite() && *days > 0.0)
                // Capped at a century and added saturating: a typed «1e300 days» must not wrap
                // the sum into the past (and the row with it out of the list).
                .map(|(sym, days)| {
                    let ms = (days.min(36_500.0) * 86_400_000.0) as i64;
                    (sym, received_at.saturating_add(ms))
                })
                .collect();
            let list = (permanent.into_iter().collect::<HashSet<_>>(), temporary);
            let symbols =
                |l: &[(String, i64)]| l.iter().map(|(s, _)| s.clone()).collect::<HashSet<_>>();
            if list.0 != self.black_list.0 || symbols(&list.1) != symbols(&self.black_list.1) {
                log::info!(
                    "global black list: {} permanent, {} temporary",
                    list.0.len(),
                    list.1.len()
                );
            }
            self.black_list = list;
        }
        if let Some((cfg, cfg2)) = ui::auto_start(payload) {
            let window = if cfg.work_time {
                // Not fractions of a day: closed, not open, like a bad WorkingTime.
                let w = WorkWindow::of_day_fractions(cfg.work_time_from, cfg.work_time_to);
                if w.is_none() {
                    log::warn!(
                        "auto-start work time {} – {} is not a time of day: strategies do not work",
                        cfg.work_time_from,
                        cfg.work_time_to
                    );
                }
                Some(w.unwrap_or(WorkWindow::Closed))
            } else {
                None
            };
            self.shots.set_work_window(window);
            let rules = autostop::Rules::from_config(&cfg, &cfg2);
            if rules != self.auto_stop {
                log::info!("auto-stop: {rules:?}");
            }
            self.auto_stop = rules;
        }
    }

    fn on_order(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(hdr) = BaseHeader::parse(payload) else {
            return;
        };
        let resp = match hdr.cmd_id {
            trade::CMD_ORDER_STATUS_REQUEST if hdr.uid == 0 => {
                trade::orders_snapshot(0, &self.orders.records(now_ms()))
            }
            trade::CMD_ORDER_STATUS_REQUEST => match self.orders.get(hdr.uid) {
                Some(o) => trade::order_image(&o.record()),
                None => trade::order_not_found(hdr.uid),
            },
            trade::CMD_ORDER_COMMAND => {
                return self.on_order_command(hdr.uid, &payload[BASE_HEADER_SIZE..]);
            }
            report::CMD_SCHEMA_REQUEST => report::schema_payload(hdr.uid, reports::FIELDS),
            report::CMD_SYNC_REQUEST => {
                let Some((from, depth)) = report::sync_request(payload) else {
                    return;
                };
                let page = self.reports.page(from, depth, now_ms());
                report::sync_page(
                    hdr.uid,
                    self.reports.epoch(),
                    page.last_rec_id,
                    self.reports.max_rec_id(),
                    page.count,
                    &page.rows,
                )
            }
            report::CMD_ALIVE_MAP_REQUEST => {
                let Some(up_to) = report::alive_map_request_up_to(payload)
                    .filter(|&n| n >= 0 && n <= self.reports.max_rec_id() + reports::ALIVE_SLACK)
                else {
                    log::warn!("reports: alive map request out of range ignored");
                    return;
                };
                let bitmap = self.reports.alive_bitmap(up_to);
                report::alive_map(hdr.uid, self.reports.epoch(), up_to, &bitmap)
            }
            report::CMD_CHECK_ROWS_REQUEST => {
                for id in report::check_rows(payload).unwrap_or_default() {
                    let resp = match self.reports.row(id) {
                        Some(row) => row_upsert(row),
                        None => report::row_delete(rand_uid(), id),
                    };
                    session.send_encrypted(ORDER, &resp, true);
                }
                return;
            }
            report::CMD_SET_ROWS_DELETED => {
                let Some(sel) = report::set_rows_deleted(payload) else {
                    return;
                };
                let changed = self.reports.set_deleted(&sel);
                if !changed.is_empty() {
                    self.replay_guards();
                }
                log::info!(
                    "reports: {} {} rows",
                    if sel.deleted { "deleted" } else { "restored" },
                    changed.len()
                );
                // Every subscriber applies the echo, the sender included.
                self.outbox.push((ORDER, payload.to_vec()));
                self.push_profit(now_ms());
                return;
            }
            other => {
                log::debug!("Order cmd {other} ignored");
                return;
            }
        };
        session.send_encrypted(ORDER, &resp, true);
    }

    /// `TOrderCommand` from the terminal; results go out through `effects`.
    fn on_order_command(&mut self, req_uid: u64, body: &[u8]) {
        let now = now_ms();
        let cmd = OrderCommand::parse(body);
        // Every order command as the terminal sent it, before the core acts on it: when a click
        // does nothing, or goes the wrong way (the direction is the terminal's bit, copied
        // verbatim), this is the line that says what was asked. A refusal follows with its own
        // reason. Hand actions only, so the volume is a person's.
        log::info!("order command {req_uid:#x}: {cmd:?}");
        let fx = match cmd {
            OrderCommand::Start(s) => self.start_order(req_uid, &s, false, now),
            OrderCommand::StartPending(s) => self.start_order(req_uid, &s, true, now),
            OrderCommand::TargetBuy {
                order_id,
                price,
                size,
            } => self.orders.target_manual(
                order_id,
                Leg::Buy,
                price,
                (size > 0.0).then_some(size),
                now,
            ),
            OrderCommand::TargetSell { order_id, price } => {
                self.orders.target(order_id, Leg::Sell, price, None)
            }
            OrderCommand::CancelBuy { order_id } => self.orders.cancel_buy(order_id, now),
            OrderCommand::CancelSell { order_id } => self.orders.cancel(order_id, Leg::Sell),
            OrderCommand::Stops { order_id, stops } => self.apply_stops(order_id, &stops),
            OrderCommand::Panic { order_id, enabled } => {
                let uid = self.orders.get(order_id).map(|o| o.uid.clone());
                match uid.as_deref().and_then(|u| self.catalog.get(u)) {
                    Some(m) => self.orders.set_panic(order_id, enabled, m, now),
                    None => Effects {
                        logs: vec![format!(
                            "Panic Sell of order {order_id}: no such order on a known market"
                        )],
                        ..Effects::default()
                    },
                }
            }
            // Modes 2/3 split the position into pieces; closing it whole
            // instead would sell what the trader meant to keep.
            OrderCommand::ClosePosition { market, mode, .. } if mode >= 2 => Effects {
                logs: vec![format!(
                    "{market}: split position is not supported by this core"
                )],
                ..Effects::default()
            },
            // Mode 0: all, `flag` = at market; mode 1: the limit close of one
            // side (`flag` = short).
            OrderCommand::ClosePosition { market, mode, flag } => {
                let (side, at_market) = if mode == 1 {
                    (Some(flag), false)
                } else {
                    (None, flag)
                };
                // The positions of the mode the core trades in: the emulator's
                // with the terminal's emulator mode on, or on a core without
                // an account (whose strategies can only be emulated).
                let emulated = self.emu_mode();
                let ready = if emulated {
                    self.listed(&market)
                } else {
                    self.tradable(&market)
                };
                match ready {
                    Ok(()) => {
                        let m = self.catalog.get(&market).expect("tradable");
                        self.orders
                            .close_position(m, emulated, side, at_market, now)
                    }
                    Err(reason) => Effects {
                        logs: vec![reason],
                        ..Effects::default()
                    },
                }
            }
            OrderCommand::Immune { order_id, enabled } => self.orders.set_immune(order_id, enabled),
            OrderCommand::PanicSellAll => self.orders.panic_all(&self.catalog, now),
            OrderCommand::PendingCancel { order_id } => self.orders.cancel_pending(order_id, now),
            OrderCommand::MoveAll {
                market,
                sells,
                kind,
                move_kind,
                side,
                price,
                ..
            } => self.move_all(&market, sells, kind, move_kind, side, price, now),
            OrderCommand::Other(op) => Effects {
                logs: vec![format!("order command {op} is not supported by this core")],
                ..Effects::default()
            },
        };
        self.effects(fx, now);
    }

    /// `Start` / `StartPending` (`pending`: the core holds the order until the
    /// price crosses its trigger). A manual entry on a market whose trade
    /// stream is down is refused: its price is not the market's.
    fn start_order(&mut self, req_uid: u64, s: &StartOrder, pending: bool, now: i64) -> Effects {
        if self.stopping {
            // The sweep has been through: an entry placed now would outlive
            // the process that placed it.
            return self
                .orders
                .fail_start(req_uid, s, "the core is stopping", now);
        }
        let manual = self.manual;
        let is_manual = s.strategy_id == 0;
        // «Use the manual strategy»: a hand trade takes its exit settings
        // (SellPrice, PriceDown, stops, trailing) instead of the toolbar's.
        let routed = if is_manual {
            self.manual_strategy()
        } else {
            None
        };
        let toolbar = is_manual && routed.is_none();
        // The terminal's emulator mode, a strategy in its own, or a strategy
        // on a core with no account: the order is answered by the emulator
        // and never reaches the exchange — so it needs no account either. A
        // hand trade without an account is refused below, not emulated: the
        // trader must not take a paper order for a real one.
        let emulated = manual.emulator
            || (!is_manual && self.trading.is_none())
            || (!toolbar && self.strategy_emulated(routed.unwrap_or(s.strategy_id)));
        let ready = if emulated {
            self.listed(&s.market)
        } else {
            self.tradable(&s.market)
        };
        if let Err(reason) = ready {
            return self.orders.fail_start(req_uid, s, &reason, now);
        }
        if !emulated && self.closed_markets.contains_key(&s.market) {
            let reason = format!(
                "{}: the exchange takes no new positions on it (up to an hour)",
                s.market
            );
            return self.orders.fail_start(req_uid, s, &reason, now);
        }
        // The strategies judge this themselves (`opposite`); a hand entry against an open
        // position would net it on the exchange (one-way mode) and leave two orders for one.
        if is_manual && self.orders.holds_against(&s.market, s.is_short, emulated) {
            let reason = format!(
                "{}: a {} position is open on it: close it before entering the other way",
                s.market,
                if s.is_short { "long" } else { "short" }
            );
            return self.orders.fail_start(req_uid, s, &reason, now);
        }
        let m = self.catalog.get(&s.market).expect("tradable");
        if !m.fresh() {
            let reason = format!(
                "{}: market data is stale (no trade stream): orders are held",
                s.market
            );
            return self.orders.fail_start(req_uid, s, &reason, now);
        }
        let mut s = s.clone();
        let mut notes = Vec::new();
        if let Some(id) = routed {
            s.strategy_id = id;
        }
        // The toolbar's take profit, when the order names no exit — MoonBot
        // applies its `cfg` to a manual order (TInvestCore, as is). It is the
        // planned sell, which the order's own stops do not speak of, so it
        // applies whether or not they came with it.
        let entry = if s.price > 0.0 { s.price } else { m.last() };
        let k = if s.is_short { -1.0 } else { 1.0 };
        let at = |pct: f64| m.nearest(entry * (1.0 + k * pct / 100.0));
        if toolbar && s.planned_sell <= 0.0 && manual.take_profit_pct > 0.0 {
            match (entry > 0.0).then(|| at(manual.take_profit_pct)) {
                Some(tp) if tp > 0.0 => s.planned_sell = tp,
                _ => notes.push(format!(
                    "{}: no price for the toolbar's take profit {}%: the order has none",
                    s.market, manual.take_profit_pct
                )),
            }
        }
        let mut fx = if pending {
            self.orders.start_pending(req_uid, &s, m, now)
        } else {
            self.orders.start(req_uid, &s, m, now)
        };
        fx.logs.extend(notes);
        let id = fx.changed.first().copied().unwrap_or(0);
        if emulated {
            self.orders.set_emulator(id);
            self.emu_markets.insert(s.market.clone());
        }
        if let Some(strategy) = routed {
            self.orders.set_hand(id);
            if emulated && !manual.emulator {
                fx.logs.push(format!(
                    "{}: the manual strategy #{strategy} is in EmulatorMode: this hand trade is emulated",
                    s.market
                ));
            }
        }
        // A pending order counts as placed: its stops are set now and wait
        // with it, so the entry is protected the moment it goes out.
        let placed = self
            .orders
            .get(id)
            .is_some_and(|o| o.status == trade::status::BUY_SET || o.is_pending());
        if !placed {
            return fx;
        }
        // The order's own stops, as the `Stops` command sets them; without
        // them, the toolbar's for a manual order.
        if let Some(st) = s.stops {
            fx.extend(self.apply_stops(id, &st));
            return fx;
        }
        if !toolbar {
            return fx;
        }
        if manual.stop_pct > 0.0 {
            fx.extend(self.orders.set_stops(id, true, false, manual.stop_pct, 0.0));
        }
        // The toolbar's trailing stop, from its take profit when it has one.
        if manual.trailing_pct > 0.0 {
            let from = (manual.trailing_from_pct > 0.0 && entry > 0.0)
                .then(|| at(manual.trailing_from_pct))
                .filter(|p| *p > 0.0);
            fx.extend(self.orders.set_trailing(
                id,
                true,
                false,
                manual.trailing_pct,
                0.0,
                from.is_some(),
                from.unwrap_or(0.0),
            ));
        }
        fx
    }

    /// An order's stops as the `Stops` command carries them.
    fn apply_stops(&mut self, id: u64, st: &InitialStops) -> Effects {
        let mut fx = self
            .orders
            .set_stops(id, st.sl_on, st.sl_fixed, st.sl_level, st.sl_spread);
        fx.extend(self.orders.set_trailing(
            id,
            st.trail_on,
            st.trail_fixed,
            st.trail_level,
            st.trail_spread,
            st.tp_on,
            st.tp,
        ));
        fx
    }

    /// The manual-order defaults of the terminal's last `TClientSettings`:
    /// the toolbar's stop, take profit, trailing, and the emulator mode.
    fn read_manual_defaults(&mut self) {
        match ui::manual_defaults(&self.client_settings) {
            Some(m) => {
                if m.emulator != self.manual.emulator {
                    log::info!("emulator mode {}", if m.emulator { "on" } else { "off" });
                }
                self.manual = m;
            }
            None => log::warn!(
                "client settings: unreadable, the previous manual defaults stay \
                 (emulator mode {})",
                if self.manual.emulator { "on" } else { "off" }
            ),
        }
    }

    /// Positions of the core still open: a filled entry, with its exit live or
    /// not yet placed. Emulated ones hold no money.
    fn open_positions(&self) -> usize {
        self.orders
            .iter()
            .filter(|o| !o.emulator && o.holds_position())
            .count()
    }

    /// The rule the core leaves by (TInvestCore, as is): no order that can
    /// open or grow a position is left resting with no core to watch it. The
    /// exits stay — a position without its exit is worse than a position —
    /// and the entries go. Shuts the terminal's `Start` first, then sweeps;
    /// returns how many it asked to withdraw. What the caller waits on is
    /// [`Self::live_entries`]: a withdrawal the exchange has not confirmed is
    /// still the exchange's.
    pub fn begin_stop(&mut self, now: i64) -> usize {
        self.stopping = true;
        self.sweep_entries(now)
    }

    /// Withdraw every entry still live, on every pass of the drain: a cancel
    /// already out is not sent twice (the model holds it while its answer is
    /// awaited), and one that failed — refused, lost to the transport — goes
    /// out again. Returns how many entries it withdrew or asked to withdraw on
    /// this pass.
    pub fn sweep_entries(&mut self, now: i64) -> usize {
        let ids: Vec<u64> = self
            .orders
            .iter()
            .filter(|o| o.status == trade::status::BUY_SET || o.is_pending())
            .map(|o| o.id)
            .collect();
        let pending = ids
            .iter()
            .filter(|id| self.orders.get(**id).is_some_and(|o| o.is_pending()))
            .count();
        let mut fx = Effects::default();
        for id in ids {
            fx.extend(self.orders.cancel_buy(id, now));
        }
        // Pending ones go at once, with no call; live ones by a cancel.
        let sent = fx.actions.len() + pending;
        if sent > 0 {
            fx.logs
                .push(format!("stop: withdrawing {sent} entry order(s)"));
        }
        self.effects(fx, now);
        // Straight out, not on the next pump: the process is leaving.
        self.flush_actions(now);
        sent
    }

    /// Exits that have not settled on the exchange: queued for the worker, or
    /// still on their way (`CoreOrder::exit_unsettled`). An entry that filled
    /// while the stop was withdrawing it gets its exit this way, an exit being
    /// moved is cancelled before its replacement goes, and the core must not
    /// leave in between.
    pub fn exits_in_flight(&self) -> usize {
        let queued = self
            .pending_actions
            .iter()
            .filter(|(a, _)| {
                matches!(
                    a,
                    Action::Post { leg: Leg::Sell, .. } | Action::Replace { leg: Leg::Sell, .. }
                )
            })
            .count();
        queued
            + self
                .orders
                .iter()
                .filter(|o| !o.emulator && o.exit_unsettled())
                .count()
    }

    /// Entry orders the exchange still holds; an emulated or pending one
    /// never reached it.
    pub fn live_entries(&self) -> usize {
        self.orders
            .iter()
            .filter(|o| !o.emulator && o.status == trade::status::BUY_SET)
            .count()
    }

    /// There is an order router to send the withdrawals to. One worker lost is no reason not to
    /// withdraw through the rest: the entries on its market come back `Failed` and are named as
    /// left (`trading::start`).
    pub fn can_withdraw(&self) -> bool {
        self.trading.is_some()
    }

    /// Nothing waits in the outbox for the sessions.
    pub fn outbox_empty(&self) -> bool {
        self.outbox.is_empty()
    }

    /// The last thing the core does: the orders on disk for the next start.
    pub fn finish(&mut self) {
        let now = now_ms();
        self.persist_orders(true, now);
        let live = self
            .orders
            .iter()
            .filter(|o| !trade::status::is_terminal(o.status))
            .count();
        log::info!("stopped: {live} live order(s) saved");
        self.farewell(now, live);
    }

    /// «Move all» (Order opcode 11) of one leg on a market (TInvestCore, as
    /// is): `kind` 2 moves every resting order `value` percent (immune ones
    /// too, as MoonBot); `kind` 0 moves the ones `move_kind` picks — All (5),
    /// LastSet (6), TopVol (2), LowVol (3) — to the price `value`; Shift (1) moves the
    /// whole grid so that its order nearest the market lands on `value` (nearest is the
    /// highest resting price of a grid below the market, the lowest above it — not the live
    /// market price). All of them for a `side` (0 both, 1 long, 2 short), skipping immune
    /// ones, which Shift leaves out of the anchor too. The price zone (`kind` 1) and other
    /// kinds are said to be unsupported.
    #[allow(clippy::too_many_arguments)]
    fn move_all(
        &mut self,
        market: &str,
        sells: bool,
        kind: u8,
        move_kind: u8,
        side: u8,
        value: f64,
        now: i64,
    ) -> Effects {
        let unsupported = |what: &str| Effects {
            logs: vec![format!(
                "{market}: Move all {what} is not supported by this core"
            )],
            ..Effects::default()
        };
        if let Err(reason) = self.tradable(market) {
            return Effects {
                logs: vec![reason],
                ..Effects::default()
            };
        }
        let m = self.catalog.get(market).expect("tradable");
        if !value.is_finite() {
            return unsupported("with this price");
        }
        let mut found = self.orders.resting(&m.symbol, sells);
        if side != 0 {
            let short = side == 2;
            found.retain(|r| self.orders.get(r.id).is_some_and(|o| o.is_short == short));
        }
        // One order of a tie wins by its id, not by the map's order.
        found.sort_by_key(|r| r.id);
        // The last-set and the largest of a tie are the smaller id, as the
        // smallest is (`trading.mdc`, Move all): the sort above makes `min_by`
        // take it, and these comparisons make `max_by` take it too.
        let later = |a: &Resting, b: &Resting| a.set_at.cmp(&b.set_at).then(b.id.cmp(&a.id));
        let larger = |a: &Resting, b: &Resting| a.value.total_cmp(&b.value).then(b.id.cmp(&a.id));
        let moves: Vec<(u64, Leg, f64)> = match (kind, move_kind) {
            (2, _) => found
                .iter()
                .map(|r| (r.id, r.leg, r.price * (1.0 + value / 100.0)))
                .collect(),
            (0, 2 | 3 | 5 | 6) => {
                found.retain(|r| !r.immune);
                let pick: Vec<Resting> = match move_kind {
                    5 => found.clone(),
                    6 => found
                        .iter()
                        .max_by(|a, b| later(a, b))
                        .copied()
                        .into_iter()
                        .collect(),
                    2 => found
                        .iter()
                        .max_by(|a, b| larger(a, b))
                        .copied()
                        .into_iter()
                        .collect(),
                    _ => found
                        .iter()
                        .min_by(|a, b| a.value.total_cmp(&b.value))
                        .copied()
                        .into_iter()
                        .collect(),
                };
                pick.into_iter().map(|r| (r.id, r.leg, value)).collect()
            }
            // Parallel shift: the order nearest the market lands on `value`, the rest keep
            // their distance to it. Each direction is its own grid (resting below the market
            // is a long entry or a short's close), so the nearest is the highest there and
            // the lowest above; the smaller id wins a tie (the sort above).
            (0, 1) => {
                found.retain(|r| !r.immune);
                let below = |r: &Resting| {
                    (r.leg == Leg::Buy) != self.orders.get(r.id).is_some_and(|o| o.is_short)
                };
                let anchor = |grid_below: bool| {
                    let grid = found.iter().filter(|r| below(r) == grid_below);
                    if grid_below {
                        grid.max_by(|a, b| a.price.total_cmp(&b.price).then(b.id.cmp(&a.id)))
                    } else {
                        grid.min_by(|a, b| a.price.total_cmp(&b.price))
                    }
                    .map(|r| r.price)
                };
                let anchors = [anchor(true), anchor(false)];
                // One tick-aligned distance for the grid: the members stay a whole number of
                // ticks apart, as they were.
                let to = m.nearest(value);
                found
                    .iter()
                    .filter_map(|r| {
                        let from = anchors[usize::from(!below(r))]?;
                        let shift = to - from;
                        Some((r.id, r.leg, r.price + shift))
                    })
                    .collect()
            }
            (1, _) => return unsupported("by price zone"),
            _ => return unsupported("of this kind"),
        };
        let mut fx = Effects::default();
        if moves.is_empty() {
            fx.logs.push(format!(
                "{market}: Move all — no {} of the core to move",
                if sells { "exit" } else { "entry" }
            ));
        }
        for (id, leg, price) in moves {
            // A price at or below zero is not a move to make, whatever the
            // band would pin it to.
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            // At a tick and inside the exchange band, as every other move.
            let price = m.within_limits(m.nearest(price));
            fx.extend(match leg {
                Leg::Sell => self.orders.target(id, Leg::Sell, price, None),
                Leg::Buy => self.orders.target_manual(id, Leg::Buy, price, None, now),
            });
        }
        fx
    }

    /// The markets closed to entries, as the strategies see them.
    fn sync_closed_markets(&mut self) {
        let closed: HashSet<String> = self.closed_markets.keys().cloned().collect();
        self.shots.set_closed_markets(&closed);
    }

    /// A market the exchange refused new positions on is tried again after `CLOSED_MARKET_MS`:
    /// `-4140` can be a halt of an hour, and a refusal that lasted the run would outlive it.
    fn expire_closed_markets(&mut self, now: i64) {
        let before = self.closed_markets.len();
        self.closed_markets
            .retain(|_, at| now - *at < CLOSED_MARKET_MS);
        if self.closed_markets.len() != before {
            self.sync_closed_markets();
        }
    }

    /// `-4140` (the symbol is closed) / `-4141` (no new positions on it) on an
    /// entry: the market takes no new entry for an hour (`CLOSED_MARKET_MS`) — the core refuses
    /// the next one instead of the exchange. Exits, closes and moves of what
    /// is already open are not touched: a position there still needs them.
    fn market_closed(&mut self, action: &Action, msg: &str) -> Effects {
        let entry = matches!(
            action,
            Action::Post { leg: Leg::Buy, .. } | Action::Replace { leg: Leg::Buy, .. }
        );
        if !entry
            || !(crate::aster::rest::msg_has_code(msg, -4140)
                || crate::aster::rest::msg_has_code(msg, -4141))
        {
            return Effects::default();
        }
        let Some(symbol) = self.orders.get(action.order()).map(|o| o.uid.clone()) else {
            return Effects::default();
        };
        if self
            .closed_markets
            .insert(symbol.clone(), now_ms())
            .is_some()
        {
            return Effects::default();
        }
        self.sync_closed_markets();
        Effects {
            logs: vec![format!(
                "{symbol}: the exchange takes no new positions on it — entries refused for an \
                 hour"
            )],
            ..Effects::default()
        }
    }

    /// The core may send exchange orders for `market`.
    fn tradable(&self, market: &str) -> Result<(), String> {
        if self.trading.is_none() {
            return Err("trading is off: the core runs without an account key".into());
        }
        self.listed(market)
    }

    /// `market` is in the catalog and trading: what an emulated order needs.
    fn listed(&self, market: &str) -> Result<(), String> {
        match self.catalog.get(market) {
            Some(m) if m.trading => Ok(()),
            Some(_) => Err(format!("{market}: the market is not trading")),
            None => Err(format!("{market}: unknown market")),
        }
    }

    /// Every strategy order is emulated: the terminal's emulator mode is on,
    /// or the core has no account to trade on — a strategy there can only be
    /// watched, and the emulator is how.
    fn emu_mode(&self) -> bool {
        self.manual.emulator || self.trading.is_none()
    }

    /// The terminal's manual strategy (`use_manual_strategy`), when it names
    /// a listed strategy of kind Manual.
    fn manual_strategy(&self) -> Option<u64> {
        let id = self.manual.manual_strategy?;
        self.strategies
            .list()
            .iter()
            .any(|s| s.strategy_id == id && s.kind() == StrategyKind::MANUAL)
            .then_some(id)
    }

    /// The strategy trades in the emulator (`EmulatorMode`).
    fn strategy_emulated(&self, strategy_id: u64) -> bool {
        self.strategies
            .list()
            .iter()
            .find(|s| s.strategy_id == strategy_id)
            .is_some_and(|s| Params::from_snapshot(s, self.strategies.schema()).emulator)
    }

    /// Per listed strategy: its emulator mode (its own `EmulatorMode` or the
    /// core-wide one) and its Sessions tab. Cached until what it reads
    /// changes (`replay_guards`, the manual settings).
    fn guard_rules(&mut self) -> Arc<GuardRules> {
        let (strategies, emu_mode) = (&self.strategies, self.emu_mode());
        Arc::clone(self.guard_rules.get_or_insert_with(|| {
            Arc::new(
                strategies
                    .list()
                    .iter()
                    .map(|s| {
                        let p = Params::from_snapshot(s, strategies.schema());
                        (s.strategy_id, (p.emulator || emu_mode, p.session))
                    })
                    .collect(),
            )
        }))
    }

    /// Rebuild the sessions from the report: at start, and whenever what
    /// counts changes (a deleted or restored row, a mode, a strategy), so the
    /// live state is the one a restart would rebuild.
    fn replay_guards(&mut self) {
        self.penalty_marks = None;
        self.guard_rules = None;
        let rules = self.guard_rules();
        let mut guards = std::mem::take(&mut self.guards);
        guards.replay(self.reports.rows().filter(|r| !r.estimated), |r| {
            rule_of(&rules, r)
        });
        self.guards = guards;
    }

    /// Broadcast the report profit counters when they changed.
    fn push_profit(&mut self, now: i64) {
        self.profit_at = now;
        let p = self.reports.profit(now);
        if p != self.profit {
            self.profit = p;
            self.outbox.push((UI, profit_state(&p)));
        }
    }

    /// An outcome of the exchange, from the worker or the user-data stream.
    fn on_trading(&mut self, ev: TradingEvent) {
        let now = now_ms();
        let fx = match ev {
            TradingEvent::Order(u) => {
                let rejected = u.status == crate::trading::ExecStatus::Rejected;
                let fx = self.order_update(&u, now);
                if rejected {
                    for &id in &fx.changed {
                        self.shots.on_failed(id, &u.message, now);
                    }
                }
                self.shots_due = true;
                fx
            }
            TradingEvent::OpenOrdersFailed => {
                self.open_orders_due = Some(now + OPEN_ORDERS_RETRY_MS);
                return;
            }
            TradingEvent::OpenOrders(list) => {
                let open: Vec<String> = list.iter().map(|u| u.exchange_id.clone()).collect();
                for u in &list {
                    let fx = self.order_update(u, now);
                    self.effects(fx, now);
                }
                let open: Vec<&str> = open.iter().map(String::as_str).collect();
                Effects {
                    actions: self.orders.missing_from(&open),
                    ..Effects::default()
                }
            }
            TradingEvent::Failed {
                action,
                definitive,
                msg,
            } => {
                // Not the exchange's refusal of the order: the connection, a
                // server error, a rate limit — what the error breaker counts.
                if !definitive || moonshot::rate_limited(&msg) {
                    self.api_errors.push_back(now);
                }
                let order = action.order();
                // The exchange's own refusal, deduplicated by market and code:
                // one market can be refused every minute for an hour. A rate
                // limit refused nothing about the order.
                if definitive && !moonshot::rate_limited(&msg) {
                    let symbol = self
                        .orders
                        .get(order)
                        .map_or("?".to_string(), |o| o.uid.clone());
                    let code = msg
                        .split_once('/')
                        .and_then(|(_, rest)| rest.split(':').next())
                        .unwrap_or("?")
                        .to_string();
                    self.tg_keyed(
                        crate::telegram::Kind::Refusal,
                        format!("{symbol} {code}"),
                        format!("⚠️ {symbol}: order refused — {msg}"),
                    );
                }
                let mut fx = self.orders.failed(&action, definitive, &msg, now);
                fx.extend(self.market_closed(&action, &msg));
                if !fx.logs.is_empty() || !fx.changed.is_empty() {
                    self.shots.on_failed(order, &msg, now);
                }
                self.shots_due = true;
                fx
            }
            TradingEvent::Ping(ms) => {
                log::debug!("orders: call answered in {ms} ms");
                self.shots.on_ping(ms, now);
                return;
            }
        };
        self.effects(fx, now);
    }

    /// A report of an exchange order: the core's own is applied; one placed
    /// outside the core is adopted as the exit of a position it closes, or
    /// left to the account (`Orders::adopt`), judged on the account's
    /// positions as last read.
    fn order_update(&mut self, u: &OrderUpdate, now: i64) -> Effects {
        if self.orders.knows(u) {
            return self.orders.apply(u, now);
        }
        let Some(m) = self.catalog.get(&u.uid) else {
            log::debug!(
                "order {} on unknown market {} ignored",
                u.exchange_id,
                u.uid
            );
            return Effects::default();
        };
        let held = self.account.as_ref().map(|a| {
            a.positions
                .iter()
                .find(|p| p.symbol == u.uid)
                .map_or(0.0, |p| p.size)
        });
        self.orders.adopt(u, m, held, now)
    }

    /// The account's positions as `Orders::reconcile` reads them: signed
    /// base quantity per symbol.
    fn held(account: &Account) -> HashMap<String, f64> {
        account
            .positions
            .iter()
            .map(|p| (p.symbol.clone(), p.size))
            .collect()
    }

    /// Queue order images and log lines for every session; hold the exchange
    /// work for `flush_actions`. A limit outside the `PERCENT_PRICE` band is
    /// pinned to it first, the image following (the gateway refuses it).
    fn effects(&mut self, mut fx: Effects, now: i64) {
        self.cap_to_band(&mut fx);
        self.orders.note_moves(&fx.changed, now);
        let mut reported = false;
        let mut opened: Vec<EntryNote> = Vec::new();
        let mut closed: Vec<DealNote> = Vec::new();
        let rules = self.guard_rules();
        for &id in &fx.changed {
            let Some(o) = self.orders.get(id) else {
                continue;
            };
            let record = o.record();
            self.outbox.push((ORDER, trade::order_image(&record)));
            if trade::status::is_terminal(o.status) {
                self.orders_refresh_at = now + ORDERS_REFRESH_MS;
            }
            let kind = self.strategies.kind_name(o.strategy_id);
            if let Some(row) = self.reports.record(&deal(o, &record, kind), now) {
                self.outbox.push((ORDER, row_upsert(row)));
                reported = true;
                self.penalty_marks = None;
                let row = row.clone();
                let rule = rule_of(&rules, &row);
                // An estimated exit (`Row.estimated`) is no session profit or loss.
                if !row.estimated {
                    fx.logs.extend(self.guards.book(&row, rule, now));
                }
                // The entry is whole (BUY_DONE, or SELL_SET once its exit is
                // on): MoonBot announces a position, not a partial fill.
                if !row.closed
                    && !row.deleted
                    && matches!(o.status, trade::status::BUY_DONE | trade::status::SELL_SET)
                {
                    opened.push(EntryNote {
                        rec_id: row.rec_id,
                        ordered: record.buy.quantity,
                    });
                }
                if row.closed && !row.deleted {
                    let fees = if o.emulator || self.trading.is_none() {
                        0
                    } else {
                        o.filled_orders().len()
                    };
                    closed.push(DealNote {
                        rec_id: row.rec_id,
                        fees,
                        due: now + ops::DEAL_SETTLE_MS,
                        deadline: now + ops::DEAL_FEE_WAIT_MS,
                        stop: record.stop.map(|(price, _)| price),
                        take: record.take_profit,
                        entry_moves: o.moves(Leg::Buy).to_vec(),
                        exit_moves: o.moves(Leg::Sell).to_vec(),
                    });
                }
                // The commission the user stream has already reported for
                // this deal's exchange orders.
                if row.closed && !o.emulator {
                    self.book_commissions(id, now);
                }
            }
        }
        for note in opened {
            self.note_entry(&note);
        }
        for note in closed {
            self.note_deal(note);
        }
        if reported {
            self.push_profit(now);
            self.check_auto_stop(now);
        }
        for text in fx.logs {
            log::info!("{text}");
            // The auto-stop and the market panic are the chat's alarms too.
            if text.starts_with("AutoStop:") || text.starts_with("AutoStart:") {
                self.tg(crate::telegram::Kind::Alarm, format!("⛔ {text}"));
            }
            self.outbox.push((LOG, log_msg(now, &text)));
        }
        // An emulated order's work goes to the emulator (`run_emulator`),
        // the rest to the worker after one snapshot of the batch.
        for action in fx.actions {
            match self.orders.get(action.order()) {
                Some(o) if o.emulator => {
                    self.emu_markets.insert(o.uid.clone());
                    self.emu_actions.push(action);
                }
                Some(o) => {
                    let uid = o.uid.clone();
                    self.pending_actions.push((action, uid));
                }
                None => {}
            }
        }
    }

    /// Answer the emulated orders' work as the exchange would: the answers go
    /// through `on_trading` like the worker's.
    fn run_emulator(&mut self, now: i64) {
        for _ in 0..EMU_ROUNDS {
            if self.emu_actions.is_empty() {
                break;
            }
            for action in std::mem::take(&mut self.emu_actions) {
                let market = self
                    .orders
                    .get(action.order())
                    .and_then(|o| self.catalog.get(&o.uid));
                let ev = self.emulator.answer(action, &self.orders, market, now);
                self.on_trading(ev);
            }
        }
        // Markets with nothing emulated resting any more leave the set.
        if !self.emu_markets.is_empty() && self.emu_actions.is_empty() {
            self.emu_markets = self.orders.emu_markets();
        }
    }

    /// The market `symbol` moved (an exchange trade at `trade`, else the
    /// book): the emulated orders resting there that it reached fill.
    fn emulate_fills(&mut self, symbol: &str, trade: Option<f64>) {
        if !self.emu_markets.contains(symbol) {
            return;
        }
        let Some(m) = self.catalog.get(symbol) else {
            return;
        };
        let resting = self.orders.emu_resting(symbol);
        for u in emulator::fills(&resting, m, trade, now_ms()) {
            self.on_trading(TradingEvent::Order(u));
        }
    }

    /// The 5m warm-up bars of one market into the strategies' windows: each
    /// bar a bucket of its own (the windows read 5-minute resolution in the
    /// history and minutes from the start on), and its turnover into the hour
    /// ledger. Only bars that ended before the core started: the live tape
    /// covers everything after, and the bar running at the start would be
    /// counted twice. The minutes between that bar's open and the start are
    /// lost to the windows — at most five, one twelfth of the first hour.
    fn seed_windows(&mut self, idx: u16, bars: &[crate::aster::json::Kline]) {
        const BAR_MS: i64 = 5 * 60_000;
        // A row the exchange sent short of a cell reads NaN there
        // (`json::kline_row`), and one NaN would poison every sum it joins.
        let whole = |b: &&crate::aster::json::Kline| {
            [b.open, b.low, b.high, b.quote_volume]
                .iter()
                .all(|v| v.is_finite())
        };
        for b in bars
            .iter()
            .filter(|b| b.open_ms + BAR_MS <= self.started_at)
            .filter(whole)
        {
            self.windows
                .seed(idx, b.open_ms, b.open, b.low, b.high, b.quote_volume);
            self.windows.seed_hour(idx, b.open_ms, b.quote_volume);
        }
    }

    /// Whether the strategy pass is due now: [`pass_due`] on the monotonic clock.
    fn shots_pass_due(&self) -> bool {
        let idle = i64::try_from(self.shots_began.elapsed().as_millis()).unwrap_or(i64::MAX);
        pass_due(idle, self.shots_took_us, self.shots_due, || {
            self.orders
                .iter()
                .any(|o| o.strategy_id != 0 && !trade::status::is_terminal(o.status))
        })
    }

    /// One strategy pass: its commands go through `Orders` like the
    /// terminal's. The auto-start rules are judged first, so a stop asked for
    /// now is in force before the pass would place the next entry.
    fn run_shots(&mut self, now: i64) {
        self.shots_began = Instant::now();
        self.shots_due = false;
        if now - self.market_delta.1 >= MARKET_DELTA_EVERY_MS {
            self.market_delta = (
                moonshot::market_delta(&self.catalog, &self.windows, now),
                now,
            );
        }
        self.check_market(now);
        self.check_circuits(now);
        self.shots.set_emulator(self.emu_mode());
        // `TotalLoss` counts over the auto-stop hours window, else a day,
        // the deals of each strategy's current mode only.
        let window_s = self.auto_stop.by_hours.map_or(86_400, |(_, w, _)| w);
        let rules = self.guard_rules();
        let counts =
            |r: &reports::Row| rules.get(&r.strategy_id).map(|&(emu, _)| emu) == Some(r.emulator);
        let totals = guards::totals(
            self.reports.rows().filter(|r| !r.estimated),
            now / 1000 - window_s,
            counts,
        );
        let (streaks, manual) = self
            .penalty_marks
            .get_or_insert_with(|| guards::penalty_marks(self.reports.rows(), counts))
            .clone();
        self.shots.set_guards(guards::View {
            totals,
            sessions: self.guards.held(now),
            streaks,
            manual,
        });
        let (permanent, temporary) = &self.black_list;
        let black: HashSet<String> = permanent
            .iter()
            .cloned()
            .chain(
                temporary
                    .iter()
                    .filter(|(_, until)| *until > now)
                    .map(|(sym, _)| sym.clone()),
            )
            .collect();
        self.shots.set_black_list(black);
        // A ranked pool taken before the warm-up lands is a pool by accident:
        // every volume key reads zero out of an empty window.
        self.shots.set_warming(!self.warmup_done);
        self.expire_closed_markets(now);
        let cmds = self.shots.tick(
            &self.strategies,
            &self.orders,
            &self.catalog,
            &self.windows,
            self.market_delta.0,
            now,
        );
        for cmd in cmds {
            let fx = match cmd {
                Cmd::Start { order, tier } => {
                    let fx = self.start_order(0, &order, false, now);
                    if let Some(&id) = fx.changed.first() {
                        self.shots.on_entry_started(id, tier);
                    }
                    fx
                }
                Cmd::Move {
                    order,
                    reason,
                    market: true,
                    ..
                } => {
                    let symbol = self.orders.get(order).map(|o| o.uid.clone());
                    match symbol.as_deref().and_then(|u| self.catalog.get(u)) {
                        Some(m) => self.orders.stop_out(order, reason, m, now),
                        None => Effects::default(),
                    }
                }
                Cmd::Move {
                    order,
                    leg,
                    price,
                    reason,
                    ..
                } => {
                    if leg == Leg::Sell {
                        self.orders.set_sell_reason(order, reason);
                    }
                    self.orders.target(order, leg, price, None)
                }
                Cmd::MoveEntry {
                    order,
                    price,
                    size,
                    planned,
                } => {
                    let fx = self.orders.target_entry(order, price, size);
                    self.effects(fx, now);
                    // MoonHook with `HookSellFixed` off re-prices its exit off
                    // the entry's new price instead of keeping the ratio.
                    self.orders.replan_exit(order, price, planned)
                }
                Cmd::Cancel { order } => self.orders.cancel_buy(order, now),
                Cmd::Stop {
                    order,
                    price,
                    spread,
                } => self.orders.set_bot_stop(order, price, spread),
                Cmd::AdoptStop { order } => self.orders.adopt_bot_stop(order),
                Cmd::Detect {
                    market,
                    strategy_id,
                    is_short,
                    msg,
                } => {
                    let payload =
                        strat::detect_signal(rand_uid(), &market, strategy_id, is_short, &msg);
                    self.outbox.push((STRAT, payload));
                    // Off by default, as in MoonBot: a detect a second would
                    // drown the deals the chat is actually for.
                    if self
                        .strategies
                        .flag(strategy_id, crate::strategies::REPORT_DETECTS)
                        == Some(true)
                    {
                        let side = if is_short { "SHORT" } else { "LONG" };
                        let label = self.strategy_label(strategy_id);
                        self.tg(
                            crate::telegram::Kind::Detect,
                            format!("🔎 {market} {side} · {label}\n{msg}"),
                        );
                    }
                    continue;
                }
                Cmd::Log(text) => Effects {
                    logs: vec![text],
                    ..Effects::default()
                },
            };
            self.effects(fx, now);
        }
    }

    /// Global auto-stop on loss (the terminal's auto-start tab).
    fn check_auto_stop(&mut self, now: i64) {
        let running = self.strategies.running();
        if !self.auto_stop.active() || !(running || self.auto_stop.sell_all) {
            return;
        }
        let deals = self.reports.closed_deals(self.auto_stop.with_emulator);
        let Some(why) = autostop::tripped(&self.auto_stop, &deals, self.loss_since, now / 1000)
        else {
            return;
        };
        self.stop_strategies();
        self.loss_since = now / 1000 + 1;
        let mut fx = Effects::default();
        fx.logs.push(format!(
            "AutoStop: {why}{}",
            if running { ": strategies stopped" } else { "" }
        ));
        if self.auto_stop.sell_all {
            fx.extend(self.orders.panic_all(&self.catalog, now));
        }
        self.effects(fx, now);
    }

    /// Auto-start circuit breakers: API errors of the last minute and the
    /// median ping of the answered order calls at or above their level stop
    /// the running strategies (optionally panic selling all); with a restart
    /// time they start again once it passed and the reading is back below.
    fn check_circuits(&mut self, now: i64) {
        while self.api_errors.front().is_some_and(|&at| at < now - 60_000) {
            self.api_errors.pop_front();
        }
        let errors = self.api_errors.len() as i64;
        let ping = self.shots.ping_median(now).unwrap_or(0);
        let rules = self.auto_stop;
        let readings = [
            ("API errors a minute", rules.errors, errors),
            ("ping ms", rules.ping, ping),
        ];
        let running = self.strategies.running();
        if running {
            self.circuit_stopped = None;
            let Some((what, c, value)) = readings
                .into_iter()
                .find_map(|(w, c, v)| c.filter(|c| v >= c.level).map(|c| (w, c, v)))
            else {
                return;
            };
            self.stop_strategies();
            self.circuit_stopped = Some((what, c.restart_ms.map(|ms| now + ms)));
            let mut fx = Effects::default();
            fx.logs.push(format!(
                "AutoStop: {what} {value} ≥ {}: strategies stopped{}",
                c.level,
                if c.sell_all { ", Panic Sell ALL" } else { "" }
            ));
            if c.sell_all {
                fx.extend(self.orders.panic_all(&self.catalog, now));
            }
            self.effects(fx, now);
            return;
        }
        let Some((what, Some(at))) = self.circuit_stopped else {
            return;
        };
        let calm = readings
            .iter()
            .all(|(_, c, v)| c.is_none_or(|c| *v < c.level));
        // A market move past its panic threshold keeps them stopped.
        if now < at || !calm || self.market_latched {
            return;
        }
        self.circuit_stopped = None;
        self.strategies.set_running(true);
        self.outbox
            .push((STRAT, strat::runtime_state(rand_uid(), true)));
        let text = format!("AutoStart: {what} back below the level: strategies started");
        self.tg(crate::telegram::Kind::Alarm, format!("▶️ {text}"));
        log::info!("{text}");
        self.outbox.push((LOG, log_msg(now, &text)));
    }

    /// Global panic on a market move and its «Restart if»: BTC's hourly
    /// delta and the markets' mean one.
    fn check_market(&mut self, now: i64) {
        let running = self.strategies.running();
        if running {
            self.market_stopped = false;
        }
        // Before the warm-up seeds the windows the hour is only the ticks
        // since the start: its delta would read about 0 %.
        if !self.warmup_done || (!self.auto_stop.watches_market() && !self.market_stopped) {
            return;
        }
        let btc = self
            .catalog
            .index_of_symbol(BTC_SYMBOL)
            .and_then(|i| Some((i, self.catalog.at(i)?)))
            .filter(|(i, m)| m.fresh() && m.last() > 0.0 && self.windows.covers(*i, now, 60))
            .and_then(|(i, m)| self.windows.delta(i, m.last(), now, 60));
        let deltas = autostop::Deltas {
            btc,
            market: self.market_delta.0,
        };
        if autostop::market_past(&self.auto_stop, deltas).is_none() {
            self.market_latched = false;
        }
        let call = autostop::market_call(
            &self.auto_stop,
            deltas,
            running,
            self.market_stopped,
            self.market_latched,
        );
        let mut fx = Effects::default();
        match call {
            Some(autostop::MarketCall::Panic(why)) => {
                self.market_latched = true;
                // Stopped by a breaker meanwhile: the market decides now.
                if !running && self.circuit_stopped.take().is_some() {
                    self.market_stopped = true;
                }
                if self.stop_strategies() {
                    self.market_stopped = true;
                }
                fx.logs.push(format!(
                    "AutoStop: {why}: {}Panic Sell ALL",
                    if running { "strategies stopped, " } else { "" }
                ));
                fx.extend(self.orders.panic_all(&self.catalog, now));
            }
            Some(autostop::MarketCall::Restart(why)) => {
                self.strategies.set_running(true);
                self.market_stopped = false;
                self.outbox
                    .push((STRAT, strat::runtime_state(rand_uid(), true)));
                fx.logs
                    .push(format!("AutoStart: {why}: strategies started"));
            }
            None => return,
        }
        self.effects(fx, now);
    }

    /// A user-stream report of an execution: its commission (`n`, in the
    /// asset `N`) is summed per exchange order — the deal's row takes it when
    /// it closes, and again if one arrives after.
    fn note_commission(&mut self, o: &crate::aster::json::OrderEvent) {
        if o.execution != "TRADE" {
            return;
        }
        let Ok(fee) = o.commission.parse::<f64>() else {
            return;
        };
        // A zero fee is an answer too: the deal waits for every one of them.
        if !fee.is_finite() {
            return;
        }
        if !o.commission_asset.is_empty() && o.commission_asset != QUOTE {
            log::warn!(
                "order {} {}: commission {fee} {} is not in {QUOTE}, not booked",
                o.symbol,
                o.id,
                o.commission_asset
            );
            return;
        }
        if self.commissions.len() >= COMMISSIONS_KEPT {
            // Keep the fees of the orders still live; the rest belong to deals
            // the report already holds. A fee of one of those arriving after
            // this would book that exchange order's later part alone — a rare
            // error against unbounded growth.
            // And of the deals still on their way to the chat, which wait
            // for exactly these fees.
            let waiting: HashSet<u64> = self
                .ops
                .deal_notes
                .iter()
                .filter_map(|n| self.reports.row(n.rec_id).map(|r| r.task_id))
                .collect();
            let live: HashSet<(String, String)> = self
                .orders
                .iter()
                .filter(|o| !trade::status::is_terminal(o.status) || waiting.contains(&o.id))
                .flat_map(|o| {
                    let symbol = o.uid.clone();
                    o.filled_orders()
                        .into_iter()
                        .map(move |f| (symbol.clone(), f.id))
                })
                .collect();
            let before = self.commissions.len();
            self.commissions.retain(|k, _| live.contains(k));
            log::warn!(
                "commissions: {before} orders kept, {} of live orders remain",
                self.commissions.len()
            );
        }
        let id = o.id.to_string();
        let entry = self
            .commissions
            .entry((o.symbol.clone(), id.clone()))
            .or_default();
        if !entry.1.insert(o.trade_id) {
            return;
        }
        entry.0 += fee;
        // A deal already closed takes the late fee now.
        let order = self
            .orders
            .iter()
            .find(|c| c.uid == o.symbol && c.filled_orders().iter().any(|f| f.id == id))
            .map(|c| c.id);
        if let Some(order) = order.filter(|&order| self.reports.has_order(order)) {
            self.book_one_commission(order, &o.symbol, &id, now_ms());
        }
    }

    /// The commissions the user stream reported for every exchange order of
    /// the closed deal `order`.
    fn book_commissions(&mut self, order: u64, now: i64) {
        let Some((symbol, ids)) = self.orders.get(order).map(|o| {
            let ids: Vec<String> = o.filled_orders().into_iter().map(|f| f.id).collect();
            (o.uid.clone(), ids)
        }) else {
            return;
        };
        for id in ids {
            self.book_one_commission(order, &symbol, &id, now);
        }
    }

    fn book_one_commission(&mut self, order: u64, symbol: &str, exchange_id: &str, now: i64) {
        let key = (symbol.to_string(), exchange_id.to_string());
        let Some(&(fee, _)) = self.commissions.get(&key) else {
            return;
        };
        let Some(row) = self.reports.add_commission(order, exchange_id, fee) else {
            return;
        };
        self.outbox.push((ORDER, row_upsert(row)));
        self.penalty_marks = None;
        let row = row.clone();
        let rules = self.guard_rules();
        let booked = (!row.estimated).then(|| self.guards.book(&row, rule_of(&rules, &row), now));
        if let Some(text) = booked.flatten() {
            log::info!("{text}");
            self.outbox.push((LOG, log_msg(now, &text)));
        }
        self.push_profit(now);
        // A late fee can carry the loss over the limit.
        self.check_auto_stop(now);
    }

    fn cap_to_band(&mut self, fx: &mut Effects) {
        for a in &mut fx.actions {
            let (order, leg, price) = match a {
                Action::Post {
                    order,
                    leg,
                    price: Some(price),
                    ..
                }
                | Action::Replace {
                    order, leg, price, ..
                } => (*order, *leg, price),
                _ => continue,
            };
            let Some(m) = self
                .orders
                .get(order)
                .and_then(|o| self.catalog.get(&o.uid))
            else {
                continue;
            };
            let capped = m.within_limits(*price);
            if capped != *price {
                let (down, up) = m.band().unwrap_or_default();
                fx.logs.push(format!(
                    "{}: price {price} is outside the exchange band {down}..{up}, placed at {capped}",
                    m.symbol
                ));
                *price = capped;
                if self.orders.reprice(order, leg, capped) && !fx.changed.contains(&order) {
                    fx.changed.push(order);
                }
            }
        }
    }

    /// Hand the queued exchange work to the order worker. A Post or Replace
    /// carries a new request key: the order store is written first, once for
    /// the whole batch, so the key is on disk before the exchange can know it.
    fn flush_actions(&mut self, now: i64) {
        if self.pending_actions.is_empty() {
            return;
        }
        let keyed = |a: &Action| matches!(a, Action::Post { .. } | Action::Replace { .. });
        // A new key not on disk is a request a restart could not recognise:
        // it does not leave.
        let stored =
            !self.pending_actions.iter().any(|(a, _)| keyed(a)) || self.persist_orders(true, now);
        let mut refused = Effects::default();
        for (action, uid) in std::mem::take(&mut self.pending_actions) {
            let grid = self.catalog.get(&uid).map(|m| Grid {
                step: m.step_size,
                tick: m.tick_size,
            });
            let why = match (&self.trading, grid) {
                // An exit still goes: a position without its exit is worse than
                // an exit a restart would take for a foreign one (`adopt`).
                _ if !stored && keyed(&action) && action_leg(&action) == Leg::Buy => {
                    "the order store could not be written, the entry is not sent".to_string()
                }
                (Some(tx), Some(grid)) => {
                    match tx.send(TradeCommand::Exchange { action, uid, grid }) {
                        Ok(()) => continue,
                        Err(e) => {
                            let TradeCommand::Exchange { action, .. } = e.0 else {
                                continue;
                            };
                            refused.extend(self.orders.failed(
                                &action,
                                true,
                                "the order worker is gone",
                                now,
                            ));
                            continue;
                        }
                    }
                }
                (None, _) => "trading is off".to_string(),
                (_, None) => format!("{uid}: not in the catalog"),
            };
            // Told to the model as a refusal, so the leg does not wait for an
            // answer that is never coming.
            log::warn!("{why}: {action:?}");
            refused.extend(self.orders.failed(&action, true, &why, now));
        }
        if !refused.changed.is_empty() || !refused.logs.is_empty() || !refused.actions.is_empty() {
            self.effects(refused, now);
        }
    }

    /// Snapshot the live orders (`force`: now, before an order request);
    /// `false` when the snapshot is not on disk.
    fn persist_orders(&mut self, force: bool, now: i64) -> bool {
        let Some(store) = &mut self.order_store else {
            return true;
        };
        {
            let state = order_store::State {
                running: self.strategies.running(),
                client_settings: &self.client_settings,
                settings_at: self.settings_at,
                loss_since: self.loss_since,
                market_stopped: self.market_stopped,
            };
            store.save(&self.orders, &state, force, now)
        }
    }

    fn on_balance(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(hdr) = BaseHeader::parse(payload) else {
            return;
        };
        if matches!(
            hdr.cmd_id,
            balance::CMD_REQUEST_REFRESH | balance::CMD_DIGEST
        ) {
            // Answering at all matters: the client asks once per init and
            // again on every digest mismatch, and silence leaves it asking.
            let resp = self.balance_payload(hdr.uid);
            session.send_encrypted(BALANCE, &resp, true);
        }
    }

    /// `TBalanceFull` of the last account read: free, locked and equity in
    /// USDT, a row per open position. A market missing from the rows is flat —
    /// the client resets every market a full snapshot leaves out.
    ///
    /// Without a key, or with the reads failing, the money is unknown, and the
    /// empty snapshot is how that is spelled on this wire — there is no "unknown" for a balance: the
    /// terminal draws an empty Assets panel, the right picture for a core that
    /// cannot trade.
    fn balance_payload(&self, uid: u64) -> Vec<u8> {
        let Some(a) = &self.account else {
            return balance::balance_full(uid, self.balance_epoch, 0.0, 0.0, 0.0, &[]);
        };
        let items: Vec<balance::BalanceItem> = a
            .positions
            .iter()
            .map(|p| balance::BalanceItem {
                market: &p.symbol,
                pos_size: p.size,
                pos_price: p.entry,
                ..balance::BalanceItem::default()
            })
            .collect();
        balance::balance_full(
            uid,
            self.balance_epoch,
            a.free,
            a.locked(),
            a.equity,
            &items,
        )
    }
}

impl Handler for CoreHandler {
    fn on_connected(&mut self, session: &mut Session) {
        log::info!("client {:#x} connected", session.client_id());
        session.send_encrypted(UI, &ui::runtime_state(rand_uid(), true, false), true);
        // A core pushes its Telegram state on connect (moonproto
        // `docs/telegram.md`); the terminal asks for it only on a reconnect.
        session.send_encrypted(
            UI,
            &ui::telegram_state(rand_uid(), TELEGRAM_UNSUPPORTED),
            true,
        );
        let running = self.strategies.running();
        session.send_encrypted(STRAT, &strat::runtime_state(rand_uid(), running), true);
    }

    fn on_command(&mut self, session: &mut Session, cmd: u8, payload: &[u8]) {
        match Command::from_byte(cmd).to_byte() {
            API => self.on_api(session, payload),
            STRAT => self.on_strat(session, payload),
            UI => self.on_ui(session, payload),
            ORDER => self.on_order(session, payload),
            BALANCE => self.on_balance(session, payload),
            _ => log::debug!(
                "cmd {} ignored, {} bytes",
                Command::from_byte(cmd).name(),
                payload.len()
            ),
        }
    }

    fn on_closed(&mut self, client_id: u64) {
        log::info!("client {client_id:#x} closed");
        // Its subscriptions go with it: a book nobody shows is a session the
        // feed keeps open for nothing.
        self.trade_subs.remove(&client_id);
        self.book_subs.remove(&client_id);
        self.candle_subs.remove(&client_id);
        self.candles_pending.remove(&client_id);
        self.sync_feed_subscriptions();
    }
}

/// Unix milliseconds, UTC — the core's one clock.
///
/// Aster's own `timezone` is UTC and every timestamp on the wire to it is
/// unix milliseconds. The one local clock is the trader's, which MoonBot's work
/// windows are read on (`moonshot::WorkWindow`).
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A uid for a command the core sends unsolicited. Ported from TInvestCore:
/// the clock keeps the low bits, a counter the high ones, so two calls in one
/// nanosecond still differ.
fn rand_uid() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos() as u64);
    (nanos & 0x0000_ffff_ffff_ffff) | (SEQ.fetch_add(1, Ordering::Relaxed) << 48)
}

/// The Sessions rule a report row counts under: its strategy's, while the
/// strategy trades in the mode the row was made in.
fn rule_of(rules: &GuardRules, row: &reports::Row) -> Option<guards::SessionRule> {
    rules
        .get(&row.strategy_id)
        .filter(|(emulated, _)| *emulated == row.emulator)
        .and_then(|&(_, rule)| rule)
}

/// Report row of `o` (its image `record`) for `Reports::record`.
fn deal<'a>(
    o: &'a CoreOrder,
    record: &'a trade::OrderRecord<'a>,
    signal_type: Option<&'a str>,
) -> Deal<'a> {
    Deal {
        order_id: o.id,
        coin: &o.market,
        is_short: o.is_short,
        strategy_id: o.strategy_id,
        signal_type,
        buy: &record.buy,
        sell: &record.sell,
        closed: trade::status::is_terminal(o.status),
        panic: record.panic,
        sell_reason: record.sell_reason,
        exit: o.exit_source(),
        ex_order_id: o.exchange_id(),
        emulator: o.emulator,
    }
}

fn row_upsert(row: &reports::Row) -> Vec<u8> {
    let mut encoded = Vec::new();
    row.encode(&mut encoded);
    report::row_upsert(rand_uid(), row.rec_id, &encoded)
}

fn profit_state(p: &Profit) -> Vec<u8> {
    ui::profit_state(rand_uid(), p.total, p.trades, p.hour_total, p.hour_trades)
}

/// The leg an action works on.
fn action_leg(a: &Action) -> Leg {
    match a {
        Action::Post { leg, .. }
        | Action::Cancel { leg, .. }
        | Action::Replace { leg, .. }
        | Action::Query { leg, .. }
        | Action::QueryRequest { leg, .. } => *leg,
    }
}

#[cfg(test)]
mod snapshot_timer_tests {
    use super::*;

    #[test]
    fn the_snapshot_is_due_every_period_and_when_the_clock_steps_back() {
        let last = 1_000_000;
        assert!(!snapshot_due(last, last));
        assert!(!snapshot_due(last + ORDERS_SNAPSHOT_EVERY_MS - 1, last));
        assert!(snapshot_due(last + ORDERS_SNAPSHOT_EVERY_MS, last));
        assert!(snapshot_due(last - 1, last), "a clock that stepped back");
        assert!(
            snapshot_due(1_790_000_000_000, 0),
            "the first one after start"
        );
    }
}

#[cfg(test)]
mod pass_timer_tests {
    use super::*;

    #[test]
    fn the_pass_runs_fast_only_while_a_strategy_order_is_live() {
        let live = || true;
        let idle_book = || false;
        // Nothing live: the second's period, as before.
        assert!(!pass_due(999, 700, false, idle_book));
        assert!(pass_due(1_000, 700, false, idle_book));
        // A live order: every 100 ms, not sooner.
        assert!(!pass_due(99, 700, false, live));
        assert!(pass_due(100, 700, false, live));
    }

    #[test]
    fn a_dear_pass_is_not_started_again_before_its_duty_is_kept() {
        // A 30 ms pass (a wide pool): the next not before 600 ms, live order or not.
        assert!(!pass_due(100, 30_000, false, || true));
        assert!(!pass_due(599, 30_000, false, || true));
        assert!(pass_due(600, 30_000, false, || true));
        // The slow period still comes whatever it cost.
        assert!(pass_due(1_000, 90_000, false, || false));
    }

    /// An order report or a strike trade wakes the pass at once — but a burst of them cannot
    /// run passes back to back: the duty holds for the wake-up as well.
    #[test]
    fn a_wake_up_runs_at_once_but_keeps_the_duty() {
        assert!(pass_due(0, 0, true, || false));
        assert!(pass_due(2, 100, true, || false));
        assert!(!pass_due(2, 700, true, || false));
        assert!(pass_due(14, 700, true, || false));
        assert!(!pass_due(0, 0, false, || true));
    }

    #[test]
    fn the_order_scan_is_skipped_when_it_cannot_matter() {
        let scanned = std::cell::Cell::new(false);
        assert!(!pass_due(50, 0, false, || {
            scanned.set(true);
            true
        }));
        assert!(!scanned.get());
    }
}

#[cfg(test)]
mod shots_cost_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_cost_is_reported_once_per_interval_and_counted_afresh() {
        let mut c = ShotsCost::default();
        let (ms, base) = (Duration::from_millis, Instant::now());
        let at = |s: u64| base + Duration::from_secs(s);
        let period = SUMMARY_EVERY_MS as u64 / 1000;
        assert!(c.record(ms(2), at(1)).is_none());
        assert!(c.record(ms(6), at(2)).is_none());
        let line = c.record(ms(4), at(1 + period)).expect("the interval is up");
        assert_eq!(line, "shots: 3 passes in 300 s, avg 4.00 ms, max 6.00 ms");
        // The next interval starts from this pass, with nothing carried over.
        assert!(c.record(ms(1), at(2 + period)).is_none());
        let line = c.record(ms(1), at(1 + 2 * period)).unwrap();
        assert_eq!(line, "shots: 2 passes in 300 s, avg 1.00 ms, max 1.00 ms");
    }

    /// An instant earlier than the start (`Instant` cannot step, but the argument can lag) is no
    /// negative period: nothing reported, nothing panics.
    #[test]
    fn an_earlier_instant_is_no_negative_period() {
        let mut c = ShotsCost::default();
        let base = Instant::now() + Duration::from_secs(10);
        assert!(c.record(Duration::from_millis(1), base).is_none());
        assert!(c
            .record(Duration::from_millis(1), base - Duration::from_secs(5))
            .is_none());
    }
}
