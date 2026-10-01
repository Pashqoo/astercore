//! MoonProto `Handler`: the Init spine the terminal has to walk before it is
//! `Ready`, and the resync it asks for right after.
//!
//! The spine is not ours to choose. `moonproto`'s own client fails init unless
//! BaseCheck, AuthCheck, `GetMarketsList`, `UpdateMarketsList` and the strategy
//! schema all answer (`client::init::steps`), and it then sends an order
//! snapshot request, a settings request, its strategy list and a balance
//! refresh whose replies are *not* part of the barrier. So this file answers
//! the first group with real data, the balance with the account when the core
//! has a key (`account.rs`), and the rest with the honest empty answer of a
//! core that does not trade yet (M2).
//!
//! Adapted from TInvestCore's `engine.rs`, which is the same spine over 6000
//! lines of trading on top. Market data (M1) is here: the tape, the books and
//! the candles the terminal subscribes to, fed from `feed.rs`. What is NOT here
//! is deliberate, not forgotten: orders (M2), strategies (M3). Every Engine API
//! method outside what is implemented answers with a refusal naming itself,
//! which is how the terminal shows a missing feature instead of waiting out a
//! timeout.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Instant;

use moonproto::server::codec::engine::{self, EngineMethod, EngineRequest, ServerInfo};
use moonproto::server::codec::log::log_msg;
use moonproto::server::codec::market_data::{self, DeepHistoryKind, BOOK_KIND_FUTURES};
use moonproto::server::codec::trade::{OrderCommand, StartOrder};
use moonproto::server::codec::{balance, strat, trade, ui, BaseHeader, BASE_HEADER_SIZE};
use moonproto::server::{Command, Handler, Session};

use crate::account::Account;
use crate::aster::ws::Stamp;
use crate::book::{LocalBook, Out};
use crate::candles5m::Candles5m;
use crate::feed::{FeedCommand, FeedEvent};
use crate::load::Load;
use crate::model::{Catalog, QUOTE, QUOTE_CODE};
use crate::order_store::{self, OrderStore};
use crate::orders::{Action, Effects, Leg, Orders};
use crate::prices::Snapshot;
use crate::strategies::Strategies;
use crate::stream_health::{Scope, StreamHealth, SUMMARY_EVERY_MS};
use crate::trades_stream::TradesStream;
use crate::trading::{Grid, OrderUpdate, TradeCommand, TradingEvent};

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
    /// Exchange work of the last effects, sent by `pump` after one snapshot
    /// of the whole batch (`flush_actions`), with the market it is on.
    pending_actions: Vec<(Action, String)>,
    /// Messages for every session (order images, log lines), sent by `pump`.
    outbox: Vec<(u8, Vec<u8>)>,
    /// When `Orders::watch` last ran.
    watch_at: i64,
    /// When to read the account's open orders (`TradeCommand::OpenOrders`).
    open_orders_due: Option<i64>,
}

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
            orders: Orders::new(),
            trading: None,
            order_store: None,
            pending_actions: Vec::new(),
            outbox: Vec::new(),
            watch_at: 0,
            open_orders_due: None,
        }
    }

    /// Start from the account `main` read at startup, so the first terminal
    /// is answered with it rather than with an empty wallet for a period.
    pub fn with_account(mut self, account: Account) -> Self {
        self.account = Some(account);
        self
    }

    /// Trade on the account: the order worker (`trading::start`) and the order
    /// store, with the orders the previous run left there.
    pub fn with_trading(
        mut self,
        tx: Sender<TradeCommand>,
        store: OrderStore,
        saved: order_store::Saved,
    ) -> Self {
        // Ids above every restored one, so a new order never takes an old id.
        self.orders = Orders::starting_at(now_ms() as u64);
        let restored = self.orders.restore(saved.orders);
        self.orders.restore_left(saved.left);
        let (refreshed, missing) = self.orders.respec(|uid| self.catalog.get(uid));
        if restored > 0 {
            log::info!(
                "orders: {restored} restored, {refreshed} on today's catalog, {missing} on a market it lacks"
            );
        }
        if !saved.client_settings.is_empty() {
            self.client_settings = saved.client_settings;
        }
        // What the exchange holds now: the restored orders are read against
        // it before any terminal acts on them.
        let _ = tx.send(TradeCommand::OpenOrders);
        self.trading = Some(tx);
        self.order_store = Some(store);
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
        if self.open_orders_due.is_some_and(|due| now >= due) {
            self.open_orders_due = None;
            if let Some(tx) = &self.trading {
                let _ = tx.send(TradeCommand::OpenOrders);
            }
        }
        self.flush_actions(now);
        self.persist_orders(false, now);
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
                }
            }
            FeedEvent::Warmup { symbol, bars } => {
                if let Some(idx) = self.catalog.index_of_symbol(&symbol) {
                    for b in &bars {
                        self.candles
                            .seed(idx, b.open_ms, b.low, b.high, b.quote_volume);
                    }
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
                let step = self.catalog.get(&o.symbol).map_or(0.0, |m| m.step_size);
                match OrderUpdate::from_event(&o, step) {
                    Some(u) => self.on_trading(TradingEvent::Order(u)),
                    None if o.status.starts_with("NEW_") => {}
                    None => log::warn!("order: unreadable report of {} {}", o.symbol, o.id),
                }
            }
            // Read once the session is likely subscribed — its handshake
            // measured 0.83 s: a read made before it leaves a window that
            // neither the read nor the stream covers. Retries in between fold
            // into the one read.
            FeedEvent::UserStreamOpen => {
                if self.open_orders_due.is_none() {
                    self.open_orders_due = Some(now_ms() + OPEN_ORDERS_AFTER_MS);
                }
            }
            FeedEvent::Account(account) => {
                // Positions that left the account outside the core close
                // their orders (`Orders::reconcile`). An unknown account
                // closes nothing.
                if let Some(a) = &account {
                    let now = now_ms();
                    let fx = self.orders.reconcile(&Self::held(a), &self.catalog, now);
                    self.effects(fx, now);
                }
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
            EngineMethod::QueryHedgeMode => engine::write_hedge_mode(false),
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
                self.client_settings = payload.to_vec();
                session.send_encrypted(UI, &ui::with_uid(payload, rand_uid()), true);
            }
            ui::CMD_SETTINGS_REQUEST => {
                let resp = ui::with_uid(&self.client_settings, hdr.uid);
                session.send_encrypted(UI, &resp, true);
                // TInvestCore follows this echo with `profit_state`, the deal
                // counters shown beside the auto-stop caps. Deliberately not
                // sent here: this core has no deals and no report store (M2),
                // and a zeroed counter is a claim about trading that has not
                // happened. It arrives with the reports.
            }
            ui::CMD_SHARED_CONFIG => {
                if let Some(blob) = ui::shared_config_blob(payload) {
                    self.shared_config = blob.to_vec();
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
                self.strategies.set_running(start);
                // The per-strategy checkboxes that rode the command. Nothing
                // here can honour them — there is no strategy list to check off
                // and nothing to run (M3) — so they are named in the journal
                // rather than dropped in silence.
                if !items.is_empty() {
                    log::info!(
                        "start/stop carried {} checked flag(s); no strategy list \
                         to apply them to yet (M3)",
                        items.len()
                    );
                }
                // The button must not quietly lie. The core keeps the flag
                // because the terminal's own button follows it, and says in the
                // terminal's log what the flag does and does not mean while the
                // strategy engines are not ported.
                if start {
                    log::warn!("start requested: no strategy engine yet (M3), nothing is entered");
                    session.send_encrypted(
                        LOG,
                        &log_msg(
                            now_ms(),
                            "Astercore M0: strategies are not implemented yet \
                             — nothing will be entered",
                        ),
                        true,
                    );
                }
                // To the sender, which at M0 is the only client there is to
                // tell. TInvestCore broadcasts this through an outbox every
                // session drains, so a second terminal's button follows the
                // first one's; that plumbing arrives with the state worth
                // converging on (the strategy list, M3). With two terminals
                // open now the second one's button lags — written down because
                // a known divergence is worth more than a silent one.
                session.send_encrypted(STRAT, &strat::runtime_state(rand_uid(), start), true);
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

    fn on_strat(&mut self, session: &mut Session, payload: &[u8]) {
        let Some(hdr) = BaseHeader::parse(payload) else {
            return;
        };
        let body = &payload[BASE_HEADER_SIZE..];
        match hdr.cmd_id {
            // The mandatory Init step: without this answer the client never
            // reaches `Ready`.
            strat::CMD_SCHEMA_REQUEST => {
                let resp = strat::schema_payload(hdr.uid, self.strategies.schema_blob());
                session.send_encrypted(STRAT, &resp, true);
            }
            strat::CMD_SNAPSHOT => {
                if let Some(snap) = strat::parse_snapshot(body) {
                    self.strategies.store(snap);
                }
            }
            other => log::debug!("Strat cmd {other} ignored until M3"),
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
            other => {
                log::debug!("Order cmd {other} ignored");
                return;
            }
        };
        session.send_encrypted(ORDER, &resp, true);
    }

    /// `TOrderCommand` from the terminal; results go out through `effects`.
    /// Ported from TInvestCore without what this core does not have yet: the
    /// manual strategy and the toolbar's stop and take profit (M3), MoveAll,
    /// the emulator.
    fn on_order_command(&mut self, req_uid: u64, body: &[u8]) {
        let now = now_ms();
        let fx = match OrderCommand::parse(body) {
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
            OrderCommand::Stops {
                order_id,
                sl_on,
                sl_fixed,
                sl_level,
                sl_spread,
                trail_on,
                trail_fixed,
                trail_level,
                trail_spread,
                tp_on,
                tp,
            } => {
                let mut fx = self
                    .orders
                    .set_stops(order_id, sl_on, sl_fixed, sl_level, sl_spread);
                fx.extend(self.orders.set_trailing(
                    order_id,
                    trail_on,
                    trail_fixed,
                    trail_level,
                    trail_spread,
                    tp_on,
                    tp,
                ));
                fx
            }
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
                match self.tradable(&market) {
                    Ok(()) => {
                        let m = self.catalog.get(&market).expect("tradable");
                        self.orders.close_position(m, false, side, at_market, now)
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
            OrderCommand::MoveAll { market, .. } => Effects {
                logs: vec![format!(
                    "{market}: Move all is not supported by this core yet"
                )],
                ..Effects::default()
            },
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
        if let Err(reason) = self.tradable(&s.market) {
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
        if pending {
            self.orders.start_pending(req_uid, s, m, now)
        } else {
            self.orders.start(req_uid, s, m, now)
        }
    }

    /// The core may send exchange orders for `market`.
    fn tradable(&self, market: &str) -> Result<(), String> {
        if self.trading.is_none() {
            return Err("trading is off: the core runs without an account key".into());
        }
        match self.catalog.get(market) {
            Some(m) if m.trading => Ok(()),
            Some(_) => Err(format!("{market}: the market is not trading")),
            None => Err(format!("{market}: unknown market")),
        }
    }

    /// An outcome of the exchange, from the worker or the user-data stream.
    fn on_trading(&mut self, ev: TradingEvent) {
        let now = now_ms();
        let fx = match ev {
            TradingEvent::Order(u) => self.order_update(&u, now),
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
            } => self.orders.failed(&action, definitive, &msg, now),
            TradingEvent::Ping(ms) => {
                log::debug!("orders: call answered in {ms} ms");
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
        for id in fx.changed {
            if let Some(o) = self.orders.get(id) {
                self.outbox.push((ORDER, trade::order_image(&o.record())));
            }
        }
        for text in fx.logs {
            log::info!("{text}");
            self.outbox.push((LOG, log_msg(now, &text)));
        }
        for action in fx.actions {
            if let Some(o) = self.orders.get(action.order()) {
                let uid = o.uid.clone();
                self.pending_actions.push((action, uid));
            }
        }
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
                running: false,
                client_settings: &self.client_settings,
                settings_at: 0,
                loss_since: 0,
                market_stopped: false,
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
/// unix milliseconds, so unlike TInvestCore there is no Moscow midnight and no
/// local-time arithmetic anywhere in this core.
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
