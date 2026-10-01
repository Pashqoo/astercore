//! Aster's market data on its own threads: static `aggTrade` sessions over
//! the whole catalog, one `!markPrice@arr` session, chunked dynamic sessions
//! for the books and candles the terminal follows (`subs`), a serial worker
//! for the on-demand REST calls (chart history, the tape of the last hour),
//! another for the books' snapshots (`depth_worker`), and the startup warm-up of the screener's 5m candles on threads of
//! its own (`spawn_warmup`). Talks to the UDP loop only through channels;
//! knows symbols, not market indexes.
//!
//! The shape is TInvestCore's `feed.rs`; what changed is the venue under it.
//! A chunk there was a grpc-web stream of 300 instruments, here it is a
//! WebSocket session of at most 200 streams (`ws::MAX_STREAMS`, measured), so
//! the catalog's 596 `aggTrade` streams are three sessions. Trading statuses
//! have no stream at all on Aster — `status` rides `exchangeInfo` — so that
//! group is gone, and so is the MOEX history thread: deep history is the
//! exchange's own `klines`.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use moonproto::server::codec::market_data::{delphi_days, Candle, HistoryTrade};

use crate::aster::json::{AggTrade, Dec, Kline, PremiumIndex, StreamData, StreamEvent};
use crate::aster::rest::{self, AggFrom, Rest};
use crate::aster::ws::{self, Beat, MAX_STREAMS};
use crate::book;
use crate::load::Load;
use crate::stream_health::{Scope, StreamHealth};
use crate::subs::{Change, Plan};

/// Mark price and funding of the whole catalog, every 3 s, in one frame
/// (measured 01.10: 767 rows a frame, the same complete set `premiumIndex`
/// answers). The `@1s` variant was measured too and costs three times the
/// bytes for a funding pair that moves in hours.
const MARKS_STREAM: &str = "!markPrice@arr";
/// The mark stream pushes every 3 s, so it is dead long before the generic
/// [`ws::IDLE_LIMIT`]: five missed pushes, and the core reopens it rather
/// than serving cleared funding for a minute.
const MARKS_IDLE: Duration = Duration::from_secs(15);
/// The levels that changed, every 100 ms, stitched to a [`BOOK_LEVELS`]
/// snapshot by `book.rs` — MoonBot's own pair (`@depth@100ms`, `limit=1000`).
const BOOK_STREAM: &str = "depth@100ms";
/// The deepest `/fapi/v1/depth` answers (measured 01.10; 5000 is refused).
const BOOK_LEVELS: u32 = 1000;
/// Pause before a failed session reopens, doubling up to the cap; reset once a
/// session has lived [`HEALTHY_SESSION`].
const RECONNECT_MIN: Duration = Duration::from_secs(3);
const RECONNECT_MAX: Duration = Duration::from_secs(60);
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Pause after each REST call of the worker.
const UNARY_PACE: Duration = Duration::from_millis(100);
/// Bars of a CoinCard chart: one `klines` call at its maximum (weight 10,
/// measured). 25 hours of minutes, five days of 5m, four years of days.
const CARD_BARS: u32 = 1500;
/// `RequestMarketHistory`: the last hour of the tape.
const HISTORY_SPAN_MS: i64 = 3_600_000;
const HISTORY_PAGE: u32 = 1000;
/// Weight 20 each (measured), so the busiest hour costs at most 100.
const HISTORY_PAGES: usize = 5;
/// How long a subscription nobody wants any more stays open (`subs::Plan`).
/// TInvestCore's value, kept: the terminal switching between two charts
/// should not cost a reopen each way.
const SUBS_GRACE_MS: i64 = 120_000;
const SUBS_LINGER_CAP: usize = MAX_STREAMS;
/// The warm-up of the screener's 5m candles (`candles5m`): one `klines` call
/// per market at the most bars weight 2 buys (measured 01.10: 2 up to 499).
const WARMUP_BARS: u32 = 499;
/// Calls in flight. Measured 01.10: a `klines 5m 499` answers in 0.27–1.1 s
/// (median 0.51 s), so one at a time is five minutes over the catalog, four
/// are about seventy seconds — ~1000 weight a minute of the 2400 allowed
/// (measured live 01.10: 596 markets in 45 s, none failed).
const WARMUP_THREADS: usize = 4;
/// The warm-up yields to everything else above this much of the minute's
/// weight, as the gateway itself counts it (`x-mbx-used-weight-1m`): it waits
/// for the next minute rather than spend what the top-of-book refresher and
/// the terminal's chart requests need.
const WARMUP_WEIGHT_CEILING: i64 = 1_600;
/// Calls per market before it is left to the tape, and the pause between two
/// that failed for any reason but the rate limit.
const WARMUP_ATTEMPTS: u32 = 3;
const WARMUP_RETRY: Duration = Duration::from_secs(2);

/// MoonProto's candle timeframes as Aster spells them. All six are native
/// exchange intervals, so nothing is aggregated here.
const INTERVALS: [(i64, &str); 6] = [
    (1, "1m"),
    (5, "5m"),
    (30, "30m"),
    (60, "1h"),
    (240, "4h"),
    (1440, "1d"),
];

pub fn interval_of(minutes: i64) -> Option<&'static str> {
    INTERVALS
        .iter()
        .find(|(m, _)| *m == minutes)
        .map(|(_, i)| *i)
}

fn minutes_of(interval: &str) -> Option<i64> {
    INTERVALS
        .iter()
        .find(|(_, i)| *i == interval)
        .map(|(m, _)| *m)
}

pub enum FeedCommand {
    /// Books the terminal wants live (symbols); replaces the previous set.
    SetBooks(Vec<String>),
    /// Live candle subscriptions `(symbol, minutes)`; replaces the previous set.
    SetCandles(Vec<(String, i64)>),
    /// `GetCoinCardCandles` -> `FeedEvent::CandlesReply`.
    Candles {
        symbol: String,
        minutes: i64,
        client_id: u64,
        request_uid: u64,
    },
    /// `RequestMarketHistory` -> `FeedEvent::HistoryReply`.
    History {
        symbol: String,
        client_id: u64,
        request_uid: u64,
    },
    /// A book's REST snapshot -> `FeedEvent::BookSnapshot`.
    BookSnapshot(String),
}

pub enum FeedEvent {
    /// Signed base quantity (negative = the aggressor sold), exchange ms.
    Trade {
        symbol: String,
        price: f64,
        qty: f64,
        time_ms: i64,
    },
    /// One `depthUpdate` of a book the terminal shows.
    BookDiff { symbol: String, diff: book::Diff },
    /// The answer to `FeedCommand::BookSnapshot`.
    BookSnapshot {
        symbol: String,
        result: Result<book::Snapshot, String>,
    },
    /// A book session ended; these books must not stay live across it.
    BooksUnavailable(Vec<String>),
    /// Live bar, base volume.
    Candle {
        symbol: String,
        minutes: i64,
        candle: Candle,
    },
    /// One `!markPrice@arr` frame: the exchange's complete word on mark price
    /// and funding, as `Catalog::apply_premium_index` reads it.
    Marks(Vec<PremiumIndex>),
    CandlesReply {
        client_id: u64,
        request_uid: u64,
        result: Result<Vec<Candle>, String>,
    },
    HistoryReply {
        client_id: u64,
        request_uid: u64,
        result: Result<Vec<HistoryTrade>, String>,
    },
    /// A feed thread the core cannot do without has died (by panicking): the
    /// engine latches it and `main` leaves (`CoreHandler::feed_lost`).
    Lost(&'static str),
    /// A read of the account that differs from the last one sent, or `None`
    /// once reads have failed for `account::STALE_AFTER` (`account.rs`). Not
    /// market data, but it reaches the sessions by the same road.
    Account(Option<crate::account::Account>),
    /// One market's `klines 5m`, oldest first, the last one still in progress.
    Warmup { symbol: String, bars: Vec<Kline> },
    /// Every market of the warm-up was asked, whatever it answered: the
    /// screener's candles are as complete as they will get from history, and
    /// the held `RequestCandlesData` answers can go out.
    WarmupDone,
}

/// Spawn the feed: the trade sessions over `symbols` and the mark-price
/// session start at once, each watched in `health`; events go to `ev_tx`, the
/// returned sink takes commands from the UDP loop.
pub fn start(
    symbols: &[String],
    ev_tx: Sender<FeedEvent>,
    health: &mut StreamHealth,
    load: Arc<Load>,
) -> Sender<FeedCommand> {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let chunks: Vec<&[String]> = symbols.chunks(MAX_STREAMS).collect();
    // The static sessions open together; their rotations are spread over the
    // cycle so that no two go blind at the same moment.
    let statics = chunks.len() as i64 + 1;
    let spread = ws::STREAM_SESSION.as_millis() as i64 / statics;
    for (i, chunk) in chunks.iter().enumerate() {
        let name = format!("trades#{i}");
        let beat = health.watch(name.clone(), Scope::Markets(chunk.to_vec()));
        beat.rotate_at_phase(i as i64 * spread);
        let streams = chunk
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();
        spawn_stream(
            name,
            streams,
            ws::IDLE_LIMIT,
            Vec::new(),
            stop_flag(),
            beat,
            ev_tx.clone(),
        );
    }
    let beat = health.watch("marks", Scope::Marks);
    beat.rotate_at_phase((statics - 1) * spread);
    spawn_stream(
        "marks".into(),
        vec![MARKS_STREAM.into()],
        MARKS_IDLE,
        Vec::new(),
        stop_flag(),
        beat,
        ev_tx.clone(),
    );

    spawn_warmup(symbols.to_vec(), ev_tx.clone());

    let (unary_tx, unary_rx) = mpsc::channel();
    thread::Builder::new()
        .name("aster-unary".into())
        .spawn({
            let ev_tx = ev_tx.clone();
            move || unary_worker(unary_rx, ev_tx)
        })
        .expect("spawn");
    let (depth_tx, depth_rx) = mpsc::channel();
    thread::Builder::new()
        .name("aster-depth".into())
        .spawn({
            let ev_tx = ev_tx.clone();
            move || depth_worker(depth_rx, ev_tx)
        })
        .expect("spawn");
    thread::Builder::new()
        .name("aster-feed".into())
        .spawn(move || coordinator(cmd_rx, unary_tx, depth_tx, ev_tx, load))
        .expect("spawn");
    cmd_tx
}

fn stop_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// One kind of dynamic subscription: its chunk plan and the stop flag of each
/// chunk's live session.
struct Group<K> {
    name: &'static str,
    plan: Plan<K>,
    wanted: Vec<K>,
    stops: Vec<Option<Arc<AtomicBool>>>,
    stream: fn(&K) -> String,
    /// Books report their loss when a session ends on its own; candles do not
    /// need to — a live bar is applied only over loaded history.
    books: fn(&[K]) -> Vec<String>,
}

impl<K: Clone + Eq + std::hash::Hash> Group<K> {
    fn new(name: &'static str, stream: fn(&K) -> String, books: fn(&[K]) -> Vec<String>) -> Self {
        Self {
            name,
            plan: Plan::new(MAX_STREAMS, SUBS_GRACE_MS, SUBS_LINGER_CAP),
            wanted: Vec::new(),
            stops: Vec::new(),
            stream,
            books,
        }
    }

    /// Settle the plan at `now` (or only expire lingering keys) and reopen the
    /// chunks it changed.
    fn apply(&mut self, settle: bool, now: i64, ev_tx: &Sender<FeedEvent>) -> Change<K> {
        let change = if settle {
            self.plan.settle(&self.wanted, now)
        } else {
            self.plan.expire(now)
        };
        for &i in &change.chunks {
            if self.stops.len() <= i {
                self.stops.resize(i + 1, None);
            }
            if let Some(stop) = self.stops[i].take() {
                stop.store(true, Ordering::Relaxed);
            }
            let members = self.plan.chunk(i);
            if members.is_empty() {
                continue;
            }
            let stop = stop_flag();
            spawn_stream(
                format!("{}#{i}", self.name),
                members.iter().map(self.stream).collect(),
                ws::IDLE_LIMIT,
                (self.books)(members),
                Arc::clone(&stop),
                Beat::new(),
                ev_tx.clone(),
            );
            self.stops[i] = Some(stop);
        }
        change
    }
}

/// Routes commands: subscription sets go to their `subs::Plan`, which reopens
/// only the chunk sessions whose membership changed; REST work goes to the
/// unary worker, book snapshots to the depth worker. Books and candles are
/// planned apart, so a candle chart opened in the terminal never reopens a
/// book session. A book leaving the subscription is reported as
/// `BooksUnavailable` here; a chunk reopened for its other members keeps their
/// books on screen, and one that missed an event across the reopen is stitched
/// again (its `pu` chain breaks and `book.rs` asks for a snapshot).
fn coordinator(
    cmd_rx: Receiver<FeedCommand>,
    unary_tx: Sender<FeedCommand>,
    depth_tx: Sender<FeedCommand>,
    ev_tx: Sender<FeedEvent>,
    load: Arc<Load>,
) {
    let mut books = Group::new(
        "books",
        |s: &String| format!("{}@{BOOK_STREAM}", s.to_lowercase()),
        |s: &[String]| s.to_vec(),
    );
    let mut candles = Group::new(
        "candles",
        |(s, m): &(String, i64)| {
            // `SetCandles` only ever carries a timeframe `interval_of` knows
            // (the engine filters by `DeepHistoryKind`), so the fallback is
            // unreachable; it is a minute bar rather than a panic.
            format!(
                "{}@kline_{}",
                s.to_lowercase(),
                interval_of(*m).unwrap_or("1m")
            )
        },
        |_: &[(String, i64)]| Vec::new(),
    );
    let epoch = Instant::now();
    let now = || epoch.elapsed().as_millis() as i64;
    loop {
        let due = [books.plan.next_due(), candles.plan.next_due()]
            .into_iter()
            .flatten()
            .min();
        let first = match due {
            None => match cmd_rx.recv() {
                Ok(cmd) => Some(cmd),
                Err(_) => return,
            },
            Some(due) => {
                let wait = Duration::from_millis((due - now()).max(0) as u64);
                match cmd_rx.recv_timeout(wait) {
                    Ok(cmd) => Some(cmd),
                    Err(mpsc::RecvTimeoutError::Timeout) => None,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return,
                }
            }
        };
        let (mut set_books, mut set_candles) = (false, false);
        let mut worker_gone: Option<&'static str> = None;
        let mut take = |cmd: FeedCommand| match cmd {
            FeedCommand::SetBooks(symbols) => {
                // The snapshot worker learns the set at once, in the order the
                // UDP loop sent it: a snapshot asked after this set must be
                // judged by it, not by the one before (a book re-subscribed
                // while its old session lingers asks within the coalesce).
                if depth_tx
                    .send(FeedCommand::SetBooks(symbols.clone()))
                    .is_err()
                {
                    worker_gone = Some("the book snapshot worker");
                }
                books.wanted = symbols;
                set_books = true;
            }
            FeedCommand::SetCandles(subs) => {
                candles.wanted = subs;
                set_candles = true;
            }
            other => {
                // A worker is gone only by panicking. The request in hand is
                // refused rather than dropped — dropped, the terminal would
                // wait out its timeout — and the core is told at once.
                let (tx, name) = match other {
                    FeedCommand::BookSnapshot(_) => (&depth_tx, "the book snapshot worker"),
                    _ => (&unary_tx, "the REST worker"),
                };
                if let Err(mpsc::SendError(cmd)) = tx.send(other) {
                    refuse(cmd, &ev_tx);
                    worker_gone = Some(name);
                }
            }
        };
        if let Some(cmd) = first {
            take(cmd);
            // Coalesce a burst of set changes into one reopen.
            while let Ok(cmd) = cmd_rx.recv_timeout(Duration::from_millis(300)) {
                take(cmd);
            }
        }
        if let Some(name) = worker_gone {
            let _ = ev_tx.send(FeedEvent::Lost(name));
            return;
        }
        let t = now();
        let change = books.apply(set_books, t, &ev_tx);
        if !change.chunks.is_empty() {
            load.books_reopened(change.chunks.len());
        }
        if !change.dropped.is_empty() {
            let _ = ev_tx.send(FeedEvent::BooksUnavailable(change.dropped));
        }
        let change = candles.apply(set_candles, t, &ev_tx);
        if !change.chunks.is_empty() {
            load.candles_reopened(change.chunks.len());
        }
    }
}

/// A request the worker can no longer serve, answered as refused.
fn refuse(cmd: FeedCommand, tx: &Sender<FeedEvent>) {
    const WHY: &str = "market data worker is gone";
    let ev = match cmd {
        FeedCommand::Candles {
            client_id,
            request_uid,
            ..
        } => FeedEvent::CandlesReply {
            client_id,
            request_uid,
            result: Err(WHY.into()),
        },
        FeedCommand::History {
            client_id,
            request_uid,
            ..
        } => FeedEvent::HistoryReply {
            client_id,
            request_uid,
            result: Err(WHY.into()),
        },
        FeedCommand::BookSnapshot(symbol) => FeedEvent::BookSnapshot {
            symbol,
            result: Err(WHY.into()),
        },
        FeedCommand::SetBooks(_) | FeedCommand::SetCandles(_) => return,
    };
    let _ = tx.send(ev);
}

/// `name` (`trades#0`, `books#1`, `marks`) names the thread and its log lines.
/// `books`: the symbols whose books this session carries, reported lost when
/// it ends on its own.
fn spawn_stream(
    name: String,
    streams: Vec<String>,
    idle: Duration,
    books: Vec<String>,
    stop: Arc<AtomicBool>,
    beat: Beat,
    ev_tx: Sender<FeedEvent>,
) {
    thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            let mut backoff = RECONNECT_MIN;
            while !stop.load(Ordering::Relaxed) {
                let opened = Instant::now();
                let res = ws::guarded(|| {
                    ws::run(&streams, idle, &stop, &beat, |frame| {
                        // A replaced session's last frame must not undo the
                        // new one's snapshot.
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        // The receiver is gone only when the core is: so is
                        // the reason to keep this session open.
                        if !forward(frame, &ev_tx) {
                            stop.store(true, Ordering::Relaxed);
                        }
                    })
                });
                // A chunk replaced by the coordinator hands its books to the
                // new session, and so does a planned rotation (`Ok`): if an
                // event fell between the two sessions, the next one breaks the
                // book's `pu` chain and a snapshot re-stitches it within a
                // second (an event comes about every 150 ms even on a quiet
                // market). Only a session that FAILED loses them for a backoff.
                if res.is_err() && !books.is_empty() && !stop.load(Ordering::Relaxed) {
                    let _ = ev_tx.send(FeedEvent::BooksUnavailable(books.clone()));
                }
                if opened.elapsed() >= HEALTHY_SESSION {
                    backoff = RECONNECT_MIN;
                }
                match res {
                    Ok(()) => log::debug!("stream {name}: session ended, reopening"),
                    Err(e) => {
                        log::warn!("stream {name}: {e}; retry in {}s", backoff.as_secs());
                        pause(backoff, &stop);
                        backoff = (backoff * 2).min(RECONNECT_MAX);
                    }
                }
            }
        })
        .expect("spawn");
}

/// Sleep `d`, or less if `stop` is set meanwhile — a replaced chunk's thread
/// must not linger a minute in its backoff.
fn pause(d: Duration, stop: &AtomicBool) {
    let until = Instant::now() + d;
    while !stop.load(Ordering::Relaxed) {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        thread::sleep(left.min(Duration::from_millis(250)));
    }
}

/// One decoded frame to the UDP loop. `false` when the loop is gone.
fn forward(frame: StreamData, tx: &Sender<FeedEvent>) -> bool {
    let out = match frame {
        StreamData::Many(events) => {
            let marks: Vec<PremiumIndex> = events
                .into_iter()
                .filter_map(|e| match e {
                    StreamEvent::MarkPrice(m) => Some(m.into()),
                    _ => None,
                })
                .collect();
            if marks.is_empty() {
                return true;
            }
            FeedEvent::Marks(marks)
        }
        StreamData::One(StreamEvent::AggTrade(t)) => match trade_of(t) {
            Some(ev) => ev,
            None => return true,
        },
        StreamData::One(StreamEvent::Depth(d)) => FeedEvent::BookDiff {
            diff: book::Diff {
                first_id: d.first_id,
                last_id: d.last_id,
                prev_id: d.prev_id,
                bids: rows(&d.bids),
                asks: rows(&d.asks),
            },
            symbol: d.symbol,
        },
        StreamData::One(StreamEvent::Kline(k)) => {
            let Some(minutes) = minutes_of(&k.kline.interval) else {
                return true;
            };
            FeedEvent::Candle {
                symbol: k.symbol,
                minutes,
                candle: candle_of(&k.kline),
            }
        }
        // A lone mark-price event is one symbol, not the catalog's complete
        // word, and merging it as one would clear every other market. The
        // core never opens a per-symbol mark stream, so this is a stream it
        // did not ask for.
        StreamData::One(StreamEvent::MarkPrice(_) | StreamEvent::Other) => return true,
    };
    tx.send(out).is_ok()
}

/// A print the tape can carry: a positive price and a positive quantity.
/// Anything else is a decode default (`json::str_f64` reads garbage as 0), and
/// a zero-priced print on the terminal's chart is a spike to the axis.
fn trade_of(t: AggTrade) -> Option<FeedEvent> {
    (t.price > 0.0 && t.qty > 0.0).then(|| FeedEvent::Trade {
        price: t.price,
        qty: t.signed_qty(),
        time_ms: t.time_ms,
        symbol: t.symbol,
    })
}

fn rows(rows: &[[Dec; 2]]) -> Vec<(f64, f64)> {
    rows.iter().map(|[p, q]| (p.0, q.0)).collect()
}

fn candle_of(k: &Kline) -> Candle {
    Candle {
        open: k.open as f32,
        high: k.high as f32,
        low: k.low as f32,
        close: k.close as f32,
        volume: k.volume as f32,
        time: delphi_days(k.open_ms),
    }
}

fn unary_worker(rx: Receiver<FeedCommand>, tx: Sender<FeedEvent>) {
    let mut rest = Rest::new();
    while let Ok(cmd) = rx.recv() {
        let ev = match cmd {
            FeedCommand::Candles {
                symbol,
                minutes,
                client_id,
                request_uid,
            } => FeedEvent::CandlesReply {
                client_id,
                request_uid,
                result: card_candles(&mut rest, &symbol, minutes),
            },
            FeedCommand::History {
                symbol,
                client_id,
                request_uid,
            } => FeedEvent::HistoryReply {
                client_id,
                request_uid,
                result: last_hour(&mut rest, &symbol, rest::now_ms()).map_err(|e| e.to_string()),
            },
            FeedCommand::SetBooks(_)
            | FeedCommand::SetCandles(_)
            | FeedCommand::BookSnapshot(_) => continue,
        };
        if tx.send(ev).is_err() {
            return;
        }
        thread::sleep(UNARY_PACE);
    }
}

/// Book snapshots, one at a time: each is `limit=1000` at weight ~20, and the
/// UDP loop asks for one per book only when its chain breaks. Kept off the
/// unary worker so a burst of them (a chunk reopened) never queues a chart
/// behind it. The weight rules are the warm-up's: an answer past
/// [`WARMUP_WEIGHT_CEILING`] or a 429 waits for the next minute; a ban (418)
/// refuses every snapshot for five minutes, since each call made under it
/// extends it. While it waits, requests pile up: a book is queued once
/// however often it is asked, and one nobody shows any more by the time its
/// turn comes (`SetBooks`, forwarded by the coordinator) is refused rather
/// than fetched.
fn depth_worker(rx: Receiver<FeedCommand>, tx: Sender<FeedEvent>) {
    const BAN_PAUSE: Duration = Duration::from_secs(300);
    let mut rest = Rest::new();
    let mut banned_until: Option<Instant> = None;
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut wanted: HashSet<String> = HashSet::new();
    let take =
        |cmd: FeedCommand, queue: &mut VecDeque<String>, wanted: &mut HashSet<String>| match cmd {
            FeedCommand::SetBooks(s) => *wanted = s.into_iter().collect(),
            FeedCommand::BookSnapshot(s) if !queue.contains(&s) => queue.push_back(s),
            _ => {}
        };
    loop {
        if queue.is_empty() {
            match rx.recv() {
                Ok(cmd) => take(cmd, &mut queue, &mut wanted),
                Err(_) => return,
            }
        }
        while let Ok(cmd) = rx.try_recv() {
            take(cmd, &mut queue, &mut wanted);
        }
        let Some(symbol) = queue.pop_front() else {
            continue;
        };
        // Every request is answered, even one not fetched: the book that asked
        // waits for exactly one answer and asks again only after one.
        let result = if !wanted.contains(&symbol) {
            Err("no longer shown".to_string())
        } else if banned_until.is_some_and(|t| Instant::now() < t) {
            Err("banned (418): snapshots paused".to_string())
        } else {
            match rest.depth(&symbol, BOOK_LEVELS) {
                Ok(d) => {
                    if rest.usage().weight_1m.unwrap_or(0) >= WARMUP_WEIGHT_CEILING {
                        to_next_minute();
                    }
                    Ok(book::Snapshot {
                        last_id: d.last_id,
                        bids: rows(&d.bids),
                        asks: rows(&d.asks),
                    })
                }
                Err(e) => {
                    log::warn!("book {symbol}: snapshot failed: {e}");
                    match e {
                        rest::Error::Api { status: 418, .. } => {
                            banned_until = Some(Instant::now() + BAN_PAUSE);
                        }
                        rest::Error::Api { status: 429, .. } => to_next_minute(),
                        _ => {}
                    }
                    Err(e.to_string())
                }
            }
        };
        if tx.send(FeedEvent::BookSnapshot { symbol, result }).is_err() {
            return;
        }
    }
}

/// The warm-up: [`WARMUP_THREADS`] threads take the catalog's markets in
/// turn, one `klines 5m` each (`warm_one`). A market that fails every attempt
/// is named in the journal and left to the tape; a ban (418) ends the whole
/// warm-up at once, because every call made under a ban extends it.
/// `WarmupDone` goes out when the last thread ends, panicking included
/// (`Finish`): the engine holds the terminal's candle request until then, and
/// must not hold it forever.
fn spawn_warmup(symbols: Vec<String>, tx: Sender<FeedEvent>) {
    let symbols = Arc::new(symbols);
    let next = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let left = Arc::new(AtomicUsize::new(WARMUP_THREADS));
    let banned = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    for i in 0..WARMUP_THREADS {
        let finish = Finish {
            left: Arc::clone(&left),
            failed: Arc::clone(&failed),
            markets: symbols.len(),
            started,
            tx: tx.clone(),
        };
        let (symbols, next, banned) =
            (Arc::clone(&symbols), Arc::clone(&next), Arc::clone(&banned));
        thread::Builder::new()
            .name(format!("warmup#{i}"))
            .spawn(move || {
                let mut rest = Rest::new();
                while let Some(symbol) = symbols.get(next.fetch_add(1, Ordering::Relaxed)) {
                    if banned.load(Ordering::Relaxed) {
                        return;
                    }
                    match warm_one(&mut rest, symbol) {
                        Ok(bars) => {
                            let ev = FeedEvent::Warmup {
                                symbol: symbol.clone(),
                                bars,
                            };
                            if finish.tx.send(ev).is_err() {
                                return;
                            }
                        }
                        Err(e @ rest::Error::Api { status: 418, .. }) => {
                            if !banned.swap(true, Ordering::Relaxed) {
                                log::error!(
                                    "warmup {symbol}: {e}; banned, the rest is left to the tape"
                                );
                            }
                            return;
                        }
                        Err(e) => {
                            log::warn!("warmup {symbol}: {e}; left to the tape");
                            finish.failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
            .expect("spawn");
    }
}

/// One market's `klines 5m`, tried [`WARMUP_ATTEMPTS`] times. A rate-limit
/// refusal (429) waits for the next minute before the next try, any other
/// failure a moment; a ban (418) is not retried at all. An answer showing the
/// minute's weight past [`WARMUP_WEIGHT_CEILING`] waits for the next minute
/// too — read off that answer only, since a failed call leaves the last
/// figure behind.
fn warm_one(rest: &mut Rest, symbol: &str) -> Result<Vec<Kline>, rest::Error> {
    let mut attempt = 1;
    loop {
        match rest.klines(symbol, "5m", WARMUP_BARS) {
            Ok(bars) => {
                if rest.usage().weight_1m.unwrap_or(0) >= WARMUP_WEIGHT_CEILING {
                    to_next_minute();
                }
                return Ok(bars);
            }
            Err(e @ rest::Error::Api { status: 418, .. }) => return Err(e),
            Err(e) if attempt >= WARMUP_ATTEMPTS => return Err(e),
            Err(e) => {
                log::debug!("warmup {symbol}: {e}; attempt {attempt} of {WARMUP_ATTEMPTS}");
                if matches!(e, rest::Error::Api { status: 429, .. }) {
                    to_next_minute();
                } else {
                    thread::sleep(WARMUP_RETRY);
                }
                attempt += 1;
            }
        }
    }
}

/// To the next minute by the local clock, plus a second for the gateway's:
/// still over the ceiling then, the next answer says so and this waits again.
fn to_next_minute() {
    let into = rest::now_ms().rem_euclid(60_000);
    thread::sleep(Duration::from_millis((60_000 - into) as u64 + 1_000));
}

/// One warm-up thread's end; the last one to end reports the warm-up done.
struct Finish {
    left: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
    markets: usize,
    started: Instant,
    tx: Sender<FeedEvent>,
}

impl Drop for Finish {
    fn drop(&mut self) {
        if self.left.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        log::info!(
            "warmup: 5m candles asked for {} markets in {:.0} s, {} failed",
            self.markets,
            self.started.elapsed().as_secs_f64(),
            self.failed.load(Ordering::Relaxed)
        );
        let _ = self.tx.send(FeedEvent::WarmupDone);
    }
}

/// The CoinCard chart: the newest [`CARD_BARS`] bars of the asked timeframe.
fn card_candles(rest: &mut Rest, symbol: &str, minutes: i64) -> Result<Vec<Candle>, String> {
    let interval =
        interval_of(minutes).ok_or_else(|| format!("unsupported timeframe {minutes}m"))?;
    let bars = rest
        .klines(symbol, interval, CARD_BARS)
        .map_err(|e| e.to_string())?;
    Ok(bars.iter().map(candle_of).collect())
}

/// The last hour of `symbol`'s tape as the chart archive wants it.
///
/// Paged BACK from the newest page, never forward from the hour's start:
/// a forward walk that hit the page cap would hold the oldest minutes and
/// lose the newest, which are the ones a chart is opened to see. A failure
/// after the first page keeps what is in hand.
pub(crate) fn last_hour(
    rest: &mut Rest,
    symbol: &str,
    now: i64,
) -> Result<Vec<HistoryTrade>, rest::Error> {
    let start = now - HISTORY_SPAN_MS;
    let mut rows = rest.agg_trades(symbol, AggFrom::Latest, HISTORY_PAGE)?;
    for _ in 1..HISTORY_PAGES {
        let Some(first) = rows.first() else {
            break;
        };
        if first.time_ms < start || first.id <= 0 {
            break;
        }
        let first_id = first.id;
        let from = (first_id - i64::from(HISTORY_PAGE)).max(0);
        match rest.agg_trades(symbol, AggFrom::Id(from), HISTORY_PAGE) {
            Ok(mut older) => {
                older.retain(|t| t.id < first_id);
                if older.is_empty() {
                    break;
                }
                older.append(&mut rows);
                rows = older;
            }
            Err(e) => {
                log::warn!("history {symbol}: {e}; keeping {} trades", rows.len());
                break;
            }
        }
    }
    Ok(history_rows(&rows, start))
}

fn history_rows(rows: &[AggTrade], start: i64) -> Vec<HistoryTrade> {
    let mut out: Vec<HistoryTrade> = rows
        .iter()
        .filter(|t| t.time_ms >= start && t.price > 0.0 && t.qty > 0.0)
        .map(|t| HistoryTrade {
            time: delphi_days(t.time_ms),
            price: t.price as f32,
            qty: t.signed_qty() as f32,
        })
        .collect();
    out.sort_by(|a, b| a.time.total_cmp(&b.time));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(id: i64, time_ms: i64, price: f64, qty: f64, buyer_is_maker: bool) -> AggTrade {
        AggTrade {
            symbol: "BTCUSDT".into(),
            id,
            price,
            qty,
            time_ms,
            buyer_is_maker,
        }
    }

    #[test]
    fn every_moonproto_timeframe_is_a_native_interval() {
        for kind in [1, 5, 30, 60, 240, 1440] {
            let name = interval_of(kind).expect("native");
            assert_eq!(minutes_of(name), Some(kind));
        }
        assert_eq!(interval_of(15), None);
    }

    #[test]
    fn a_print_is_signed_by_its_aggressor_and_garbage_is_dropped() {
        let Some(FeedEvent::Trade { qty, .. }) = trade_of(agg(1, 1, 100.0, 2.0, true)) else {
            panic!("a trade")
        };
        assert_eq!(qty, -2.0, "buyer the maker: a sell");
        assert!(trade_of(agg(1, 1, 0.0, 2.0, false)).is_none());
        assert!(trade_of(agg(1, 1, 100.0, 0.0, false)).is_none());
    }

    #[test]
    fn the_hour_keeps_its_own_trades_in_time_order() {
        let rows = [
            agg(1, 999, 1.0, 1.0, false),
            agg(3, 2_000, 3.0, 1.0, true),
            agg(2, 1_000, 2.0, 1.0, false),
        ];
        let out = history_rows(&rows, 1_000);
        let prices: Vec<f32> = out.iter().map(|t| t.price).collect();
        assert_eq!(prices, [2.0, 3.0], "the one before the hour is gone");
        assert_eq!(out[1].qty, -1.0);
    }
}
