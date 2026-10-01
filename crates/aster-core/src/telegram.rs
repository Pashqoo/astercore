//! Telegram reports: what the core did, in the trader's own chat.
//!
//! Two threads, because one of them is always blocked. The **sender** owns the
//! outgoing queue and is the only one that calls `sendMessage`; the **poller**
//! sits in a long `getUpdates` and pairs the chat by PIN (and from Ф3 takes the
//! commands). Neither is ever on the trading loop's path: the loop hands a
//! [`Note`] to a channel whose depth it counts itself, and past [`QUEUE_CAP`]
//! the note is dropped with a counter in the journal — a stalled proxy costs
//! reports, never a step of the loop.
//!
//! The bot token never reaches the journal: it sits inside every request URL,
//! so everything logged from here goes through [`hide`] first.
//!
//! One token, one core: `getUpdates` from two processes on the same token is a
//! `409` and loses commands, the same rule as two cores on one account.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use ureq::{Agent, Proxy};

use crate::chart;
use crate::clock::format_rfc3339;
use crate::clock::{trader_midnight as msk_midnight, TRADER_OFFSET_MS as MSK_OFFSET_MS};
use crate::control::{self, AskError, ControlCmd, Topic};
use crate::reports::Row;
use crate::settings::{Events, Settings, Telegram};
use moonproto::server::codec::market_data::Candle;

/// The timeframes `/chart` draws: minutes per bar, each one an interval
/// Aster's `klines` serves as it is (`1m 5m 30m 1h 4h 1d`).
const TIMEFRAMES: [i64; 6] = [1, 5, 30, 60, 240, 1440];

const API: &str = "https://api.telegram.org";
/// Wrong PINs before a new one is drawn.
const PIN_TRIES: u32 = 10;
/// `/chart` pictures being made at once; more are refused, not queued — each
/// one is a REST call on the feed's one worker.
const CHARTS_AT_ONCE: usize = 2;
static CHARTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// Where the Bot API lives, for a verification run against a stub of it
/// (`ASTER_TELEGRAM_API=http://127.0.0.1:8099`); unset means Telegram. The
/// variable is read once per agent, from the same environment `.env` fills —
/// whoever can write that file owns the core already.
const API_VAR: &str = "ASTER_TELEGRAM_API";

fn api_base() -> String {
    std::env::var(API_VAR)
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| API.to_owned())
}
/// One message a second: a burst of alarms must not spend the chat's budget
/// (Telegram's own is 20 a minute) and arrive as a `429` besides.
const SEND_EVERY: Duration = Duration::from_millis(1_000);
/// Notes waiting for the network. Past it the oldest go, because the newest
/// alarm is the one worth reading.
const QUEUE_CAP: usize = 200;
/// Pictures waiting with them. A chart is ~15 KB and a stalled proxy would
/// otherwise hold a hundred of them in memory for nothing; the text of those
/// deals went out on a message of its own anyway.
const PHOTO_CAP: usize = 16;
/// Telegram's own limits: 4096 characters in a message, 1024 in a caption.
const TEXT_MAX: usize = 4_000;
const CAPTION_MAX: usize = 1_000;
/// Repeats of one refusal inside this window are one message and a count
/// (21.09: 37 refusals in 26 minutes).
const DEDUP: Duration = Duration::from_secs(10 * 60);
/// `getUpdates` long poll. The poller's agent waits longer than this.
const POLL_S: u64 = 25;
/// Waiting for something to change: no token, or the last call failed.
const IDLE: Duration = Duration::from_secs(5);
/// The floor under one `getUpdates` round. Telegram holds the call for
/// [`POLL_S`] itself, but a proxy (or a stub) that answers at once would
/// otherwise turn the loop into a hot one.
const POLL_FLOOR: Duration = Duration::from_secs(1);
/// After a `409` (another core polls the same token) — the conflict is not
/// ours to fix, and retrying at speed only takes the other one's updates.
const CONFLICT_BACKOFF: Duration = Duration::from_secs(30);
/// How long a chat command may hold the poller. The loop answers between two
/// UDP steps, so this is generous; a command that misses it says so instead of
/// leaving the chat without an answer.
const ASK: Duration = Duration::from_secs(5);
/// A chart waits for the feed itself (`control::CHART_WAIT_MS`), so it runs on
/// a thread of its own: `/panic` must never queue behind a picture.
const CHART_ASK: Duration = Duration::from_secs(95);
/// `/chart TICKER` without a timeframe, and how many of the last bars the
/// answer is about — the picture and the numbers under it show the same ones.
const CHART_MINUTES: i64 = 1;
const CHART_BARS: usize = 60;
/// How often an unpaired core repeats its PIN in the journal.
const PIN_REMIND: Duration = Duration::from_secs(10 * 60);
/// A note the network refused this often is dropped; a refusal is usually
/// the message itself (a chat that is gone), not the line.
const MAX_TRIES: u32 = 3;
/// The sender's own tick: dedupe windows and the drop counter.
const TICK: Duration = Duration::from_millis(500);
/// Messages one `flush` may push out before the process leaves.
const FLUSH_MAX: usize = 5;
/// How long the dropped-notes counter waits before it says so again.
const DROPS_EVERY: Duration = Duration::from_secs(60);

/// What a note is about: it goes out only when its switch in [`Events`] is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Deal,
    Detect,
    Refusal,
    Alarm,
    Lifecycle,
    Daily,
    /// The core answering the chat itself (the pairing reply): not a switch.
    Reply,
}

impl Kind {
    fn on(self, e: &Events) -> bool {
        match self {
            Self::Deal => e.deals,
            Self::Detect => e.detects,
            Self::Refusal => e.refusals,
            Self::Alarm => e.alarms,
            Self::Lifecycle => e.lifecycle,
            Self::Daily => e.daily,
            Self::Reply => true,
        }
    }
}

/// One message to be sent. With a `photo` it goes out as a picture whose
/// caption is the text — one message in the chat, not two.
#[derive(Clone, Debug)]
pub struct Note {
    pub kind: Kind,
    /// Repeats of this key inside [`DEDUP`] fold into a count.
    pub key: Option<String>,
    pub text: String,
    pub photo: Option<Vec<u8>>,
}

enum Req {
    Note(Note),
    /// `data/config.json` changed: the token, the chat, the proxy, the events.
    Settings(Box<Telegram>),
    /// Push what is queued and answer: the process is leaving.
    Flush(Sender<()>),
}

/// The handle the engine holds. Cheap to clone, never blocks.
#[derive(Clone)]
pub struct Reporter {
    tx: Sender<Req>,
    poll: Sender<Settings>,
    /// Notes the sender thread has not taken yet: the cap is counted here, so
    /// a stalled proxy cannot grow the channel without bound.
    queued: Arc<AtomicUsize>,
    /// Notes nobody will ever read (the queue was full, or there is no chat).
    dropped: Arc<AtomicUsize>,
    /// The sender thread is gone (it never started, or it died): said once,
    /// because the alternative is a report channel that is silently off.
    dead: Arc<AtomicBool>,
}

impl Reporter {
    pub fn note(&self, kind: Kind, text: impl Into<String>) {
        self.push(Note {
            kind,
            key: None,
            text: text.into(),
            photo: None,
        });
    }

    /// A PNG with a caption: the deal charts (`chart.rs`).
    pub fn picture(&self, kind: Kind, caption: impl Into<String>, png: Vec<u8>) {
        self.push(Note {
            kind,
            key: None,
            text: caption.into(),
            photo: Some(png),
        });
    }

    /// A note that repeats: the first one goes out, the rest of the window
    /// come back as one count.
    pub fn deduped(&self, kind: Kind, key: impl Into<String>, text: impl Into<String>) {
        self.push(Note {
            kind,
            key: Some(key.into()),
            text: text.into(),
            photo: None,
        });
    }

    /// The settings were replaced (the page, or our own pairing): both threads
    /// pick up the new token, chat, proxy and switches.
    pub fn settings_changed(&self, settings: &Settings) {
        let _ = self
            .tx
            .send(Req::Settings(Box::new(settings.telegram.clone())));
        let _ = self.poll.send(settings.clone());
    }

    /// Give the sender `wait` to push what is queued. Called on the way out:
    /// the stop message is worth the last second of the process's life.
    pub fn flush(&self, wait: Duration) {
        let (ack, done) = mpsc::channel();
        if self.tx.send(Req::Flush(ack)).is_ok() {
            let _ = done.recv_timeout(wait);
        }
    }

    fn push(&self, note: Note) {
        if self.queued.load(Ordering::Relaxed) >= QUEUE_CAP {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.queued.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(Req::Note(note)).is_err() {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            if !self.dead.swap(true, Ordering::Relaxed) {
                log::error!("telegram: the sender thread is gone; the core reports nothing now");
            }
        }
    }
}

/// A reporter with no threads behind it: what the core would have sent stays
/// in [`Sent`], for the tests of the code that reports.
#[cfg(test)]
pub(crate) fn test_reporter() -> (Reporter, Sent) {
    let (tx, rx) = mpsc::channel();
    let (poll, _poll_rx) = mpsc::channel();
    let reporter = Reporter {
        tx,
        poll,
        queued: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::new(AtomicUsize::new(0)),
        dead: Arc::new(AtomicBool::new(false)),
    };
    (reporter, Sent(rx))
}

/// The notes a [`test_reporter`] was handed, oldest first.
#[cfg(test)]
pub(crate) struct Sent(Receiver<Req>);

#[cfg(test)]
impl Sent {
    pub(crate) fn notes(&self) -> Vec<Note> {
        self.0
            .try_iter()
            .filter_map(|req| match req {
                Req::Note(note) => Some(note),
                _ => None,
            })
            .collect()
    }
}

/// Start both threads. They live as long as the process; the settings they
/// were started with are replaced through [`Reporter::settings_changed`].
pub fn start(settings: &Settings, control: Sender<ControlCmd>) -> Reporter {
    let (tx, rx) = mpsc::channel();
    let (poll, poll_rx) = mpsc::channel();
    let reporter = Reporter {
        tx,
        poll,
        queued: Arc::new(AtomicUsize::new(0)),
        dropped: Arc::new(AtomicUsize::new(0)),
        dead: Arc::new(AtomicBool::new(false)),
    };
    let (cfg, queued, dropped) = (
        settings.telegram.clone(),
        Arc::clone(&reporter.queued),
        Arc::clone(&reporter.dropped),
    );
    spawn("telegram", move || send_loop(cfg, &rx, &queued, &dropped));
    let (settings, mine) = (settings.clone(), reporter.clone());
    spawn("telegram-poll", move || {
        poll_loop(settings, &poll_rx, &mine, &control);
    });
    reporter
}

fn spawn(name: &str, body: impl FnOnce() + Send + 'static) {
    if let Err(e) = thread::Builder::new().name(name.into()).spawn(body) {
        log::error!("telegram: {name} thread did not start: {e}");
    }
}

// ----- sending ---------------------------------------------------------------

struct Pending {
    note: Note,
    tries: u32,
}

/// The sender's whole state: its own copy of the settings, so nothing it does
/// takes a lock the trading loop could wait on.
struct Sending {
    cfg: Telegram,
    agent: Agent,
    proxy: String,
    queue: VecDeque<Pending>,
    /// Open dedupe windows: key to when it closes and what was folded into it.
    folded: HashMap<String, (Instant, usize)>,
    next_send: Instant,
    drops_at: Instant,
    said_drops: usize,
}

impl Sending {
    fn new(cfg: Telegram) -> Self {
        let now = Instant::now();
        Self {
            agent: agent_for(&cfg.proxy, Duration::from_secs(30)),
            proxy: cfg.proxy.clone(),
            cfg,
            queue: VecDeque::new(),
            folded: HashMap::new(),
            next_send: now,
            drops_at: now,
            said_drops: 0,
        }
    }

    /// A reachable chat: a token and a chat that answered the PIN.
    fn ready(&self) -> bool {
        !self.cfg.token.is_empty() && self.cfg.chat_id != 0
    }

    fn reconfigure(&mut self, cfg: Telegram) {
        if cfg.proxy != self.proxy {
            self.agent = agent_for(&cfg.proxy, Duration::from_secs(30));
            self.proxy.clone_from(&cfg.proxy);
        }
        self.cfg = cfg;
    }

    /// Take a note in, or drop it: the switch is off, there is no chat, or the
    /// same thing was already said inside its window.
    fn offer(&mut self, note: Note, now: Instant, dropped: &AtomicUsize) {
        if !note.kind.on(&self.cfg.events) {
            return;
        }
        if !self.ready() {
            dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(key) = note.key.clone() {
            match self.folded.get_mut(&key) {
                Some((until, folded)) if now < *until => {
                    *folded += 1;
                    return;
                }
                // The window closed before `sweep` reached it: its count is
                // said now, or it would be lost under the new window.
                Some((_, folded)) if *folded > 0 => {
                    let more = more_note(&key, *folded);
                    self.queue.push_back(Pending {
                        note: more,
                        tries: 0,
                    });
                }
                _ => {}
            }
            self.folded.insert(key, (now + DEDUP, 0));
        }
        let picture = note.photo.is_some();
        self.queue.push_back(Pending { note, tries: 0 });
        while self.queue.len() > QUEUE_CAP {
            self.queue.pop_front();
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        // The oldest picture goes rather than the newest: a chart of a deal
        // from half an hour ago is the one nobody is waiting for.
        while picture && self.queue.iter().filter(|p| p.note.photo.is_some()).count() > PHOTO_CAP {
            let Some(at) = self.queue.iter().position(|p| p.note.photo.is_some()) else {
                break;
            };
            self.queue.remove(at);
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Closed dedupe windows that folded something become one line each.
    fn sweep(&mut self, now: Instant) {
        let mut more: Vec<(String, usize)> = Vec::new();
        self.folded.retain(|key, (until, folded)| {
            if now < *until {
                return true;
            }
            if *folded > 0 {
                more.push((key.clone(), *folded));
            }
            false
        });
        for (key, folded) in more {
            self.queue.push_back(Pending {
                note: more_note(&key, folded),
                tries: 0,
            });
        }
    }

    /// How long the loop may sleep: until the next message may go out, or the
    /// tick that closes the dedupe windows.
    fn wait(&self, now: Instant) -> Duration {
        if self.queue.is_empty() {
            return TICK;
        }
        self.next_send.saturating_duration_since(now).min(TICK)
    }

    /// One message, if one is due. `spaced` keeps the pace; a flush on the way
    /// out does not wait for it.
    fn step(&mut self, now: Instant, spaced: bool) -> bool {
        if spaced && now < self.next_send {
            return false;
        }
        let Some(mut item) = self.queue.pop_front() else {
            return false;
        };
        self.next_send = now + SEND_EVERY;
        let sent = match &item.note.photo {
            Some(png) => send_photo(&self.agent, &self.cfg, &item.note.text, png),
            None => send_message(&self.agent, &self.cfg, &item.note.text),
        };
        match sent {
            Ok(()) => true,
            Err(Fail::RetryAfter(s)) => {
                self.next_send = now + Duration::from_secs(s.clamp(1, 60));
                self.queue.push_front(item);
                false
            }
            Err(Fail::Refused(e)) => {
                log::warn!("telegram: message refused: {e}");
                false
            }
            Err(Fail::Transport(e)) => {
                item.tries += 1;
                if item.tries >= MAX_TRIES {
                    log::warn!("telegram: giving up on a message after {MAX_TRIES} tries: {e}");
                    return false;
                }
                self.queue.push_front(item);
                false
            }
        }
    }

    /// What was lost, once a minute at most: a silent report channel would
    /// otherwise look like a quiet day.
    fn report_drops(&mut self, now: Instant, dropped: &AtomicUsize) {
        let total = dropped.load(Ordering::Relaxed);
        if total == self.said_drops || now < self.drops_at + DROPS_EVERY {
            return;
        }
        log::warn!(
            "telegram: {} report(s) dropped ({})",
            total - self.said_drops,
            if self.ready() {
                "the queue was full"
            } else {
                "no approved chat"
            }
        );
        self.said_drops = total;
        self.drops_at = now;
    }
}

/// What a closed dedupe window has to add.
fn more_note(key: &str, folded: usize) -> Note {
    let mins = DEDUP.as_secs() / 60;
    Note {
        kind: Kind::Refusal,
        key: None,
        text: format!("{key}: {folded} more in the last {mins} min"),
        photo: None,
    }
}

fn send_loop(cfg: Telegram, rx: &Receiver<Req>, queued: &AtomicUsize, dropped: &AtomicUsize) {
    let mut s = Sending::new(cfg);
    loop {
        let wait = s.wait(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(Req::Note(note)) => {
                let _ = queued.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(1))
                });
                s.offer(note, Instant::now(), dropped);
            }
            Ok(Req::Settings(cfg)) => s.reconfigure(*cfg),
            Ok(Req::Flush(ack)) => {
                for _ in 0..FLUSH_MAX {
                    if !s.step(Instant::now(), false) {
                        break;
                    }
                }
                let _ = ack.send(());
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        s.sweep(now);
        s.step(now, true);
        s.report_drops(now, dropped);
    }
}

enum Fail {
    /// `429`: Telegram says when it will take the next one.
    RetryAfter(u64),
    /// The message itself will never go through (no such chat, bad token).
    Refused(String),
    Transport(String),
}

fn send_message(agent: &Agent, cfg: &Telegram, text: &str) -> Result<(), Fail> {
    let body = json!({
        "chat_id": cfg.chat_id,
        "text": cut(text, TEXT_MAX),
        "disable_web_page_preview": true,
    });
    let url = format!("{}/bot{}/sendMessage", api_base(), cfg.token);
    let resp = agent
        .post(&url)
        .send_json(&body)
        .map_err(|e| Fail::Transport(hide(&cfg.token, &e.to_string())))?;
    answer(resp, &cfg.token)
}

/// `sendPhoto` as multipart, written by hand: three fields is no reason for a
/// dependency.
fn send_photo(agent: &Agent, cfg: &Telegram, caption: &str, png: &[u8]) -> Result<(), Fail> {
    let boundary = format!(
        "aster{:x}{:x}",
        png.len() as u64,
        cfg.chat_id.unsigned_abs()
    );
    let mut body = Vec::with_capacity(png.len() + 512);
    let mut field = |name: &str, value: &str| {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    };
    field("chat_id", &cfg.chat_id.to_string());
    field("caption", &cut(caption, CAPTION_MAX));
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"photo\"; filename=\"deal.png\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: image/png\r\n\r\n");
    body.extend_from_slice(png);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let url = format!("{}/bot{}/sendPhoto", api_base(), cfg.token);
    let resp = agent
        .post(&url)
        .header(
            "Content-Type",
            &format!("multipart/form-data; boundary={boundary}"),
        )
        .send(&body[..])
        .map_err(|e| Fail::Transport(hide(&cfg.token, &e.to_string())))?;
    answer(resp, &cfg.token)
}

/// Whole characters up to `max`: a caption cut inside one would not be a
/// string any more.
fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        None => text.to_owned(),
        Some((end, _)) => format!("{}\u{2026}", &text[..end]),
    }
}

/// What Telegram said about a message we sent.
fn answer(resp: ureq::http::Response<ureq::Body>, token: &str) -> Result<(), Fail> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(());
    }
    let body = resp.into_body().read_to_string().unwrap_or_default();
    let json: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    if let Some(after) = json
        .pointer("/parameters/retry_after")
        .and_then(Value::as_u64)
    {
        return Err(Fail::RetryAfter(after));
    }
    let why = json
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or(&body)
        .to_owned();
    let why = hide(token, &format!("HTTP {status}: {why}"));
    // 5xx is the service, not the message: worth the same retries as a
    // dropped connection.
    if status >= 500 {
        return Err(Fail::Transport(why));
    }
    Err(Fail::Refused(why))
}

// ----- pairing and polling ---------------------------------------------------

/// One `getUpdates` item, as far as the pairing cares.
#[derive(Debug, PartialEq, Eq)]
struct Update {
    id: i64,
    chat_id: i64,
    text: String,
}

fn poll_loop(
    mut settings: Settings,
    rx: &Receiver<Settings>,
    out: &Reporter,
    control: &Sender<ControlCmd>,
) {
    let mut proxy = settings.telegram.proxy.clone();
    let mut agent = agent_for(&proxy, Duration::from_secs(POLL_S + 20));
    let mut token = settings.telegram.token.clone();
    let mut offset = 0_i64;
    // The first answer of a token is the backlog Telegram kept while no
    // core was polling (up to a day): a /panic or a /stop sent then is not an
    // order for now. It is skipped, said, and polling starts from after it.
    let mut caught_up = false;
    let mut pin = String::new();
    let mut pin_at: Option<Instant> = None;
    // Wrong PINs against the current one: past PIN_TRIES a new PIN is drawn,
    // so guessing a million values is not a matter of patience.
    let mut pin_misses = 0_u32;
    // Strangers already named in the journal, so one does not flood it.
    let mut strangers: std::collections::HashSet<i64> = std::collections::HashSet::new();
    loop {
        while let Ok(next) = rx.try_recv() {
            settings = next;
        }
        if settings.telegram.token != token {
            // Another bot is another update stream and another pairing.
            token.clone_from(&settings.telegram.token);
            offset = 0;
            caught_up = false;
            pin.clear();
            pin_at = None;
        }
        if settings.telegram.proxy != proxy {
            proxy.clone_from(&settings.telegram.proxy);
            agent = agent_for(&proxy, Duration::from_secs(POLL_S + 20));
        }
        if token.is_empty() {
            idle(rx, IDLE, &mut settings);
            continue;
        }
        if settings.telegram.chat_id == 0 {
            if pin.is_empty() {
                pin = new_pin();
            }
            if pin_at.is_none_or(|at| at.elapsed() >= PIN_REMIND) {
                pin_at = Some(Instant::now());
                log::warn!(
                    "telegram: no approved chat yet; send {pin} to the bot \
                     from the chat that should get the reports"
                );
            }
        }
        // The backlog of a token, before the first live poll: asked without
        // waiting, page by page (100 a page), and skipped — a /panic or a
        // /stop sent while no core was polling is not an order for now. A
        // long poll is never mistaken for it: a message that arrives during
        // one is live.
        if !caught_up {
            match get_updates_waiting(&agent, &token, offset, 0) {
                Ok(page) if page.is_empty() => caught_up = true,
                Ok(page) => {
                    let last = page.iter().map(|u| u.id).max().unwrap_or(offset);
                    offset = offset.max(last + 1);
                    log::info!(
                        "telegram: {} message(s) sent while the core was away, skipped",
                        page.len()
                    );
                    continue;
                }
                Err(e) => {
                    let e = match e {
                        Fail::RetryAfter(s) => format!("retry after {s} s"),
                        Fail::Refused(e) | Fail::Transport(e) => e,
                    };
                    log::warn!("telegram: getUpdates (backlog): {e}");
                    idle(rx, IDLE, &mut settings);
                    continue;
                }
            }
        }
        let started = Instant::now();
        let updates = match get_updates(&agent, &token, offset) {
            Ok(updates) => updates,
            Err(Fail::RetryAfter(s)) => {
                // A 409 arrives here too: another core is polling this token.
                idle(rx, Duration::from_secs(s), &mut settings);
                continue;
            }
            Err(Fail::Refused(e) | Fail::Transport(e)) => {
                log::warn!("telegram: getUpdates: {e}");
                idle(rx, IDLE, &mut settings);
                continue;
            }
        };
        if let Some(left) = POLL_FLOOR.checked_sub(started.elapsed()) {
            idle(rx, left, &mut settings);
        }
        for u in updates {
            offset = offset.max(u.id + 1);
            if settings.telegram.chat_id != 0 {
                if u.chat_id == settings.telegram.chat_id {
                    obey(&u.text, out, control);
                } else {
                    // The approved chat is the white list: nobody else gets to
                    // stop the trading, whoever found the bot.
                    if strangers.insert(u.chat_id) {
                        log::warn!(
                            "telegram: chat {} is not the approved one, ignored",
                            u.chat_id
                        );
                    }
                }
                continue;
            }
            if u.text.trim() != pin {
                pin_misses += 1;
                if pin_misses == 1 || pin_misses.is_multiple_of(PIN_TRIES) {
                    log::warn!(
                        "telegram: chat {} sent something that is not the PIN ({pin_misses} so far)",
                        u.chat_id
                    );
                }
                if pin_misses.is_multiple_of(PIN_TRIES) {
                    pin = new_pin();
                    pin_at = None;
                }
                continue;
            }
            pin_misses = 0;
            // Ours only to stop asking for the PIN: the trading loop owns the
            // settings, writes the chat into them, saves the file and answers
            // the chat — this thread's copy is as old as its last long poll.
            if control.send(ControlCmd::ChatApproved(u.chat_id)).is_err() {
                // Nobody wrote it down, so this thread has not paired either:
                // the PIN stays, and the next message is another chance.
                log::error!("telegram: the core is not taking commands; the chat is not saved");
                continue;
            }
            // Ours only to stop asking for the PIN. The settings come back
            // from the loop, which owns them — this thread's copy is as old as
            // its last long poll, and handing it on would revert whatever was
            // edited meanwhile.
            settings.telegram.chat_id = u.chat_id;
            pin.clear();
            pin_at = None;
            log::info!("telegram: chat {} approved", u.chat_id);
        }
    }
}

/// What the chat may ask for. An unknown `/command`, and a bare `help`, are
/// answered with this list; ordinary talk in the chat is left alone.
const HELP: &str = "\
/status — what the core is doing right now\n\
/profit — today's deals and the totals\n\
/start, /stop — the strategies\n\
/panic — every position out at once; a resting entry that already filled in \
part is withdrawn with it (stopping the core withdraws the entries too, but \
leaves the exits alone)\n\
/chart TICKER [minutes] — the last bars of a market\n\
talk, silent — deal reports on or off";

/// One message from the approved chat: the command and its words, or nothing
/// when the chat was simply talking. A group addresses the bot as
/// `/status@thebot`; MoonBot's `talk` and `silent` carry no slash, and are
/// therefore taken only as a message of their own — "stop worrying about it"
/// must never reach `/stop`, and the chat this listens to is the one that can
/// panic-exit every position.
fn command_of(text: &str) -> Option<(String, Vec<String>)> {
    let mut words = text.split_whitespace();
    let head = words.next()?;
    let args: Vec<String> = words.map(str::to_owned).collect();
    if let Some(cmd) = head.strip_prefix('/') {
        let cmd = cmd.split('@').next().unwrap_or(cmd).to_lowercase();
        return (!cmd.is_empty()).then_some((cmd, args));
    }
    let bare = head.to_lowercase();
    match bare.as_str() {
        "talk" | "silent" | "help" if args.is_empty() => Some((bare, args)),
        _ => None,
    }
}

/// Carry out what the chat asked. Everything that changes the core goes
/// through the control queue, the same one the page uses, so a stop is a stop
/// whoever pressed it.
fn obey(text: &str, out: &Reporter, control: &Sender<ControlCmd>) {
    let Some((cmd, args)) = command_of(text) else {
        return;
    };
    let answer = match cmd.as_str() {
        "status" | "profit" => {
            let topic = if cmd == "status" {
                Topic::Status
            } else {
                Topic::Profit
            };
            said(control::ask(
                control,
                |reply| ControlCmd::Text { topic, reply },
                ASK,
            ))
        }
        "start" | "stop" => {
            let start = cmd == "start";
            match control::ask(
                control,
                |reply| ControlCmd::Strategies { start, reply },
                ASK,
            ) {
                Ok(true) => "strategies running".to_string(),
                Ok(false) => "strategies stopped".to_string(),
                Err(e) => e.to_string(),
            }
        }
        // The loop says what a panic did, whoever asked for it (the page will
        // too), so nothing is added here on the way out. A wait that ran out
        // is still worth a line: the command may have been carried out late,
        // and then the loop's own note follows it.
        "panic" => match control::ask(control, ControlCmd::PanicAll, ASK) {
            Ok(_) => return,
            Err(e) => e.to_string(),
        },
        "talk" | "silent" => {
            // The loop owns the settings: it flips the switch, writes the file
            // and answers the chat itself.
            match control.send(ControlCmd::Talk(cmd == "talk")) {
                Ok(()) => return,
                Err(_) => AskError::Gone.to_string(),
            }
        }
        "chart" => {
            chart(&args, out, control);
            return;
        }
        _ => HELP.to_string(),
    };
    out.note(Kind::Reply, answer);
}

/// `/chart TICKER [minutes]`. On its own thread: the feed may take half a
/// minute to answer and the poller has other commands to take meanwhile.
fn chart(args: &[String], out: &Reporter, control: &Sender<ControlCmd>) {
    let Some(market) = args.first().map(|m| m.to_uppercase()) else {
        out.note(Kind::Reply, "/chart TICKER [minutes]");
        return;
    };
    let minutes = match args.get(1) {
        None => CHART_MINUTES,
        Some(raw) => match raw.parse::<i64>() {
            // Only what the feeds can build: any other number is accepted
            // here and refused by the gateway a round trip later, and past
            // the axis's own ceiling the picture would clamp the spacing and
            // keep the asked-for number in the title.
            Ok(m) if TIMEFRAMES.contains(&m) => m,
            // Silently drawing a different timeframe than the one asked for
            // is a chart that lies about its own axis.
            _ => {
                out.note(
                    Kind::Reply,
                    format!(
                        "{raw:?} is not a timeframe: /chart TICKER [{} \
                         minutes in a bar]",
                        TIMEFRAMES.map(|m| m.to_string()).join(" ")
                    ),
                );
                return;
            }
        },
    };
    use std::sync::atomic::Ordering;
    if CHARTS.fetch_add(1, Ordering::SeqCst) >= CHARTS_AT_ONCE {
        CHARTS.fetch_sub(1, Ordering::SeqCst);
        out.note(
            Kind::Reply,
            "another /chart is being drawn; ask again in a minute",
        );
        return;
    }
    // Released however the thread ends — or with the closure, when the
    // thread never starts.
    struct Slot;
    impl Drop for Slot {
        fn drop(&mut self) {
            CHARTS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let slot = Slot;
    let (control, out) = (control.clone(), out.clone());
    spawn("telegram-chart", move || {
        let _slot = slot;
        let answer = match control::ask(
            &control,
            |reply| ControlCmd::Chart {
                market: market.clone(),
                minutes,
                reply,
            },
            CHART_ASK,
        ) {
            Ok(Ok(candles)) => {
                // The same data a deal's note gets: a picture, with the
                // numbers under it as the caption.
                let bars = &candles[candles.len().saturating_sub(CHART_BARS)..];
                let text = chart_text(&market, minutes, bars);
                let drawn = chart::deal_png(
                    chart::Series::Bars(bars),
                    &chart::Deal {
                        market: &market,
                        minutes,
                        short: false,
                        // No deal behind this one: the marks and the levels
                        // are a deal's, and this is a market.
                        entry: 0.0,
                        entry_ms: 0,
                        exit: 0.0,
                        exit_ms: 0,
                        stop: None,
                        take: None,
                        // Nor a line either leg drew: there is no order.
                        entry_moves: &[],
                        exit_moves: &[],
                        caption: &ascii_line(bars),
                        // Nor a position and a day's result: the stats line
                        // falls back to what a market chart can say.
                        outcome: None,
                    },
                );
                if let Some(png) = drawn {
                    out.picture(Kind::Reply, text, png);
                    return;
                }
                text
            }
            Ok(Err(why)) => why,
            // The loop drops the reply channel when the feed gave up on the
            // candles (`expire_chart_waits`), which reaches us as `Gone` —
            // the same value a core that is not answering at all produces.
            Err(AskError::Gone) => format!(
                "{market}: no candles — the feed stayed quiet, \
                 or the core is not taking commands"
            ),
            Err(e) => e.to_string(),
        };
        out.note(Kind::Reply, answer);
    });
}

/// The line drawn inside a market's picture, plainer than the chat's own
/// text (TInvestCore kept a currency sign Go Mono lacks out of it; USDT is
/// plain letters).
fn ascii_line(bars: &[Candle]) -> String {
    let (Some(first), Some(last)) = (bars.first(), bars.last()) else {
        return String::new();
    };
    let (open, close) = (f64::from(first.open), f64::from(last.close));
    let change = if open > 0.0 {
        pct_text((close - open) / open * 100.0)
    } else {
        String::new()
    };
    format!("{} BARS  LAST {}  {change}", bars.len(), num(close))
}

/// An answer the loop was supposed to put in words, or why there is none.
fn said(answer: Result<String, AskError>) -> String {
    answer.unwrap_or_else(|e| e.to_string())
}

/// Wait for a settings change, or for `wait` to pass.
fn idle(rx: &Receiver<Settings>, wait: Duration, settings: &mut Settings) {
    if let Ok(next) = rx.recv_timeout(wait) {
        *settings = next;
    }
}

fn get_updates(agent: &Agent, token: &str, offset: i64) -> Result<Vec<Update>, Fail> {
    get_updates_waiting(agent, token, offset, POLL_S)
}

/// `getUpdates` holding the call up to `wait_s` seconds (0: what is waiting
/// now, at once).
fn get_updates_waiting(
    agent: &Agent,
    token: &str,
    offset: i64,
    wait_s: u64,
) -> Result<Vec<Update>, Fail> {
    let url = format!("{}/bot{token}/getUpdates", api_base());
    let body = json!({
        "offset": offset,
        "timeout": wait_s,
        "allowed_updates": ["message", "channel_post"],
    });
    let resp = agent
        .post(&url)
        .send_json(&body)
        .map_err(|e| Fail::Transport(hide(token, &e.to_string())))?;
    let status = resp.status().as_u16();
    let body = resp
        .into_body()
        .read_to_string()
        .map_err(|e| Fail::Transport(hide(token, &e.to_string())))?;
    let json: Value =
        serde_json::from_str(&body).map_err(|e| Fail::Transport(format!("getUpdates: {e}")))?;
    if let Some(after) = json
        .pointer("/parameters/retry_after")
        .and_then(Value::as_u64)
    {
        return Err(Fail::RetryAfter(after.clamp(1, 300)));
    }
    if status == 409 {
        log::warn!(
            "telegram: 409 Conflict — another process polls this bot token; \
             one token, one core"
        );
        return Err(Fail::RetryAfter(CONFLICT_BACKOFF.as_secs()));
    }
    if !(200..300).contains(&status) {
        let why = json
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or(&body);
        return Err(Fail::Refused(hide(token, &format!("HTTP {status}: {why}"))));
    }
    Ok(updates_of(&json))
}

/// The messages of a `getUpdates` answer; anything else (edits, callbacks) is
/// skipped, its id still counted so the offset moves past it.
fn updates_of(json: &Value) -> Vec<Update> {
    let Some(list) = json.get("result").and_then(Value::as_array) else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|u| {
            let id = u.get("update_id").and_then(Value::as_i64)?;
            let msg = u.get("message").or_else(|| u.get("channel_post"));
            let chat_id = msg
                .and_then(|m| m.pointer("/chat/id"))
                .and_then(Value::as_i64);
            Some(Update {
                id,
                chat_id: chat_id.unwrap_or(0),
                text: msg
                    .and_then(|m| m.get("text"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
        })
        .collect()
}

/// Six digits nobody can guess from the outside: the chat that sends them back
/// gets to stop the core (Ф3), so it is not a counter.
fn new_pin() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos() as u64);
    let mixed = nanos
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(u64::from(std::process::id()))
        .rotate_left(17)
        .wrapping_mul(0x2545_F491_4F6C_DD1D);
    format!("{:06}", (mixed >> 11) % 1_000_000)
}

// ----- plumbing --------------------------------------------------------------

/// An agent for `api.telegram.org`: public roots (the Russian root is the
/// gateway's alone), and the proxy from the settings — empty means direct,
/// whatever the environment says, because the setting is the truth.
fn agent_for(proxy: &str, global: Duration) -> Agent {
    let proxy = match proxy.trim() {
        "" => None,
        raw => match Proxy::new(&with_scheme(raw)) {
            Ok(p) => Some(p),
            Err(e) => {
                log::error!("telegram: proxy {raw:?} is not a proxy url ({e}); going direct");
                None
            }
        },
    };
    Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(Duration::from_secs(10)))
        .timeout_global(Some(global))
        .proxy(proxy)
        .build()
        .new_agent()
}

/// `127.0.0.1:1080` is a SOCKS5 proxy — the server's xray, the one the core
/// reaches Telegram through. A url that names its own protocol keeps it.
fn with_scheme(raw: &str) -> String {
    if raw.contains("://") {
        raw.to_owned()
    } else {
        format!("socks5://{raw}")
    }
}

/// The token out of anything that goes to the journal: it is part of every
/// request url, so a transport error carries it.
fn hide(token: &str, text: &str) -> String {
    if token.is_empty() {
        return text.to_owned();
    }
    text.replace(token, "<token>")
}

// ----- what the messages say -------------------------------------------------

/// A closed deal, in MoonBot's own words: the strategy that did it, then the
/// exit with what it made and the price it went out at.
///
/// Nothing else, on purpose (30.09): the fee, the reason for the exit and how
/// long the position lived are in the report and on the page, and MoonBot's
/// line never carried them. The one thing that is added is [`mark`]: an
/// emulator deal must not read as money.
pub fn deal_text(row: &Row, strategy: &str) -> String {
    let pct = if row.spent > 0.0 {
        row.profit / row.spent * 100.0
    } else {
        0.0
    };
    // A short is closed by a buy: «sell price» would name the wrong side of
    // the book, and the direction is the one thing this shape does not spell.
    let (word, side) = if row.is_short {
        ("Buy", "buy")
    } else {
        ("Sell", "sell")
    };
    format!(
        "{}: \n{}{word} #{}  {} ({})   {side} price: {}",
        whose(strategy),
        mark(row),
        row.coin,
        signed_usdt(row.profit),
        pct1(pct),
        num(row.sell_price),
    )
}

/// The entry, the moment it filled. Until now the chat heard nothing until
/// the deal was over, which on a position held for an hour is an hour of
/// silence about money that is already at risk. It waits for nothing, unlike
/// the deal's own message: there is no commission to net into an entry.
///
/// `ordered` is the units the entry asked for (the entry leg's `quantity`,
/// which stays the whole ask even after a cancellation cut the fill short), and
/// the percentage is the fill against it: a whole entry reads `(100%)`, one
/// the core cut short at half reads `(50%)` beside the half it did spend.
pub fn entry_text(row: &Row, strategy: &str, ordered: f64) -> String {
    // Nothing to divide by. Not a case today's `Orders` produces — an entry
    // leg keeps the size it asked for even after a cancellation cuts the fill
    // short, and a foreign order is only ever adopted as an exit — but an
    // `inf%` in a message about money is worth the one branch.
    let filled = if ordered <= 0.0 || row.quantity >= ordered {
        100.0
    } else {
        // And a fill one lot short of the whole must not round up to «100%»:
        // saying the entry was cut short is the only job this number has.
        (row.quantity / ordered * 100.0).min(99.9)
    };
    // A short opens by selling.
    let word = if row.is_short { "Sold" } else { "Bought" };
    format!(
        "{}: \n{}{word} {} #{}  ({}%) {} USDT",
        whose(strategy),
        mark(row),
        num(row.quantity),
        row.coin,
        one(filled),
        grouped(row.spent),
    )
}

/// Closed, real deals since `since_s` and what they made. The terminal's own
/// counters (`Reports::profit`, MoonBot's shape) count a row from the moment
/// its entry filled and bucket the hour by that entry — right for the table
/// the terminal draws, wrong for a chat asking what today made.
pub(crate) fn tally<'a>(rows: impl Iterator<Item = &'a Row>, since_s: i64) -> (f64, usize) {
    rows.filter(|r| r.closed && !r.deleted && !r.emulator && r.close_date >= since_s)
        .fold((0.0, 0), |(sum, n), r| (sum + r.profit, n + 1))
}

/// `/status`: the whole core in the shape a phone can read.
pub fn status_text<'a>(
    s: &control::Status,
    rows: impl Iterator<Item = &'a Row> + Clone,
    now_ms: i64,
) -> String {
    let checked = s.strategies.iter().filter(|st| st.checked).count();
    let mut text = format!(
        "{} {} · {checked} of {} strategies checked · up {}",
        if s.running { "▶️" } else { "⏹" },
        if s.running { "running" } else { "stopped" },
        s.strategies.len(),
        duration(s.uptime_s),
    );
    if let Some(why) = &s.circuit_stopped {
        text.push_str(&format!("\nstopped by {why}"));
    }
    if s.market_stopped {
        text.push_str("\nstopped by a market panic");
    }
    if !s.warmup_done {
        text.push_str("\nthe warm-up is still running");
    }
    text.push_str(&format!(
        "\norders {} · {}",
        s.orders.len(),
        if s.trading {
            "orders reach the exchange"
        } else {
            "trading is off"
        }
    ));
    let (today, today_n) = tally(rows.clone(), msk_midnight(now_ms) / 1000);
    let (hour, hour_n) = tally(rows, now_ms / 1000 - 3_600);
    text.push_str(&format!(
        "\ntoday {today_n} deal(s) {} · last hour {hour_n} {}",
        usdt(today),
        usdt(hour),
    ));
    if !s.settings.telegram_events.deals {
        text.push_str("\ndeal reports are off (talk turns them on)");
    }
    let silent: Vec<&str> = s
        .streams
        .iter()
        .filter(|st| !st.alive)
        .map(|st| st.name.as_str())
        .collect();
    text.push_str(&if silent.is_empty() {
        format!("\nstreams: all {} live", s.streams.len())
    } else {
        format!("\n⚠️ streams silent: {}", silent.join(", "))
    });
    text
}

/// `/profit`: the day, the hour and everything the report holds — counted the
/// same way, over deals that actually closed.
pub fn profit_text<'a>(rows: impl Iterator<Item = &'a Row> + Clone, now_ms: i64) -> String {
    let (hour, hour_n) = tally(rows.clone(), now_ms / 1000 - 3_600);
    let (all, all_n) = tally(rows.clone(), 0);
    format!(
        "{}\nlast hour {hour_n} deal(s) {}\nall time {all_n} deal(s) {}",
        daily_text(rows, msk_midnight(now_ms) / 1000),
        usdt(hour),
        usdt(all),
    )
}

/// `/chart`: the tail of the history in words. Ф4 draws it instead.
pub fn chart_text(market: &str, minutes: i64, tail: &[Candle]) -> String {
    let (Some(first), Some(last)) = (tail.first(), tail.last()) else {
        return format!("{market}: no candles");
    };
    let open = f64::from(first.open);
    let close = f64::from(last.close);
    let high = tail.iter().map(|c| f64::from(c.high)).fold(open, f64::max);
    let low = tail.iter().map(|c| f64::from(c.low)).fold(open, f64::min);
    let volume: f64 = tail.iter().map(|c| f64::from(c.volume)).sum();
    // A window that opened at zero says nothing about a move: the percentage
    // is left out rather than divided by it.
    let change = if open > 0.0 {
        pct_text((close - open) / open * 100.0)
    } else {
        "?".into()
    };
    format!(
        "{market} {minutes}m · {} bars\nlast {} ({change})\nhigh {} · low {} · volume {}",
        tail.len(),
        num(close),
        num(high),
        num(low),
        num2(volume),
    )
}

/// The line drawn inside a deal's picture, in the characters Go Mono draws
/// (the chat's own caption, [`shot_caption`], may carry more).
pub fn shot_caption_drawn(row: &Row) -> String {
    let pct = if row.spent > 0.0 {
        row.profit / row.spent * 100.0
    } else {
        0.0
    };
    let mut text = format!("{} USDT ({})", num2(row.profit), pct_text(pct));
    if row.commission > 0.0 {
        text.push_str(&format!("  FEE {}", num2(row.commission)));
    }
    if !row.sell_reason.is_empty() {
        text.push_str("  ");
        text.push_str(&row.sell_reason);
    }
    text
}

/// The line under a deal's picture. It repeats the result on purpose: the
/// deal's own message is MoonBot's bare line and went out separately, so a
/// chat scrolled by pictures has only this one to read the deal off.
pub fn shot_caption(row: &Row) -> String {
    let pct = if row.spent > 0.0 {
        row.profit / row.spent * 100.0
    } else {
        0.0
    };
    format!(
        "{}{} {} {} ({})",
        mark(row),
        row.coin,
        if row.is_short { "SHORT" } else { "LONG" },
        usdt(row.profit),
        pct_text(pct),
    )
}

/// The summary of the deals closed today, sent at the Moscow hour
/// `telegram.daily_at` names (23:50 by default, MoonBot's).
pub fn daily_text<'a>(rows: impl Iterator<Item = &'a Row>, day_start_s: i64) -> String {
    let (mut profit, mut fees, mut wins, mut losses) = (0.0, 0.0, 0, 0);
    let (mut best, mut worst): (Option<&Row>, Option<&Row>) = (None, None);
    for r in rows.filter(|r| r.closed && !r.deleted && !r.emulator && r.close_date >= day_start_s) {
        profit += r.profit;
        fees += r.commission;
        if r.profit >= 0.0 {
            wins += 1;
        } else {
            losses += 1;
        }
        if best.is_none_or(|b| r.profit > b.profit) {
            best = Some(r);
        }
        if worst.is_none_or(|w| r.profit < w.profit) {
            worst = Some(r);
        }
    }
    let day = msk_date(day_start_s * 1000);
    if wins + losses == 0 {
        return format!("📊 {day}: no deals closed today");
    }
    let mut text = format!(
        "📊 {day}\ndeals {} ({wins} up / {losses} down) · {}",
        wins + losses,
        usdt(profit)
    );
    if fees > 0.0 {
        text.push_str(&format!(" · fees {} USDT", num2(fees)));
    }
    if let Some(b) = best.filter(|b| b.profit > 0.0) {
        text.push_str(&format!("\nbest {} {}", b.coin, usdt(b.profit)));
    }
    if let Some(w) = worst.filter(|w| w.profit < 0.0) {
        text.push_str(&format!("\nworst {} {}", w.coin, usdt(w.profit)));
    }
    text
}

/// `27.09` of an MSK day, from the Unix milliseconds of any moment in it.
fn msk_date(ms: i64) -> String {
    let iso = format_rfc3339(ms + MSK_OFFSET_MS);
    // `2026-09-27T20:50:00Z` -> `27.09`.
    match (iso.get(8..10), iso.get(5..7)) {
        (Some(day), Some(month)) => format!("{day}.{month}"),
        _ => iso,
    }
}

/// The emulator's mark, on every one of a deal's three messages (30.09). An
/// emulator deal costs nothing and earns nothing, and the shape it is reported
/// in is otherwise the same as a real one's: without this the chat is a ledger
/// of money that never moved. It goes on the line that names the deal, not on
/// the strategy heading above it, and the `(E)` the terminal uses on its own
/// rows is the same idea in its own place.
fn mark(row: &Row) -> &'static str {
    if row.emulator {
        "[E] "
    } else {
        ""
    }
}

/// Whose deal it is, for the line both messages are headed with. A hand trade
/// and a strategy that has since been deleted are still somebody's deal, so
/// the heading is never blank.
fn whose(strategy: &str) -> &str {
    if strategy.is_empty() {
        "manual"
    } else {
        strategy
    }
}

/// USDT the way MoonBot's chat writes an amount: thousands apart, cents
/// only when there are any — `500 000`, `1 500.37`. Unsigned; the sign is the
/// caller's ([`signed_usdt`]), because MoonBot stands it apart from the number.
fn grouped(v: f64) -> String {
    let text = format!("{:.2}", v.abs());
    let (whole, cents) = text.split_once('.').unwrap_or((text.as_str(), "00"));
    let mut out = String::with_capacity(whole.len() + 8);
    for (i, c) in whole.char_indices() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(c);
    }
    if cents != "00" {
        out.push('.');
        out.push_str(cents);
    }
    out
}

/// A result in USDT, sign apart: `+ 20.05 USDT`, `- 12 USDT`. The sign comes off
/// the number that is printed, not the one that was measured: a third of a
/// cent rounds to `0 USDT`, and `- 0 USDT` would be a loss the amount denies.
fn signed_usdt(v: f64) -> String {
    let text = grouped(v);
    let sign = if text == "0" {
        ""
    } else if v < 0.0 {
        "- "
    } else {
        "+ "
    };
    format!("{sign}{text} USDT")
}

/// How much of the order went through, MoonBot's way: `100`, `50`, `99.6` —
/// one digit, and no digit at all when there is nothing behind it.
fn one(v: f64) -> String {
    let text = format!("{v:.1}");
    let text = text.strip_suffix(".0").unwrap_or(&text);
    if text == "-0" {
        "0".to_owned()
    } else {
        text.to_owned()
    }
}

/// A result in percent, MoonBot's one digit and always that digit: `+4.1%`,
/// `-0.5%`, `+5.0%`. The sign comes off the printed number here too, so a
/// result too small to reach a tenth is `0.0%` whichever side of nothing it
/// fell on — `+0.0%` and `-0.0%` are one outcome written two ways.
fn pct1(v: f64) -> String {
    let text = format!("{:.1}", v.abs());
    let sign = if text == "0.0" {
        ""
    } else if v < 0.0 {
        "-"
    } else {
        "+"
    };
    format!("{sign}{text}%")
}

/// USDT with their sign: `+1 240.50 USDT`.
fn usdt(v: f64) -> String {
    format!("{}{} USDT", if v > 0.0 { "+" } else { "" }, num2(v))
}

fn pct_text(v: f64) -> String {
    format!("{}{:.2}%", if v > 0.0 { "+" } else { "" }, v)
}

/// A price or a size: enough digits for a cent, no trailing zeroes.
fn num(v: f64) -> String {
    let text = format!("{v:.6}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    if text.is_empty() {
        "0".into()
    } else {
        text.to_owned()
    }
}

fn num2(v: f64) -> String {
    format!("{v:.2}")
}

fn duration(secs: i64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3_600 => format!("{}m", s / 60),
        s => format!("{}h{:02}m", s / 3_600, (s % 3_600) / 60),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events_all_on() -> Events {
        Events {
            deals: true,
            detects: true,
            refusals: true,
            alarms: true,
            lifecycle: true,
            daily: true,
        }
    }

    fn row(coin: &str, profit: f64) -> Row {
        Row {
            coin: coin.into(),
            quantity: 10.0,
            buy_price: 250.1,
            sell_price: 251.35,
            spent: 2_501.0,
            gained: 2_513.5,
            profit,
            commission: 1.2,
            closed: true,
            sell_reason: "TakeProfit".into(),
            buy_date: 1_000,
            close_date: 1_260,
            ..Row::default()
        }
    }

    #[test]
    fn a_switch_that_is_off_keeps_its_notes_at_home() {
        let events = Events {
            deals: false,
            ..events_all_on()
        };
        assert!(!Kind::Deal.on(&events));
        assert!(Kind::Alarm.on(&events));
        // The pairing answer is not a switch: it is the core being spoken to.
        assert!(Kind::Reply.on(&Events {
            deals: false,
            detects: false,
            refusals: false,
            alarms: false,
            lifecycle: false,
            daily: false,
        }));
    }

    /// 37 refusals in 26 minutes are one message and a count, not 37 messages.
    #[test]
    fn repeats_of_one_key_fold_into_a_count() {
        let mut s = Sending::new(Telegram {
            token: "123:abc".into(),
            chat_id: 42,
            ..Telegram::default()
        });
        let dropped = AtomicUsize::new(0);
        let start = Instant::now();
        let refusal = |text: &str| Note {
            kind: Kind::Refusal,
            key: Some("SLEN:30042".into()),
            text: text.into(),
            photo: None,
        };
        for _ in 0..37 {
            s.offer(refusal("SLEN: 30042"), start, &dropped);
        }
        assert_eq!(s.queue.len(), 1, "the first one goes, the rest fold");
        // Another key is another window.
        s.offer(
            Note {
                key: Some("AKRN:30042".into()),
                ..refusal("AKRN: 30042")
            },
            start,
            &dropped,
        );
        assert_eq!(s.queue.len(), 2);
        // The window closes: what was folded is said once, with its count.
        s.queue.clear();
        s.sweep(start + DEDUP + Duration::from_secs(1));
        let texts: Vec<&str> = s.queue.iter().map(|p| p.note.text.as_str()).collect();
        assert_eq!(texts, ["SLEN:30042: 36 more in the last 10 min"]);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    /// The queue is capped where the sender holds it too: a proxy that is down
    /// for an hour must not grow it without bound.
    #[test]
    fn a_full_queue_drops_the_oldest_and_counts_it() {
        let mut s = Sending::new(Telegram {
            token: "123:abc".into(),
            chat_id: 42,
            ..Telegram::default()
        });
        let dropped = AtomicUsize::new(0);
        let now = Instant::now();
        for i in 0..QUEUE_CAP + 10 {
            s.offer(
                Note {
                    kind: Kind::Alarm,
                    key: None,
                    text: format!("alarm {i}"),
                    photo: None,
                },
                now,
                &dropped,
            );
        }
        assert_eq!(s.queue.len(), QUEUE_CAP);
        assert_eq!(dropped.load(Ordering::Relaxed), 10);
        assert_eq!(s.queue.front().unwrap().note.text, "alarm 10");
    }

    /// Without an approved chat there is nowhere to send: the note is dropped
    /// at once rather than waiting in a queue for a pairing that may not come.
    #[test]
    fn notes_without_a_chat_are_dropped_not_queued() {
        let mut s = Sending::new(Telegram {
            token: "123:abc".into(),
            ..Telegram::default()
        });
        let dropped = AtomicUsize::new(0);
        s.offer(
            Note {
                kind: Kind::Alarm,
                key: None,
                text: "alarm".into(),
                photo: None,
            },
            Instant::now(),
            &dropped,
        );
        assert!(s.queue.is_empty());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn the_token_never_reaches_a_log_line() {
        let text = hide(
            "123:secret",
            "https://api.telegram.org/bot123:secret/sendMessage: timed out",
        );
        assert!(!text.contains("secret"), "{text}");
        assert!(text.contains("<token>"), "{text}");
        // An empty token replaces nothing (every string contains "").
        assert_eq!(hide("", "plain"), "plain");
    }

    #[test]
    fn a_bare_host_port_is_a_socks5_proxy() {
        assert_eq!(with_scheme("127.0.0.1:1080"), "socks5://127.0.0.1:1080");
        assert_eq!(with_scheme("http://10.0.0.1:3128"), "http://10.0.0.1:3128");
        // A proxy that cannot be parsed is not a reason to stop reporting.
        let _ = agent_for("не адрес", Duration::from_secs(1));
    }

    #[test]
    fn updates_carry_the_chat_and_the_text() {
        let json: Value = serde_json::from_str(
            r#"{"ok":true,"result":[
                {"update_id":10,"message":{"chat":{"id":-1001},"text":" 123456 "}},
                {"update_id":11,"edited_message":{"chat":{"id":-1001},"text":"x"}},
                {"update_id":12,"channel_post":{"chat":{"id":7},"text":"hi"}}
            ]}"#,
        )
        .unwrap();
        let updates = updates_of(&json);
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[0].chat_id, -1001);
        assert_eq!(updates[0].text.trim(), "123456");
        // An update that is not a message still counts: the offset has to
        // move past it or `getUpdates` hands it back forever.
        assert_eq!(updates[1].id, 11);
        assert_eq!(updates[1].chat_id, 0);
        assert_eq!(updates[2].text, "hi");
        assert!(updates_of(&Value::Null).is_empty());
    }

    #[test]
    fn a_pin_is_six_digits_and_not_the_same_twice() {
        let pin = new_pin();
        assert_eq!(pin.len(), 6);
        assert!(pin.chars().all(|c| c.is_ascii_digit()), "{pin}");
        assert_ne!(pin, new_pin());
    }

    /// MoonBot's own three lines, copied (30.09): the strategy, then the exit
    /// with what it made and the price it left at. The byte-for-byte compare
    /// is the point — the shape is the thing the trader reads at a glance, and
    /// a stray space in it is a change nobody asked for.
    #[test]
    fn a_deal_reads_the_way_moonbot_writes_one() {
        assert_eq!(
            deal_text(&row("SBER", 125.4), "strike shares"),
            "strike shares: \nSell #SBER  + 125.40 USDT (+5.0%)   sell price: 251.35"
        );
        // A loss keeps the sign apart too, and no strategy is a hand trade.
        assert_eq!(
            deal_text(&row("GAZP", -12.0), ""),
            "manual: \nSell #GAZP  - 12 USDT (-0.5%)   sell price: 251.35"
        );
        // A short goes out by buying: naming that price a sell price would
        // point the trader at the wrong side of the book.
        assert_eq!(
            deal_text(
                &Row {
                    is_short: true,
                    ..row("SiZ6", 125.4)
                },
                "strike futures"
            ),
            "strike futures: \nBuy #SiZ6  + 125.40 USDT (+5.0%)   buy price: 251.35"
        );
        // Nothing MoonBot's line never carried: the fee, how long it lived,
        // why it went out. They are in the report and on the page.
        let text = deal_text(&row("SBER", 125.4), "strike shares");
        assert!(
            !text.contains("fee") && !text.contains("TakeProfit"),
            "{text}"
        );
    }

    /// The entry's own message: what was bought, how much of the order that
    /// was, and the USDT it cost. No result in it, because there is none.
    #[test]
    fn an_entry_says_what_was_opened_and_how_much_of_it_filled() {
        let opened = Row {
            quantity: 7.0,
            buy_price: 275.21,
            spent: 1_926.47,
            closed: false,
            sell_price: 0.0,
            profit: 0.0,
            ..row("SBER", 0.0)
        };
        assert_eq!(
            entry_text(&opened, "hook shares", 7.0),
            "hook shares: \nBought 7 #SBER  (100%) 1 926.47 USDT"
        );
        // Half the order bought is half the percentage and half the money —
        // and `spent` already is the half, because it is what went out.
        assert_eq!(
            entry_text(&opened, "hook shares", 14.0),
            "hook shares: \nBought 7 #SBER  (50%) 1 926.47 USDT"
        );
        // One unit of 3500 short is not a full fill, whatever it rounds to.
        let nearly = Row {
            quantity: 3_499.0,
            ..opened.clone()
        };
        assert_eq!(
            entry_text(&nearly, "hook shares", 3_500.0),
            "hook shares: \nBought 3499 #SBER  (99.9%) 1 926.47 USDT"
        );
        // A position adopted off the account asked for nothing: all of it is
        // what there is.
        assert_eq!(
            entry_text(&opened, "", 0.0),
            "manual: \nBought 7 #SBER  (100%) 1 926.47 USDT"
        );
        // A short opens by selling.
        assert_eq!(
            entry_text(
                &Row {
                    is_short: true,
                    ..opened
                },
                "",
                7.0
            ),
            "manual: \nSold 7 #SBER  (100%) 1 926.47 USDT"
        );
    }

    /// An emulator deal carries `[E]` on every one of its three messages, and
    /// a real one on none of them: the two are otherwise the same shape, and a
    /// chat that cannot tell them apart is a ledger of money that never moved.
    #[test]
    fn the_emulator_marks_all_three_of_its_messages() {
        let closed = Row {
            emulator: true,
            ..row("SBER", 125.4)
        };
        let open = Row {
            quantity: 7.0,
            buy_price: 275.21,
            spent: 1_926.47,
            closed: false,
            sell_price: 0.0,
            profit: 0.0,
            emulator: true,
            ..row("SBER", 0.0)
        };
        assert_eq!(
            entry_text(&open, "hook shares", 7.0),
            "hook shares: \n[E] Bought 7 #SBER  (100%) 1 926.47 USDT"
        );
        assert_eq!(
            deal_text(&closed, "strike shares"),
            "strike shares: \n[E] Sell #SBER  + 125.40 USDT (+5.0%)   sell price: 251.35"
        );
        assert_eq!(shot_caption(&closed), "[E] SBER LONG +125.40 USDT (+5.01%)");
        // The mark rides the line that names the deal, never the strategy
        // heading above it.
        assert!(
            deal_text(&closed, "strike shares").starts_with("strike shares: \n[E] "),
            "the heading is the strategy's, not the emulator's"
        );
        // And a real deal is marked on none of the three.
        let real = row("SBER", 125.4);
        for text in [
            entry_text(
                &Row {
                    emulator: false,
                    ..open
                },
                "hook shares",
                7.0,
            ),
            deal_text(&real, "strike shares"),
            shot_caption(&real),
        ] {
            assert!(!text.contains("[E]"), "{text}");
        }
    }

    /// The amount is MoonBot's: thousands apart, cents only when the money
    /// has any, and the sign standing away from the number.
    #[test]
    fn money_reads_with_its_thousands_apart() {
        assert_eq!(grouped(500_000.0), "500 000");
        assert_eq!(grouped(1_500.37), "1 500.37");
        assert_eq!(grouped(999.0), "999");
        assert_eq!(grouped(1_234_567.5), "1 234 567.50");
        assert_eq!(grouped(0.0), "0");
        assert_eq!(signed_usdt(20.05), "+ 20.05 USDT");
        assert_eq!(signed_usdt(-12.0), "- 12 USDT");
        assert_eq!(signed_usdt(0.0), "0 USDT");
        // A third of a cent is nothing, and nothing takes no sign — either
        // way round, or one outcome reads as a win and as a loss.
        assert_eq!(signed_usdt(-0.003), "0 USDT");
        assert_eq!(signed_usdt(0.003), "0 USDT");
        assert_eq!(pct1(-0.01), "0.0%");
        assert_eq!(pct1(0.01), "0.0%");
        assert_eq!(pct1(-0.48), "-0.5%");
        assert_eq!(pct1(4.1), "+4.1%");
        assert_eq!(one(100.0), "100");
        assert_eq!(one(99.94), "99.9");
    }

    #[test]
    fn a_summary_counts_the_day_and_names_its_edges() {
        let rows = [
            row("SBER", 125.4),
            row("GAZP", -12.0),
            Row {
                close_date: 500,
                ..row("OLD", 900.0)
            },
            Row {
                emulator: true,
                ..row("EMU", 500.0)
            },
        ];
        let text = daily_text(rows.iter(), 1_000);
        assert!(text.contains("deals 2 (1 up / 1 down)"), "{text}");
        assert!(text.contains("+113.40 USDT"), "{text}");
        assert!(text.contains("best SBER +125.40 USDT"), "{text}");
        assert!(text.contains("worst GAZP -12.00 USDT"), "{text}");
        assert!(!text.contains("OLD") && !text.contains("EMU"), "{text}");
        let quiet = daily_text([].iter(), 1_000);
        assert!(quiet.contains("no deals closed today"), "{quiet}");
    }

    #[test]
    fn a_command_survives_a_group_and_a_missing_slash() {
        let of = |t: &str| command_of(t).map(|(c, a)| (c, a.join(" ")));
        assert_eq!(of("/status"), Some(("status".into(), String::new())));
        // A group addresses the bot by name, and MoonBot's own words have no
        // slash at all.
        assert_eq!(
            of("/status@aster_core_bot"),
            Some(("status".into(), String::new()))
        );
        assert_eq!(of(" TALK "), Some(("talk".into(), String::new())));
        // A sentence is a sentence: this chat can panic-exit every position.
        assert_eq!(of("stop worrying about it"), None);
        assert_eq!(of("panic, I forgot to top up the account"), None);
        assert_eq!(of("talk to you later"), None);
        assert_eq!(of("/"), None);
        // A bare `help` is how somebody asks what this bot can do.
        assert_eq!(of("help"), Some(("help".into(), String::new())));
        assert_eq!(of("/chart sber 5"), Some(("chart".into(), "sber 5".into())));
        assert_eq!(of("   "), None);
    }

    #[test]
    fn the_status_reads_as_the_core_at_a_glance() {
        let status = control::Status {
            uptime_s: 3_725,
            account: "mainnet 0x21cF…1bb0".into(),
            trading: true,
            feed: true,
            warmup_done: true,
            running: true,
            market_stopped: false,
            circuit_stopped: None,
            markets: 2_454,
            strategies: vec![
                control::Strategy {
                    id: 1,
                    name: "strike shares".into(),
                    kind: "MoonStrike".into(),
                    checked: true,
                    pool: 100,
                    filtered: Vec::new(),
                },
                control::Strategy {
                    id: 2,
                    name: "hook futures".into(),
                    kind: "MoonHook".into(),
                    checked: false,
                    pool: 0,
                    filtered: Vec::new(),
                },
            ],
            orders: Vec::new(),
            // `status_text` counts the deals itself out of the rows: these
            // are here to be ignored, and a wrong one must not show up in it.
            profit: control::Profit {
                day_total: 1_240.5,
                day_trades: 14,
                hour_total: -12.0,
                hour_trades: 2,
                report_total: -9_999.0,
                report_trades: 999,
            },
            streams: vec![
                control::Stream {
                    name: "trades#0".into(),
                    alive: true,
                },
                control::Stream {
                    name: "orders".into(),
                    alive: false,
                },
            ],
            settings: control::SettingsView::from(&Settings::default()),
            // The chat's `/status` says nothing about either: the rules live
            // in the terminal and the thresholds are the picture's business.
            auto_stop: control::AutoStopView::default(),
            terminal_shots: None,
        };
        // Two deals closed today, one of them inside the hour; the third is
        // the terminal's kind of row — bought, still open — and no deal yet.
        let now_ms = 1_790_542_200_000;
        let now_s = now_ms / 1000;
        let rows = [
            Row {
                close_date: now_s - 100,
                ..row("SBER", 125.4)
            },
            Row {
                close_date: msk_midnight(now_ms) / 1000 + 60,
                ..row("GAZP", -12.0)
            },
            Row {
                closed: false,
                close_date: 0,
                buy_date: now_s - 60,
                ..row("OPEN", 0.0)
            },
        ];
        let text = status_text(&status, rows.iter(), now_ms);
        assert!(
            text.contains("running · 1 of 2 strategies checked · up 1h02m"),
            "{text}"
        );
        assert!(text.contains("today 2 deal(s) +113.40 USDT"), "{text}");
        assert!(text.contains("last hour 1 +125.40 USDT"), "{text}");
        assert!(text.contains("streams silent: orders"), "{text}");
        assert!(!text.contains("all 2 live"), "{text}");
        // The switch the chat itself can flip is part of the answer.
        assert!(!text.contains("deal reports are off"), "{text}");
    }

    #[test]
    fn a_chart_sums_up_the_last_bars() {
        let bar = |open: f32, high: f32, low: f32, close: f32| Candle {
            open,
            high,
            low,
            close,
            volume: 10.0,
            time: 0.0,
        };
        let candles = vec![
            bar(100.0, 101.0, 99.0, 100.5),
            bar(100.5, 103.0, 100.0, 102.0),
        ];
        let text = chart_text("SBER", 5, &candles);
        assert!(text.starts_with("SBER 5m · 2 bars"), "{text}");
        assert!(text.contains("last 102 (+2.00%)"), "{text}");
        assert!(text.contains("high 103 · low 99 · volume 20.00"), "{text}");
        assert_eq!(chart_text("SBER", 1, &[]), "SBER: no candles");
        // A window that opened at zero is not a percentage.
        let zero = vec![bar(0.0, 1.0, 0.0, 1.0)];
        assert!(chart_text("X", 1, &zero).contains("(?)"));
    }

    #[test]
    fn a_date_is_the_moscow_day() {
        // 2026-09-27 20:50 UTC is already the 28th in Moscow (23:50 MSK is
        // the 27th's summary, sent an hour before the Moscow midnight).
        assert_eq!(msk_date(1_790_542_200_000), "27.09");
        assert_eq!(msk_date(1_790_542_200_000 + 3_600_000), "28.09");
    }
}
