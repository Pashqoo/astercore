//! The core's control channel: one queue of commands the trading loop answers
//! between two UDP steps.
//!
//! Everything that is not the terminal — the web page, and later the Telegram
//! chat — talks to the core through here, on a thread of its own, and waits for
//! the answer on a reply channel it sends along with the command. The loop
//! itself never blocks on any of them: it drains what is queued, answers it,
//! and goes on. A caller that gave up is a `send` that fails and is dropped.
//!
//! Stopping goes the same way, because the loop owns the state that has to
//! reach the disk first: a `Shutdown` or `Restart` saves the orders and leaves
//! the halt behind in [`Control`], which `main::run` reads on its next
//! iteration and turns into the process exit code — `0` for a stop that was
//! asked for, [`RESTART_CODE`] for one the supervisor is meant to undo.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Once;
use std::time::Duration;

use moonproto::server::codec::market_data::Candle;
use serde::Serialize;

use crate::autostop;
use crate::settings::{self, Events, Settings, Shots};

/// Exit code of a restart the supervisor is meant to undo: `Restart=on-failure`
/// sees a failure, brings the core back, and a plain stop (0) it leaves alone.
pub const RESTART_CODE: u8 = 70;
/// Feed client id reserved for the core's own candle requests, so a reply
/// never goes looking for a terminal session (`u64::MAX` is not a client id
/// any handshake produces).
pub const CLIENT_ID: u64 = u64::MAX;
/// Commands answered per loop iteration: a flood must not starve the UDP step.
const BATCH: usize = 64;
/// A candle request whose reply never came is dropped after this long; the
/// caller sees the closed channel.
///
/// Generous on purpose: a cold history is a REST call on the feed's one
/// worker, behind whatever the terminal asked for first. Nothing waits on the
/// trading loop meanwhile — the caller is a thread of its own.
pub const CHART_WAIT_MS: i64 = 90_000;

/// How a command's answer travels back. A closed channel means the caller
/// stopped waiting — never an error worth a line.
pub type Reply<T> = Sender<T>;
/// The one [`ControlCmd::Screen`] refusal that is not the caller's fault: the
/// loop answers one catalog sweep per step and this one came second.
///
/// A constant because two sides read it and neither may guess: the loop
/// writes it, and `web` turns it into a 503 rather than the 400 every other
/// refusal earns — «ask again» behind a client error is how a caller learns
/// not to.
pub const SCREEN_BUSY: &str = "the core screened once this step; ask again";

/// Where a `Chart` answer goes: the candles, or why there are none.
pub type ChartReply = Reply<Result<Vec<Candle>, String>>;

pub enum ControlCmd {
    Status(Reply<Status>),
    /// Start or stop every strategy, as the terminal's own button does; the
    /// answer is the flag as it now stands.
    Strategies {
        start: bool,
        reply: Reply<bool>,
    },
    /// Panic exit on every position of the core. Answer: how many were sent.
    PanicAll(Reply<usize>),
    /// Save and exit 0. Live exchange orders are left where they are — they are
    /// restored at the next start; withdrawing them is `PanicAll`, on purpose
    /// a separate button.
    Shutdown(Reply<()>),
    /// Save and exit [`RESTART_CODE`], for the supervisor to bring back.
    Restart(Reply<()>),
    /// Candles for a deal's chart: `minutes` per bar of `market`.
    Chart {
        market: String,
        minutes: i64,
        reply: Reply<Result<Vec<Candle>, String>>,
    },
    /// The page changed settings: merge the patch, save `data/config.json` and
    /// apply what applies without a restart. `Err` means the whole patch was
    /// refused and nothing changed; [`Applied`] says what did.
    ///
    /// The loop does the merging because it owns the settings: the page is
    /// never shown the bot token or the password, and a whole [`Settings`] from
    /// a form would also revert whatever the chat's `talk` flipped while the
    /// form was open.
    SettingsEdit {
        edit: Box<settings::Edit>,
        reply: Reply<Result<Applied, String>>,
    },
    /// The page's «Leverage» tab: the same settings the terminal's «Настройка плеча» sends,
    /// applied at once. `Err` — nothing was changed and why.
    Leverage {
        edit: LevEdit,
        /// `true` when the settings switch something on (a pass has work to do).
        reply: Reply<Result<bool, String>>,
    },
    /// The page's strategy helper: screen the whole catalog with one
    /// strategy's settings, `fields` moved as the operator moved them. Read
    /// only — nothing of this reaches the strategy, the file or the terminal;
    /// it answers «which markets would this watch, and which of them would it
    /// enter right now».
    ///
    /// The loop does the work because the screener's context is the loop's:
    /// the catalog and the tick windows the pass measures against. A snapshot
    /// of those handed to the web thread would be a picture of the market as
    /// it was, screened against settings the operator is moving now.
    Screen {
        strategy_id: u64,
        /// Schema field name → its value as the form has it. A field not
        /// here keeps whatever the strategy holds.
        fields: Vec<(String, String)>,
        reply: Reply<Result<Screen, String>>,
    },
    /// Put the core's state in words for the chat (`/status`, `/profit`):
    /// the loop holds the report rows and the strategy list, so the answer is
    /// built where they are and travels back as the finished message.
    Text {
        topic: Topic,
        reply: Reply<String>,
    },
    /// MoonBot's `talk` / `silent` from the chat: deal reports on or off. No
    /// reply channel — the loop saves the file and answers the chat itself,
    /// the way an approved chat is answered.
    Talk(bool),
    /// The Telegram poller approved a chat by PIN. Only the chat id travels:
    /// the loop owns the settings, so it is the one that writes the field and
    /// saves the file — a thread that carried its own whole copy would revert
    /// whatever was edited while its long poll was in flight.
    ChatApproved(i64),
}

/// What an accepted [`ControlCmd::SettingsEdit`] did.
///
/// A failed save is part of the answer and not an error of its own: the merge
/// is live either way, and the page has to say so — the alternative is an
/// operator who reads «refused» and does not know the core is already running
/// on a setting that the next start will not find.
#[derive(Debug, Default, Serialize)]
pub struct Applied {
    /// Changed fields that only take effect at the next start.
    pub restart: Vec<&'static str>,
    /// Why the file did not take it; `None` when it did.
    pub unsaved: Option<String>,
}

/// What the chat asked about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Topic {
    Status,
    Profit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Halted {
    Stop,
    Restart,
}

impl Halted {
    pub fn code(self) -> u8 {
        match self {
            Self::Stop => 0,
            Self::Restart => RESTART_CODE,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }
}

/// Set by the signal handler only: an `AtomicBool` store is all a handler may
/// safely do.
static SIGNALLED: AtomicBool = AtomicBool::new(false);
static INSTALL: Once = Once::new();

/// `SIGTERM` (`systemctl stop`, `deploy`) and `SIGINT` (a hand on the console)
/// become the same graceful stop as the page's button: the flag is picked up by
/// the loop, which saves and exits 0.
///
/// The handler puts the default action back, so a core that is wedged
/// somewhere else in the loop still dies on the second signal.
#[cfg(unix)]
pub fn install_signals() {
    extern "C" fn on_signal(sig: libc::c_int) {
        SIGNALLED.store(true, Ordering::Relaxed);
        // Async-signal-safe, and the point of doing it here: the next one is
        // the operator's escape hatch.
        unsafe { libc::signal(sig, libc::SIG_DFL) };
    }
    INSTALL.call_once(|| {
        for sig in [libc::SIGTERM, libc::SIGINT] {
            // Through a pointer: casting a function item straight to an
            // integer is a warning, and `-D warnings` makes it an error.
            unsafe { libc::signal(sig, on_signal as *const () as libc::sighandler_t) };
        }
    });
}

#[cfg(not(unix))]
pub fn install_signals() {}

/// The queue and the halt the loop and `main::run` share.
pub struct Control {
    /// Kept so the receiver never reports a disconnect while the core lives,
    /// and handed out to every control thread.
    tx: Sender<ControlCmd>,
    rx: Receiver<ControlCmd>,
    /// 0 = none, else `Halted` + 1: the first request wins.
    halt: AtomicU8,
}

impl Control {
    /// The channel and the halt. The signal handlers are the process's own —
    /// `main` installs them with [`install_signals`], so building a `Control`
    /// in a test does not change how that test's binary answers Ctrl-C.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            tx,
            rx,
            halt: AtomicU8::new(0),
        }
    }

    /// A sender for a control thread (the web page, the Telegram poller).
    pub fn sender(&self) -> Sender<ControlCmd> {
        self.tx.clone()
    }

    /// Commands to answer this iteration, oldest first.
    pub fn drain(&self) -> impl Iterator<Item = ControlCmd> + '_ {
        self.rx.try_iter().take(BATCH)
    }

    /// Ask the loop to stop; the first request decides, so a stop that is
    /// already saving is not turned into a restart.
    pub fn request(&self, halt: Halted) {
        let code = match halt {
            Halted::Stop => 1,
            Halted::Restart => 2,
        };
        let _ = self
            .halt
            .compare_exchange(0, code, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// What was asked for, if anything: read by `main::run` after every pump.
    pub fn halted(&self) -> Option<Halted> {
        match self.halt.load(Ordering::SeqCst) {
            1 => Some(Halted::Stop),
            2 => Some(Halted::Restart),
            _ => None,
        }
    }

    /// A signal arrived since the last call (once per signal).
    pub fn signalled(&self) -> bool {
        SIGNALLED.swap(false, Ordering::Relaxed)
    }
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AskError {
    /// The core is not answering its queue (stopped, or stopping).
    Gone,
    Timeout,
}

impl std::fmt::Display for AskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gone => f.write_str("the core is not taking commands"),
            Self::Timeout => f.write_str("the core did not answer in time"),
        }
    }
}

/// Send one command and wait `wait` for its answer. Every caller is a thread
/// that must not hang on a busy or stopping core, so the wait is bounded and
/// the failure is a value.
pub fn ask<T>(
    tx: &Sender<ControlCmd>,
    make: impl FnOnce(Reply<T>) -> ControlCmd,
    wait: Duration,
) -> Result<T, AskError> {
    let (reply, answers) = mpsc::channel();
    tx.send(make(reply)).map_err(|_| AskError::Gone)?;
    answers.recv_timeout(wait).map_err(|e| match e {
        RecvTimeoutError::Timeout => AskError::Timeout,
        RecvTimeoutError::Disconnected => AskError::Gone,
    })
}

/// The leverage settings as the page sends them: the terminal's window, field for field.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LevEdit {
    pub auto_max_order: bool,
    pub auto_lev_up: bool,
    pub auto_isolated: bool,
    pub auto_cross: bool,
    pub auto_fix_lev: bool,
    pub fix_lev: i32,
    pub lev_control: String,
}

/// The leverage management as the page shows it.
#[derive(Debug, Default, Serialize)]
pub struct LeverageView {
    /// An account is there to act on the settings.
    pub applies: bool,
    /// The settings the core holds (from the terminal or the page); `None` before the first.
    pub config: Option<LevEditView>,
    /// The Config line read the way the core reads it: `def` and the explicit rules.
    pub limits: Vec<String>,
    pub last: crate::levman::Last,
}

#[derive(Debug, Serialize)]
pub struct LevEditView {
    pub auto_max_order: bool,
    pub auto_lev_up: bool,
    pub auto_isolated: bool,
    pub auto_cross: bool,
    pub auto_fix_lev: bool,
    pub fix_lev: i32,
    pub lev_control: String,
}

/// What the page shows: the whole core in one answer, so a reader never has to
/// stitch two states from two moments together.
#[derive(Debug, Serialize)]
pub struct Status {
    pub uptime_s: i64,
    /// The account the core trades on, as the page names it: the API
    /// wallet's signer, shortened (`account.rs`), or `none`.
    pub account: String,
    /// Orders reach the exchange (a key file the exchange accepted).
    pub trading: bool,
    /// A market-data feed is running.
    pub feed: bool,
    pub warmup_done: bool,
    pub running: bool,
    /// A market panic stopped the strategies («Restart if» may start them).
    pub market_stopped: bool,
    /// A circuit breaker stopped them: which.
    pub circuit_stopped: Option<String>,
    pub markets: usize,
    pub strategies: Vec<Strategy>,
    pub orders: Vec<Order>,
    pub profit: Profit,
    pub streams: Vec<Stream>,
    pub settings: SettingsView,
    /// The terminal's auto-stop and panic rules, as the core is running under
    /// them right now.
    pub auto_stop: AutoStopView,
    /// The terminal's own picture thresholds while it is connected: they win
    /// over `settings.telegram_shots` (Ф4), so the page can say which ones
    /// actually decide instead of showing a form that does not.
    pub terminal_shots: Option<Shots>,
    pub leverage: LeverageView,
}

/// The terminal's auto-stop and panic rules as the page states them.
///
/// Read only, because they arrive with the terminal's `TClientSettings` and
/// are edited there — the page says what the core will act on, it does not
/// offer to change it. Every field carries its unit in its name and none of
/// them is a bare tuple: a loss limit shown in the wrong unit, or a window
/// read as hours when it is seconds, misstates the risk the core is actually
/// running under, and nothing on the page would give that away.
#[derive(Debug, Default, Serialize)]
pub struct AutoStopView {
    pub by_trades: Option<ByTrades>,
    pub by_time: Option<ByTime>,
    /// The stop also panic-sells every position of the core.
    pub sell_all: bool,
    /// Emulator deals count towards the loss.
    pub with_emulator: bool,
    /// BTC's hourly delta that trips the panic on a fall, **sign
    /// included**: the core holds this threshold as a positive magnitude and
    /// trips at minus it (`autostop::market_past` compares `delta <= -drop`,
    /// and `from_config` takes its absolute value), so it is negated here
    /// rather than left for the page to remember. A fall threshold printed
    /// without its sign reads as a rise, which is the opposite alarm.
    pub panic_fall_pct: Option<f64>,
    /// … and the one that trips it on a rise, positive.
    pub panic_rise_pct: Option<f64>,
    /// The markets' mean hourly delta that trips it on a fall, negative.
    pub panic_market_fall_pct: Option<f64>,
    pub restart_band: Option<RestartBand>,
    /// API errors in the last minute.
    pub errors: Option<CircuitView>,
    /// Median round trip of the answered API calls, ms.
    pub ping: Option<CircuitView>,
}

/// Stop when the last `trades` closed deals lost more than `loss_usdt`.
#[derive(Debug, Serialize)]
pub struct ByTrades {
    pub loss_usdt: f64,
    pub trades: usize,
}

/// Stop when `window_s` seconds lost more than `loss_usdt` over at least
/// `min_trades` deals.
#[derive(Debug, Serialize)]
pub struct ByTime {
    pub loss_usdt: f64,
    pub window_s: i64,
    pub min_trades: usize,
}

/// Start again once BTC's hourly delta is back inside the band and the
/// market's above its floor.
#[derive(Debug, Serialize)]
pub struct RestartBand {
    pub low_pct: f64,
    pub high_pct: f64,
    pub market_min_pct: f64,
}

/// One circuit breaker of the terminal's auto-start tab.
#[derive(Debug, Serialize)]
pub struct CircuitView {
    /// Trips at or above it (errors a minute, or ms).
    pub level: i64,
    pub sell_all: bool,
    /// Seconds after the stop before the strategies start again; `None` = it
    /// stays stopped. Held as milliseconds inside the core, divided here so
    /// the page never has to know that.
    pub restart_s: Option<i64>,
}

impl From<&autostop::Rules> for AutoStopView {
    fn from(r: &autostop::Rules) -> Self {
        let circuit = |c: &autostop::Circuit| CircuitView {
            level: c.level,
            sell_all: c.sell_all,
            restart_s: c.restart_ms.map(|ms| ms / 1000),
        };
        Self {
            by_trades: r
                .by_trades
                .map(|(loss_usdt, trades)| ByTrades { loss_usdt, trades }),
            by_time: r.by_hours.map(|(loss_usdt, window_s, min_trades)| ByTime {
                loss_usdt,
                window_s,
                min_trades,
            }),
            sell_all: r.sell_all,
            with_emulator: r.with_emulator,
            panic_fall_pct: r.panic_drop.map(|drop| -drop),
            panic_rise_pct: r.panic_rise,
            panic_market_fall_pct: r.panic_market_drop.map(|drop| -drop),
            restart_band: r
                .restart
                .map(|(low_pct, high_pct, market_min_pct)| RestartBand {
                    low_pct,
                    high_pct,
                    market_min_pct,
                }),
            errors: r.errors.as_ref().map(circuit),
            ping: r.ping.as_ref().map(circuit),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Strategy {
    pub id: u64,
    pub name: String,
    pub kind: String,
    pub checked: bool,
    /// Markets the screener watches for this strategy right now.
    ///
    /// The volume bounds and the deltas no longer shrink it — they gate the
    /// entry on a market of the pool — so the pool is what the white list or
    /// `DynWL_Count` says, and a strategy watching 100 markets while entering
    /// 12 is normal. `filtered` is the difference, and without it the page
    /// would show a busy pool and an idle strategy with nothing in between.
    pub pool: usize,
    /// Of that pool, how many markets each filter was the first to refuse at
    /// the last recompute; only the ones that refused anything, in the order
    /// the filters are asked.
    pub filtered: Vec<FilterCount>,
}

/// One strategy's screener and Filters tab run over the whole catalog: what
/// the page's helper draws.
///
/// Nothing here is saved anywhere. The operator moves the settings on the
/// page, the core answers which markets that would watch and which of them it
/// would enter right now, and the strategy itself is untouched — it stays the
/// terminal's to edit.
#[derive(Debug, Serialize)]
pub struct Screen {
    pub strategy_id: u64,
    pub name: String,
    pub kind: String,
    /// Why this strategy trades nothing as it stands — the same three a pass
    /// reports, in the same order: a screener that produces no pool
    /// (`screener::problem`), a `WorkingTime` that is not understood, a
    /// filter setting that cannot be judged.
    ///
    /// Only the first empties the pool, and then every market reads `nopool`.
    /// The other two leave the seats exactly as they are — the screener works,
    /// the strategy still enters nothing — so the table stays worth reading
    /// and this line is what says the strategy is stopped anyway.
    pub problem: Option<String>,
    /// List entries that name no market of the catalog
    /// (`screener::unknown_symbols`). Not a `problem` and never shown as one:
    /// it empties nothing, the pool beside it is real, and the page paints it
    /// a warning so an operator reads «fix this word», not «this strategy is
    /// stopped».
    pub unknown: Option<String>,
    /// The warm-up is still filling the tick windows. Said out loud because
    /// half the deltas read `waiting` until it is done, and a table taken now
    /// is a table of a market the core has not finished measuring.
    pub warming: bool,
    /// The settings this answer was screened with, as the core read them
    /// back: the page fills its form from here, so what it shows is what was
    /// actually asked and never the form's own idea of it.
    pub fields: Vec<ScreenField>,
    /// The ranking in force: `DynWL_SortBy` and whether it counts down.
    /// `None` = `DynWL_Count` is 0 and nothing is ranked.
    pub sort_by: Option<String>,
    pub sort_desc: bool,
    /// The ranking key's unit: USDT of turnover, against a signed per
    /// cent. The key is the one number with no check behind it to carry its
    /// unit, and USDT printed as a percentage read as nonsense.
    pub sort_turnover: bool,
    /// `DynWL_Count`: where the line across the ranking falls.
    pub count: usize,
    /// The Filters tab as the checks an entry passes, in the order they are
    /// asked — one column of the table each.
    pub checks: Vec<ScreenCheck>,
    pub markets: Vec<ScreenMarket>,
    /// Markets in the pool, and of those the ones every check passes right
    /// now (see [`ScreenMarket::entered`] for what that does and does not
    /// claim). Counted where the rule lives rather than on the page: the same
    /// walk that decides each row decides these.
    pub pool: usize,
    pub entered: usize,
}

/// One control of the helper's form.
#[derive(Debug, Serialize)]
pub struct ScreenField {
    pub name: String,
    pub value: String,
    /// `edit`, `check` or `pick`.
    pub ui: &'static str,
    /// Choices of a `pick`; empty otherwise.
    pub choices: Vec<String>,
    /// The card it belongs to (`strategies::SCREEN_FIELDS`).
    pub card: String,
}

/// One check of the Filters tab as a column.
#[derive(Debug, Serialize)]
pub struct ScreenCheck {
    pub what: String,
    /// The corridor, `None` on a side with no bound. Infinities do not
    /// survive JSON, and an open side printed as 0 would read as a bound of
    /// zero — which is the one thing MoonBot's «0 = not checked» does not
    /// mean.
    pub lo: Option<f64>,
    pub hi: Option<f64>,
    /// USDT of turnover, against a signed per cent.
    pub turnover: bool,
    /// A leverage in `x` (neither of the two).
    pub leverage: bool,
}

/// One market of the catalog under these settings.
#[derive(Debug, Serialize)]
pub struct ScreenMarket {
    pub symbol: String,
    /// Instrument class (`MarketTags`), empty for one the catalog did not
    /// classify.
    pub class: String,
    /// Where it stands in the screener: `pool`, `ranked`, `unranked`,
    /// `dynblack`, `black`, `notlisted`, `otherclass`, `tokentags`, `nopool`.
    pub seat: &'static str,
    pub rank: Option<usize>,
    /// The ranking key's value, in the key's own unit.
    pub key: Option<f64>,
    /// The market's value per check, in the checks' order; `None` where the
    /// windows do not measure it yet.
    pub values: Vec<Option<f64>>,
    /// One letter per check, same order: `p` inside its corridor, `r`
    /// outside, `w` not measurable yet (the check WAITS — it is not a
    /// refusal), `b` a check that cannot be asked at all.
    pub states: String,
    /// The check that refuses the entry: the first that is neither a pass nor
    /// a wait, which is exactly where the entry gate stops. `None` = no check
    /// refuses it.
    pub refused: Option<usize>,
    /// In the pool AND refused by no check. The one green in the table, and
    /// the core's answer rather than the page's, so it cannot disagree with
    /// the gate.
    ///
    /// It is the screener and the Filters tab, and nothing else: whether an
    /// order actually goes out is the rest of the pass — `MaxMarkets`,
    /// `WorkingTime`, the penalties, the session guards, a position already
    /// open — and a helper that claimed to answer that would be wrong on
    /// every market the strategy is already standing on.
    pub entered: bool,
}

/// One filter of the Filters tab and how much of the pool it holds back.
#[derive(Debug, Serialize)]
pub struct FilterCount {
    /// The field's own name, as the core's log line spells it.
    pub what: String,
    pub markets: usize,
}

#[derive(Debug, Serialize)]
pub struct Order {
    pub id: u64,
    pub market: String,
    pub short: bool,
    pub status: u8,
    pub strategy_id: u64,
    pub emulator: bool,
    pub panic: bool,
    /// Units of the entry, and what filled of it at which mean price.
    pub quantity: f64,
    pub filled: f64,
    pub entry_price: f64,
    /// Where the exit rests (0 = none yet) and what filled of it.
    pub exit_price: f64,
    pub exit_filled: f64,
}

/// What the page shows about money. Only the page reads it: the terminal gets
/// MoonBot's own counters over the wire (`engine::profit_state`) and the chat
/// counts its own, so this is free to be the three figures a trader asks for
/// rather than the shape the terminal's table happens to need.
///
/// All three count a deal by **when it closed**, the way the chat and the
/// picture thresholds already do, and only the window differs — the last one
/// is every closed deal `data/reports.jsonl` still holds, however old. The
/// distinction from the terminal's counter is the whole point: that one also
/// counts the positions still open, and it was once shown here labelled
/// «day», where it read as a day's loss when it was a week's.
#[derive(Debug, Default, Serialize)]
pub struct Profit {
    /// Closed since trader's midnight.
    pub day_total: f64,
    pub day_trades: i32,
    /// Closed in the last hour.
    pub hour_total: f64,
    pub hour_trades: i32,
    /// Every deal the report still holds, however old. Closed ones only, like
    /// the two above: a position that is open is not a deal yet.
    pub report_total: f64,
    pub report_trades: i32,
}

#[derive(Debug, Serialize)]
pub struct Stream {
    pub name: String,
    pub alive: bool,
}

/// The editable settings as the page needs them: the bot token never leaves
/// the core, only whether there is one.
#[derive(Debug, Serialize)]
pub struct SettingsView {
    pub log_level: String,
    pub log_keep_days: i64,
    pub utc_offset_min: i32,
    pub start_strategies: settings::StartMode,
    pub telegram_token_set: bool,
    pub telegram_chat_id: i64,
    pub telegram_proxy: String,
    pub telegram_daily_at: String,
    pub telegram_ready: bool,
    /// What the core reports; `talk` / `silent` from the chat flips
    /// `events.deals`, so the view has to carry it back.
    pub telegram_events: Events,
    /// The core's own picture thresholds. While a terminal is connected its
    /// `send_shots_config` is the one in force (Ф4), so the page labels these
    /// as the fallback rather than pretending they decide.
    pub telegram_shots: Shots,
    pub web_bind: String,
    pub web_password_set: bool,
}

impl From<&Settings> for SettingsView {
    fn from(s: &Settings) -> Self {
        Self {
            log_level: s.log_level.clone(),
            log_keep_days: s.log_keep_days,
            utc_offset_min: s.utc_offset_min,
            start_strategies: s.start_strategies,
            telegram_token_set: !s.telegram.token.is_empty(),
            telegram_chat_id: s.telegram.chat_id,
            telegram_proxy: s.telegram.proxy.clone(),
            telegram_daily_at: s.telegram.daily_at.clone(),
            telegram_ready: s.telegram_ready(),
            telegram_events: s.telegram.events.clone(),
            telegram_shots: s.telegram.shots.clone(),
            web_bind: s.web.bind.clone(),
            web_password_set: !s.web.password.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_halt_wins_and_the_codes_are_the_units() {
        let control = Control::new();
        assert!(control.halted().is_none());
        control.request(Halted::Stop);
        control.request(Halted::Restart);
        assert_eq!(control.halted(), Some(Halted::Stop));
        assert_eq!(Halted::Stop.code(), 0);
        assert_eq!(Halted::Restart.code(), RESTART_CODE);
    }

    #[test]
    fn a_drained_command_is_taken_once() {
        let control = Control::new();
        let tx = control.sender();
        let (reply, answers) = mpsc::channel();
        tx.send(ControlCmd::PanicAll(reply)).unwrap();
        assert_eq!(control.drain().count(), 1);
        assert_eq!(control.drain().count(), 0);
        // The command was dropped without an answer: the caller is not left
        // waiting for one.
        assert!(answers.recv().is_err());
    }

    /// A core that is not answering its queue must not hang the caller's
    /// thread — the web page has one per request.
    #[test]
    fn ask_gives_up_instead_of_hanging() {
        let control = Control::new();
        let tx = control.sender();
        let err = ask(&tx, ControlCmd::PanicAll, Duration::from_millis(20)).unwrap_err();
        assert_eq!(err, AskError::Timeout);
        drop(control);
        let err = ask(&tx, ControlCmd::PanicAll, Duration::from_millis(20)).unwrap_err();
        assert_eq!(err, AskError::Gone);
    }

    // The signal handler itself is not unit-tested: the flag it sets is
    // process-global, and every other test in this binary drains it through
    // `pump`. `systemctl stop` is verified against the running core instead.

    /// The units are the whole point of this view: the page prints what it is
    /// handed, so a window that is seconds inside the core must not arrive
    /// labelled as anything else, and a restart delay held in milliseconds
    /// must not reach it as a number of seconds a thousand times off.
    #[test]
    fn the_auto_stop_view_keeps_every_unit_where_it_belongs() {
        let rules = autostop::Rules {
            by_trades: Some((150.0, 5)),
            by_hours: Some((400.0, 12 * 3600, 3)),
            sell_all: true,
            with_emulator: false,
            panic_drop: Some(3.0),
            panic_rise: Some(2.0),
            panic_market_drop: Some(1.5),
            restart: Some((-0.5, 0.5, -0.1)),
            errors: Some(autostop::Circuit {
                level: 3,
                sell_all: false,
                restart_ms: Some(120_000),
            }),
            ping: Some(autostop::Circuit {
                level: 1000,
                sell_all: true,
                restart_ms: None,
            }),
        };
        let view = AutoStopView::from(&rules);
        let by_trades = view.by_trades.unwrap();
        assert_eq!((by_trades.loss_usdt, by_trades.trades), (150.0, 5));
        let by_time = view.by_time.unwrap();
        assert_eq!(by_time.window_s, 12 * 3600, "seconds, not hours");
        assert_eq!((by_time.loss_usdt, by_time.min_trades), (400.0, 3));
        let band = view.restart_band.unwrap();
        assert_eq!(
            (band.low_pct, band.high_pct, band.market_min_pct),
            (-0.5, 0.5, -0.1),
            "the band is (low, high, market_min) — market_call reads it in that order"
        );
        assert_eq!(view.errors.unwrap().restart_s, Some(120), "ms -> s");
        assert_eq!(view.ping.unwrap().restart_s, None, "no restart stays none");
        // The core stores +3 and trips at −3; the view is what is shown.
        assert_eq!(
            view.panic_fall_pct,
            Some(-3.0),
            "a fall threshold is negative"
        );
        assert_eq!(
            view.panic_rise_pct,
            Some(2.0),
            "a rise threshold is positive"
        );
        assert_eq!(view.panic_market_fall_pct, Some(-1.5));
        assert!(view.sell_all && !view.with_emulator);

        // Nothing configured is nothing shown, not a page full of zeroes.
        let empty = AutoStopView::from(&autostop::Rules::default());
        assert!(empty.by_trades.is_none() && empty.by_time.is_none());
        assert!(empty.restart_band.is_none() && empty.errors.is_none());
    }

    #[test]
    fn the_view_never_carries_the_bot_token() {
        let mut settings = Settings::default();
        settings.telegram.token = "123:secret".into();
        settings.web.password = "pw".into();
        let view = SettingsView::from(&settings);
        let json = serde_json::to_string(&view).unwrap();
        assert!(!json.contains("secret") && !json.contains("pw"), "{json}");
        assert!(view.telegram_token_set && view.web_password_set);
        assert!(!view.telegram_ready, "no approved chat yet");
    }
}
