//! The terminal's «Настройка плеча» (MoonBot's leverage management), served by the core: the
//! terminal sends its whole `TLevManageCommand` on Apply (the page's Leverage tab sends the same
//! settings; the newer terminal's «Настройки ядра» window sends them inside the shared config,
//! and is shown what the core holds there), the core keeps it, applies it at once
//! and again every hour (MoonBot checks hourly too: the brackets change on the exchange's side).
//! A pass that does not cover the account (a 429, the network, the site's figures missing) is
//! made again sooner: 5 min, doubling while they keep falling short, back to the hour at most.
//!
//! What is asked of a market, in MoonBot's order of priority:
//! 1. the position limit of the Config line (`5000 def`, `10k def 30k BTC ETH`) with «Авто плечи
//!    по макс. ордеру»: the highest leverage whose bracket still holds that notional;
//! 2. else the fixed `Auto Leverage N`;
//! 3. else the leverage is left as it is. A limit of `0` means the limit does not manage that
//!    market (`0 def 5k TRX`); the fixed leverage, if ticked, still applies to it.
//!
//! `Allow leverage Up` decides whether a leverage below the target is raised; one above it is
//! always lowered. The fixed leverage is a target to be held, so it is raised without that
//! flag. `Auto Isolated` / `Auto Cross` set the margin type. A market with an open position or
//! a resting order is never touched (`setup::plan_for`): the exchange refuses a margin change
//! there, and a leverage change would move the liquidation of a live position.
//!
//! An entry the exchange refuses for the symbol's notional limit (`-5018`, `Msg::Capped`) is
//! MoonBot's «Auto Leverage»: that market is lowered at once, below the leverage refused, past
//! the strategies' resting orders (the exchange refuses a margin change there, not a leverage
//! one: `-4047` is the margin type's), and no pass raises it back to the refused leverage for
//! [`CAP_HOLD`] — the site's figures put WLFIUSDT back on 5x two hours after it was lowered off
//! it (07.10), and the strategies were refused there again.
//!
//! Every pass reads the account itself (positions, orders, brackets, the market list: a
//! market listed since the last pass is seen), on a thread of its own with its own client, and
//! sends one call at a time (`setup::apply`).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use moonproto::server::codec::ui;
use moonproto::shared_config::SharedConfig;
use moonproto::LevManage;

use crate::aster::json::{PositionRisk, SymbolBrackets};
use crate::aster::rest::{self, Rest};
use crate::aster::sign::Signer;
use crate::model::Catalog;
use crate::setup::{self, Want};

/// How often the account is looked over without being asked (MoonBot: once an hour).
const PERIOD: Duration = Duration::from_secs(3600);

/// How soon a pass that did not cover the account is made again: one stopped by a 429 or the
/// network, or one that went without the site's figures and so raised nothing. Inside
/// [`OI_TTL`], so the figures the short pass did read are not asked for again. It doubles with
/// each short pass in a row, up to [`PERIOD`]: a ban or an outage is not probed every 5 min.
const RETRY: Duration = Duration::from_secs(300);

/// How long a market refused for its notional limit (`Msg::Capped`) is kept below the leverage
/// refused.
const CAP_HOLD: Duration = Duration::from_secs(24 * 3600);

/// How soon a `-5018` pass that could not read the account, or was stopped by a rate limit, is
/// made again: doubled while they keep falling short, up to [`PERIOD`] — a ban or an outage is
/// not read into once a minute.
const CAPPED_RETRY: Duration = Duration::from_secs(60);

/// One position-limit rule of the Config line: a limit for the markets its token names.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    limit: i64,
    /// Upper case; `*` and `?` make it a pattern.
    token: String,
}

/// The Config line, parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Limits {
    /// The limit of `def`, `0` when there is none.
    default: i64,
    rules: Vec<Rule>,
}

impl Limits {
    /// `(number) (coins or def)` groups, as MoonBot reads them: commas and dots are blanks, a
    /// number takes `k`/`m`, words before the first number are ignored, `def` is every market
    /// no rule names. A number that does not fit is not a number.
    pub fn parse(text: &str) -> Self {
        let mut out = Self::default();
        let mut limit = None;
        for word in text.replace([',', '.'], " ").split_whitespace() {
            if let Some(n) = number(word) {
                limit = Some(n);
            } else if let Some(limit) = limit {
                if word.eq_ignore_ascii_case("def") {
                    out.default = limit;
                } else {
                    out.rules.push(Rule {
                        limit,
                        token: word.to_ascii_uppercase(),
                    });
                }
            }
        }
        out
    }

    /// The limit of a market: the last rule that names its base coin or its symbol, else `def`.
    pub fn of(&self, symbol: &str, base: &str) -> i64 {
        self.rules
            .iter()
            .rev()
            .find(|r| glob(&r.token, base) || glob(&r.token, symbol))
            .map_or(self.default, |r| r.limit)
    }

    /// The limits as the core reads them, one line each, for the page.
    pub fn describe(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.default != 0 {
            out.push(format!("def: {}", self.default));
        }
        out.extend(
            self.rules
                .iter()
                .map(|r| format!("{}: {}", r.token, r.limit)),
        );
        out
    }

    fn is_empty(&self) -> bool {
        self.default <= 0 && self.rules.iter().all(|r| r.limit <= 0)
    }
}

fn number(word: &str) -> Option<i64> {
    if !word.as_bytes().first()?.is_ascii_digit() {
        return None;
    }
    let (digits, factor) = match word.as_bytes().last()? {
        b'k' | b'K' => (&word[..word.len() - 1], 1_000),
        b'm' | b'M' => (&word[..word.len() - 1], 1_000_000),
        _ => (word, 1),
    };
    digits.parse::<i64>().ok()?.checked_mul(factor)
}

/// `*` and `?` against `text`, case-insensitive on ASCII.
fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some((b'*', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some((c, rest)) => t.split_first().is_some_and(|(x, tail)| {
                (*c == b'?' || c.eq_ignore_ascii_case(x)) && go(rest, tail)
            }),
        }
    }
    go(pattern.as_bytes(), text.as_bytes())
}

/// The terminal's leverage-management settings as the core acts on them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub auto_max_order: bool,
    pub auto_lev_up: bool,
    pub auto_isolated: bool,
    pub auto_cross: bool,
    pub auto_fix_lev: bool,
    pub fix_lev: i32,
    pub tlg_report: bool,
    /// The Config line as typed, for the page and for sending the settings back to terminals.
    pub text: String,
    pub limits: Limits,
}

impl Config {
    pub fn from_wire(l: &LevManage) -> Self {
        Self {
            auto_max_order: l.auto_max_order,
            auto_lev_up: l.auto_lev_up,
            auto_isolated: l.auto_isolated,
            auto_cross: l.auto_cross,
            auto_fix_lev: l.auto_fix_lev,
            fix_lev: l.fix_lev,
            tlg_report: l.tlg_report,
            text: l.lev_control.clone(),
            limits: Limits::parse(&l.lev_control),
        }
    }

    /// The settings as `TLevManageCommand` for the terminals.
    pub fn to_wire(&self, uid: u64) -> Vec<u8> {
        ui::build_lev_manage(
            uid,
            [
                self.auto_max_order,
                self.auto_lev_up,
                self.auto_isolated,
                self.auto_cross,
                self.auto_fix_lev,
            ],
            self.fix_lev,
            self.tlg_report,
            &self.text,
        )
    }

    /// The settings as the terminal's «Настройки ядра» window holds them: `trading.auto_manage_lev`
    /// and `trading.auto_lev_control` of the shared config, the Config line as typed.
    pub fn from_shared(cfg: &SharedConfig) -> Self {
        let m = &cfg.trading.auto_manage_lev;
        Self {
            auto_max_order: m.auto_max_order,
            auto_lev_up: m.auto_lev_up,
            auto_isolated: m.auto_isolated,
            auto_cross: m.auto_cross,
            auto_fix_lev: m.auto_fix_lev,
            fix_lev: m.fix_lev,
            tlg_report: m.tlg_report,
            text: cfg.trading.auto_lev_control.clone(),
            limits: Limits::parse(&cfg.trading.auto_lev_control),
        }
    }

    /// The settings written into the shared config, for that window to show.
    pub fn write_shared(&self, cfg: &mut SharedConfig) {
        let m = &mut cfg.trading.auto_manage_lev;
        m.auto_max_order = self.auto_max_order;
        m.auto_lev_up = self.auto_lev_up;
        m.auto_isolated = self.auto_isolated;
        m.auto_cross = self.auto_cross;
        m.auto_fix_lev = self.auto_fix_lev;
        m.fix_lev = self.fix_lev;
        m.tlg_report = self.tlg_report;
        cfg.trading.auto_lev_control.clone_from(&self.text);
    }

    /// The settings a terminal or the page may hand over: a whole fixed leverage, a Config line
    /// a person could have typed.
    pub fn check(&self) -> Result<(), String> {
        if !(0..=1000).contains(&self.fix_lev) {
            return Err("the fixed leverage is a whole number from 0 to 1000".into());
        }
        if self.text.len() > 1000 || self.text.chars().any(char::is_control) {
            return Err("the Config line is too long or has a control character".into());
        }
        Ok(())
    }

    /// Whether there is anything to do at all.
    pub fn is_active(&self) -> bool {
        (self.auto_max_order && !self.limits.is_empty())
            || (self.auto_fix_lev && self.fix_lev > 0)
            || self.auto_isolated
            || self.auto_cross
    }

    /// The margin type: both boxes ticked is a contradiction, and isolated is the safer one.
    fn margin(&self) -> Option<&'static str> {
        if self.auto_isolated {
            Some(setup::ISOLATED)
        } else if self.auto_cross {
            Some(setup::CROSSED)
        } else {
            None
        }
    }

    /// What is wanted of one market, given its brackets (`None` brackets: no leverage wanted).
    pub fn want(
        &self,
        symbol: &str,
        base: &str,
        brackets: Option<&SymbolBrackets>,
        oi: Option<&[(i32, f64)]>,
    ) -> Want {
        let mut want = Want {
            margin: self.margin(),
            ..Want::default()
        };
        let Some(max) = brackets.and_then(SymbolBrackets::max_leverage) else {
            return want;
        };
        let limit = if self.auto_max_order {
            self.limits.of(symbol, base)
        } else {
            0
        };
        let by_limit = brackets
            .filter(|_| limit > 0)
            .and_then(|b| b.leverage_for_limit_within(limit as f64, oi.unwrap_or(&[])));
        if by_limit.is_some() {
            want.leverage = by_limit;
            // `oi`: `None` is a network without the site's figures (the table rules); `Some`
            // and empty is a market whose figures were wanted and are missing (a failed read, an
            // unknown symbol): the table may be wrong in either direction, so it lowers a
            // leverage and does not raise one.
            want.raise = self.auto_lev_up && oi.is_none_or(|o| !o.is_empty());
        } else if self.auto_fix_lev && self.fix_lev > 0 {
            want.leverage = Some(self.fix_lev.min(max));
            want.raise = true;
        }
        want
    }
}

/// The raw `TLevManageCommand` the core keeps across restarts, if one is on disk and parses.
pub fn load(path: &Path) -> Option<Vec<u8>> {
    let bytes = fs::read(path).ok()?;
    ui::lev_manage(&bytes).map(|_| bytes)
}

/// Keeps the payload: written beside the file and renamed over it, so a crash leaves the old one.
pub fn save(path: &Path, payload: &[u8]) {
    let tmp = path.with_extension("tmp");
    let written = fs::write(&tmp, payload).and_then(|()| fs::rename(&tmp, path));
    if let Err(e) = written {
        log::warn!("leverage: {} not saved: {e}", path.display());
    }
}

/// What the last pass did, for the page.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Last {
    /// A pass is running now.
    pub running: bool,
    /// When the last pass began (Unix ms; 0 = none yet) and why: start, applied, hourly, retry.
    pub at_ms: i64,
    pub why: String,
    pub markets: usize,
    pub read: usize,
    pub with_brackets: usize,
    pub to_change: usize,
    pub margin_set: usize,
    pub leverage_set: usize,
    pub left_alone: usize,
    pub failed: usize,
    /// Why the pass ended early, if it did.
    pub stopped: Option<String>,
    /// The first markets left alone or failed, with the reason.
    pub notes: Vec<String>,
}

/// The worker's state as the page reads it.
pub type Shared = Arc<Mutex<Last>>;

/// A message to the worker.
pub enum Msg {
    /// The terminal (or the page's Leverage tab) applied these settings: act at once.
    Config(Config),
    /// The exchange refused an entry on `symbol` for its notional limit (`-5018`) at `leverage`
    /// (the account's figure the engine held; `None`: the worker reads it): lower it now, and
    /// keep it below that for [`CAP_HOLD`].
    Capped {
        symbol: String,
        leverage: Option<i32>,
    },
}

/// Markets handled between two reads of the account: a position or an order that appears during
/// a pass is seen at the next chunk (~20 s of calls), not at the next pass. The window is not
/// closed: a leverage change on a live position is not known to be refused on Aster (only
/// Binance's margin refusals are assumed, `setup.rs`).
const CHUNK: usize = 60;
/// Between two reads of the site's open interest: ~5 a second, one host, no weight.
const OI_PACE: Duration = Duration::from_millis(200);
/// How long the site's figures of a symbol are reused.
const OI_TTL: Duration = Duration::from_secs(600);
/// Failed reads in a row after which the pass stops asking the site.
const OI_GIVE_UP: u32 = 3;
/// A `429` on the clock read is waited out this many times, this long each.
const CLOCK_WAITS: u32 = 3;
const CLOCK_WAIT: Duration = Duration::from_secs(20);

/// Starts the worker. With `initial` (the settings of the previous run) the first pass is made
/// at once, as MoonBot checks at a restart. The sender is the engine's; dropping it ends the
/// thread.
pub fn start(rest: Rest, signer: Signer, initial: Option<Config>, status: Shared) -> Sender<Msg> {
    let (tx, rx) = mpsc::channel::<Msg>();
    let mut worker = Worker {
        rest,
        signer,
        config: initial,
        status,
        oi_down: false,
        oi_failures: 0,
        oi_cache: HashMap::new(),
        short_passes: 0,
        clock_at: None,
        caps: HashMap::new(),
        capped: Vec::new(),
        capped_retry_at: None,
        capped_fails: 0,
    };
    thread::Builder::new()
        .name("aster-levman".into())
        .spawn(move || {
            let mut why = Some("start");
            let mut due = Instant::now();
            loop {
                if worker.capped_due() {
                    let run = panic::catch_unwind(AssertUnwindSafe(|| worker.capped_pass()));
                    if run.is_err() {
                        worker.capped.clear();
                        worker.capped_retry_at = None;
                        log::error!("leverage: a -5018 pass panicked");
                    }
                }
                if let Some(reason) = why.take().filter(|_| worker.config.is_some()) {
                    // A pass that panics is lost, not the thread: the next hour tries again.
                    let run = panic::catch_unwind(AssertUnwindSafe(|| worker.pass(reason, &rx)));
                    match run {
                        // New settings came in during the pass: it was dropped, they go first.
                        Ok(Some(newer)) => {
                            worker.config = Some(newer);
                            why = Some("applied");
                            continue;
                        }
                        Ok(None) => {}
                        // Made again it would panic again: the hour stands.
                        Err(_) => {
                            worker.short_passes = 0;
                            log::error!("leverage: a pass panicked, the next is in an hour");
                            let mut last =
                                worker.status.lock().unwrap_or_else(PoisonError::into_inner);
                            last.running = false;
                            last.stopped = Some("the pass panicked".into());
                        }
                    }
                    due = Instant::now() + next_pass(worker.short_passes).0;
                }
                // A `-5018` that came during the pass is acted on now.
                if worker.capped_due() {
                    continue;
                }
                // A `-5018` pass in between does not move the next pass nobody asked for; one
                // to be made again wakes the worker sooner.
                let wake = match worker.capped_retry_at.filter(|_| !worker.capped.is_empty()) {
                    Some(retry) => retry.min(due),
                    None => due,
                };
                match rx.recv_timeout(wake.saturating_duration_since(Instant::now())) {
                    Ok(msg) => {
                        if let Some(c) = take(&mut worker.capped, msg) {
                            worker.config = Some(newest(&rx, c, &mut worker.capped));
                            why = Some("applied");
                        }
                    }
                    Err(RecvTimeoutError::Timeout) if Instant::now() >= due => {
                        why = Some(next_pass(worker.short_passes).1);
                        due = Instant::now() + next_pass(worker.short_passes).0;
                    }
                    // The `-5018` retry is due, not the pass.
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
        })
        .expect("spawn");
    tx
}

/// The wait before the next pass nobody asked for, and its reason on the page, after `short`
/// passes in a row that did not cover the account.
fn next_pass(short: u32) -> (Duration, &'static str) {
    match short {
        0 => (PERIOD, "hourly"),
        n => (
            RETRY.saturating_mul(1 << (n - 1).min(8)).min(PERIOD),
            "retry",
        ),
    }
}

struct Worker {
    rest: Rest,
    signer: Signer,
    config: Option<Config>,
    status: Shared,
    /// The site's open-interest read failed in this pass: the rest of it goes by the table.
    oi_down: bool,
    /// Site reads that failed in a row, after their retry.
    oi_failures: u32,
    /// The site's figures by symbol and when they were read: a second pass minutes after the
    /// first (an Apply) does not read them all again.
    oi_cache: HashMap<String, (Instant, Vec<(i32, f64)>)>,
    /// Passes in a row that did not cover every market with the site's figures; while there
    /// are, the next is sooner than the hour ([`next_pass`]).
    short_passes: u32,
    /// When the clock was last measured: it is measured again only past
    /// [`CLOCK_EVERY`](crate::account::CLOCK_EVERY), as every other thread does.
    clock_at: Option<Instant>,
    /// Markets refused for their notional limit: since when, and the leverage refused, which no
    /// pass sets them at or above for [`CAP_HOLD`] ([`capped_want`]).
    caps: HashMap<String, (Instant, i32)>,
    /// `Msg::Capped` not acted on yet: the next [`Self::capped_pass`] lowers them.
    capped: Vec<(String, Option<i32>)>,
    /// When a `-5018` pass that fell short is made again ([`CAPPED_RETRY`]).
    capped_retry_at: Option<Instant>,
    /// `-5018` passes in a row that fell short.
    capped_fails: u32,
}

/// Settings returned for the caller to take; a `-5018` is queued in `capped` for
/// [`Worker::capped_pass`].
fn take(capped: &mut Vec<(String, Option<i32>)>, msg: Msg) -> Option<Config> {
    match msg {
        Msg::Config(c) => Some(c),
        Msg::Capped { symbol, leverage } => {
            capped.push((symbol, leverage));
            None
        }
    }
}

/// The last of the settings that arrived since the last look, if any; a `-5018` among them is
/// queued, not dropped.
fn arrived(rx: &Receiver<Msg>, capped: &mut Vec<(String, Option<i32>)>) -> Option<Config> {
    let mut newer = None;
    while let Ok(msg) = rx.try_recv() {
        if let Some(c) = take(capped, msg) {
            newer = Some(c);
        }
    }
    newer
}

/// Only the last of several queued settings counts.
fn newest(rx: &Receiver<Msg>, config: Config, capped: &mut Vec<(String, Option<i32>)>) -> Config {
    arrived(rx, capped).unwrap_or(config)
}

impl Worker {
    /// A `-5018` is waiting and not held back by a retry still to come.
    fn capped_due(&self) -> bool {
        !self.capped.is_empty() && self.capped_retry_at.is_none_or(|at| Instant::now() >= at)
    }

    /// The refused leverage of every `-5018` waiting, held at once — a pass running now does
    /// not raise those markets back before [`Self::capped_pass`] lowers them. The lower figure
    /// stands: the engine's can be older than a lowering made since.
    fn note_caps(&mut self) {
        let now = Instant::now();
        for (symbol, leverage) in &self.capped {
            let Some(l) = *leverage else { continue };
            self.caps
                .entry(symbol.clone())
                .and_modify(|(at, held)| {
                    *at = now;
                    *held = (*held).min(l);
                })
                .or_insert((now, l));
        }
    }

    /// The markets of `Msg::Capped`, lowered below the leverage refused at once, their resting
    /// orders notwithstanding; an open position still holds a market as it is. With the
    /// leverage management off nothing is changed: MoonBot's «Auto Leverage» is its own box, and
    /// the core has none.
    fn capped_pass(&mut self) {
        self.capped_retry_at = None;
        // With the leverage management off nothing is held either: turned on later, it would
        // keep a market below a figure this pass said it left alone.
        if self.config.as_ref().is_some_and(Config::is_active) {
            self.note_caps();
        }
        let mut asked = std::mem::take(&mut self.capped);
        let mut seen = HashSet::new();
        asked.retain(|(s, _)| seen.insert(s.clone()));
        let names = asked
            .iter()
            .map(|(s, _)| s.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let Some(config) = self.config.clone().filter(Config::is_active) else {
            log::info!(
                "leverage: -5018 on {names}: the leverage management is off, the leverage is left \
                 as it is"
            );
            return;
        };
        // The room the site showed at the refused leverage is what just ran out. A refused
        // leverage the engine did not know is read from the account (`plans`).
        for (symbol, _) in &asked {
            self.oi_cache.remove(symbol);
        }
        let limits = (config.auto_max_order && !config.limits.is_empty()).then_some(&config.limits);
        let (markets, brackets) = match self.markets_and_brackets(limits) {
            Ok(read) => read,
            Err(e) => {
                log::warn!("leverage: -5018 on {names}: not lowered, made again later: {e}");
                self.retry_capped(asked);
                return;
            }
        };
        let chunk: Vec<(String, String)> = markets
            .into_iter()
            .filter(|(s, _)| seen.contains(s))
            .collect();
        let plans = match self.plans(&config, &chunk, &brackets, true) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("leverage: -5018 on {names}: not lowered, made again later: {e}");
                self.retry_capped(asked);
                return;
            }
        };
        let report = setup::apply(&mut self.rest, &mut self.signer, &plans);
        let untouched = plans.iter().filter(|p| p.is_noop()).count();
        log::info!(
            "leverage: -5018 on {names}: leverage set {}, nothing lower to set {untouched}, left \
             alone {}, failed {}{}",
            report.leverage_set,
            report.skipped.len(),
            report.failed.len(),
            report
                .aborted
                .as_ref()
                .map_or(String::new(), |a| format!(", stopped: {a}")),
        );
        for (symbol, reason) in report.skipped.iter().chain(&report.failed) {
            log::info!("leverage: {symbol}: {reason}");
        }
        // A rate limit stopped it: what was not sent is sent later.
        if report.aborted.is_some() {
            self.retry_capped(asked);
        } else {
            self.capped_fails = 0;
        }
    }

    /// `asked` queued again, [`CAPPED_RETRY`] (doubled per short pass in a row) from now, ahead
    /// of what came meanwhile.
    fn retry_capped(&mut self, mut asked: Vec<(String, Option<i32>)>) {
        asked.append(&mut self.capped);
        self.capped = asked;
        self.capped_fails += 1;
        let wait = CAPPED_RETRY
            .saturating_mul(1 << (self.capped_fails - 1).min(8))
            .min(PERIOD);
        self.capped_retry_at = Some(Instant::now() + wait);
    }

    /// One pass over every trading market. The brackets and the market list are read once, the
    /// positions and orders before each chunk of [`CHUNK`] markets, whose plan is made from that
    /// read and sent at once. Settings that arrive meanwhile end the pass and are returned.
    fn pass(&mut self, why: &str, rx: &Receiver<Msg>) -> Option<Config> {
        let Some(config) = self.config.clone().filter(Config::is_active) else {
            log::info!("leverage: {why}: nothing to manage");
            self.publish(Last {
                at_ms: crate::engine::now_ms(),
                why: why.into(),
                stopped: Some(
                    "nothing to manage: no limit, fixed leverage or margin is switched on".into(),
                ),
                ..Last::default()
            });
            self.short_passes = 0;
            return None;
        };
        self.oi_down = false;
        self.oi_failures = 0;
        let mut last = Last {
            running: true,
            at_ms: crate::engine::now_ms(),
            why: why.into(),
            ..Last::default()
        };
        self.publish(last.clone());
        let limits = (config.auto_max_order && !config.limits.is_empty()).then_some(&config.limits);
        let (markets, brackets) = match self.markets_and_brackets(limits) {
            Ok(read) => read,
            Err(e) => {
                log::warn!("leverage: {why}: not made: {e}");
                self.short_passes += 1;
                last.running = false;
                last.stopped = Some(e);
                self.publish(last);
                return None;
            }
        };
        last.markets = markets.len();
        last.with_brackets = markets
            .iter()
            .filter(|(s, _)| brackets.contains_key(s))
            .count();
        let mut total = setup::Report::default();
        let mut stopped = None;
        let now = Instant::now();
        self.caps
            .retain(|_, (at, _)| now.duration_since(*at) < CAP_HOLD);
        for chunk in markets.chunks(CHUNK) {
            let newer = arrived(rx, &mut self.capped);
            self.note_caps();
            if let Some(c) = newer {
                log::info!(
                    "leverage: {why}: new settings arrived, the pass is dropped after {} markets \
                     (margin set {}, leverage set {}, failed {})",
                    last.read,
                    total.margin_set,
                    total.leverage_set,
                    total.failed.len()
                );
                last.running = false;
                last.stopped = Some("dropped: new settings arrived".into());
                Self::fill(&mut last, &total);
                self.publish(last);
                return Some(newest(rx, c, &mut self.capped));
            }
            let plans = match self.plans(&config, chunk, &brackets, false) {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("leverage: {why}: stopped after {} markets: {e}", last.read);
                    stopped = Some(e);
                    break;
                }
            };
            last.to_change += plans.iter().filter(|p| !p.is_noop()).count();
            last.read += chunk.len();
            let part = setup::apply(&mut self.rest, &mut self.signer, &plans);
            total.margin_set += part.margin_set;
            total.leverage_set += part.leverage_set;
            total.skipped.extend(part.skipped);
            total.failed.extend(part.failed);
            Self::fill(&mut last, &total);
            self.publish(last.clone());
            if part.aborted.is_some() {
                stopped = part.aborted;
                break;
            }
        }
        log::info!(
            "leverage: {why}: {} of {} markets read ({} with brackets), {} to change — margin set \
             {}, leverage set {}, left alone {}, failed {}{}",
            last.read,
            markets.len(),
            last.with_brackets,
            last.to_change,
            total.margin_set,
            total.leverage_set,
            total.skipped.len(),
            total.failed.len(),
            stopped
                .as_ref()
                .map_or(String::new(), |a| format!(", stopped: {a}")),
        );
        for (symbol, reason) in total.skipped.iter().chain(&total.failed).take(20) {
            log::info!("leverage: {symbol}: {reason}");
        }
        Self::fill(&mut last, &total);
        last.running = false;
        if stopped.is_none() && !self.oi_down {
            self.short_passes = 0;
        } else {
            self.short_passes += 1;
            log::info!(
                "leverage: {why}: the pass is made again in {} min",
                next_pass(self.short_passes).0.as_secs() / 60
            );
        }
        last.stopped = stopped;
        self.publish(last);
        None
    }

    /// The counts and the first reasons of `total`, into what the page reads.
    fn fill(last: &mut Last, total: &setup::Report) {
        last.margin_set = total.margin_set;
        last.leverage_set = total.leverage_set;
        last.left_alone = total.skipped.len();
        last.failed = total.failed.len();
        last.notes = total
            .skipped
            .iter()
            .chain(&total.failed)
            .take(20)
            .map(|(symbol, reason)| format!("{symbol}: {reason}"))
            .collect();
    }

    /// The clock measured again when the last measure is older than
    /// [`CLOCK_EVERY`](crate::account::CLOCK_EVERY): a pass asked the gateway once for every chunk
    /// of 60 markets, and two passes a minute apart had the CDN refuse `/fapi/v1/time` to the
    /// whole core (05.10). A `429` (the IP's weight, or that refusal) is waited out
    /// [`CLOCK_WAITS`] times instead of ending the pass. A ban (`418`) is not.
    fn sync_clock(&mut self) -> Result<i64, rest::Error> {
        if self
            .clock_at
            .is_some_and(|at| at.elapsed() < crate::account::CLOCK_EVERY)
        {
            return Ok(self.rest.clock_delta_ms());
        }
        for waited in 0..=CLOCK_WAITS {
            match self.rest.sync_clock() {
                Ok(delta) => {
                    self.clock_at = Some(Instant::now());
                    return Ok(delta);
                }
                Err(e @ rest::Error::Api { status: 429, .. }) if waited < CLOCK_WAITS => {
                    let wait = e.retry_after().unwrap_or_default().max(CLOCK_WAIT);
                    log::info!(
                        "leverage: the exchange says 429, waiting {} s",
                        wait.as_secs()
                    );
                    thread::sleep(wait);
                }
                other => return other,
            }
        }
        unreachable!("the last round returns")
    }

    /// One symbol's figures from the site, [`OI_PACE`] after the last call; a transport failure
    /// is tried once more after a second.
    fn read_oi(&mut self, symbol: &str) -> Result<Vec<(i32, f64)>, rest::Error> {
        thread::sleep(OI_PACE);
        match self.rest.leverage_oi_remaining(symbol) {
            Err(rest::Error::Transport(_)) => {
                thread::sleep(Duration::from_secs(1));
                self.rest.leverage_oi_remaining(symbol)
            }
            other => other,
        }
    }

    fn publish(&self, last: Last) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = last;
    }

    /// The trading markets (symbol, base coin) in symbol order, listed now, and their brackets.
    #[allow(clippy::type_complexity)]
    /// `limits`: the Config line's, when it manages leverage — read against the site's table.
    fn markets_and_brackets(
        &mut self,
        limits: Option<&Limits>,
    ) -> Result<(Vec<(String, String)>, HashMap<String, SymbolBrackets>), String> {
        let err = |what: &'static str| move |e| format!("{what}: {e}");
        self.sync_clock().map_err(err("clock"))?;
        let info = self.rest.exchange_info().map_err(err("exchangeInfo"))?;
        let catalog = Catalog::build(&info);
        let mut markets: Vec<(String, String)> = catalog
            .trading()
            .map(|m| (m.symbol.clone(), m.base.clone()))
            .collect();
        markets.sort();
        let known: HashSet<&str> = markets.iter().map(|(s, _)| s.as_str()).collect();
        let rows = self
            .rest
            .leverage_brackets(&mut self.signer, |s| known.contains(s))
            .map_err(err("leverageBracket"))?;
        // A short answer is not an account: a market missing from it would go unmanaged.
        if rows.is_empty() {
            return Err("no bracket rows".into());
        }
        let mut brackets: HashMap<String, SymbolBrackets> =
            rows.into_iter().map(|b| (b.symbol.clone(), b)).collect();
        // A limit is read against the site's table: its leverage dialog holds an order to these
        // caps, and the signed table does not (05.10: BTC 151–200x holds 400 USDT on the site, the
        // signed table had the core put a 10 000 limit on 200x). Not read, no pass; a market
        // with a limit the site has no usable row for gets no leverage — the signed caps are what
        // raised BTC. A market without a limit (a fixed leverage, a margin type) needs no caps and
        // keeps the signed table where the site has no row.
        if let Some(limits) = limits.filter(|_| self.rest.has_open_interest_feed()) {
            let base: HashMap<&str, &str> = markets
                .iter()
                .map(|(s, b)| (s.as_str(), b.as_str()))
                .collect();
            let mut site: HashMap<String, SymbolBrackets> = self
                .rest
                .site_brackets()
                .map_err(err("site brackets"))?
                .into_iter()
                .filter(|b| {
                    b.brackets
                        .iter()
                        .any(|r| r.initial_leverage > 0 && r.notional_cap > 0.0)
                })
                .map(|b| (b.symbol.clone(), b))
                .collect();
            let mut missing: Vec<&str> = Vec::new();
            brackets.retain(|symbol, slot| match site.remove(symbol) {
                Some(b) => {
                    *slot = b;
                    true
                }
                None => {
                    let b = base.get(symbol.as_str()).copied().unwrap_or("");
                    if limits.of(symbol, b) <= 0 {
                        return true;
                    }
                    missing.push(known.get(symbol.as_str()).copied().unwrap_or(""));
                    false
                }
            });
            if !missing.is_empty() {
                missing.sort_unstable();
                log::warn!(
                    "leverage: {} market(s) not in the site's bracket table, their leverage is left \
                     alone: {}",
                    missing.len(),
                    missing.iter().take(10).copied().collect::<Vec<_>>().join(", ")
                );
            }
        }
        Ok((markets, brackets))
    }

    /// The plan of one chunk of markets, from the account as it is read now. `capped`: the
    /// markets of a `-5018` ([`Self::capped_pass`]), only lowered, resting orders or not.
    fn plans(
        &mut self,
        config: &Config,
        chunk: &[(String, String)],
        brackets: &HashMap<String, SymbolBrackets>,
        capped: bool,
    ) -> Result<Vec<setup::Plan>, String> {
        let err = |what: &'static str| move |e| format!("{what}: {e}");
        // A pass is minutes of writes: the clock is looked at again for every chunk, and
        // measured once it is old (`sync_clock`).
        self.sync_clock().map_err(err("clock"))?;
        // What the site says is left of the open interest at each leverage, for the markets the
        // limit manages (read before the account, so the snapshot `plan_for` decides on is not older
        // than the paced reads): the table alone puts NEAR on 50x where the site allows an order of 0.
        let feed = self.rest.has_open_interest_feed();
        let managed =
            |symbol: &str, base: &str| config.auto_max_order && config.limits.of(symbol, base) > 0;
        let mut oi: HashMap<&str, Vec<(i32, f64)>> = HashMap::new();
        for (symbol, base) in chunk {
            if self.oi_down || !feed || !managed(symbol, base) {
                continue;
            }
            if let Some((at, figures)) = self.oi_cache.get(symbol.as_str()) {
                if at.elapsed() < OI_TTL {
                    oi.insert(symbol.as_str(), figures.clone());
                    continue;
                }
            }
            match self.read_oi(symbol) {
                Ok(figures) => {
                    self.oi_failures = 0;
                    self.oi_cache
                        .insert(symbol.clone(), (Instant::now(), figures.clone()));
                    oi.insert(symbol.as_str(), figures);
                }
                // One answer that does not read: that market gets no figures, so it is not raised.
                Err(e @ rest::Error::Decode(_)) => {
                    log::warn!("leverage: {symbol}: open interest by leverage not read ({e})");
                }
                // The site does not answer: a few in a row and the rest of the pass does not ask
                // again; the markets without figures are lowered by the table but not raised.
                Err(e) => {
                    self.oi_failures += 1;
                    log::warn!(
                        "leverage: {symbol}: open interest by leverage not read ({e}), {} in a row",
                        self.oi_failures
                    );
                    if self.oi_failures >= OI_GIVE_UP {
                        log::warn!("leverage: the rest of the pass raises nothing");
                        self.oi_down = true;
                    }
                }
            }
        }
        let wanted: HashSet<&str> = chunk.iter().map(|(s, _)| s.as_str()).collect();
        let rows = self
            .rest
            .position_risk(&mut self.signer, |s| wanted.contains(s))
            .map_err(err("positionRisk"))?;
        let orders = self
            .rest
            .open_orders(&mut self.signer)
            .map_err(err("openOrders"))?;
        // A market without a row (a pre-listing one) is blocked by `plan_for`, not sent.
        let ordered: HashSet<&str> = orders.iter().map(|o| o.symbol.as_str()).collect();
        let mut by_symbol: HashMap<&str, &PositionRisk> = HashMap::new();
        for r in &rows {
            // Hedge mode gives two rows a symbol: the one that holds a position speaks.
            let slot = by_symbol.entry(r.symbol.as_str()).or_insert(r);
            if slot.amount == 0.0 && r.amount != 0.0 {
                *slot = r;
            }
        }
        Ok(chunk
            .iter()
            .map(|(symbol, base)| {
                let site = oi.get(symbol.as_str()).map(Vec::as_slice);
                let wanted = managed(symbol, base) && feed;
                let mut want = config.want(
                    symbol,
                    base,
                    brackets.get(symbol),
                    wanted.then(|| site.unwrap_or(&[])),
                );
                let row = by_symbol.get(symbol.as_str()).copied();
                // A `-5018` whose leverage the engine did not know: the account's is the one
                // refused.
                if capped && !self.caps.contains_key(symbol.as_str()) {
                    if let Some(l) = row.and_then(|r| r.leverage) {
                        self.caps.insert(symbol.clone(), (Instant::now(), l));
                    }
                }
                let cap = self.caps.get(symbol.as_str()).map(|&(_, l)| l);
                if let Some(refused) = cap {
                    want = capped_want(want, brackets.get(symbol), refused);
                }
                // Only lowered, and so past a resting order: the exchange refuses the margin
                // type there, not the leverage. A market whose refused leverage is unknown is
                // left alone.
                if capped {
                    want.margin = None;
                    want.raise = false;
                    if cap.is_none() {
                        want.leverage = None;
                    }
                }
                let resting = ordered.contains(symbol.as_str()) && !capped;
                let mut plan = setup::plan_for(symbol, row, &want, resting);
                // The table can promise more than the exchange gives (-5018): a raise has the
                // lower brackets to fall back on, never below the leverage the market has.
                // A row that does not state its leverage gets no ladder: there is no floor to keep.
                if let (Some(target), Some(now), Some(b)) = (
                    plan.leverage,
                    row.and_then(|r| r.leverage),
                    brackets.get(symbol),
                ) {
                    plan.fallback = b.leverages_between(Some(now), target);
                }
                plan
            })
            .collect())
    }
}

/// `want` kept below the leverage the exchange refused an entry at (`-5018`): at most the
/// highest bracket leverage under it, which a market without its own target is lowered to. With
/// no bracket under it there is nothing lower to set, and nothing at or above it is.
fn capped_want(want: Want, brackets: Option<&SymbolBrackets>, refused: i32) -> Want {
    let ceiling = brackets.and_then(|b| b.leverages_between(None, refused).first().copied());
    let leverage = match (want.leverage, ceiling) {
        (Some(l), Some(c)) => Some(l.min(c)),
        (None, Some(c)) => Some(c),
        (Some(l), None) => (l < refused).then_some(l),
        (None, None) => None,
    };
    Want { leverage, ..want }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brackets(rows: &[(i32, f64)]) -> SymbolBrackets {
        serde_json::from_value(serde_json::json!({
            "symbol": "BTCUSDT",
            "brackets": rows.iter().map(|(l, c)| serde_json::json!(
                {"initialLeverage": l, "notionalCap": c})).collect::<Vec<_>>(),
        }))
        .unwrap()
    }

    fn cfg(text: &str) -> Config {
        Config {
            auto_max_order: true,
            auto_lev_up: true,
            limits: Limits::parse(text),
            ..Config::default()
        }
    }

    /// A pass that stopped short (a 429, the network) or went without the site's figures is
    /// made again in minutes: an hour would leave markets unmanaged and raises waiting.
    #[test]
    fn a_pass_short_of_the_account_is_made_again_soon() {
        assert_eq!(next_pass(0), (PERIOD, "hourly"));
        let (wait, why) = next_pass(1);
        assert_eq!(why, "retry");
        assert!(wait < PERIOD / 6, "{wait:?}");
        // Falling short again and again (a ban, an outage) backs off to the hour, no further.
        assert_eq!(next_pass(2).0, wait * 2);
        assert_eq!(next_pass(5).0, PERIOD);
        assert_eq!(next_pass(u32::MAX).0, PERIOD);
    }

    #[test]
    fn the_config_line_reads_as_moonbot_documents_it() {
        let l = Limits::parse("10k def 30k BTC ETH");
        assert_eq!(l.of("BTCUSDT", "BTC"), 30_000);
        assert_eq!(l.of("ETHUSDT", "ETH"), 30_000);
        assert_eq!(l.of("ADAUSDT", "ADA"), 10_000);
        let l = Limits::parse("0 def 5k TRX LRC 10k ADA");
        assert_eq!(l.of("ADAUSDT", "ADA"), 10_000);
        assert_eq!(l.of("TRXUSDT", "TRX"), 5_000);
        assert_eq!(l.of("BTCUSDT", "BTC"), 0, "0 = not managed");
        assert_eq!(Limits::parse("5000 def").of("X", "X"), 5_000);
        assert_eq!(Limits::parse("1m def").of("X", "X"), 1_000_000);
    }

    #[test]
    fn a_def_without_a_number_before_it_is_nothing() {
        // The terminal's field is `number first`: `def 200` names no limit.
        let l = Limits::parse("def 200");
        assert!(l.is_empty());
        assert_eq!(l.of("BTCUSDT", "BTC"), 0);
        assert!(Limits::parse("").is_empty());
        assert!(Limits::parse("99999999999999999999 def").is_empty());
    }

    #[test]
    fn a_pattern_and_the_symbol_name_a_market() {
        let l = Limits::parse("1k def 7k 1000* DOG? 9k SOLUSDT");
        assert_eq!(l.of("1000PEPEUSDT", "1000PEPE"), 7_000);
        assert_eq!(l.of("DOGEUSDT", "DOGE"), 7_000, "`?` is one character");
        assert_eq!(l.of("DOGUSDT", "DOG"), 1_000, "and it needs one");
        assert_eq!(l.of("SOLUSDT", "SOL"), 9_000, "a symbol names a market too");
        // The last rule that names a market wins, as MoonBot applies them in order.
        let l = Limits::parse("1k BTC 2k BTC");
        assert_eq!(l.of("BTCUSDT", "BTC"), 2_000);
    }

    #[test]
    fn the_leverage_is_the_highest_that_holds_the_limit() {
        let b = brackets(&[(125, 10_000.0), (50, 50_000.0), (20, 500_000.0)]);
        assert_eq!(b.leverage_for_limit(200.0), Some(125));
        assert_eq!(
            b.leverage_for_limit(10_000.0),
            Some(125),
            "a cap holds its own limit"
        );
        assert_eq!(b.leverage_for_limit(10_001.0), Some(50));
        assert_eq!(
            b.leverage_for_limit(5_000_000.0),
            Some(20),
            "above every cap: the widest bracket"
        );
        assert_eq!(brackets(&[(10, 0.0)]).leverage_for_limit(100.0), None);
    }

    #[test]
    fn the_limit_beats_the_fixed_leverage_and_zero_hands_over_to_it() {
        let b = brackets(&[(125, 10_000.0), (50, 50_000.0)]);
        let mut c = cfg("20000 def");
        c.auto_fix_lev = true;
        c.fix_lev = 10;
        let w = c.want("BTCUSDT", "BTC", Some(&b), None);
        assert_eq!((w.leverage, w.raise), (Some(50), true));
        c.limits = Limits::parse("0 def");
        let w = c.want("BTCUSDT", "BTC", Some(&b), None);
        assert_eq!((w.leverage, w.raise), (Some(10), true));
        // The fixed leverage is clamped to what the brackets allow.
        c.fix_lev = 500;
        assert_eq!(c.want("BTCUSDT", "BTC", Some(&b), None).leverage, Some(125));
    }

    #[test]
    fn the_fallback_runs_down_the_brackets_but_not_below_the_current_leverage() {
        let b = brackets(&[
            (75, 25_000.0),
            (50, 80_000.0),
            (25, 800_000.0),
            (15, 2e6),
            (15, 3e6),
        ]);
        assert_eq!(b.leverages_between(Some(15), 75), vec![50, 25]);
        assert_eq!(b.leverages_between(None, 50), vec![25, 15]);
        assert!(b.leverages_between(Some(50), 75).is_empty());
    }

    /// `-5018` at 5x on a 5/4/3 table: no pass sets 5x again, a target above the next bracket
    /// down is held to it, and a market without a target of its own is lowered to it.
    #[test]
    fn a_market_refused_for_its_notional_limit_is_kept_below_the_refused_leverage() {
        let b = brackets(&[(5, 5_000.0), (4, 10_000.0), (3, 50_000.0)]);
        let want = |leverage| Want {
            leverage,
            raise: true,
            ..Want::default()
        };
        assert_eq!(capped_want(want(Some(5)), Some(&b), 5).leverage, Some(4));
        assert_eq!(capped_want(want(Some(3)), Some(&b), 5).leverage, Some(3));
        assert_eq!(capped_want(want(None), Some(&b), 5).leverage, Some(4));
        assert!(
            capped_want(want(None), Some(&b), 5).raise,
            "the rest of the wish stands"
        );
        // The table's lowest refused: nothing lower to set, nothing at it either.
        assert_eq!(capped_want(want(Some(3)), Some(&b), 3).leverage, None);
        assert_eq!(capped_want(want(None), None, 5).leverage, None);
    }

    /// A `-5018` that comes in with or behind new settings is queued, not swallowed by the
    /// reads that look for the settings.
    #[test]
    fn a_notional_refusal_among_new_settings_is_kept() {
        let (tx, rx) = mpsc::channel();
        let capped_msg = |s: &str| Msg::Capped {
            symbol: s.into(),
            leverage: Some(5),
        };
        tx.send(capped_msg("WLFIUSDT")).unwrap();
        tx.send(Msg::Config(cfg("5000 def"))).unwrap();
        tx.send(capped_msg("NEARUSDT")).unwrap();
        let mut capped = Vec::new();
        assert!(arrived(&rx, &mut capped).is_some());
        let names: Vec<&str> = capped.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(names, ["WLFIUSDT", "NEARUSDT"]);
        tx.send(capped_msg("ORCAUSDT")).unwrap();
        newest(&rx, cfg("0 def"), &mut capped);
        assert_eq!(capped.len(), 3);
    }

    #[test]
    fn without_allow_up_the_limit_only_lowers() {
        let b = brackets(&[(125, 10_000.0), (50, 50_000.0)]);
        let mut c = cfg("200 def");
        c.auto_lev_up = false;
        let w = c.want("BTCUSDT", "BTC", Some(&b), None);
        assert_eq!((w.leverage, w.raise), (Some(125), false));
        assert!(!cfg("0 def").is_active());
        assert!(cfg("1k def").is_active());
        let mut off = cfg("1k def");
        off.auto_max_order = false;
        assert!(!off.is_active(), "the box is the switch");
        assert_eq!(off.want("BTCUSDT", "BTC", Some(&b), None).leverage, None);
    }

    #[test]
    fn margin_is_isolated_when_both_boxes_are_ticked() {
        let mut c = Config::default();
        assert_eq!(c.margin(), None);
        c.auto_cross = true;
        assert_eq!(c.margin(), Some(setup::CROSSED));
        c.auto_isolated = true;
        assert_eq!(c.margin(), Some(setup::ISOLATED));
        assert!(c.is_active());
    }

    #[test]
    fn a_market_without_brackets_keeps_its_leverage_and_its_margin_wish() {
        let mut c = cfg("200 def");
        c.auto_isolated = true;
        let w = c.want("NEWUSDT", "NEW", None, None);
        assert_eq!((w.leverage, w.margin), (None, Some(setup::ISOLATED)));
        // Brackets that state no cap name no leverage for a limit: the fixed one stands in.
        let uncapped = brackets(&[(125, 0.0), (50, 0.0)]);
        c.auto_fix_lev = true;
        c.fix_lev = 20;
        assert_eq!(
            c.want("BTCUSDT", "BTC", Some(&uncapped), None).leverage,
            Some(20)
        );
    }
}
