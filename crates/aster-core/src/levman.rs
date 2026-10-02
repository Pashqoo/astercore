//! The terminal's «Настройка плеча» (MoonBot's leverage management), served by the core: the
//! terminal sends its whole `TLevManageCommand` on Apply (the page's Leverage tab sends the same
//! settings), the core keeps it, applies it at once
//! and again every hour (MoonBot checks hourly too: the brackets change on the exchange's side).
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
use std::time::Duration;

use moonproto::server::codec::ui;
use moonproto::LevManage;

use crate::aster::json::{PositionRisk, SymbolBrackets};
use crate::aster::rest::Rest;
use crate::aster::sign::Signer;
use crate::model::Catalog;
use crate::setup::{self, Want};

/// How often the account is looked over without being asked (MoonBot: once an hour).
const PERIOD: Duration = Duration::from_secs(3600);

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
    pub fn want(&self, symbol: &str, base: &str, brackets: Option<&SymbolBrackets>) -> Want {
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
            .and_then(|b| b.leverage_for_limit(limit as f64));
        if by_limit.is_some() {
            want.leverage = by_limit;
            want.raise = self.auto_lev_up;
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
    /// When the last pass began (Unix ms; 0 = none yet) and why: start, applied, hourly.
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
}

/// Markets handled between two reads of the account: a position or an order that appears during
/// a pass is seen at the next chunk (~20 s of calls), not at the next pass. The window is not
/// closed: a leverage change on a live position is not known to be refused on Aster (only
/// Binance's margin refusals are assumed, `setup.rs`).
const CHUNK: usize = 60;

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
    };
    thread::Builder::new()
        .name("aster-levman".into())
        .spawn(move || {
            let mut why = "start";
            loop {
                if worker.config.is_some() {
                    // A pass that panics is lost, not the thread: the next hour tries again.
                    let run = panic::catch_unwind(AssertUnwindSafe(|| worker.pass(why, &rx)));
                    match run {
                        // New settings came in during the pass: it was dropped, they go first.
                        Ok(Some(newer)) => {
                            worker.config = Some(newer);
                            why = "applied";
                            continue;
                        }
                        Ok(None) => {}
                        Err(_) => {
                            log::error!("leverage: a pass panicked, the next is in an hour");
                            let mut last =
                                worker.status.lock().unwrap_or_else(PoisonError::into_inner);
                            last.running = false;
                            last.stopped = Some("the pass panicked".into());
                        }
                    }
                }
                match rx.recv_timeout(PERIOD) {
                    Ok(Msg::Config(c)) => {
                        worker.config = Some(newest(&rx, c));
                        why = "applied";
                    }
                    Err(RecvTimeoutError::Timeout) => why = "hourly",
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
        })
        .expect("spawn");
    tx
}

/// Only the last of several queued settings counts.
fn newest(rx: &Receiver<Msg>, mut config: Config) -> Config {
    while let Ok(Msg::Config(newer)) = rx.try_recv() {
        config = newer;
    }
    config
}

struct Worker {
    rest: Rest,
    signer: Signer,
    config: Option<Config>,
    status: Shared,
}

impl Worker {
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
            return None;
        };
        let mut last = Last {
            running: true,
            at_ms: crate::engine::now_ms(),
            why: why.into(),
            ..Last::default()
        };
        self.publish(last.clone());
        let (markets, brackets) = match self.markets_and_brackets() {
            Ok(read) => read,
            Err(e) => {
                log::warn!("leverage: {why}: not made: {e}");
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
        for chunk in markets.chunks(CHUNK) {
            if let Ok(Msg::Config(c)) = rx.try_recv() {
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
                return Some(newest(rx, c));
            }
            let plans = match self.plans(&config, chunk, &brackets) {
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

    fn publish(&self, last: Last) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = last;
    }

    /// The trading markets (symbol, base coin) in symbol order, listed now, and their brackets.
    #[allow(clippy::type_complexity)]
    fn markets_and_brackets(
        &mut self,
    ) -> Result<(Vec<(String, String)>, HashMap<String, SymbolBrackets>), String> {
        let err = |what: &'static str| move |e| format!("{what}: {e}");
        self.rest.sync_clock().map_err(err("clock"))?;
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
        Ok((
            markets,
            rows.into_iter().map(|b| (b.symbol.clone(), b)).collect(),
        ))
    }

    /// The plan of one chunk of markets, from the account as it is read now.
    fn plans(
        &mut self,
        config: &Config,
        chunk: &[(String, String)],
        brackets: &HashMap<String, SymbolBrackets>,
    ) -> Result<Vec<setup::Plan>, String> {
        let err = |what: &'static str| move |e| format!("{what}: {e}");
        // The clock is measured again for every chunk: a pass is minutes of writes.
        self.rest.sync_clock().map_err(err("clock"))?;
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
                let want = config.want(symbol, base, brackets.get(symbol));
                setup::plan_for(
                    symbol,
                    by_symbol.get(symbol.as_str()).copied(),
                    &want,
                    ordered.contains(symbol.as_str()),
                )
            })
            .collect())
    }
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
        let w = c.want("BTCUSDT", "BTC", Some(&b));
        assert_eq!((w.leverage, w.raise), (Some(50), true));
        c.limits = Limits::parse("0 def");
        let w = c.want("BTCUSDT", "BTC", Some(&b));
        assert_eq!((w.leverage, w.raise), (Some(10), true));
        // The fixed leverage is clamped to what the brackets allow.
        c.fix_lev = 500;
        assert_eq!(c.want("BTCUSDT", "BTC", Some(&b)).leverage, Some(125));
    }

    #[test]
    fn without_allow_up_the_limit_only_lowers() {
        let b = brackets(&[(125, 10_000.0), (50, 50_000.0)]);
        let mut c = cfg("200 def");
        c.auto_lev_up = false;
        let w = c.want("BTCUSDT", "BTC", Some(&b));
        assert_eq!((w.leverage, w.raise), (Some(125), false));
        assert!(!cfg("0 def").is_active());
        assert!(cfg("1k def").is_active());
        let mut off = cfg("1k def");
        off.auto_max_order = false;
        assert!(!off.is_active(), "the box is the switch");
        assert_eq!(off.want("BTCUSDT", "BTC", Some(&b)).leverage, None);
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
        let w = c.want("NEWUSDT", "NEW", None);
        assert_eq!((w.leverage, w.margin), (None, Some(setup::ISOLATED)));
        // Brackets that state no cap name no leverage for a limit: the fixed one stands in.
        let uncapped = brackets(&[(125, 0.0), (50, 0.0)]);
        c.auto_fix_lev = true;
        c.fix_lev = 20;
        assert_eq!(c.want("BTCUSDT", "BTC", Some(&uncapped)).leverage, Some(20));
    }
}
