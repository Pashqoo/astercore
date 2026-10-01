//! The operator's side of the engine (M4): the control queue the web page and
//! the Telegram chat talk through, the status they read, the settings they
//! edit, and what the core reports to the chat — entries, closed deals with
//! their picture, the daily summary. Ported from TInvestCore's `engine.rs`,
//! where it sat among the trading code; here it is a child module so it can
//! reach the handler's state without the state being made public.

use std::collections::HashMap;

use moonproto::server::codec::market_data::{Candle, HistoryTrade};
use moonproto::{StrategyFieldType, StrategyFieldUiKind, StrategySnapshot};

use super::*;
use crate::chart;
use crate::clock::trader_midnight;
use crate::control::{self, Control, ControlCmd, Halted};
use crate::moonshot::{Ctx, Judged};
use crate::orders::Move;
use crate::screener::{self, Seat};
use crate::settings::{self, Settings};
use crate::stderr_log;
use crate::strategies;
use crate::strategy_file;
use crate::telegram;

const DAY_MS: i64 = 86_400_000;
/// A closed deal waits at least this long before it is reported: its fee
/// comes on the user stream's execution event, and the message is supposed to
/// carry the net profit, not the gross one.
pub(super) const DEAL_SETTLE_MS: i64 = 5_000;
/// While fees are still owed the deal is looked at again this often, and not
/// past the deadline: a deal reported gross beats one never reported.
const DEAL_FEE_RECHECK_MS: i64 = 5_000;
pub(super) const DEAL_FEE_WAIT_MS: i64 = 60_000;
/// Reported deals remembered, so a row that changes after it closed is not
/// reported twice; past this the oldest half goes.
const DEALS_NOTED_CAP: usize = 10_000;
/// Minutes in one bar of a deal's picture; the window around the deal is
/// `SHOT_LEAD_MS`..`SHOT_TRAIL_MS` (TInvestCore's, as is).
const SHOT_BAR_MINUTES: i64 = 1;
const SHOT_LEAD_MS: i64 = 60_000;
const SHOT_TRAIL_MS: i64 = 30_000;
/// A picture whose data never came is forgotten.
const SHOT_WAIT_MS: i64 = 90_000;
/// Bars one picture may hold, however long the position it covers lived.
const SHOT_BARS_MAX: usize = 240;
/// How long the last chat message may take on the way out.
const TELEGRAM_FLUSH: std::time::Duration = std::time::Duration::from_secs(3);

/// A filled entry waiting to be announced; `ordered` comes off the order
/// record, because the report row keeps only what filled.
pub(super) struct EntryNote {
    pub(super) rec_id: i64,
    pub(super) ordered: f64,
}

/// A closed deal on its way to the chat.
pub(super) struct DealNote {
    pub(super) rec_id: i64,
    /// Exchange orders whose fee the user stream may still report (0 = none
    /// to wait for: the emulator, or a core that does not trade).
    pub(super) fees: usize,
    pub(super) due: i64,
    pub(super) deadline: i64,
    /// The levels and lines the deal was planned around, taken while the
    /// order was still there: the report row keeps none of them.
    pub(super) stop: Option<f64>,
    pub(super) take: Option<f64>,
    pub(super) entry_moves: Vec<Move>,
    pub(super) exit_moves: Vec<Move>,
}

/// A deal's picture, waiting for the reply it is drawn from: the tape first,
/// and the minute bars when the tape could not reach the deal.
#[derive(Clone)]
pub(super) struct DealShot {
    rec_id: i64,
    until: i64,
    stop: Option<f64>,
    take: Option<f64>,
    entry_moves: Vec<Move>,
    exit_moves: Vec<Move>,
}

/// The operator's state of the handler, apart from the trading state.
#[derive(Default)]
pub(super) struct Ops {
    pub(super) settings: Settings,
    pub(super) settings_path: std::path::PathBuf,
    pub(super) telegram: Option<telegram::Reporter>,
    /// The core's own candle requests (`/chart`), by request uid: the
    /// deadline and the caller waiting.
    chart_waits: HashMap<u64, (i64, control::ChartReply)>,
    deal_shots: HashMap<u64, DealShot>,
    pub(super) deal_notes: Vec<DealNote>,
    deals_noted: HashSet<i64>,
    entries_noted: HashSet<i64>,
    /// The trader's day the summary last went out for; 0 until the first pass.
    daily_day: i64,
    /// The terminal's own picture thresholds, while it has sent them.
    terminal_shots: Option<settings::Shots>,
    /// The account as the page and the chat name it.
    pub(super) account: String,
}

impl Ops {
    /// Defaults, with the settings kept at `settings_path`.
    pub(super) fn new(settings_path: std::path::PathBuf) -> Self {
        Self {
            settings_path,
            account: "none".into(),
            ..Self::default()
        }
    }
}

impl CoreHandler {
    /// The editable settings this run started with (`data/config.json`), and
    /// where an edit from the page or the chat is saved.
    pub fn with_settings(mut self, settings: Settings, path: std::path::PathBuf) -> Self {
        self.ops.settings = settings;
        self.ops.settings_path = path;
        self
    }

    /// Report to Telegram (`telegram.rs`): the handle is a channel, so every
    /// note costs a send and nothing else.
    pub fn with_telegram(mut self, reporter: telegram::Reporter) -> Self {
        self.ops.telegram = Some(reporter);
        self
    }

    /// The account as the page and the chat name it (the signer, shortened).
    pub fn with_account_label(mut self, label: String) -> Self {
        self.ops.account = label;
        self
    }

    /// Answer what the control threads queued (the web page, the chat) and
    /// pick up a signal; `main` calls it after every pump. They change the
    /// same state the terminal's own commands do, through the same methods,
    /// so a stop is a stop whoever asked for it.
    pub fn run_control(&mut self, control: &Control) {
        let now = now_ms();
        if control.signalled() {
            // A signal is the machine's stop — a deploy, a reboot, `systemctl
            // stop` — not the trader's: the running flag is left as it is, and
            // a core that comes back comes back trading.
            log::info!("signal: stopping");
            Self::ask_halt(control, Halted::Stop);
        }
        // The terminal's own shutdown (`TShutdownCommand`), agreed by
        // `on_ui`: a stop, exit 0.
        if self.shutdown_requested {
            Self::ask_halt(control, Halted::Stop);
        }
        let mut queued = control.drain();
        // One catalog sweep per pass, whoever asked: the screening walks
        // every market, and this loop also places and withdraws orders.
        let mut screened = false;
        while control.halted().is_none() {
            let Some(cmd) = queued.next() else {
                break;
            };
            match cmd {
                ControlCmd::Status(reply) => {
                    let status = self.status(now);
                    let _ = reply.send(status);
                }
                ControlCmd::Strategies { start, reply } => {
                    self.set_strategies(start);
                    let _ = reply.send(self.strategies.running());
                }
                ControlCmd::PanicAll(reply) => {
                    let mut fx = self.orders.panic_all(&self.catalog, now);
                    if self.trading.is_none() {
                        fx.logs.push(
                            "trading is off: exits of real positions do not reach the exchange"
                                .into(),
                        );
                    }
                    let moved = fx.changed.len();
                    self.tg(
                        telegram::Kind::Reply,
                        format!("⛔ Panic by hand: {moved} position(s) to a panic exit"),
                    );
                    self.effects(fx, now);
                    let _ = reply.send(moved);
                }
                // The answer goes out before the halt: the caller is told its
                // stop was taken, and the process is gone a moment later.
                ControlCmd::Shutdown(reply) => {
                    let _ = reply.send(());
                    // The page's Stop is the trader stopping: the flag it
                    // leaves behind is what the next start reads.
                    self.set_strategies(false);
                    Self::ask_halt(control, Halted::Stop);
                }
                ControlCmd::Restart(reply) => {
                    let _ = reply.send(());
                    Self::ask_halt(control, Halted::Restart);
                }
                ControlCmd::Chart {
                    market,
                    minutes,
                    reply,
                } => self.control_chart(&market, minutes, reply, now),
                ControlCmd::Screen {
                    strategy_id,
                    fields,
                    reply,
                } => {
                    let answer = if screened {
                        Err(control::SCREEN_BUSY.to_string())
                    } else {
                        screened = true;
                        self.control_screen(strategy_id, &fields, now)
                    };
                    let _ = reply.send(answer);
                }
                ControlCmd::Text { topic, reply } => {
                    let text = match topic {
                        control::Topic::Status => {
                            let status = self.status(now);
                            let rows: Vec<&reports::Row> = self.reports.rows().collect();
                            telegram::status_text(&status, rows.iter().copied(), now)
                        }
                        control::Topic::Profit => {
                            let rows: Vec<&reports::Row> = self.reports.rows().collect();
                            telegram::profit_text(rows.iter().copied(), now)
                        }
                    };
                    let _ = reply.send(text);
                }
                ControlCmd::Talk(on) => {
                    self.ops.settings.telegram.events.deals = on;
                    let _ = self.settings_written(if on {
                        "deal reports are on"
                    } else {
                        "deal reports are off"
                    });
                }
                ControlCmd::ChatApproved(chat_id) => {
                    self.ops.settings.telegram.chat_id = chat_id;
                    let _ = self.settings_written(&format!("chat {chat_id} approved"));
                }
                ControlCmd::SettingsEdit { edit, reply } => {
                    let answer = self.settings_edited(&edit);
                    let _ = reply.send(answer);
                }
            }
        }
        // Behind a halt: a Stop the page queued is still the trader stopping
        // the strategies, and the flag it leaves is what the next start reads.
        // Everything else is answered by the closed channel.
        let mut behind: Vec<ControlCmd> = queued.collect();
        if control.halted().is_some() {
            // The whole queue, not one pass's batch: `main` does not call
            // this again once a halt is in.
            loop {
                let more: Vec<ControlCmd> = control.drain().collect();
                if more.is_empty() {
                    break;
                }
                behind.extend(more);
            }
        }
        for cmd in behind {
            if let ControlCmd::Shutdown(reply) = cmd {
                let _ = reply.send(());
                self.set_strategies(false);
            }
        }
        self.expire_chart_waits(now);
        self.flush_deal_notes(now, false);
        self.run_daily(now);
    }

    /// End the loop: `main` turns the halt into the process exit code. Every
    /// halt withdraws the entries on the way out (`main::withdraw_entries`).
    fn ask_halt(control: &Control, halt: Halted) {
        if control.halted().is_some() {
            return;
        }
        log::info!("{}: asked for, exit code {}", halt.name(), halt.code());
        control.request(halt);
    }

    /// Candles for a control caller (`/chart`): the reply comes back on the
    /// feed's own channel under the reserved client id, and `on_control_reply`
    /// hands it to the waiting caller.
    fn control_chart(&mut self, market: &str, minutes: i64, reply: control::ChartReply, now: i64) {
        // What the operator typed into the chat: matched the way a strategy's
        // lists are, whatever the case.
        let Some(symbol) = self
            .catalog
            .index_of_symbol_ci(market)
            .and_then(|i| self.catalog.at(i))
            .map(|m| m.symbol.clone())
        else {
            let _ = reply.send(Err(format!("unknown market {market}")));
            return;
        };
        let request_uid = rand_uid();
        if !self.feed_send(FeedCommand::Candles {
            symbol,
            minutes,
            client_id: control::CLIENT_ID,
            request_uid,
        }) {
            let _ = reply.send(Err("no market data source".into()));
            return;
        }
        self.ops
            .chart_waits
            .insert(request_uid, (now + control::CHART_WAIT_MS, reply));
    }

    /// The feed's answer to one of the core's own requests: a `/chart`, or the
    /// data a deal's picture is drawn on. `true` when it was one.
    pub(super) fn on_control_candles(
        &mut self,
        request_uid: u64,
        result: Result<Vec<Candle>, String>,
    ) {
        if let Some(shot) = self.ops.deal_shots.remove(&request_uid) {
            match result {
                Ok(candles) => self.send_deal_shot(&shot, &candles),
                Err(why) => log::warn!("deal chart {request_uid}: {why}"),
            }
            return;
        }
        match self.ops.chart_waits.remove(&request_uid) {
            Some((_, reply)) => {
                let _ = reply.send(result);
            }
            None => log::warn!("chart {request_uid}: nobody is waiting any more"),
        }
    }

    /// The trades a deal's picture asked for: drawn from them and the core's
    /// own tape, else the minute bars are asked for.
    pub(super) fn on_control_history(
        &mut self,
        request_uid: u64,
        result: Result<Vec<HistoryTrade>, String>,
    ) {
        let Some(shot) = self.ops.deal_shots.remove(&request_uid) else {
            log::warn!("deal chart {request_uid}: nobody is waiting any more");
            return;
        };
        let drawn = match result {
            Ok(trades) => self.send_deal_ticks(&shot, &trades),
            Err(why) => {
                // The core's own tape may hold the whole of a short deal.
                log::warn!("deal chart {request_uid}: {why}");
                self.send_deal_ticks(&shot, &[])
            }
        };
        if !drawn {
            self.request_deal_bars(&shot, now_ms());
        }
    }

    /// The page's strategy helper: one strategy's screener and Filters tab
    /// run over the whole catalog with the form's settings in place of the
    /// strategy's own — read only: the snapshot is cloned before a field is
    /// moved, and the answer comes from the same `screener` and
    /// `DeltaFilters` the pass uses, in the same `Ctx`.
    fn control_screen(
        &self,
        strategy_id: u64,
        edits: &[(String, String)],
        now: i64,
    ) -> Result<control::Screen, String> {
        let schema = self.strategies.schema();
        let Some(found) = self
            .strategies
            .list()
            .iter()
            .find(|s| s.strategy_id == strategy_id)
        else {
            return Err(format!("no strategy {strategy_id} in the core"));
        };
        let mut snap = found.clone();
        for (name, text) in edits {
            let Some(f) = schema.field(name) else {
                return Err(format!("{name}: the core has no such setting"));
            };
            // A cleared box is the bound switched off (MoonBot's «0 = not
            // checked»); a text field keeps its own empty.
            let blank = text.is_empty() && !matches!(f.type_id, StrategyFieldType::String);
            let Some(v) = strategy_file::value(f.type_id, if blank { "0" } else { text }) else {
                return Err(format!("{name}: {text:?} is not a number"));
            };
            snap.fields.insert(name.as_str(), v);
        }
        let p = Params::from_snapshot(&snap, schema);
        let problem = screener::problem(&p)
            .or_else(|| p.working_time.clone().err())
            .or_else(|| p.deltas.as_ref().and_then(|d| d.problem).map(String::from));
        let unknown = screener::unknown_symbols(&p, &self.catalog);
        let broken = p.deltas.as_ref().is_some_and(|d| d.problem.is_some());
        let cx = Ctx::new(&self.catalog, &self.windows, now, self.market_delta.0);
        let seen = self
            .shots
            .screen(&p, &cx, &self.shots.pool_set(strategy_id));
        let checks: &[moonshot::Check] = p.deltas.as_ref().map_or(&[], |d| d.checks.as_slice());
        let (mut pool, mut entered) = (0, 0);
        let mut markets = Vec::with_capacity(seen.len());
        for s in &seen {
            let Some(m) = self.catalog.at(s.idx) else {
                continue;
            };
            let judged = p
                .deltas
                .as_ref()
                .map_or_else(Vec::new, |d| d.judge_all(s.idx, &cx));
            let refused = (!broken)
                .then(|| {
                    judged
                        .iter()
                        .position(|j| matches!(j, Judged::Refused(_) | Judged::Broken(_)))
                })
                .flatten();
            let in_pool = s.seat == Seat::Pool;
            let may_enter = in_pool && !broken && refused.is_none();
            pool += usize::from(in_pool);
            entered += usize::from(may_enter);
            markets.push(control::ScreenMarket {
                symbol: m.symbol.clone(),
                class: m
                    .tags
                    .iter()
                    .map(|t| t.name())
                    .collect::<Vec<_>>()
                    .join(", "),
                seat: match s.seat {
                    Seat::Pool => "pool",
                    Seat::NoPool => "nopool",
                    Seat::Black => "black",
                    Seat::NotListed => "notlisted",
                    Seat::OtherClass => "otherclass",
                    Seat::DynBlack => "dynblack",
                    Seat::Ranked => "ranked",
                    Seat::Unranked => "unranked",
                },
                rank: s.rank,
                key: finite(s.key),
                values: judged
                    .iter()
                    .map(|j| match j {
                        Judged::Pass(v) | Judged::Refused(v) => finite(Some(*v)),
                        Judged::Waiting | Judged::Broken(_) => None,
                    })
                    .collect(),
                states: judged
                    .iter()
                    .map(|j| match j {
                        Judged::Pass(_) => 'p',
                        Judged::Refused(_) => 'r',
                        Judged::Waiting => 'w',
                        Judged::Broken(_) => 'b',
                    })
                    .collect(),
                refused,
                entered: may_enter,
            });
        }
        Ok(control::Screen {
            strategy_id,
            name: moonshot::label(found),
            kind: self
                .strategies
                .kind_name(strategy_id)
                .unwrap_or_default()
                .to_owned(),
            problem,
            unknown,
            warming: !self.warmup_done,
            fields: Self::screen_fields(&snap, schema),
            sort_by: p
                .dyn_wl
                .on()
                .then(|| p.dyn_wl.by.as_ref().ok().map(|k| k.name().to_owned()))
                .flatten(),
            sort_desc: p.dyn_wl.desc,
            sort_turnover: p.dyn_wl.by.as_ref().is_ok_and(|k| k.turnover()),
            count: p.dyn_wl.count,
            checks: checks
                .iter()
                .map(|c| control::ScreenCheck {
                    what: c.what.to_string(),
                    lo: finite(Some(c.lo)),
                    hi: finite(Some(c.hi)),
                    turnover: c.turnover(),
                })
                .collect(),
            markets,
            pool,
            entered,
        })
    }

    /// The helper's form: the fields `strategies::SCREEN_FIELDS` names, each
    /// with the value this screening used and the control the schema asks for.
    fn screen_fields(
        snap: &StrategySnapshot,
        schema: &moonproto::StrategySchema,
    ) -> Vec<control::ScreenField> {
        let mut out = Vec::new();
        for (card, names) in strategies::SCREEN_FIELDS {
            for name in *names {
                let Some(f) = schema.field(name) else {
                    continue;
                };
                out.push(control::ScreenField {
                    name: (*name).to_owned(),
                    value: moonshot::field(&snap.fields, schema, name)
                        .as_ref()
                        .map(strategy_file::text)
                        .unwrap_or_default(),
                    ui: match f.ui_kind {
                        StrategyFieldUiKind::Checkbox => "check",
                        StrategyFieldUiKind::Combo => "pick",
                        _ => "edit",
                    },
                    choices: f.static_picklist.clone(),
                    card: (*card).to_owned(),
                });
            }
        }
        out
    }

    /// A feed that never answered must not leave the caller waiting or the
    /// map growing; dropping the channel is the answer.
    fn expire_chart_waits(&mut self, now: i64) {
        self.ops.chart_waits.retain(|uid, (until, _)| {
            let live = now < *until;
            if !live {
                log::warn!("chart {uid}: no candles from the feed, giving up");
            }
            live
        });
        self.ops.deal_shots.retain(|uid, shot| {
            let live = now < shot.until;
            if !live {
                log::warn!("deal chart {uid}: no data from the feed, no picture");
            }
            live
        });
    }

    /// Hand a note to the Telegram thread: a channel send, the trading loop
    /// never waits for the network. A no-op without a reporter.
    pub(super) fn tg(&self, kind: telegram::Kind, text: String) {
        if let Some(r) = &self.ops.telegram {
            r.note(kind, text);
        }
    }

    /// The same, for what repeats: the first one goes out, the rest of the
    /// window come back as a count.
    pub(super) fn tg_keyed(&self, kind: telegram::Kind, key: String, text: String) {
        if let Some(r) = &self.ops.telegram {
            r.deduped(kind, key, text);
        }
    }

    /// The core is up. Called by `main` once the socket is bound, so what the
    /// chat hears is a core that can actually be talked to.
    pub fn announce(&mut self) {
        let text = format!(
            "▶️ core started: account {}, {} markets, strategies {}{}",
            self.ops.account,
            self.catalog.len(),
            if self.strategies.running() {
                "running"
            } else {
                "stopped"
            },
            if self.trading.is_some() {
                ""
            } else {
                ", trading off (emulator only)"
            },
        );
        self.tg(telegram::Kind::Lifecycle, text);
    }

    /// The core's last word to the chat, waited for: the process is about to
    /// leave and the sender thread with it.
    pub(super) fn farewell(&mut self, now: i64, live: usize) {
        self.flush_deal_notes(now, true);
        if let Some(r) = &self.ops.telegram {
            r.note(
                telegram::Kind::Lifecycle,
                format!("⏹ core stopped: {live} live order(s) saved"),
            );
            r.flush(TELEGRAM_FLUSH);
        }
    }

    /// Write the settings the chat or the page just changed, and say what
    /// happened — including that the file did not take it.
    fn settings_written(&mut self, done: &str) -> Result<(), String> {
        let saved = self.ops.settings.save(&self.ops.settings_path);
        let path = self.ops.settings_path.display();
        let failure = saved
            .as_ref()
            .err()
            .map(|e| format!("{path} was not written: {e}"));
        match &failure {
            None => log::info!(
                "config: {done} (telegram {}, web {})",
                if self.ops.settings.telegram_ready() {
                    "ready"
                } else {
                    "off"
                },
                self.ops.settings.web.bind
            ),
            Some(why) => log::error!("config: {done}, but {why}"),
        }
        if let Some(r) = &self.ops.telegram {
            r.settings_changed(&self.ops.settings);
            r.note(
                telegram::Kind::Reply,
                match &failure {
                    None => format!("{done}."),
                    Some(why) => {
                        format!("{done}, but {why}: it is lost when the core restarts.")
                    }
                },
            );
        }
        failure.map_or(Ok(()), Err)
    }

    /// One edit from the page: merge it, apply what applies at once, save.
    /// `Err` is only a refused patch, and then nothing changed.
    fn settings_edited(&mut self, edit: &settings::Edit) -> Result<control::Applied, String> {
        let restart = self.ops.settings.apply(edit).map_err(|e| {
            log::error!("config: the page's edit was refused: {e}");
            e
        })?;
        stderr_log::set_level(&self.ops.settings.log_level);
        stderr_log::set_keep_days(self.ops.settings.log_keep_days);
        let unsaved = self
            .settings_written("settings changed from the page")
            .err();
        Ok(control::Applied { restart, unsaved })
    }

    /// An entry is reported once, the moment it filled; a strategy that keeps
    /// its deals to itself (`ReportTradesToTelegram` off) keeps its entries.
    pub(super) fn note_entry(&mut self, note: &EntryNote) {
        if self.ops.telegram.is_none() || !self.ops.entries_noted.insert(note.rec_id) {
            return;
        }
        if self.ops.entries_noted.len() > DEALS_NOTED_CAP {
            let newest = self.ops.entries_noted.iter().copied().max().unwrap_or(0);
            let cut = newest - (DEALS_NOTED_CAP as i64) / 2;
            self.ops.entries_noted.retain(|&id| id >= cut);
        }
        let Some(row) = self.reports.row(note.rec_id).cloned() else {
            return;
        };
        if !self.reports_trades(row.strategy_id) {
            return;
        }
        let label = self.strategy_label(row.strategy_id);
        self.tg(
            telegram::Kind::Deal,
            telegram::entry_text(&row, &label, note.ordered),
        );
    }

    /// A deal is reported once, a moment after it closed: its fee arrives on
    /// its own event and the message carries the net profit.
    pub(super) fn note_deal(&mut self, note: DealNote) {
        if self.ops.telegram.is_none() || !self.ops.deals_noted.insert(note.rec_id) {
            return;
        }
        if self.ops.deals_noted.len() > DEALS_NOTED_CAP {
            let newest = self.ops.deals_noted.iter().copied().max().unwrap_or(0);
            let cut = newest - (DEALS_NOTED_CAP as i64) / 2;
            self.ops.deals_noted.retain(|&id| id >= cut);
        }
        self.ops.deal_notes.push(note);
    }

    /// A hand trade (no strategy) and a strategy that is gone are reported;
    /// a strategy says for itself with `ReportTradesToTelegram`.
    fn reports_trades(&self, strategy_id: u64) -> bool {
        strategy_id == 0
            || self
                .strategies
                .flag(strategy_id, strategies::REPORT_TRADES)
                .unwrap_or(true)
    }

    /// The deals whose wait is over, oldest first. `force` sends what is
    /// waiting whatever fee is still owed: the process is leaving.
    fn flush_deal_notes(&mut self, now: i64, force: bool) {
        if self.ops.deal_notes.is_empty() {
            return;
        }
        let mut waiting = Vec::new();
        for note in std::mem::take(&mut self.ops.deal_notes) {
            if !force && now < note.due {
                waiting.push(note);
                continue;
            }
            let Some(row) = self.reports.row(note.rec_id).cloned() else {
                continue;
            };
            if !force && row.commissions.len() < note.fees && now < note.deadline {
                waiting.push(DealNote {
                    due: now + DEAL_FEE_RECHECK_MS,
                    ..note
                });
                continue;
            }
            if !self.reports_trades(row.strategy_id) {
                continue;
            }
            let label = self.strategy_label(row.strategy_id);
            self.tg(telegram::Kind::Deal, telegram::deal_text(&row, &label));
            // The picture follows on its own message: the text must not wait
            // for a feed that may be slow or have nothing to give.
            if !force && self.wants_shot(&row, now) {
                self.request_deal_shot(&row, note, now);
            }
        }
        self.ops.deal_notes.extend(waiting);
    }

    /// Read the terminal's screenshot thresholds out of the shared-config
    /// blob it sent: its editor is where a MoonBot user sets them, so while it
    /// is attached they win over the file.
    pub(super) fn read_terminal_shots(&mut self) {
        let parsed = gunzip(&self.shared_config)
            .and_then(|plain| moonproto::shared_config::parse_payload(&plain).ok());
        let Some(config) = parsed else {
            if self.ops.terminal_shots.take().is_some() {
                log::warn!(
                    "shots: the terminal's shared config did not parse, config.json decides"
                );
            }
            return;
        };
        let s = &config.trading.send_shots_config;
        let shots = settings::Shots {
            may_send: s.may_send,
            profit_abs: f64::from(s.profit_abs),
            profit_pct: f64::from(s.profit_pers),
            profit_session: f64::from(s.profit_session),
            send_negative: s.send_negative,
        };
        if self.ops.terminal_shots.as_ref() == Some(&shots) {
            return;
        }
        log::info!(
            "shots: from the terminal — {}, abs {}, pct {}, session {}{}",
            if shots.may_send { "on" } else { "off" },
            shots.profit_abs,
            shots.profit_pct,
            shots.profit_session,
            if shots.send_negative {
                ", losses too"
            } else {
                ""
            }
        );
        self.ops.terminal_shots = Some(shots);
    }

    /// The picture thresholds in force: the terminal's while it has sent
    /// them, else the ones in `data/config.json`.
    fn shot_rules(&self) -> &settings::Shots {
        self.ops
            .terminal_shots
            .as_ref()
            .unwrap_or(&self.ops.settings.telegram.shots)
    }

    /// MoonBot's picture rule: any armed threshold passed sends one (0 = off),
    /// a losing deal only with «send negative».
    fn wants_shot(&self, row: &reports::Row, now: i64) -> bool {
        let s = self.shot_rules();
        if !s.may_send || self.feed.is_none() {
            return false;
        }
        if row.profit < 0.0 && !s.send_negative {
            return false;
        }
        let abs = row.profit.abs();
        let pct = if row.spent > 0.0 {
            (row.profit / row.spent * 100.0).abs()
        } else {
            0.0
        };
        // The session is the trader's day, the one the summary counts.
        let session = telegram::tally(self.reports.rows(), trader_midnight(now) / 1000).0;
        (s.profit_abs > 0.0 && abs >= s.profit_abs)
            || (s.profit_pct > 0.0 && pct >= s.profit_pct)
            || (s.profit_session > 0.0 && session >= s.profit_session)
    }

    /// Ask the feed for the trades a deal's picture is drawn on — MoonBot
    /// draws the tape; the minute bars are the fallback.
    fn request_deal_shot(&mut self, row: &reports::Row, note: DealNote, now: i64) {
        if self.catalog.get(&row.coin).is_none() {
            log::warn!("{}: not in the catalog, no picture", row.coin);
            return;
        }
        let request_uid = rand_uid();
        if !self.feed_send(FeedCommand::History {
            symbol: row.coin.clone(),
            client_id: control::CLIENT_ID,
            request_uid,
        }) {
            log::warn!("{}: no market data source, no picture", row.coin);
            return;
        }
        self.ops.deal_shots.insert(
            request_uid,
            DealShot {
                rec_id: row.rec_id,
                until: now + SHOT_WAIT_MS,
                stop: note.stop,
                take: note.take,
                entry_moves: note.entry_moves,
                exit_moves: note.exit_moves,
            },
        );
    }

    /// The minute bars, asked for only when the trades could not draw the
    /// deal (a position held longer than the hour of prints the feed walks).
    fn request_deal_bars(&mut self, shot: &DealShot, now: i64) {
        let Some(row) = self.reports.row(shot.rec_id).cloned() else {
            return;
        };
        if row.deleted || self.catalog.get(&row.coin).is_none() {
            return;
        }
        let request_uid = rand_uid();
        if !self.feed_send(FeedCommand::Candles {
            symbol: row.coin.clone(),
            minutes: SHOT_BAR_MINUTES,
            client_id: control::CLIENT_ID,
            request_uid,
        }) {
            log::warn!("{}: no market data source, no picture", row.coin);
            return;
        }
        self.ops.deal_shots.insert(
            request_uid,
            DealShot {
                until: now + SHOT_WAIT_MS,
                ..shot.clone()
            },
        );
    }

    /// Draw the deal on the trades that came back, the core's own tape
    /// carrying what the exchange has not published yet. `false` when the
    /// window holds none, and the bars get their turn.
    fn send_deal_ticks(&mut self, shot: &DealShot, trades: &[HistoryTrade]) -> bool {
        let Some((row, from, to)) = self.deal_window(shot) else {
            return true;
        };
        let idx = self.catalog.index_of_symbol(&row.coin);
        let window = self.tape.window(idx, trades, from, to);
        if window.prints.is_empty() {
            return false;
        }
        log::info!(
            "deal chart {}: {} print(s) in the window, {} of them the core's own",
            row.coin,
            window.prints.len(),
            window.own
        );
        self.draw_and_send(
            &row,
            shot,
            chart::Series::Ticks(&window.prints),
            SHOT_BAR_MINUTES,
        )
    }

    /// The row and the minutes the picture covers, or `None` when there is no
    /// longer a deal to draw.
    fn deal_window(&self, shot: &DealShot) -> Option<(reports::Row, i64, i64)> {
        let row = self.reports.row(shot.rec_id).cloned()?;
        if row.deleted {
            return None;
        }
        let from = row.buy_date * 1000 - SHOT_LEAD_MS;
        let to = row.close_date * 1000 + SHOT_TRAIL_MS;
        Some((row, from, to))
    }

    /// Draw the deal on the bars that came back and send it.
    fn send_deal_shot(&mut self, shot: &DealShot, candles: &[Candle]) {
        let Some((row, from, to)) = self.deal_window(shot) else {
            return;
        };
        let period = SHOT_BAR_MINUTES * 60_000;
        let bars: Vec<Candle> = candles
            .iter()
            .filter(|c| {
                let t = chart::unix_ms(c.time);
                t + period > from && t <= to
            })
            .cloned()
            .collect();
        if bars.is_empty() {
            log::warn!("{}: no candles around the deal, no picture", row.coin);
            return;
        }
        // Too many bars are aggregated, never cut: cutting keeps the exit
        // side and throws the entry away.
        let (bars, group) = chart::squeeze(&bars, SHOT_BARS_MAX);
        self.draw_and_send(
            &row,
            shot,
            chart::Series::Bars(&bars),
            SHOT_BAR_MINUTES * group as i64,
        );
    }

    /// The picture itself, whichever the deal was drawn from. `false` when
    /// there was nothing a chart could say about it.
    fn draw_and_send(
        &self,
        row: &reports::Row,
        shot: &DealShot,
        series: chart::Series,
        minutes: i64,
    ) -> bool {
        let caption = telegram::shot_caption(row);
        let drawn = telegram::shot_caption_drawn(row);
        let deal = chart::Deal {
            market: &row.coin,
            minutes,
            short: row.is_short,
            entry: row.buy_price,
            entry_ms: row.buy_date * 1000,
            exit: row.sell_price,
            exit_ms: row.close_date * 1000,
            stop: shot.stop,
            take: shot.take,
            entry_moves: &shot.entry_moves,
            exit_moves: &shot.exit_moves,
            caption: &drawn,
            outcome: Some(chart::Outcome {
                profit: row.profit,
                spent: row.spent,
                // The day the deal belongs to, not the day it is drawn on.
                session: telegram::tally(
                    self.reports.rows(),
                    trader_midnight(if row.close_date > 0 {
                        row.close_date * 1000
                    } else {
                        now_ms()
                    }) / 1000,
                )
                .0,
            }),
        };
        match chart::deal_png(series, &deal) {
            Some(png) => {
                if let Some(r) = &self.ops.telegram {
                    r.picture(telegram::Kind::Deal, caption, png);
                }
                true
            }
            None => {
                log::warn!("{}: the deal's data draws no chart", row.coin);
                false
            }
        }
    }

    /// The summary of the day's closed deals, once a trader's day, at the hour
    /// `telegram.daily_at` names (MoonBot's 23:50 by default). The first pass
    /// only takes note, so a core started past the hour reports tomorrow.
    fn run_daily(&mut self, now: i64) {
        let day = trader_midnight(now);
        let due = day + self.ops.settings.daily_at_ms();
        if self.ops.daily_day == 0 {
            self.ops.daily_day = if now >= due { day } else { day - DAY_MS };
            return;
        }
        if self.ops.daily_day >= day || now < due {
            return;
        }
        self.ops.daily_day = day;
        let text = telegram::daily_text(self.reports.rows(), day / 1000);
        self.tg(telegram::Kind::Daily, text);
    }

    /// What to call a strategy in a message; a hand trade has no strategy.
    pub(super) fn strategy_label(&self, strategy_id: u64) -> String {
        if strategy_id == 0 {
            return "manual".into();
        }
        self.strategies
            .list()
            .iter()
            .find(|s| s.strategy_id == strategy_id)
            .map(moonshot::label)
            .unwrap_or_default()
    }

    /// The whole core in one answer for the control callers.
    fn status(&mut self, now: i64) -> control::Status {
        let strategies = self
            .strategies
            .list()
            .iter()
            .map(|s| {
                let (pool, filtered) = self.shots.pool_filters(s.strategy_id);
                control::Strategy {
                    id: s.strategy_id,
                    name: moonshot::label(s),
                    kind: self
                        .strategies
                        .kind_name(s.strategy_id)
                        .unwrap_or_default()
                        .to_owned(),
                    checked: s.checked,
                    pool,
                    filtered: filtered
                        .into_iter()
                        .map(|(what, markets)| control::FilterCount {
                            what: what.to_string(),
                            markets,
                        })
                        .collect(),
                }
            })
            .collect();
        let orders = self
            .orders
            .records(now)
            .iter()
            .map(|r| control::Order {
                id: r.id,
                market: r.market.to_owned(),
                short: r.is_short,
                status: r.status,
                strategy_id: r.strategy_id,
                emulator: r.emulator,
                panic: r.panic,
                quantity: r.buy.quantity,
                filled: r.buy.filled,
                entry_price: if r.buy.mean_price > 0.0 {
                    r.buy.mean_price
                } else {
                    r.buy.price
                },
                exit_price: r.sell.price,
                exit_filled: r.sell.filled,
            })
            .collect();
        let (day_total, day) = telegram::tally(self.reports.rows(), trader_midnight(now) / 1000);
        let (hour_total, hour) = telegram::tally(self.reports.rows(), now / 1000 - 3_600);
        let (report_total, report) = telegram::tally(self.reports.rows(), 0);
        let count = |n: usize| i32::try_from(n).unwrap_or(i32::MAX);
        let streams = self
            .feed
            .as_ref()
            .map(|f| {
                f.health
                    .states()
                    .map(|(name, alive)| control::Stream {
                        name: name.to_owned(),
                        alive,
                    })
                    .collect()
            })
            .unwrap_or_default();
        control::Status {
            uptime_s: (now - self.started_at) / 1000,
            account: self.ops.account.clone(),
            trading: self.trading.is_some(),
            feed: self.feed.is_some(),
            warmup_done: self.warmup_done,
            running: self.strategies.running(),
            market_stopped: self.market_stopped,
            circuit_stopped: self.circuit_stopped.map(|(what, until)| match until {
                Some(at) => format!("{what} (restart in {} s)", (at - now).max(0) / 1000),
                None => what.to_owned(),
            }),
            markets: self.catalog.len(),
            strategies,
            orders,
            profit: control::Profit {
                day_total,
                day_trades: count(day),
                hour_total,
                hour_trades: count(hour),
                report_total,
                report_trades: count(report),
            },
            streams,
            settings: control::SettingsView::from(&self.ops.settings),
            auto_stop: control::AutoStopView::from(&self.auto_stop),
            terminal_shots: self.ops.terminal_shots.clone(),
        }
    }
}

/// A number fit to travel as JSON: an open side of a corridor is an infinity
/// inside the core and `null` on the wire.
fn finite(v: Option<f64>) -> Option<f64> {
    v.filter(|x| x.is_finite())
}

/// The shared-config blob the terminal sends is gzip.
fn gunzip(data: &[u8]) -> Option<Vec<u8>> {
    use std::io::Read;
    // A blob claiming to be bigger than any real config is not read.
    const MAX: u64 = 8 << 20;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .take(MAX)
        .read_to_end(&mut out)
        .ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::fixtures;
    use moonproto::server::codec::trade::StartOrder;

    /// A hand trade in the emulator: its entry is announced the moment it
    /// fills, the deal once it closed and its fee wait is over — and never a
    /// second time (TInvestCore's test, on Aster's catalog and money).
    #[test]
    fn a_closed_deal_is_reported_to_the_chat_once() {
        let mut catalog = fixtures::sber_catalog();
        let m = catalog.at_mut(1).unwrap();
        (m.last_price, m.bid, m.ask) = (Some(100.0), Some(99.9), Some(100.1));
        m.step_size = 1.0;
        m.min_qty = 1.0;
        let (reporter, sent) = telegram::test_reporter();
        let mut h = CoreHandler::new(1, "x".into(), catalog, Strategies::new(None, 0))
            .with_telegram(reporter);
        let now = now_ms();
        let start = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 1000.0,
            price: 99.0,
            planned_sell: 101.0,
            stops: None,
        };
        let market = h.catalog.at(1).unwrap();
        let fx = h.orders.start(0, &start, market, now);
        let id = fx.changed[0];
        h.orders.set_emulator(id);
        h.effects(fx, now);
        h.run_emulator(now);
        h.emulate_fills("u-sber", Some(99.0));
        h.run_emulator(now);
        h.catalog.at_mut(1).unwrap().bid = Some(101.0);
        h.emulate_fills("u-sber", None);
        assert_eq!(
            h.orders.get(id).unwrap().status,
            trade::status::SELL_DONE,
            "the deal is closed"
        );
        assert_eq!(h.ops.deal_notes.len(), 1, "one deal is waiting");
        // The entry announced itself the moment it filled.
        let opened = sent.notes();
        assert_eq!(opened.len(), 1, "{opened:?}");
        assert_eq!(opened[0].kind, telegram::Kind::Deal);
        assert_eq!(
            opened[0].text, "manual: \n[E] Bought 10 #SBER  (100%) 990 USDT",
            "{}",
            opened[0].text
        );
        // Too early for the deal itself: its fee may still be on its way.
        h.flush_deal_notes(now, false);
        assert!(sent.notes().is_empty());
        h.flush_deal_notes(now + DEAL_SETTLE_MS + 1_000, false);
        let notes = sent.notes();
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(
            notes[0].text.starts_with("manual: \n[E] Sell #SBER  + "),
            "{}",
            notes[0].text
        );
        // And never a second time.
        h.flush_deal_notes(now + 2 * DEAL_SETTLE_MS + 1_000, false);
        assert!(sent.notes().is_empty());
        assert!(h.ops.deal_notes.is_empty());
    }
}
