//! Trade reports: one row per core order whose entry filled, in the shape of
//! MoonBot's `Orders` table, kept in `data/reports.jsonl` and replicated to
//! the terminal (`server::codec::report`). Ported from TInvestCore; money is
//! USDT of linear contracts, so a price unit of a unit is worth one USDT.
//!
//! File: a header line `{"epoch":N}`, then one JSON row per change; the last
//! line of a rec id wins. Rows are never physically removed, so the largest
//! rec id is the persistent high-water the replication protocol needs.

use moonproto::server::codec::report::{self, Field, FieldKind, RowsDeleted, Value};
use moonproto::server::codec::trade::LegState;
use moonproto::state::SellReason;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

/// `TBaseCurrency::USDT` (`moonproto::BaseCurrency::USDT`).
const BASE_CURRENCY_USDT: i64 = 1;
/// History served to a fresh cursor with `depth_days = 0` (server default).
const DEFAULT_DEPTH_DAYS: i64 = 30;
/// `depth_days` asking for the whole history.
const DEPTH_ALL: u16 = u16::MAX;
/// Uncompressed row bytes per sync page (the session drops payloads past
/// 256 slices, ~125 KB at the minimum PMTU).
const PAGE_BYTES: usize = 48 * 1024;
/// Rec ids an alive-map request may reach past the high-water: the client
/// asks for the `max_rec_id` it was served, so anything far beyond is not a
/// replica of this database and would only allocate a huge bitmap.
pub const ALIVE_SLACK: i64 = 1 << 20;
const DAY_S: i64 = 86_400;
const HOUR_S: i64 = 3_600;

const fn f(name: &'static str, kind: FieldKind, sql: &'static str) -> Field {
    Field { name, kind, sql }
}
const INT: FieldKind = FieldKind::Integer;
const REAL: FieldKind = FieldKind::Float;
const TEXT: FieldKind = FieldKind::Text;

/// MoonBot 7.52 `Orders` columns with their SQL declarations. `newRecID`
/// stays first: the schema is append-only for a running client, and the core
/// published it alone at index 0 before. `Commission` is ours, at the tail.
pub const FIELDS: &[Field] = &[
    f("newRecID", INT, "INTEGER"),
    f("ID", INT, "INTEGER"),
    f("exOrderID", TEXT, "TEXT"),
    f("Coin", TEXT, "TEXT"),
    f("BuyDate", INT, "INT"),
    f("SellSetDate", INT, "INT"),
    f("CloseDate", INT, "INT"),
    f("Quantity", REAL, "REAL"),
    f("BuyPrice", REAL, "REAL"),
    f("SellPrice", REAL, "Real"),
    f("SpentBTC", REAL, "REAL"),
    f("GainedBTC", REAL, "REAL"),
    f("ProfitBTC", REAL, "REAL"),
    f("Source", INT, "INT"),
    f("Channel", INT, "INT"),
    f("ChannelName", TEXT, "TEXT"),
    f("Status", INT, "INT"),
    f("Comment", TEXT, "TEXT"),
    f("BaseCurrency", INT, "INT default 0"),
    f("BoughtQ", REAL, "REAL"),
    f("BTC1hDelta", REAL, "REAL"),
    f("Exchange1hDelta", REAL, "REAL"),
    f("SignalType", TEXT, "TEXT"),
    f("SellReason", TEXT, "TEXT"),
    f("FName", TEXT, "TEXT"),
    f("deleted", INT, "INT default 0"),
    f("Emulator", INT, "INT default 0"),
    f("Imp", INT, "INT default 0"),
    f("BTC24hDelta", REAL, "REAL default 0"),
    f("Exchange24hDelta", REAL, "REAL default 0"),
    f("bvsvRatio", REAL, "REAL default 0"),
    f("BTC5mDelta", REAL, "REAL default 0"),
    f("IsShort", INT, "INT default 0"),
    f("Pump1H", REAL, "REAL default 0"),
    f("Dump1H", REAL, "REAL default 0"),
    f("d24h", REAL, "REAL default 0"),
    f("d3h", REAL, "REAL default 0"),
    f("d1h", REAL, "REAL default 0"),
    f("d15m", REAL, "REAL default 0"),
    f("d5m", REAL, "REAL default 0"),
    f("d1m", REAL, "REAL default 0"),
    f("dBTC1m", REAL, "REAL default 0"),
    f("PriceBug", REAL, "REAL default 0"),
    f("Vd1m", REAL, "REAL default 0"),
    f("Lev", INT, "INT default 1"),
    f("hVol", REAL, "REAL default 0"),
    f("hVolF", REAL, "REAL default 0"),
    f("dVol", REAL, "REAL default 0"),
    f("TaskID", INT, "INT default 0"),
    f("StrategyID", INT, "sqlite3_uint64 default 0"),
    f("TakeProfitLag", INT, "INT default 0"),
    f("da1m", REAL, "REAL default 0"),
    f("d5s", REAL, "REAL default 0"),
    f("dmark", REAL, "REAL default 0"),
    f("Commission", REAL, "REAL default 0"),
];

/// Who closed the position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitSource {
    /// The core's own exit order.
    Core,
    /// A foreign exchange order (the exchange's own app) taken as the exit.
    Foreign,
    /// The position left the account without an order the core saw.
    Outside,
}

/// A core order as the report sees it (built by the engine).
pub struct Deal<'a> {
    pub order_id: u64,
    pub coin: &'a str,
    pub is_short: bool,
    pub strategy_id: u64,
    /// MoonBot kind name of the order's strategy (`SignalType`); `None`
    /// when manual or the strategy is gone.
    pub signal_type: Option<&'a str>,
    pub buy: &'a LegState,
    pub sell: &'a LegState,
    /// The order reached a terminal status.
    pub closed: bool,
    pub panic: bool,
    /// MoonBot `SellReason` code the order carries (0 = none).
    pub sell_reason: u8,
    pub exit: ExitSource,
    /// Exchange id of the exit, else of the entry.
    pub ex_order_id: &'a str,
    /// An emulator order's deal.
    pub emulator: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Row {
    pub rec_id: i64,
    pub task_id: u64,
    pub ex_order_id: String,
    pub coin: String,
    /// Unix seconds (UTC); `close_date = 0` while open.
    pub buy_date: i64,
    pub sell_set_date: i64,
    pub close_date: i64,
    /// Units bought.
    pub quantity: f64,
    /// Mean prices in quote units.
    pub buy_price: f64,
    pub sell_price: f64,
    /// USDT: entry and exit value, realized profit net of commission.
    pub spent: f64,
    pub gained: f64,
    pub profit: f64,
    /// Exchange commission, USDT: the total and its part per exchange order
    /// (a late or repeated reply for one order is not counted twice).
    pub commission: f64,
    pub commissions: BTreeMap<String, f64>,
    pub closed: bool,
    pub comment: String,
    pub signal_type: String,
    pub sell_reason: String,
    pub deleted: bool,
    pub is_short: bool,
    pub strategy_id: u64,
    /// The emulator's deal: shown in the report, kept out of the profit.
    pub emulator: bool,
}

impl Row {
    /// Refresh from the order; dates already set and the user's `deleted`
    /// flag are kept.
    fn apply(&mut self, d: &Deal, now_s: i64) {
        let secs = |ms: i64| if ms > 0 { ms / 1000 } else { now_s };
        self.task_id = d.order_id;
        self.ex_order_id = d.ex_order_id.to_owned();
        self.coin = d.coin.to_owned();
        self.is_short = d.is_short;
        self.emulator = d.emulator;
        self.strategy_id = d.strategy_id;
        // A deleted strategy's rows keep the kind they were written with.
        match (d.strategy_id, d.signal_type) {
            (0, _) => self.signal_type = "Manual".into(),
            (_, Some(kind)) => self.signal_type = kind.into(),
            (_, None) if self.signal_type.is_empty() => self.signal_type = "MoonShot".into(),
            _ => {}
        }
        if self.buy_date == 0 {
            self.buy_date = secs(d.buy.close_ms);
        }
        if self.sell_set_date == 0 && d.sell.create_ms > 0 {
            self.sell_set_date = d.sell.create_ms / 1000;
        }
        self.quantity = d.buy.filled;
        self.buy_price = d.buy.mean_price;
        self.sell_price = d.sell.mean_price;
        self.spent = d.buy.spent;
        self.gained = d.sell.spent;
        self.closed = d.closed;
        if d.closed {
            if self.close_date == 0 {
                self.close_date = secs(d.sell.close_ms);
            }
            let gross = if d.is_short {
                self.spent - self.gained
            } else {
                self.gained - self.spent
            };
            // An adopted foreign exit carries a synthetic entry with no
            // price: its cost is unknown, so is the profit (MAGE 22.09: the
            // whole 898.5 RUB of the exit was booked as profit, TInvestCore).
            self.profit = if self.spent > 0.0 {
                gross - self.commission
            } else {
                0.0
            };
            let fallback = match (d.exit, d.panic) {
                (ExitSource::Foreign | ExitSource::Outside, _) => SellReason::ManualSell,
                (ExitSource::Core, true) => SellReason::PanicSell,
                (ExitSource::Core, false) => SellReason::SellPrice,
            };
            self.sell_reason = reason_of(d.sell_reason)
                .unwrap_or(fallback)
                .description()
                .into();
            self.comment = match d.exit {
                ExitSource::Foreign => "foreign exit",
                ExitSource::Outside => "closed outside the core",
                ExitSource::Core => "",
            }
            .into();
        } else {
            self.close_date = 0;
            self.profit = 0.0;
        }
    }

    fn value(&self, name: &str) -> Option<Value> {
        use Value::{Float, Int, Text};
        Some(match name {
            "newRecID" | "ID" => Int(self.rec_id),
            "exOrderID" => Text(self.ex_order_id.clone()),
            "Coin" => Text(self.coin.clone()),
            "BuyDate" => Int(self.buy_date),
            "SellSetDate" => Int(self.sell_set_date),
            "CloseDate" => Int(self.close_date),
            "Quantity" | "BoughtQ" => Float(self.quantity),
            "BuyPrice" => Float(self.buy_price),
            "SellPrice" => Float(self.sell_price),
            "SpentBTC" => Float(self.spent),
            "GainedBTC" => Float(self.gained),
            "ProfitBTC" => Float(self.profit),
            "Status" => Int(i64::from(self.closed)),
            "Comment" => Text(self.comment.clone()),
            "BaseCurrency" => Int(BASE_CURRENCY_USDT),
            "SignalType" => Text(self.signal_type.clone()),
            "SellReason" => Text(self.sell_reason.clone()),
            "deleted" => Int(i64::from(self.deleted)),
            "Emulator" => Int(i64::from(self.emulator)),
            "IsShort" => Int(i64::from(self.is_short)),
            "Lev" => Int(1),
            "TaskID" => Int(self.task_id as i64),
            "StrategyID" => Int(self.strategy_id as i64),
            "Commission" => Float(self.commission),
            // `FName` is MoonBot's saved chart file; the signal analytics it
            // records at entry: the column default.
            _ => return None,
        })
    }

    /// The row on the wire: every column the core fills.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let values: Vec<(u16, Value)> = FIELDS
            .iter()
            .enumerate()
            .filter_map(|(i, f)| Some((i as u16, self.value(f.name)?)))
            .collect();
        report::encode_row(out, &values);
    }
}

#[derive(Serialize, Deserialize)]
struct Header {
    epoch: i32,
}

/// Report profit counters (`TProfitStateCommand`).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Profit {
    pub total: f64,
    pub trades: i32,
    pub hour_total: f64,
    pub hour_trades: i32,
}

/// One sync page: encoded rows and the ids the page header carries.
pub struct Page {
    pub rows: Vec<u8>,
    pub count: u16,
    pub last_rec_id: i64,
}

pub struct Reports {
    path: Option<PathBuf>,
    epoch: i32,
    rows: BTreeMap<i64, Row>,
    by_order: HashMap<u64, i64>,
    /// Row lines in the file (to decide on compaction).
    lines: usize,
    /// The file exists and could not be read: what is in memory is not what is on disk, and
    /// nothing is written over it (the history it holds is the trader's).
    unreadable: bool,
    /// The file was read but not whole (a line that was not UTF-8 or not a row): a copy is
    /// kept before the first rewrite drops it.
    damaged: bool,
    /// After a failed rewrite: no new attempt until the line count passes this.
    retry_above: usize,
}

impl Reports {
    /// Load `path` (created with a fresh epoch when missing); `None` keeps
    /// everything in memory.
    pub fn open(path: Option<PathBuf>, now_ms: i64) -> Self {
        let mut r = Self {
            path,
            epoch: 0,
            rows: BTreeMap::new(),
            by_order: HashMap::new(),
            lines: 0,
            unreadable: false,
            damaged: false,
            retry_above: 0,
        };
        if let Some(path) = r.path.clone() {
            match File::open(&path) {
                Ok(file) => r.load(BufReader::new(file)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    log::error!(
                        "reports: {}: {e}; the history is left as it is and nothing is written to it",
                        path.display()
                    );
                    r.unreadable = true;
                }
            }
        }
        if r.epoch == 0 {
            r.epoch = new_epoch(now_ms);
            r.rewrite();
        } else if r.lines > 2 * r.rows.len() + 64 {
            r.rewrite();
        }
        r
    }

    fn load(&mut self, reader: impl BufRead) {
        // A line that is not UTF-8 is skipped (the lines after it are good); any other read
        // error ends the load and leaves the file alone (`unreadable`).
        let mut texts = Vec::new();
        for line in reader.lines() {
            match line {
                Ok(text) => texts.push(text),
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    log::warn!("reports: line {} is not text, skipped", texts.len() + 1);
                    self.damaged = true;
                    texts.push(String::new());
                }
                Err(e) => {
                    log::error!("reports: read stopped at line {}: {e}", texts.len() + 1);
                    self.unreadable = true;
                    break;
                }
            }
        }
        let mut lines = texts.into_iter().enumerate().peekable();
        let header = lines
            .peek()
            .and_then(|(_, h)| serde_json::from_str::<Header>(h).ok());
        match header {
            Some(h) if h.epoch != 0 => {
                self.epoch = h.epoch;
                lines.next();
            }
            // The rows (the first line too) are kept; a new epoch makes
            // replicas start over.
            _ => {
                log::warn!("reports: bad header, rows kept under a new epoch");
                self.damaged = true;
            }
        }
        for (n, line) in lines {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Row>(&line) {
                Ok(row) if row.rec_id > 0 => {
                    self.lines += 1;
                    self.by_order.insert(row.task_id, row.rec_id);
                    self.rows.insert(row.rec_id, row);
                }
                _ => {
                    log::warn!("reports: line {} skipped", n + 1);
                    self.damaged = true;
                }
            }
        }
    }

    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Persistent high-water: rows are never physically removed.
    pub fn max_rec_id(&self) -> i64 {
        self.rows.keys().next_back().copied().unwrap_or(0)
    }

    /// The order has a report row (its entry filled).
    pub fn has_order(&self, order_id: u64) -> bool {
        self.by_order.contains_key(&order_id)
    }

    pub fn row(&self, rec_id: i64) -> Option<&Row> {
        self.rows.get(&rec_id)
    }

    /// Create or refresh the row of `d`'s order. `None` when the entry has
    /// not filled yet or nothing changed.
    pub fn record(&mut self, d: &Deal, now_ms: i64) -> Option<&Row> {
        let rec_id = match self.by_order.get(&d.order_id) {
            Some(&id) => id,
            None if d.buy.filled > 0.0 => self.max_rec_id() + 1,
            None => return None,
        };
        let mut row = self.rows.get(&rec_id).cloned().unwrap_or(Row {
            rec_id,
            ..Row::default()
        });
        row.apply(d, now_ms / 1000);
        if self.rows.get(&rec_id) == Some(&row) {
            return None;
        }
        self.by_order.insert(d.order_id, rec_id);
        self.store(row);
        self.rows.get(&rec_id)
    }

    /// The exchange commission of one exchange order of `order_id`'s deal; the
    /// row when it changed. A closed row's profit is net of it.
    pub fn add_commission(&mut self, order_id: u64, exchange_id: &str, usdt: f64) -> Option<&Row> {
        let rec_id = *self.by_order.get(&order_id)?;
        let mut row = self.rows.get(&rec_id)?.clone();
        row.commissions.insert(exchange_id.to_owned(), usdt);
        let total: f64 = row.commissions.values().sum();
        if row.closed && row.spent > 0.0 {
            row.profit += row.commission - total;
        }
        row.commission = total;
        if self.rows.get(&rec_id) == Some(&row) {
            return None;
        }
        self.store(row);
        self.rows.get(&rec_id)
    }

    /// Soft-delete or restore the selected rows; the rec ids that changed.
    pub fn set_deleted(&mut self, sel: &RowsDeleted) -> Vec<i64> {
        let ids: Vec<i64> = self
            .rows
            .values()
            .filter(|r| r.deleted != sel.deleted && sel.contains(r.rec_id))
            .map(|r| r.rec_id)
            .collect();
        for id in &ids {
            let mut row = self.rows[id].clone();
            row.deleted = sel.deleted;
            self.store(row);
        }
        ids
    }

    /// Rows from `from_rec_id`. A fresh cursor (0) starts at rows bought within
    /// `depth_days` (0 = default, `u16::MAX` = all); continuations take every
    /// later row, as MoonBot's `newRecID >= from` query does.
    pub fn page(&self, from_rec_id: i64, depth_days: u16, now_ms: i64) -> Page {
        let since = match (from_rec_id, depth_days) {
            (f, _) if f > 0 => i64::MIN,
            (_, DEPTH_ALL) => i64::MIN,
            (_, 0) => now_ms / 1000 - DEFAULT_DEPTH_DAYS * DAY_S,
            (_, d) => now_ms / 1000 - i64::from(d) * DAY_S,
        };
        let mut page = Page {
            rows: Vec::new(),
            count: 0,
            last_rec_id: 0,
        };
        for row in self
            .rows
            .range(from_rec_id.max(1)..)
            .map(|(_, r)| r)
            .filter(|r| r.buy_date >= since)
        {
            if page.rows.len() >= PAGE_BYTES || page.count == u16::MAX {
                break;
            }
            row.encode(&mut page.rows);
            page.count += 1;
            page.last_rec_id = row.rec_id;
        }
        page
    }

    /// `ceil(up_to / 8)` bytes, bit `rec_id - 1` set for a row that exists
    /// and is not deleted. The caller bounds `up_to` (see `ALIVE_SLACK`).
    pub fn alive_bitmap(&self, up_to_rec_id: i64) -> Vec<u8> {
        if up_to_rec_id < 1 {
            return Vec::new();
        }
        let len = usize::try_from((up_to_rec_id + 7) / 8).unwrap_or(0);
        let mut bits = vec![0u8; len];
        for row in self.rows.range(1..=up_to_rec_id).map(|(_, r)| r) {
            if !row.deleted {
                let i = (row.rec_id - 1) as usize;
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        bits
    }

    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.values()
    }

    /// Closed, not deleted deals for the auto-stop (emulator ones on request).
    pub fn closed_deals(&self, with_emulator: bool) -> Vec<crate::autostop::Closed> {
        self.rows
            .values()
            .filter(|r| r.closed && !r.deleted && (with_emulator || !r.emulator))
            .map(|r| crate::autostop::Closed {
                close_s: r.close_date,
                profit: r.profit,
            })
            .collect()
    }

    /// MoonBot's counters: profit and count of the real rows not deleted,
    /// and of those bought within the last hour. The emulator's deals move
    /// no money: they stay in the report only.
    pub fn profit(&self, now_ms: i64) -> Profit {
        let hour_ago = now_ms / 1000 - HOUR_S;
        let mut p = Profit::default();
        for r in self.rows.values().filter(|r| !r.deleted && !r.emulator) {
            p.total += r.profit;
            p.trades += 1;
            if r.buy_date > hour_ago {
                p.hour_total += r.profit;
                p.hour_trades += 1;
            }
        }
        p
    }

    fn store(&mut self, row: Row) {
        if let Some(path) = self.path.clone().filter(|_| !self.unreadable) {
            // One `write`: a line written in two (`writeln!` on a `File`) can be cut between
            // them, and the next line then glues onto the first.
            let mut line = serde_json::to_string(&row).expect("row serializes");
            line.push('\n');
            let appended = OpenOptions::new()
                .append(true)
                .open(&path)
                .and_then(|mut f| f.write_all(line.as_bytes()));
            match appended {
                Ok(()) => self.lines += 1,
                Err(e) => log::warn!("reports: {}: {e}", path.display()),
            }
        }
        self.rows.insert(row.rec_id, row);
        // The file keeps every update of a row; past twice the rows it is written afresh, or it
        // grows without bound while the core runs.
        if self.lines > 2 * self.rows.len() + 64 && self.lines > self.retry_above {
            self.rewrite();
        }
    }

    /// Write the header and the current rows to a temp file, then rename.
    fn rewrite(&mut self) {
        let Some(path) = &self.path else {
            return;
        };
        if self.unreadable {
            return;
        }
        if self.damaged && path.exists() {
            let mut bak = path.as_os_str().to_owned();
            bak.push(".bak");
            match fs::copy(path, PathBuf::from(&bak)) {
                Ok(_) => self.damaged = false,
                // No copy, no rewrite: the rewrite would drop the lines that could not be
                // read, and the copy is all that would be left of them.
                Err(e) => {
                    log::error!(
                        "reports: backup of {} failed ({e}); the file is left as it is",
                        path.display()
                    );
                    self.retry_above = self.lines + self.rows.len() + 64;
                    return;
                }
            }
        }
        let mut text = serde_json::to_string(&Header { epoch: self.epoch }).expect("header");
        text.push('\n');
        for row in self.rows.values() {
            text.push_str(&serde_json::to_string(row).expect("row serializes"));
            text.push('\n');
        }
        let tmp = path.with_extension("jsonl.tmp");
        let written = path
            .parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| {
                let mut f = File::create(&tmp)?;
                f.write_all(text.as_bytes())?;
                f.sync_all()
            })
            .and_then(|()| fs::rename(&tmp, path));
        match written {
            Ok(()) => {
                self.lines = self.rows.len();
                self.retry_above = 0;
            }
            Err(e) => {
                log::warn!("reports: {}: {e}", path.display());
                // Not again on the very next row: after as many more updates as the file has
                // rows, or a full disk would cost a full serialisation per update.
                self.retry_above = self.lines + self.rows.len() + 64;
            }
        }
    }
}

/// The upstream reason of a code the core sets (`orders::reason`).
fn reason_of(code: u8) -> Option<SellReason> {
    use crate::orders::reason;
    Some(match code {
        reason::SELL_PRICE => SellReason::SellPrice,
        reason::AUTO_PRICE_DOWN => SellReason::AutoPriceDown,
        reason::PANIC_SELL => SellReason::PanicSell,
        reason::STOP_LOSS => SellReason::StopLoss,
        reason::TRAILING => SellReason::Trailing,
        reason::MANUAL_SELL => SellReason::ManualSell,
        reason::BV_SV_STOP => SellReason::BvSvStop,
        _ => return None,
    })
}

/// A database identity other than 0 (invalid) and 1 (the empty stub the core
/// served before it kept reports).
fn new_epoch(now_ms: i64) -> i32 {
    let e = (now_ms.wrapping_mul(0x9E37_79B9) >> 7) as i32 & 0x7fff_ffff;
    if e < 2 {
        e + 2
    } else {
        e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(filled: f64, mean: f64, close_ms: i64) -> LegState {
        LegState {
            filled,
            quantity: filled,
            mean_price: mean,
            spent: filled * mean,
            close_ms,
            closed: close_ms > 0,
            create_ms: close_ms,
            ..LegState::default()
        }
    }

    fn deal<'a>(id: u64, buy: &'a LegState, sell: &'a LegState, closed: bool) -> Deal<'a> {
        Deal {
            order_id: id,
            coin: "BTCUSDT",
            is_short: false,
            strategy_id: 7,
            signal_type: Some("MoonShot"),
            buy,
            sell,
            closed,
            panic: false,
            sell_reason: 0,
            exit: ExitSource::Core,
            ex_order_id: "42",
            emulator: false,
        }
    }

    fn temp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("aster-reports-{name}-{}", std::process::id()));
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn open_then_close_updates_one_row() {
        let mut r = Reports::open(None, 1_000_000);
        let none = LegState::default();
        assert!(r.record(&deal(5, &none, &none, false), 1_000_000).is_none());
        let buy = leg(10.0, 300.0, 2_000_000);
        let row = r
            .record(&deal(5, &buy, &none, false), 2_000_500)
            .unwrap()
            .clone();
        assert_eq!((row.rec_id, row.buy_date, row.close_date), (1, 2000, 0));
        assert_eq!((row.spent, row.profit, row.closed), (3000.0, 0.0, false));
        assert!(r.record(&deal(5, &buy, &none, false), 2_000_900).is_none());
        let sell = leg(10.0, 306.0, 3_000_000);
        let row = r.record(&deal(5, &buy, &sell, true), 3_000_100).unwrap();
        assert_eq!((row.rec_id, row.close_date, row.closed), (1, 3000, true));
        assert_eq!((row.gained, row.profit), (3060.0, 60.0));
        assert_eq!(row.sell_reason, "Sell Price");
        assert_eq!(r.max_rec_id(), 1);
        // The order's own code wins over the fallback.
        let mut d = deal(5, &buy, &sell, true);
        d.sell_reason = crate::orders::reason::AUTO_PRICE_DOWN;
        assert_eq!(
            r.record(&d, 3_000_200).unwrap().sell_reason,
            "Auto Price Down"
        );
        d.sell_reason = crate::orders::reason::STOP_LOSS;
        assert_eq!(r.record(&d, 3_000_300).unwrap().sell_reason, "StopLoss");
        // `SignalType` is the strategy's kind; a deleted strategy keeps it.
        d.signal_type = Some("DropsDetection");
        d.sell_reason = crate::orders::reason::SELL_PRICE;
        let row = r.record(&d, 3_000_400).unwrap();
        assert_eq!(row.signal_type, "DropsDetection");
        d.signal_type = None;
        d.sell_reason = crate::orders::reason::AUTO_PRICE_DOWN;
        let row = r.record(&d, 3_000_500).unwrap();
        assert_eq!(row.signal_type, "DropsDetection");
    }

    #[test]
    fn commission_is_counted_once_per_exchange_order_and_nets_the_profit() {
        let mut r = Reports::open(None, 0);
        let buy = leg(10.0, 100.0, 1000);
        let none = LegState::default();
        r.record(&deal(1, &buy, &none, false), 1000);
        // Before the close: kept, profit still 0.
        let row = r.add_commission(1, "b1", 0.4).unwrap();
        assert_eq!((row.commission, row.profit), (0.4, 0.0));
        let sell = leg(10.0, 101.0, 2000);
        let row = r.record(&deal(1, &buy, &sell, true), 2000).unwrap();
        assert!((row.profit - 9.6).abs() < 1e-9);
        let row = r.add_commission(1, "s1", 0.41).unwrap();
        assert!((row.commission - 0.81).abs() < 1e-9 && (row.profit - 9.19).abs() < 1e-9);
        // A repeated reply changes nothing; an unknown order has no row.
        assert!(r.add_commission(1, "s1", 0.41).is_none());
        assert!(r.add_commission(9, "x", 1.0).is_none());
        assert!((r.profit(3000).total - 9.19).abs() < 1e-9);
    }

    #[test]
    fn profit_by_side() {
        let mut r = Reports::open(None, 0);
        let buy = leg(2.0, 100.0, 1000);
        let sell = leg(2.0, 90.0, 2000);
        let mut d = deal(1, &buy, &sell, true);
        d.is_short = true;
        let row = r.record(&d, 3000).unwrap();
        assert_eq!((row.spent, row.gained, row.profit), (200.0, 180.0, 20.0));
        d.order_id = 2;
        d.is_short = false;
        d.exit = ExitSource::Outside;
        let row = r.record(&d, 3000).unwrap();
        assert_eq!(row.profit, -20.0);
        assert_eq!(
            (row.sell_reason.as_str(), row.comment.as_str()),
            ("Manual Sell", "closed outside the core")
        );
    }

    #[test]
    fn emulator_deals_are_reported_but_not_counted() {
        let mut r = Reports::open(None, 0);
        let buy = leg(1.0, 100.0, 1000);
        let sell = leg(1.0, 110.0, 2000);
        let mut d = deal(1, &buy, &sell, true);
        d.emulator = true;
        let row = r.record(&d, 3000).unwrap();
        assert_eq!(
            (row.profit, row.value("Emulator")),
            (10.0, Some(Value::Int(1)))
        );
        assert_eq!((r.profit(3000).total, r.profit(3000).trades), (0.0, 0));
        d.order_id = 2;
        d.emulator = false;
        let row = r.record(&d, 3000).unwrap();
        assert_eq!(row.value("Emulator"), Some(Value::Int(0)));
        assert_eq!((r.profit(3000).total, r.profit(3000).trades), (10.0, 1));
    }

    /// MAGE 22.09: an adopted foreign exit's synthetic entry has no price, so
    /// the exit's whole value must not be booked as profit.
    #[test]
    fn entry_without_cost_books_no_profit() {
        let mut r = Reports::open(None, 0);
        let buy = leg(300.0, 0.0, 1000);
        let sell = leg(300.0, 2.995, 2000);
        let mut d = deal(1, &buy, &sell, true);
        d.exit = ExitSource::Foreign;
        let row = r.record(&d, 3000).unwrap();
        assert_eq!((row.spent, row.profit), (0.0, 0.0));
        // A commission arriving later leaves the unknown profit alone.
        let row = r.add_commission(1, "s1", 0.36).unwrap();
        assert_eq!((row.commission, row.profit), (0.36, 0.0));
        assert_eq!(r.profit(3000).total, 0.0);
    }

    #[test]
    fn file_reload_keeps_epoch_last_line_and_deleted() {
        let path = temp("reload");
        let buy = leg(1.0, 10.0, 1000);
        let none = LegState::default();
        let epoch = {
            let mut r = Reports::open(Some(path.clone()), 77_000);
            r.record(&deal(1, &buy, &none, false), 1000);
            r.record(&deal(2, &buy, &none, false), 1000);
            let sell = leg(1.0, 11.0, 2000);
            r.record(&deal(1, &buy, &sell, true), 2000);
            let del = RowsDeleted {
                deleted: true,
                ranges: vec![],
                singles: vec![2],
            };
            assert_eq!(r.set_deleted(&del), vec![2]);
            assert!(r.set_deleted(&del).is_empty());
            r.epoch()
        };
        let r = Reports::open(Some(path.clone()), 99_000);
        assert_eq!(r.epoch(), epoch);
        assert!(epoch >= 2);
        assert_eq!(r.max_rec_id(), 2);
        assert!(r.row(1).unwrap().closed && r.row(2).unwrap().deleted);
        // A reopened order keeps its row.
        let mut r = r;
        let more = leg(2.0, 10.0, 1000);
        assert_eq!(
            r.record(&deal(2, &more, &none, false), 3000)
                .unwrap()
                .rec_id,
            2
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn compaction_rewrites_the_file() {
        let path = temp("compact");
        let none = LegState::default();
        {
            let mut r = Reports::open(Some(path.clone()), 1);
            for i in 1..200 {
                let buy = leg(f64::from(i), 1.0, 1000);
                r.record(&deal(1, &buy, &none, false), 1000);
            }
        }
        let r = Reports::open(Some(path.clone()), 2);
        assert_eq!(r.row(1).unwrap().quantity, 199.0);
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 2);
        let _ = fs::remove_file(path);
    }

    /// A line that is not UTF-8 costs that line, not the history after it; the file as it was is
    /// kept beside, since the rewrite drops what it could not read.
    #[test]
    fn a_line_that_is_not_text_costs_only_itself() {
        let path = temp("nonutf8");
        let buy = leg(1.0, 10.0, 1000);
        let none = LegState::default();
        {
            let mut r = Reports::open(Some(path.clone()), 5_000);
            r.record(&deal(1, &buy, &none, false), 1000);
            r.record(&deal(2, &buy, &none, false), 1000);
            r.record(&deal(3, &buy, &none, false), 1000);
        }
        let mut bytes = fs::read(&path).unwrap();
        let first_row = bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
        bytes.splice(first_row..first_row, b"\xff\xfe broken\n".iter().copied());
        fs::write(&path, &bytes).unwrap();
        let mut r = Reports::open(Some(path.clone()), 6_000);
        assert_eq!(r.max_rec_id(), 3, "the rows after the broken line are kept");
        assert_eq!(fs::read(&path).unwrap(), bytes, "reading rewrote nothing");
        // The first rewrite (here the compaction after many updates) drops the broken line —
        // after the file has been copied.
        for i in 2..150 {
            let more = leg(f64::from(i), 10.0, 1000);
            r.record(&deal(1, &more, &none, false), 2000);
        }
        let mut bak = path.as_os_str().to_owned();
        bak.push(".bak");
        let bak = PathBuf::from(bak);
        assert!(
            fs::read(&bak).unwrap().starts_with(&bytes),
            "the file as found (and what was appended to it) is kept"
        );
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(bak);
    }

    /// A history that cannot be read is not replaced by an empty one: nothing is written.
    #[test]
    fn an_unreadable_history_is_not_overwritten() {
        let dir = temp("unreadable");
        let _ = fs::remove_dir_all(&dir);
        // A directory where the file should be: opening it for reading fails with something
        // other than NotFound.
        fs::create_dir_all(&dir).unwrap();
        let buy = leg(1.0, 10.0, 1000);
        let none = LegState::default();
        let mut r = Reports::open(Some(dir.clone()), 5_000);
        assert!(r.epoch() >= 2, "a working epoch in memory");
        r.record(&deal(1, &buy, &none, false), 1000);
        assert_eq!(r.max_rec_id(), 1, "the deal is still booked in memory");
        assert!(dir.is_dir(), "and nothing replaced what was there");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_header_keeps_rows_under_a_new_epoch() {
        let path = temp("header");
        let buy = leg(1.0, 10.0, 1000);
        let none = LegState::default();
        let old = {
            let mut r = Reports::open(Some(path.clone()), 5_000);
            r.record(&deal(1, &buy, &none, false), 1000);
            r.epoch()
        };
        let text = fs::read_to_string(&path).unwrap();
        let row = text.lines().nth(1).unwrap();
        fs::write(&path, format!("garbage\n{row}\n")).unwrap();
        let r = Reports::open(Some(path.clone()), 9_000);
        assert_eq!(r.max_rec_id(), 1);
        // No header at all: the first line is a row, not lost.
        fs::write(&path, format!("{row}\n")).unwrap();
        assert_eq!(Reports::open(Some(path.clone()), 10_000).max_rec_id(), 1);
        fs::write(&path, format!("garbage\n{row}\n")).unwrap();
        let r = Reports::open(Some(path.clone()), 9_000);
        assert!(r.epoch() >= 2 && r.epoch() != old);
        assert_eq!(Reports::open(Some(path.clone()), 11_000).epoch(), r.epoch());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pages_depth_and_continuation() {
        let mut r = Reports::open(None, 0);
        let none = LegState::default();
        let day = DAY_S * 1000;
        for (id, bought) in [(1, 0), (2, 40 * day), (3, 59 * day)] {
            let buy = leg(1.0, 1.0, bought + 1000);
            r.record(&deal(id, &buy, &none, false), bought + 1000);
        }
        let now = 60 * day;
        let fresh = r.page(0, 0, now); // 30 days
        assert_eq!((fresh.count, fresh.last_rec_id), (2, 3));
        assert_eq!(r.page(0, DEPTH_ALL, now).count, 3);
        assert_eq!(r.page(0, 5, now).count, 1);
        let cont = r.page(1, 0, now);
        assert_eq!((cont.count, cont.last_rec_id), (3, 3));
        let empty = r.page(4, 0, now);
        assert_eq!((empty.count, empty.last_rec_id), (0, 0));
        assert!(empty.rows.is_empty());
    }

    #[test]
    fn alive_bitmap_and_profit_skip_deleted() {
        let mut r = Reports::open(None, 0);
        let buy = leg(1.0, 10.0, 1000);
        let sell = leg(1.0, 12.0, 2000);
        for id in 1..=9 {
            r.record(&deal(id, &buy, &sell, true), 5_000_000);
        }
        r.set_deleted(&RowsDeleted {
            deleted: true,
            ranges: vec![(2, 3)],
            singles: vec![9],
        });
        assert_eq!(r.alive_bitmap(9), vec![0b1111_1001, 0]);
        assert_eq!(r.alive_bitmap(0), Vec::<u8>::new());
        let p = r.profit(5_000_000);
        assert_eq!((p.trades, p.total), (6, 12.0));
        assert_eq!(p.hour_trades, 0); // bought at t = 1 s
        let p = r.profit(1000 + 1000);
        assert_eq!(p.hour_trades, 6);
    }

    #[test]
    fn schema_keeps_the_stub_field_and_valid_names() {
        assert_eq!((FIELDS[0].name, FIELDS[0].sql), ("newRecID", "INTEGER"));
        let mut names = std::collections::HashSet::new();
        for f in FIELDS {
            assert!(names.insert(f.name.to_ascii_lowercase()), "{}", f.name);
            assert!(!f.sql.to_ascii_uppercase().contains("PRIMARY"));
            let row = Row::default();
            if let Some(v) = row.value(f.name) {
                let kind = match v {
                    Value::Int(_) => FieldKind::Integer,
                    Value::Float(_) => FieldKind::Float,
                    Value::Text(_) => FieldKind::Text,
                };
                assert_eq!(kind, f.kind, "{}", f.name);
            }
        }
    }
}
