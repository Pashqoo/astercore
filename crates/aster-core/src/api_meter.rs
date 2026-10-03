//! What the core spends at the gateway, counted where it is paid: one cell per
//! minute per call (`GET /fapi/v1/klines`), a 24 h window, and the exchange's
//! own limits beside it — so the page can say how close the core is to being
//! refused instead of leaving the operator to guess from the journal. Ported
//! from TInvestCore, whose gateway priced methods in tariff groups; Aster
//! publishes three limits instead (`exchangeInfo.rateLimits`, measured 01.10:
//! `REQUEST_WEIGHT` 2400/min, `ORDERS` 1200/min and 300/10 s), and it reports
//! what this IP has spent of each in every answer's headers
//! (`x-mbx-used-weight-1m`, `x-mbx-order-count-1m`, `x-mbx-order-count-10s`).
//! Those three are drawn as cards of their own: the exchange's count, not one
//! the core keeps and could drift from.
//!
//! `aster::rest::Rest::exchange` is the one place every REST call goes
//! through, and it counts there (the process-wide meter, [`global`]), so a
//! call added later is counted without anyone remembering to.
//!
//! Nothing in here reaches the trading loop. The counters live behind one
//! mutex, taken for a few field additions per request; the page reads a
//! snapshot of them in the web thread, and a thread of its own writes the
//! window to disk once a minute so a restart does not wipe the day's history.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use serde::{Deserialize, Serialize};

/// Minutes kept, and the width of the page's axis.
pub const SPAN_MIN: i64 = 24 * 60;

/// The file format's own version. It is written, never checked: every field
/// below defaults, so an older file loads and a newer one loses only what this
/// build does not know about. The number is there for the reader of a file.
const FORMAT: u32 = 1;

/// The link line: the cheapest answer the gateway gives (`/fapi/v1/time`,
/// weight 1), over a connection the heartbeat keeps warm — what MoonBot's
/// terminal calls its ping. Nothing heavier belongs here: a catalog or a
/// candle window answers slowly because it is big, and averaged in it would
/// read as a latency alarm.
const LINK_METHODS: [&str; 1] = ["GET /fapi/v1/time"];

/// The order line: what it costs to place or pull an order — the exchange's
/// work and not the wire's, drawn apart from the link for that reason.
const ORDER_METHODS: [&str; 3] = [
    "POST /fapi/v3/order",
    "PUT /fapi/v3/order",
    "DELETE /fapi/v3/order",
];

/// Every call the core makes, so a card for each is on the page from the
/// first paint — flat at zero is an answer too. A call site added without its
/// line here is not lost: it appears as its own card the first time it is
/// counted (see [`ApiMeter::view`]).
pub const METHODS: [&str; 20] = [
    "GET /fapi/v1/time",
    "GET /fapi/v1/exchangeInfo",
    "GET /fapi/v1/ticker/24hr",
    "GET /fapi/v1/ticker/bookTicker",
    "GET /fapi/v1/premiumIndex",
    "GET /fapi/v1/klines",
    "GET /fapi/v3/trades",
    "GET /fapi/v3/historicalTrades",
    "GET /fapi/v1/depth",
    "GET /fapi/v3/balance",
    "GET /fapi/v3/positionRisk",
    "GET /fapi/v3/openOrders",
    "GET /fapi/v3/order",
    "POST /fapi/v3/order",
    "PUT /fapi/v3/order",
    "DELETE /fapi/v3/order",
    "POST /fapi/v3/listenKey",
    "POST /fapi/v3/leverage",
    "POST /fapi/v3/marginType",
    "GET /fapi/v3/leverageBracket",
];

/// The exchange's own figures of what this IP has spent, from the answers'
/// headers, as cards: the name, the header behind it, and its limit. The limits are written
/// here as `exchangeInfo.rateLimits` gave them on 01.10 (2400 weight, 1200 orders a minute, 300
/// a 10 s window), not read at start: a change on the exchange's side shows as a card that
/// disagrees with the header, and is changed here.
const GAUGES: [(&str, &str, i64); 3] = [
    ("weight / 1 min", "x-mbx-used-weight-1m", 2400),
    ("new orders / 1 min", "x-mbx-order-count-1m", 1200),
    ("new orders / 10 s", "x-mbx-order-count-10s", 300),
];

/// The meter every REST client of the process counts into, once `main` has
/// set it; `None` in the tests and the examples, which count nothing.
static GLOBAL: OnceLock<Arc<ApiMeter>> = OnceLock::new();

/// Make `meter` the process-wide one. The first call wins.
pub fn set_global(meter: Arc<ApiMeter>) {
    let _ = GLOBAL.set(meter);
}

pub fn global() -> Option<&'static Arc<ApiMeter>> {
    GLOBAL.get()
}

/// What one request did. The round trip is the real one either way — a refusal
/// answers as much as a fill does, and a call that never answered carries the
/// time it waited, which is what makes a dead gateway look dead on the chart
/// instead of leaving a gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Call {
    Ok {
        rtt_ms: i64,
    },
    Err {
        rtt_ms: i64,
    },
    /// Never sent: the caller held it back itself. It cost no quota, and it
    /// is the minute the core was blind — which is why it is counted apart
    /// from an error. (TInvestCore's token budget; on Aster no caller holds
    /// a call yet, and the strip stays empty.)
    Held,
}

fn zero_u32(n: &u32) -> bool {
    *n == 0
}

fn zero_u64(n: &u64) -> bool {
    *n == 0
}

fn empty(v: &[u32]) -> bool {
    v.iter().all(|n| *n == 0)
}

/// One method's minute. Short names and skipped zeros, because this is written
/// to disk every minute and most cells hold nothing but a call count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cell {
    /// Requests that left for the gateway, answered or not.
    #[serde(rename = "c", default, skip_serializing_if = "zero_u32")]
    pub calls: u32,
    /// Of those, the ones that came back an error or never came back.
    #[serde(rename = "e", default, skip_serializing_if = "zero_u32")]
    pub errors: u32,
    /// Requests the core refused to send itself (see [`Call::Held`]).
    #[serde(rename = "h", default, skip_serializing_if = "zero_u32")]
    pub held: u32,
    /// Round trips behind `calls`: how many were measured, their sum in ms and
    /// the worst of them. A mean is kept as a pair, not as a mean, so minutes
    /// and methods add up without weighting anything wrong.
    #[serde(rename = "n", default, skip_serializing_if = "zero_u32")]
    pub rtt_n: u32,
    #[serde(rename = "s", default, skip_serializing_if = "zero_u64")]
    pub rtt_sum: u64,
    #[serde(rename = "x", default, skip_serializing_if = "zero_u32")]
    pub rtt_max: u32,
}

impl Cell {
    fn note(&mut self, call: Call) {
        match call {
            Call::Ok { rtt_ms } | Call::Err { rtt_ms } => {
                self.calls += 1;
                if matches!(call, Call::Err { .. }) {
                    self.errors += 1;
                }
                // A negative round trip is a stepped clock, not a measurement.
                let ms = rtt_ms.clamp(0, u32::from(u16::MAX) as i64 * 10) as u32;
                self.rtt_n += 1;
                self.rtt_sum += u64::from(ms);
                self.rtt_max = self.rtt_max.max(ms);
            }
            Call::Held => self.held += 1,
        }
    }
}

/// Calls that share a ceiling, and the ceiling (TInvestCore's tariff group).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TariffGroup {
    /// The keys the counters use (`POST /fapi/v3/order`).
    pub methods: Vec<String>,
    pub per_minute: i64,
    pub per_second: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Row {
    min: i64,
    #[serde(default)]
    m: HashMap<String, Cell>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    minutes: Vec<Row>,
    /// Minute-epoch → the highest figure of each [`GAUGES`] header seen in it.
    #[serde(default)]
    gauges: Vec<(i64, [u32; 3])>,
}

/// A card before its series are filled: what it is called, what it may spend
/// and which counter keys feed it.
struct Card {
    name: String,
    per_minute: Option<i64>,
    per_second: Option<i64>,
    methods: Vec<String>,
}

#[derive(Default)]
struct Inner {
    /// Minute-epoch → method → cell, oldest first, at most [`SPAN_MIN`] of them.
    minutes: BTreeMap<i64, HashMap<String, Cell>>,
    /// The newest minute a call was counted in. A wall clock that stepped back
    /// must not open a minute behind it: the axis is time, and a bucket in the
    /// past would move calls under a part of the chart already drawn.
    last_min: i64,
    /// The groups priced together, empty until set.
    tariff: Vec<TariffGroup>,
    /// Minute-epoch → the highest figure of each [`GAUGES`] header.
    gauges: BTreeMap<i64, [u32; 3]>,
}

pub struct ApiMeter {
    inner: Mutex<Inner>,
    /// Held across a whole [`ApiMeter::save`], so two savers cannot write the
    /// same temp file at once.
    writing: Mutex<()>,
    /// Where the window is kept between runs; `None` — nowhere (a test, an
    /// example, any client whose numbers nobody reads).
    path: Option<PathBuf>,
}

impl ApiMeter {
    /// A meter that keeps its window in `path` (loaded now, saved by whoever
    /// calls [`ApiMeter::save`]).
    pub fn new(path: Option<PathBuf>) -> Arc<Self> {
        let meter = Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            writing: Mutex::new(()),
            path,
        });
        meter.load();
        meter
    }

    /// A meter nobody reads: for the examples and the tests, so a `Client`
    /// always has one and no call site has to ask whether it is counted.
    pub fn detached() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            writing: Mutex::new(()),
            path: None,
        })
    }

    /// A poisoned mutex still holds the truth about what was counted: a panic
    /// in one request must not take the page's numbers away for good.
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Count one request, reading the wall clock here. Every caller is a
    /// request path that would otherwise have to thread a clock through for
    /// this alone.
    pub fn note_now(&self, method: &str, call: Call) {
        self.note(method, crate::engine::now_ms(), call);
    }

    /// Count one request. `at_ms` is the wall clock, because the axis is time.
    pub fn note(&self, method: &str, at_ms: i64, call: Call) {
        let mut inner = self.inner();
        let min = (at_ms / 60_000).max(inner.last_min);
        inner.last_min = min;
        let cell = inner.minutes.entry(min).or_default();
        // The allocation is on the first call of a method in a minute, not on
        // every call: `entry` would take the key by value every time.
        match cell.get_mut(method) {
            Some(c) => c.note(call),
            None => {
                let mut c = Cell::default();
                c.note(call);
                cell.insert(method.to_string(), c);
            }
        }
        inner.prune(min);
    }

    /// The exchange's figures of what this IP has spent, from one answer's
    /// headers, in the order of [`GAUGES`]; `None` = the answer did not carry
    /// it. The minute keeps the highest.
    pub fn note_usage(&self, at_ms: i64, figures: [Option<i64>; 3]) {
        let mut inner = self.inner();
        let min = (at_ms / 60_000).max(inner.last_min);
        let row = inner.gauges.entry(min).or_default();
        for (cell, v) in row.iter_mut().zip(figures) {
            if let Some(v) = v {
                *cell = (*cell).max(v.clamp(0, i64::from(u32::MAX)) as u32);
            }
        }
        let oldest = min - (SPAN_MIN - 1);
        while inner
            .gauges
            .first_key_value()
            .is_some_and(|(m, _)| *m < oldest)
        {
            inner.gauges.pop_first();
        }
    }

    /// The groups priced together. Without them the cards carry no ceiling,
    /// which is the honest picture of not knowing.
    pub fn set_tariff(&self, groups: Vec<TariffGroup>) {
        self.inner().tariff = groups;
    }

    /// What the page draws.
    ///
    /// The mutex is held for the whole build, and it is the same one every
    /// request takes to be counted — so the build walks the window **once**,
    /// with each method's card resolved beforehand, rather than once per card.
    /// A page polling this during a stop must not be what slows a withdrawal
    /// down.
    pub fn view(&self, now_ms: i64) -> View {
        let inner = self.inner();
        // The axis ends at the newest of the two: a clock that stepped back
        // would otherwise cut the minutes already counted off the right edge.
        let end = (now_ms / 60_000).max(inner.last_min);
        let from = end - (SPAN_MIN - 1);
        let span = SPAN_MIN as usize;

        // Every method that has a card: the ones the core is known to call and
        // the ones it turned out to call.
        let mut known: Vec<&str> = METHODS.to_vec();
        for cells in inner.minutes.values() {
            for m in cells.keys() {
                if !known.contains(&m.as_str()) {
                    known.push(m);
                }
            }
        }

        // The cards, in the order the page shows them: the groups the tariff
        // priced together first, alphabetically, then what it never named —
        // anything no limit prices. Two
        // runs on purpose: a card with a ceiling and a card without are
        // different questions, and mixing them alphabetically would scatter
        // the ones an operator opens this tab for.
        let mut cards: Vec<Card> = Vec::new();
        let mut grouped: Vec<String> = Vec::new();
        for t in &inner.tariff {
            let mut mine: Vec<String> = t
                .methods
                .iter()
                .filter(|m| known.contains(&m.as_str()))
                .cloned()
                .collect();
            if mine.is_empty() {
                continue;
            }
            mine.sort();
            grouped.extend(mine.iter().cloned());
            cards.push(Card {
                name: group_name(&mine),
                per_minute: Some(t.per_minute),
                per_second: t.per_second,
                methods: mine,
            });
        }
        cards.sort_by(|a, b| a.name.cmp(&b.name));
        let mut loose: Vec<&str> = known
            .iter()
            .filter(|m| !grouped.iter().any(|g| g == *m))
            .copied()
            .collect();
        // By the name the card carries, not by the key behind it: the page
        // shows `PostOrder`, and an order that reads as unsorted there is a
        // list the eye has to search instead of scan.
        loose.sort_unstable_by(|a, b| short(a).cmp(&short(b)).then(a.cmp(b)));
        for m in loose {
            cards.push(Card {
                name: short(m),
                per_minute: None,
                per_second: None,
                methods: vec![m.to_string()],
            });
        }

        // Which cards each method's counts go to, so the pass below can touch
        // every cell exactly once. Cards, plural: nothing says two groups may
        // not price the same method, and its calls then spend both ceilings —
        // a single index would have quietly left one of the two cards flat.
        let mut card: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, c) in cards.iter().enumerate() {
            for m in &c.methods {
                card.entry(m.as_str()).or_default().push(i);
            }
        }
        let mut groups: Vec<GroupView> = cards
            .iter()
            .map(|c| GroupView {
                name: c.name.clone(),
                per_minute: c.per_minute,
                per_second: c.per_second,
                methods: c.methods.iter().map(|m| short(m)).collect(),
                calls: vec![0; span],
                errors: vec![0; span],
                held: vec![0; span],
            })
            .collect();
        let (mut link, mut orders) = (Bars::new(span), Bars::new(span));

        for (min, cells) in &inner.minutes {
            if *min < from || *min > end {
                continue;
            }
            let i = (min - from) as usize;
            for (m, c) in cells {
                if let Some(mine) = card.get(m.as_str()) {
                    for &g in mine {
                        let row = &mut groups[g];
                        row.calls[i] += c.calls;
                        row.errors[i] += c.errors;
                        row.held[i] += c.held;
                    }
                }
                // A method on neither list is on neither line: the cards
                // below still count it, but it is not a round trip either
                // question is asked about.
                if LINK_METHODS.contains(&m.as_str()) {
                    link.note(i, c);
                }
                if ORDER_METHODS.contains(&m.as_str()) {
                    orders.note(i, c);
                }
            }
        }
        // The exchange's own counts, first: they are the limits the core is
        // refused on, whatever its calls added up to.
        let gauges: Vec<GroupView> = GAUGES
            .iter()
            .enumerate()
            .map(|(k, &(name, header, limit))| {
                let mut calls = vec![0; span];
                for (min, row) in inner.gauges.range(from..=end) {
                    calls[(min - from) as usize] = row[k];
                }
                GroupView {
                    name: name.to_string(),
                    per_minute: Some(limit),
                    per_second: None,
                    methods: vec![header.to_string()],
                    calls,
                    errors: Vec::new(),
                    held: Vec::new(),
                }
            })
            .collect();
        groups.splice(0..0, gauges);
        // Zeros the page does not need to be told about: an all-zero series
        // travels as `[]` and reads as zeros. On a quiet day that is most of
        // the payload.
        for g in &mut groups {
            if empty(&g.errors) {
                g.errors.clear();
            }
            if empty(&g.held) {
                g.held.clear();
            }
        }

        View {
            from_min: from,
            span_min: SPAN_MIN,
            tariff: !inner.tariff.is_empty(),
            groups,
            ping: PingView {
                link: link.into_series(),
                orders: orders.into_series(),
                link_methods: short_names(&LINK_METHODS),
                order_methods: short_names(&ORDER_METHODS),
            },
        }
    }

    /// Write the window. Temp + rename, so a core killed mid-write leaves the
    /// last good file rather than half of a new one.
    pub fn save(&self) {
        let Some(path) = &self.path else { return };
        // One writer at a time: the meter thread saves every minute and the
        // stop saves once more on the way out, and they share the temp file.
        // Its own lock, not the counters' — a request being counted must not
        // wait for a file.
        let _writing = self.writing.lock().unwrap_or_else(PoisonError::into_inner);
        let saved = {
            let inner = self.inner();
            Saved {
                version: FORMAT,
                gauges: inner.gauges.iter().map(|(m, g)| (*m, *g)).collect(),
                minutes: inner
                    .minutes
                    .iter()
                    .map(|(min, m)| Row {
                        min: *min,
                        m: m.clone(),
                    })
                    .collect(),
            }
        };
        let json = match serde_json::to_vec(&saved) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("api meter: {e}");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        if let Err(e) = fs::write(&tmp, &json).and_then(|()| fs::rename(&tmp, path)) {
            log::warn!("api meter: {} not written: {e}", path.display());
        }
    }

    /// Read the window back. Missing is not an error and neither is broken:
    /// this is a chart, and a core that refused to start over one would be
    /// trading nothing to protect a picture.
    fn load(&self) {
        let Some(path) = &self.path else { return };
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                log::warn!("api meter: {} not read: {e}", path.display());
                return;
            }
        };
        let saved: Saved = match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(e) => {
                log::warn!(
                    "api meter: {} is not readable: {e}; starting empty",
                    path.display()
                );
                return;
            }
        };
        let mut inner = self.inner();
        inner.gauges.extend(saved.gauges);
        for row in saved.minutes {
            if row.m.is_empty() {
                continue;
            }
            inner.last_min = inner.last_min.max(row.min);
            inner.minutes.insert(row.min, row.m);
        }
        let last = inner.last_min;
        inner.prune(last);
        let kept = inner.minutes.len();
        drop(inner);
        log::info!("api meter: {kept} minute(s) of history restored");
    }
}

impl Inner {
    /// Drop what has left the window.
    fn prune(&mut self, now_min: i64) {
        let cutoff = now_min - (SPAN_MIN - 1);
        if self
            .minutes
            .first_key_value()
            .is_some_and(|(m, _)| *m >= cutoff)
        {
            return;
        }
        self.minutes = self.minutes.split_off(&cutoff);
    }
}

/// A card's name: the call without its API version, `GET /fapi/v1/klines` →
/// `GET klines`. The verb stays: `POST order` and `DELETE order` are two
/// different costs at the exchange.
fn short(method: &str) -> String {
    let (verb, path) = method.split_once(' ').unwrap_or(("", method));
    let path = path
        .strip_prefix("/fapi/")
        .and_then(|p| p.split_once('/'))
        .map_or(path, |(_, rest)| rest);
    if verb.is_empty() {
        path.to_string()
    } else {
        format!("{verb} {path}")
    }
}

/// What a card is called: the one call it holds, or the calls behind it.
fn group_name(methods: &[String]) -> String {
    match methods {
        [] => "—".into(),
        [one] => short(one),
        many => many
            .iter()
            .map(|m| short(m))
            .collect::<Vec<_>>()
            .join(" + "),
    }
}

/// One card: what it is, what it may spend, and what it spent, minute by
/// minute. `errors` and `held` arrive empty when they are all zeros.
#[derive(Debug, Serialize)]
pub struct GroupView {
    pub name: String,
    pub per_minute: Option<i64>,
    pub per_second: Option<i64>,
    pub methods: Vec<String>,
    pub calls: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub held: Vec<u32>,
}

/// One line of the ping chart: per minute, the mean and the worst round trip
/// of the methods behind it, and how many samples they came from. A minute with
/// no sample carries `n = 0`, and the page draws a gap there — a zero would
/// read as an instant answer.
#[derive(Debug, Serialize)]
pub struct PingSeries {
    pub avg: Vec<u32>,
    pub max: Vec<u32>,
    pub n: Vec<u32>,
}

/// The two lines, and they are never averaged together: [`LINK_METHODS`] is
/// the wire and the gateway's cheapest answer, [`ORDER_METHODS`] is the
/// exchange. The method names travel with them so the page can say which call
/// it is showing without keeping a second copy of the lists to drift from
/// these.
#[derive(Debug, Serialize)]
pub struct PingView {
    pub link: PingSeries,
    pub orders: PingSeries,
    pub link_methods: Vec<String>,
    pub order_methods: Vec<String>,
}

/// One series while [`ApiMeter::view`] is walking the window: the sums a mean
/// is made of at the end, rather than a mean per minute that later minutes
/// would have to be weighted back into.
struct Bars {
    sums: Vec<u64>,
    max: Vec<u32>,
    n: Vec<u32>,
}

impl Bars {
    fn new(span: usize) -> Self {
        Self {
            sums: vec![0; span],
            max: vec![0; span],
            n: vec![0; span],
        }
    }

    fn note(&mut self, i: usize, c: &Cell) {
        self.sums[i] += c.rtt_sum;
        self.n[i] += c.rtt_n;
        self.max[i] = self.max[i].max(c.rtt_max);
    }

    fn into_series(self) -> PingSeries {
        let avg = self
            .n
            .iter()
            .zip(&self.sums)
            .map(|(&n, &sum)| {
                if n > 0 {
                    (sum / u64::from(n)) as u32
                } else {
                    0
                }
            })
            .collect();
        PingSeries {
            avg,
            max: self.max,
            n: self.n,
        }
    }
}

/// The short names of a line's methods, for the page's legend.
fn short_names(methods: &[&str]) -> Vec<String> {
    methods.iter().map(|m| short(m)).collect()
}

#[derive(Debug, Serialize)]
pub struct View {
    /// Minute-epoch of point 0; point `i` is `from_min + i`.
    pub from_min: i64,
    pub span_min: i64,
    /// Whether the groups priced together are known: without them no call
    /// card has a ceiling (the exchange's own gauges always do).
    pub tariff: bool,
    pub groups: Vec<GroupView>,
    pub ping: PingView,
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000;
    /// An arbitrary but fixed wall clock, on a minute boundary.
    const T0: i64 = 29_000_000 * MIN;

    fn tariff() -> Vec<TariffGroup> {
        vec![
            TariffGroup {
                methods: vec![
                    "GET /fapi/v1/klines".into(),
                    "GET /fapi/v3/historicalTrades".into(),
                ],
                per_minute: 600,
                per_second: None,
            },
            TariffGroup {
                methods: vec!["POST /fapi/v3/order".into()],
                per_minute: 900,
                per_second: Some(15),
            },
        ]
    }

    fn group<'a>(v: &'a View, name: &str) -> &'a GroupView {
        v.groups
            .iter()
            .find(|g| g.name == name)
            .unwrap_or_else(|| panic!("no card {name}; cards: {:?}", names(v)))
    }

    fn names(v: &View) -> Vec<&str> {
        v.groups.iter().map(|g| g.name.as_str()).collect()
    }

    fn path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aster-meter-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("{name}.json"))
    }

    #[test]
    fn a_minute_holds_the_calls_of_that_minute() {
        let m = ApiMeter::detached();
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 30 });
        m.note("POST /fapi/v3/order", T0 + 999, Call::Ok { rtt_ms: 40 });
        m.note("POST /fapi/v3/order", T0 + MIN, Call::Ok { rtt_ms: 50 });
        let v = m.view(T0 + MIN);
        let g = group(&v, "POST order");
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(g.calls[last], 1, "the newest minute");
        assert_eq!(g.calls[last - 1], 2, "the one before it");
        assert_eq!(v.from_min, T0 / MIN + 1 - (SPAN_MIN - 1));
    }

    #[test]
    fn what_leaves_the_window_is_forgotten() {
        let m = ApiMeter::detached();
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 10 });
        let v = m.view(T0);
        assert_eq!(group(&v, "POST order").calls[(SPAN_MIN - 1) as usize], 1);
        // One minute past the window, counted by a later call.
        let later = T0 + SPAN_MIN * MIN;
        m.note("GET /fapi/v3/openOrders", later, Call::Ok { rtt_ms: 10 });
        let v = m.view(later);
        assert!(
            group(&v, "POST order").calls.iter().all(|n| *n == 0),
            "the old minute is gone, not shifted"
        );
        assert_eq!(
            group(&v, "GET openOrders").calls[(SPAN_MIN - 1) as usize],
            1
        );
    }

    #[test]
    fn a_clock_that_stepped_back_keeps_the_newest_minute() {
        let m = ApiMeter::detached();
        m.note("POST /fapi/v3/order", T0 + 5 * MIN, Call::Ok { rtt_ms: 10 });
        // The wall clock jumps back two minutes: the call still belongs to the
        // newest minute, and the axis does not shrink under the page.
        m.note("POST /fapi/v3/order", T0 + 3 * MIN, Call::Ok { rtt_ms: 10 });
        let v = m.view(T0 + 3 * MIN);
        let g = group(&v, "POST order");
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(g.calls[last], 2, "both in the newest minute");
        assert_eq!(v.from_min, (T0 + 5 * MIN) / MIN - (SPAN_MIN - 1));
    }

    #[test]
    fn an_error_and_a_hold_are_counted_apart() {
        let m = ApiMeter::detached();
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 10 });
        m.note("POST /fapi/v3/order", T0, Call::Err { rtt_ms: 20 });
        m.note("POST /fapi/v3/order", T0, Call::Held);
        let v = m.view(T0);
        let g = group(&v, "POST order");
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(g.calls[last], 2, "a held call was never sent");
        assert_eq!(g.errors[last], 1);
        assert_eq!(g.held[last], 1);
    }

    #[test]
    fn a_series_of_nothing_but_zeros_travels_empty() {
        let m = ApiMeter::detached();
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 10 });
        let v = m.view(T0);
        let g = group(&v, "POST order");
        assert!(g.errors.is_empty(), "no errors: nothing to send");
        assert!(g.held.is_empty());
        assert_eq!(g.calls.len(), SPAN_MIN as usize, "the calls always travel");
    }

    #[test]
    fn the_tariff_names_the_cards_and_their_ceiling() {
        let m = ApiMeter::detached();
        m.set_tariff(tariff());
        let v = m.view(T0);
        let md = group(&v, "GET klines + GET historicalTrades");
        assert_eq!(md.per_minute, Some(600));
        assert_eq!(md.per_second, None);
        assert_eq!(md.methods, ["GET klines", "GET historicalTrades"]);
        let post = group(&v, "POST order");
        assert_eq!(post.per_minute, Some(900));
        assert_eq!(post.per_second, Some(15));
        assert!(v.tariff);
    }

    #[test]
    fn the_two_series_of_a_group_add_its_methods_up() {
        let m = ApiMeter::detached();
        m.set_tariff(tariff());
        m.note("GET /fapi/v1/klines", T0, Call::Ok { rtt_ms: 10 });
        m.note(
            "GET /fapi/v3/historicalTrades",
            T0,
            Call::Err { rtt_ms: 10 },
        );
        let v = m.view(T0);
        let g = group(&v, "GET klines + GET historicalTrades");
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(g.calls[last], 2);
        assert_eq!(g.errors[last], 1);
    }

    #[test]
    fn a_method_no_group_named_gets_a_card_of_its_own() {
        let m = ApiMeter::detached();
        m.set_tariff(tariff());
        // Declared in METHODS, named by no group: its own card, no ceiling.
        let v = m.view(T0);
        let s = group(&v, "POST listenKey");
        assert_eq!(s.per_minute, None);
        // And one nobody declared at all, counted for the first time.
        m.note("GET /fapi/v1/fundingRate", T0, Call::Ok { rtt_ms: 300 });
        let v = m.view(T0);
        assert_eq!(
            group(&v, "GET fundingRate").calls[(SPAN_MIN - 1) as usize],
            1
        );
    }

    #[test]
    fn the_cards_with_a_ceiling_come_first() {
        let m = ApiMeter::detached();
        m.set_tariff(tariff());
        let v = m.view(T0);
        let ceilinged: Vec<&str> = v
            .groups
            .iter()
            .take_while(|g| g.per_minute.is_some())
            .map(|g| g.name.as_str())
            .collect();
        // The exchange's own gauges lead, then the priced groups, sorted.
        assert_eq!(
            ceilinged,
            [
                "weight / 1 min",
                "new orders / 1 min",
                "new orders / 10 s",
                "GET klines + GET historicalTrades",
                "POST order"
            ],
            "sorted, and first"
        );
        assert!(
            v.groups[ceilinged.len()..]
                .iter()
                .all(|g| g.per_minute.is_none()),
            "and nothing with a ceiling after them"
        );
        let loose: Vec<&str> = v.groups[ceilinged.len()..]
            .iter()
            .map(|g| g.name.as_str())
            .collect();
        let mut sorted = loose.clone();
        sorted.sort_unstable();
        assert_eq!(loose, sorted, "the ones without a ceiling are sorted too");
    }

    #[test]
    fn a_method_two_groups_price_feeds_both_cards() {
        let m = ApiMeter::detached();
        m.set_tariff(vec![
            TariffGroup {
                methods: vec!["POST /fapi/v3/order".into()],
                per_minute: 900,
                per_second: Some(15),
            },
            // Hypothetical, and that is the point: the gateway partitions its
            // methods today, and the chart must not quietly drop one of the
            // two ceilings the day it stops.
            TariffGroup {
                methods: vec!["POST /fapi/v3/order".into(), "DELETE /fapi/v3/order".into()],
                per_minute: 300,
                per_second: None,
            },
        ]);
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 10 });
        let v = m.view(T0);
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(group(&v, "POST order").calls[last], 1, "its own group");
        assert_eq!(
            group(&v, "DELETE order + POST order").calls[last],
            1,
            "and the shared one"
        );
    }

    #[test]
    fn a_meter_says_whether_the_tariff_answered() {
        let m = ApiMeter::detached();
        m.set_tariff(tariff());
    }

    #[test]
    fn without_a_tariff_every_method_is_its_own_card() {
        let m = ApiMeter::detached();
        let v = m.view(T0);
        assert!(!v.tariff);
        // The exchange's three gauges, then a card for every call, none of
        // which has a ceiling.
        assert_eq!(v.groups.len(), GAUGES.len() + METHODS.len());
        assert!(v.groups[GAUGES.len()..]
            .iter()
            .all(|g| g.per_minute.is_none()));
        assert!(names(&v).contains(&"POST order"));
    }

    #[test]
    fn the_link_and_the_order_lines_are_never_averaged_together() {
        let m = ApiMeter::detached();
        // A catalog page answers in seconds because it is big.
        m.note("GET /fapi/v1/exchangeInfo", T0, Call::Ok { rtt_ms: 9_000 });
        // The portfolio poll is a read, but not a light one: on neither line.
        m.note("GET /fapi/v3/positionRisk", T0, Call::Ok { rtt_ms: 50 });
        m.note("GET /fapi/v1/time", T0, Call::Ok { rtt_ms: 10 });
        m.note("GET /fapi/v1/time", T0, Call::Ok { rtt_ms: 20 });
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 160 });
        m.note("DELETE /fapi/v3/order", T0, Call::Ok { rtt_ms: 300 });
        let v = m.view(T0);
        let last = (SPAN_MIN - 1) as usize;

        assert_eq!(v.ping.link.n[last], 2, "the catalog is not a link sample");
        assert_eq!(v.ping.link.avg[last], 15);
        assert_eq!(v.ping.link.max[last], 20);
        assert_eq!(
            v.ping.link.n[last - 1],
            0,
            "a minute with no sample is a gap"
        );

        assert_eq!(v.ping.orders.n[last], 2);
        assert_eq!(v.ping.orders.avg[last], 230);
        assert_eq!(v.ping.orders.max[last], 300);

        // The whole point of the split: one 300 ms replacement must not move
        // the line that answers "is the link healthy".
        assert!(v.ping.link.avg[last] < v.ping.orders.avg[last] / 10);
    }

    #[test]
    fn the_page_is_told_which_calls_each_line_is() {
        let v = ApiMeter::detached().view(T0);
        assert_eq!(v.ping.link_methods, vec!["GET time"]);
        assert_eq!(
            v.ping.order_methods,
            vec!["POST order", "PUT order", "DELETE order"]
        );
    }

    #[test]
    fn a_refusal_is_a_round_trip_and_a_hold_is_not() {
        let m = ApiMeter::detached();
        m.note("GET /fapi/v1/time", T0, Call::Err { rtt_ms: 80 });
        m.note("GET /fapi/v1/time", T0, Call::Held);
        let v = m.view(T0);
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(
            v.ping.link.n[last], 1,
            "a refusal answered; a hold never left"
        );
        assert_eq!(v.ping.link.avg[last], 80);
    }

    #[test]
    fn the_window_survives_a_restart() {
        let p = path("restart");
        let _ = fs::remove_file(&p);
        let m = ApiMeter::new(Some(p.clone()));
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 30 });
        m.note("POST /fapi/v3/order", T0, Call::Err { rtt_ms: 40 });
        m.save();
        let back = ApiMeter::new(Some(p.clone()));
        let v = back.view(T0);
        let g = group(&v, "POST order");
        let last = (SPAN_MIN - 1) as usize;
        assert_eq!(g.calls[last], 2);
        assert_eq!(g.errors[last], 1);
        assert_eq!(v.ping.orders.n[last], 2, "the round trips came back too");
        assert_eq!(v.ping.orders.avg[last], 35);
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn a_file_from_outside_the_window_is_dropped_on_load() {
        let p = path("old");
        fs::write(
            &p,
            format!(
                r#"{{"version":1,"minutes":[{{"min":{},"m":{{"POST /fapi/v3/order":{{"c":5}}}}}},
                     {{"min":{},"m":{{"GET /fapi/v3/openOrders":{{"c":7}}}}}}]}}"#,
                T0 / MIN,
                T0 / MIN + SPAN_MIN
            ),
        )
        .unwrap();
        let m = ApiMeter::new(Some(p.clone()));
        let v = m.view(T0 + SPAN_MIN * MIN);
        assert!(
            group(&v, "POST order").calls.iter().all(|n| *n == 0),
            "a day older than the newest row is out"
        );
        assert_eq!(
            group(&v, "GET openOrders").calls[(SPAN_MIN - 1) as usize],
            7
        );
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn a_broken_file_starts_empty_instead_of_refusing() {
        let p = path("broken");
        fs::write(&p, b"{not json").unwrap();
        let m = ApiMeter::new(Some(p.clone()));
        let v = m.view(T0);
        assert!(v.groups.iter().all(|g| g.calls.iter().all(|n| *n == 0)));
        // And it writes over it rather than leaving the rubbish.
        m.note("POST /fapi/v3/order", T0, Call::Ok { rtt_ms: 10 });
        m.save();
        let back = ApiMeter::new(Some(p.clone()));
        assert_eq!(
            group(&back.view(T0), "POST order").calls[(SPAN_MIN - 1) as usize],
            1
        );
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn a_cell_of_an_older_build_loads_with_what_it_has() {
        let p = path("partial");
        // No `e`, `h`, `n`, `s`, `x`: every field defaults.
        fs::write(
            &p,
            format!(
                r#"{{"minutes":[{{"min":{},"m":{{"POST /fapi/v3/order":{{"c":3}}}}}}]}}"#,
                T0 / MIN
            ),
        )
        .unwrap();
        let m = ApiMeter::new(Some(p.clone()));
        let v = m.view(T0);
        assert_eq!(group(&v, "POST order").calls[(SPAN_MIN - 1) as usize], 3);
        assert_eq!(v.ping.orders.n[(SPAN_MIN - 1) as usize], 0);
        let _ = fs::remove_file(&p);
    }

    #[test]
    fn every_ping_method_is_a_method_the_core_calls() {
        for m in LINK_METHODS.iter().chain(&ORDER_METHODS) {
            assert!(
                METHODS.contains(m),
                "{m} is on a ping line but not among the methods the core calls"
            );
            assert!(
                !(LINK_METHODS.contains(m) && ORDER_METHODS.contains(m)),
                "{m} is on both lines; one call would be counted as two answers"
            );
        }
    }

    /// The exchange's own counts: the highest figure each header gave in a
    /// minute, against the limit it is refused at; an answer without the
    /// header changes nothing.
    #[test]
    fn the_exchange_headers_are_gauges_with_their_limits() {
        let m = ApiMeter::detached();
        m.note_usage(T0, [Some(40), None, Some(3)]);
        m.note_usage(T0 + 1_000, [Some(42), Some(7), Some(1)]);
        m.note_usage(T0 + 2_000, [None, None, None]);
        m.note_usage(T0 + MIN, [Some(2), None, None]);
        let v = m.view(T0 + MIN);
        let last = (SPAN_MIN - 1) as usize;
        let w = group(&v, "weight / 1 min");
        assert_eq!(w.per_minute, Some(2400));
        assert_eq!((w.calls[last - 1], w.calls[last]), (42, 2));
        let o = group(&v, "new orders / 10 s");
        assert_eq!((o.per_minute, o.calls[last - 1]), (Some(300), 3));
        assert_eq!(group(&v, "new orders / 1 min").calls[last - 1], 7);
    }

    #[test]
    fn a_name_is_the_call_without_its_api_version() {
        assert_eq!(short("GET /fapi/v1/klines"), "GET klines");
        assert_eq!(short("POST /fapi/v3/order"), "POST order");
        assert_eq!(short("GET /fapi/v1/ticker/24hr"), "GET ticker/24hr");
        assert_eq!(short("x-mbx-used-weight-1m"), "x-mbx-used-weight-1m");
        assert_eq!(group_name(&["POST /fapi/v3/order".into()]), "POST order");
        assert_eq!(
            group_name(&["POST /fapi/v3/order".into(), "DELETE /fapi/v3/order".into()]),
            "POST order + DELETE order"
        );
        assert_eq!(group_name(&[]), "—");
    }
}
