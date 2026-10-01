//! The strategy screener: which markets a strategy watches this pass. The
//! white list, or the `MarketTags` classes ranked by MoonBot's dynamic white
//! list — the pool sorted by one key and its head kept — minus the black
//! lists, static and dynamic.
//!
//! The volume bounds and the deltas are NOT here. They gate the entry on a
//! market of the pool, pass by pass (`moonshot::delta_gate`), so a market that
//! goes quiet for an hour keeps its place, its subscription and its detect
//! instead of being dropped and replaced by the next one down the ranking.
//! The pool answers *which markets this strategy is about*; the filters answer
//! *may it enter this one right now*. What it then actually stands on is
//! `MaxMarkets`' business, not this file's.

use std::collections::{HashMap, HashSet};

use crate::model::{Catalog as Model, Market};
use crate::moonshot::{Ctx, Params};

/// Floor under `Dyn_Refresh`: recomputing the pool re-sorts the whole
/// catalog and moves the subscription sets with it. MoonBot's own advice is
/// no faster than 61 s ("too frequent a recount loads the CPU and can earn a
/// ban for re-placing orders too often"); the core takes 30 s as the hard
/// floor and MoonBot's 61 s as the default.
pub const REFRESH_MIN_MS: i64 = 30_000;
/// Ceiling over it: a pool older than an hour is not a screener any more, and
/// a file carrying milliseconds where seconds belong (or anything else absurd)
/// would otherwise freeze the pool for good — `now / period` stops changing
/// and nothing says why.
pub const REFRESH_MAX_MS: i64 = 3_600_000;
/// MoonBot's own default (demo `Binance Futures-BTC-strat.txt`), and the
/// schema's: `Dyn_Refresh` seconds.
pub const REFRESH_DEFAULT_S: i32 = 61;

/// How far past `DynWL_Count` a market ALREADY in the pool may fall before it
/// leaves, as a fraction of the count: one fifth, so a top-100 pool holds its
/// members down to rank 120.
///
/// Without it the pool trades its own boundary: the markets around rank
/// `Count` swap places on every recompute, and each swap costs the leaving
/// market its book subscription, its hook tape and its strike track — which it
/// then rebuilds from nothing when it comes back a minute later. The pool
/// stays exactly `Count` markets: a straggler keeps its slot by displacing the
/// WORST-ranked newcomer, never a better one, so a market at rank 1 is never
/// held out and at most a fifth of the pool is held by markets on their way
/// out.
const KEEP_BAND: usize = 5;

/// A pool past this is worth a line of its own: it is one market-data stream
/// chunk (`feed::MAX_INSTRUMENTS`), and with the volume bounds gone from the
/// screener nothing but `DynWL_Count` keeps a class pool below it.
pub const POOL_WIDE: usize = 300;

/// A sort key of MoonBot's dynamic lists. The Binance-only keys (`MaxOrder`,
/// `Orders`, `Session`, `MaxPos`, `MarkPrice`, `Funding`, `Leverage`) are not
/// here yet: ported from TInvestCore, whose venue had nothing behind them.
/// Aster has a mark price and funding, so two of them could come back. `24h-Delta` is missing for
/// its own reason — MoonBot counts it from the previous session's close, which
/// is `GetClosePrices`, not our 24 h window (that one opens at the oldest
/// minute traded, a different number on any day with a gap).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Last1m,
    Last15m,
    Last30m,
    Last1h,
    Last2h,
    Last3h,
    Pump5m,
    Pump1h,
    Dump1h,
    MinuteVol,
    Vol3m,
    Vol5m,
    HourlyVol,
    DailyVol,
}

impl SortKey {
    /// MoonBot's spellings (FAQ, "Настройки стратегий"), `HourtyVol` typo
    /// included: a strategy file written by MoonBot carries it.
    pub const NAMES: [(&'static str, SortKey); 14] = [
        ("Last1mDelta", SortKey::Last1m),
        ("Last15mDelta", SortKey::Last15m),
        ("Last30mDelta", SortKey::Last30m),
        ("Last1hDelta", SortKey::Last1h),
        ("Last2hDelta", SortKey::Last2h),
        ("Last3hDelta", SortKey::Last3h),
        ("Pump5m", SortKey::Pump5m),
        ("Pump1h", SortKey::Pump1h),
        ("Dump1h", SortKey::Dump1h),
        ("MinuteVol", SortKey::MinuteVol),
        ("3Min-Vol", SortKey::Vol3m),
        ("5Min-Vol", SortKey::Vol5m),
        ("HourtyVol", SortKey::HourlyVol),
        ("DailyVol", SortKey::DailyVol),
    ];
    /// The terminal's combo for `DynWL_SortBy` / `DynBL_SortBy`.
    pub const PICKLIST: &'static str = "Last1mDelta|Last15mDelta|Last30mDelta|Last1hDelta|\
        Last2hDelta|Last3hDelta|Pump5m|Pump1h|Dump1h|MinuteVol|3Min-Vol|5Min-Vol|HourtyVol|DailyVol";

    /// The key under the name a strategy file and the combo spell it with —
    /// [`parse`](Self::parse) reads exactly this back.
    pub fn name(self) -> &'static str {
        Self::NAMES
            .iter()
            .find(|&&(_, k)| k == self)
            .map_or("", |&(n, _)| n)
    }

    /// `Err` holds the name as written, for the log line.
    pub fn parse(name: &str) -> Result<Self, String> {
        let name = name.trim();
        // MoonBot's own typo has the obvious alias; nothing else is guessed.
        let wanted = if name.eq_ignore_ascii_case("HourlyVol") {
            "HourtyVol"
        } else {
            name
        };
        Self::NAMES
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(wanted))
            .map(|&(_, key)| key)
            .ok_or_else(|| name.to_string())
    }

    /// The key's unit: turnover of turnover, or a signed % of the window's
    /// opening price. `FilterBy` reports the number it refused in the log
    /// line, and turnover printed as a percentage read as nonsense.
    pub fn turnover(self) -> bool {
        matches!(
            self,
            SortKey::MinuteVol
                | SortKey::Vol3m
                | SortKey::Vol5m
                | SortKey::HourlyVol
                | SortKey::DailyVol
        )
    }

    /// What the market sorts by, in the key's own unit: % of the window's
    /// opening price for the deltas and the pump/dump, turnover for the
    /// volumes. `None` = not measured — a market whose window holds no price
    /// to measure from (a restart before its warm-up bars land, an instrument
    /// class ISS has no minute bars for) is not the same as one that stood
    /// still, and it takes no place in either list. Turnover has no such
    /// state: an empty window is zero turnover, the same reading the volume
    /// filters (`MinVolume` and friends) take — a key that disagreed with the
    /// filter beside it would be the worse answer.
    pub fn value(self, idx: u16, cx: &Ctx) -> Option<f64> {
        let (win, now) = (cx.win, cx.now);
        let delta = |minutes| {
            let last = cx.model.at(idx).map_or(0.0, |m| m.last());
            win.delta(idx, last, now, minutes)
        };
        match self {
            SortKey::Last1m => delta(1),
            SortKey::Last15m => delta(15),
            SortKey::Last30m => delta(30),
            SortKey::Last1h => delta(60),
            SortKey::Last2h => delta(120),
            SortKey::Last3h => delta(180),
            SortKey::Pump5m => win.pump(idx, now, 5),
            SortKey::Pump1h => win.pump(idx, now, 60),
            SortKey::Dump1h => win.dump(idx, now, 60),
            SortKey::MinuteVol => Some(win.turnover(idx, now, 1)),
            SortKey::Vol3m => Some(win.turnover(idx, now, 3)),
            SortKey::Vol5m => Some(win.turnover(idx, now, 5)),
            SortKey::HourlyVol => Some(win.turnover(idx, now, 60)),
            SortKey::DailyVol => Some(win.vol24(idx, now)),
        }
    }
}

/// One of the two dynamic lists: how to sort the pool and how many markets to
/// take off the top. `count` 0 turns the list off — MoonBot's default, and
/// what every strategy file written before these fields reads as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynList {
    /// `Err` holds the name as written; it only matters while the list is on.
    pub by: Result<SortKey, String>,
    pub desc: bool,
    pub count: usize,
}

impl DynList {
    pub fn new(by: &str, desc: bool, count: i64) -> Self {
        Self {
            by: SortKey::parse(by),
            desc,
            count: count.max(0) as usize,
        }
    }

    pub fn on(&self) -> bool {
        self.count > 0
    }

    /// `pool` sorted by this list's key, the market it ranks first at the
    /// front: `(market, value)` for the markets the key measures, and nothing
    /// for the ones it does not — those are candidates for neither list.
    /// Markets that tie keep the pool's own order (the sort is stable).
    ///
    /// [`head`](Self::head) takes its count off the front of this, and
    /// [`explain`] numbers the whole of it: the ranking the strategy trades by
    /// and the ranking the page prints are one sort, in one place.
    fn ranked(&self, pool: &[u16], cx: &Ctx) -> Vec<(u16, f64)> {
        let Ok(key) = self.by else {
            return Vec::new();
        };
        let mut rows: Vec<(u16, f64)> = pool
            .iter()
            .filter_map(|&i| Some((i, key.value(i, cx)?)))
            .collect();
        rows.sort_by(|a, b| {
            if self.desc {
                b.1.total_cmp(&a.1)
            } else {
                a.1.total_cmp(&b.1)
            }
        });
        rows
    }

    /// The head of `pool` by this list's key: `count` markets off the front of
    /// [`ranked`](Self::ranked).
    ///
    /// `prev` are the markets the pool held last time, and they leave only
    /// once they fall past the `KEEP_BAND` band — see its note for why the
    /// boundary must not be a revolving door. `prev` empty is the plain
    /// top-`count`, which is what the black list and a first recompute want.
    fn head(&self, pool: &[u16], cx: &Ctx, prev: &HashSet<u16>) -> Vec<u16> {
        let mut rows = self.ranked(pool, cx);
        rows.truncate(self.count + self.count.div_ceil(KEEP_BAND));
        // Back down to `count`: the worst-ranked NEWCOMER leaves first, so a
        // market already in the pool keeps its slot while it is inside the
        // band; only once the band holds nothing but incumbents does the
        // worst-ranked of those leave.
        let mut keep = vec![true; rows.len()];
        let mut excess = rows.len().saturating_sub(self.count);
        for incumbent in [false, true] {
            for (i, row) in rows.iter().enumerate().rev() {
                if excess == 0 {
                    break;
                }
                if keep[i] && prev.contains(&row.0) == incumbent {
                    keep[i] = false;
                    excess -= 1;
                }
            }
        }
        rows.iter()
            .zip(keep)
            .filter_map(|(&(i, _), k)| k.then_some(i))
            .collect()
    }
}

/// What keeps this strategy's screener from producing a pool, if anything:
/// the text goes to the terminal's log once per change (`MoonShot::tick`), and
/// [`universe`] returns nothing while it holds. A misconfigured screener
/// trades nothing rather than trading the wrong markets.
pub fn problem(p: &Params) -> Option<String> {
    if p.white.is_empty() {
        match &p.market_tags {
            Err(tag) => {
                return Some(format!(
                    "MarketTags: unknown tag <{tag}> (known: {}), no markets",
                    crate::model::MarketTags::known()
                ))
            }
            Ok(tags) if tags.is_empty() => {
                return Some("empty white list and no MarketTags, no markets".to_string())
            }
            _ => {}
        }
        // A class names the whole catalog (2455 markets on 28.09), and every
        // one of them would be subscribed, sampled and judged every pass. The
        // volume bounds used to cut that down; they are entry filters now, so
        // what a class screener watches is exactly what `DynWL_Count` says —
        // and a screener that names no count names no pool.
        if !p.dyn_wl.on() {
            return Some(
                "MarketTags with DynWL_Count 0: the pool would be the whole class, no markets"
                    .to_string(),
            );
        }
    }
    for (list, name) in [(&p.dyn_wl, "DynWL_SortBy"), (&p.dyn_bl, "DynBL_SortBy")] {
        if let (true, Err(key)) = (list.on(), &list.by) {
            return Some(format!(
                "{name}: unknown sort key <{key}> (known: {}), no markets",
                SortKey::PICKLIST.replace('|', ", ")
            ));
        }
    }
    None
}

/// The markets this strategy is about: its white list, or the `MarketTags`
/// classes ranked by `DynWL_*` down to its count, black lists removed.
///
/// `prev` is the pool of the last recompute, which gives its members their
/// hold on the boundary (`DynList::head`). Every market that comes out is
/// watched — `MaxMarkets` caps the markets the strategy *stands on*, not the
/// ones it looks at (`MoonShot::tick`) — and whether it may be entered right
/// now is the filters' answer, not this one's. A screener [`problem`] trades
/// nothing.
pub fn universe(p: &Params, cx: &Ctx, prev: &HashSet<u16>) -> Vec<u16> {
    if problem(p).is_some() {
        return Vec::new();
    }
    dynamic(p, cx, candidates(p, cx), prev).pool
}

/// List entries that name no market of the catalog: a typo, or an instrument
/// that has left it since the strategy was written.
///
/// This is deliberately NOT a [`problem`]. A problem empties the pool, and a
/// strategy standing on four markets must not stop trading because a fifth
/// was misspelled — the four are still a pool and still its operator's
/// intent. It is only said out loud, because the silent version is the one
/// that costs a morning: a list that quietly holds nothing, a pool short of a
/// market, and no line anywhere saying which word was wrong.
///
/// Ask it with the strategy's OWN params, before the terminal's global black
/// list is folded in (`MoonShot::blacked` adds catalog spellings, which name
/// markets by construction, and the global list is not this strategy's to
/// answer for).
pub fn unknown_symbols(p: &Params, model: &Model) -> Option<String> {
    // A `fn`, not a closure: it hands back borrows of `list`, and a closure
    // cannot be written over two different ones. The two buckets are the two
    // answers an operator gets: a word that names nothing, and a word that
    // names two markets at once — which is not a typo and must not be called
    // one (`Model::symbol_is_ambiguous`).
    fn missing<'a>(list: &'a [String], model: &Model) -> (Vec<&'a str>, Vec<&'a str>) {
        let (mut absent, mut ambiguous) = (Vec::new(), Vec::new());
        for sym in list {
            let known = model.index_of_symbol_ci(sym).is_some();
            if known {
                continue;
            }
            let out = if model.symbol_is_ambiguous(sym) {
                &mut ambiguous
            } else {
                &mut absent
            };
            if !out.contains(&sym.as_str()) {
                out.push(sym);
            }
        }
        (absent, ambiguous)
    }
    let said: Vec<String> = [
        ("CoinsWhiteList", missing(&p.white, model)),
        ("CoinsBlackList", missing(&p.black, model)),
    ]
    .into_iter()
    .flat_map(|(field, (absent, ambiguous))| {
        [
            (!absent.is_empty()).then(|| {
                format!(
                    "{field}: no such market in the catalog: {}",
                    absent.join(", ")
                )
            }),
            (!ambiguous.is_empty()).then(|| {
                format!(
                    "{field}: two markets answer to this spelling, write one of them \
                     exactly: {}",
                    ambiguous.join(", ")
                )
            }),
        ]
    })
    .flatten()
    .collect();
    (!said.is_empty()).then(|| said.join("; "))
}

/// The black-listed markets as indexes: the strategy's own `CoinsBlackList`
/// plus whatever the caller folded into it from the terminal's global one
/// (`MoonShot::blacked`). Matched however the operator spelled it, like the
/// white list below — both are fields somebody typed by hand. An entry that
/// names no market of the catalog holds nothing, as it always did.
fn blocked(p: &Params, model: &Model) -> HashSet<u16> {
    p.black
        .iter()
        .filter_map(|sym| model.index_of_symbol_ci(sym))
        .collect()
}

/// The markets the strategy is about BEFORE the dynamic lists: its white list
/// in the order it was written, or every market of its `MarketTags` classes;
/// black-listed markets are out of both. [`universe`] ranks this down to the pool, and [`explain`] tells the
/// markets that never were candidates from the ones the ranking dropped.
fn candidates(p: &Params, cx: &Ctx) -> Vec<u16> {
    let model = cx.model;
    let black = blocked(p, model);
    let ok = |i: u16, _: &Market| !black.contains(&i);
    if !p.white.is_empty() {
        // A white list is its own bound and keeps its own order: the operator
        // wrote the markets down, and a dynamic list over them is optional.
        let mut seen = HashSet::new();
        p.white
            .iter()
            .filter_map(|sym| model.index_of_symbol_ci(sym))
            .filter(|&i| model.at(i).is_some_and(|m| ok(i, m)) && seen.insert(i))
            .collect()
    } else if let Ok(tags) = &p.market_tags {
        // Unranked and unordered: `dynamic` below both cuts it to size and
        // gives it its order, and `problem` has already refused a class pool
        // with no count to cut it by.
        model
            .iter()
            .filter(|&(i, m)| tags.matches(&m.tags) && ok(i, m))
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    }
}

/// What the dynamic lists made of a candidate list: the pool, and the two
/// sets it was made of.
///
/// The parts are carried out rather than thrown away because [`explain`] has
/// to say WHICH list dropped a market, and the two lists overlap — a market
/// can be ranked past `DynWL_Count` and be in the black list's head at the
/// same time. The order the pool is built in is the order the reason has to
/// be read in, and the only way to be sure of that is to read it off the same
/// computation.
struct Dynamic {
    pool: Vec<u16>,
    /// The white list's head, or `None` when the list is off — then every
    /// candidate is in it and nothing was ranked out.
    wl_head: Option<HashSet<u16>>,
    banned: HashSet<u16>,
}

/// The dynamic lists over the candidates: the white one keeps its head (and
/// hands its order on — the head of the sort is the strategy's first choice),
/// the black one is taken from the same candidates by its own key and
/// subtracted.
fn dynamic(p: &Params, cx: &Ctx, pool: Vec<u16>, prev: &HashSet<u16>) -> Dynamic {
    if !p.dyn_wl.on() && !p.dyn_bl.on() {
        return Dynamic {
            pool,
            wl_head: None,
            banned: HashSet::new(),
        };
    }
    let banned: HashSet<u16> = if p.dyn_bl.on() {
        // No hold on the boundary here: a market the black list lets go is one
        // the strategy may trade again at once, and keeping it banned past
        // what its key says would be a ban nobody asked for.
        p.dyn_bl
            .head(&pool, cx, &HashSet::new())
            .into_iter()
            .collect()
    } else {
        HashSet::new()
    };
    let (mut out, wl_head) = if p.dyn_wl.on() {
        let head = p.dyn_wl.head(&pool, cx, prev);
        let seen = head.iter().copied().collect();
        (head, Some(seen))
    } else {
        (pool, None)
    };
    out.retain(|i| !banned.contains(i));
    Dynamic {
        pool: out,
        wl_head,
        banned,
    }
}

/// Where one market of the catalog stands in a strategy's screener.
///
/// Only [`Seat::Pool`] is a market the strategy watches; the rest are the
/// reasons it does not, and they are kept apart on purpose. «Not in the pool»
/// covers a bond a shares strategy was never about and a market that lost its
/// place by one rank this minute, and the page that cannot tell them apart
/// tells the operator nothing about what to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seat {
    /// Watched: the strategy's pool this pass.
    Pool,
    /// The screener has a [`problem`] and produces no pool at all, so no
    /// market has a place in it — including the ones that otherwise would.
    NoPool,
    /// A black list holds it: the strategy's own `CoinsBlackList`, or the
    /// terminal's global one, which the caller folds in before asking.
    Black,
    /// The white list names markets, and not this one.
    NotListed,
    /// No white list, and its class is not in `MarketTags`.
    OtherClass,
    /// `DynBL_*` took it off the pool.
    DynBlack,
    /// A candidate ranked past `DynWL_Count`.
    Ranked,
    /// A candidate `DynWL_SortBy` does not measure — no opening price to
    /// measure a delta from, most often a market whose history the warm-up
    /// has not reached. It is a candidate for neither dynamic list, so it
    /// falls out of a ranked pool without ever being ranked.
    Unranked,
}

/// One market of the catalog with its seat and its place in the ranking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Seen {
    pub idx: u16,
    pub seat: Seat,
    /// Place in the `DynWL_SortBy` ranking of the candidates, 1 for the
    /// first; `None` when the list is off, its key is unknown, or the key
    /// does not measure this market. The rank runs over EVERY candidate, not
    /// only the pool: a market at 104 of a top-100 is the number the operator
    /// is looking for when deciding what `DynWL_Count` should be.
    pub rank: Option<usize>,
    /// The ranking key's own value, in its own unit (`SortKey::turnover`).
    pub key: Option<f64>,
}

/// Every market of the catalog with the seat this strategy's screener gives
/// it, for the page's strategy helper.
///
/// Membership and the reasons both come off ONE run of the same [`dynamic`]
/// the pool is built by, so what this says is watched and what the strategy
/// actually watches cannot drift apart, and the reason a market is out is
/// read in the order the pool was built — the white list's ranking first,
/// then the black list. The two overlap, and a market ranked past
/// `DynWL_Count` that the black list would also have taken was dropped by the
/// ranking: telling the operator to look at `DynBL_Count` would send them to
/// the wrong control. `prev` has the same meaning as in [`universe`] — pass
/// the strategy's live pool and the boundary's hold (`KEEP_BAND`) is the one
/// it is really running under.
pub fn explain(p: &Params, cx: &Ctx, prev: &HashSet<u16>) -> Vec<Seen> {
    let stuck = problem(p).is_some();
    let cand = if stuck { Vec::new() } else { candidates(p, cx) };
    // One run of the lists, and the seats below are read off it in the order
    // it built the pool. Asking the black list a second time would be a
    // second opinion, and the two lists overlap.
    let made = dynamic(p, cx, cand.clone(), prev);
    let pool: HashSet<u16> = made.pool.iter().copied().collect();
    // Only a list that is on has a ranking: with `DynWL_Count` 0 nothing is
    // ranked, and a number in that column would be a place in an order the
    // strategy is not keeping.
    let ranking: HashMap<u16, (usize, f64)> = if p.dyn_wl.on() {
        p.dyn_wl
            .ranked(&cand, cx)
            .into_iter()
            .enumerate()
            .map(|(i, (idx, v))| (idx, (i + 1, v)))
            .collect()
    } else {
        HashMap::new()
    };
    let in_cand: HashSet<u16> = cand.iter().copied().collect();
    let black = blocked(p, cx.model);
    cx.model
        .iter()
        .map(|(idx, _)| {
            let place = ranking.get(&idx).copied();
            let seat = if pool.contains(&idx) {
                Seat::Pool
            } else if stuck {
                Seat::NoPool
            } else if !in_cand.contains(&idx) {
                if black.contains(&idx) {
                    Seat::Black
                } else if p.white.is_empty() {
                    Seat::OtherClass
                } else {
                    Seat::NotListed
                }
            } else if made.wl_head.as_ref().is_some_and(|h| !h.contains(&idx)) {
                // The white list drops it BEFORE the black one is subtracted,
                // which is the order `dynamic` works in — a market that never
                // made the head is not the black list's doing, however much
                // the black list would also have taken it.
                if place.is_none() {
                    Seat::Unranked
                } else {
                    Seat::Ranked
                }
            } else if made.banned.contains(&idx) {
                Seat::DynBlack
            } else {
                // In the head and not banned, yet not in the pool: `dynamic`
                // builds the pool out of exactly those two, so this is
                // unreachable — `explain_seats_the_pool_universe_returned`
                // is what keeps it so.
                Seat::Ranked
            };
            Seen {
                idx,
                seat,
                rank: place.map(|(r, _)| r),
                // The key is a measurement and is worth printing whatever
                // seat the market got: it is what the operator moves
                // `DynWL_Count` against.
                key: place
                    .map(|(_, v)| v)
                    .or_else(|| p.dyn_wl.by.as_ref().ok().and_then(|k| k.value(idx, cx))),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The combo offers exactly the names the parser knows: an item the
    /// parser rejects would be a typo shipped by the schema itself.
    #[test]
    fn picklist_matches_the_names() {
        assert_eq!(
            SortKey::PICKLIST,
            SortKey::NAMES
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join("|")
        );
        for (name, key) in SortKey::NAMES {
            assert_eq!(SortKey::parse(name), Ok(key));
        }
        assert_eq!(SortKey::parse("last2hdelta"), Ok(SortKey::Last2h));
        assert_eq!(SortKey::parse(" HourlyVol "), Ok(SortKey::HourlyVol));
        assert_eq!(SortKey::parse("MarkPrice"), Err("MarkPrice".to_string()));
        assert_eq!(SortKey::parse("24h-Delta"), Err("24h-Delta".to_string()));
        // Turnover and percents are not the same log line.
        assert!(SortKey::DailyVol.turnover() && SortKey::HourlyVol.turnover());
        assert!(!SortKey::Last2h.turnover() && !SortKey::Pump5m.turnover());
    }
}
