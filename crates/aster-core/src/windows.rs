//! Per-market tick windows for the strategies: minute buckets (open, low,
//! high, USDT turnover) over the last 24 h, fed by the trades stream.
//! Ported from TInvestCore; turnover is the quote (USDT) of each trade.
//! Excursions and ranges over 15 m / 1 h / 3 h drive the MoonShot delta
//! shifts, the turnover sums drive the screener's volume filters, and the
//! opening price of a window drives its sort keys (`screener`): MoonBot counts
//! every delta from the price at the window's start.
//!
//! Beside them an hour ledger, reaching days back rather than one day: the
//! screener's 24 h volume ([`Windows::vol24`]) is the last 24 hours the market
//! actually traded, not the last 24 hours of the clock.

use std::collections::VecDeque;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
const KEEP_MINUTES: i64 = 24 * 60;
/// Traded hours [`Windows::vol24`] sums.
const VOL_HOURS: usize = 24;
/// Turnover an hour needs to count as one of them. An hour nobody traded in
/// takes no slot; an hour that turned over less than a USDT is not a traded
/// hour either. On a crypto perpetual, which trades round the clock, the 24
/// traded hours are the clock's 24; the rule matters for Aster's stock and
/// forex perpetuals (`Market::has_sessions`), whose quiet hours would empty a
/// clock-bound sum and take them off every strategy with a volume bound —
/// the reason TInvestCore, on MOEX, made it (28.09).
const HOUR_MIN_TURNOVER: f64 = 1.0;
/// How far back a traded hour may sit and still count, in hours. A market
/// silent for a week reads zero again, the way the old rolling day did, so
/// `MinVolume` still drops an instrument that stopped trading; long enough that
/// a long weekend never empties the window. The bound belongs to the reader
/// ([`Windows::vol24`], which is given the clock), not to the writers: a
/// timestamp from the far future must not be able to decide what counts as old.
const HOUR_KEEP: i64 = 5 * 24;
/// Hour buckets one market keeps. The window [`Windows::vol24`] reads is
/// inclusive at both ends, so it spans `HOUR_KEEP + 1` distinct hours and the
/// cap is that: it then drops only hours the reader has already stopped
/// counting. A bucket stamped in the future is not dropped by it — the cap
/// takes the oldest — it is simply never read while the clock is behind it.
const HOUR_CAP: usize = HOUR_KEEP as usize + 1;

#[derive(Debug, Clone, Copy)]
struct Bucket {
    minute: i64,
    /// First trade of the minute; 0 when the bucket came from a source that
    /// carries no open (TInvestCore's day row; Aster's warm-up bars all do).
    open: f64,
    lo: f64,
    hi: f64,
    turnover: f64,
}

/// One hour of USDT turnover. An hour the market did not trade in has no
/// bucket at all — that absence is what keeps the night and the weekend out of
/// [`Windows::vol24`].
#[derive(Debug, Clone, Copy)]
struct Hour {
    hour: i64,
    turnover: f64,
}

#[derive(Default)]
pub struct Windows {
    by_idx: Vec<VecDeque<Bucket>>,
    /// Hour buckets per market, oldest first, at most [`HOUR_CAP`] of them.
    hours: Vec<VecDeque<Hour>>,
}

impl Windows {
    /// One trade of `qty` at `price`: its turnover is `price × qty` USDT
    /// (linear USDT contracts).
    pub fn push(&mut self, idx: u16, time_ms: i64, price: f64, qty: f64) {
        if price <= 0.0 {
            return;
        }
        let i = usize::from(idx);
        if self.by_idx.len() <= i {
            self.by_idx.resize_with(i + 1, VecDeque::new);
        }
        let q = &mut self.by_idx[i];
        let minute = time_ms / MINUTE_MS;
        let turnover = price * qty.abs();
        match q.back_mut() {
            // Same or older minute (late tick): fold into the current bucket.
            Some(b) if b.minute >= minute => {
                b.lo = b.lo.min(price);
                b.hi = b.hi.max(price);
                b.turnover += turnover;
            }
            _ => q.push_back(Bucket {
                minute,
                open: price,
                lo: price,
                hi: price,
                turnover,
            }),
        }
        while q.front().is_some_and(|b| b.minute < minute - KEEP_MINUTES) {
            q.pop_front();
        }
        self.hour_mut(idx, time_ms).turnover += turnover;
    }

    /// The hour bucket `time_ms` falls into, inserted in order when it is new.
    /// Nothing is rejected here and nothing is aged out: how old an hour may be
    /// is [`Windows::vol24`]'s call, which holds the clock. This only keeps the
    /// deque bounded, dropping the oldest bucket past [`HOUR_CAP`].
    fn hour_mut(&mut self, idx: u16, time_ms: i64) -> &mut Hour {
        let i = usize::from(idx);
        if self.hours.len() <= i {
            self.hours.resize_with(i + 1, VecDeque::new);
        }
        let q = &mut self.hours[i];
        let hour = time_ms.div_euclid(HOUR_MS);
        let mut pos = match q.binary_search_by_key(&hour, |b| b.hour) {
            Ok(pos) => return &mut q[pos],
            Err(pos) => pos,
        };
        if q.len() >= HOUR_CAP && q.pop_front().is_some() {
            pos = pos.saturating_sub(1);
        }
        q.insert(
            pos,
            Hour {
                hour,
                turnover: 0.0,
            },
        );
        &mut q[pos]
    }

    /// Historical turnover (USDT) of the hour `time_ms` falls in — a warm-up
    /// bar — added to whatever that hour already holds. The engine seeds only
    /// bars that ended before the core started (`seed_windows`), so they and
    /// the live ticks cover different slices of an hour and the sum is the
    /// hour.
    pub fn seed_hour(&mut self, idx: u16, time_ms: i64, turnover: f64) {
        if turnover <= 0.0 {
            return;
        }
        self.hour_mut(idx, time_ms).turnover += turnover;
    }

    /// USDT turnover of the last [`VOL_HOURS`] hours the market traded in —
    /// the screener's `Vol.` and what `MinVolume`/`MaxVolume` and `DailyVol`
    /// read. Hours without trades take no slot, so the sum survives a session
    /// market's night and weekend; hours further back than [`HOUR_KEEP`]
    /// are not counted, so a market that stopped trading still falls to zero.
    ///
    /// Both ends are cut against the clock this is given, which is the only
    /// thing that knows the time: an hour still ahead of it — a trade or a bar
    /// with a timestamp from the future — is not a traded hour yet, and would
    /// otherwise stay in every sum for good, since nothing ages it out.
    pub fn vol24(&self, idx: u16, now_ms: i64) -> f64 {
        let current = now_ms.div_euclid(HOUR_MS);
        let oldest = current - HOUR_KEEP;
        self.hours
            .get(usize::from(idx))
            .into_iter()
            .flat_map(|q| q.iter().rev())
            .skip_while(|h| h.hour > current)
            .take_while(|h| h.hour >= oldest)
            .filter(|h| h.turnover > HOUR_MIN_TURNOVER)
            .take(VOL_HOURS)
            .map(|h| h.turnover)
            .sum()
    }

    /// A historical minute (warm-up bar): merged into the bucket of that
    /// minute or inserted in order, so it may arrive after live ticks. `open`
    /// is the bar's first trade and wins over one taken from a live tick — a
    /// bar covers the whole minute, a tick only the moment it arrived; 0 means
    /// the source has none (a day row) and leaves the bucket's own open alone.
    pub fn seed(&mut self, idx: u16, time_ms: i64, open: f64, lo: f64, hi: f64, turnover: f64) {
        if lo <= 0.0 || hi < lo {
            return;
        }
        let i = usize::from(idx);
        if self.by_idx.len() <= i {
            self.by_idx.resize_with(i + 1, VecDeque::new);
        }
        let q = &mut self.by_idx[i];
        let minute = time_ms / MINUTE_MS;
        if q.back().is_some_and(|b| minute < b.minute - KEEP_MINUTES) {
            return;
        }
        match q.binary_search_by_key(&minute, |b| b.minute) {
            Ok(pos) => {
                let b = &mut q[pos];
                if open > 0.0 {
                    b.open = open;
                }
                b.lo = b.lo.min(lo);
                b.hi = b.hi.max(hi);
                b.turnover += turnover;
            }
            Err(pos) => q.insert(
                pos,
                Bucket {
                    minute,
                    open,
                    lo,
                    hi,
                    turnover,
                },
            ),
        }
    }

    /// The buckets of the last `minutes`, newest first. Minute granularity:
    /// the bucket the window's start falls into is taken whole, so an
    /// `N`-minute window covers `N` to `N + 1` minutes of trading. Every
    /// metric here shares that rule — the turnover the screener filters on,
    /// the excursions and ranges MoonShot's delta shifts were reconstructed
    /// from (M3, against the MoonBot logs), and the sort keys.
    fn span(&self, idx: u16, now_ms: i64, minutes: i64) -> impl Iterator<Item = &Bucket> {
        let from = now_ms / MINUTE_MS - minutes;
        self.by_idx
            .get(usize::from(idx))
            .into_iter()
            .flat_map(move |q| q.iter().rev().take_while(move |b| b.minute >= from))
    }

    /// The market's history reaches back to the start of the last `minutes`
    /// (within a twentieth of it): a delta over them is the window's, not
    /// that of the few minutes since a start.
    pub fn covers(&self, idx: u16, now_ms: i64, minutes: i64) -> bool {
        let from = now_ms / MINUTE_MS - minutes + (minutes / 20).max(1);
        self.by_idx
            .get(usize::from(idx))
            .and_then(|q| q.front())
            .is_some_and(|b| b.minute <= from)
    }

    /// Low and high of the last `minutes`; `None` without ticks.
    pub fn extremes(&self, idx: u16, now_ms: i64, minutes: i64) -> Option<(f64, f64)> {
        self.span(idx, now_ms, minutes)
            .fold(None, |acc: Option<(f64, f64)>, b| match acc {
                Some((lo, hi)) => Some((lo.min(b.lo), hi.max(b.hi))),
                None => Some((b.lo, b.hi)),
            })
    }

    /// Excursion of `last` from the window's extremes, %: the larger of the
    /// climb from the low and the drop from the high (TMB `exc`), ≥ 0.
    pub fn excursion(&self, idx: u16, last: f64, now_ms: i64, minutes: i64) -> f64 {
        match self.extremes(idx, now_ms, minutes) {
            Some((lo, hi)) if last > 0.0 && lo > 0.0 => {
                (last / lo - 1.0).max(hi / last - 1.0).max(0.0) * 100.0
            }
            _ => 0.0,
        }
    }

    /// Window range widened by `last`, % of its low (TMB `rng15`), ≥ 0.
    pub fn range(&self, idx: u16, last: f64, now_ms: i64, minutes: i64) -> f64 {
        match self.extremes(idx, now_ms, minutes) {
            Some((lo, hi)) if last > 0.0 => {
                let (lo, hi) = (lo.min(last), hi.max(last));
                if lo > 0.0 {
                    (hi - lo) / lo * 100.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    /// USDT turnover over the last `minutes`.
    pub fn turnover(&self, idx: u16, now_ms: i64, minutes: i64) -> f64 {
        self.span(idx, now_ms, minutes).map(|b| b.turnover).sum()
    }

    /// `(price at the start of the window, low, high)` over the last
    /// `minutes`. The opening price is the open of the oldest bucket that
    /// carries one — a bucket seeded from a day row has none and is skipped,
    /// so the window opens at the oldest minute actually traded. `None`
    /// without ticks or when no bucket in the window carries an open, which is
    /// "not measured", not "flat": the callers keep the two apart.
    ///
    /// The reference point slides with the window, so a market that stopped
    /// trading keeps moving deltas without a single trade — the window's start
    /// is a moment in time, not the market's last move. It ends where it
    /// should: `minutes` of silence empty the span and the metrics go to
    /// `None`.
    fn shape(&self, idx: u16, now_ms: i64, minutes: i64) -> Option<(f64, f64, f64)> {
        let mut open = 0.0;
        let mut range: Option<(f64, f64)> = None;
        for b in self.span(idx, now_ms, minutes) {
            range = Some(match range {
                Some((lo, hi)) => (lo.min(b.lo), hi.max(b.hi)),
                None => (b.lo, b.hi),
            });
            // Oldest first wins: the span runs from the newest bucket back.
            if b.open > 0.0 {
                open = b.open;
            }
        }
        let (lo, hi) = range?;
        (open > 0.0).then_some((open, lo, hi))
    }

    /// Move of `last` from the window's opening price, % of it and signed
    /// (MoonBot's `Last1mDelta` … `Last3hDelta`). `None` = nothing to measure
    /// from (no ticks in the window, or no price yet): a market whose move is
    /// unknown must not sort as one that stood still.
    pub fn delta(&self, idx: u16, last: f64, now_ms: i64, minutes: i64) -> Option<f64> {
        match self.shape(idx, now_ms, minutes) {
            Some((open, _, _)) if last > 0.0 => Some((last - open) / open * 100.0),
            _ => None,
        }
    }

    /// Climb from the window's opening price to its high, % of the opening
    /// price (MoonBot's `Pump5m` / `Pump1h`), ≥ 0; `None` as for [`Self::delta`].
    pub fn pump(&self, idx: u16, now_ms: i64, minutes: i64) -> Option<f64> {
        let (open, _, hi) = self.shape(idx, now_ms, minutes)?;
        Some(((hi - open) / open * 100.0).max(0.0))
    }

    /// Fall from the window's opening price to its low, % of the opening
    /// price (MoonBot's `Dump1h`), ≥ 0; `None` as for [`Self::delta`].
    pub fn dump(&self, idx: u16, now_ms: i64, minutes: i64) -> Option<f64> {
        let (open, lo, _) = self.shape(idx, now_ms, minutes)?;
        Some(((open - lo) / open * 100.0).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_windows_and_metrics() {
        let mut w = Windows::default();
        let t0 = 1_000_000 * MINUTE_MS;
        w.push(3, t0, 100.0, 10.0);
        w.push(3, t0 + 30_000, 90.0, 10.0);
        w.push(3, t0 + 20 * MINUTE_MS, 110.0, 5.0);
        w.push(3, t0 + 19 * MINUTE_MS, 120.0, 1.0); // late tick folds into minute 20
        let now = t0 + 21 * MINUTE_MS;
        assert_eq!(w.extremes(3, now, 15), Some((110.0, 120.0)));
        assert_eq!(w.extremes(3, now, 60), Some((90.0, 120.0)));
        assert_eq!(w.extremes(4, now, 60), None);
        // From the 1h low of 90 a last of 99 is +10%; from the high of 120 it is +21.2%.
        let exc = w.excursion(3, 99.0, now, 60);
        assert!((exc - (120.0 / 99.0 - 1.0) * 100.0).abs() < 1e-9);
        assert!((w.excursion(3, 115.0, now, 15) - (115.0 / 110.0 - 1.0) * 100.0).abs() < 1e-9);
        assert!((w.range(3, 100.0, now, 15) - 20.0).abs() < 1e-9);
        assert_eq!(w.turnover(3, now, 15), 110.0 * 5.0 + 120.0);
        assert_eq!(w.turnover(3, now, 60), 1000.0 + 900.0 + 550.0 + 120.0);
        assert_eq!(w.excursion(9, 1.0, now, 15), 0.0);
        // Old buckets fall off after 24 h.
        w.push(3, t0 + (KEEP_MINUTES + 25) * MINUTE_MS, 50.0, 1.0);
        assert_eq!(w.by_idx[3].len(), 1);
    }

    #[test]
    fn seeds_history_in_order() {
        let mut w = Windows::default();
        let t0 = 2_000_000 * MINUTE_MS;
        w.push(1, t0 + 10 * MINUTE_MS, 100.0, 1.0);
        w.seed(1, t0 + 5 * MINUTE_MS, 96.0, 95.0, 105.0, 500.0); // older than live: inserted before
        w.seed(1, t0 + 10 * MINUTE_MS, 100.5, 99.0, 101.0, 50.0); // same minute: merged
        w.seed(1, t0 + 7 * MINUTE_MS, 1.0, 0.0, 1.0, 1.0); // no price: ignored
        w.seed(1, t0 - 2 * KEEP_MINUTES * MINUTE_MS, 1.0, 1.0, 2.0, 1.0); // too old: ignored
        let minutes: Vec<i64> = w.by_idx[1]
            .iter()
            .map(|b| b.minute - t0 / MINUTE_MS)
            .collect();
        assert_eq!(minutes, [5, 10]);
        let now = t0 + 11 * MINUTE_MS;
        assert_eq!(w.extremes(1, now, 15), Some((95.0, 105.0)));
        assert_eq!(w.turnover(1, now, 15), 500.0 + 100.0 + 50.0);
        // The window opens at the oldest bar's open, and a bar's open wins
        // over the live tick of the same minute.
        assert_eq!(w.shape(1, now, 15), Some((96.0, 95.0, 105.0)));
        assert_eq!(w.shape(1, now, 1), Some((100.5, 99.0, 101.0)));
    }

    /// The sort keys of the screener: every one of them is measured from the
    /// price the window opened at.
    #[test]
    fn deltas_pump_and_dump_run_from_the_window_open() {
        let mut w = Windows::default();
        let t0 = 4_000_000 * MINUTE_MS;
        let now = t0 + 10 * MINUTE_MS;
        w.push(1, t0, 100.0, 1.0);
        w.push(1, t0 + 2 * MINUTE_MS, 120.0, 1.0);
        w.push(1, t0 + 4 * MINUTE_MS, 80.0, 1.0);
        w.push(1, t0 + 9 * MINUTE_MS, 110.0, 1.0);
        // Opening price 100: last 110 is +10%, the high 120 is a 20% pump,
        // the low 80 a 20% dump.
        assert!((w.delta(1, 110.0, now, 15).unwrap() - 10.0).abs() < 1e-9);
        assert!(
            (w.delta(1, 90.0, now, 15).unwrap() + 10.0).abs() < 1e-9,
            "signed"
        );
        assert!((w.pump(1, now, 15).unwrap() - 20.0).abs() < 1e-9);
        assert!((w.dump(1, now, 15).unwrap() - 20.0).abs() < 1e-9);
        // A shorter window opens later: from minute 9 there is neither pump
        // nor dump, and the delta is measured from 110.
        assert_eq!(
            (w.pump(1, now, 1), w.dump(1, now, 1)),
            (Some(0.0), Some(0.0))
        );
        assert!((w.delta(1, 121.0, now, 1).unwrap() - 10.0).abs() < 1e-9);
        // A day row carries no open: it never becomes the window's start.
        let mut w = Windows::default();
        w.seed(2, t0, 0.0, 50.0, 200.0, 1_000.0);
        w.push(2, t0 + 5 * MINUTE_MS, 100.0, 1.0);
        assert_eq!(w.shape(2, now, 15).map(|s| s.0), Some(100.0));
        assert!((w.delta(2, 101.0, now, 15).unwrap() - 1.0).abs() < 1e-9);
        // Nothing but a day row: no opening price, so nothing measured —
        // `None`, which the screener keeps apart from a market that stood
        // still.
        let mut w = Windows::default();
        w.seed(3, t0, 0.0, 50.0, 200.0, 1_000.0);
        assert_eq!(w.shape(3, now, 15), None);
        assert_eq!(
            (
                w.delta(3, 100.0, now, 15),
                w.pump(3, now, 15),
                w.dump(3, now, 15)
            ),
            (None, None, None)
        );
        // No ticks at all: the same, never a division by zero.
        assert_eq!(w.delta(9, 100.0, now, 15), None);
        // A market that stopped trading: the window's start slides on, so the
        // delta keeps changing without a single trade, and once the span has
        // run past the last bucket there is nothing left to measure.
        let mut stale = Windows::default();
        stale.push(1, t0, 100.0, 1.0);
        stale.push(1, t0 + 5 * MINUTE_MS, 130.0, 1.0);
        let last = 130.0;
        assert!((stale.delta(1, last, t0 + 6 * MINUTE_MS, 15).unwrap() - 30.0).abs() < 1e-9);
        assert!(
            (stale.delta(1, last, t0 + 17 * MINUTE_MS, 15).unwrap() - 0.0).abs() < 1e-9,
            "minute 0 left the window, minute 5 opens it now"
        );
        assert_eq!(
            stale.delta(1, last, t0 + 21 * MINUTE_MS, 15),
            None,
            "the span ran past the last trade"
        );
    }

    /// The volume the screener filters on: the last 24 hours the market traded
    /// in. The night between them holds no bucket, so it takes no slot — the
    /// number no longer empties at Moscow midnight or over a weekend.
    #[test]
    fn vol24_sums_the_traded_hours_and_skips_the_silent_ones() {
        let mut w = Windows::default();
        let t0 = 500_000 * HOUR_MS;
        // Three hours of a session, then 15 hours of night, then two more.
        for h in 0..3 {
            w.push(1, t0 + h * HOUR_MS, 100.0, 10.0);
        }
        for h in 18..20 {
            w.push(1, t0 + h * HOUR_MS, 100.0, 20.0);
        }
        let now = t0 + 19 * HOUR_MS + 30 * MINUTE_MS;
        assert_eq!(w.vol24(1, now), 3.0 * 1000.0 + 2.0 * 2000.0);
        // A day on, with the market silent since: the clock's own last 24 h
        // hold nothing at all, and the traded hours still hold the session.
        assert_eq!(w.turnover(1, t0 + 44 * HOUR_MS, KEEP_MINUTES), 0.0);
        assert_eq!(w.vol24(1, t0 + 44 * HOUR_MS), 7000.0);
        assert_eq!(w.vol24(9, now), 0.0, "a market with no history");
    }

    /// Only the newest 24 traded hours count, and an hour with a rouble of
    /// turnover is not a traded hour.
    #[test]
    fn vol24_takes_twenty_four_hours_over_the_dust() {
        let mut w = Windows::default();
        let t0 = 600_000 * HOUR_MS;
        // 30 hours in a row, 1000 turnover each.
        for h in 0..30 {
            w.push(2, t0 + h * HOUR_MS, 10.0, 100.0);
        }
        let now = t0 + 30 * HOUR_MS;
        assert_eq!(w.vol24(2, now), 24.0 * 1000.0, "the oldest six fall out");
        let mut w = Windows::default();
        w.push(2, t0, 0.5, 1.0); // half a rouble: not a traded hour
        w.push(2, t0 + HOUR_MS, 100.0, 10.0);
        assert_eq!(w.vol24(2, now), 1000.0);
    }

    /// A market that stopped trading falls back to zero, so `MinVolume` drops
    /// it the way the old rolling day did. The age is the reader's call: a
    /// timestamp from the far future adds its own bucket and nothing else — it
    /// cannot make the hours around it look old, which would blind the market
    /// for as long as it sat there.
    #[test]
    fn vol24_forgets_hours_past_the_keep_window() {
        let mut w = Windows::default();
        let t0 = 700_000 * HOUR_MS;
        w.push(3, t0, 100.0, 10.0);
        assert_eq!(w.vol24(3, t0 + HOUR_KEEP * HOUR_MS), 1000.0);
        assert_eq!(w.vol24(3, t0 + (HOUR_KEEP + 1) * HOUR_MS), 0.0);
        // A tick stamped a year ahead: the hour it opens is read while the
        // clock is there, and the real hour beside it is read as before.
        let bogus = t0 + 365 * 24 * HOUR_MS;
        w.push(3, bogus, 100.0, 1.0);
        assert_eq!(
            w.vol24(3, t0 + HOUR_MS),
            1000.0,
            "the real hour still reads"
        );
        w.push(3, t0 + HOUR_MS, 100.0, 2.0);
        assert_eq!(
            w.vol24(3, t0 + 2 * HOUR_MS),
            1200.0,
            "and still takes ticks"
        );
        assert_eq!(w.vol24(3, bogus), 100.0, "the bogus hour holds only itself");
        // The deque stays bounded whatever arrives.
        let mut w = Windows::default();
        for h in 0..(HOUR_CAP as i64 + 10) {
            w.push(3, t0 + h * HOUR_MS, 100.0, 1.0);
        }
        assert_eq!(w.hours[3].len(), HOUR_CAP);
    }

    /// The hour deque stays sorted whatever order the hours arrive in, at the
    /// cap as well as below it: every later trade finds its bucket by binary
    /// search, so a bucket out of order would go on collecting turnover of its
    /// own under a neighbour's hour, and nothing would say so.
    #[test]
    fn hour_buckets_stay_in_order_at_the_cap() {
        let mut w = Windows::default();
        let t0 = 900_000 * HOUR_MS;
        // Fill to the cap with every second hour, so there is room between them.
        for h in 0..HOUR_CAP as i64 {
            w.seed_hour(5, t0 + 2 * h * HOUR_MS, 100.0);
        }
        assert_eq!(w.hours[5].len(), HOUR_CAP);
        // An hour between two buckets, one older than the front, one newer than
        // the back — each of them insert past the front the cap has to drop.
        w.seed_hour(5, t0 + 101 * HOUR_MS, 7.0);
        w.seed_hour(5, t0 - 5 * HOUR_MS, 8.0);
        w.seed_hour(5, t0 + 500 * HOUR_MS, 9.0);
        let hours: Vec<i64> = w.hours[5].iter().map(|h| h.hour).collect();
        assert_eq!(w.hours[5].len(), HOUR_CAP);
        assert!(hours.windows(2).all(|p| p[0] < p[1]), "sorted and distinct");
        // And the search still lands on the bucket it was given, not beside it.
        w.seed_hour(5, t0 + 101 * HOUR_MS, 3.0);
        let mid = w.hours[5]
            .iter()
            .find(|h| h.hour == (t0 + 101 * HOUR_MS) / HOUR_MS)
            .expect("the hour is still there");
        assert_eq!(mid.turnover, 10.0);
    }

    /// The warm-up's hourly bars carry the history, the live ticks carry the
    /// minutes since the start, and the running hour is the two of them
    /// together: the ISS bar ends ~15 min back, the ticks begin seconds ago.
    #[test]
    fn hourly_bars_seed_the_history_the_live_ticks_do_not_cover() {
        let mut w = Windows::default();
        let t0 = 800_000 * HOUR_MS;
        let now = t0 + 2 * HOUR_MS + 40 * MINUTE_MS;
        // Ticks of the running hour since the start, 300 turnover of them.
        w.push(4, t0 + 2 * HOUR_MS + 30 * MINUTE_MS, 100.0, 3.0);
        // The warm-up lands: two closed hours, and the part of the running one
        // that had already happened before the core opened its stream.
        w.seed_hour(4, t0, 1000.0);
        w.seed_hour(4, t0 + HOUR_MS, 2000.0);
        w.seed_hour(4, t0 + 2 * HOUR_MS, 500.0);
        assert_eq!(w.vol24(4, now), 3800.0);
        // Nothing to add, nothing to insert.
        w.seed_hour(4, t0 + 3 * HOUR_MS, 0.0);
        assert_eq!(w.hours[4].len(), 3);
        // The hour ledger is the minute buckets' own business: seeded hours
        // leave the minute windows (and the delta shifts) untouched.
        assert_eq!(w.turnover(4, now, 60), 300.0);
    }
}
