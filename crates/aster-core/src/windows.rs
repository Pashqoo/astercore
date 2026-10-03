//! Per-market tick windows for the strategies: minute buckets (open, low,
//! high, USDT turnover) over the last 24 h, fed by the trades stream.
//! Ported from TInvestCore; turnover is the quote (USDT) of each trade.
//! Excursions and ranges over 15 m / 1 h / 3 h drive the MoonShot delta
//! shifts, the turnover sums drive the screener's volume filters, and the
//! opening price of a window drives its sort keys (`screener`): MoonBot counts
//! every delta from the price at the window's start.
//!
//! The screener's 24 h volume ([`Windows::vol24`]) is the same minute
//! buckets over the clock's last day, as MoonBot reads `DailyVol` (`faqru`:
//! «за 24ч»).

use std::collections::VecDeque;

const MINUTE_MS: i64 = 60_000;
const KEEP_MINUTES: i64 = 24 * 60;

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

#[derive(Default)]
pub struct Windows {
    by_idx: Vec<VecDeque<Bucket>>,
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
    }

    /// USDT turnover of the clock's last 24 hours — the screener's `Vol.`
    /// and what `MinVolume`/`MaxVolume` and `DailyVol` read. A market silent
    /// for a day reads zero, so `MinVolume` drops it.
    pub fn vol24(&self, idx: u16, now_ms: i64) -> f64 {
        self.turnover(idx, now_ms, KEEP_MINUTES)
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

    /// The volume the screener filters on is the clock's last 24 hours, as
    /// MoonBot's `DailyVol`: a night of silence costs the hours it lasted, and
    /// a market quiet for a day reads zero — no reaching days back for the
    /// hours it did trade, which made a dead market pass `MinVolume`.
    #[test]
    fn vol24_is_the_clocks_last_day() {
        const HOUR_MS: i64 = 60 * MINUTE_MS;
        let mut w = Windows::default();
        let t0 = 500_000 * HOUR_MS;
        // Three hours of trading, then 15 hours of silence, then two more.
        for h in 0..3 {
            w.push(1, t0 + h * HOUR_MS, 100.0, 10.0);
        }
        for h in 18..20 {
            w.push(1, t0 + h * HOUR_MS, 100.0, 20.0);
        }
        let now = t0 + 19 * HOUR_MS + 30 * MINUTE_MS;
        assert_eq!(w.vol24(1, now), 3.0 * 1000.0 + 2.0 * 2000.0);
        // The first three hours leave the day as the clock passes them (a
        // window takes the minute its start falls into whole, as every
        // window here does: `span`).
        assert_eq!(w.vol24(1, t0 + 26 * HOUR_MS + MINUTE_MS), 2.0 * 2000.0);
        assert_eq!(
            w.vol24(1, t0 + 44 * HOUR_MS),
            0.0,
            "a silent day reads zero"
        );
        assert_eq!(w.vol24(9, now), 0.0, "a market with no history");
    }

    /// Warm-up bars and live ticks fill the same day: the bars cover the
    /// history up to the start, the ticks everything since, and a bar from
    /// before the day does not count.
    #[test]
    fn vol24_adds_the_warm_up_bars_and_the_ticks() {
        const BAR_MS: i64 = 5 * MINUTE_MS;
        let mut w = Windows::default();
        let t0 = 800_000 * 60 * MINUTE_MS;
        let now = t0 + 30 * 60 * MINUTE_MS;
        w.push(4, now - MINUTE_MS, 100.0, 3.0);
        w.seed(4, now - 25 * 60 * MINUTE_MS, 1.0, 1.0, 1.0, 5000.0);
        w.seed(4, now - 2 * BAR_MS, 1.0, 1.0, 1.0, 1000.0);
        w.seed(4, now - 3 * BAR_MS, 1.0, 1.0, 1.0, 2000.0);
        assert_eq!(w.vol24(4, now), 3300.0);
        assert_eq!(w.vol24(4, now), w.turnover(4, now, KEEP_MINUTES));
    }
}
