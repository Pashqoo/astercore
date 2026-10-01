//! MoonHook detector (MoonBot FAQ «Специфические параметры стратегии MoonHook»).
//! Inside `HookTimeFrame` the tape holds the move the price made — from where
//! it started (the frame's high before the low, or with `HookAntiPump` the
//! mean price before it) down to its extreme — and how far the price has come
//! back since. Turnover traded over the frame (`HookDetectMinVolume`) and the
//! fall of the two minutes before the move (`HookDropMin/Max`) qualify it.
//! A short's move is the mirror: a rise to its high, rolling back down.
//! Prices come from exchange trades, so `held_beyond` answers the
//! `HookRollBackWait` question at trade resolution while the detect itself is
//! taken on the strategy pass.

use std::collections::{HashMap, VecDeque};

/// FAQ: MoonHook recomputes its detect conditions once in 0.5 s; one bucket
/// per slot holds what the trades of that half-second did.
pub const BUCKET_MS: i64 = 500;
/// `HookDropMin/Max` measure the fall of the two minutes before the move.
pub const DROP_WINDOW_MS: i64 = 120_000;
/// FAQ: `HookTimeFrame` is meant for 2 s and plays up to 40 s; a longer one
/// is cut to this.
pub const MAX_FRAME_MS: i64 = 40_000;
/// Raw trades kept for `HookRollBackWait`. A whole frame's worth: the FAQ
/// asks that the move, its rollback and the wait all fit inside
/// `HookTimeFrame`, so a wait this buffer cannot represent could not have
/// fired in MoonBot either.
const HOLD_MS: i64 = MAX_FRAME_MS;

/// The trades of one 0.5 s slot.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    slot: i64,
    high: f64,
    low: f64,
    /// First trade of the slot. The slot keeps no order of the trades
    /// inside it, and this is the one price in it known to have printed
    /// before all the others — `detect` reads the move's start from it.
    open: f64,
    sum: f64,
    n: u32,
    turnover: f64,
}

#[derive(Default)]
struct Track {
    /// Since when the market is tracked: a detect needs a full frame of it.
    since: Option<i64>,
    buckets: VecDeque<Bucket>,
    /// `(at, price)` of the recent trades, for `held_beyond`.
    trades: VecDeque<(i64, f64)>,
    last: f64,
}

impl Track {
    fn trim(&mut self, now: i64) {
        let oldest = (now - DROP_WINDOW_MS - MAX_FRAME_MS).div_euclid(BUCKET_MS);
        while self.buckets.front().is_some_and(|b| b.slot < oldest) {
            self.buckets.pop_front();
        }
        while self
            .trades
            .front()
            .is_some_and(|&(at, _)| at < now - HOLD_MS)
        {
            self.trades.pop_front();
        }
    }
}

/// The move one frame holds and how far the price came back from it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Detect {
    /// Price the move started from (with `HookAntiPump`, the mean before it).
    pub reference: f64,
    /// The move's extreme: its low for a fall, its high for a rise.
    pub extreme: f64,
    /// `|reference − extreme| / min(reference, extreme) · 100` — MoonBot reads
    /// the move off its lower price.
    pub depth: f64,
    /// How far the price came back from the extreme, % of the move.
    pub rollback: f64,
    /// Turnover traded over the frame.
    pub turnover: f64,
    /// The fall before the move over `DROP_WINDOW_MS`, in % of `depth`
    /// (MoonBot's `HookDrop`: a 20 % fall before a 10 % strike = 200 %).
    pub drop_ratio: f64,
}

impl Detect {
    /// The price `pct` % of the move away from its extreme: 0 = the extreme,
    /// 100 = where the move started. `HookPriceRollBack` and
    /// `HookInitialPrice` are read off this line; `HookSellLevel` is not —
    /// it measures from the entry (`MoonShot::Hook::planned`).
    pub fn level(&self, pct: f64) -> f64 {
        self.extreme + (self.reference - self.extreme) * pct / 100.0
    }
}

#[derive(Default)]
pub struct Tracks {
    by_idx: HashMap<u16, Track>,
}

impl Tracks {
    /// Keeps the market's tape alive on the pass; trades fill it.
    pub fn sample(&mut self, idx: u16, now: i64) {
        let t = self.by_idx.entry(idx).or_default();
        t.since.get_or_insert(now);
        t.trim(now);
    }

    pub fn contains(&self, idx: u16) -> bool {
        self.by_idx.contains_key(&idx)
    }

    /// A market that left the universe or stopped trading normally: a move
    /// across the gap is not a move.
    pub fn reset(&mut self, idx: u16) {
        self.by_idx.remove(&idx);
    }

    pub fn retain(&mut self, keep: impl Fn(u16) -> bool) {
        self.by_idx.retain(|&idx, _| keep(idx));
    }

    /// An exchange trade (USDT of turnover) of a tracked market.
    pub fn trade(&mut self, idx: u16, now: i64, price: f64, turnover: f64) {
        let Some(t) = self.by_idx.get_mut(&idx) else {
            return;
        };
        if price <= 0.0 {
            return;
        }
        t.trim(now);
        let slot = now.div_euclid(BUCKET_MS);
        match t.buckets.back_mut() {
            // A trade reported late, behind the newest slot, joins nothing:
            // the frame it belongs to has been judged already.
            Some(b) if b.slot > slot => return,
            Some(b) if b.slot == slot => {
                b.high = b.high.max(price);
                b.low = b.low.min(price);
                b.sum += price;
                b.n += 1;
                b.turnover += turnover;
            }
            _ => t.buckets.push_back(Bucket {
                slot,
                high: price,
                low: price,
                open: price,
                sum: price,
                n: 1,
                turnover,
            }),
        }
        t.trades.push_back((now, price));
        t.last = price;
    }

    /// The move of the last `frame_ms` on `short`'s side (a fall for a long
    /// detect, a rise for a short one). `None` until the market has been
    /// tracked a whole frame or while the frame holds no trade.
    pub fn detect(
        &self,
        idx: u16,
        now: i64,
        frame_ms: i64,
        anti_pump: bool,
        short: bool,
    ) -> Option<Detect> {
        let t = self.by_idx.get(&idx)?;
        let frame = frame_ms.clamp(BUCKET_MS, MAX_FRAME_MS);
        if now - t.since? < frame {
            return None;
        }
        let now_slot = now.div_euclid(BUCKET_MS);
        let from = now_slot - frame / BUCKET_MS + 1;
        let win: Vec<&Bucket> = t
            .buckets
            .iter()
            .filter(|b| b.slot >= from && b.slot <= now_slot)
            .collect();
        if win.is_empty() {
            return None;
        }
        // The move's end: the latest extreme of the frame, so a price that
        // set the same low twice rolls back from the second one.
        let side = |b: &Bucket| if short { b.high } else { b.low };
        let better = |a: f64, b: f64| if short { a >= b } else { a <= b };
        let mut k = 0;
        for (i, b) in win.iter().enumerate() {
            if better(side(b), side(win[k])) {
                k = i;
            }
        }
        let extreme = side(win[k]);
        let head = &win[..=k];
        // Where it started: the frame's own extreme up to that point, or the
        // mean price before it — `HookAntiPump`, which keeps a strike that
        // only undid a fast rise from counting as a deep one. The mean runs
        // over the whole head, the extreme's own slot included, and so it
        // keeps prints whose order against the extreme is as unknown as the
        // ones the guard below drops: on a spike inside one slot it still
        // returns a depth where the plain reading returns zero. It stays
        // that way because MoonBot's own number says so — the 2.589 of the
        // MAGEP replay (`moonshot::hook_replays_the_magep_spike_of_21_09`)
        // is the mean of the whole head, and taking the slot out moves it.
        // `HookAntiPump` off is the reading to trust on a thin tape.
        let reference = if anti_pump {
            let (sum, n) = head
                .iter()
                .fold((0.0, 0u32), |(s, n), b| (s + b.sum, n + b.n));
            if n == 0 {
                return None;
            }
            sum / f64::from(n)
        } else {
            // The price the move started from printed BEFORE its extreme.
            // Every slot ahead of the extreme's is wholly before it and
            // offers its own high (a short's low); the extreme's own slot
            // keeps no order of its trades, and the only price in it known
            // to precede the extreme is where it opened — its high may as
            // well have printed after. A frame whose single slot is the
            // extreme's then leaves the reference on that open, and on the
            // extreme itself when the slot opened there: a move of zero,
            // which no `HookDetectDepth` passes. That is what keeps a spike
            // printed inside one slot from reading as its own mirror
            // (KCHEP 23.09 10:25:50: a 2.54 % rise taken as a fall).
            win[..k]
                .iter()
                .map(|b| if short { b.low } else { b.high })
                .chain(std::iter::once(win[k].open))
                .fold(extreme, |a, b| if better(a, b) { b } else { a })
        };
        if reference <= 0.0 || extreme <= 0.0 {
            return None;
        }
        // Measured off the lower of the two prices, as every MoonBot delta is
        // (`(high / low − 1)·100`): a fall of 110 → 100 is its 10 %, not 9.09 %.
        // A rise (a short's move) already has the reference below the extreme,
        // so only a fall changes base here.
        let depth = (reference - extreme).abs() / reference.min(extreme) * 100.0;
        // `span` is signed, so the same ratio reads the rollback of a fall
        // and of a rise.
        let span = reference - extreme;
        let rollback = if span == 0.0 || t.last <= 0.0 {
            0.0
        } else {
            ((t.last - extreme) / span * 100.0).max(0.0)
        };
        let drop_ratio = self.prior_drop(t, now_slot, win[k].slot, reference, depth, short);
        Some(Detect {
            reference,
            extreme,
            depth,
            rollback,
            turnover: win.iter().map(|b| b.turnover).sum(),
            drop_ratio,
        })
    }

    /// `HookDrop`: how far the price had already moved the same way over the
    /// two minutes up to the move, in % of the move's own depth.
    fn prior_drop(
        &self,
        t: &Track,
        now_slot: i64,
        // The slot the extreme sits in. The window ends before it, the way
        // `reference` stops there: its own prints may belong to the rollback
        // rather than to the trend the move came out of. That slot's open is
        // already in `reference`, which is what the fold starts from.
        head_slot: i64,
        reference: f64,
        depth: f64,
        short: bool,
    ) -> f64 {
        if depth <= 0.0 {
            return 0.0;
        }
        let from = now_slot - DROP_WINDOW_MS / BUCKET_MS + 1;
        let bound = t
            .buckets
            .iter()
            .filter(|b| b.slot >= from && b.slot < head_slot)
            .map(|b| if short { b.low } else { b.high })
            .fold(reference, |a, b| if short { a.min(b) } else { a.max(b) });
        if bound <= 0.0 {
            return 0.0;
        }
        // Same rule as `depth` above — divide by the lower of the two prices,
        // which here is `reference` and there `extreme` — or the two would not
        // divide: MoonBot's `HookDrop` is one delta over another.
        let prior =
            (bound - reference) / bound.min(reference) * 100.0 * if short { -1.0 } else { 1.0 };
        (prior / depth * 100.0).max(0.0)
    }

    /// How long (ms) the price has held at or past `level` in the rollback's
    /// direction — `HookRollBackWait`, which drops a rollback that was one
    /// print. Capped by the trades kept (`HOLD_MS`).
    pub fn held_beyond(&self, idx: u16, now: i64, level: f64, short: bool) -> i64 {
        let Some(t) = self.by_idx.get(&idx) else {
            return 0;
        };
        let beyond = |p: f64| if short { p <= level } else { p >= level };
        if t.last <= 0.0 || !beyond(t.last) {
            return 0;
        }
        let mut since = t.trades.front().map_or(now, |&(at, _)| at);
        for &(at, p) in t.trades.iter().rev() {
            if !beyond(p) {
                since = at;
                break;
            }
        }
        (now - since).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tape of one market: flat at 100 for 1.5 s, a strike to 95, a rollback
    /// to 97. A trade at 1 s puts 120 outside the frame but inside the two
    /// minutes `HookDrop` looks at.
    fn tape() -> Tracks {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 1_000, 120.0, 100.0);
        t.trade(1, 3_000, 100.0, 1_000.0);
        t.trade(1, 3_600, 100.0, 1_000.0);
        t.trade(1, 4_100, 95.0, 2_000.0);
        t.trade(1, 4_600, 97.0, 500.0);
        t
    }

    /// The frame's move is measured from its high to its low, the rollback
    /// from the low to the price now; the frame's turnover come with it.
    #[test]
    fn fall_depth_rollback_volume_and_hold() {
        let mut t = tape();
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!((d.reference, d.extreme), (100.0, 95.0));
        assert!((d.depth - 5.0 / 95.0 * 100.0).abs() < 1e-9, "off the low");
        assert!(
            (d.rollback - 40.0).abs() < 1e-9,
            "97 is 40 % back of 100 → 95"
        );
        assert_eq!(d.turnover, 4_500.0, "the 120 print is outside the frame");
        // `HookInitialPrice` / `HookPriceRollBack` read the same line.
        assert!((d.level(20.0) - 96.0).abs() < 1e-9);
        assert!((d.level(100.0) - 100.0).abs() < 1e-9);
        // The price has held above 96 since the 95 print.
        assert_eq!(t.held_beyond(1, 5_000, 96.0, false), 900);
        assert_eq!(t.held_beyond(1, 5_000, 97.5, false), 0, "not past it now");
    }

    /// `HookAntiPump` measures the depth from the mean of the window up to
    /// the low, so a strike that only undid a fast rise does not count as a
    /// deep one: 110, 100 and the 95 average 101.67 — a 7.0 % strike, where
    /// the plain reading takes the whole 15.8 % off the 110. The mean keeps
    /// the slot the low sits in; the direction guard is about a price the
    /// tape printed before the extreme, and a mean is not one.
    #[test]
    fn anti_pump_measures_from_the_mean_before_the_move() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 3_000, 110.0, 1_000.0);
        t.trade(1, 3_600, 100.0, 1_000.0);
        t.trade(1, 4_100, 95.0, 2_000.0);
        t.trade(1, 4_600, 97.0, 500.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, true, false).unwrap();
        assert!((d.reference - 305.0 / 3.0).abs() < 1e-9);
        assert!((d.depth - (305.0 / 3.0 - 95.0) / 95.0 * 100.0).abs() < 1e-9);
        assert!(d.depth > 7.0 && d.depth < 7.1);
        let plain = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert!((plain.reference - 110.0).abs() < 1e-9);
        assert!((plain.depth - 15.0 / 95.0 * 100.0).abs() < 1e-9);
    }

    /// KCHEP 23.09 10:25:50: a 2.54 % rise whose whole tape printed inside
    /// one 0.5 s slot. The slot keeps no order of its trades, so its high
    /// over its low must not read as a fall — and with no earlier slot to
    /// start one in, the fall measures zero and no `HookDetectDepth` passes
    /// it. The rise is read as a rise, and it has not come back, so neither
    /// side fires.
    #[test]
    fn a_spike_inside_one_slot_is_not_a_fall() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 4_600, 1.180, 100_000.0);
        t.trade(1, 4_700, 1.210, 150_000.0);
        t.sample(1, 5_000);
        let fall = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!(fall.depth, 0.0, "no slot before the spike to fall from");
        assert_eq!(fall.rollback, 0.0);
        let d = t.detect(1, 5_000, 4_000, false, true).unwrap();
        assert_eq!((d.reference, d.extreme), (1.180, 1.210));
        assert!((d.depth - 0.03 / 1.18 * 100.0).abs() < 1e-9);
        assert_eq!(d.rollback, 0.0, "the rise has not come back");
    }

    /// The extreme's own slot offers its open and nothing else: a fall that
    /// opened at its high keeps that high as the start, while one whose
    /// high printed after the low (100 → 90 → 105 → 95 inside the slot)
    /// does not get it — a net direction read off open and close would have
    /// called that slot falling and handed the 105 over, turning an 11 %
    /// move into a 17 % one.
    #[test]
    fn the_extreme_slot_offers_only_its_open_as_the_start() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 4_600, 100.0, 100_000.0);
        t.trade(1, 4_700, 95.0, 100_000.0);
        t.trade(1, 4_800, 97.0, 50_000.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!((d.reference, d.extreme), (100.0, 95.0));
        assert!((d.rollback - 40.0).abs() < 1e-9);

        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 4_600, 100.0, 100_000.0);
        t.trade(1, 4_700, 90.0, 100_000.0);
        t.trade(1, 4_800, 105.0, 100_000.0);
        t.trade(1, 4_900, 95.0, 50_000.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!(
            (d.reference, d.extreme),
            (100.0, 90.0),
            "the 105 printed after the low, so it is not where the fall began"
        );
        assert!((d.depth - 10.0 / 90.0 * 100.0).abs() < 1e-9);
    }

    /// `HookDrop`: the fall of the two minutes up to the move, in % of the
    /// move's own depth — 120 → 100 before a 5 % strike is 333 %.
    #[test]
    fn prior_drop_is_a_share_of_the_moves_depth() {
        let mut t = tape();
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert!((d.drop_ratio - 20.0 / (5.0 / 0.95) * 100.0).abs() < 1e-9);
        assert!((d.drop_ratio - 380.0).abs() < 0.01);
    }

    /// MoonBot 7.71 (AKE, 20.09 11:24:40): a frame high of 0.070238 over a low
    /// of 0.069384 is logged as `Depth: 1.23%`, and with a 0.072049 print in
    /// the two minutes before it, `Drop: 209.48%`. Both percentages are read
    /// off the *lower* of the two prices, the way every MoonBot delta is.
    #[test]
    fn depth_and_drop_match_the_moonbot_log() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 1_000, 0.072_049, 100.0);
        t.trade(1, 3_000, 0.070_238, 1_000.0);
        t.trade(1, 3_600, 0.070_238, 1_000.0);
        t.trade(1, 4_100, 0.069_384, 2_000.0);
        t.trade(1, 4_600, 0.070_261, 500.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!((d.reference, d.extreme), (0.070_238, 0.069_384));
        assert!(
            (d.depth - 1.23).abs() < 0.005,
            "depth {} != 1.23 %",
            d.depth
        );
        assert!(
            (d.drop_ratio - 209.48).abs() < 0.01,
            "drop {} != 209.48 %",
            d.drop_ratio
        );
    }

    /// A short's move is the mirror: a rise to its high, rolling back down.
    #[test]
    fn rise_detect_mirrors_the_fall() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 3_000, 100.0, 1_000.0);
        t.trade(1, 3_600, 100.0, 1_000.0);
        t.trade(1, 4_100, 105.0, 2_000.0);
        t.trade(1, 4_600, 103.0, 500.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, true).unwrap();
        assert_eq!((d.reference, d.extreme), (100.0, 105.0));
        assert!((d.depth - 5.0).abs() < 1e-9);
        assert!((d.rollback - 40.0).abs() < 1e-9);
        assert!((d.level(20.0) - 104.0).abs() < 1e-9);
        assert_eq!(t.held_beyond(1, 5_000, 104.0, true), 900);
        assert!(t.detect(1, 5_000, 4_000, false, false).unwrap().depth == 0.0);
    }

    /// Nothing before the market has been tracked a whole frame, and nothing
    /// across a reset: a move over a halt is not a move.
    #[test]
    fn a_full_frame_of_tracking_is_required() {
        let mut t = tape();
        assert!(t.detect(1, 3_900, 4_000, false, false).is_none());
        assert!(t.detect(1, 5_000, 4_000, false, false).is_some());
        t.reset(1);
        assert!(!t.contains(1));
        t.sample(1, 5_000);
        assert!(
            t.detect(1, 9_000, 4_000, false, false).is_none(),
            "no trades"
        );
        assert_eq!(t.held_beyond(1, 9_000, 1.0, false), 0);
    }

    /// The trade buffer covers a whole frame, so a `HookRollBackWait` that
    /// could fit inside `HookTimeFrame` is representable.
    #[test]
    fn a_rollback_can_be_held_for_a_whole_frame() {
        let mut t = Tracks::default();
        t.sample(1, 0);
        t.trade(1, 1_000, 99.0, 1.0);
        // A market trading right through the window keeps the buffer trimmed
        // to `HOLD_MS`, which is what caps the answer.
        for k in 0..=MAX_FRAME_MS / 1_000 {
            t.trade(1, 2_000 + k * 1_000, 100.0, 1.0);
        }
        let now = 2_000 + MAX_FRAME_MS;
        assert_eq!(t.held_beyond(1, now, 99.5, false), MAX_FRAME_MS);
        // The hold still starts at the last trade under the level.
        t.trade(1, now + 500, 99.0, 1.0);
        t.trade(1, now + 1_000, 100.0, 1.0);
        assert_eq!(t.held_beyond(1, now + 2_000, 99.5, false), 1_500);
    }

    /// A trade reported behind the newest slot joins nothing; the frame it
    /// belongs to has been judged already.
    #[test]
    fn a_late_trade_does_not_reopen_an_older_slot() {
        let mut t = tape();
        t.trade(1, 3_100, 1.0, 9_999.0);
        t.sample(1, 5_000);
        let d = t.detect(1, 5_000, 4_000, false, false).unwrap();
        assert_eq!((d.extreme, d.turnover), (95.0, 4_500.0));
    }
}
