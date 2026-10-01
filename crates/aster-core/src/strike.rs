//! MoonStrike detector (MoonBot FAQ «Специфические параметры MoonStrike»).
//! LastBidEMA over 4 ticks of 2 s: when the previous tick's bid is below
//! the EMA the EMA drops to it at once, otherwise it moves as EMA(4) — a
//! fall is followed at once, a rise slowly («MoonStrikePriceSmooth»). A
//! strike is the run of trades below the EMA; its depth is measured from
//! the EMA to the run's low, its volume sums their turnover.
//!
//! A short measures from a LastAskEMA, the strict mirror: a rise is followed
//! at once, a fall moves as EMA(4), and the strike is the run of trades above
//! it. Measuring a short from the bid — as this did until 24.09 — is a
//! one-way ratchet: every trade prints at or above the bid, so the spread
//! alone is counted as depth and a market whose book is a percent wide
//! signals on single trades that moved nothing. CRCLperpA 24.09 16:34:42 MSK:
//! `LastBID 93.77 max.Price 95.05 Depth 1.36%` on a run of two trades at one
//! price, while the high of the preceding minute (95.29) was already behind
//! and the price was falling; the day's `strike futures` ran 16 detects, all
//! short, none long, with `MStrikeDirection=Both`.
//!
//! A strike is sharp: an extreme not renewed for a whole tick ends it — the
//! strike is the moment of its extreme (MoonBot: the order goes in «before
//! the rebound»); turnover traded later count toward a new strike, and an
//! old high must not signal again minutes later.

use std::collections::HashMap;

/// One EMA tick, as MoonBot's price ticks.
pub const TICK_MS: i64 = 2_000;
/// EMA(4): `α = 2 / (4 + 1)`.
const ALPHA: f64 = 2.0 / 5.0;
/// `MStrikeWaitDip` waits for the rebound this long, then drops the signal.
pub const WAIT_DIP_MS: i64 = 10_000;

/// Trades beyond the EMA: the extreme price, when it was set (local ms) and
/// their USDT volume.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Run {
    extreme: f64,
    extreme_at: i64,
    turnover: f64,
}

impl Run {
    fn stale(&self, now: i64) -> bool {
        now - self.extreme_at >= TICK_MS
    }
}

#[derive(Default)]
struct Track {
    tick: i64,
    /// This tick's bid and ask (the previous tick's once the next begins).
    bid: f64,
    ask: f64,
    ema_bid: f64,
    ema_ask: f64,
    down: Option<Run>,
    up: Option<Run>,
    last_trade: f64,
    /// Local time of the last trade above / below the one before it.
    rise_at: i64,
    fall_at: i64,
}

/// A strike on one side: the price before it (the EMA), its extreme, depth %
/// and USDT volume.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Strike {
    pub reference: f64,
    pub extreme: f64,
    pub depth: f64,
    pub turnover: f64,
}

#[derive(Default)]
pub struct Tracks {
    by_idx: HashMap<u16, Track>,
}

impl Tracks {
    /// The two sides of the book now (`last` for a side without one); a new
    /// tick first moves each EMA by the previous tick's quote. A run whose
    /// extreme is a tick old is over.
    pub fn sample(&mut self, idx: u16, now: i64, bid: f64, ask: f64) {
        let t = self.by_idx.entry(idx).or_default();
        if t.down.is_some_and(|r| r.stale(now)) {
            t.down = None;
        }
        if t.up.is_some_and(|r| r.stale(now)) {
            t.up = None;
        }
        let tick = now.div_euclid(TICK_MS);
        if tick > t.tick {
            if t.bid > 0.0 {
                t.ema_bid = step(t.ema_bid, t.bid, false);
            }
            if t.ask > 0.0 {
                t.ema_ask = step(t.ema_ask, t.ask, true);
            }
            t.tick = tick;
            // The EMA reached the run: it is the price now, not a strike.
            if t.down.is_some_and(|r| r.extreme >= t.ema_bid) {
                t.down = None;
            }
            if t.up.is_some_and(|r| r.extreme <= t.ema_ask) {
                t.up = None;
            }
        }
        t.bid = bid;
        t.ask = ask;
    }

    pub fn contains(&self, idx: u16) -> bool {
        self.by_idx.contains_key(&idx)
    }

    /// A market no longer tracked or not trading normally: start over.
    pub fn reset(&mut self, idx: u16) {
        self.by_idx.remove(&idx);
    }

    pub fn retain(&mut self, keep: impl Fn(u16) -> bool) {
        self.by_idx.retain(|&idx, _| keep(idx));
    }

    /// An exchange trade of a tracked market; returns whether it extends a
    /// strike (the strategies should look at once).
    pub fn trade(&mut self, idx: u16, now: i64, price: f64, turnover: f64) -> bool {
        let Some(t) = self.by_idx.get_mut(&idx) else {
            return false;
        };
        if price <= 0.0 {
            return false;
        }
        if t.last_trade > 0.0 {
            if price > t.last_trade {
                t.rise_at = now;
            } else if price < t.last_trade {
                t.fall_at = now;
            }
        }
        t.last_trade = price;
        let extend = |run: &mut Option<Run>, beyond: bool, better: fn(f64, f64) -> f64| {
            if !beyond || run.is_some_and(|r| r.stale(now)) {
                *run = None;
            }
            if !beyond {
                return false;
            }
            let r = run.get_or_insert(Run {
                extreme: price,
                extreme_at: now,
                turnover: 0.0,
            });
            if better(r.extreme, price) != r.extreme {
                r.extreme = price;
                r.extreme_at = now;
            }
            r.turnover += turnover;
            true
        };
        let down = extend(&mut t.down, t.ema_bid > 0.0 && price < t.ema_bid, f64::min);
        let up = extend(&mut t.up, t.ema_ask > 0.0 && price > t.ema_ask, f64::max);
        down || up
    }

    /// The strike running on `short`'s side (up from the LastAskEMA for a
    /// short, down from the LastBidEMA for a long), if any.
    pub fn strike(&self, idx: u16, short: bool) -> Option<Strike> {
        let t = self.by_idx.get(&idx)?;
        let (run, reference) = if short {
            (t.up?, t.ema_ask)
        } else {
            (t.down?, t.ema_bid)
        };
        (reference > 0.0).then(|| Strike {
            reference,
            extreme: run.extreme,
            depth: (reference - run.extreme).abs() / reference * 100.0,
            turnover: run.turnover,
        })
    }

    /// Local time of the last trade that turned back against the strike
    /// (above the one before it for a long); 0 = none seen.
    pub fn rebound_at(&self, idx: u16, short: bool) -> i64 {
        self.by_idx
            .get(&idx)
            .map_or(0, |t| if short { t.fall_at } else { t.rise_at })
    }
}

/// One EMA tick. The quote is taken whole when it moves away from the side
/// the strategy enters on — a bid below the EMA, an ask (`up`) above it —
/// and smoothed as EMA(4) when it moves toward it. So each EMA trails the
/// market on its own side, and depth is what the trades did beyond the
/// quote, never the spread between the two sides.
fn step(ema: f64, x: f64, up: bool) -> f64 {
    let follows = if up { x > ema } else { x < ema };
    if ema <= 0.0 || follows {
        x
    } else {
        ema + (x - ema) * ALPHA
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    /// One tick of both sides of the book.
    fn tick(t: &mut Tracks, n: i64, bid: f64, ask: f64) {
        t.sample(1, n * TICK_MS, bid, ask);
    }

    /// A bid falling tick by tick drags the EMA down at once; a rising one
    /// moves it by 2/5 of the gap.
    #[test]
    fn last_bid_ema_follows_falls_at_once_and_rises_slowly() {
        let mut t = Tracks::default();
        tick(&mut t, 1, 100.0, 100.1);
        tick(&mut t, 2, 100.0, 100.1);
        assert_eq!(t.by_idx[&1].ema_bid, 100.0);
        tick(&mut t, 3, 98.0, 98.1);
        tick(&mut t, 4, 98.0, 98.1);
        assert_eq!(t.by_idx[&1].ema_bid, 98.0);
        tick(&mut t, 5, 103.0, 103.1);
        assert_eq!(
            t.by_idx[&1].ema_bid, 98.0,
            "the new tick counts from the next one"
        );
        tick(&mut t, 6, 103.0, 103.1);
        assert!((t.by_idx[&1].ema_bid - 100.0).abs() < 1e-9);
    }

    /// The short's reference is the strict mirror: a rising ask is taken
    /// whole, a falling one moves by 2/5 of the gap.
    #[test]
    fn last_ask_ema_follows_rises_at_once_and_falls_slowly() {
        let mut t = Tracks::default();
        tick(&mut t, 1, 99.9, 100.0);
        tick(&mut t, 2, 99.9, 100.0);
        assert_eq!(t.by_idx[&1].ema_ask, 100.0);
        tick(&mut t, 3, 101.9, 102.0);
        tick(&mut t, 4, 101.9, 102.0);
        assert_eq!(t.by_idx[&1].ema_ask, 102.0, "a rise is followed at once");
        tick(&mut t, 5, 96.9, 97.0);
        assert_eq!(
            t.by_idx[&1].ema_ask, 102.0,
            "the new tick counts from the next one"
        );
        tick(&mut t, 6, 96.9, 97.0);
        assert!((t.by_idx[&1].ema_ask - 100.0).abs() < 1e-9);
    }

    /// Trades below the EMA make a strike: depth from the EMA to their low,
    /// their turnover; a trade back at the EMA ends it; a rise after a fall
    /// is the rebound.
    #[test]
    fn strike_depth_volume_and_rebound() {
        let mut t = Tracks::default();
        assert!(!t.trade(1, 0, 99.0, 1.0), "untracked market");
        t.sample(1, TICK_MS, 100.0, 100.0);
        t.sample(1, 2 * TICK_MS, 100.0, 100.0);
        assert!(!t.trade(1, 4_100, 100.0, 1_000.0));
        assert!(t.trade(1, 4_200, 99.0, 2_000.0));
        assert!(t.trade(1, 4_300, 97.0, 3_000.0));
        let s = t.strike(1, false).unwrap();
        assert_eq!((s.reference, s.extreme, s.turnover), (100.0, 97.0, 5_000.0));
        assert!((s.depth - 3.0).abs() < 1e-9);
        assert_eq!(t.rebound_at(1, false), 0);
        assert!(t.trade(1, 4_400, 98.0, 1.0));
        assert_eq!(t.rebound_at(1, false), 4_400);
        assert!(t.strike(1, true).is_none());
        assert!(!t.trade(1, 4_500, 100.0, 1.0));
        assert!(t.strike(1, false).is_none());
        // Up for a short: above the LastAskEMA, which this book shares with
        // the bid.
        assert!(t.trade(1, 4_600, 102.0, 10.0));
        let s = t.strike(1, true).unwrap();
        assert_eq!((s.reference, s.extreme), (100.0, 102.0));
        assert!((s.depth - 2.0).abs() < 1e-9);
    }

    /// 24.09, CRCLperpA 16:34:42 MSK: a book over a percent wide, one trade
    /// at the ask, no move at all. Measured from the bid this scored
    /// `Depth: 1.36%` and opened a short; from the ask it is the 0.05 % the
    /// trade actually travelled, and nothing fires.
    #[test]
    fn a_wide_book_is_not_depth_for_a_short() {
        let mut t = Tracks::default();
        t.sample(1, TICK_MS, 93.77, 95.00);
        t.sample(1, 2 * TICK_MS, 93.77, 95.00);
        assert!(t.trade(1, 4_100, 95.05, 1.0));
        let s = t.strike(1, true).unwrap();
        assert_eq!(s.reference, 95.00);
        assert!(s.depth < 0.06, "depth from the ask: {}", s.depth);
        // The bid is still tracked, and still a percent away: that distance
        // is the book's width, and it is no longer anyone's depth.
        let bid = t.by_idx[&1].ema_bid;
        assert_eq!(bid, 93.77);
        assert!(
            (s.extreme - bid) / bid * 100.0 > 1.3,
            "the bid would have cleared the 1 % threshold on the spread alone"
        );
    }

    /// The EMA that catches up with the run's low ends the strike.
    #[test]
    fn ema_reaching_the_low_ends_the_strike() {
        let mut t = Tracks::default();
        t.sample(1, TICK_MS, 100.0, 100.0);
        t.sample(1, 2 * TICK_MS, 100.0, 100.0);
        t.trade(1, 4_100, 97.0, 1.0);
        t.sample(1, 4_200, 97.0, 97.0);
        assert!(t.strike(1, false).is_some());
        t.sample(1, 3 * TICK_MS, 97.0, 97.0);
        assert!(t.strike(1, false).is_none());
    }

    /// SLEN 21.09 09:53: a spike to 3.565 over an ask of 3.455, then no
    /// trade for minutes; the old high must not signal again a minute later.
    #[test]
    fn a_strike_without_a_new_extreme_for_a_tick_is_over() {
        let mut t = Tracks::default();
        t.sample(1, TICK_MS, 3.450, 3.455);
        t.sample(1, 2 * TICK_MS, 3.450, 3.455);
        assert!(t.trade(1, 4_100, 3.525, 1.0));
        assert!(t.trade(1, 4_200, 3.565, 1.0));
        // Trades below the high keep it and add their turnover.
        t.sample(1, 4_200 + TICK_MS - 1, 3.450, 3.455);
        assert!(t.trade(1, 4_200 + TICK_MS - 1, 3.515, 1.0));
        let s = t.strike(1, true).unwrap();
        assert_eq!((s.extreme, s.turnover), (3.565, 3.0));
        t.sample(1, 4_200 + TICK_MS, 3.450, 3.455);
        assert!(t.strike(1, true).is_none(), "the high is a tick old");
        t.sample(1, 64_200, 3.450, 3.455);
        assert!(t.strike(1, true).is_none());
        // A new trade above the EMA starts a new strike from its own price.
        assert!(t.trade(1, 64_300, 3.460, 1.0));
        assert_eq!(t.strike(1, true).unwrap().extreme, 3.460);
        // A trade a tick after the high, before any sample, does not feed it.
        assert!(t.trade(1, 66_300, 3.458, 1.0));
        let s = t.strike(1, true).unwrap();
        assert_eq!((s.extreme, s.turnover), (3.458, 1.0));
    }
}
