//! The screener's retained 5-minute candles, what `RequestCandlesData`
//! answers: per market, the low, the high and the QUOTE turnover of each
//! 5-minute period over the last [`KEEP`] of them.
//!
//! The terminal's client loads this once per connection and sums it into its
//! window columns (1 h to 72 h volumes and ranges); its own tape then extends
//! it with the period in progress. Turnover is in USDT because that is what
//! the client's own candle accumulates (`traded_value`) and what every other
//! volume column of the screener shows (`Vol.` is the 24-hour quote volume).
//!
//! Seeded from `klines 5m` at startup (`feed.rs`, the warm-up) and kept from
//! the `aggTrade` tape after it. TInvestCore derived the same answer from its
//! minute windows (`windows.rs`, 24 hours deep); those arrive here with the
//! strategies (M3), and a day of minutes would not reach the 41 hours the
//! client keeps anyway.

use std::collections::VecDeque;

use moonproto::server::codec::market_data::{delphi_days, Candle};

const PERIOD_MS: i64 = 5 * 60_000;
/// Sealed periods a market keeps and answers with: the client's ring holds
/// 500 (`DEEP_5M_CAPACITY` in `moonproto`), so an older one is never read.
/// The period in progress is kept on top of them.
const KEEP: usize = 500;

#[derive(Debug, Clone, Copy)]
struct Period {
    /// `time_ms / PERIOD_MS`.
    n: i64,
    lo: f64,
    hi: f64,
    quote: f64,
}

#[derive(Default)]
pub struct Candles5m {
    by_idx: Vec<VecDeque<Period>>,
}

impl Candles5m {
    /// One print of the tape: `qty` in base units, either sign.
    pub fn push(&mut self, idx: u16, time_ms: i64, price: f64, qty: f64) {
        if price.is_nan() || price <= 0.0 {
            return;
        }
        if let Some(p) = self.slot(idx, time_ms.div_euclid(PERIOD_MS)) {
            p.lo = p.lo.min(price);
            p.hi = p.hi.max(price);
            p.quote += price * qty.abs();
        }
    }

    /// One `klines 5m` bar, `quote` its quote turnover. It REPLACES what the
    /// tape put into that period: a sealed bar is the exchange's complete word
    /// on it, prints from before the core started included. The bar still in
    /// progress is the exchange's word up to the moment it answered, and the
    /// tape adds what comes after; a print that crossed the answer in flight
    /// can be counted twice or not at all, which is milliseconds of one
    /// period.
    pub fn seed(&mut self, idx: u16, open_ms: i64, lo: f64, hi: f64, quote: f64) {
        if lo.is_nan() || lo <= 0.0 || hi.is_nan() || hi < lo {
            return;
        }
        if let Some(p) = self.slot(idx, open_ms.div_euclid(PERIOD_MS)) {
            p.lo = lo;
            p.hi = hi;
            // A bar without the figure (NaN) keeps what the tape counted.
            if !quote.is_nan() {
                p.quote = quote.max(0.0);
            }
        }
    }

    /// The period `n` of a market, inserted in order when it is new (with an
    /// empty range the caller fills). `None` for a period older than the
    /// market keeps: it would be dropped on the next trim anyway.
    fn slot(&mut self, idx: u16, n: i64) -> Option<&mut Period> {
        let i = usize::from(idx);
        if self.by_idx.len() <= i {
            self.by_idx.resize_with(i + 1, VecDeque::new);
        }
        let q = &mut self.by_idx[i];
        let newest = q.back().map_or(n, |p| p.n.max(n));
        let oldest = newest - KEEP as i64;
        if n < oldest {
            return None;
        }
        while q.front().is_some_and(|p| p.n < oldest) {
            q.pop_front();
        }
        let pos = match q.binary_search_by_key(&n, |p| p.n) {
            Ok(pos) => pos,
            Err(pos) => {
                q.insert(
                    pos,
                    Period {
                        n,
                        lo: f64::INFINITY,
                        hi: 0.0,
                        quote: 0.0,
                    },
                );
                pos
            }
        };
        q.get_mut(pos)
    }

    /// The sealed candles of a market at `now_ms`, oldest first, each stamped
    /// with the END of its period (the client's convention for this ring), the
    /// period in progress left out — the client builds that one itself.
    ///
    /// A market whose last sealed period is not the latest one gets a neutral
    /// (all-zero) candle at the latest boundary: the client discards a whole
    /// snapshot of a market whose newest candle is older than 11 minutes
    /// (`STALE_CANDLES_SNAPSHOT_MS`), and a quiet market would lose its day of
    /// history for one silent quarter of an hour. Zero low, high and volume
    /// are skipped by every accumulator of the client. Ported from TInvestCore.
    pub fn sealed(&self, idx: u16, now_ms: i64) -> Vec<Candle> {
        let current = now_ms.div_euclid(PERIOD_MS);
        let Some(q) = self.by_idx.get(usize::from(idx)) else {
            return Vec::new();
        };
        let sealed: Vec<&Period> = q.iter().filter(|p| p.n < current).collect();
        let neutral = sealed.last().is_some_and(|p| p.n + 1 < current);
        // The neutral candle takes a slot of the client's ring too.
        let from = sealed.len().saturating_sub(KEEP - usize::from(neutral));
        let mut out: Vec<Candle> = sealed[from..]
            .iter()
            .map(|p| Candle {
                open: 0.0,
                high: p.hi as f32,
                low: p.lo as f32,
                close: 0.0,
                volume: p.quote as f32,
                time: delphi_days((p.n + 1) * PERIOD_MS),
            })
            .collect();
        if neutral {
            out.push(Candle {
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
                time: delphi_days(current * PERIOD_MS),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 6_000_000 * PERIOD_MS;

    #[test]
    fn the_tape_fills_periods_and_the_one_in_progress_is_not_answered() {
        let mut c = Candles5m::default();
        c.push(2, T0 + 1_000, 100.0, 2.0);
        c.push(2, T0 + 2_000, 90.0, -1.0);
        c.push(2, T0 + PERIOD_MS + 5, 110.0, 1.0);
        // A late print folds into its own period, not the newest one.
        c.push(2, T0 + 3_000, 120.0, 0.5);
        let out = c.sealed(2, T0 + PERIOD_MS + 10);
        assert_eq!(out.len(), 1, "the period in progress stays the client's");
        let k = out[0];
        assert_eq!((k.low, k.high, k.volume), (90.0, 120.0, 350.0));
        assert_eq!(k.time, delphi_days(T0 + PERIOD_MS), "stamped at its end");
        assert!(c.sealed(3, T0 + PERIOD_MS).is_empty(), "unknown market");
    }

    #[test]
    fn a_bar_replaces_what_the_tape_put_into_its_period() {
        let mut c = Candles5m::default();
        c.push(0, T0 + 1_000, 100.0, 1.0);
        c.seed(0, T0, 95.0, 105.0, 5_000.0);
        // The tape goes on after the bar was taken.
        c.push(0, T0 + 200_000, 106.0, 1.0);
        let out = c.sealed(0, T0 + PERIOD_MS);
        assert_eq!(
            (out[0].low, out[0].high, out[0].volume),
            (95.0, 106.0, 5_106.0)
        );
        // A bar without its quote figure keeps the tape's turnover.
        c.seed(0, T0, 95.0, 105.0, f64::NAN);
        assert_eq!(c.sealed(0, T0 + PERIOD_MS)[0].volume, 5_106.0);
        // Garbage bars are not ranges.
        c.seed(0, T0 - PERIOD_MS, 0.0, 1.0, 1.0);
        c.seed(0, T0 - PERIOD_MS, 2.0, 1.0, 1.0);
        assert_eq!(c.sealed(0, T0 + PERIOD_MS).len(), 1);
    }

    #[test]
    fn a_market_keeps_the_newest_periods_whatever_order_they_arrive_in() {
        let mut c = Candles5m::default();
        let total = KEEP as i64 + 20;
        // Newest first, as a late warm-up would land under a running tape.
        for i in (0..total).rev() {
            c.seed(1, T0 + i * PERIOD_MS, 1.0, 2.0, i as f64);
        }
        let now = T0 + total * PERIOD_MS;
        let out = c.sealed(1, now);
        assert_eq!(out.len(), KEEP);
        assert_eq!(out[0].volume, 20.0, "the oldest twenty fell out");
        assert_eq!(out[KEEP - 1].time, delphi_days(now));
        assert!(c.by_idx[1].len() <= KEEP + 1);
    }

    #[test]
    fn a_quiet_market_gets_a_neutral_candle_at_the_latest_boundary() {
        let mut c = Candles5m::default();
        c.seed(4, T0, 1.0, 2.0, 10.0);
        let now = T0 + 4 * PERIOD_MS + 30_000;
        let out = c.sealed(4, now);
        assert_eq!(out.len(), 2);
        let n = out[1];
        assert_eq!((n.low, n.high, n.volume), (0.0, 0.0, 0.0));
        assert_eq!(n.time, delphi_days(T0 + 4 * PERIOD_MS));
        // The latest period sealed: nothing to add.
        assert_eq!(c.sealed(4, T0 + PERIOD_MS + 1).len(), 1);
        // With a full history the neutral candle still fits the client's ring.
        for i in 0..KEEP as i64 + 5 {
            c.seed(5, T0 + i * PERIOD_MS, 1.0, 2.0, 1.0);
        }
        let out = c.sealed(5, T0 + (KEEP as i64 + 10) * PERIOD_MS);
        assert_eq!(out.len(), KEEP);
        assert_eq!(out[KEEP - 1].volume, 0.0, "the neutral one is the newest");
    }
}
