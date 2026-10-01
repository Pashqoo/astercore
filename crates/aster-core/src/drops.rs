//! DropsDetection detector (MoonBot FAQ «Drop Detection»): the price is
//! sampled every 2 s; over the last `DropsMaxTime` s the highest average of
//! `DropsPriceMA` s is compared with the current price (the average of the
//! last `DropsLastPriceMA` samples); the drop is `(high / current − 1)·100`.

use std::collections::{HashMap, VecDeque};

/// MoonBot receives prices once in 2 s; one sample per slot.
pub const SAMPLE_MS: i64 = 2_000;
/// Longest `DropsMaxTime` the tape serves, s; a longer window is cut to it.
pub const MAX_TIME_S: i64 = 3_600;

/// Samples of one market, oldest first: `(slot, price)`, `slot = ms / SAMPLE_MS`.
#[derive(Default)]
pub struct Tapes {
    by_idx: HashMap<u16, VecDeque<(i64, f64)>>,
}

impl Tapes {
    /// The market's price now; a later sample of the same slot replaces it.
    pub fn sample(&mut self, idx: u16, now: i64, price: f64) {
        if price <= 0.0 {
            return;
        }
        let slot = now.div_euclid(SAMPLE_MS);
        let q = self.by_idx.entry(idx).or_default();
        match q.back_mut() {
            Some(last) if last.0 >= slot => last.1 = price,
            _ => q.push_back((slot, price)),
        }
        let keep = MAX_TIME_S * 1000 / SAMPLE_MS;
        while q.front().is_some_and(|&(s, _)| s <= slot - keep) {
            q.pop_front();
        }
    }

    /// The market stopped trading normally or its feed went stale: a drop
    /// across the gap is not a drop.
    pub fn reset(&mut self, idx: u16) {
        self.by_idx.remove(&idx);
    }

    /// Keeps the tapes of `keep` markets only.
    pub fn retain(&mut self, keep: impl Fn(u16) -> bool) {
        self.by_idx.retain(|&idx, _| keep(idx));
    }

    /// Drop over the last `max_time` s, `None` until the tape covers it.
    pub fn drop_at(
        &self,
        idx: u16,
        now: i64,
        max_time: f64,
        price_ma: f64,
        last_ma: i64,
        last: f64,
    ) -> Option<Drop> {
        let q = self.by_idx.get(&idx)?;
        let n = slots(max_time).min(MAX_TIME_S * 1000 / SAMPLE_MS);
        let now_slot = now.div_euclid(SAMPLE_MS);
        let from = now_slot - n + 1;
        if q.front().is_none_or(|&(s, _)| s > from) {
            return None;
        }
        let upto: Vec<f64> = q
            .iter()
            .filter(|&&(s, _)| s <= now_slot)
            .map(|&(_, p)| p)
            .collect();
        let in_window = q
            .iter()
            .filter(|&&(s, _)| s >= from && s <= now_slot)
            .count();
        if in_window == 0 {
            return None;
        }
        let prices = &upto[upto.len() - in_window..];
        let k = (slots(price_ma) as usize).clamp(1, prices.len());
        let mut sum: f64 = prices[..k].iter().sum();
        let mut high = sum / k as f64;
        for i in k..prices.len() {
            sum += prices[i] - prices[i - k];
            high = high.max(sum / k as f64);
        }
        // The current price may average past the window: the whole tape serves.
        let current = if last_ma > 0 {
            let j = (last_ma as usize).min(upto.len());
            upto[upto.len() - j..].iter().sum::<f64>() / j as f64
        } else {
            last
        };
        (current > 0.0).then(|| Drop {
            high,
            current,
            pct: (high / current - 1.0) * 100.0,
        })
    }
}

/// A measured drop: the highest average, the current price and the drop, %.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Drop {
    pub high: f64,
    pub current: f64,
    pub pct: f64,
}

/// Samples in `secs` seconds, at least one.
fn slots(secs: f64) -> i64 {
    ((secs * 1000.0 / SAMPLE_MS as f64).round() as i64).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tape(prices: &[f64]) -> Tapes {
        let mut t = Tapes::default();
        for (i, &p) in prices.iter().enumerate() {
            t.sample(1, i as i64 * SAMPLE_MS, p);
        }
        t
    }

    /// FAQ example: 100 s window, 20 s averages; the highest average 102
    /// against the current 100 is a 2 % drop.
    #[test]
    fn highest_average_against_current_price() {
        let mut prices = vec![102.0; 10];
        prices.extend([101.0; 30]);
        prices.extend([100.0; 10]);
        let t = tape(&prices);
        let now = 49 * SAMPLE_MS;
        let d = t.drop_at(1, now, 100.0, 20.0, 1, 0.0).unwrap();
        assert_eq!((d.high, d.current), (102.0, 100.0));
        assert!((d.pct - 2.0).abs() < 1e-9);
        // The average of the last two samples, and the live price with 0.
        let spike = tape(&[102.0, 102.0, 102.0, 96.0]);
        let d = spike.drop_at(1, 3 * SAMPLE_MS, 8.0, 2.0, 2, 0.0).unwrap();
        assert_eq!(d.current, 99.0);
        let d = spike.drop_at(1, 3 * SAMPLE_MS, 8.0, 2.0, 0, 95.0).unwrap();
        assert_eq!(d.current, 95.0);
        // The current average may reach past a shorter window.
        let d = spike.drop_at(1, 3 * SAMPLE_MS, 2.0, 2.0, 4, 0.0).unwrap();
        assert_eq!((d.high, d.current), (96.0, 100.5));
        // A 20 s average smooths the high: (102·9 + 96) / 10 ≠ 102.
        let d = tape(&[102.0; 9].into_iter().chain([96.0]).collect::<Vec<_>>())
            .drop_at(1, 9 * SAMPLE_MS, 20.0, 20.0, 1, 0.0)
            .unwrap();
        assert!((d.high - 101.4).abs() < 1e-9);
    }

    /// No verdict until the tape spans the window; a reset starts it over.
    #[test]
    fn needs_a_full_window() {
        let mut t = tape(&[100.0; 5]);
        assert!(t.drop_at(1, 4 * SAMPLE_MS, 12.0, 2.0, 1, 0.0).is_none());
        assert!(t.drop_at(1, 4 * SAMPLE_MS, 10.0, 2.0, 1, 0.0).is_some());
        t.reset(1);
        assert!(t.drop_at(1, 4 * SAMPLE_MS, 2.0, 2.0, 1, 0.0).is_none());
        // Same slot: the later price wins.
        t.sample(1, 10 * SAMPLE_MS, 100.0);
        t.sample(1, 10 * SAMPLE_MS + 500, 99.0);
        let d = t.drop_at(1, 10 * SAMPLE_MS, 2.0, 2.0, 1, 0.0).unwrap();
        assert_eq!((d.high, d.current), (99.0, 99.0));
    }
}
