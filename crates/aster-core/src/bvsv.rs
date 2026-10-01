//! Buy and sell volume per market for the BV/SV stop and its entry check
//! (MoonBot's `UseBV_SV_Stop`): turnover bought and sold by aggressors over
//! the last `BV_SV_TradesN` exchange trades or seconds (`BV_SV_Kind`). Only
//! markets a BV/SV strategy watches keep a tape; it starts empty, so a window
//! counts only once it is covered.

use std::collections::{HashMap, VecDeque};

/// A per-market bound on memory, whatever the strategies ask for.
const MAX_TRADES: usize = 100_000;
const MAX_SECONDS: i64 = 24 * 3600;

/// `BV_SV_Kind`: the window is the last N trades or the last N seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Trades,
    Time,
}

impl Kind {
    pub const PICKLIST: &'static str = "TradesCount|Time";

    pub fn parse(v: &str) -> Self {
        if v.trim().eq_ignore_ascii_case("time") {
            Self::Time
        } else {
            Self::Trades
        }
    }
}

#[derive(Default)]
struct Tape {
    /// Local time the market began to be watched: a time window is covered
    /// once it lies wholly after it.
    since: i64,
    need_trades: usize,
    need_secs: i64,
    /// Signed turnover of the last trades (a buy positive).
    trades: VecDeque<f64>,
    /// `(second, bought, sold)` turnover per second with trades.
    secs: VecDeque<(i64, f64, f64)>,
}

#[derive(Default)]
pub struct Tapes {
    by_idx: HashMap<u16, Tape>,
}

impl Tapes {
    /// A strategy reads `kind`'s window of `n` on the market this pass.
    pub fn watch(&mut self, idx: u16, kind: Kind, n: i64, now: i64) {
        let tape = self.by_idx.entry(idx).or_insert_with(|| Tape {
            since: now,
            ..Tape::default()
        });
        let n = n.max(1);
        match kind {
            Kind::Trades => tape.need_trades = tape.need_trades.max((n as usize).min(MAX_TRADES)),
            Kind::Time => tape.need_secs = tape.need_secs.max(n.min(MAX_SECONDS)),
        }
    }

    /// One exchange trade of `turnover`; `buy` = the aggressor bought.
    pub fn trade(&mut self, idx: u16, now: i64, turnover: f64, buy: bool) {
        let Some(tape) = self.by_idx.get_mut(&idx) else {
            return;
        };
        if tape.need_trades > 0 {
            tape.trades
                .push_back(if buy { turnover } else { -turnover });
            while tape.trades.len() > tape.need_trades {
                tape.trades.pop_front();
            }
        }
        if tape.need_secs > 0 {
            let sec = now.div_euclid(1000);
            match tape.secs.back_mut() {
                Some((s, b, sl)) if *s == sec => *if buy { b } else { sl } += turnover,
                _ => tape.secs.push_back(if buy {
                    (sec, turnover, 0.0)
                } else {
                    (sec, 0.0, turnover)
                }),
            }
            while tape
                .secs
                .front()
                .is_some_and(|&(s, _, _)| s <= sec - tape.need_secs)
            {
                tape.secs.pop_front();
            }
        }
    }

    /// `(bought, sold)` turnover of the window; `None` while it is not
    /// covered yet (fewer trades seen, or watched for less than its time).
    pub fn volumes(&self, idx: u16, kind: Kind, n: i64, now: i64) -> Option<(f64, f64)> {
        let tape = self.by_idx.get(&idx)?;
        let n = n.max(1);
        match kind {
            Kind::Trades => {
                let n = n as usize;
                if tape.trades.len() < n {
                    return None;
                }
                Some(
                    tape.trades
                        .iter()
                        .rev()
                        .take(n)
                        .fold(
                            (0.0, 0.0),
                            |(b, s), &v| {
                                if v > 0.0 {
                                    (b + v, s)
                                } else {
                                    (b, s - v)
                                }
                            },
                        ),
                )
            }
            Kind::Time => {
                if now - tape.since < n * 1000 {
                    return None;
                }
                let from = now.div_euclid(1000) - n;
                Some(
                    tape.secs
                        .iter()
                        .filter(|&&(s, _, _)| s > from)
                        .fold((0.0, 0.0), |(b, s), &(_, tb, ts)| (b + tb, s + ts)),
                )
            }
        }
    }

    /// Keep only the markets a strategy still watches.
    pub fn retain(&mut self, keep: impl Fn(u16) -> bool) {
        self.by_idx.retain(|&idx, _| keep(idx));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trades_window_counts_the_last_n_once_covered() {
        let mut t = Tapes::default();
        t.trade(1, 0, 100.0, true);
        t.watch(1, Kind::Trades, 3, 0);
        t.trade(1, 1, 100.0, true);
        t.trade(1, 2, 50.0, false);
        assert_eq!(
            t.volumes(1, Kind::Trades, 3, 3),
            None,
            "unwatched trade not kept"
        );
        t.trade(1, 3, 30.0, false);
        t.trade(1, 4, 40.0, true);
        assert_eq!(t.volumes(1, Kind::Trades, 3, 5), Some((40.0, 80.0)));
    }

    #[test]
    fn time_window_waits_for_its_span_and_drops_old_seconds() {
        let mut t = Tapes::default();
        t.watch(1, Kind::Time, 10, 1_000);
        t.trade(1, 2_500, 100.0, true);
        t.trade(1, 2_900, 20.0, false);
        t.trade(1, 9_000, 10.0, false);
        assert_eq!(t.volumes(1, Kind::Time, 10, 9_000), None);
        assert_eq!(t.volumes(1, Kind::Time, 10, 11_000), Some((100.0, 30.0)));
        // Second 2 leaves the 10 s window at 12 s.
        assert_eq!(t.volumes(1, Kind::Time, 10, 12_000), Some((0.0, 10.0)));
        t.trade(1, 12_100, 5.0, true);
        assert_eq!(t.volumes(1, Kind::Time, 10, 12_100), Some((5.0, 10.0)));
    }

    #[test]
    fn retained_markets_only() {
        let mut t = Tapes::default();
        t.watch(1, Kind::Trades, 1, 0);
        t.watch(2, Kind::Trades, 1, 0);
        t.retain(|idx| idx == 2);
        t.trade(1, 0, 1.0, true);
        t.trade(2, 0, 1.0, true);
        assert_eq!(t.volumes(1, Kind::Trades, 1, 0), None);
        assert_eq!(t.volumes(2, Kind::Trades, 1, 0), Some((1.0, 0.0)));
    }
}
