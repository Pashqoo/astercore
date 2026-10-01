//! The core's own tape: every exchange print it heard, per market, for the
//! last few minutes.
//!
//! A deal's picture used to be drawn from the gateway's `GetLastTrades`
//! alone, and that reply is published late. Measured on VKCO, 30.09: the
//! picture was asked for at 10:33:52 MSK, twenty seconds after the deal
//! closed, and the reply ended at 10:32:5x — so the entry (10:33:32, on a
//! sweep of ~160 prints from 112.70 down to 112.45) was not in it at all, and
//! the chart drew eight prints of run-up, a `TAPE ENDS` marker, and nothing
//! where the deal was. Against 171 exchange prints in that window.
//!
//! The prints were in this process the whole time — the strike that opened
//! the deal was computed from them — so the picture now takes what the
//! gateway has published and this ring for everything after it. The seam is
//! the gateway's own last print, not the ring's first: that way neither side
//! can hide a print the other one had.

use std::collections::VecDeque;

use moonproto::server::codec::market_data::{delphi_days, HistoryTrade};

use crate::chart::unix_ms;

/// How far back one market's prints are kept. The seam above only closes
/// while this outlives the gateway's publication lag — ~1 minute measured on
/// 30.09, and three leaves room for a worse one. A deal older than this has
/// its run-up from the gateway, which by then has long published it.
const KEEP_MS: i64 = 3 * 60_000;

/// Prints one market may hold, whatever the clock says. Evicting here is not
/// free — it shortens the reach below, and the stretch between what the
/// gateway has published and what the ring still holds is then nobody's — so
/// the bound is set off the measured busiest three minutes of 30.09 morning:
/// SiZ6 1946 prints, VTBR 470, SBER 468. Four times the busiest, and 8192
/// prints are 128 KiB for the one or two markets that ever hold them.
const CAP: usize = 8192;

#[derive(Debug, Clone, Copy)]
struct Print {
    time_ms: i64,
    price: f32,
    /// Signed, in instrument units — negative is a sell, the same sign the
    /// chart's volume pane splits the layer on.
    qty: f32,
}

/// Recent prints per market index. Only markets the trades stream actually
/// delivers ever get a deque, so this is the subscribed pool, not the
/// catalog.
#[derive(Default)]
pub struct Tape {
    by_idx: Vec<VecDeque<Print>>,
}

impl Tape {
    /// One exchange print (a dealer print is not one — it is not on the chart
    /// the trader compares this picture with).
    ///
    /// `now_ms` is the wall clock, not the print's own stamp: a trade stamped
    /// in the future must not be able to age out everything behind it. Prints
    /// arrive in the order the stream batched them, which is time order bar
    /// the odd late one; nothing downstream depends on that, since the reader
    /// filters by the clock and the chart sorts what it is handed.
    pub fn push(&mut self, idx: u16, now_ms: i64, time_ms: i64, price: f32, qty: f32) {
        if !price.is_finite() || price <= 0.0 || !qty.is_finite() {
            return;
        }
        let i = usize::from(idx);
        if self.by_idx.len() <= i {
            self.by_idx.resize_with(i + 1, VecDeque::new);
        }
        let q = &mut self.by_idx[i];
        q.push_back(Print {
            time_ms,
            price,
            qty,
        });
        while q.len() > CAP {
            q.pop_front();
        }
        while q.front().is_some_and(|p| p.time_ms < now_ms - KEEP_MS) {
            q.pop_front();
        }
    }

    /// Every print of this market stamped later than `after_ms`, as the chart
    /// takes them. Empty when the market said nothing — or when the core was
    /// not listening to it, which to the picture is the same thing.
    fn after(&self, idx: u16, after_ms: i64) -> Vec<HistoryTrade> {
        self.by_idx
            .get(usize::from(idx))
            .map(|q| {
                q.iter()
                    .filter(|p| p.time_ms > after_ms)
                    .map(|p| HistoryTrade {
                        time: delphi_days(p.time_ms),
                        price: p.price,
                        qty: p.qty,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The oldest print this market still has here, by its own stamp rather
    /// than by its place: the stream batches prints in time order bar the odd
    /// late one, and the front of the deque is only usually the oldest.
    fn oldest(&self, idx: u16) -> Option<i64> {
        self.by_idx
            .get(usize::from(idx))?
            .iter()
            .map(|p| p.time_ms)
            .min()
    }

    /// The prints a deal's picture is drawn from: what the gateway has
    /// `published`, plus everything this ring heard after it, clipped to
    /// `from..=to`.
    ///
    /// The seam is the gateway's own last print and not the ring's first:
    /// taken the other way round, a market whose oldest prints the ring had
    /// already evicted would silently lose the stretch between the two.
    ///
    /// Which side owns the seam's own millisecond is the rest of it. A whole
    /// sweep can print inside one — the VKCO deal above opened on 160 prints
    /// of a single microsecond — so the cut has to fall between prints, never
    /// through a timestamp both sides hold a piece of. Where this ring was
    /// listening *strictly before* that millisecond it has it whole and takes
    /// it; where it was not — evicted, or the market subscribed into the
    /// middle of the sweep — the gateway keeps it and the ring adds only what
    /// comes after. At worst that leaves undrawn a print of that one
    /// millisecond only the ring heard; nothing is ever drawn twice, the two
    /// sides of the cut being disjoint.
    ///
    /// What the reach cannot see: a stream gap that ends inside the seam's
    /// own millisecond, and a dealer print, which the gateway's reply carries
    /// (`TRADE_SOURCE_ALL`) and this ring deliberately does not. Neither is
    /// reachable from the prints themselves; both cost that one millisecond
    /// and no more. On this venue dealer prints measured zero (30.09, TQBR
    /// and FORTS alike).
    ///
    /// `idx` is `None` for a market this core does not know: the gateway's
    /// reply then stands alone, the way it did before this ring existed.
    pub fn window(
        &self,
        idx: Option<u16>,
        published: &[HistoryTrade],
        from: i64,
        to: i64,
    ) -> Window {
        let seam = published
            .iter()
            .map(|t| unix_ms(t.time))
            .max()
            // Nothing published — or nothing came back at all: the ring
            // carries the window by itself.
            .unwrap_or(from - 1);
        // Strictly before: a ring whose own first print sits inside the seam's
        // millisecond heard only part of it, and handing it the whole would
        // drop the gateway's share of exactly the sweep this rule exists for.
        let reaches = idx
            .and_then(|i| self.oldest(i))
            .is_some_and(|oldest| oldest < seam);
        let cut = if reaches { seam - 1 } else { seam };
        let inside = |t: &HistoryTrade| (from..=to).contains(&unix_ms(t.time));
        let own: Vec<HistoryTrade> = idx
            .map(|i| self.after(i, cut))
            .unwrap_or_default()
            .into_iter()
            .filter(inside)
            .collect();
        Window {
            own: own.len(),
            prints: published
                .iter()
                .filter(|t| unix_ms(t.time) <= cut && inside(t))
                .cloned()
                .chain(own)
                .collect(),
        }
    }
}

/// What [`Tape::window`] found on both sides of the seam.
pub struct Window {
    /// Every print inside the deal's window, the gateway's first. Order is
    /// not time order and does not have to be: the chart sorts what it is
    /// handed.
    pub prints: Vec<HistoryTrade>,
    /// How many of them came from this ring rather than from the reply —
    /// which in the seam's own millisecond includes prints the gateway had
    /// published too, since that millisecond is taken from one side whole.
    pub own: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_759_217_612_000;

    fn stamps(rows: &[HistoryTrade]) -> Vec<i64> {
        rows.iter().map(|r| unix_ms(r.time)).collect()
    }

    #[test]
    fn the_seam_is_the_gateway_s_last_print() {
        let mut tape = Tape::default();
        for i in 0..5 {
            tape.push(7, T0 + i * 1_000, T0 + i * 1_000, 112.5, -10.0);
        }
        // The gateway published up to the third one; the picture wants the
        // two it has not, and neither of them twice.
        assert_eq!(
            stamps(&tape.after(7, T0 + 2_000)),
            vec![T0 + 3_000, T0 + 4_000]
        );
        // Nothing published at all: the ring carries the whole window.
        assert_eq!(tape.after(7, T0 - 60_000).len(), 5);
    }

    #[test]
    fn the_window_holds_both_sides_of_the_seam_and_nothing_twice() {
        let mut tape = Tape::default();
        let published = |n: i64| -> Vec<HistoryTrade> {
            (0..n)
                .map(|i| HistoryTrade {
                    time: delphi_days(T0 + i * 1_000),
                    price: 100.0,
                    qty: 1.0,
                })
                .collect()
        };
        // Ten seconds of prints in the core's ear; the gateway has published
        // the first four of them and nothing since.
        for i in 0..10 {
            tape.push(1, T0 + 9_000, T0 + i * 1_000, 100.0, 1.0);
        }
        let w = tape.window(Some(1), &published(4), T0 - 60_000, T0 + 60_000);
        assert_eq!(
            (w.prints.len(), w.own),
            (10, 7),
            "ten prints once each — the ring owns the seam's millisecond, so
             three come from the gateway and seven from here"
        );

        // The deal's window clips both sides alike.
        let w = tape.window(Some(1), &published(4), T0 + 5_000, T0 + 7_000);
        assert_eq!(
            stamps(&w.prints),
            vec![T0 + 5_000, T0 + 6_000, T0 + 7_000],
            "the published prints are all before it"
        );
        assert_eq!(w.own, 3);

        // A market this core does not know: the gateway's reply stands alone,
        // the way it did before the ring existed.
        let w = tape.window(None, &published(4), T0 - 60_000, T0 + 60_000);
        assert_eq!((w.prints.len(), w.own), (4, 0));

        // And nothing published at all — the reply was empty, or errored and
        // never came: the ring carries the whole window.
        let w = tape.window(Some(1), &[], T0 - 60_000, T0 + 60_000);
        assert_eq!((w.prints.len(), w.own), (10, 10));
    }

    #[test]
    fn a_sweep_inside_the_seam_s_own_millisecond_is_not_lost() {
        let mut tape = Tape::default();
        let print = |ms: i64, price: f32, qty: f32| HistoryTrade {
            time: delphi_days(ms),
            price,
            qty,
        };
        // The run-up, then a sweep of 160 prints in one millisecond — the
        // VKCO deal of 30.09. The gateway published the run-up and three of
        // the sweep, and its reply stops there.
        tape.push(1, T0, T0 - 1_000, 100.0, 1.0);
        for _ in 0..160 {
            tape.push(1, T0, T0, 99.0, -5.0);
        }
        let published = vec![
            print(T0 - 1_000, 100.0, 1.0),
            print(T0, 99.0, -5.0),
            print(T0, 99.0, -5.0),
            print(T0, 99.0, -5.0),
        ];
        let w = tape.window(Some(1), &published, T0 - 60_000, T0 + 60_000);
        assert_eq!(
            (w.prints.len(), w.own),
            (161, 160),
            "the whole sweep, once: the ring heard the millisecond through and owns it"
        );
    }

    #[test]
    fn the_gateway_keeps_its_last_millisecond_where_the_ring_starts_after_it() {
        let mut tape = Tape::default();
        // The ring's oldest print is later than the gateway's last one, so
        // the core was not listening when that one printed: handing the
        // millisecond to the ring would lose it for good.
        tape.push(1, T0, T0 + 1_000, 99.0, -5.0);
        let published = vec![HistoryTrade {
            time: delphi_days(T0),
            price: 100.0,
            qty: 1.0,
        }];
        let w = tape.window(Some(1), &published, T0 - 60_000, T0 + 60_000);
        assert_eq!((w.prints.len(), w.own), (2, 1));
    }

    #[test]
    fn a_ring_that_starts_inside_the_seam_s_millisecond_does_not_own_it() {
        let mut tape = Tape::default();
        // Subscribed into the middle of the sweep: the ring's first print is
        // the seam itself. It heard two of the millisecond's prints, the
        // gateway published another two, and neither side has all four — so
        // the gateway keeps its own and the ring adds only what came later.
        tape.push(1, T0, T0, 99.0, -5.0);
        tape.push(1, T0, T0, 99.0, -5.0);
        tape.push(1, T0, T0 + 1_000, 99.5, 5.0);
        let print = |ms: i64| HistoryTrade {
            time: delphi_days(ms),
            price: 99.0,
            qty: -5.0,
        };
        let w = tape.window(Some(1), &[print(T0), print(T0)], T0 - 60_000, T0 + 60_000);
        assert_eq!(
            (w.prints.len(), w.own),
            (3, 1),
            "the gateway's two and the one print after the seam"
        );
    }

    #[test]
    fn a_market_the_core_never_heard_has_no_prints() {
        let tape = Tape::default();
        assert!(tape.after(3, 0).is_empty(), "no deque, not a panic");
        let mut tape = Tape::default();
        tape.push(9, T0, T0, 1.0, 1.0);
        assert!(tape.after(4, 0).is_empty(), "a gap below the one it has");
    }

    #[test]
    fn a_print_older_than_the_window_is_dropped() {
        let mut tape = Tape::default();
        tape.push(1, T0, T0 - KEEP_MS - 1, 100.0, 1.0);
        tape.push(1, T0, T0 - KEEP_MS + 1, 100.0, 1.0);
        tape.push(1, T0, T0, 100.0, 1.0);
        assert_eq!(
            stamps(&tape.after(1, 0)),
            vec![T0 - KEEP_MS + 1, T0],
            "the stale one went, the one inside the window stayed"
        );
    }

    #[test]
    fn a_stamp_from_the_future_does_not_empty_the_ring() {
        let mut tape = Tape::default();
        tape.push(1, T0, T0 - 1_000, 100.0, 1.0);
        // A print stamped a day ahead: aged against the wall clock, it takes
        // nothing with it. Aged against its own stamp it would have taken the
        // whole market's tape.
        tape.push(1, T0, T0 + 86_400_000, 100.0, 1.0);
        assert_eq!(tape.after(1, 0).len(), 2);
    }

    #[test]
    fn the_ring_is_bounded_whatever_the_clock_says() {
        let mut tape = Tape::default();
        for i in 0..(CAP as i64 + 500) {
            // Every print inside the window: only the cap can bound this.
            tape.push(1, T0, T0 + i % 1_000, 100.0, 1.0);
        }
        assert_eq!(tape.after(1, 0).len(), CAP);
    }

    #[test]
    fn a_price_that_is_not_a_price_never_enters() {
        let mut tape = Tape::default();
        tape.push(1, T0, T0, f32::NAN, 1.0);
        tape.push(1, T0, T0, 0.0, 1.0);
        tape.push(1, T0, T0, f32::INFINITY, 1.0);
        tape.push(1, T0, T0, 100.0, f32::NAN);
        assert!(
            tape.after(1, 0).is_empty(),
            "the chart would have drawn them at the frame's edge"
        );
    }
}
