//! One market's order book, kept from Aster's diff stream the way MoonBot keeps
//! Binance's: a REST snapshot of 1000 levels a side, stitched to the
//! `<symbol>@depth@100ms` events by their update ids.
//!
//! The top-20 stream (`depth20`) needed none of this, and was dropped for it:
//! measured 01.10 it spans ±0.02 % of BTCUSDT, a hairline on a terminal zoomed
//! to ±10 %. `limit=1000` is the most `/fapi/v1/depth` returns (5000 is refused
//! with -1130) and spans −2.3 / +3.4 % of BTCUSDT; MoonBot's own binary asks
//! for `&limit=1000` beside its `@depth@100ms`.
//!
//! The stitch, measured on Aster 01.10 (the Binance futures rules, verbatim):
//! update ids are one sequence across all symbols, the snapshot's
//! `lastUpdateId` falls INSIDE one event's `[U, u]`, and every next event's
//! `pu` is the previous one's `u` (0 breaks in 72 events). So: events are
//! buffered until the snapshot is in; those with `u < lastUpdateId` are
//! dropped; the first one applied must straddle it; a `pu` that is not the
//! last `u` is a gap, and a gap is a new snapshot.
//!
//! What goes to the terminal is shaped by how the client applies a diff
//! (`moonproto` `state/order_books/apply.rs`, ported from MoonBot's Delphi):
//! each side's levels are merged in book order, a zero quantity removes a
//! level, and then the side is CUT at the first level of the OTHER side's diff
//! — every bid at or above the first ask of the diff goes, every ask at or
//! below the first bid. A removed level that the price has crossed would cut
//! live levels there, so [`LocalBook::wire_diff`] leaves it out (the cut
//! removes it on the client anyway) and always leads each side with its
//! current best level, which puts the cut exactly at the touch.

use std::collections::BTreeMap;

use moonproto::server::codec::market_data::Level;

/// Events held while a snapshot is on its way: a minute of a busy
/// `@depth@100ms`. Past it the buffer is dropped, not the request: the
/// snapshot in flight still lands, and the stitch rules ask again if it is
/// older than what follows.
const BUFFER_CAP: usize = 600;
/// A snapshot that failed is asked again no sooner than this.
pub const SNAPSHOT_RETRY_MS: i64 = 5_000;

/// One `depthUpdate`: `U`, `u`, `pu` and the changed levels (a zero quantity
/// removes one).
#[derive(Debug, Clone, PartialEq)]
pub struct Diff {
    pub first_id: i64,
    pub last_id: i64,
    pub prev_id: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// `GET /fapi/v1/depth`: the book as of `last_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub last_id: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// What the UDP loop does after a step.
#[derive(Debug, PartialEq)]
pub enum Out {
    Nothing,
    /// Ask the feed for a snapshot (`gap`: the book was live and broke).
    AskSnapshot {
        gap: bool,
    },
    /// Send the whole book.
    Full,
    /// Send these levels, already in the client's order.
    Diff {
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
}

#[derive(Debug)]
enum State {
    /// No usable book: events wait here for a snapshot. `asked` is a request
    /// in flight; `retry_at` holds the next one off after a failure.
    Waiting {
        buffer: Vec<Diff>,
        asked: bool,
        retry_at: i64,
    },
    /// A snapshot as of `id` is in, and no event has reached past it yet.
    Fresh { id: i64 },
    /// Applied through `id`; the next event must say `pu == id`.
    Live { id: i64 },
}

/// A price as a map key. Only positive finite prices get here, and for those
/// the bit pattern orders as the number does — exact, with no tick scale to
/// pick per market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Px(u64);

impl Px {
    fn of(price: f64) -> Option<Self> {
        (price.is_finite() && price > 0.0).then(|| Self(price.to_bits()))
    }

    fn price(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(Debug)]
pub struct LocalBook {
    state: State,
    bids: BTreeMap<Px, f64>,
    asks: BTreeMap<Px, f64>,
}

impl Default for LocalBook {
    fn default() -> Self {
        Self {
            state: State::Waiting {
                buffer: Vec::new(),
                asked: false,
                retry_at: 0,
            },
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}

impl LocalBook {
    /// The book has been synced at least once: what it holds is worth
    /// answering `RequestOrderBookFull` with, even while a gap is re-stitched.
    pub fn has_book(&self) -> bool {
        !self.bids.is_empty() || !self.asks.is_empty()
    }

    /// Bids best first, then asks best first: the order of a full packet.
    pub fn levels(&self) -> (Vec<Level>, Vec<Level>) {
        (
            self.bids.iter().rev().map(level).collect(),
            self.asks.iter().map(level).collect(),
        )
    }

    pub fn on_diff(&mut self, diff: Diff, now_ms: i64) -> Out {
        match &mut self.state {
            State::Waiting {
                buffer,
                asked,
                retry_at,
            } => {
                if buffer.len() >= BUFFER_CAP {
                    buffer.clear();
                }
                buffer.push(diff);
                if *asked || now_ms < *retry_at {
                    return Out::Nothing;
                }
                *asked = true;
                Out::AskSnapshot { gap: false }
            }
            State::Fresh { id } => {
                let id = *id;
                if diff.last_id < id {
                    return Out::Nothing;
                }
                if diff.first_id > id {
                    return self.restart(diff);
                }
                self.state = State::Live { id: diff.last_id };
                self.apply_out(&diff)
            }
            State::Live { id } => {
                if diff.prev_id != *id {
                    return self.restart(diff);
                }
                self.state = State::Live { id: diff.last_id };
                self.apply_out(&diff)
            }
        }
    }

    /// A snapshot came back. `Err` waits [`SNAPSHOT_RETRY_MS`] and asks again
    /// on a later event, and blanks the book meanwhile: a failure that lasts
    /// (a 418 pauses snapshots for minutes) must not leave the terminal
    /// drawing a frozen book as live — the rule of the price rows, where no
    /// quote beats an old one.
    pub fn on_snapshot(&mut self, snap: Result<Snapshot, String>, now_ms: i64) -> Out {
        let State::Waiting { buffer, .. } = &mut self.state else {
            // Not waiting for one (a late answer to a request a gap already
            // superseded): the book in hand is newer.
            return Out::Nothing;
        };
        let snap = match snap {
            Ok(s) => s,
            Err(_) => {
                let buffer = std::mem::take(buffer);
                self.state = State::Waiting {
                    buffer,
                    asked: false,
                    retry_at: now_ms + SNAPSHOT_RETRY_MS,
                };
                if !self.has_book() {
                    return Out::Nothing;
                }
                self.bids.clear();
                self.asks.clear();
                return Out::Full;
            }
        };
        let buffer = std::mem::take(buffer);
        self.bids = side(&snap.bids);
        self.asks = side(&snap.asks);
        self.state = State::Fresh { id: snap.last_id };
        for diff in buffer {
            match &self.state {
                State::Fresh { id } if diff.last_id < *id => continue,
                State::Fresh { id } if diff.first_id > *id => return self.restart(diff),
                State::Fresh { .. } => {}
                State::Live { id } if diff.prev_id != *id => return self.restart(diff),
                State::Live { .. } => {}
                State::Waiting { .. } => unreachable!("set to Fresh above"),
            }
            self.state = State::Live { id: diff.last_id };
            self.apply(&diff);
        }
        Out::Full
    }

    /// The chain broke at `diff`: keep it as the first of the next stitch and
    /// ask for a snapshot. The levels stay — stale, but the last book there was.
    fn restart(&mut self, diff: Diff) -> Out {
        self.state = State::Waiting {
            buffer: vec![diff],
            asked: true,
            retry_at: 0,
        };
        Out::AskSnapshot { gap: true }
    }

    fn apply(&mut self, diff: &Diff) {
        merge(&mut self.bids, &diff.bids);
        merge(&mut self.asks, &diff.asks);
    }

    fn apply_out(&mut self, diff: &Diff) -> Out {
        self.apply(diff);
        self.wire_diff(diff)
    }

    /// The diff as the client must receive it to end up with this book (see
    /// the module notes). A side left empty cannot cut the client's stale
    /// levels — the cut needs a level to cut at — so that rare case is a full
    /// book instead.
    fn wire_diff(&self, diff: &Diff) -> Out {
        let best_bid = self.bids.keys().next_back().copied();
        let best_ask = self.asks.keys().next().copied();
        let (Some(bb), Some(ba)) = (best_bid, best_ask) else {
            return Out::Full;
        };
        let mut bids = BTreeMap::new();
        for &(p, q) in &diff.bids {
            let Some(k) = Px::of(p) else { continue };
            if q > 0.0 || k < ba {
                bids.insert(k, self.bids.get(&k).copied().unwrap_or(0.0));
            }
        }
        bids.insert(bb, self.bids[&bb]);
        let mut asks = BTreeMap::new();
        for &(p, q) in &diff.asks {
            let Some(k) = Px::of(p) else { continue };
            if q > 0.0 || k > bb {
                asks.insert(k, self.asks.get(&k).copied().unwrap_or(0.0));
            }
        }
        asks.insert(ba, self.asks[&ba]);
        Out::Diff {
            bids: bids.iter().rev().map(level).collect(),
            asks: asks.iter().map(level).collect(),
        }
    }
}

fn level((k, q): (&Px, &f64)) -> Level {
    Level {
        price: k.price() as f32,
        qty: *q as f32,
    }
}

fn side(rows: &[(f64, f64)]) -> BTreeMap<Px, f64> {
    let mut m = BTreeMap::new();
    merge(&mut m, rows);
    m
}

/// A row with a price that is not a positive number, or a quantity that is not
/// a number, is a decode default and is skipped; a zero quantity removes.
fn merge(side: &mut BTreeMap<Px, f64>, rows: &[(f64, f64)]) {
    for &(p, q) in rows {
        let Some(k) = Px::of(p) else { continue };
        if !q.is_finite() {
            continue;
        }
        if q > 0.0 {
            side.insert(k, q);
        } else {
            side.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diff(u0: i64, u1: i64, pu: i64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> Diff {
        Diff {
            first_id: u0,
            last_id: u1,
            prev_id: pu,
            bids: bids.to_vec(),
            asks: asks.to_vec(),
        }
    }

    fn snap(id: i64) -> Snapshot {
        Snapshot {
            last_id: id,
            bids: vec![(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)],
            asks: vec![(101.0, 1.0), (102.0, 2.0), (103.0, 3.0)],
        }
    }

    fn prices(levels: &[Level]) -> Vec<(f32, f32)> {
        levels.iter().map(|l| (l.price, l.qty)).collect()
    }

    /// The client's apply, transcribed from `moonproto`
    /// `state/order_books/apply.rs` (merge, then cut at the other side's first
    /// diff level), so the wire diff is checked against what the terminal
    /// will actually hold. The loopback test checks the same through the real
    /// client.
    fn client_apply(book: &mut Vec<Level>, diff: &[Level], shrink: &[Level], buy: bool) {
        if diff.is_empty() {
            return;
        }
        let old = std::mem::take(book);
        let mut k = 0;
        for d in diff {
            while k < old.len()
                && (if buy {
                    old[k].price > d.price
                } else {
                    old[k].price < d.price
                })
            {
                book.push(old[k]);
                k += 1;
            }
            if d.qty > 0.0 {
                book.push(*d);
            }
            if k < old.len() && old[k].price == d.price {
                k += 1;
            }
        }
        book.extend_from_slice(&old[k..]);
        if let Some(cut) = shrink.iter().find(|l| l.price > 0.0).map(|l| l.price) {
            book.retain(|l| if buy { l.price < cut } else { l.price > cut });
        }
    }

    fn synced() -> LocalBook {
        let mut b = LocalBook::default();
        assert_eq!(
            b.on_diff(diff(5, 9, 4, &[], &[]), 0),
            Out::AskSnapshot { gap: false }
        );
        assert_eq!(b.on_snapshot(Ok(snap(10)), 0), Out::Full);
        b
    }

    #[test]
    fn events_wait_for_the_snapshot_and_the_one_that_straddles_it_goes_first() {
        let mut b = LocalBook::default();
        assert_eq!(
            b.on_diff(diff(1, 5, 0, &[(100.0, 9.0)], &[]), 0),
            Out::AskSnapshot { gap: false }
        );
        // Asked once, not once per event.
        assert_eq!(
            b.on_diff(diff(6, 12, 5, &[(99.0, 7.0)], &[]), 0),
            Out::Nothing
        );
        assert_eq!(
            b.on_diff(diff(13, 14, 12, &[(98.0, 0.0)], &[]), 0),
            Out::Nothing
        );
        // As of 10: the first event is older and dropped, the second straddles.
        assert_eq!(b.on_snapshot(Ok(snap(10)), 0), Out::Full);
        let (bids, _) = b.levels();
        assert_eq!(prices(&bids), vec![(100.0, 1.0), (99.0, 7.0)]);
        // And the chain goes on from 14.
        assert!(matches!(
            b.on_diff(diff(15, 15, 14, &[(97.0, 1.0)], &[]), 0),
            Out::Diff { .. }
        ));
    }

    #[test]
    fn a_snapshot_with_nothing_buffered_waits_for_the_straddling_event() {
        let mut b = synced();
        // Older than the snapshot: dropped, still fresh.
        assert_eq!(
            b.on_diff(diff(7, 9, 6, &[(100.0, 5.0)], &[]), 0),
            Out::Nothing
        );
        assert!(matches!(
            b.on_diff(diff(10, 11, 9, &[(100.0, 5.0)], &[]), 0),
            Out::Diff { .. }
        ));
        assert_eq!(prices(&b.levels().0)[0], (100.0, 5.0));
    }

    #[test]
    fn a_broken_chain_asks_again_and_keeps_the_last_book_meanwhile() {
        let mut b = synced();
        assert!(matches!(
            b.on_diff(diff(10, 11, 9, &[], &[(101.0, 4.0)]), 0),
            Out::Diff { .. }
        ));
        // pu 12 is not the last u (11): an event was lost.
        assert_eq!(
            b.on_diff(diff(13, 14, 12, &[], &[]), 0),
            Out::AskSnapshot { gap: true }
        );
        assert!(b.has_book());
        assert_eq!(b.on_diff(diff(15, 15, 14, &[], &[]), 0), Out::Nothing);
        assert_eq!(b.on_snapshot(Ok(snap(14)), 0), Out::Full);
    }

    #[test]
    fn a_snapshot_older_than_every_buffered_event_is_asked_again() {
        let mut b = LocalBook::default();
        b.on_diff(diff(20, 25, 19, &[], &[]), 0);
        assert_eq!(
            b.on_snapshot(Ok(snap(10)), 0),
            Out::AskSnapshot { gap: true }
        );
    }

    #[test]
    fn a_failed_snapshot_is_asked_again_only_after_the_pause() {
        let mut b = LocalBook::default();
        b.on_diff(diff(1, 2, 0, &[], &[]), 0);
        assert_eq!(b.on_snapshot(Err("429".into()), 1_000), Out::Nothing);
        assert_eq!(b.on_diff(diff(3, 4, 2, &[], &[]), 2_000), Out::Nothing);
        assert_eq!(
            b.on_diff(diff(5, 6, 4, &[], &[]), 1_000 + SNAPSHOT_RETRY_MS),
            Out::AskSnapshot { gap: false }
        );
        // And the events kept meanwhile still stitch.
        assert_eq!(b.on_snapshot(Ok(snap(3)), 0), Out::Full);
    }

    #[test]
    fn a_failed_snapshot_blanks_a_book_that_broke() {
        let mut b = synced();
        b.on_diff(diff(12, 13, 11, &[], &[]), 0);
        assert_eq!(b.on_snapshot(Err("418".into()), 0), Out::Full);
        assert!(!b.has_book());
        // Already blank: nothing more to send.
        b.on_diff(diff(14, 15, 13, &[], &[]), SNAPSHOT_RETRY_MS);
        assert_eq!(
            b.on_snapshot(Err("418".into()), SNAPSHOT_RETRY_MS),
            Out::Nothing
        );
    }

    #[test]
    fn a_full_buffer_is_dropped_without_asking_twice() {
        let mut b = LocalBook::default();
        assert_eq!(
            b.on_diff(diff(1, 1, 0, &[], &[]), 0),
            Out::AskSnapshot { gap: false }
        );
        for i in 2..=(BUFFER_CAP as i64 + 5) {
            assert_eq!(b.on_diff(diff(i, i, i - 1, &[], &[]), 0), Out::Nothing);
        }
        // The answer to the one request still stitches what came after it.
        let last = BUFFER_CAP as i64 + 5;
        assert_eq!(b.on_snapshot(Ok(snap(last - 1)), 0), Out::Full);
        assert!(matches!(
            b.on_diff(diff(last + 1, last + 1, last, &[], &[]), 0),
            Out::Diff { .. }
        ));
    }

    #[test]
    fn a_late_snapshot_does_not_overwrite_a_live_book() {
        let mut b = synced();
        assert_eq!(b.on_snapshot(Ok(snap(5)), 0), Out::Nothing);
    }

    #[test]
    fn garbage_rows_are_skipped_and_zero_removes() {
        let mut b = synced();
        b.on_diff(
            diff(
                10,
                11,
                9,
                &[(0.0, 5.0), (f64::NAN, 1.0), (99.0, 0.0), (98.0, f64::NAN)],
                &[],
            ),
            0,
        );
        assert_eq!(prices(&b.levels().0), vec![(100.0, 1.0), (98.0, 3.0)]);
    }

    /// The case the client's cut makes dangerous: the price runs up through
    /// the asks, which are removed, and new bids appear where they were. Sent
    /// as the exchange said it, the removed ask at 101 would cut the client's
    /// new bids at 101 and 102.
    #[test]
    fn the_client_ends_up_with_this_book_when_the_price_crosses() {
        let mut b = synced();
        let (mut cb, mut ca) = b.levels();
        let steps = [
            diff(
                10,
                11,
                9,
                &[(101.0, 4.0), (102.0, 1.0)],
                &[(101.0, 0.0), (102.0, 0.0), (104.0, 2.0)],
            ),
            diff(
                12,
                12,
                11,
                &[(102.0, 0.0), (101.0, 0.0), (100.0, 0.0)],
                &[(99.5, 1.0), (103.0, 0.0)],
            ),
            diff(13, 13, 12, &[(99.0, 0.5)], &[]),
        ];
        for d in steps {
            let Out::Diff { bids, asks } = b.on_diff(d, 0) else {
                panic!("a diff");
            };
            client_apply(&mut cb, &bids, &asks, true);
            client_apply(&mut ca, &asks, &bids, false);
            let (eb, ea) = b.levels();
            assert_eq!(prices(&cb), prices(&eb));
            assert_eq!(prices(&ca), prices(&ea));
        }
    }

    #[test]
    fn a_side_emptied_by_a_diff_goes_out_whole() {
        let mut b = synced();
        assert_eq!(
            b.on_diff(
                diff(10, 10, 9, &[(100.0, 0.0), (99.0, 0.0), (98.0, 0.0)], &[]),
                0
            ),
            Out::Full
        );
    }
}
