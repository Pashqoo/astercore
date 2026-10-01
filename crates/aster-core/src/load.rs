//! What the dynamic subscriptions cost, counted where it is paid: the
//! subscription sets the feed applied and the chunk sessions they reopened, the
//! book losses the UDP loop saw, and the feed-event batches that filled. One
//! `load:` line beside the `streams:` summary, per period; rendering zeroes
//! the counters, so a line reads as a rate.
//!
//! Ported from TInvestCore, trimmed: no strategy passes or pools (M3) and no
//! trading-status streams (Aster has none — status rides `exchangeInfo`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters of one summary period, shared between the feed thread and the UDP
/// loop. A pair of counters is two atomics, not one; a render landing between
/// the two increments splits a pair across two lines. Tolerated, as in
/// TInvestCore — the numbers are read as a rate over hours.
#[derive(Default)]
pub struct Load {
    batches_full: AtomicU64,
    book_sets: AtomicU64,
    book_chunks: AtomicU64,
    candle_sets: AtomicU64,
    candle_chunks: AtomicU64,
    books_off: AtomicU64,
    books_off_markets: AtomicU64,
    book_snapshots: AtomicU64,
    book_gaps: AtomicU64,
}

impl Load {
    pub fn batch_full(&self) {
        self.batches_full.fetch_add(1, Ordering::Relaxed);
    }

    /// The book subscription changed and `chunks` sessions reopened or closed.
    pub fn books_reopened(&self, chunks: usize) {
        self.book_sets.fetch_add(1, Ordering::Relaxed);
        self.book_chunks.fetch_add(chunks as u64, Ordering::Relaxed);
    }

    /// The candle subscription changed and `chunks` sessions reopened or closed.
    pub fn candles_reopened(&self, chunks: usize) {
        self.candle_sets.fetch_add(1, Ordering::Relaxed);
        self.candle_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
    }

    /// A book session ended and the books of `markets` were dropped.
    pub fn books_off(&self, markets: usize) {
        self.books_off.fetch_add(1, Ordering::Relaxed);
        self.books_off_markets
            .fetch_add(markets as u64, Ordering::Relaxed);
    }

    /// A book asked for a REST snapshot (`gap`: its update chain broke).
    pub fn book_snapshot(&self, gap: bool) {
        self.book_snapshots.fetch_add(1, Ordering::Relaxed);
        if gap {
            self.book_gaps.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The line for the period; zeroes the counters.
    pub fn summary(&self) -> String {
        let take = |c: &AtomicU64| c.swap(0, Ordering::Relaxed);
        format!(
            "load: batch full {} · books {} sets / {} chunks · \
             candles {} sets / {} chunks · books off {} sessions / {} markets · \
             book snapshots {} ({} gaps)",
            take(&self.batches_full),
            take(&self.book_sets),
            take(&self.book_chunks),
            take(&self.candle_sets),
            take(&self.candle_chunks),
            take(&self.books_off),
            take(&self.books_off_markets),
            take(&self.book_snapshots),
            take(&self.book_gaps),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_period_reports_its_own_counts_and_starts_over() {
        let load = Load::default();
        load.batch_full();
        load.books_reopened(2);
        load.books_reopened(3);
        load.candles_reopened(1);
        load.books_off(4);
        load.book_snapshot(false);
        load.book_snapshot(true);
        assert_eq!(
            load.summary(),
            "load: batch full 1 · books 2 sets / 5 chunks · \
             candles 1 sets / 1 chunks · books off 1 sessions / 4 markets · \
             book snapshots 2 (1 gaps)"
        );
        assert_eq!(
            load.summary(),
            "load: batch full 0 · books 0 sets / 0 chunks · \
             candles 0 sets / 0 chunks · books off 0 sessions / 0 markets · \
             book snapshots 0 (0 gaps)"
        );
    }
}
