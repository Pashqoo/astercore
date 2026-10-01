//! Subscription planner: which chunk streams a wanted set of subscriptions
//! (books, candles) lives in, and which of them must reopen when the set
//! changes. Policy only — no threads, no sockets; `feed` opens and closes what
//! it says.
//!
//! Ported from TInvestCore unchanged in logic. There a chunk was a grpc-web
//! stream of up to 300 instruments; here it is a WebSocket session of up to
//! 200 streams (`aster::ws::MAX_STREAMS`, measured), and the chunk size is the
//! caller's argument, so nothing in this file had to move.
//!
//! A key keeps its chunk for as long as it stays subscribed, so one added key
//! reopens only the chunk it lands in, not every chunk after it. A key that
//! stops being wanted lingers for `grace_ms` before it leaves: the strategy
//! pool blinks at its volume boundary once a recompute, and a key back within
//! the grace costs no reopen at all. A key past its grace leaves with the next
//! change of the set (`settle`), so its chunk reopens once for the keys in and
//! out — the pool is recomputed on a grid, and a grace timer of its own would
//! reopen the chunk a second time next to every recompute. A set that stops
//! changing lets them go on its own at twice the grace (`expire`). Lingering is
//! bounded — past `linger_cap` the longest-lingering keys leave at once.

use std::collections::HashMap;
use std::hash::Hash;

pub struct Plan<K> {
    per_chunk: usize,
    grace_ms: i64,
    linger_cap: usize,
    /// Members per chunk; an emptied chunk keeps its index (its stream is
    /// closed) and takes the next key that needs a slot.
    chunks: Vec<Vec<K>>,
    chunk_of: HashMap<K, usize>,
    /// Members no longer wanted, with the time they stopped being wanted.
    lingering: HashMap<K, i64>,
}

/// What one `settle` changed.
pub struct Change<K> {
    /// Chunks whose membership changed, ascending: reopen each non-empty one,
    /// close each emptied one.
    pub chunks: Vec<usize>,
    /// Keys that left the subscription.
    pub dropped: Vec<K>,
}

impl<K: Clone + Eq + Hash> Plan<K> {
    pub fn new(per_chunk: usize, grace_ms: i64, linger_cap: usize) -> Self {
        assert!(per_chunk > 0);
        Self {
            per_chunk,
            grace_ms,
            linger_cap,
            chunks: Vec::new(),
            chunk_of: HashMap::new(),
            lingering: HashMap::new(),
        }
    }

    pub fn chunk(&self, i: usize) -> &[K] {
        &self.chunks[i]
    }

    /// Replace the wanted set at `now` (ms, any monotonic origin): keys no
    /// longer wanted start lingering, lingering ones past their grace leave,
    /// and then new keys take the first chunk with room — slots just freed
    /// count, so a full chunk that lost a key takes a new one without a new
    /// chunk opening.
    pub fn settle(&mut self, wanted: &[K], now: i64) -> Change<K> {
        let wanted_set: std::collections::HashSet<&K> = wanted.iter().collect();
        self.lingering.retain(|k, _| !wanted_set.contains(k));
        for k in self.chunk_of.keys() {
            if !wanted_set.contains(k) {
                self.lingering.entry(k.clone()).or_insert(now);
            }
        }
        let mut change = self.drop_lingering(now, self.grace_ms);
        for k in wanted {
            if self.chunk_of.contains_key(k) {
                continue;
            }
            let i = match self.chunks.iter().position(|c| c.len() < self.per_chunk) {
                Some(i) => i,
                None => {
                    self.chunks.push(Vec::new());
                    self.chunks.len() - 1
                }
            };
            self.chunks[i].push(k.clone());
            self.chunk_of.insert(k.clone(), i);
            change.chunks.push(i);
        }
        change.chunks.sort_unstable();
        change.chunks.dedup();
        change
    }

    /// No change of the set came: drop the lingering keys at twice their
    /// grace (`next_due`), and the oldest ones past `linger_cap`.
    pub fn expire(&mut self, now: i64) -> Change<K> {
        self.drop_lingering(now, 2 * self.grace_ms)
    }

    /// Drop the keys lingering `after` ms or longer at `now`, and the oldest
    /// ones past `linger_cap`.
    fn drop_lingering(&mut self, now: i64, after: i64) -> Change<K> {
        let mut by_age: Vec<(i64, K)> = self
            .lingering
            .iter()
            .map(|(k, &since)| (since, k.clone()))
            .collect();
        by_age.sort_by_key(|&(since, _)| since);
        let over_cap = by_age.len().saturating_sub(self.linger_cap);
        let mut chunks = Vec::new();
        let mut dropped = Vec::new();
        for (n, (since, k)) in by_age.into_iter().enumerate() {
            if n >= over_cap && now - since < after {
                break;
            }
            self.lingering.remove(&k);
            if let Some(i) = self.chunk_of.remove(&k) {
                self.chunks[i].retain(|m| m != &k);
                chunks.push(i);
            }
            dropped.push(k);
        }
        chunks.sort_unstable();
        chunks.dedup();
        Change { chunks, dropped }
    }

    /// When `expire` drops the next lingering key, if any lingers.
    pub fn next_due(&self) -> Option<i64> {
        self.lingering
            .values()
            .min()
            .map(|since| since + 2 * self.grace_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(r: std::ops::Range<u32>) -> Vec<u32> {
        r.collect()
    }

    #[test]
    fn an_added_key_reopens_only_the_chunk_it_lands_in() {
        let mut p = Plan::new(3, 1_000, 10);
        let c = p.settle(&keys(0..5), 0);
        assert_eq!(c.chunks, [0, 1]);
        assert_eq!(p.chunk(0), [0, 1, 2]);
        assert_eq!(p.chunk(1), [3, 4]);
        // A new key sorting first does not shift anyone: only chunk 1 has room.
        let c = p.settle(&[99, 0, 1, 2, 3, 4], 10);
        assert_eq!(c.chunks, [1]);
        assert!(c.dropped.is_empty());
        assert_eq!(p.chunk(0), [0, 1, 2]);
        assert_eq!(p.chunk(1), [3, 4, 99]);
        let c = p.settle(&[99, 0, 1, 2, 3, 4], 20);
        assert!(c.chunks.is_empty(), "the same set changes nothing");
    }

    #[test]
    fn a_removed_key_lingers_for_the_grace_and_a_return_costs_nothing() {
        let mut p = Plan::new(3, 1_000, 10);
        p.settle(&keys(0..3), 0);
        let c = p.settle(&[0, 1], 100);
        assert!(c.chunks.is_empty() && c.dropped.is_empty());
        assert_eq!(p.next_due(), Some(2_100));
        let c = p.settle(&keys(0..3), 900);
        assert!(c.chunks.is_empty(), "back within the grace");
        assert_eq!(p.next_due(), None);
        p.settle(&[0, 1], 1_000);
        // Past the grace the key waits for the next change of the set: one
        // reopen for the key in and the key out.
        assert!(p.expire(2_000).chunks.is_empty());
        let c = p.settle(&[0, 1, 5], 2_000);
        assert_eq!((c.chunks, c.dropped), (vec![0], vec![2]));
        assert_eq!(p.chunk(0), [0, 1, 5]);
        assert_eq!(p.next_due(), None);
    }

    #[test]
    fn a_set_that_stops_changing_drops_its_lingering_keys_at_twice_the_grace() {
        let mut p = Plan::new(3, 1_000, 10);
        p.settle(&keys(0..3), 0);
        p.settle(&[0, 1], 100);
        assert_eq!(p.next_due(), Some(2_100));
        assert!(p.expire(2_099).chunks.is_empty());
        let c = p.expire(2_100);
        assert_eq!((c.chunks, c.dropped), (vec![0], vec![2]));
    }

    #[test]
    fn a_freed_slot_is_reused_before_a_new_chunk_opens() {
        let mut p = Plan::new(2, 0, 10);
        p.settle(&keys(0..4), 0);
        let c = p.settle(&[1, 2, 3], 0);
        assert_eq!((c.chunks, c.dropped), (vec![0], vec![0]));
        let c = p.settle(&[1, 2, 3, 7], 0);
        assert_eq!(c.chunks, [0]);
        assert_eq!(p.chunk(0), [1, 7]);
        // An emptied chunk keeps its index and is closed.
        let c = p.settle(&[1, 7], 0);
        assert_eq!(c.chunks, [1]);
        assert!(p.chunk(1).is_empty());
        let c = p.settle(&[1, 7, 8], 0);
        assert_eq!(c.chunks, [1]);
        assert_eq!(p.chunk(1), [8]);
    }

    #[test]
    fn lingering_past_the_cap_drops_the_oldest_at_once() {
        let mut p = Plan::new(10, 60_000, 2);
        p.settle(&keys(0..5), 0);
        p.settle(&keys(1..5), 10);
        p.settle(&keys(2..5), 20);
        let c = p.settle(&keys(3..5), 30);
        assert_eq!(c.dropped, [0], "three linger, the cap is two");
        assert_eq!(p.chunk(0), [1, 2, 3, 4]);
        assert_eq!(p.next_due(), Some(120_020), "key 1 lingers since 20");
    }
}
