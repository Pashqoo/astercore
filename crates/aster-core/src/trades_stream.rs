//! Core-wide `TradesStream` packetizer: batches trades into numbered packets,
//! keeps recent packets for `TradesResend`, and emits empty heartbeats so the
//! terminal sees the stream as alive between trades.
//!
//! Ported from TInvestCore as is: the packet is MoonProto's, not the venue's.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use moonproto::server::codec::market_data::{
    delphi_days, trades_packet, trades_resend_response, TradeRow,
};

const FLUSH_AFTER: Duration = Duration::from_millis(20);
const FLUSH_ROWS: usize = 200;
/// Terminal treats >5 s without packets as a dead stream.
const HEARTBEAT: Duration = Duration::from_secs(1);
const RING_PACKETS: usize = 1024;

struct Pending {
    m_index: u16,
    time_ms: i64,
    price: f32,
    qty: f32,
}

pub struct TradesStream {
    pending: Vec<Pending>,
    first_pending_at: Instant,
    packet_num: u16,
    ring: VecDeque<(u16, Vec<u8>)>,
    last_sent: Instant,
    now_ms: fn() -> i64,
}

impl TradesStream {
    pub fn new(now_ms: fn() -> i64) -> Self {
        let now = Instant::now();
        Self {
            pending: Vec::new(),
            first_pending_at: now,
            packet_num: 0,
            ring: VecDeque::with_capacity(RING_PACKETS),
            last_sent: now,
            now_ms,
        }
    }

    /// Queue one trade; `qty` signed (negative = sell), already in units.
    pub fn push(&mut self, m_index: u16, time_ms: i64, price: f32, qty: f32) {
        if self.pending.is_empty() {
            self.first_pending_at = Instant::now();
        }
        self.pending.push(Pending {
            m_index,
            time_ms,
            price,
            qty,
        });
    }

    /// Next packet to broadcast when a batch or heartbeat is due.
    pub fn poll(&mut self, now: Instant) -> Option<Vec<u8>> {
        let due = if self.pending.is_empty() {
            now.duration_since(self.last_sent) >= HEARTBEAT
        } else {
            self.pending.len() >= FLUSH_ROWS
                || now.duration_since(self.first_pending_at) >= FLUSH_AFTER
        };
        if !due {
            return None;
        }
        let packet = self.build();
        self.last_sent = now;
        if self.ring.len() == RING_PACKETS {
            self.ring.pop_front();
        }
        self.ring.push_back((self.packet_num, packet.clone()));
        self.packet_num = self.packet_num.wrapping_add(1);
        Some(packet)
    }

    fn build(&mut self) -> Vec<u8> {
        let base_ms = self.pending.first().map_or_else(self.now_ms, |p| p.time_ms);
        self.pending.sort_by_key(|p| p.m_index);
        let rows: Vec<TradeRow> = self
            .pending
            .iter()
            .map(|p| TradeRow {
                time_delta_ms: (p.time_ms - base_ms).clamp(i16::MIN as i64, i16::MAX as i64) as i16,
                price: p.price,
                qty: p.qty,
            })
            .collect();
        let mut sections: Vec<(u16, &[TradeRow])> = Vec::new();
        let mut start = 0;
        for i in 1..=rows.len() {
            if i == rows.len() || self.pending[i].m_index != self.pending[start].m_index {
                sections.push((self.pending[start].m_index, &rows[start..i]));
                start = i;
            }
        }
        let packet = trades_packet(delphi_days(base_ms), self.packet_num, &sections);
        self.pending.clear();
        packet
    }

    /// `TradesResendResponse` body for the packets still in the ring.
    pub fn resend(&self, nums: &[u16]) -> Vec<u8> {
        let found: Vec<&[u8]> = nums
            .iter()
            .filter_map(|n| {
                self.ring
                    .iter()
                    .find(|(k, _)| k == n)
                    .map(|(_, p)| p.as_slice())
            })
            .collect();
        trades_resend_response(&found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_now() -> i64 {
        1_700_000_000_000
    }

    #[test]
    fn batches_by_market_numbers_packets_and_resends() {
        let mut s = TradesStream::new(fixed_now);
        let t0 = Instant::now();
        assert!(s.poll(t0).is_none(), "nothing due right after start");
        s.push(2, fixed_now(), 10.0, 1.0);
        s.push(1, fixed_now() + 5, 20.0, -2.0);
        s.push(2, fixed_now() + 7, 10.5, 3.0);
        assert!(s.poll(t0).is_none(), "batch window still open");
        let t0 = Instant::now();
        let p0 = s.poll(t0 + FLUSH_AFTER).expect("batch due");
        // base_time f64 + packet_num u16, then sections sorted by market index.
        assert_eq!(u16::from_le_bytes([p0[8], p0[9]]), 0);
        assert_eq!(u16::from_le_bytes([p0[10], p0[11]]), 1);
        assert_eq!(p0[12], 1);
        let sec2 = 13 + 10;
        assert_eq!(u16::from_le_bytes([p0[sec2], p0[sec2 + 1]]), 2);
        assert_eq!(p0[sec2 + 2], 2);
        assert_eq!(i16::from_le_bytes([p0[sec2 + 13], p0[sec2 + 14]]), 7);

        let hb = s.poll(t0 + FLUSH_AFTER + HEARTBEAT).expect("heartbeat");
        assert_eq!(hb.len(), 11, "empty packet: base_time + num + flags");
        assert_eq!(u16::from_le_bytes([hb[8], hb[9]]), 1);

        let resend = s.resend(&[1, 0, 99]);
        assert_eq!(resend[0], 2);
        assert_eq!(
            u16::from_le_bytes([resend[1], resend[2]]) as usize,
            hb.len()
        );
    }
}
