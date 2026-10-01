//! One client session on the server: session keys, replay windows, reliable
//! (High) resend, Sliced send/receive, Ping/RTT/PMTU.
//!
//! The session never touches a socket: outgoing datagrams are pushed to
//! [`Session::out`] and drained by the server loop. Time is passed in as
//! monotonic milliseconds so the state machine is testable.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::crypto::{self, Aes128Gcm};
use crate::protocol::control::{PingFrame, PingTelemetry};
use crate::protocol::crypted::{self, CRYPTO_HEADER_SIZE};
use crate::protocol::handshake::{handshake_aad, Hello};
use crate::protocol::slicing::{self, SliceHeader, SlicingReceiver, SLICE_HEADER_SIZE};
use crate::protocol::slider::Slider;
use crate::protocol::Command;
use crate::{compression, MoonKey};

use super::wire::{pack_server_packet, SERVER_HDR_SIZE};
use super::Wire;

pub(crate) const COMPRESSED_FLAG: u8 = 0x80;
/// Client starts its crypted counter at `64*64/2 - 1`; the same start keeps our
/// pre-auth packets inside the client's reorder window.
const INITIAL_CRYPTED_MSG_COUNTER: u64 = 64 * 64 / 2 - 1;
const PING_INTERVAL_MS: i64 = 1000;
const HIGH_MAX_RETRIES: i32 = 15;
const SLICED_MAX_RETRIES: i32 = 30;
/// Sliced retry clock before the first RTT sample (client `UNKNOWN_RTT_SLICED_FLOOR_MS`).
const UNKNOWN_RTT_MS: i64 = 200;
/// Server tick (`Server::RECV_TIMEOUT`); the Sliced byte budget is per tick.
const TICK_MS: f64 = 5.0;
/// Sliced send rate, bytes/s: start and clamps mirror the client (`StartCanSendRate`).
const START_SEND_RATE: i32 = 2 * 1024 * 1024;
const MIN_SEND_RATE: i32 = 256 * 1024;
const MAX_SEND_RATE: i32 = 8 * 1024 * 1024;
const DEFAULT_PMTU: u16 = 508;
const PROBE_SIZES: [u16; 4] = [1400, 1200, 1000, 700];
const PROBE_TIMEOUT_MS: i64 = 1500;

/// Delphi `TDateTime` now (days since 1899-12-30), raw system clock.
pub fn delphi_now() -> f64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    crate::time::UNIX_EPOCH_AS_DELPHI_DAYS + d.as_secs_f64() / 86_400.0
}

struct PendingHigh {
    msg_num: u64,
    wire_cmd: u8,
    wire_data: Vec<u8>,
    last_sent: i64,
    retries_left: i32,
}

struct SentSliced {
    datagram_num: u16,
    slices: Vec<Vec<u8>>,
    acked: [u8; 32],
    /// Per-block last send time, 0 = never sent.
    piece_last_sent: Vec<i64>,
    sent_count: usize,
    /// Set by every ACK that brings new flags: progress restarts the budget.
    retries_left: i32,
    last_retry: i64,
}

impl SentSliced {
    fn is_acked(&self, block: usize) -> bool {
        self.acked[block / 8] & (1 << (block % 8)) != 0
    }
    fn fully_acked(&self) -> bool {
        (0..self.slices.len()).all(|b| self.is_acked(b))
    }
    /// Set union; `true` when at least one new block got acknowledged.
    fn merge_ack(&mut self, flags: [u8; 32]) -> bool {
        let mut changed = false;
        for (dst, src) in self.acked.iter_mut().zip(flags) {
            changed |= *dst | src != *dst;
            *dst |= src;
        }
        changed
    }
    /// Retransmit share of a finished datagram, percent.
    fn overhead_pct(&self) -> f64 {
        (self.sent_count as f64 / self.slices.len() as f64 - 1.0) * 100.0
    }
}

pub struct Session {
    wire: Arc<Wire>,
    pub(crate) client_id: u64,
    pub(crate) addr: SocketAddr,
    pub(crate) authorized: bool,
    pub(crate) session_rnd: [u8; 16],
    pub(crate) server_token: u64,
    pub(crate) peer_mix: u64,
    /// Server-side handshake counter; every accepted client value bumps it.
    pub(crate) mix_ts: u64,
    encode: Aes128Gcm,
    decode: Aes128Gcm,
    ack_session32: u32,
    recv_slider: Slider,
    recv_slicer: SlicingReceiver,
    crypt_msg_counter: u64,
    pending_high: Vec<PendingHigh>,
    sending: Vec<SentSliced>,
    send_datagram_num: u16,
    /// Sliced pacing: bytes/s, adapted once per ping from our own retransmit share.
    can_send_rate: i32,
    /// EMA of `SentSliced::overhead_pct` over finished datagrams.
    avg_over_heat: f64,
    /// A tick used ≥80 % of its budget since the last ping (rate is only
    /// re-evaluated when it actually limited us).
    used_send_limit: bool,
    /// Receiver counters (unique, duplicate blocks) at the last ping — RSQ is
    /// the unique share since then.
    rsq_base: (u64, u64),
    pub(crate) pmtu: u16,
    rtt_ms: i32,
    last_ping_sent: i64,
    pub(crate) last_recv: i64,
    probe: Option<(u16, i64)>,
    total_sent: u64,
    total_recv: u64,
    /// Datagrams to deliver to `addr`; drained by the server loop.
    pub(crate) out: Vec<Vec<u8>>,
}

/// Application-facing command decoded from one client datagram.
pub(crate) enum Delivered {
    Command { cmd: u8, payload: Vec<u8> },
    SessionClose,
}

impl Session {
    pub(crate) fn new(
        wire: Arc<Wire>,
        client_id: u64,
        addr: SocketAddr,
        master_key: &MoonKey,
        mac_key: &MoonKey,
        hello: &Hello,
    ) -> Self {
        let now = wire.now();
        let server_token = loop {
            let t: u64 = rand::random();
            if t != 0 {
                break t;
            }
        };
        // `generate_session_sub_keys` returns (client_encode, client_decode).
        let (client_encode, client_decode) =
            crypto::generate_session_sub_keys(master_key, client_id, server_token, &hello.rnd);
        Self {
            wire,
            client_id,
            addr,
            authorized: false,
            session_rnd: hello.rnd,
            server_token,
            peer_mix: rand::random(),
            mix_ts: hello.mix_ts.wrapping_add(1),
            encode: crypto::cipher_from_key(&client_decode),
            decode: crypto::cipher_from_key(&client_encode),
            ack_session32: crypto::ack_session32(mac_key, client_id, server_token, &hello.rnd),
            recv_slider: Slider::new(),
            recv_slicer: SlicingReceiver::new(),
            crypt_msg_counter: INITIAL_CRYPTED_MSG_COUNTER,
            pending_high: Vec::new(),
            sending: Vec::new(),
            send_datagram_num: 1,
            can_send_rate: START_SEND_RATE,
            avg_over_heat: 0.0,
            used_send_limit: false,
            rsq_base: (0, 0),
            pmtu: DEFAULT_PMTU,
            rtt_ms: 0,
            last_ping_sent: 0,
            last_recv: now,
            probe: None,
            total_sent: 0,
            total_recv: 0,
            out: Vec::new(),
        }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn is_authorized(&self) -> bool {
        self.authorized
    }

    fn hello(&self, rnd: [u8; 16], peer_mix: u64, app_token: u64) -> Hello {
        Hello {
            rnd,
            mix_ts: self.mix_ts,
            timestamp: delphi_now(),
            server_token: self.server_token,
            peer_mix,
            app_token,
        }
    }

    /// Accept a client counter value (`accepted_server_mix_ts` mirror).
    pub(crate) fn accept_mix_ts(&mut self, mix_ts: u64) {
        if (mix_ts.wrapping_sub(self.mix_ts) as i64) >= 0 {
            self.mix_ts = mix_ts.wrapping_add(1);
        }
    }

    // ----- handshake replies -------------------------------------------------

    pub(crate) fn who_are_you(&self, master_key: &MoonKey, app_token: u64) -> Vec<u8> {
        let hello = self.hello(self.session_rnd, self.peer_mix, app_token);
        let aad = handshake_aad(self.client_id, Command::WhoAreYou.to_byte());
        crypto::encrypt(master_key, &hello.to_bytes_packed(), &aad)
    }

    /// Decrypt `ImFriend` with the session key and check it binds to this handshake.
    pub(crate) fn accept_imfriend(&mut self, payload: &[u8]) -> bool {
        let aad = handshake_aad(self.client_id, Command::ImFriend.to_byte());
        let Some(plain) = crypto::decrypt_with_cipher(&self.decode, payload, &aad) else {
            return false;
        };
        let Some(hello) = Hello::from_bytes(&plain) else {
            return false;
        };
        if hello.rnd != self.session_rnd
            || hello.server_token != self.server_token
            || hello.peer_mix != self.peer_mix
        {
            return false;
        }
        self.accept_mix_ts(hello.mix_ts);
        true
    }

    /// `HelloAgain` proof for this session (client knows `server_token` and `session_rnd`).
    pub(crate) fn hello_again_matches(&self, hello: &Hello) -> bool {
        hello.server_token == self.server_token
            && hello.peer_mix
                == crypto::calculate_hello_again_peer_mix(
                    &hello.rnd,
                    hello.mix_ts,
                    self.server_token,
                    &self.session_rnd,
                )
    }

    /// Encrypted `Fine` carrying the client's current handshake `rnd`.
    pub(crate) fn fine(&self, rnd: [u8; 16], app_token: u64) -> Vec<u8> {
        let hello = self.hello(rnd, 0, app_token);
        let aad = handshake_aad(self.client_id, Command::Fine.to_byte());
        crypto::encrypt_with_cipher(&self.encode, &hello.to_bytes_packed(), &aad)
    }

    // ----- outbound ------------------------------------------------------------

    pub(crate) fn push_raw(&mut self, cmd: u8, payload: &[u8]) {
        let packet = pack_server_packet(&self.wire.mac_ctx, cmd, payload, self.wire.mask_ver);
        self.total_sent += packet.len() as u64;
        self.out.push(packet);
    }

    /// Nothing left to send: no packet queued, no reliable direct message
    /// waiting for its ACK, no sliced datagram in flight. A sliced one that
    /// ran out of retries counts as given up, not as delivered. A core leaving
    /// waits for this, or the last order images never reach the terminal.
    pub fn quiet(&self) -> bool {
        self.out.is_empty() && self.pending_high.is_empty() && self.sending.is_empty()
    }

    /// Plaintext command (public market feed and public API responses).
    pub fn send(&mut self, cmd: u8, payload: &[u8]) {
        let (cmd, data) = maybe_compress(cmd, payload);
        self.send_wire(cmd, &data);
    }

    /// AES-GCM command. `reliable` keeps it in the High queue until the client
    /// acknowledges the message number through its Ping bitmap.
    pub fn send_encrypted(&mut self, cmd: u8, payload: &[u8], reliable: bool) {
        if !self.authorized {
            return;
        }
        let now = self.wire.now();
        let (inner_cmd, data) = maybe_compress(cmd, payload);
        self.crypt_msg_counter += 1;
        let msg_num = self.crypt_msg_counter;
        let mut plain = Vec::with_capacity(CRYPTO_HEADER_SIZE + data.len());
        plain.extend_from_slice(&rand::random::<u16>().to_le_bytes());
        plain.extend_from_slice(&msg_num.to_le_bytes());
        plain.push(inner_cmd);
        plain.push(u8::from(reliable));
        plain.extend_from_slice(&data);
        let wire_data = crypto::encrypt_with_cipher(&self.encode, &plain, &[]);
        let wire_cmd = Command::Crypted.to_byte() | (inner_cmd & COMPRESSED_FLAG);
        let sliced = self.send_wire(wire_cmd, &wire_data);
        // Sliced datagrams have their own ACK/retry; only direct packets need High.
        if reliable && !sliced {
            self.pending_high.push(PendingHigh {
                msg_num,
                wire_cmd,
                wire_data,
                last_sent: now,
                retries_left: HIGH_MAX_RETRIES,
            });
        }
    }

    /// Direct when it fits the PMTU, otherwise queued as Sliced (blocks go out
    /// from `tick` under the byte budget). Returns `true` when sliced.
    fn send_wire(&mut self, cmd: u8, data: &[u8]) -> bool {
        if SERVER_HDR_SIZE + data.len() <= self.pmtu as usize {
            self.push_raw(cmd, data);
            return false;
        }
        let block = self.pmtu as usize - SERVER_HDR_SIZE - SLICE_HEADER_SIZE;
        let n_blocks = (data.len() + 1).div_ceil(block).max(1);
        if n_blocks > 256 {
            log::warn!(target: "moonproto::server", "payload {} too large to slice", data.len());
            return true;
        }
        let datagram_num = self.send_datagram_num;
        self.send_datagram_num = self.send_datagram_num.wrapping_add(1);
        let mut slices = Vec::with_capacity(n_blocks);
        let mut pos = 0;
        for b in 0..n_blocks {
            let mut s = Vec::with_capacity(SLICE_HEADER_SIZE + block);
            SliceHeader {
                datagram_num,
                block_num: b as u8,
                max_block_num: (n_blocks - 1) as u8,
            }
            .write_to(&mut s);
            let room = if b == 0 {
                s.push(cmd);
                block - 1
            } else {
                block
            };
            let take = room.min(data.len() - pos);
            s.extend_from_slice(&data[pos..pos + take]);
            pos += take;
            slices.push(s);
        }
        // Smaller datagrams go first (the client orders its queue the same way).
        let at = self
            .sending
            .iter()
            .position(|s| s.slices.len() > n_blocks)
            .unwrap_or(self.sending.len());
        self.sending.insert(
            at,
            SentSliced {
                datagram_num,
                slices,
                acked: [0; 32],
                piece_last_sent: vec![0; n_blocks],
                sent_count: 0,
                retries_left: SLICED_MAX_RETRIES,
                last_retry: 0,
            },
        );
        true
    }

    // ----- inbound -------------------------------------------------------------

    /// Route one authenticated client datagram. Handshake commands are handled
    /// by the server (they need the master key); everything else lands here.
    pub(crate) fn on_packet(
        &mut self,
        cmd: u8,
        payload: &[u8],
        recv_len: usize,
        delivered: &mut Vec<Delivered>,
    ) {
        let now = self.wire.now();
        self.last_recv = now;
        self.total_recv += recv_len as u64;
        self.recv_slicer.set_last_online(now);
        self.recv_slicer.do_cleanup();
        match Command::from_byte(cmd) {
            Command::Ping => self.on_ping(payload),
            Command::SlicedACK => self.on_sliced_ack(payload),
            Command::ProbeMTUAck => self.on_probe_ack(payload),
            Command::Sliced => {
                let (assembled, ack) = self
                    .recv_slicer
                    .on_new_sliced_with_session(payload, self.ack_session32);
                self.push_raw(Command::SlicedACK.to_byte(), &ack);
                if let Some((datagram_num, inner_cmd, data, _, _)) = assembled {
                    self.dispatch(inner_cmd, &data, delivered);
                    self.recv_slicer.receiving.remove(&datagram_num);
                }
            }
            Command::Grouped => {
                let mut pos = 0;
                while pos + 3 <= payload.len() {
                    let sub_cmd = payload[pos];
                    let sz = u16::from_le_bytes([payload[pos + 1], payload[pos + 2]]) as usize;
                    pos += 3;
                    if pos + sz > payload.len() {
                        break;
                    }
                    self.dispatch(sub_cmd, &payload[pos..pos + sz], delivered);
                    pos += sz;
                }
            }
            Command::SizeAck | Command::Echo | Command::EchoReply => {}
            _ => self.dispatch(cmd, payload, delivered),
        }
    }

    /// Delphi `DataReadInt`: Crypted unwrap, replay check, decompress, deliver.
    fn dispatch(&mut self, raw_cmd: u8, payload: &[u8], delivered: &mut Vec<Delivered>) {
        let mut cmd = raw_cmd;
        let mut data: Vec<u8>;
        let mut was_crypted = false;
        if Command::from_byte(cmd) == Command::Crypted {
            let Some((inner, plain, _want_ack)) =
                crypted::decrypt_command(&self.decode, payload, &mut self.recv_slider)
            else {
                if log::log_enabled!(target: "moonproto::server", log::Level::Trace) {
                    let dup = crypted::decrypt_command_no_replay(&self.decode, payload)
                        .map(|(h, _)| (h.cmd, h.msg_num));
                    log::trace!(
                        target: "moonproto::server",
                        "client {:#x}: crypted packet rejected ({} bytes, replay={:?}, window from {})",
                        self.client_id,
                        payload.len(),
                        dup,
                        self.recv_slider.start_num
                    );
                }
                return;
            };
            cmd = inner;
            data = plain;
            was_crypted = true;
        } else {
            data = payload.to_vec();
        }
        if cmd & COMPRESSED_FLAG != 0 {
            cmd &= 0x7F;
            let Some(d) = compression::mp_decompress(&data) else {
                return;
            };
            data = d;
        }
        match Command::from_byte(cmd) {
            Command::SessionClose if was_crypted => delivered.push(Delivered::SessionClose),
            Command::SessionClose | Command::None => {}
            // Sensitive families must have crossed AES-GCM.
            Command::Order | Command::UI | Command::Strat | Command::API | Command::Balance
                if !was_crypted => {}
            _ => delivered.push(Delivered::Command { cmd, payload: data }),
        }
    }

    fn on_ping(&mut self, payload: &[u8]) {
        let Some(ping) = PingFrame::read(payload) else {
            return;
        };
        let rtt = ((delphi_now() - ping.initial_time) * 86_400_000.0).round();
        if rtt.is_finite() && rtt >= 0.0 {
            self.rtt_ms = rtt.min(i32::MAX as f64) as i32;
        }
        if ping.ack_session != self.ack_session32 || payload.len() <= ping.ack_words_offset {
            return;
        }
        let ack_start = u64::from_le_bytes(payload[42..50].try_into().unwrap());
        let words = &payload[ping.ack_words_offset..];
        let mut slider = Slider::new();
        slider.start_num = ack_start;
        let r_count = (words.len() / 8).min(64);
        for i in 0..r_count {
            slider.bit_field[i] = u64::from_le_bytes(words[i * 8..i * 8 + 8].try_into().unwrap());
        }
        slider.r_count = r_count as i32;
        self.pending_high
            .retain(|h| !slider.ack_confirms_msg(h.msg_num));
    }

    fn on_sliced_ack(&mut self, payload: &[u8]) {
        let Some((flags, datagram_num, session)) = slicing::parse_ack_bytes(payload) else {
            return;
        };
        if session != self.ack_session32 {
            return;
        }
        let Some(idx) = self
            .sending
            .iter()
            .position(|s| s.datagram_num == datagram_num)
        else {
            return;
        };
        let s = &mut self.sending[idx];
        if !s.merge_ack(flags) {
            return;
        }
        // Progress proves the peer is alive: the retry budget starts over.
        s.retries_left = SLICED_MAX_RETRIES;
        if s.fully_acked() {
            let pct = s.overhead_pct();
            self.avg_over_heat = if self.avg_over_heat == 0.0 {
                pct
            } else {
                (self.avg_over_heat * 9.0 + pct) * 0.1
            };
            self.sending.remove(idx);
        }
    }

    fn on_probe_ack(&mut self, payload: &[u8]) {
        if payload.len() < 5 {
            return;
        }
        let received = u16::from_le_bytes([payload[3], payload[4]]);
        if received > self.pmtu {
            self.pmtu = received;
        }
        self.probe = None;
    }

    // ----- timers --------------------------------------------------------------

    /// Periodic work: Ping, PMTU probe, High and Sliced retries.
    pub(crate) fn tick(&mut self) {
        if !self.authorized {
            return;
        }
        let now = self.wire.now();
        if now - self.last_ping_sent >= PING_INTERVAL_MS {
            self.last_ping_sent = now;
            self.send_ping();
        }
        self.probe_mtu(now);
        let path_delay = ((self.rtt_ms as f64 * 1.1 + 10.0).round() as i64).clamp(200, 500);
        let mut i = 0;
        while i < self.pending_high.len() {
            let h = &mut self.pending_high[i];
            if now - h.last_sent > path_delay {
                h.last_sent = now;
                h.retries_left -= 1;
                if h.retries_left < 0 {
                    self.pending_high.remove(i);
                    continue;
                }
                let (cmd, data) = (h.wire_cmd, h.wire_data.clone());
                self.push_raw(cmd, &data);
            }
            i += 1;
        }
        self.flush_sliced(now);
    }

    /// One paced pass over the Sliced queue: first sends and retries of blocks
    /// older than the path delay, up to the per-tick byte budget. A datagram
    /// spends one retry per pass that retransmitted something; when the budget
    /// runs out it is dropped (an ACK with progress refills it).
    fn flush_sliced(&mut self, now: i64) {
        if self.sending.is_empty() {
            return;
        }
        let rtt = if self.rtt_ms <= 0 {
            UNKNOWN_RTT_MS
        } else {
            self.rtt_ms as i64
        };
        let path_delay = (rtt as f64 * 1.1 + 10.0).round() as i64;
        let limit = (self.can_send_rate as f64 * TICK_MS * 0.001).round() as usize;
        let mut sent = 0usize;
        let client_id = self.client_id;
        let Self {
            wire,
            sending,
            out,
            total_sent,
            ..
        } = self;
        let mut i = 0;
        while i < sending.len() && sent < limit {
            let s = &mut sending[i];
            let mut retried = false;
            for b in 0..s.slices.len() {
                if sent >= limit {
                    break;
                }
                let last = s.piece_last_sent[b];
                if s.is_acked(b) || (last != 0 && now - last <= path_delay) {
                    continue;
                }
                retried |= last != 0;
                s.piece_last_sent[b] = now;
                s.sent_count += 1;
                let packet = pack_server_packet(
                    &wire.mac_ctx,
                    Command::Sliced.to_byte(),
                    &s.slices[b],
                    wire.mask_ver,
                );
                sent += s.slices[b].len();
                *total_sent += packet.len() as u64;
                out.push(packet);
            }
            if retried && now - s.last_retry > path_delay {
                s.last_retry = now;
                s.retries_left -= 1;
                if s.retries_left < 0 {
                    log::debug!(
                        target: "moonproto::server",
                        "client {:#x}: sliced datagram {} dropped after {} retries",
                        client_id,
                        s.datagram_num,
                        SLICED_MAX_RETRIES
                    );
                    sending.remove(i);
                    continue;
                }
            }
            i += 1;
        }
        if sent as f64 >= limit as f64 * 0.8 {
            self.used_send_limit = true;
        }
    }

    /// Once per ping: re-evaluate the Sliced rate when the budget limited us
    /// (clean transfers → +3 %, ≥5 % retransmits → −15 %, client's rule).
    fn adapt_send_rate(&mut self) {
        if !self.used_send_limit {
            return;
        }
        self.used_send_limit = false;
        let rate = self.can_send_rate as f64;
        let next = if self.avg_over_heat >= 5.0 {
            rate * 0.85
        } else if self.avg_over_heat < 1.0 {
            rate + (rate * 0.03).max(32.0 * 1024.0)
        } else {
            rate
        };
        self.can_send_rate = (next.round() as i32).clamp(MIN_SEND_RATE, MAX_SEND_RATE);
    }

    /// Receive quality of the client's Sliced traffic since the last ping:
    /// unique blocks over all blocks, scaled to `u8` (255 = clean or idle). The
    /// client drives its own send rate from this value.
    fn receive_quality(&mut self) -> u8 {
        let p = self.recv_slicer.progress_snapshot();
        let (unique, dup) = (
            p.unique_blocks.wrapping_sub(self.rsq_base.0),
            p.duplicate_blocks.wrapping_sub(self.rsq_base.1),
        );
        self.rsq_base = (p.unique_blocks, p.duplicate_blocks);
        match unique + dup {
            0 => 255,
            total => (255.0 * unique as f64 / total as f64).round() as u8,
        }
    }

    fn send_ping(&mut self) {
        self.adapt_send_rate();
        let rsq = self.receive_quality();
        let now_dt = delphi_now();
        let (ack_start, words) = self.recv_slider.build_ack_half();
        let frame = PingFrame {
            time: now_dt,
            initial_time: now_dt,
            trip_delay: self.rtt_ms,
            pmtu: self.pmtu,
            global_timing_orders: 0,
            overheat: 0,
            rsq,
            ack_session: self.ack_session32,
            moment_cpu_percent: 0,
            total_cpu_percent: 0,
            memory: None,
            ack_words_offset: 0,
        };
        let mut payload = frame.response_bytes(
            now_dt,
            self.total_sent,
            self.total_recv,
            ack_start,
            self.ack_session32,
            PingTelemetry {
                moment_cpu_percent: 0,
                total_cpu_percent: 0,
                memory: None,
            },
        );
        for w in words {
            payload.extend_from_slice(&w.to_le_bytes());
        }
        self.push_raw(Command::Ping.to_byte(), &payload);
    }

    /// Probe PMTU top-down; the first acknowledged size wins, smaller ones are skipped.
    fn probe_mtu(&mut self, now: i64) {
        let next = match self.probe {
            Some((_, sent)) if now - sent < PROBE_TIMEOUT_MS => return,
            Some((size, _)) => PROBE_SIZES.iter().copied().find(|&s| s < size),
            None if self.pmtu > DEFAULT_PMTU => None,
            None => PROBE_SIZES.first().copied(),
        };
        let Some(size) = next.filter(|&s| s > self.pmtu) else {
            return;
        };
        self.probe = Some((size, now));
        // WireProbeMtu: probe_id u16, probe_index u8, test_size u16; padded to test_size.
        let mut payload = vec![0u8; size as usize - SERVER_HDR_SIZE];
        payload[0..2].copy_from_slice(&1u16.to_le_bytes());
        payload[2] = 0;
        payload[3..5].copy_from_slice(&size.to_le_bytes());
        self.push_raw(Command::ProbeMTU.to_byte(), &payload);
    }
}

fn maybe_compress(cmd: u8, data: &[u8]) -> (u8, std::borrow::Cow<'_, [u8]>) {
    if cmd & COMPRESSED_FLAG == 0 {
        if let Some(c) = compression::mp_compress(data) {
            return (cmd | COMPRESSED_FLAG, std::borrow::Cow::Owned(c));
        }
    }
    (cmd, std::borrow::Cow::Borrowed(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{transport_unpack, MacContext};
    use std::time::Instant;

    const MAC_KEY: [u8; 16] = [8; 16];

    fn session() -> Session {
        let wire = Arc::new(Wire {
            mac_ctx: MacContext::new(&MAC_KEY),
            mask_ver: 0,
            epoch: Instant::now(),
        });
        let hello = Hello::new(1, 2);
        let mut s = Session::new(
            wire,
            42,
            "127.0.0.1:1".parse().unwrap(),
            &[1u8; 16],
            &MAC_KEY,
            &hello,
        );
        s.authorized = true;
        s
    }

    /// Incompressible bytes so `send` takes the Sliced path.
    fn noise(len: usize) -> Vec<u8> {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (x >> 56) as u8
            })
            .collect()
    }

    /// Sliced slice bodies in `out`, drained.
    fn drain_slices(s: &mut Session) -> Vec<Vec<u8>> {
        s.out
            .drain(..)
            .filter_map(|p| {
                let (hdr, body) = transport_unpack(&MAC_KEY, &p, 0).expect("unpack");
                (hdr.cmd == Command::Sliced.to_byte()).then_some(body)
            })
            .collect()
    }

    fn ack(s: &mut Session, blocks: impl IntoIterator<Item = usize>) {
        let mut flags = [0u8; 32];
        for b in blocks {
            flags[b / 8] |= 1 << (b % 8);
        }
        let payload = slicing::build_ack_bytes(&flags, s.sending[0].datagram_num, s.ack_session32);
        s.on_sliced_ack(&payload);
    }

    #[test]
    fn sliced_send_is_paced_by_the_tick_budget() {
        let mut s = session();
        s.send(Command::API.to_byte(), &noise(30_000));
        let blocks = s.sending[0].slices.len();
        let limit = (START_SEND_RATE as f64 * TICK_MS * 0.001).round() as usize;
        assert!(blocks * 400 > limit, "payload must exceed one tick");

        s.flush_sliced(1000);
        let first = drain_slices(&mut s);
        let bytes: usize = first.iter().map(Vec::len).sum();
        assert!(first.len() < blocks);
        assert!(
            bytes >= limit && bytes < limit + 600,
            "budget {bytes} vs {limit}"
        );
        assert!(s.used_send_limit);

        // Unsent blocks follow on the next ticks regardless of the retry clock.
        let mut total = first.len();
        for t in 1..10 {
            s.flush_sliced(1000 + t);
            total += drain_slices(&mut s).len();
        }
        assert_eq!((total, s.sending[0].sent_count), (blocks, blocks));
        // Nothing is due before the path delay elapses.
        s.flush_sliced(1100);
        assert!(drain_slices(&mut s).is_empty());
    }

    #[test]
    fn ack_progress_refills_the_retry_budget() {
        let mut s = session();
        s.send(Command::API.to_byte(), &noise(900)); // two blocks at PMTU 508
        assert_eq!(s.sending[0].slices.len(), 2);
        s.flush_sliced(1000);
        assert_eq!(drain_slices(&mut s).len(), 2);
        assert_eq!(s.sending[0].retries_left, SLICED_MAX_RETRIES);

        let mut t = 1000;
        for round in 1..=3 {
            t += 231; // path delay at unknown RTT = 200 * 1.1 + 10
            s.flush_sliced(t);
            assert_eq!(drain_slices(&mut s).len(), 2);
            assert_eq!(s.sending[0].retries_left, SLICED_MAX_RETRIES - round);
        }
        ack(&mut s, [0]);
        assert_eq!(s.sending[0].retries_left, SLICED_MAX_RETRIES);
        // A repeated ACK without new blocks changes nothing.
        s.sending[0].retries_left = 5;
        ack(&mut s, [0]);
        assert_eq!(s.sending[0].retries_left, 5);

        t += 231;
        s.flush_sliced(t);
        assert_eq!(
            drain_slices(&mut s).len(),
            1,
            "only the unacked block is resent"
        );
        ack(&mut s, [0, 1]);
        assert!(s.sending.is_empty());
        // 2 blocks, 9 sends → 350 % retransmit overhead feeds the rate EMA.
        assert_eq!(s.avg_over_heat, 350.0);
        s.used_send_limit = true;
        s.adapt_send_rate();
        assert_eq!(
            s.can_send_rate,
            (START_SEND_RATE as f64 * 0.85).round() as i32
        );
    }

    #[test]
    fn retry_budget_exhaustion_drops_the_datagram() {
        let mut s = session();
        s.send(Command::API.to_byte(), &noise(600));
        let mut t = 1000;
        s.flush_sliced(t);
        for _ in 0..=SLICED_MAX_RETRIES {
            t += 231;
            s.flush_sliced(t);
        }
        assert!(s.sending.is_empty());
    }

    #[test]
    fn receive_quality_is_the_unique_block_share() {
        let mut s = session();
        assert_eq!(s.receive_quality(), 255, "idle link is clean");
        let mut slice = Vec::new();
        SliceHeader {
            datagram_num: 7,
            block_num: 0,
            max_block_num: 1,
        }
        .write_to(&mut slice);
        slice.extend_from_slice(&[1, 2, 3]);
        for _ in 0..3 {
            s.recv_slicer
                .on_new_sliced_with_session(&slice, s.ack_session32);
        }
        assert_eq!(s.receive_quality(), 85, "1 unique of 3 blocks");
        assert_eq!(s.receive_quality(), 255, "counters are deltas per ping");
    }
}
