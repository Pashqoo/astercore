//! Server side of MoonProto for Astercore.
//!
//! Everything under `server/` is Astercore code layered on the vendored
//! upstream client crate: encoders for server-to-client packets, parsers for
//! client-to-server ones, the UDP session, and key export. Upstream files are
//! never edited; `tools/check-vendor.sh` enforces that.

pub mod codec;
pub mod key_export;
pub mod session;
mod wire;

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::crypto;
use crate::protocol::handshake::{handshake_aad, Hello};
pub use crate::protocol::Command;
use crate::transport::MacContext;
use crate::MoonKey;

use key_export::ServerKey;
use session::Delivered;
pub use session::Session;
use wire::Inbound;

/// Transport parameters shared by all sessions.
pub struct Wire {
    pub(crate) mac_ctx: MacContext,
    pub(crate) mask_ver: u8,
    epoch: Instant,
}

impl Wire {
    pub(crate) fn now(&self) -> i64 {
        self.epoch.elapsed().as_millis() as i64
    }
}

/// Application callbacks. Sessions passed in are authorized.
pub trait Handler {
    /// Transport authorized (`Fine` sent) for the first time in this session.
    fn on_connected(&mut self, session: &mut Session);
    /// One decoded command (Crypted/compressed already unwrapped).
    fn on_command(&mut self, session: &mut Session, cmd: u8, payload: &[u8]);
    fn on_closed(&mut self, client_id: u64);
}

const RECV_TIMEOUT: Duration = Duration::from_millis(5);
const SESSION_IDLE_MS: i64 = 60_000;

pub struct Server<H: Handler> {
    socket: UdpSocket,
    wire: Arc<Wire>,
    master_key: MoonKey,
    mac_key: MoonKey,
    app_token: u64,
    sessions: HashMap<u64, Session>,
    handler: H,
    delivered: Vec<Delivered>,
    buf: Box<[u8; 65535]>,
}

impl<H: Handler> Server<H> {
    pub fn bind(key: &ServerKey, handler: H) -> io::Result<Self> {
        let socket = UdpSocket::bind(("0.0.0.0", key.port))?;
        socket.set_read_timeout(Some(RECV_TIMEOUT))?;
        Ok(Self {
            socket,
            wire: Arc::new(Wire {
                mac_ctx: MacContext::new(&key.mac_key),
                mask_ver: key.transport_mode.to_byte(),
                epoch: Instant::now(),
            }),
            master_key: key.master_key,
            mac_key: key.mac_key,
            app_token: rand::random(),
            sessions: HashMap::new(),
            handler,
            delivered: Vec::new(),
            buf: Box::new([0u8; 65535]),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn sessions_mut(&mut self) -> impl Iterator<Item = &mut Session> {
        self.sessions.values_mut().filter(|s| s.authorized)
    }

    /// Handler and authorized sessions at once, for pushes driven from outside
    /// the receive path (the handler queues into sessions, `step` flushes).
    pub fn split(&mut self) -> (&mut H, impl Iterator<Item = &mut Session>) {
        (
            &mut self.handler,
            self.sessions.values_mut().filter(|s| s.authorized),
        )
    }

    /// Run until `stop` is set.
    pub fn run(&mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            self.step();
        }
    }

    /// One receive (bounded by `RECV_TIMEOUT`) plus timers and flush.
    pub fn step(&mut self) {
        match self.socket.recv_from(&mut self.buf[..]) {
            Ok((n, from)) => {
                let datagram = self.buf[..n].to_vec();
                self.on_datagram(&datagram, from);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(e) => log::warn!(target: "moonproto::server", "recv: {e}"),
        }
        let now = self.wire.now();
        let mut dead = Vec::new();
        for (&id, s) in self.sessions.iter_mut() {
            if now - s.last_recv > SESSION_IDLE_MS {
                dead.push(id);
                continue;
            }
            s.tick();
        }
        for id in dead {
            self.close(id);
        }
        self.flush();
    }

    fn flush(&mut self) {
        for s in self.sessions.values_mut() {
            for packet in s.out.drain(..) {
                if let Err(e) = self.socket.send_to(&packet, s.addr) {
                    log::debug!(target: "moonproto::server", "send_to {}: {e}", s.addr);
                }
            }
        }
    }

    fn close(&mut self, client_id: u64) {
        if let Some(s) = self.sessions.remove(&client_id) {
            if s.authorized {
                self.handler.on_closed(client_id);
            }
        }
    }

    fn on_datagram(&mut self, datagram: &[u8], from: SocketAddr) {
        let (cmd, client_id, payload) = match wire::unpack_client_packet(
            &self.wire.mac_ctx,
            datagram,
            self.wire.mask_ver,
        ) {
            Some(Inbound::Packet {
                cmd,
                client_id,
                payload,
            }) => (cmd, client_id, payload),
            Some(Inbound::DnsWarmup) => {
                let _ = self.socket.send_to(wire::dns_warmup_response(), from);
                return;
            }
            None => {
                log::debug!(target: "moonproto::server", "unpack failed: {} bytes from {from}", datagram.len());
                return;
            }
        };
        match Command::from_byte(cmd) {
            Command::Hello => self.on_hello(client_id, &payload, from),
            Command::ImFriend => self.on_imfriend(client_id, &payload, from),
            Command::HelloAgain => self.on_hello_again(client_id, &payload, from),
            _ => {
                let Some(s) = self.sessions.get_mut(&client_id) else {
                    log::debug!(target: "moonproto::server", "cmd {cmd} from unknown client {client_id:#x}");
                    return;
                };
                if !s.authorized {
                    log::debug!(target: "moonproto::server", "cmd {cmd} from unauthorized client {client_id:#x}");
                    return;
                }
                if s.addr != from {
                    log::debug!(target: "moonproto::server", "client {client_id:#x} moved {} -> {from}", s.addr);
                }
                s.addr = from;
                s.on_packet(cmd, &payload, datagram.len(), &mut self.delivered);
                let delivered = std::mem::take(&mut self.delivered);
                for d in delivered {
                    match d {
                        Delivered::Command { cmd, payload } => {
                            if let Some(s) = self.sessions.get_mut(&client_id) {
                                self.handler.on_command(s, cmd, &payload);
                            }
                        }
                        Delivered::SessionClose => self.close(client_id),
                    }
                }
            }
        }
    }

    fn decode_hello(&self, client_id: u64, cmd: Command, payload: &[u8]) -> Option<Hello> {
        let aad = handshake_aad(client_id, cmd.to_byte());
        Hello::from_bytes(&crypto::decrypt(&self.master_key, payload, &aad)?)
    }

    fn on_hello(&mut self, client_id: u64, payload: &[u8], from: SocketAddr) {
        let Some(hello) = self.decode_hello(client_id, Command::Hello, payload) else {
            log::debug!(target: "moonproto::server", "Hello from {client_id:#x}: undecodable");
            return;
        };
        log::debug!(target: "moonproto::server", "Hello from {client_id:#x} {from}");
        // A retried Hello for the handshake in flight must not rotate the token.
        let reuse = self
            .sessions
            .get(&client_id)
            .is_some_and(|s| !s.authorized && s.session_rnd == hello.rnd);
        if !reuse {
            let was_authorized = self.sessions.get(&client_id).is_some_and(|s| s.authorized);
            if was_authorized {
                self.handler.on_closed(client_id);
            }
            let session = Session::new(
                Arc::clone(&self.wire),
                client_id,
                from,
                &self.master_key,
                &self.mac_key,
                &hello,
            );
            self.sessions.insert(client_id, session);
        }
        let s = self.sessions.get_mut(&client_id).expect("inserted");
        s.addr = from;
        let reply = s.who_are_you(&self.master_key, self.app_token);
        s.push_raw(Command::WhoAreYou.to_byte(), &reply);
    }

    fn on_imfriend(&mut self, client_id: u64, payload: &[u8], from: SocketAddr) {
        let Some(s) = self.sessions.get_mut(&client_id) else {
            return;
        };
        if !s.accept_imfriend(payload) {
            log::debug!(target: "moonproto::server", "ImFriend from {client_id:#x} rejected");
            return;
        }
        s.addr = from;
        let first = !s.authorized;
        s.authorized = true;
        let fine = s.fine(s.session_rnd, self.app_token);
        s.push_raw(Command::Fine.to_byte(), &fine);
        if first {
            self.handler.on_connected(s);
        }
    }

    fn on_hello_again(&mut self, client_id: u64, payload: &[u8], from: SocketAddr) {
        let Some(hello) = self.decode_hello(client_id, Command::HelloAgain, payload) else {
            log::debug!(target: "moonproto::server", "HelloAgain from {client_id:#x}: undecodable");
            return;
        };
        if let Some(s) = self
            .sessions
            .get_mut(&client_id)
            .filter(|s| s.hello_again_matches(&hello))
        {
            log::debug!(target: "moonproto::server", "HelloAgain from {client_id:#x}: session resumed");
            s.addr = from;
            s.accept_mix_ts(hello.mix_ts);
            let first = !s.authorized;
            s.authorized = true;
            let fine = s.fine(hello.rnd, self.app_token);
            s.push_raw(Command::Fine.to_byte(), &fine);
            if first {
                self.handler.on_connected(s);
            }
            return;
        }
        // Unknown session (server restart): ask for a fresh Hello.
        log::debug!(target: "moonproto::server", "HelloAgain from {client_id:#x}: no session, WantNewHello");
        let reply = Hello {
            rnd: hello.rnd,
            mix_ts: hello.mix_ts.wrapping_add(1),
            timestamp: session::delphi_now(),
            server_token: 0,
            peer_mix: 0,
            app_token: self.app_token,
        };
        let aad = handshake_aad(client_id, Command::WantNewHello.to_byte());
        let packet = wire::pack_server_packet(
            &self.wire.mac_ctx,
            Command::WantNewHello.to_byte(),
            &crypto::encrypt(&self.master_key, &reply.to_bytes_packed(), &aad),
            self.wire.mask_ver,
        );
        let _ = self.socket.send_to(&packet, from);
    }
}

#[cfg(test)]
mod tests;
