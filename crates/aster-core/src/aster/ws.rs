//! Aster's streams over WebSocket: one session at a time, read on the calling
//! thread — the market's combined streams ([`run`]) and the account's
//! user-data stream ([`run_at`]).
//!
//! The shape is TInvestCore's `tinvest/stream.rs` without the grpc-web: a
//! session is opened for a FIXED set of streams named in the URL
//! (`/stream?streams=a@trade/b@depth@100ms`), read until it ends, and the
//! owner reopens it. Naming the set in the URL rather than sending `SUBSCRIBE`
//! keeps the exchange's "10 incoming messages a second" limit out of the
//! picture entirely, and a changed set is a new session — the same rule the
//! T-Invest streams had.
//!
//! Measured 01.10, and every number below rests on it: a session takes at
//! most **200 streams** (the 201st is refused with close code 3003, "subscribed
//! channels exceeds limit"), 200 `aggTrade` names (`trade` ones are shorter) make a 3658-byte URL that
//! the gateway takes while the whole catalog in one URL is refused with
//! HTTP 414, the handshake costs ~0.8 s, and the gateway closes a connection
//! after 24 hours on its own.
//!
//! Liveness is the age of the last frame ([`Beat`]), not the session state: a
//! thread blocked in a read does not see its own silence, and one that died
//! stops beating by itself (`stream_health`).

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tungstenite::Message;

use super::json::{Envelope, StreamData};

pub const HOST: &str = "fstream.asterdex.com";
/// Streams one session may carry — the gateway's limit, measured.
pub const MAX_STREAMS: usize = 200;
/// How long a session lives before the core reopens it on purpose.
///
/// Half the gateway's own 24 h, so the core's rotation always comes first and
/// a close is never the exchange's surprise. Phased per stream
/// ([`Beat::rotate_at_phase`]) so streams opened together do not go blind
/// together. Cost of a rotation: one handshake, ~0.8 s of one chunk.
pub const STREAM_SESSION: Duration = Duration::from_secs(12 * 3600);
/// A rotation slot nearer than this to the session's open is skipped: a
/// stream reopened off its slot (an error, a reconnect) does not reopen again
/// at once.
const MIN_SESSION_MS: i64 = 30_000;
const NO_PHASE: i64 = -1;
/// TCP connect and handshake bound. The handshake measured 0.82 s.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How often a blocked read wakes to look at `stop` and the rotation clock.
/// It is also the worst case for a replaced chunk to let go of its socket.
const READ_POLL: Duration = Duration::from_millis(500);
/// The core pings the gateway this often, so that every live session hears at
/// least a pong this often — whatever its streams carry.
///
/// Without it a session has no lower bound on its frame rate: the gateway's own
/// ping comes every 5 minutes, and a quiet market's tape or book can say
/// nothing for longer. A connection that died without a FIN — a NAT entry
/// dropped, the Wi-Fi switched, the Mac asleep — then looks exactly like a
/// quiet one, and nothing but the 12 h rotation would ever reopen it.
/// Measured 01.10: the gateway answers a client ping with a pong in 0.26 s,
/// on a stream of three non-trading markets that carried no data at all.
const PING_EVERY: Duration = Duration::from_secs(30);
/// A session that has heard nothing at all — no data, no pong — for this long
/// is dead and is reopened. Two missed pongs and change.
pub const IDLE_LIMIT: Duration = Duration::from_secs(75);

#[derive(Debug)]
pub enum Error {
    Connect(String),
    /// The gateway closed the session, with its code and reason when it gave
    /// them (3003 is the stream-count refusal).
    Closed(String),
    Transport(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(e) => write!(f, "connect: {e}"),
            Self::Closed(e) => write!(f, "closed by the gateway: {e}"),
            Self::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

/// A moment on two clocks. The wall clock alone lets a dead stream look young
/// once it is set back; the monotonic one alone stops while macOS sleeps. An
/// age is the larger of the two, so either failure reads as old. Ported from
/// TInvestCore as is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// Unix ms.
    pub wall: i64,
    /// ms since the first stamp in this process.
    pub mono: i64,
}

impl Stamp {
    pub fn now() -> Self {
        static BASE: OnceLock<Instant> = OnceLock::new();
        Self {
            wall: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
            mono: BASE.get_or_init(Instant::now).elapsed().as_millis() as i64,
        }
    }

    /// ms from `earlier` to `self`.
    pub fn since(self, earlier: Stamp) -> i64 {
        (self.wall - earlier.wall).max(self.mono - earlier.mono)
    }

    pub fn plus(self, ms: i64) -> Self {
        Self {
            wall: self.wall + ms,
            mono: self.mono + ms,
        }
    }
}

/// The last frame a stream received: the stream thread writes it, the UDP
/// loop judges liveness by its age. Starts at creation, so a stream still
/// opening counts as alive for one staleness window. Also counts the sessions
/// the stream opened (the `streams:` line) and carries its rotation phase.
#[derive(Debug, Clone)]
pub struct Beat(Arc<[AtomicI64; 5]>);

impl Beat {
    pub fn new() -> Self {
        let beat = Self(Arc::new(std::array::from_fn(|_| AtomicI64::new(0))));
        beat.0[3].store(NO_PHASE, Ordering::Relaxed);
        beat.touch();
        beat
    }

    pub fn touch(&self) {
        let now = Stamp::now();
        self.0[0].store(now.wall, Ordering::Relaxed);
        self.0[1].store(now.mono, Ordering::Relaxed);
    }

    pub fn at(&self) -> Stamp {
        Stamp {
            wall: self.0[0].load(Ordering::Relaxed),
            mono: self.0[1].load(Ordering::Relaxed),
        }
    }

    /// A frame of DATA arrived (a text frame — not a pong or a ping, which `touch` also takes).
    /// What «this session heard something» is judged on.
    pub fn touch_data(&self) {
        self.0[4].store(Stamp::now().mono, Ordering::Relaxed);
    }

    /// Whether a data frame arrived after `since` (monotonic clock; a wall-clock step cannot
    /// fake it).
    pub fn heard_data_since(&self, since: Stamp) -> bool {
        self.0[4].load(Ordering::Relaxed) > since.mono
    }

    /// A session opened. Not a frame: the beat stays as the last frame left it, or a gateway
    /// that accepts the handshake and says nothing would keep the stream «alive» through
    /// every reconnect, each shorter than the staleness bound.
    fn opened(&self) {
        self.0[2].fetch_add(1, Ordering::Relaxed);
    }

    /// Sessions opened since the stream was created.
    pub fn sessions(&self) -> i64 {
        self.0[2].load(Ordering::Relaxed)
    }

    /// Rotate the stream's sessions at `phase_ms` of each [`STREAM_SESSION`]
    /// cycle of the wall clock.
    pub fn rotate_at_phase(&self, phase_ms: i64) {
        let cycle = STREAM_SESSION.as_millis() as i64;
        self.0[3].store(phase_ms.rem_euclid(cycle), Ordering::Relaxed);
    }

    /// How long a session opening at `now` (unix ms) may last: to the next
    /// rotation slot at least `MIN_SESSION_MS` away, [`STREAM_SESSION`]
    /// without a phase.
    fn session(&self, now: i64) -> Duration {
        let phase = self.0[3].load(Ordering::Relaxed);
        if phase == NO_PHASE {
            return STREAM_SESSION;
        }
        let cycle = STREAM_SESSION.as_millis() as i64;
        let earliest = now + MIN_SESSION_MS;
        let slot = earliest + (phase - earliest).rem_euclid(cycle);
        Duration::from_millis((slot - now) as u64)
    }
}

impl Default for Beat {
    fn default() -> Self {
        Self::new()
    }
}

/// The combined-stream URL for these stream names.
pub fn url(streams: &[String]) -> String {
    format!("wss://{HOST}/stream?streams={}", streams.join("/"))
}

/// Read one session of `streams` until the gateway ends it, the rotation slot
/// comes, or `stop` is set. Every decoded frame goes to `on_frame` whole — an
/// `!…@arr` frame is one snapshot of the catalog and is handed on as one;
/// every frame, pings included, touches `beat`. `Ok(())` means "reopen when
/// still wanted"; `Err` is a real failure the caller backs off on.
///
/// Pings are answered by `tungstenite` itself on the next read, which is why
/// the read loop never sleeps longer than [`READ_POLL`]: the gateway pings
/// every 5 minutes and wants the pong within 15. The core pings too
/// ([`PING_EVERY`]), and a session that hears nothing for `idle` ends with an
/// error, so a connection that died silently is reopened within `idle` rather
/// than at the next rotation.
pub fn run(
    streams: &[String],
    idle: Duration,
    stop: &AtomicBool,
    beat: &Beat,
    mut on_frame: impl FnMut(StreamData),
) -> Result<(), Error> {
    assert!(
        !streams.is_empty() && streams.len() <= MAX_STREAMS,
        "a session carries 1..={MAX_STREAMS} streams"
    );
    run_at(
        HOST,
        url(streams),
        idle,
        stop,
        beat,
        || {},
        |text| {
            match serde_json::from_str::<Envelope>(text) {
                Ok(env) => on_frame(env.data),
                // One frame lost, not the session: the next one is a whole
                // snapshot for every stream this core reads except the tape, and a
                // frame that does not decode is a format change the journal must
                // name.
                Err(e) => log::warn!(
                    "ws: undecodable frame ({e}): {}",
                    text.chars().take(160).collect::<String>()
                ),
            }
        },
    )
}

/// [`run`] for any `url` on `host`, every text frame handed on as it came:
/// the account's user-data stream (`/ws/<listenKey>`), whose frames are bare
/// events rather than combined-stream envelopes, on the signer's network, and
/// whose reader must hear of a frame even when it does not decode.
pub fn run_at(
    host: &str,
    url: String,
    idle: Duration,
    stop: &AtomicBool,
    beat: &Beat,
    on_open: impl FnOnce(),
    mut on_text: impl FnMut(&str),
) -> Result<(), Error> {
    // Every address the name resolves to, in turn: a dual-stack host whose IPv6 does not route
    // would otherwise fail every session on its first address for good (REST tries them all).
    let addrs: Vec<_> = (host, 443)
        .to_socket_addrs()
        .map_err(|e| Error::Connect(format!("resolve {host}: {e}")))?
        .collect();
    if addrs.is_empty() {
        return Err(Error::Connect(format!("resolve {host}: no address")));
    }
    let mut last = String::new();
    let mut connected = None;
    for addr in &addrs {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        match TcpStream::connect_timeout(addr, CONNECT_TIMEOUT) {
            Ok(tcp) => {
                connected = Some(tcp);
                break;
            }
            Err(e) => last = format!("{addr}: {e}"),
        }
    }
    let tcp = connected.ok_or(Error::Connect(last))?;
    // A second handle on the same socket, so the read timeout can be moved
    // from the handshake's bound to the poll period once TLS owns the first.
    let knob = tcp.try_clone().map_err(|e| Error::Connect(e.to_string()))?;
    let set_timeout = |t: Duration| {
        knob.set_read_timeout(Some(t))
            .and_then(|()| knob.set_write_timeout(Some(CONNECT_TIMEOUT)))
            .map_err(|e| Error::Connect(e.to_string()))
    };
    set_timeout(CONNECT_TIMEOUT)?;
    let (mut socket, _) =
        tungstenite::client_tls(url, tcp).map_err(|e| Error::Connect(e.to_string()))?;
    set_timeout(READ_POLL)?;
    beat.opened();
    on_open();
    let deadline = Instant::now() + beat.session(Stamp::now().wall);

    let mut heard = Instant::now();
    // The first ping goes out at once: its pong is the first sign of life of a session that
    // opens on a quiet stream, and the beat no longer takes the handshake for one.
    let mut pinged = Instant::now()
        .checked_sub(PING_EVERY)
        .unwrap_or_else(Instant::now);
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        if heard.elapsed() >= idle {
            return Err(Error::Transport(format!(
                "silent for {} s, reopening",
                heard.elapsed().as_secs()
            )));
        }
        if pinged.elapsed() >= PING_EVERY {
            pinged = Instant::now();
            socket
                .send(Message::Ping(Vec::new().into()))
                .map_err(|e| Error::Transport(format!("ping: {e}")))?;
        }
        let read = socket.read();
        if read.is_ok() {
            heard = Instant::now();
        }
        match read {
            Ok(Message::Text(text)) => {
                beat.touch();
                beat.touch_data();
                on_text(text.as_str());
            }
            Ok(Message::Close(frame)) => {
                return Err(Error::Closed(frame.map_or_else(
                    || "no reason".into(),
                    |f| format!("{} {}", f.code, f.reason),
                )));
            }
            Ok(_) => beat.touch(),
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                return Err(Error::Closed("connection closed".into()));
            }
            Err(e) => return Err(Error::Transport(e.to_string())),
        }
    }
    // Polite, best effort: the session is over either way.
    let _ = socket.close(None);
    let _ = socket.flush();
    Ok(())
}

/// One session with a panic turned into an error, so the owner's loop backs
/// off and reopens instead of its thread dying: a dead thread stops beating
/// and its markets stay stale until the core restarts. Ported from
/// TInvestCore as is.
pub fn guarded(session: impl FnOnce() -> Result<(), Error>) -> Result<(), Error> {
    panic::catch_unwind(AssertUnwindSafe(session)).unwrap_or_else(|payload| {
        let what = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string payload".into());
        Err(Error::Transport(format!("panic in session: {what}")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_phased_session_ends_at_its_slot_and_never_sooner_than_the_minimum() {
        let beat = Beat::new();
        assert_eq!(beat.session(0), STREAM_SESSION, "no phase: a whole cycle");
        let cycle = STREAM_SESSION.as_millis() as i64;
        beat.rotate_at_phase(60_000);
        assert_eq!(beat.session(0).as_millis() as i64, 60_000);
        // Opened 10 s before its slot: the slot is skipped, the next one taken.
        assert_eq!(
            beat.session(50_000).as_millis() as i64,
            cycle + 10_000,
            "a slot nearer than the minimum is the next cycle's"
        );
    }

    #[test]
    fn the_url_names_every_stream() {
        let u = url(&["btcusdt@trade".into(), "!markPrice@arr".into()]);
        assert_eq!(
            u,
            "wss://fstream.asterdex.com/stream?streams=btcusdt@trade/!markPrice@arr"
        );
    }
}
