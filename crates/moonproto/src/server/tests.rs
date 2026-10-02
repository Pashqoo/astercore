//! Contract tests: the upstream `MoonClient` talks to `server::Server` on loopback.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::key_export::ServerKey;
use super::{Handler, Server, Session};
use crate::client::{ClientConfig, ConnectConfig, InitConfig, LifecycleEvent, MoonClient};
use crate::TransportMode;

struct Recorder {
    connected: Arc<AtomicUsize>,
    commands: Arc<AtomicUsize>,
}

impl Handler for Recorder {
    fn on_connected(&mut self, _s: &mut Session) {
        self.connected.fetch_add(1, Ordering::SeqCst);
    }
    fn on_command(&mut self, _s: &mut Session, _cmd: u8, _payload: &[u8]) {
        self.commands.fetch_add(1, Ordering::SeqCst);
    }
    fn on_closed(&mut self, _client_id: u64) {}
}

struct Harness {
    key: ServerKey,
    connected: Arc<AtomicUsize>,
    commands: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Harness {
    fn start(mode: TransportMode) -> Self {
        let connected = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(AtomicUsize::new(0));
        let mut key = ServerKey::generate(None, 0, mode);
        let mut server = Server::bind(
            &key,
            Recorder {
                connected: Arc::clone(&connected),
                commands: Arc::clone(&commands),
            },
        )
        .expect("bind");
        key.port = server.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let thread = thread::spawn(move || server.run(&stop_flag));
        Self {
            key,
            connected,
            commands,
            stop,
            thread: Some(thread),
        }
    }

    fn client(&self) -> MoonClient {
        let cfg = ClientConfig::new(
            "127.0.0.1",
            self.key.port,
            self.key.master_key,
            self.key.mac_key,
        )
        .with_transport_mode(self.key.transport_mode);
        MoonClient::connect(cfg, ConnectConfig::new(InitConfig::default())).expect("client")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn wait_for(
    client: &MoonClient,
    timeout: Duration,
    mut pred: impl FnMut(&LifecycleEvent) -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.drain_lifecycle_events().iter().any(&mut pred) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

fn client_reaches_connected(mode: TransportMode) {
    let h = Harness::start(mode);
    let client = h.client();
    assert!(
        wait_for(&client, Duration::from_secs(5), |e| matches!(
            e,
            LifecycleEvent::Connected { fresh: true }
        )),
        "mode {}: client never reached Connected",
        mode.name()
    );
    assert_eq!(h.connected.load(Ordering::SeqCst), 1);
    // Init spine starts right after Fine: BaseCheck arrives as an encrypted API command.
    let deadline = Instant::now() + Duration::from_secs(3);
    while h.commands.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        h.commands.load(Ordering::SeqCst) > 0,
        "no encrypted command reached the server"
    );
    let _ = client.disconnect();
}

#[test]
fn large_payload_is_sliced_and_reassembles_with_upstream_receiver() {
    use crate::protocol::handshake::Hello;
    use crate::protocol::slicing::SlicingReceiver;
    use crate::protocol::Command;
    use crate::transport::{transport_unpack, MacContext};

    let mac_key = [8u8; 16];
    let wire = Arc::new(super::Wire {
        mac_ctx: MacContext::new(&mac_key),
        mask_ver: 0,
        epoch: Instant::now(),
    });
    let hello = Hello::new(1, 2);
    let mut s = Session::new(
        wire,
        42,
        "127.0.0.1:1".parse().unwrap(),
        &[1u8; 16],
        &mac_key,
        &hello,
    );
    s.authorized = true;
    // Incompressible payload forces the Sliced path at PMTU 508.
    let payload: Vec<u8> = (0..5000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    s.send(Command::API.to_byte(), &payload);
    // Slices leave the session from `tick` (paced), not from `send`.
    assert!(s.out.is_empty());
    s.tick();

    let mut rx = SlicingReceiver::new();
    let mut assembled = None;
    let mut slices = 0;
    for packet in s.out.drain(..) {
        let (hdr, body) = transport_unpack(&mac_key, &packet, 0).expect("unpack");
        if hdr.cmd != Command::Sliced.to_byte() {
            continue; // Ping / PMTU probe from the same tick
        }
        slices += 1;
        let (done, _ack) = rx.on_new_sliced_with_session(&body, 0);
        if let Some((_, cmd, data, _, _)) = done {
            assembled = Some((cmd, data));
        }
    }
    assert!(slices > 1);
    let (cmd, mut data) = assembled.expect("reassembled");
    if cmd & super::session::COMPRESSED_FLAG != 0 {
        data = crate::compression::mp_decompress(&data).expect("decompress");
    }
    assert_eq!(cmd & 0x7F, Command::API.to_byte());
    assert_eq!(data, payload);
}

#[test]
fn client_reaches_connected_v0() {
    client_reaches_connected(TransportMode::V0);
}

#[test]
fn client_reaches_connected_v1_stun() {
    client_reaches_connected(TransportMode::V1);
}

#[test]
fn client_reaches_connected_v2_dns() {
    client_reaches_connected(TransportMode::V2);
}

/// A client silent for more than `SESSION_IDLE_MS` is dropped by the server's own timer, and
/// the handler hears of it.
#[test]
fn a_silent_session_is_closed_after_the_idle_limit() {
    use crate::protocol::handshake::Hello;
    use std::sync::Mutex;

    struct Closed(Arc<Mutex<Vec<u64>>>);
    impl Handler for Closed {
        fn on_connected(&mut self, _s: &mut Session) {}
        fn on_command(&mut self, _s: &mut Session, _cmd: u8, _payload: &[u8]) {}
        fn on_closed(&mut self, client_id: u64) {
            self.0.lock().unwrap().push(client_id);
        }
    }
    let closed = Arc::new(Mutex::new(Vec::new()));
    let key = ServerKey::generate(None, 0, TransportMode::V0);
    let mut server = Server::bind(&key, Closed(Arc::clone(&closed))).expect("bind");
    let mut session = Session::new(
        Arc::clone(&server.wire),
        77,
        "127.0.0.1:1".parse().unwrap(),
        &server.master_key,
        &server.mac_key,
        &Hello::new(1, 2),
    );
    session.authorized = true;
    session.last_recv = server.wire.now() - super::SESSION_IDLE_MS + 5_000;
    server.sessions.insert(77, session);

    server.step();
    assert!(
        closed.lock().unwrap().is_empty(),
        "inside the limit it stays"
    );
    assert!(server.sessions.contains_key(&77));

    server.sessions.get_mut(&77).unwrap().last_recv =
        server.wire.now() - super::SESSION_IDLE_MS - 1;
    server.step();
    assert_eq!(*closed.lock().unwrap(), [77]);
    assert!(!server.sessions.contains_key(&77));
}
