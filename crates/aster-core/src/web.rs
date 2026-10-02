//! The core's own control page: a minimal HTTP/1.1 server on a `TcpListener`,
//! one page of vanilla JS, and the control queue everything goes through.
//!
//! Why our own server: the page holds the bot token and stops the trading, so
//! it never leaves loopback (`settings::validate` refuses a network endpoint
//! with no password, and the operator reaches the server's page over `ssh -L`),
//! and a web framework would be a dependency tree for eight routes. What
//! arrives is untrusted all the same — the request line, the headers and the
//! body are capped, every socket has a timeout, and the connections served at
//! once are bounded, because the alternative to each of those is a core a
//! stuck browser can hold up.
//!
//! Nothing here touches the core's state. Every button is a `ControlCmd` the
//! trading loop answers between two UDP steps, waited for with a bound
//! (`control::ask`): a page nobody is looking at costs the loop nothing, and a
//! loop that is busy costs the page a 503 instead of a hung thread.

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;

use crate::api_meter::ApiMeter;
use crate::control::{self, AskError, ControlCmd};
use crate::engine::now_ms;
use crate::settings::{self, Settings};
use crate::stderr_log;

/// The page and the login form: inline CSS and JS, so there is no second path
/// to serve and nothing that could be asked for by name.
const PAGE: &str = include_str!("web/page.html");
const LOGIN: &str = include_str!("web/login.html");

/// How long one command may keep a request waiting. The loop answers between
/// two UDP steps, so this is only ever spent when it is busy or stopping.
const ASK_WAIT: Duration = Duration::from_secs(5);
/// The wait of a settings edit that changes the page's own password.
const PASSWORD_ASK_WAIT: Duration = Duration::from_secs(60);
/// Read and write timeout: a client that connected and went away must not hold
/// a slot until the core restarts. It bounds ONE syscall, which is why
/// [`REQUEST_DEADLINE`] exists beside it.
const IO_TIMEOUT: Duration = Duration::from_secs(15);
/// The whole request, from the first byte to the last of the body. A socket
/// timeout is re-armed by every byte that arrives, so a client trickling one
/// just inside [`IO_TIMEOUT`] would hold its slot for as long as it liked —
/// and there are only [`MAX_CONNS`] of them between the operator and the Stop
/// button.
const REQUEST_DEADLINE: Duration = Duration::from_secs(20);
/// The headers of a request are a few hundred bytes a browser sends at once: a connection that
/// has not finished them in this long is holding one of the [`MAX_CONNS`] slots for nothing.
const HEAD_DEADLINE: Duration = Duration::from_secs(5);
/// What one request may be at most.
const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 64 * 1024;
/// Connections served at once — the page is one browser; the rest get a 503.
const MAX_CONNS: usize = 8;
/// A login lasts this long, then the password is asked for again.
const SESSION_MS: i64 = 12 * 3_600_000;
/// Logins kept; a new one past this drops the oldest.
const MAX_SESSIONS: usize = 8;
/// Failed logins before the page stops answering them for [`BLOCK_MS`].
const MAX_FAILS: u32 = 5;
const BLOCK_MS: i64 = 30_000;
/// Journal lines the page shows, and the most it may ask for.
const LOG_LINES: usize = 300;
const MAX_LOG_LINES: usize = 5_000;

/// Start the page. A page that cannot be served is an error line and nothing
/// more: the core is a trading core first, and a taken port must not be what
/// takes it off the market — under a restarting supervisor it would never come
/// back.
pub fn start(settings: &Settings, control: Sender<ControlCmd>, meter: Option<Arc<ApiMeter>>) {
    let addr = match settings.web_addr() {
        Ok(addr) => addr,
        Err(e) => {
            log::error!("web: {e}; the page is off");
            return;
        }
    };
    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(e) => {
            log::error!("web: {addr}: {e}; the page is off");
            return;
        }
    };
    log::info!(
        "web: http://{addr} ({})",
        if settings.web.password.is_empty() {
            "no password, loopback only"
        } else {
            "password"
        }
    );
    let web = Arc::new(Web {
        control,
        meter,
        guard: Mutex::new(Guard {
            password: settings.web.password.clone(),
            sessions: Vec::new(),
            fails: 0,
            blocked_until: 0,
        }),
        bound: addr.ip().to_string(),
        live: AtomicUsize::new(0),
    });
    if let Err(e) = thread::Builder::new()
        .name("web".into())
        .spawn(move || accept_loop(&listener, &web))
    {
        log::error!("web: the thread did not start: {e}; the page is off");
    }
}

struct Web {
    control: Sender<ControlCmd>,
    /// The request counters behind the API tab, read straight here rather than
    /// asked of the trading loop: a day of them is thousands of numbers, and
    /// the loop answers between two UDP steps. `None` — no token, so no client
    /// and nothing counted.
    meter: Option<Arc<ApiMeter>>,
    guard: Mutex<Guard>,
    /// The address the page is bound to, as a `Host` header names it.
    bound: String,
    /// Connections being served, against [`MAX_CONNS`].
    live: AtomicUsize,
}

/// Who may press the buttons. The password is a copy of the settings' one: the
/// page is the only thing that can change it, and this copy follows whatever
/// answer the loop called applied — a failed save included, because the loop
/// is already checking against the new one by then.
struct Guard {
    password: String,
    /// Session id and the millisecond it dies at.
    sessions: Vec<(String, i64)>,
    fails: u32,
    blocked_until: i64,
}

impl Web {
    /// The lock is only ever held for a few comparisons, and a poisoned one is
    /// still the truth about who is logged in — a panic in one request must not
    /// lock the operator out of the core's Stop button.
    fn guard(&self) -> MutexGuard<'_, Guard> {
        self.guard.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Guard {
    /// No password = no login: `settings::validate` only allows that on
    /// loopback, where whoever reaches the port is already on the machine.
    fn open(&self) -> bool {
        self.password.is_empty()
    }

    fn authed(&mut self, sid: &str, now: i64) -> bool {
        if self.open() {
            return true;
        }
        self.sessions.retain(|(_, dies)| *dies > now);
        !sid.is_empty() && self.sessions.iter().any(|(id, _)| id == sid)
    }

    fn remember(&mut self, sid: String, now: i64) {
        if self.sessions.len() >= MAX_SESSIONS {
            self.sessions.remove(0);
        }
        self.sessions.push((sid, now + SESSION_MS));
    }
}

fn accept_loop(listener: &TcpListener, web: &Arc<Web>) {
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            // One refused accept is not the end of the page: the listener is
            // still there, and the next connection may well come in.
            Err(e) => {
                log::warn!("web: accept: {e}");
                continue;
            }
        };
        if web.live.load(Ordering::Relaxed) >= MAX_CONNS {
            // The timeout first: this write happens on the accept thread, and
            // a client that never reads its 503 would otherwise block it —
            // one refused connection would cost every later one, the
            // operator's Stop button included.
            let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
            let _ = write_response(
                &stream,
                &Response::plain(503, "the page is serving as many requests as it will"),
            );
            continue;
        }
        web.live.fetch_add(1, Ordering::Relaxed);
        let mine = Arc::clone(web);
        if let Err(e) = thread::Builder::new()
            .name("web-conn".into())
            .spawn(move || {
                // The slot is given back by `Drop`, so a panic inside one
                // request does not leak it.
                let _slot = Slot(&mine);
                serve(&stream, &mine);
            })
        {
            web.live.fetch_sub(1, Ordering::Relaxed);
            log::warn!("web: connection thread: {e}");
        }
    }
    log::error!("web: the listener is gone; the page is off until a restart");
}

struct Slot<'a>(&'a Web);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}

fn serve(stream: &TcpStream, web: &Web) {
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
    let response = match read_request(stream) {
        Ok(req) => route(web, &req),
        // Not a log line per bad request: a port scan is not news, and at
        // debug the operator can still see what arrived.
        Err(e) => {
            log::debug!("web: bad request: {e}");
            Response::plain(400, "bad request")
        }
    };
    if let Err(e) = write_response(stream, &response) {
        log::debug!("web: write: {e}");
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    /// `sid` out of the Cookie header.
    sid: String,
    /// The page's own header. A cross-site form cannot set one, and a browser
    /// asks for permission (a CORS preflight) before it sends it — which is
    /// never given. Without it any page the trader has open in another tab
    /// could POST `/api/panic` at the loopback port, cookie or no cookie.
    tagged: bool,
    /// The `Host` header, as the browser sent it.
    host: String,
    body: Vec<u8>,
}

/// Whether `host` (a `Host` header, port and all) names this machine's loopback or the address
/// the page is bound to. A page without a password is protected by being loopback only; a name
/// that merely resolves there (DNS rebinding: the attacker's domain pointed at 127.0.0.1) makes
/// a foreign page same-origin with it, and the `Host` it sends gives it away.
fn host_ok(host: &str, bound: &str) -> bool {
    // No `Host` at all is no rebinding browser (they always send one): HTTP/1.0 tools and curl
    // without it are the operator's own.
    if host.trim().is_empty() {
        return true;
    }
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split_once(']').map_or(rest, |(ip, _)| ip)
    } else if host.matches(':').count() > 1 {
        // A bare IPv6 address: the colons are its own.
        host
    } else {
        host.rsplit_once(':').map_or(host, |(name, _)| name)
    };
    let name = name.to_ascii_lowercase();
    matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1") || name == bound
}

fn read_request(stream: &TcpStream) -> Result<Request, String> {
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let mut reader = BufReader::new(stream);
    // A read blocks up to its socket timeout, and the head deadline is looked at only between
    // reads: the timeout of the head is the head deadline, the body's is the usual one.
    let _ = stream.set_read_timeout(Some(HEAD_DEADLINE));
    let head = read_head(&mut reader, Instant::now() + HEAD_DEADLINE);
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let head = head?;
    let head = String::from_utf8_lossy(&head);
    let mut lines = head.lines();
    let first = lines.next().ok_or("no request line")?;
    let mut parts = first.split_whitespace();
    let method = parts.next().ok_or("no method")?.to_owned();
    let target = parts.next().ok_or("no target")?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let (mut len, mut sid, mut tagged, mut host) = (0usize, String::new(), false, String::new());
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                len = value
                    .parse()
                    .map_err(|_| format!("content-length {value:?}"))?;
            }
            "cookie" => sid = cookie(value, "sid"),
            "x-astercore" => tagged = true,
            "host" => host = value.to_owned(),
            _ => {}
        }
    }
    if len > MAX_BODY {
        return Err(format!("a body of {len} bytes, over {MAX_BODY}"));
    }
    let body = read_body(&mut reader, len, deadline)?;
    Ok(Request {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        sid,
        tagged,
        host,
        body,
    })
}

/// The request line and the headers, up to the blank line that ends them.
///
/// A byte at a time out of the buffer, and not `read_line`: that one appends
/// into a `String` until a newline arrives, so a client that sends none is
/// bounded by nothing at all — 200 MB of it took this core from 6 MB resident
/// to 81. Here the cap is checked as the bytes arrive, and the deadline with
/// it, because a socket timeout only ever bounds one read.
fn read_head(reader: &mut BufReader<&TcpStream>, deadline: Instant) -> Result<Vec<u8>, String> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        reader
            .read_exact(&mut byte)
            .map_err(|e| format!("reading the headers: {e}"))?;
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") || head.ends_with(b"\n\n") {
            return Ok(head);
        }
        if head.len() > MAX_HEAD {
            return Err(format!("the headers are longer than {MAX_HEAD} bytes"));
        }
        if Instant::now() > deadline {
            return Err("the headers took longer than the deadline".into());
        }
    }
}

/// Exactly `len` bytes of body, in whatever chunks they arrive in, with the
/// same deadline over the whole of it.
fn read_body(
    reader: &mut BufReader<&TcpStream>,
    len: usize,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    let mut body = vec![0u8; len];
    let mut read = 0;
    while read < len {
        match reader.read(&mut body[read..]) {
            Ok(0) => return Err("the peer closed inside the body".into()),
            Ok(n) => read += n,
            Err(e) => return Err(format!("reading the body: {e}")),
        }
        if read < len && Instant::now() > deadline {
            return Err("the body took longer than the deadline".into());
        }
    }
    Ok(body)
}

/// One cookie's value out of a `Cookie` header, empty when it is not there.
fn cookie(header: &str, name: &str) -> String {
    header
        .split(';')
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(key, _)| *key == name)
        .map_or_else(String::new, |(_, value)| value.trim().to_owned())
}

/// A decimal query parameter, or `default`: the page is the only caller, so a
/// value that is not a number is a default and not an error to explain.
fn param(query: &str, name: &str, default: usize) -> usize {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(default)
}

struct Response {
    code: u16,
    ctype: &'static str,
    body: Vec<u8>,
    cookie: Option<String>,
}

impl Response {
    fn html(body: &str) -> Self {
        Self {
            code: 200,
            ctype: "text/html; charset=utf-8",
            body: body.as_bytes().to_vec(),
            cookie: None,
        }
    }

    fn plain(code: u16, body: &str) -> Self {
        Self {
            code,
            ctype: "text/plain; charset=utf-8",
            body: format!("{body}\n").into_bytes(),
            cookie: None,
        }
    }

    fn json(code: u16, value: &serde_json::Value) -> Self {
        Self {
            code,
            ctype: "application/json",
            // A status that will not serialize is a bug in the core, not in
            // the request: it is said out loud rather than shown as an empty
            // page.
            body: serde_json::to_vec(value).unwrap_or_else(|e| {
                log::error!("web: the answer would not serialize: {e}");
                b"{}".to_vec()
            }),
            cookie: None,
        }
    }

    fn with_cookie(mut self, sid: &str) -> Self {
        // No `Secure`: the page is plain HTTP on loopback, and over the
        // network it is reached through an ssh tunnel that is the encryption.
        self.cookie = Some(match sid {
            "" => "sid=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0".to_owned(),
            sid => format!(
                "sid={sid}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
                SESSION_MS / 1000
            ),
        });
        self
    }
}

fn write_response(stream: &TcpStream, r: &Response) -> std::io::Result<()> {
    let reason = match r.code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Service Unavailable",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n",
        r.code,
        r.ctype,
        r.body.len()
    );
    if let Some(cookie) = &r.cookie {
        head.push_str(&format!("Set-Cookie: {cookie}\r\n"));
    }
    head.push_str("\r\n");
    let mut out = stream;
    out.write_all(head.as_bytes())?;
    out.write_all(&r.body)?;
    out.flush()
}

#[derive(Deserialize)]
struct Login {
    password: String,
}

#[derive(Deserialize)]
struct Switch {
    start: bool,
}

/// The strategy helper's question: which strategy, and the form as it stands.
/// A `BTreeMap` because the page sends a JSON object and the order of its
/// keys decides nothing — every field is applied to the snapshot before a
/// single one is read.
#[derive(Deserialize)]
struct ScreenAsk {
    strategy_id: u64,
    #[serde(default)]
    fields: std::collections::BTreeMap<String, String>,
}

fn route(web: &Web, req: &Request) -> Response {
    let now = now_ms();
    // Without a password the page is open to whoever reaches the port, and that is safe only
    // for requests that name this machine.
    if web.guard().open() && !host_ok(&req.host, &web.bound) {
        return Response::plain(403, "unknown host");
    }
    let authed = web.guard().authed(&req.sid, now);
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/") => Response::html(if authed { PAGE } else { LOGIN }),
        ("POST", "/login") => login(web, req, now),
        ("POST", "/logout") if req.tagged => {
            let mut guard = web.guard();
            guard.sessions.retain(|(id, _)| *id != req.sid);
            Response::plain(200, "logged out").with_cookie("")
        }
        // Everything below is the core itself. The page's own header is asked
        // for first: without it the request did not come from the page.
        _ if !req.tagged => Response::plain(403, "this endpoint is the page's"),
        _ if !authed => Response::json(401, &json!({ "error": "log in" })),
        ("GET", "/api/status") => match control::ask(&web.control, ControlCmd::Status, ASK_WAIT) {
            Ok(status) => Response::json(
                200,
                &serde_json::to_value(&status).unwrap_or_else(|_| json!({})),
            ),
            Err(e) => busy(&e),
        },
        // Not a `ControlCmd`: the counters are the client's, not the loop's,
        // and the loop has nothing to say about them.
        ("GET", "/api/metrics") => match &web.meter {
            Some(meter) => Response::json(
                200,
                &serde_json::to_value(meter.view(now)).unwrap_or_else(|_| json!({})),
            ),
            // The same shape `ApiMeter::view` serves, empty. The page is not
            // the one who reads it — `drawApi` stops at the empty `groups`
            // before it reaches the chart — but this route is read with curl
            // as often as with a browser, and a literal that drifts from the
            // struct it stands in for lies to whoever reads it that way.
            None => Response::json(
                200,
                &json!({ "from_min": 0, "span_min": 0, "tariff": false, "groups": [],
                         "ping": { "link": { "avg": [], "max": [], "n": [] },
                                   "orders": { "avg": [], "max": [], "n": [] },
                                   "link_methods": [], "order_methods": [] } }),
            ),
        },
        ("GET", "/api/log") => Response::plain(
            200,
            &stderr_log::tail(
                now,
                param(&req.query, "lines", LOG_LINES).min(MAX_LOG_LINES),
            ),
        ),
        ("POST", "/api/strategies") => match parse::<Switch>(&req.body) {
            Err(e) => Response::json(400, &json!({ "error": e })),
            Ok(Switch { start }) => match control::ask(
                &web.control,
                |reply| ControlCmd::Strategies { start, reply },
                ASK_WAIT,
            ) {
                Ok(running) => Response::json(200, &json!({ "running": running })),
                Err(e) => busy(&e),
            },
        },
        ("POST", "/api/panic") => {
            match control::ask(&web.control, ControlCmd::PanicAll, ASK_WAIT) {
                Ok(moved) => Response::json(200, &json!({ "moved": moved })),
                Err(e) => busy(&e),
            }
        }
        // The answer comes back before the core is gone: the loop replies,
        // then latches the halt. A closed channel here means it is already on
        // its way out, which is what was asked for either way.
        ("POST", "/api/stop") => match control::ask(&web.control, ControlCmd::Shutdown, ASK_WAIT) {
            Ok(()) | Err(AskError::Gone) => Response::json(200, &json!({ "halt": "stop" })),
            Err(e) => busy(&e),
        },
        ("POST", "/api/restart") => match control::ask(&web.control, ControlCmd::Restart, ASK_WAIT)
        {
            Ok(()) | Err(AskError::Gone) => Response::json(200, &json!({ "halt": "restart" })),
            Err(e) => busy(&e),
        },
        // Read-only, and a POST all the same: the helper's whole form travels
        // with the question, and a form does not belong in a query string.
        ("POST", "/api/screen") => match parse::<ScreenAsk>(&req.body) {
            Err(e) => Response::json(400, &json!({ "error": e })),
            Ok(ask) => {
                let fields: Vec<(String, String)> = ask.fields.into_iter().collect();
                match control::ask(
                    &web.control,
                    |reply| ControlCmd::Screen {
                        strategy_id: ask.strategy_id,
                        fields,
                        reply,
                    },
                    ASK_WAIT,
                ) {
                    Ok(Ok(screen)) => Response::json(
                        200,
                        &serde_json::to_value(&screen).unwrap_or_else(|_| json!({})),
                    ),
                    // The loop's own «ask again» is not a bad request: a
                    // caller told 400 learns to stop, and this one is meant
                    // to come back.
                    Ok(Err(e)) if e == control::SCREEN_BUSY => {
                        Response::json(503, &json!({ "error": e }))
                    }
                    Ok(Err(e)) => Response::json(400, &json!({ "error": e })),
                    Err(e) => busy(&e),
                }
            }
        },
        ("POST", "/api/settings") => settings_edit(web, req),
        ("GET" | "POST", _) => Response::plain(404, "no such page"),
        _ => Response::plain(405, "GET or POST"),
    }
}

fn parse<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, String> {
    serde_json::from_slice(body).map_err(|e| e.to_string())
}

fn busy(e: &AskError) -> Response {
    Response::json(503, &json!({ "error": e.to_string() }))
}

fn login(web: &Web, req: &Request, now: i64) -> Response {
    let Ok(Login { password }) = parse::<Login>(&req.body) else {
        return Response::json(400, &json!({ "error": "no password in the request" }));
    };
    let mut guard = web.guard();
    if guard.open() {
        return Response::json(200, &json!({ "ok": true }));
    }
    // The password is checked first, and a right one is never made to wait:
    // the throttle is there to slow guessing, and a throttle that also holds
    // the operator out is a lock anyone who can reach the port can turn. What
    // it still does is cap the guessing — a wrong password inside the block is
    // refused and not even counted, so the window cannot be walked through five
    // tries at a time.
    if !same(&guard.password, &password) {
        if guard.blocked_until > now {
            let wait = (guard.blocked_until - now + 999) / 1000;
            return Response::json(
                429,
                &json!({ "error": format!("too many wrong passwords; {wait} s to go") }),
            );
        }
        guard.fails += 1;
        if guard.fails >= MAX_FAILS {
            guard.fails = 0;
            guard.blocked_until = now + BLOCK_MS;
            log::warn!(
                "web: {MAX_FAILS} wrong passwords; no login for the next {} s",
                BLOCK_MS / 1000
            );
        }
        return Response::json(401, &json!({ "error": "wrong password" }));
    }
    let Some(sid) = session_id() else {
        log::error!("web: no randomness for a session id; the login was refused");
        return Response::json(
            500,
            &json!({ "error": "the core could not mint a session" }),
        );
    };
    guard.fails = 0;
    guard.blocked_until = 0;
    guard.remember(sid.clone(), now);
    log::info!("web: logged in");
    Response::json(200, &json!({ "ok": true })).with_cookie(&sid)
}

/// One settings form. The core merges and saves it (`ControlCmd::SettingsEdit`)
/// because the page was never given the whole config; what comes back is the
/// fields that only take effect at the next start.
///
/// A new web password is also this layer's own: it is applied here, and every
/// session — including the one that set it — is dropped, so the browser that
/// changed it proves it knows the new one. It is applied on any answer the
/// loop calls applied, a failed save included: the loop is already running on
/// the new password then, and a copy here that stayed on the old one would
/// check a credential the core no longer has.
fn settings_edit(web: &Web, req: &Request) -> Response {
    let edit = match parse::<settings::Edit>(&req.body) {
        Ok(edit) => edit,
        Err(e) => return Response::json(400, &json!({ "error": e })),
    };
    let changed_password = edit.web_password.clone();
    // A password change should not time out half-applied — the loop would
    // take it while the page kept checking the old one. The loop answers in
    // milliseconds; the long wait narrows the window to a loop busy for a
    // minute, it does not close it.
    let wait = if changed_password.is_some() {
        PASSWORD_ASK_WAIT
    } else {
        ASK_WAIT
    };
    let answer = control::ask(
        &web.control,
        |reply| ControlCmd::SettingsEdit {
            edit: Box::new(edit),
            reply,
        },
        wait,
    );
    match answer {
        Err(e) => busy(&e),
        Ok(Err(e)) => Response::json(400, &json!({ "error": e })),
        Ok(Ok(applied)) => {
            let mut logout = false;
            if let Some(password) = changed_password {
                let mut guard = web.guard();
                if guard.password != password {
                    guard.password = password;
                    guard.sessions.clear();
                    guard.fails = 0;
                    guard.blocked_until = 0;
                    logout = true;
                    log::info!("web: the page's password was changed; every login was dropped");
                }
            }
            let answer = Response::json(
                200,
                &json!({
                    "restart": applied.restart,
                    "unsaved": applied.unsaved,
                    "logout": logout,
                }),
            );
            if logout {
                answer.with_cookie("")
            } else {
                answer
            }
        }
    }
}

/// Compare without telling the caller where the strings first differ.
fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// 128 bits from the kernel, hex. A guessable session id is the password, so
/// there is no fallback to the clock: a login the core cannot make private is
/// a login it refuses.
#[cfg(unix)]
fn session_id() -> Option<String> {
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .map_err(|e| log::error!("web: /dev/urandom: {e}"))
        .ok()?;
    Some(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(not(unix))]
fn session_id() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Through a real socket: these functions read a stream, and the point of
    /// every case below is what they do with what comes off one.
    fn on_stream<T>(raw: &str, read: impl FnOnce(&TcpStream) -> T) -> T {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let raw = raw.as_bytes().to_vec();
        let client = thread::spawn(move || {
            let mut out = TcpStream::connect(addr).unwrap();
            // A send the reader never finishes with is a broken pipe here,
            // which is the client's business and not the case under test.
            let _ = out.write_all(&raw);
            let _ = out.flush();
            // Held open until the server has read: a close mid-body would be
            // a different case than the one under test.
            let _ = out.read(&mut [0u8; 1]);
        });
        let (stream, _) = listener.accept().unwrap();
        let out = read(&stream);
        drop(stream);
        let _ = client.join();
        out
    }

    fn request(raw: &str) -> Result<Request, String> {
        on_stream(raw, read_request)
    }

    #[test]
    fn a_request_is_read_down_to_its_cookie_and_its_body() {
        let req = request(
            "POST /api/settings?lines=7 HTTP/1.1\r\nHost: x\r\nCookie: other=1; sid=abc \r\n\
             X-Astercore: 1\r\nContent-Length: 9\r\n\r\n{\"a\":1234}",
        )
        .unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/api/settings");
        assert_eq!(req.query, "lines=7");
        assert_eq!(req.sid, "abc");
        assert!(req.tagged);
        // Exactly Content-Length bytes, not whatever else was in the socket.
        assert_eq!(req.body, b"{\"a\":1234");
        assert_eq!(param(&req.query, "lines", 300), 7);
        assert_eq!(param(&req.query, "missing", 300), 300);
    }

    /// The caps are the whole reason this parser is safe to point at a socket:
    /// a body nobody would send must not become a body the core allocates.
    #[test]
    fn what_is_too_big_is_refused_before_it_is_read() {
        let refused = |raw: &str| match request(raw) {
            Err(why) => why,
            // `Request` is deliberately not `Debug`: its body carries the
            // password a login posts, and a derive is one `{:?}` away from
            // putting that in a log line.
            Ok(_) => panic!("read as a request: {raw:?}"),
        };
        let long = "x".repeat(MAX_HEAD + 1);
        let err = refused(&format!("GET / HTTP/1.1\r\nX-Pad: {long}\r\n\r\n"));
        assert!(err.contains("headers"), "{err}");
        let err = refused(&format!(
            "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        ));
        assert!(err.contains("over"), "{err}");
        assert!(refused("GET\r\n\r\n").contains("target"));
        // The cap has to hold while the bytes arrive, not once a line ends:
        // 200 MB without a single newline took the running core from 6 MB
        // resident to 81 before this was checked where it is now.
        let err = refused(&format!(
            "GET / HTTP/1.1\r\nX-Pad: {}",
            "x".repeat(MAX_HEAD + 1)
        ));
        assert!(err.contains("headers"), "{err}");
    }

    /// A socket timeout bounds one read; this bounds the request. Driven with
    /// a deadline that is already up, because the real one is 5 s (`HEAD_DEADLINE`).
    #[test]
    fn a_request_that_outstays_its_deadline_is_dropped() {
        let err = on_stream("GET / HTTP/1.1\r\nX-Pad: no newline here", |stream| {
            let mut reader = BufReader::new(stream);
            read_head(&mut reader, Instant::now()).unwrap_err()
        });
        assert!(err.contains("deadline"), "{err}");
    }

    #[test]
    fn a_cookie_is_read_by_name_only() {
        assert_eq!(cookie("sid=a1; other=b2", "sid"), "a1");
        assert_eq!(cookie("notsid=a1", "sid"), "");
        assert_eq!(cookie("", "sid"), "");
    }

    #[test]
    fn a_session_id_is_random_and_hex() {
        let (one, two) = (session_id().unwrap(), session_id().unwrap());
        assert_eq!(one.len(), 32);
        assert!(one.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(one, two);
    }

    #[test]
    fn the_password_comparison_does_not_shortcut_on_length() {
        assert!(same("s3cret", "s3cret"));
        assert!(!same("s3cret", "s3cre"));
        assert!(!same("s3cret", "s3crets"));
        assert!(!same("s3cret", ""));
        assert!(same("", ""));
        // A NUL where the shorter string ends must not read as equal: that is
        // what a naive pad-with-zero comparison gets wrong.
        assert!(!same("a\0", "a"));
    }

    fn web(password: &str) -> (Web, std::sync::mpsc::Receiver<ControlCmd>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (
            Web {
                control: tx,
                meter: Some(ApiMeter::detached()),
                guard: Mutex::new(Guard {
                    password: password.to_owned(),
                    sessions: Vec::new(),
                    fails: 0,
                    blocked_until: 0,
                }),
                bound: "127.0.0.1".into(),
                live: AtomicUsize::new(0),
            },
            rx,
        )
    }

    fn post(path: &str, body: &str, sid: &str) -> Request {
        Request {
            method: "POST".into(),
            path: path.into(),
            query: String::new(),
            sid: sid.into(),
            tagged: true,
            host: "127.0.0.1:3102".into(),
            body: body.as_bytes().to_vec(),
        }
    }

    /// A page with a password: the buttons answer 401 until a login, and the
    /// login is the only route that takes one without a session.
    #[test]
    fn nothing_but_the_login_answers_without_a_session() {
        let (web, rx) = web("s3cret");
        assert_eq!(route(&web, &post("/api/panic", "", "")).code, 401);
        assert_eq!(route(&web, &post("/api/stop", "", "")).code, 401);
        assert!(rx.try_recv().is_err(), "no command reached the core");

        let wrong = route(&web, &post("/login", r#"{"password":"nope"}"#, ""));
        assert_eq!(wrong.code, 401);
        assert!(wrong.cookie.is_none());
        let ok = route(&web, &post("/login", r#"{"password":"s3cret"}"#, ""));
        assert_eq!(ok.code, 200);
        let sid = web.guard().sessions[0].0.clone();
        assert!(ok.cookie.as_ref().unwrap().contains(&sid));
        assert!(ok.cookie.as_ref().unwrap().contains("HttpOnly"));

        // With the session the command reaches the queue.
        let panic = thread::scope(|s| {
            let sent = s.spawn(|| route(&web, &post("/api/panic", "", &sid)));
            match rx.recv().unwrap() {
                ControlCmd::PanicAll(reply) => reply.send(3).unwrap(),
                other => panic!("not PanicAll: {}", name_of(&other)),
            }
            sent.join().unwrap()
        });
        assert_eq!(panic.code, 200);
        assert!(String::from_utf8_lossy(&panic.body).contains("\"moved\":3"));
    }

    /// The page's own header is what tells a request from the page apart from
    /// one a site the trader has open made on its behalf.
    #[test]
    fn a_request_without_the_pages_header_reaches_nothing() {
        let (web, rx) = web("");
        let mut req = post("/api/panic", "", "");
        req.tagged = false;
        let answer = route(&web, &req);
        assert_eq!(answer.code, 403);
        assert!(rx.try_recv().is_err());
        // The page itself is not behind the header — it is what sets it.
        let page = route(
            &web,
            &Request {
                method: "GET".into(),
                path: "/".into(),
                query: String::new(),
                sid: String::new(),
                tagged: false,
                host: "localhost:3102".into(),
                body: Vec::new(),
            },
        );
        assert_eq!(page.code, 200);
    }

    /// DNS rebinding: a foreign name pointed at the loopback port reaches an open page with its
    /// own `Host`, and is refused; the machine's own names are not.
    #[test]
    fn an_open_page_answers_only_to_its_own_host() {
        for ok in [
            "127.0.0.1:3102",
            "localhost:3102",
            "[::1]:3102",
            "LOCALHOST",
            "10.0.0.5:80",
        ] {
            assert!(host_ok(ok, "10.0.0.5"), "{ok}");
        }
        assert!(host_ok("::1", "127.0.0.1") && host_ok("", "127.0.0.1"));
        for bad in [
            "evil.example:3102",
            "127.0.0.1.evil.example",
            "localhost.evil.example",
        ] {
            assert!(!host_ok(bad, "127.0.0.1"), "{bad:?}");
        }
        let (open, _rx) = web("");
        let mut req = post("/api/panic", "", "");
        req.host = "rebind.example:3102".into();
        assert_eq!(route(&open, &req).code, 403);
        // With a password the login is the guard, wherever the page is reached from.
        let (locked, _rx) = web("s3cret");
        let mut req = post("/login", "{}", "");
        req.host = "rebind.example:3102".into();
        assert_ne!(route(&locked, &req).code, 403);
    }

    /// Wrong passwords are throttled; the right one never is. A block that
    /// also held the operator out would be a lock anyone who can reach the
    /// port could turn, on a page whose buttons stop the trading.
    #[test]
    fn guessing_is_throttled_and_the_operator_is_not() {
        let (web, _rx) = web("s3cret");
        for _ in 0..MAX_FAILS {
            assert_eq!(
                route(&web, &post("/login", r#"{"password":"x"}"#, "")).code,
                401
            );
        }
        // More guesses are refused without being tried at all.
        let blocked = route(&web, &post("/login", r#"{"password":"y"}"#, ""));
        assert_eq!(blocked.code, 429);
        assert!(web.guard().blocked_until > now_ms());
        // The right password walks through the same block.
        let ok = route(&web, &post("/login", r#"{"password":"s3cret"}"#, ""));
        assert_eq!(ok.code, 200);
        assert_eq!(web.guard().sessions.len(), 1);
        assert_eq!(web.guard().blocked_until, 0, "the block is spent");
    }

    /// An empty password is the default loopback case: the page opens and the
    /// buttons work, because being on the machine is the credential.
    #[test]
    fn without_a_password_the_page_is_open() {
        let (web, rx) = web("");
        let answer = thread::scope(|s| {
            let sent = s.spawn(|| route(&web, &post("/api/strategies", r#"{"start":true}"#, "")));
            match rx.recv().unwrap() {
                ControlCmd::Strategies { start, reply } => {
                    assert!(start);
                    reply.send(true).unwrap();
                }
                other => panic!("not Strategies: {}", name_of(&other)),
            }
            sent.join().unwrap()
        });
        assert_eq!(answer.code, 200);
        assert!(String::from_utf8_lossy(&answer.body).contains("\"running\":true"));
        // A body that is not what the route expects is the caller's fault and
        // not a command.
        assert_eq!(route(&web, &post("/api/strategies", "{}", "")).code, 400);
        assert!(rx.try_recv().is_err());
    }

    /// A core that is not answering its queue is a 503, not a thread that sits
    /// on a connection slot forever.
    #[test]
    fn a_silent_core_answers_the_page_anyway() {
        let (web, rx) = web("");
        // A queue with nobody at the other end: the same 503 as a busy loop,
        // and the test does not wait `ASK_WAIT` out to see it.
        drop(rx);
        let answer = route(&web, &post("/api/panic", "", ""));
        assert_eq!(answer.code, 503);
        assert!(String::from_utf8_lossy(&answer.body).contains("not taking commands"));
    }

    /// A new password drops every login, including the one that set it.
    #[test]
    fn a_changed_password_logs_everyone_out() {
        let (web, rx) = web("old");
        web.guard().remember("live-session".into(), now_ms());
        let answer = thread::scope(|s| {
            let sent = s.spawn(|| {
                route(
                    &web,
                    &post("/api/settings", r#"{"web_password":"new"}"#, "live-session"),
                )
            });
            match rx.recv().unwrap() {
                ControlCmd::SettingsEdit { edit, reply } => {
                    assert_eq!(edit.web_password.as_deref(), Some("new"));
                    reply.send(Ok(control::Applied::default())).unwrap();
                }
                other => panic!("not SettingsEdit: {}", name_of(&other)),
            }
            sent.join().unwrap()
        });
        assert_eq!(answer.code, 200);
        assert!(String::from_utf8_lossy(&answer.body).contains("\"logout\":true"));
        assert_eq!(
            answer.cookie.as_deref(),
            Some("sid=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0")
        );
        let mut guard = web.guard();
        assert_eq!(guard.password, "new");
        assert!(!guard.authed("live-session", now_ms()));
    }

    /// An edit the file would not take is still an edit the core is running
    /// on: the page hears about the unsaved file, and this layer's copy of the
    /// password follows the core's rather than staying on one it no longer
    /// checks against.
    #[test]
    fn a_password_that_could_not_be_saved_is_still_the_one_in_force() {
        let (web, rx) = web("old");
        web.guard().remember("live-session".into(), now_ms());
        let answer = thread::scope(|s| {
            let sent = s.spawn(|| {
                route(
                    &web,
                    &post("/api/settings", r#"{"web_password":"new"}"#, "live-session"),
                )
            });
            match rx.recv().unwrap() {
                ControlCmd::SettingsEdit { reply, .. } => reply
                    .send(Ok(control::Applied {
                        restart: Vec::new(),
                        unsaved: Some("data/config.json was not written: no space".into()),
                    }))
                    .unwrap(),
                other => panic!("not SettingsEdit: {}", name_of(&other)),
            }
            sent.join().unwrap()
        });
        assert_eq!(answer.code, 200);
        let body = String::from_utf8_lossy(&answer.body);
        assert!(body.contains("no space"), "{body}");
        let mut guard = web.guard();
        assert_eq!(guard.password, "new", "the core is running on this one");
        assert!(!guard.authed("live-session", now_ms()));
    }

    /// A refused edit comes back as the reason, not as a 200 the operator
    /// reads as saved.
    #[test]
    fn a_refused_edit_is_the_pages_error() {
        let (web, rx) = web("");
        let answer = thread::scope(|s| {
            let sent = s.spawn(|| {
                route(
                    &web,
                    &post("/api/settings", r#"{"log_level":"chatty"}"#, ""),
                )
            });
            match rx.recv().unwrap() {
                ControlCmd::SettingsEdit { reply, .. } => {
                    reply
                        .send(Err("log_level \"chatty\" is not a level".into()))
                        .unwrap();
                }
                other => panic!("not SettingsEdit: {}", name_of(&other)),
            }
            sent.join().unwrap()
        });
        assert_eq!(answer.code, 400);
        assert!(String::from_utf8_lossy(&answer.body).contains("chatty"));
        // A field the core does not know is refused before it is a command.
        assert_eq!(
            route(&web, &post("/api/settings", r#"{"nope":1}"#, "")).code,
            400
        );
        assert!(rx.try_recv().is_err());
    }

    fn name_of(cmd: &ControlCmd) -> &'static str {
        match cmd {
            ControlCmd::Status(_) => "Status",
            ControlCmd::Strategies { .. } => "Strategies",
            ControlCmd::PanicAll(_) => "PanicAll",
            ControlCmd::Shutdown(_) => "Shutdown",
            ControlCmd::Restart(_) => "Restart",
            ControlCmd::Chart { .. } => "Chart",
            ControlCmd::Screen { .. } => "Screen",
            ControlCmd::SettingsEdit { .. } => "SettingsEdit",
            ControlCmd::Text { .. } => "Text",
            ControlCmd::Talk(_) => "Talk",
            ControlCmd::ChatApproved(_) => "ChatApproved",
        }
    }
}
