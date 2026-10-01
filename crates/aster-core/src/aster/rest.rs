//! Synchronous REST client for `fapi.asterdex.com`.
//!
//! Aster speaks the Binance USDⓈ-M Futures dialect: plain `GET`/`POST` with
//! query parameters, JSON answers, decimal strings for every number. Signed
//! endpoints are not reached yet — M0 and M1 are entirely public, which is why
//! the credential question (`PLAN.md` §10) does not block them.
//!
//! Synchronous on purpose (`ureq`, no Tokio): the core is thread-per-stream,
//! and a runtime would be the only async thing in the process.

use std::fmt;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use ureq::http::Response;
use ureq::{Agent, Body};

use super::json::{ApiError, ExchangeInfo, PremiumIndex, ServerTime, Ticker24h};

pub const BASE: &str = "https://fapi.asterdex.com";

/// A call is given up after this long. Measured 01.10 from the Mac:
/// `/fapi/v1/time` answered in 0.30 s and the 843 KB `exchangeInfo` well
/// inside a second, so 30 s is a dead-gateway bound, not a working budget.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum Error {
    /// Non-2xx with Aster's own body. `code` is the exchange's negative
    /// integer (`-1021`, `-2019`); the policy table in `PLAN.md` is keyed on it.
    Api {
        status: u16,
        code: i64,
        msg: String,
    },
    Transport(String),
    Decode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api { status, code, msg } => write!(f, "api {status}/{code}: {msg}"),
            Self::Transport(e) => write!(f, "transport: {e}"),
            Self::Decode(e) => write!(f, "decode: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        Self::Transport(e.to_string())
    }
}

/// What the gateway says this client has spent, read from the answer's headers
/// rather than counted here.
///
/// `x-mbx-used-weight-1m` comes back on every call (measured: `42` right after
/// the all-symbol 24-hour ticker, which matches that endpoint's documented
/// weight). Reading it beats a local counter, which can only ever drift from
/// the number the exchange will actually ban on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    /// `x-mbx-used-weight-1m`, or `None` if the answer carried no such header.
    pub weight_1m: Option<i64>,
    /// `x-mbx-order-count-1m`.
    pub orders_1m: Option<i64>,
    /// `x-mbx-order-count-10s`.
    pub orders_10s: Option<i64>,
}

pub struct Rest {
    agent: Agent,
    /// `server_time - local_time`, in milliseconds, from the last
    /// [`Rest::sync_clock`]. Signed calls add it to their `timestamp`, or the
    /// gateway answers `-1021 INVALID_TIMESTAMP`.
    clock_delta_ms: i64,
    usage: Usage,
    /// Round trip of the last call that got an answer of any status.
    last_rtt: Option<Duration>,
}

impl Rest {
    pub fn new() -> Self {
        let agent = Agent::config_builder()
            // Aster's refusals carry the code we need to act on, so a non-2xx
            // must reach `check_status` as a response with a body, not as a
            // transport error that has thrown the body away.
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_global(Some(CALL_TIMEOUT))
            .build()
            .new_agent();
        Self {
            agent,
            clock_delta_ms: 0,
            usage: Usage::default(),
            last_rtt: None,
        }
    }

    pub fn usage(&self) -> Usage {
        self.usage
    }

    pub fn clock_delta_ms(&self) -> i64 {
        self.clock_delta_ms
    }

    pub fn last_rtt(&self) -> Option<Duration> {
        self.last_rtt
    }

    /// `GET /fapi/v1/time`, weight 1.
    pub fn server_time(&mut self) -> Result<i64, Error> {
        let t: ServerTime = self.get("/fapi/v1/time", &[])?;
        Ok(t.server_time_ms)
    }

    /// Measure the clock difference against the gateway and remember it.
    ///
    /// Half the round trip is charged to the network, which is the usual
    /// estimate and is honest about the sign: a delta measured without it would
    /// be systematically late by the full trip.
    pub fn sync_clock(&mut self) -> Result<i64, Error> {
        let before = now_ms();
        let server = self.server_time()?;
        let after = now_ms();
        self.clock_delta_ms = server - (before + after) / 2;
        Ok(self.clock_delta_ms)
    }

    /// `GET /fapi/v1/exchangeInfo`, weight 1. 843 KB, 613 symbols as measured.
    pub fn exchange_info(&mut self) -> Result<ExchangeInfo, Error> {
        self.get("/fapi/v1/exchangeInfo", &[])
    }

    /// `GET /fapi/v1/ticker/24hr` for every symbol at once.
    ///
    /// One call carries the 24-hour USDT turnover of the whole catalog, which is
    /// what the volume filters are bounded on — replacing TInvestCore's entire
    /// MOEX-ISS warm-up. Documented weight is 40 for the no-symbol form; what
    /// was MEASURED on 01.10 is `x-mbx-used-weight-1m: 42` right after it, which
    /// is the minute's running total, not this call's price. The meter reads the
    /// header rather than either number.
    pub fn ticker_24h_all(&mut self) -> Result<Vec<Ticker24h>, Error> {
        self.get("/fapi/v1/ticker/24hr", &[])
    }

    /// `GET /fapi/v1/premiumIndex` for every symbol at once: the funding pair.
    ///
    /// Measured 01.10: 766 rows, 167 KB, 1.0 s. One call covers the catalog,
    /// which is why the funding fields of `GetMarketsList` can be filled at
    /// startup instead of waiting for the `markPrice` stream that M1 brings —
    /// the catalog goes to the terminal once per session, so a field left empty
    /// here is empty until the terminal reconnects.
    pub fn premium_index_all(&mut self) -> Result<Vec<PremiumIndex>, Error> {
        self.get("/fapi/v1/premiumIndex", &[])
    }

    fn get<T: DeserializeOwned>(&mut self, path: &str, query: &[(&str, &str)]) -> Result<T, Error> {
        let mut url = format!("{BASE}{path}");
        for (i, (k, v)) in query.iter().enumerate() {
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(k);
            url.push('=');
            url.push_str(v);
        }

        let sent = Instant::now();
        let sending = self
            .agent
            .get(&url)
            .header("Accept", "application/json")
            .call();
        // The elapsed time of the one exchange, whatever came of it: a refusal
        // answers as much as a body does, and a call that never answered
        // carries the time it waited — which is what makes a dead gateway look
        // dead instead of leaving a gap.
        let rtt = sent.elapsed();
        let resp = match sending {
            Ok(r) => {
                self.last_rtt = Some(rtt);
                r
            }
            Err(e) => {
                self.last_rtt = None;
                return Err(e.into());
            }
        };

        self.note_usage(&resp);
        let resp = Self::check_status(resp)?;
        let text = resp
            .into_body()
            .read_to_string()
            .map_err(|e| Error::Transport(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| Error::Decode(format!("{path}: {e}")))
    }

    fn note_usage(&mut self, resp: &Response<Body>) {
        let head = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok())
        };
        // Each header is kept only when this answer carried it: overwriting a
        // known figure with `None` would read as "spent nothing" on the next
        // look, which is the opposite of what a missing header means.
        if let Some(v) = head("x-mbx-used-weight-1m") {
            self.usage.weight_1m = Some(v);
        }
        if let Some(v) = head("x-mbx-order-count-1m") {
            self.usage.orders_1m = Some(v);
        }
        if let Some(v) = head("x-mbx-order-count-10s") {
            self.usage.orders_10s = Some(v);
        }
    }

    fn check_status(resp: Response<Body>) -> Result<Response<Body>, Error> {
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(resp);
        }
        let text = resp.into_body().read_to_string().unwrap_or_default();
        let err: ApiError = serde_json::from_str(&text).unwrap_or_default();
        Err(Error::Api {
            status,
            code: err.code,
            msg: if err.msg.is_empty() { text } else { err.msg },
        })
    }
}

impl Default for Rest {
    fn default() -> Self {
        Self::new()
    }
}

/// Milliseconds since the Unix epoch, the unit every Aster timestamp uses.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
