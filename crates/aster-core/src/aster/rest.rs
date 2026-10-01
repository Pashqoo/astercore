//! Synchronous REST client for `fapi.asterdex.com`.
//!
//! Aster speaks the Binance USDⓈ-M Futures dialect: plain `GET`/`POST` with
//! query parameters, JSON answers, decimal strings for every number. Signed
//! calls take the v3 form (`sign.rs`) and go to the client's own network
//! ([`Rest::on`]): the account's client lives where its signer does, so its
//! clock is measured against the gateway that checks its nonces.
//!
//! Synchronous on purpose (`ureq`, no Tokio): the core is thread-per-stream,
//! and a runtime would be the only async thing in the process.

use std::fmt;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use ureq::http::Response;
use ureq::{Agent, Body};

use super::json::{
    kline_row, AggTrade, ApiError, Balance, BookTicker, DepthSnapshot, ExchangeInfo, Kline,
    ListenKey, PositionRisk, PremiumIndex, ServerTime, Ticker24h,
};
use super::sign::{Network, Signer};

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
    network: Network,
    /// `server_time - local_time`, in milliseconds, from the last
    /// [`Rest::sync_clock`]. Signed calls add it to their nonce, which the
    /// gateway refuses outside ±60 s of its own clock.
    clock_delta_ms: i64,
    usage: Usage,
    /// Round trip of the last call that got an answer of any status.
    last_rtt: Option<Duration>,
}

impl Rest {
    /// A mainnet client: the market data, which only mainnet carries.
    pub fn new() -> Self {
        Self::on(Network::Mainnet)
    }

    /// A client of `network`'s gateway: every call it makes, the clock
    /// included, goes there. Weight is counted per client, which is per IP
    /// at the gateway — a separate figure, not a separate budget.
    pub fn on(network: Network) -> Self {
        let agent = Agent::config_builder()
            // Aster's refusals carry the code we need to act on, so a non-2xx
            // must reach `check_status` as a response with a body, not as a
            // transport error that has thrown the body away.
            .http_status_as_error(false)
            // A signed query is a bearer for its nonce's minute: a 3xx must not
            // carry it to whatever host the redirect names.
            .max_redirects(0)
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_global(Some(CALL_TIMEOUT))
            .build()
            .new_agent();
        Self {
            agent,
            network,
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

    /// `GET /fapi/v1/ticker/bookTicker` for every symbol at once: the top of
    /// book of the whole catalog.
    ///
    /// Measured 01.10: 589 rows, `x-mbx-used-weight-1m: 2` right after it — the
    /// cheapest call the core makes. It is what the bid and ask of
    /// `UpdateMarketsList` are built from, for the whole catalog — kept over
    /// the `!bookTicker` stream on purpose (`prices.rs` says why) — and at that
    /// weight it can be re-read on a period without a meter.
    pub fn book_ticker_all(&mut self) -> Result<Vec<BookTicker>, Error> {
        self.get("/fapi/v1/ticker/bookTicker", &[])
    }

    /// `GET /fapi/v1/premiumIndex` for every symbol at once: the funding pair.
    ///
    /// Measured 01.10: 766 rows, 167 KB, 1.0 s. One call covers the catalog,
    /// which is why it is read once at startup: the catalog goes to the
    /// terminal once per session, so its funding fields must be filled before
    /// the socket binds, not by the first frame of `!markPrice@arr`, which
    /// carries the same rows every 3 s from then on (`feed.rs`).
    pub fn premium_index_all(&mut self) -> Result<Vec<PremiumIndex>, Error> {
        self.get("/fapi/v1/premiumIndex", &[])
    }

    /// `GET /fapi/v1/klines`: the newest `limit` bars of `interval`, oldest
    /// first, BASE volume in each.
    ///
    /// Measured 01.10: weight 1 for up to 99 bars, 2 for 499, 10 for 1500 (the
    /// most one call returns; 2000 is refused with HTTP 400). BTCUSDT's daily
    /// history reaches back to 2022-08-24.
    ///
    /// A row whose cells do not read is dropped, not fatal: one bar lost from a
    /// chart is better than no chart.
    pub fn klines(
        &mut self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<Kline>, Error> {
        let limit = limit.to_string();
        let rows: Vec<Vec<serde_json::Value>> = self.get(
            "/fapi/v1/klines",
            &[
                ("symbol", symbol),
                ("interval", interval),
                ("limit", &limit),
            ],
        )?;
        Ok(rows.iter().filter_map(|r| kline_row(r)).collect())
    }

    /// `GET /fapi/v1/depth`: one market's book, `limit` levels a side.
    ///
    /// Measured 01.10: 1000 is the most it returns (5000 is refused with
    /// -1130), at a weight of about 20 — the minute's header moved 51 → 73 on
    /// it; 500 cost about 10. A thin market answers with what it has (DOGEUSDT
    /// 408 / 355 levels).
    pub fn depth(&mut self, symbol: &str, limit: u32) -> Result<DepthSnapshot, Error> {
        let limit = limit.to_string();
        self.get("/fapi/v1/depth", &[("symbol", symbol), ("limit", &limit)])
    }

    /// `GET /fapi/v1/aggTrades`: the newest `limit` aggregate trades, or
    /// `limit` of them from an id onwards, oldest first either way.
    ///
    /// Measured 01.10: weight **20** per call whatever the form, at most 1000
    /// rows; BTCUSDT's last hour was ~1800 rows. Aggregate ids are consecutive
    /// per symbol, which is what lets a caller page BACK from the newest page
    /// (`fromId = oldest - limit`) and so always hold the newest trades first.
    pub fn agg_trades(
        &mut self,
        symbol: &str,
        from: AggFrom,
        limit: u32,
    ) -> Result<Vec<AggTrade>, Error> {
        let limit = limit.to_string();
        match from {
            AggFrom::Latest => self.get(
                "/fapi/v1/aggTrades",
                &[("symbol", symbol), ("limit", &limit)],
            ),
            AggFrom::Id(id) => {
                let id = id.to_string();
                self.get(
                    "/fapi/v1/aggTrades",
                    &[("symbol", symbol), ("fromId", &id), ("limit", &limit)],
                )
            }
        }
    }

    /// `GET /fapi/v3/balance`, signed, weight 5: the row of `asset`, or
    /// `None` when the account has none.
    ///
    /// Only that row is decoded, and strictly (`Balance`): the answer lists
    /// every asset of the account (35 on 01.10), and a field the core does not
    /// read, odd in a row it does not read, must not cost it the one it does.
    pub fn balance(&mut self, signer: &mut Signer, asset: &str) -> Result<Option<Balance>, Error> {
        let rows: Vec<serde_json::Value> = self.signed_get(signer, "/fapi/v3/balance", &[])?;
        rows.into_iter()
            .find(|r| r.get("asset").and_then(|a| a.as_str()) == Some(asset))
            .map(|r| {
                serde_json::from_value(r)
                    .map_err(|e| Error::Decode(format!("/fapi/v3/balance {asset}: {e}")))
            })
            .transpose()
    }

    /// `GET /fapi/v3/positionRisk`, signed, weight 5 (docs): one row per
    /// symbol and side, flat ones included — the rows of the symbols `keep`
    /// names.
    ///
    /// Only those rows are decoded, and strictly (`PositionRisk`), for the
    /// reason `balance` gives: the answer covers every symbol of the venue,
    /// and an odd row of one the core does not count must not cost it the
    /// read.
    pub fn position_risk(
        &mut self,
        signer: &mut Signer,
        keep: impl Fn(&str) -> bool,
    ) -> Result<Vec<PositionRisk>, Error> {
        let rows: Vec<serde_json::Value> = self.signed_get(signer, "/fapi/v3/positionRisk", &[])?;
        rows.into_iter()
            .filter(|r| r.get("symbol").and_then(|s| s.as_str()).is_some_and(&keep))
            .map(|r| {
                serde_json::from_value(r)
                    .map_err(|e| Error::Decode(format!("/fapi/v3/positionRisk: {e}")))
            })
            .collect()
    }

    /// `POST /fapi/v3/listenKey`, signed, weight 1: the account's user-data
    /// stream key. An account with an active key gets that same key back with
    /// its 60 minutes renewed (docs; measured 01.10: two calls, one key), so
    /// this one call is both the start and the keepalive — and, unlike `PUT`,
    /// it says WHICH key is active. That matters, because the stream itself
    /// never does: measured 01.10, `/ws/<made-up key>` is accepted with 101
    /// and answers pings, exactly like a live quiet stream.
    pub fn listen_key(&mut self, signer: &mut Signer) -> Result<String, Error> {
        let key: ListenKey = self.signed_post(signer, "/fapi/v3/listenKey", &[])?;
        if key.listen_key.is_empty() {
            return Err(Error::Decode("/fapi/v3/listenKey: empty key".into()));
        }
        Ok(key.listen_key)
    }

    fn get<T: DeserializeOwned>(&mut self, path: &str, query: &[(&str, &str)]) -> Result<T, Error> {
        let mut url = format!("{}{path}", self.network.rest_base());
        for (i, (k, v)) in query.iter().enumerate() {
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(k);
            url.push('=');
            url.push_str(v);
        }
        self.fetch(&url, path)
    }

    /// A v3-signed `GET`: the signed query string goes after `?` as it was
    /// signed, because the gateway checks the signature against those bytes.
    fn signed_get<T: DeserializeOwned>(
        &mut self,
        signer: &mut Signer,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, Error> {
        let signed = self.sign(signer, path, query)?;
        let url = format!("{}{path}?{signed}", self.network.rest_base());
        self.fetch(&url, path)
    }

    /// A v3-signed `POST`: the signed string is the form-encoded body, sent
    /// as it was signed — the docs' own example passes every parameter of a
    /// `POST` "through the request body".
    fn signed_post<T: DeserializeOwned>(
        &mut self,
        signer: &mut Signer,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, Error> {
        let body = self.sign(signer, path, query)?;
        let url = format!("{}{path}", self.network.rest_base());
        const FORM: &str = "application/x-www-form-urlencoded";
        self.exchange(path, |agent| {
            agent
                .post(&url)
                .header("Accept", "application/json")
                .header("Content-Type", FORM)
                .send(&body)
        })
    }

    /// The signed parameter string of one call, its nonce taken from this
    /// client's clock.
    ///
    /// Refused unsent when the signer is on another network than this client:
    /// the nonce would carry a clock measured against the wrong gateway, and
    /// the chain id inside the signature would be refused there anyway.
    fn sign(
        &self,
        signer: &mut Signer,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<String, Error> {
        if signer.network() != self.network {
            return Err(Error::Transport(format!(
                "{path}: the signer is on {}, this client on {}",
                signer.network().name(),
                self.network.name()
            )));
        }
        let now_us = (now_us() + self.clock_delta_ms * 1000).max(0) as u64;
        Ok(signer.sign_query(query, now_us))
    }

    fn fetch<T: DeserializeOwned>(&mut self, url: &str, path: &str) -> Result<T, Error> {
        self.exchange(path, |agent| {
            agent.get(url).header("Accept", "application/json").call()
        })
    }

    /// One call made by `send`, its answer metered, checked and decoded.
    fn exchange<T: DeserializeOwned>(
        &mut self,
        path: &str,
        send: impl FnOnce(&Agent) -> Result<Response<Body>, ureq::Error>,
    ) -> Result<T, Error> {
        let sent = Instant::now();
        let sending = send(&self.agent);
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

/// Where an [`Rest::agg_trades`] page starts.
#[derive(Debug, Clone, Copy)]
pub enum AggFrom {
    /// The newest page.
    Latest,
    /// From this aggregate id onwards.
    Id(i64),
}

/// Milliseconds since the Unix epoch, the unit every Aster timestamp uses.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Microseconds since the Unix epoch, the unit of a v3 nonce.
pub fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
