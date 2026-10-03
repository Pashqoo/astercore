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
    LeverageSet, ListenKey, OrderReply, PositionRisk, PremiumIndex, RawTradeRow, ServerTime,
    SymbolBrackets, Ticker24h,
};
use super::sign::{Network, Signer};
use crate::api_meter::{self, Call};

pub const BASE: &str = "https://fapi.asterdex.com";

/// A call is given up after this long. Measured 01.10 from the Mac:
/// `/fapi/v1/time` answered in 0.30 s and the 843 KB `exchangeInfo` well
/// inside a second, so 30 s is a dead-gateway bound, not a working budget.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub enum Error {
    /// Non-2xx with Aster's own body. `code` is the exchange's negative
    /// integer (`-1021`, `-2019`). The order worker passes a failure on as TEXT, so the readers
    /// of that text ([`msg_has_code`], [`msg_has_status`]) live beside `Display`, which writes
    /// it; `PLAN.md`'s policy table is keyed on the code.
    Api {
        status: u16,
        code: i64,
        msg: String,
    },
    Transport(String),
    Decode(String),
}

/// How an exchange code sits inside an `Error`'s text: `/-4141:` (`api 400/-4141: …`). The
/// order worker hands failures to the engine as text, and the engine recognises codes by this
/// marker — so it is written here, once, for both the writer (`Display`) and the readers
/// ([`msg_has_code`]).
pub fn code_marker(code: i64) -> String {
    format!("/{code}:")
}

/// Whether `msg`, the text of an [`Error::Api`], carries HTTP status `status` (`api 429/…`).
pub fn msg_has_status(msg: &str, status: u16) -> bool {
    msg.contains(&format!("api {status}/"))
}

/// Whether `msg`, the text of an [`Error::Api`], carries the exchange code `code`.
pub fn msg_has_code(msg: &str, code: i64) -> bool {
    msg.contains(&code_marker(code))
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api { status, code, msg } => {
                write!(f, "api {status}{} {msg}", code_marker(*code))
            }
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

    /// Take a clock difference another client of the same gateway measured.
    pub fn set_clock_delta_ms(&mut self, delta_ms: i64) {
        self.clock_delta_ms = delta_ms;
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

    /// Whether [`Self::leverage_oi_remaining`] has a source on this network.
    pub fn has_open_interest_feed(&self) -> bool {
        self.network == Network::Mainnet
    }

    /// The site's own public read of what is left of the open interest at each leverage of one
    /// symbol (`/bapi/futures/v1/public/future/common/symbol/leverageoi/remaining`): the number
    /// behind «Remaining openable notional value» in its leverage dialog, which the signed API
    /// does not carry (NEAR at 50x: `maxNotional` 1 000 000 there, 0 on the site). Not the
    /// exchange's API: another host, outside the weight meter, and no testnet counterpart (an
    /// empty answer). Ascending (leverage, remaining USDT); empty when the site knows no figures.
    pub fn leverage_oi_remaining(&mut self, symbol: &str) -> Result<Vec<(i32, f64)>, Error> {
        if self.network != Network::Mainnet {
            return Ok(Vec::new());
        }
        // A symbol goes into the query as it is: only what exchangeInfo spells like one does
        // (`B-MONEYUSDT` has a hyphen).
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(Error::Decode(format!("leverageoi: odd symbol {symbol:?}")));
        }
        let url = format!(
            "https://www.asterdex.com/bapi/futures/v1/public/future/common/symbol/leverageoi/remaining?symbol={symbol}"
        );
        let resp = self
            .agent
            .get(&url)
            .header("Accept", "application/json")
            .call()?;
        // The agent hands back any status as an answer: a 429 or a WAF page is not a figure.
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(Error::Transport(format!(
                "leverageoi {symbol}: HTTP {status}"
            )));
        }
        let text = resp
            .into_body()
            .read_to_string()
            .map_err(|e| Error::Transport(e.to_string()))?;
        super::json::parse_leverage_oi(&text)
            .map_err(|e| Error::Decode(format!("leverageoi {symbol}: {e}")))
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

    /// The raw fills of `symbol`, oldest first, public: the newest `limit` of them
    /// (`GET /fapi/v3/trades`, weight 1) or `limit` from a trade id onwards
    /// (`GET /fapi/v3/historicalTrades`, weight 20 by the docs), at most 1000 rows.
    ///
    /// Measured 02.10 on UAIUSDT: `historicalTrades` takes no key on the v3 path (the v1 path asks
    /// for one, `-2014`) and pages by `fromId`; `/fapi/v1/trades` ignores `fromId` and always
    /// answers the newest page. Trade ids are consecutive per symbol, which is what lets a caller
    /// page BACK from the newest page (`fromId = oldest - limit`) and so always hold the newest
    /// trades first. The raw tape holds every fill; `aggTrades`, which this replaced for the
    /// history, merged the fills of one taker order (11-37 % fewer rows on liquid markets).
    pub fn raw_trades(
        &mut self,
        symbol: &str,
        from: TradesFrom,
        limit: u32,
    ) -> Result<Vec<AggTrade>, Error> {
        let limit = limit.to_string();
        let rows: Vec<RawTradeRow> = match from {
            TradesFrom::Latest => {
                self.get("/fapi/v3/trades", &[("symbol", symbol), ("limit", &limit)])
            }
            TradesFrom::Id(id) => {
                let id = id.to_string();
                self.get(
                    "/fapi/v3/historicalTrades",
                    &[("symbol", symbol), ("fromId", &id), ("limit", &limit)],
                )
            }
        }?;
        Ok(rows.into_iter().map(AggTrade::from).collect())
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

    /// `POST /fapi/v3/leverage`, signed, weight 1: `symbol`'s leverage on this account. The
    /// answer says what the exchange now holds, which is what the caller trusts.
    pub fn set_leverage(
        &mut self,
        signer: &mut Signer,
        symbol: &str,
        leverage: i32,
    ) -> Result<LeverageSet, Error> {
        let leverage = leverage.to_string();
        self.signed_send(
            Method::Post,
            signer,
            "/fapi/v3/leverage",
            &[("symbol", symbol), ("leverage", &leverage)],
        )
    }

    /// `POST /fapi/v3/marginType`, signed, weight 1: `symbol`'s margin type, `ISOLATED` or
    /// `CROSSED`. The exchange refuses it with `-4046` when the type already is that (nothing to
    /// change), and while the symbol has an open position or order.
    pub fn set_margin_type(
        &mut self,
        signer: &mut Signer,
        symbol: &str,
        margin_type: &str,
    ) -> Result<(), Error> {
        let ack: serde_json::Value = self.signed_send(
            Method::Post,
            signer,
            "/fapi/v3/marginType",
            &[("symbol", symbol), ("marginType", margin_type)],
        )?;
        // `{"code":200,"msg":"success"}`: a 200 whose body names another code is a refusal.
        match ack.get("code").and_then(serde_json::Value::as_i64) {
            Some(code) if code != 200 => Err(Error::Api {
                status: 200,
                code,
                msg: ack
                    .get("msg")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            }),
            _ => Ok(()),
        }
    }

    /// `GET /fapi/v3/leverageBracket`, signed, weight 1: every symbol's notional brackets, the
    /// highest `initialLeverage` of which is the most the account may ask for. Rows the core has no use for are
    /// not decoded, for the reason `balance` gives.
    pub fn leverage_brackets(
        &mut self,
        signer: &mut Signer,
        keep: impl Fn(&str) -> bool,
    ) -> Result<Vec<SymbolBrackets>, Error> {
        let rows: Vec<serde_json::Value> =
            self.signed_get(signer, "/fapi/v3/leverageBracket", &[])?;
        Ok(rows
            .into_iter()
            .filter(|r| r.get("symbol").and_then(|s| s.as_str()).is_some_and(&keep))
            .filter_map(|r| serde_json::from_value(r).ok())
            .collect())
    }

    /// `GET /fapi/v3/positionSide/dual`, signed, weight 30: `true` when the account is in hedge
    /// mode (a long and a short per symbol). The order model is written for one-way mode
    /// (`positionSide` is never sent), where a short nets against a long.
    pub fn dual_side_position(&mut self, signer: &mut Signer) -> Result<bool, Error> {
        let v: serde_json::Value = self.signed_get(signer, "/fapi/v3/positionSide/dual", &[])?;
        v.get("dualSidePosition")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| Error::Decode(format!("/fapi/v3/positionSide/dual: {v}")))
    }

    /// `POST /fapi/v3/listenKey`, signed, weight 1: the account's user-data
    /// stream key. An account with an active key gets that same key back with
    /// its 60 minutes renewed (docs; measured 01.10: two calls, one key), so
    /// this one call is both the start and the keepalive — and, unlike `PUT`,
    /// it says WHICH key is active. That matters, because the stream itself
    /// never does: measured 01.10, `/ws/<made-up key>` is accepted with 101
    /// and answers pings, exactly like a live quiet stream.
    pub fn listen_key(&mut self, signer: &mut Signer) -> Result<String, Error> {
        let key: ListenKey = self.signed_send(Method::Post, signer, "/fapi/v3/listenKey", &[])?;
        if key.listen_key.is_empty() {
            return Err(Error::Decode("/fapi/v3/listenKey: empty key".into()));
        }
        Ok(key.listen_key)
    }

    /// `POST /fapi/v3/order`, signed: a new order with `params` (symbol, side,
    /// type, quantity, price, `newClientOrderId`, …). Weight 0 by the docs;
    /// the order counters (`x-mbx-order-count-*`) are what it spends.
    pub fn new_order(
        &mut self,
        signer: &mut Signer,
        params: &[(&str, &str)],
    ) -> Result<OrderReply, Error> {
        self.signed_send(Method::Post, signer, "/fapi/v3/order", params)
    }

    /// `DELETE /fapi/v3/order`, signed, weight 1: cancel `symbol`'s order.
    pub fn cancel_order(
        &mut self,
        signer: &mut Signer,
        symbol: &str,
        order: OrderRef<'_>,
    ) -> Result<OrderReply, Error> {
        let (k, v) = order.param();
        self.signed_send(
            Method::Delete,
            signer,
            "/fapi/v3/order",
            &[("symbol", symbol), (k, &v)],
        )
    }

    /// `GET /fapi/v3/order`, signed, weight 1: one order of `symbol`, by the
    /// exchange's id or by our key. Not found once it is cancelled or expired
    /// without a fill and 7 days old (docs).
    pub fn query_order(
        &mut self,
        signer: &mut Signer,
        symbol: &str,
        order: OrderRef<'_>,
    ) -> Result<OrderReply, Error> {
        let (k, v) = order.param();
        self.signed_get(signer, "/fapi/v3/order", &[("symbol", symbol), (k, &v)])
    }

    /// `GET /fapi/v3/openOrders` of every symbol, signed, weight 40 (docs):
    /// the account's live orders, to reconcile the core's against.
    pub fn open_orders(&mut self, signer: &mut Signer) -> Result<Vec<OrderReply>, Error> {
        self.signed_get(signer, "/fapi/v3/openOrders", &[])
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

    /// A v3-signed `POST` or `DELETE`: the signed string is the form-encoded
    /// body, sent as it was signed — the docs pass every parameter of these
    /// methods "in the request body".
    fn signed_send<T: DeserializeOwned>(
        &mut self,
        method: Method,
        signer: &mut Signer,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T, Error> {
        let body = self.sign(signer, path, query)?;
        let url = format!("{}{path}", self.network.rest_base());
        const FORM: &str = "application/x-www-form-urlencoded";
        let verb = match method {
            Method::Post => "POST",
            Method::Delete => "DELETE",
        };
        self.exchange(verb, path, |agent| match method {
            Method::Post => agent
                .post(&url)
                .header("Accept", "application/json")
                .header("Content-Type", FORM)
                .send(&body),
            Method::Delete => agent
                .delete(&url)
                .header("Accept", "application/json")
                .header("Content-Type", FORM)
                .force_send_body()
                .send(&body),
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
        self.exchange("GET", path, |agent| {
            agent.get(url).header("Accept", "application/json").call()
        })
    }

    /// One call made by `send`, its answer metered, checked and decoded.
    fn exchange<T: DeserializeOwned>(
        &mut self,
        verb: &str,
        path: &str,
        send: impl FnOnce(&Agent) -> Result<Response<Body>, ureq::Error>,
    ) -> Result<T, Error> {
        let meter = api_meter::global();
        let key = format!("{verb} {path}");
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
                if let Some(m) = meter {
                    m.note_now(
                        &key,
                        Call::Err {
                            rtt_ms: rtt.as_millis() as i64,
                        },
                    );
                }
                return Err(e.into());
            }
        };

        let figures = Self::usage_of(&resp);
        self.note_usage(figures);
        let status = resp.status().as_u16();
        if let Some(m) = meter {
            let rtt_ms = rtt.as_millis() as i64;
            let call = if (200..300).contains(&status) {
                Call::Ok { rtt_ms }
            } else {
                Call::Err { rtt_ms }
            };
            m.note_now(&key, call);
            // This answer's own figures: a remembered one could carry the
            // last minute's count into the next.
            m.note_usage(
                now_ms(),
                [figures.weight_1m, figures.orders_1m, figures.orders_10s],
            );
        }
        if matches!(status, 418 | 429) {
            // 429 (over a limit) and 418 (banned for ignoring one) name how
            // long to wait. Said here, once, for every caller; the waiting is
            // each caller's own (`feed.rs` pauses its REST worker and its
            // warm-up on these codes). Not a gate on the whole process: an
            // exit or a cancel must still go out while a warm-up is told to
            // wait — the order calls have their own budget (`ORDERS`).
            // At most one line a few seconds: a burst of refusals is one
            // event, and the journal is read by a person.
            static SAID_AT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
            let now = now_ms();
            let said = SAID_AT.load(std::sync::atomic::Ordering::Relaxed);
            if now - said >= 5_000 {
                SAID_AT.store(now, std::sync::atomic::Ordering::Relaxed);
                let after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("?")
                    .trim()
                    .to_string();
                log::warn!("{key}: HTTP {status}, Retry-After {after} s");
            }
        }
        let resp = Self::check_status(resp)?;
        let text = resp
            .into_body()
            .read_to_string()
            .map_err(|e| Error::Transport(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| Error::Decode(format!("{path}: {e}")))
    }

    /// What one answer's headers say this IP has spent.
    fn usage_of(resp: &Response<Body>) -> Usage {
        let head = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok())
        };
        Usage {
            weight_1m: head("x-mbx-used-weight-1m"),
            orders_1m: head("x-mbx-order-count-1m"),
            orders_10s: head("x-mbx-order-count-10s"),
        }
    }

    fn note_usage(&mut self, seen: Usage) {
        // Each header is kept only when this answer carried it: overwriting a
        // known figure with `None` would read as "spent nothing" on the next
        // look, which is the opposite of what a missing header means.
        if let Some(v) = seen.weight_1m {
            self.usage.weight_1m = Some(v);
        }
        if let Some(v) = seen.orders_1m {
            self.usage.orders_1m = Some(v);
        }
        if let Some(v) = seen.orders_10s {
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
            // Not the exchange's JSON (a gateway's HTML 502), or a message of unknown length:
            // this text goes to the journal and to Telegram, so it is cut.
            msg: shorten(if err.msg.is_empty() { &text } else { &err.msg }),
        })
    }
}

/// The first 200 characters of a text, one line: a body that is not the exchange's JSON (a
/// gateway's HTML), or a JSON message of unknown length — it is going to a journal and a chat.
pub(crate) fn shorten(text: &str) -> String {
    const MAX: usize = 200;
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= MAX {
        return flat;
    }
    let cut: String = flat.chars().take(MAX).collect();
    format!("{cut}… ({} bytes)", text.len())
}

impl Default for Rest {
    fn default() -> Self {
        Self::new()
    }
}

/// The methods a signed call is sent with besides `GET`.
#[derive(Debug, Clone, Copy)]
enum Method {
    Post,
    Delete,
}

/// Which order a cancel or a query names: the exchange's id, or our key.
#[derive(Debug, Clone, Copy)]
pub enum OrderRef<'a> {
    Id(&'a str),
    Key(&'a str),
}

impl OrderRef<'_> {
    fn param(self) -> (&'static str, String) {
        match self {
            Self::Id(id) => ("orderId", id.to_string()),
            Self::Key(key) => ("origClientOrderId", key.to_string()),
        }
    }
}

/// Where a [`Rest::raw_trades`] page starts.
#[derive(Debug, Clone, Copy)]
pub enum TradesFrom {
    /// The newest page.
    Latest,
    /// From this trade id onwards.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A gateway's HTML page in place of the exchange's JSON is cut to one short line before it
    /// goes to the journal and to Telegram.
    #[test]
    fn a_body_that_is_not_json_is_cut_to_one_short_line() {
        let html = format!(
            "<html>\n<body>\n{}\n</body></html>",
            "502 Bad Gateway ".repeat(100)
        );
        let line = shorten(&html);
        assert!(!line.contains('\n') && line.chars().count() < 260, "{line}");
        assert!(line.starts_with("<html> <body> 502 Bad Gateway"));
        assert_eq!(shorten("  short \n body "), "short body");
    }

    /// The engine finds an exchange code in the text of the error the worker made: the writer
    /// and the readers share `code_marker`, so a change of the format cannot part them.
    #[test]
    fn a_code_is_found_in_the_text_the_error_makes() {
        let e = Error::Api {
            status: 400,
            code: -4141,
            msg: "Symbol is closed for new positions.".into(),
        };
        assert_eq!(
            e.to_string(),
            "api 400/-4141: Symbol is closed for new positions."
        );
        assert!(msg_has_code(&e.to_string(), -4141));
        assert!(!msg_has_code(&e.to_string(), -4140));
        // A code is not found inside another number, nor in a message that only mentions it.
        assert!(!msg_has_code("api 400/-41410: x", -4141));
        assert!(!msg_has_code("transport: reset", -4141));
        assert!(msg_has_status(&e.to_string(), 400));
        assert!(!msg_has_status(&e.to_string(), 429));
        assert!(!msg_has_status("transport: api 429 reset", 429));
    }
}
