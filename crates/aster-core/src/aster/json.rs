//! Wire forms of the Aster REST answers this core reads.
//!
//! Aster is a Binance USDⓈ-M Futures clone: every number arrives as a decimal
//! *string* (`"0.1"`, not `0.1`), so each numeric field goes through
//! [`str_f64`]. Reading them as `f64` directly fails the whole document on the
//! first field, which is the kind of break that looks like "the exchange is
//! down".
//!
//! Tolerance is deliberate and goes further than an `Other` arm: Aster ships
//! changes weekly (its changelog lists 30+ entries for 2026 alone), and the
//! catalog is 613 symbols in ONE document — so a single malformed field would
//! otherwise mean no catalog at all and a core that cannot start. Hence unknown
//! `filterType` values fall into `Other`, every field inside a known filter
//! defaults when ABSENT, and [`str_f64`] never fails when one is PRESENT but
//! malformed — the three together are what make the tolerance real, since
//! `serde(default)` alone does nothing for a bad value. What a default must
//! never do is read as a permissive bound: an unusable `LOT_SIZE` leaves
//! `step_size` at 0, and `model::Market::sizable` turns that into a refusal to
//! size the market rather than an unbounded order.

use serde::de::Deserializer;
use serde::Deserialize;

/// Deserialize one of Aster's decimal strings into `f64`.
///
/// An empty string is `0.0`: the exchange uses `""` for "not applicable" on
/// some optional fields, and a hard error there would reject the symbol. So
/// are a bare number and `null`, for the reason spelled out on each arm — a
/// strict decode fails the whole document, not the one field that surprised
/// it, and this core reads answers of 613 and 766 rows.
fn str_f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    struct Tolerant;

    impl serde::de::Visitor<'_> for Tolerant {
        type Value = f64;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a decimal string, a number, or null")
        }

        // The documented shape: `"0.1"`.
        //
        // Never fails, and that is the whole point. `#[serde(default)]` covers
        // an ABSENT field only — a field that is PRESENT and malformed
        // (`"abc"`, or `"NaN"`, which `parse` accepts happily) would still fail
        // the decode of all 613 symbols and leave the core with no catalog at
        // all. So anything unreadable becomes 0.0, which
        // `model::Market::sizable` turns into a refusal to size that one
        // market, counted and named in the `catalog:` line. One market degraded
        // and reported beats the whole catalog lost.
        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<f64, E> {
            Ok(s.parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .unwrap_or(0.0))
        }

        // A BARE NUMBER, which this exchange's dialect is not supposed to send
        // and does send: `"count"`-like integers appear unquoted, and a venue
        // that changes weekly can unquote a price field in any release. Taking
        // it costs nothing; rejecting it costs the whole answer, because serde
        // fails the document and not the field.
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<f64, E> {
            Ok(if v.is_finite() { v } else { 0.0 })
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<f64, E> {
            Ok(v as f64)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<f64, E> {
            Ok(v as f64)
        }

        // `null`, the one shape `#[serde(default)]` does NOT cover: the field
        // is present, so the default never fires, and a strict decode would
        // lose the other 612 symbols over it.
        fn visit_unit<E: serde::de::Error>(self) -> Result<f64, E> {
            Ok(0.0)
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<f64, E> {
            Ok(0.0)
        }
    }

    // `deserialize_any` rather than `deserialize_str`: the point is to accept
    // whichever of the three shapes arrives, and an empty string is 0.0 through
    // the same path as any other unparseable one.
    d.deserialize_any(Tolerant)
}

/// `GET /fapi/v1/time`.
#[derive(Debug, Deserialize)]
pub struct ServerTime {
    #[serde(rename = "serverTime")]
    pub server_time_ms: i64,
}

/// Non-2xx body: `{"code":-1121,"msg":"Invalid symbol."}`.
///
/// `code` is a negative integer, unlike T-Invest's string codes — the policy
/// table in `PLAN.md` is keyed on these numbers.
#[derive(Debug, Default, Deserialize)]
pub struct ApiError {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub msg: String,
}

/// `GET /fapi/v1/exchangeInfo`.
#[derive(Debug, Deserialize)]
pub struct ExchangeInfo {
    #[serde(rename = "serverTime")]
    pub server_time_ms: i64,
    #[serde(default)]
    pub timezone: String,
    #[serde(default, rename = "rateLimits")]
    pub rate_limits: Vec<RateLimit>,
    pub symbols: Vec<SymbolInfo>,
}

/// One row of `exchangeInfo.rateLimits`.
///
/// Measured 01.10: three rows, not the two the general-info page names —
/// `REQUEST_WEIGHT` 2400/MINUTE, `ORDERS` 1200/MINUTE and `ORDERS` 300 per
/// 10 SECOND. The third is only visible here, so the meter reads its windows
/// from this list rather than from constants.
#[derive(Debug, Deserialize)]
pub struct RateLimit {
    #[serde(default, rename = "rateLimitType")]
    pub kind: String,
    #[serde(default)]
    pub interval: String,
    #[serde(default, rename = "intervalNum")]
    pub interval_num: i64,
    /// A zero means the gateway did not state the ceiling, never "no budget":
    /// the meter that reads this must treat it as unknown. Defaulted because a
    /// malformed row here would otherwise cost the whole 613-symbol catalog.
    #[serde(default)]
    pub limit: i64,
}

/// One row of `exchangeInfo.symbols`.
#[derive(Debug, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(default, rename = "marginAsset")]
    pub margin_asset: String,
    /// `TRADING` | `PENDING_TRADING` | `SETTLING` | `CLOSE`. Measured 01.10:
    /// 589 / 5 / 19 / 0 of 613.
    pub status: String,
    #[serde(default, rename = "contractType")]
    pub contract_type: String,
    #[serde(default, rename = "pricePrecision")]
    pub price_precision: i32,
    #[serde(default, rename = "quantityPrecision")]
    pub quantity_precision: i32,
    /// Vanilla perpetuals carry `4133404800000` (year 2101) — a sentinel, not a
    /// date. Measured 01.10: 19 of 596 USDT symbols carry a real one, and they
    /// are the `SETTLING` ones.
    #[serde(default, rename = "deliveryDate")]
    pub delivery_date_ms: i64,
    #[serde(default, rename = "onboardDate")]
    pub onboard_date_ms: i64,
    /// Human display name; empty for most crypto, filled for stock perps.
    #[serde(default)]
    pub name: String,
    /// `nasdaq` | `forex` | `hkstock` | `krstock` | `astock` | `{}` | `""`.
    /// The literal two-character `{}` is how Aster spells "none" for 347 of
    /// them — it is a string, not an object.
    #[serde(default)]
    pub channel: String,
    /// `STOCK`, `Meme`, `AI`, `Top`, `Commodities`, `ETF`, `Semiconductor`,
    /// `AOS2`, `USD1-RWA`, `pre-launch`, in any combination.
    #[serde(default, rename = "underlyingSubType")]
    pub underlying_sub_type: Vec<String>,
    /// A flag that CHANGES DURING THE DAY, and whose meaning is an open
    /// question — parsed, recorded, and read by nothing.
    ///
    /// It was taken for "this symbol has trading sessions" until two live
    /// snapshots on 01.10 disagreed: 121 of the 596 USDT symbols carried `1` at
    /// 10:29 UTC and 20 at 13:30 UTC, the difference being all 101 `nasdaq`
    /// symbols flipping to `0` — at 09:30 ET, the US open. In neither snapshot
    /// did any of the 9 `forex` symbols carry `1`, though forex has sessions.
    /// So `model::Market::has_sessions` is derived from `channel` instead, and
    /// what this field means is to be measured across a full day before
    /// anything is gated on it (`PLAN.md`).
    #[serde(default, rename = "tradingMode")]
    pub trading_mode: i32,
    #[serde(default, rename = "orderTypes")]
    pub order_types: Vec<String>,
    #[serde(default, rename = "timeInForce")]
    pub time_in_force: Vec<String>,
    /// Maintenance margin, percent. Feeds the liquidation price that makes
    /// `DontSellBelowLiq` applicable for the first time.
    #[serde(default, deserialize_with = "str_f64", rename = "maintMarginPercent")]
    pub maint_margin_percent: f64,
    #[serde(
        default,
        deserialize_with = "str_f64",
        rename = "requiredMarginPercent"
    )]
    pub required_margin_percent: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "liquidationFee")]
    pub liquidation_fee: f64,
    /// How far a MARKET order may slip, as a fraction (`0.02`).
    #[serde(default, deserialize_with = "str_f64", rename = "marketTakeBound")]
    pub market_take_bound: f64,
    /// Minimum distance a stop trigger must keep from the current price, as a
    /// fraction (`0.02`). Violating it is `-2021 ORDER_WOULD_IMMEDIATELY_TRIGGER`.
    #[serde(default, deserialize_with = "str_f64", rename = "triggerProtect")]
    pub trigger_protect: f64,
    #[serde(default)]
    pub filters: Vec<Filter>,
}

/// One entry of `symbols[].filters`, tagged by `filterType`.
///
/// Measured 01.10 across all 613 symbols, exactly seven types occur. Aster has
/// no `MAX_NOTIONAL` filter at all — the encoder's `bn_max_notional` zero is
/// the one inherited zero that is honest (see `PLAN.md`).
#[derive(Debug, Deserialize)]
#[serde(tag = "filterType")]
pub enum Filter {
    #[serde(rename = "PRICE_FILTER")]
    Price {
        #[serde(default, deserialize_with = "str_f64", rename = "tickSize")]
        tick_size: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "minPrice")]
        min_price: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "maxPrice")]
        max_price: f64,
    },
    #[serde(rename = "LOT_SIZE")]
    LotSize {
        #[serde(default, deserialize_with = "str_f64", rename = "stepSize")]
        step_size: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "minQty")]
        min_qty: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "maxQty")]
        max_qty: f64,
    },
    /// Separate step and ceiling for MARKET orders; on BTCUSDT the step is the
    /// same but `maxQty` is 120 against LOT_SIZE's 1000.
    #[serde(rename = "MARKET_LOT_SIZE")]
    MarketLotSize {
        #[serde(default, deserialize_with = "str_f64", rename = "stepSize")]
        step_size: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "minQty")]
        min_qty: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "maxQty")]
        max_qty: f64,
    },
    #[serde(rename = "MIN_NOTIONAL")]
    MinNotional {
        #[serde(default, deserialize_with = "str_f64")]
        notional: f64,
    },
    /// How far from the mark an order price may sit. Per-symbol, not one
    /// constant: BTCUSDT 1.02/0.98, others 1.05/0.95. This is the bound that
    /// makes TInvestCore's "limit deep through the book" close impossible here.
    #[serde(rename = "PERCENT_PRICE")]
    PercentPrice {
        #[serde(default, deserialize_with = "str_f64", rename = "multiplierUp")]
        multiplier_up: f64,
        #[serde(default, deserialize_with = "str_f64", rename = "multiplierDown")]
        multiplier_down: f64,
    },
    /// Plain orders per symbol; 200 on every symbol measured.
    #[serde(rename = "MAX_NUM_ORDERS")]
    MaxNumOrders {
        #[serde(default)]
        limit: i64,
    },
    /// Conditional (STOP/TAKE_PROFIT/TRAILING) orders per symbol; **10**. The
    /// ported stop design keeps stops in the core rather than on the exchange,
    /// so it does not meet this ceiling — recorded so nobody "improves" it.
    #[serde(rename = "MAX_NUM_ALGO_ORDERS")]
    MaxNumAlgoOrders {
        #[serde(default)]
        limit: i64,
    },
    /// A filter type this build does not know. Must not fail the catalog.
    #[serde(other)]
    Other,
}

/// One row of `GET /fapi/v1/premiumIndex` without a symbol: the funding pair
/// and the mark price.
///
/// `indexPrice` and `interestRate` are still not read: nothing in the core acts
/// on them. `markPrice` is, since `UpdateMarketsList` has to carry one —
/// the price row's mark field has a `found` flag beside it, so a core that
/// parsed no mark price would be telling the terminal this venue publishes
/// none. Measured 01.10: all 766 rows carry a positive `markPrice`. After
/// startup the same rows come from `!markPrice@arr` ([`MarkPriceUpdate`],
/// converted into this type), so one merge serves both sources.
///
/// Measured 01.10: **766 rows**, more than `exchangeInfo`'s 613 — the answer
/// also carries index symbols (`GNSUSD`, `USD1USD`, `AAPLUSD`) and symbols the
/// catalog no longer lists. So rows are matched to the catalog by symbol and
/// the extras dropped, never zipped by position. In the other direction one
/// symbol is missing: of the 596 USDT perpetuals, **595 have a row and
/// `MBLUSDT` has none** — which is why funding on a market is an `Option` all
/// the way to the wire rather than a zero.
#[derive(Debug, Deserialize)]
pub struct PremiumIndex {
    pub symbol: String,
    /// A FRACTION, not a percent: BTCUSDT answered `0.00008979`, i.e. 0.009 %
    /// per charge. The wire to the terminal wants percent — the conversion is
    /// `moonproto::server::codec::engine::Funding`'s whole doc comment.
    #[serde(default, deserialize_with = "str_f64", rename = "lastFundingRate")]
    pub last_funding_rate: f64,
    /// Unix milliseconds of the next charge, UTC.
    ///
    /// Measured 01.10: present and non-zero on every one of the 595 rows, with
    /// two distinct values across the whole catalog (13:00 and 16:00 UTC) —
    /// funding is charged on a shared schedule, not per symbol. A zero is still
    /// treated as "no funding": it is what a venue without funding would send,
    /// and the terminal's own absence test is this field.
    #[serde(default, rename = "nextFundingTime")]
    pub next_funding_time_ms: i64,
    /// The exchange's mark price — what `PERCENT_PRICE`, the margin and the
    /// liquidation price are all computed against, and therefore the price the
    /// terminal's own band and risk columns mean.
    #[serde(default, deserialize_with = "str_f64", rename = "markPrice")]
    pub mark_price: f64,
}

/// One row of `GET /fapi/v1/ticker/bookTicker` without a symbol: the top of
/// book of every market in one call.
///
/// Measured 01.10: **589 rows for weight 2**, and every one of them carries
/// both sides. 589 is exactly the number of `TRADING` symbols in the same
/// snapshot, so a market that is `SETTLING` or `PENDING_TRADING` simply has no
/// row — matched by symbol like every other merge, never zipped.
///
/// This is what makes M0's price rows real prices. `ticker/24hr` carries a last
/// price but no sides at all, and a bid and an ask invented from the last price
/// would be a zero spread on the money path of every market at once.
#[derive(Debug, Deserialize)]
pub struct BookTicker {
    pub symbol: String,
    #[serde(default, deserialize_with = "str_f64", rename = "bidPrice")]
    pub bid_price: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "askPrice")]
    pub ask_price: f64,
}

/// One row of `GET /fapi/v1/ticker/24hr` without a symbol.
///
/// Measured 01.10: 608 rows for weight 42, and `quoteVolume` is the 24-hour
/// USDT turnover of every symbol. This single call replaces TInvestCore's whole
/// MOEX-ISS warm-up.
#[derive(Debug, Deserialize)]
pub struct Ticker24h {
    pub symbol: String,
    #[serde(default, deserialize_with = "str_f64", rename = "lastPrice")]
    pub last_price: f64,
    /// Turnover in the quote currency (USDT) — the number MoonBot's volume
    /// filters mean.
    #[serde(default, deserialize_with = "str_f64", rename = "quoteVolume")]
    pub quote_volume: f64,
    /// Turnover in the base asset — what a deep-history chart row carries.
    #[serde(default, deserialize_with = "str_f64")]
    pub volume: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "priceChangePercent")]
    pub price_change_percent: f64,
    #[serde(default)]
    pub count: i64,
}

/// One of Aster's decimal strings as a value of its own — for the places a
/// number sits in an array rather than in a named field (book levels), where
/// `deserialize_with` has no field to hang on. Same tolerance as [`str_f64`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
pub struct Dec(#[serde(deserialize_with = "str_f64")] pub f64);

/// One aggregated trade, from `<symbol>@aggTrade` or `GET /fapi/v1/aggTrades`.
///
/// Measured 01.10 the two carry the same fields under the same names; the
/// stream adds `e`, `E` and `s`, which is why `symbol` defaults — the REST
/// answer is per symbol and does not repeat it.
#[derive(Debug, Clone, Deserialize)]
pub struct AggTrade {
    #[serde(default, rename = "s")]
    pub symbol: String,
    /// Aggregate trade id, the paging key of the REST call.
    #[serde(default, rename = "a")]
    pub id: i64,
    #[serde(default, deserialize_with = "str_f64", rename = "p")]
    pub price: f64,
    /// Base quantity, unsigned.
    #[serde(default, deserialize_with = "str_f64", rename = "q")]
    pub qty: f64,
    /// Trade time, unix ms.
    #[serde(default, rename = "T")]
    pub time_ms: i64,
    /// The buyer was the maker — so the aggressor SOLD. This is the side the
    /// tape is signed by: MoonBot's negative quantity is a sell.
    #[serde(default, rename = "m")]
    pub buyer_is_maker: bool,
}

impl AggTrade {
    /// Quantity signed by the aggressor's side, the way `TradesStream` wants it.
    pub fn signed_qty(&self) -> f64 {
        if self.buyer_is_maker {
            -self.qty
        } else {
            self.qty
        }
    }
}

/// `<symbol>@depth20@100ms`: the top twenty levels of each side, WHOLE.
///
/// Measured 01.10: 55 messages in 6 s, every one exactly (20, 20) levels and
/// none with a zero quantity — a snapshot, not a delta, so the core keeps no
/// local book (`PLAN.md`, "Стакан").
#[derive(Debug, Clone, Deserialize)]
pub struct Depth {
    #[serde(default, rename = "s")]
    pub symbol: String,
    #[serde(default, rename = "b")]
    pub bids: Vec<[Dec; 2]>,
    #[serde(default, rename = "a")]
    pub asks: Vec<[Dec; 2]>,
}

/// `<symbol>@kline_<interval>`.
#[derive(Debug, Clone, Deserialize)]
pub struct KlineEvent {
    #[serde(default, rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Kline {
    /// Bar open, unix ms.
    #[serde(default, rename = "t")]
    pub open_ms: i64,
    /// `1m`, `5m`, … — the exchange's own spelling.
    #[serde(default, rename = "i")]
    pub interval: String,
    #[serde(default, deserialize_with = "str_f64", rename = "o")]
    pub open: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "h")]
    pub high: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "l")]
    pub low: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "c")]
    pub close: f64,
    /// BASE volume — the one a chart row carries (`PLAN.md`, "Объёмы").
    #[serde(default, deserialize_with = "str_f64", rename = "v")]
    pub volume: f64,
}

/// One row of `!markPrice@arr`.
///
/// Measured 01.10: every message, every 3 s, carries all 767 symbols — the
/// same complete answer `GET /fapi/v1/premiumIndex` gives, under one-letter
/// names. It is turned into a [`PremiumIndex`] so that one merge
/// (`Catalog::apply_premium_index`) owns both sources and their rules.
#[derive(Debug, Clone, Deserialize)]
pub struct MarkPriceUpdate {
    #[serde(default, rename = "s")]
    pub symbol: String,
    #[serde(default, deserialize_with = "str_f64", rename = "p")]
    pub mark_price: f64,
    #[serde(default, deserialize_with = "str_f64", rename = "r")]
    pub funding_rate: f64,
    #[serde(default, rename = "T")]
    pub next_funding_time_ms: i64,
}

impl From<MarkPriceUpdate> for PremiumIndex {
    fn from(m: MarkPriceUpdate) -> Self {
        Self {
            symbol: m.symbol,
            last_funding_rate: m.funding_rate,
            next_funding_time_ms: m.next_funding_time_ms,
            mark_price: m.mark_price,
        }
    }
}

/// One event of a combined stream, by its `e` field. Anything this core does
/// not read is `Other` rather than a decode error: one new event type must
/// not fail the frame that carries it.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "e")]
pub enum StreamEvent {
    #[serde(rename = "aggTrade")]
    AggTrade(AggTrade),
    #[serde(rename = "depthUpdate")]
    Depth(Depth),
    #[serde(rename = "kline")]
    Kline(KlineEvent),
    #[serde(rename = "markPriceUpdate")]
    MarkPrice(MarkPriceUpdate),
    #[serde(other)]
    Other,
}

/// A combined-stream frame: `{"stream": "...", "data": ...}`. `data` is one
/// event, or an array of them for the `!…@arr` streams.
#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(default)]
    pub stream: String,
    pub data: StreamData,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum StreamData {
    Many(Vec<StreamEvent>),
    One(StreamEvent),
}

/// One row of `GET /fapi/v1/klines`: a 12-cell array of mixed numbers and
/// decimal strings. Read cell by cell, because a tuple struct of fixed length
/// fails the whole answer the day the exchange appends a thirteenth cell.
pub fn kline_row(cells: &[serde_json::Value]) -> Option<Kline> {
    let num = |i: usize| -> Option<f64> {
        match cells.get(i)? {
            serde_json::Value::String(s) => s.parse().ok().filter(|v: &f64| v.is_finite()),
            serde_json::Value::Number(n) => n.as_f64(),
            _ => None,
        }
    };
    Some(Kline {
        open_ms: cells.first()?.as_i64()?,
        interval: String::new(),
        open: num(1)?,
        high: num(2)?,
        low: num(3)?,
        close: num(4)?,
        volume: num(5)?,
    })
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    /// The frames below are the ones the exchange sent on 01.10, cut short.
    #[test]
    fn the_four_stream_shapes_decode() {
        let agg = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1790870843144,"a":88157604,"s":"BTCUSDT","p":"84249.3","q":"0.016","f":148352617,"l":148352617,"T":1790870842950,"m":true}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::AggTrade(t)),
            ..
        } = serde_json::from_str(agg).unwrap()
        else {
            panic!("aggTrade")
        };
        assert_eq!(
            (t.symbol.as_str(), t.price, t.time_ms),
            ("BTCUSDT", 84249.3, 1790870842950)
        );
        assert_eq!(t.signed_qty(), -0.016, "buyer the maker: a sell");

        let depth = r#"{"stream":"btcusdt@depth20@100ms","data":{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["84249.3","0.492"],["84248.5","0.083"]],"a":[["84249.4","1.154"]]}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::Depth(d)),
            ..
        } = serde_json::from_str(depth).unwrap()
        else {
            panic!("depth")
        };
        assert_eq!(d.bids.len(), 2);
        assert_eq!((d.asks[0][0].0, d.asks[0][1].0), (84249.4, 1.154));

        let kline = r#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1790870820000,"T":1790870879999,"s":"BTCUSDT","i":"1m","f":1,"L":2,"o":"84259.8","c":"84249.3","h":"84259.8","l":"84242.3","v":"3.502","n":10,"x":false,"q":"295045.4887","V":"2.903","Q":"244579.6424","B":"0"}}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::Kline(k)),
            ..
        } = serde_json::from_str(kline).unwrap()
        else {
            panic!("kline")
        };
        assert_eq!((k.kline.interval.as_str(), k.kline.volume), ("1m", 3.502));

        let arr = r#"{"stream":"!markPrice@arr","data":[{"e":"markPriceUpdate","E":1,"s":"BTCUSDT","p":"84249.30000000","P":"1","i":"1","r":"0.00009554","T":1790899200000},{"e":"somethingNew","s":"X"}]}"#;
        let Envelope {
            data: StreamData::Many(rows),
            ..
        } = serde_json::from_str(arr).unwrap()
        else {
            panic!("arr")
        };
        let StreamEvent::MarkPrice(m) = &rows[0] else {
            panic!("mark")
        };
        let p = PremiumIndex::from(m.clone());
        assert_eq!((p.mark_price, p.last_funding_rate), (84249.3, 0.00009554));
        assert!(
            matches!(rows[1], StreamEvent::Other),
            "an unknown event is not an error"
        );
    }

    #[test]
    fn a_rest_kline_row_reads_its_cells() {
        let row: Vec<serde_json::Value> = serde_json::from_str(
            r#"[1661299200000,"21514.0","21899.0","21140.0","21351.0","1073.265",1661385599999,"2.3E7",1,"1","1","0",7]"#,
        )
        .unwrap();
        let k = kline_row(&row).unwrap();
        assert_eq!(
            (k.open_ms, k.high, k.volume),
            (1661299200000, 21899.0, 1073.265)
        );
        assert!(kline_row(&row[..3]).is_none(), "a short row is no bar");
    }
}
