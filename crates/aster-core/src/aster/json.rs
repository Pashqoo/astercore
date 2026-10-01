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
/// on them. `markPrice` is, since M0's `UpdateMarketsList` has to carry one —
/// the price row's mark field has a `found` flag beside it, so a core that
/// parsed no mark price would be telling the terminal this venue publishes
/// none. Measured 01.10: all 766 rows carry a positive `markPrice`. The
/// `markPrice@1s` stream of M1 replaces this snapshot as the source, not the
/// field.
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
