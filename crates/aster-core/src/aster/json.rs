//! Wire forms of the Aster REST answers this core reads.
//!
//! Aster is a Binance USDⓈ-M Futures clone: every number arrives as a decimal
//! *string* (`"0.1"`, not `0.1`), so each numeric field of the market data goes
//! through [`str_f64`], and each one of the account through the strict
//! [`dec_f64`] — the tolerance below is the market data's, and the account's
//! reason to refuse it is on [`dec_f64`]. Reading them as `f64` directly fails the whole document on the
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

/// An integer field read the way `str_f64` reads a decimal: `null`, a string, a float or garbage
/// is not worth the whole document (a 767-row `!markPrice@arr` frame every 3 s). A number is
/// taken (a float truncated), a numeric string parsed, anything else is 0.
///
/// ONLY for a field where 0 is the same as «absent» (a funding time, a count, a limit, a trading
/// mode). Never for an identifier, a time that orders data, or a precision: a 0 there is a
/// plausible value that would be acted on (an order id "0", a depth link, a bar dated 1970), and
/// failing the frame is the safer answer. Those fields stay strict `#[serde(default)]`.
fn tolerant_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    struct Tolerant;

    impl<'de> serde::de::Visitor<'de> for Tolerant {
        type Value = i64;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an integer, a numeric string, or null")
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<i64, E> {
            Ok(i64::try_from(v).unwrap_or(i64::MAX))
        }

        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<i64, E> {
            Ok(if v.is_finite() { v as i64 } else { 0 })
        }

        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<i64, E> {
            let s = s.trim();
            Ok(s.parse::<i64>()
                .ok()
                .or_else(|| {
                    s.parse::<f64>()
                        .ok()
                        .filter(|v| v.is_finite())
                        .map(|v| v as i64)
                })
                .unwrap_or(0))
        }

        fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<i64, E> {
            Ok(0)
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<i64, E> {
            Ok(0)
        }

        // `[]` and `{}` where a number belongs: consumed whole (the document goes on from the
        // right place) and read as 0.
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<i64, A::Error> {
            while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
            Ok(0)
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<i64, A::Error> {
            while map
                .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                .is_some()
            {}
            Ok(0)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<i64, E> {
            Ok(0)
        }
    }

    d.deserialize_any(Tolerant)
}

fn int_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    tolerant_i64(d)
}

fn int_i32<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    tolerant_i64(d).map(|v| i32::try_from(v).unwrap_or(0))
}

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

/// `POST /fapi/v3/listenKey`: `{"listenKey": "…"}`.
#[derive(Deserialize)]
pub struct ListenKey {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

/// The key opens the account's order stream: never printed.
impl std::fmt::Debug for ListenKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListenKey").finish_non_exhaustive()
    }
}

/// Non-2xx body: `{"code":-1121,"msg":"Invalid symbol."}`.
///
/// `code` is a negative integer, unlike T-Invest's string codes — the policy
/// table in `PLAN.md` is keyed on these numbers.
#[derive(Debug, Default, Deserialize)]
pub struct ApiError {
    #[serde(default, deserialize_with = "int_i64")]
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
    #[serde(default, deserialize_with = "int_i64", rename = "intervalNum")]
    pub interval_num: i64,
    /// A zero means the gateway did not state the ceiling, never "no budget":
    /// the meter that reads this must treat it as unknown. Defaulted because a
    /// malformed row here would otherwise cost the whole 613-symbol catalog.
    #[serde(default, deserialize_with = "int_i64")]
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
    #[serde(default, deserialize_with = "int_i64", rename = "onboardDate")]
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
    #[serde(default, deserialize_with = "int_i32", rename = "tradingMode")]
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
        #[serde(default, deserialize_with = "int_i64")]
        limit: i64,
    },
    /// Conditional (STOP/TAKE_PROFIT/TRAILING) orders per symbol; **10**. The
    /// ported stop design keeps stops in the core rather than on the exchange,
    /// so it does not meet this ceiling — recorded so nobody "improves" it.
    #[serde(rename = "MAX_NUM_ALGO_ORDERS")]
    MaxNumAlgoOrders {
        #[serde(default, deserialize_with = "int_i64")]
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
    #[serde(default, deserialize_with = "int_i64", rename = "nextFundingTime")]
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
    #[serde(default, deserialize_with = "int_i64")]
    pub count: i64,
}

/// One of Aster's decimal strings as a value of its own — for the places a
/// number sits in an array rather than in a named field (book levels), where
/// `deserialize_with` has no field to hang on. Same tolerance as [`str_f64`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
pub struct Dec(#[serde(deserialize_with = "str_f64")] pub f64);

/// Deserialize an ACCOUNT number: a decimal string or a bare number, and
/// nothing else.
///
/// The opposite of [`str_f64`] on purpose. The catalog's tolerance trades one
/// market's field for the other 612 symbols; on the account the trade runs the
/// other way — a malformed `positionAmt` read as 0.0 is an open position the
/// terminal shows as flat, and a malformed balance is money that is not there.
/// Failing the decode fails the one refresh, the last good snapshot stays,
/// and the error is logged (`account.rs`).
fn dec_f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    struct Strict;

    impl serde::de::Visitor<'_> for Strict {
        type Value = f64;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a finite decimal string or number")
        }

        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<f64, E> {
            s.parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| E::custom(format!("not a finite decimal: {s:?}")))
        }

        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<f64, E> {
            if v.is_finite() {
                Ok(v)
            } else {
                Err(E::custom("not a finite number"))
            }
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<f64, E> {
            Ok(v as f64)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<f64, E> {
            Ok(v as f64)
        }
    }

    d.deserialize_any(Strict)
}

/// One asset of `GET /fapi/v3/balance` (signed, weight 5), the fields this
/// core reads.
///
/// Required and strict ([`dec_f64`]): an absent or malformed amount fails the
/// row rather than reading as an empty wallet. Only the row the core reads is
/// decoded this way (`Rest::balance`), so another asset's odd row cannot fail
/// it.
#[derive(Debug, Clone, Deserialize)]
pub struct Balance {
    pub asset: String,
    /// Wallet balance: deposits plus realized PnL, isolated wallets included,
    /// unrealized PnL NOT included.
    #[serde(deserialize_with = "dec_f64")]
    pub balance: f64,
    #[serde(deserialize_with = "dec_f64", rename = "availableBalance")]
    pub available: f64,
}

/// One row of `GET /fapi/v3/positionRisk` (signed, weight 5).
///
/// Every symbol comes back, flat ones with `positionAmt` `"0.000"` (the docs'
/// one-way example). One-way mode answers one `BOTH` row a symbol, hedge mode a
/// `LONG` and a `SHORT` row, each with its own entry price and with
/// `positionAmt` already signed (the docs' SHORT row is `"-10.000"`). Read
/// strictly, like [`Balance`], and like it only for the rows the core counts
/// (`Rest::position_risk`).
#[derive(Debug, Clone, Deserialize)]
pub struct PositionRisk {
    pub symbol: String,
    /// Signed base quantity: long > 0.
    #[serde(deserialize_with = "dec_f64", rename = "positionAmt")]
    pub amount: f64,
    #[serde(deserialize_with = "dec_f64", rename = "entryPrice")]
    pub entry_price: f64,
    /// In the symbol's margin asset, at the mark price.
    #[serde(deserialize_with = "dec_f64", rename = "unRealizedProfit")]
    pub unrealized: f64,
    /// The symbol's leverage on this account (`"leverage": "21"`), whether or not a position is
    /// open. Lenient where the money fields are strict: a row that does not say, or says
    /// something that is no whole number above zero, reads as `None` — the terminal then shows
    /// the leverage as unknown, and the position it sits beside still counts.
    #[serde(default, deserialize_with = "leverage_of")]
    pub leverage: Option<i32>,
    /// The margin type as the row states it — `ISOLATED`/`CROSSED`, in either case on the wire
    /// (the docs' fixtures use lowercase) — or `None` when it does not.
    #[serde(default, rename = "marginType")]
    pub margin_type: Option<String>,
}

fn leverage_of<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(v.and_then(|v| match v {
        serde_json::Value::String(s) => s.trim().parse::<i32>().ok(),
        serde_json::Value::Number(n) => n.as_i64().and_then(|n| i32::try_from(n).ok()),
        _ => None,
    })
    .filter(|&l| l > 0))
}

/// `POST /fapi/v3/leverage`: `{"leverage": 21, "maxNotionalValue": "1000000", "symbol": "BTCUSDT"}`.
#[derive(Debug, Clone, Deserialize)]
pub struct LeverageSet {
    pub leverage: i32,
    pub symbol: String,
}

/// One symbol of `GET /fapi/v3/leverageBracket`: the brackets run from the smallest notional up,
/// and the highest `initialLeverage` of them is the most the account may ask for.
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolBrackets {
    pub symbol: String,
    #[serde(default)]
    pub brackets: Vec<LeverageBracket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LeverageBracket {
    #[serde(rename = "initialLeverage")]
    pub initial_leverage: i32,
    /// The largest notional (USDT) this bracket's leverage holds for; `0` when the row states
    /// none, which no limit is read against.
    #[serde(default, rename = "notionalCap", deserialize_with = "cap_of")]
    pub notional_cap: f64,
}

/// Lenient, like the leverage figure: the cap is read when it is a positive number (or a string
/// of one) and is `0` otherwise, so a row without it still gives its maximum.
fn cap_of<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(v.and_then(|v| match v {
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
        serde_json::Value::Number(n) => n.as_f64(),
        _ => None,
    })
    .filter(|c| c.is_finite() && *c > 0.0)
    .unwrap_or(0.0))
}

impl SymbolBrackets {
    /// The highest leverage any bracket allows, `None` when there is none above zero.
    pub fn max_leverage(&self) -> Option<i32> {
        self.brackets
            .iter()
            .map(|b| b.initial_leverage)
            .filter(|&l| l > 0)
            .max()
    }

    /// The bracket leverages strictly between `above` (exclusive; `None` = none) and `below`
    /// (exclusive), highest first, each once.
    pub fn leverages_between(&self, above: Option<i32>, below: i32) -> Vec<i32> {
        let mut out: Vec<i32> = self
            .brackets
            .iter()
            .map(|b| b.initial_leverage)
            .filter(|&l| l > 0 && l < below && above.is_none_or(|a| l > a))
            .collect();
        out.sort_unstable_by(|a, b| b.cmp(a));
        out.dedup();
        out
    }

    /// The highest leverage that still holds a position of `limit` USDT: a bracket's leverage
    /// holds up to its `notionalCap`, and the caps shrink as the leverage grows. When `limit`
    /// is above every cap, the leverage of the widest bracket (the lowest the exchange offers);
    /// `None` when no bracket states a cap, so nothing is guessed.
    pub fn leverage_for_limit(&self, limit: f64) -> Option<i32> {
        let capped = || {
            self.brackets
                .iter()
                .filter(|b| b.initial_leverage > 0 && b.notional_cap > 0.0)
        };
        capped()
            .filter(|b| b.notional_cap >= limit)
            .map(|b| b.initial_leverage)
            .max()
            .or_else(|| {
                capped()
                    .max_by(|a, b| a.notional_cap.total_cmp(&b.notional_cap))
                    .map(|b| b.initial_leverage)
            })
    }
}

/// An order as `POST`, `DELETE` and `GET /fapi/v3/order` answer it (docs:
/// the three answers carry the same fields).
///
/// Strict, like the account rows ([`dec_f64`]): a quantity misread as 0 is a
/// fill the core never counts, so a malformed one fails the call and the
/// model reconciles the order instead.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderReply {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(default, rename = "clientOrderId")]
    pub client_order_id: String,
    pub symbol: String,
    pub status: String,
    pub side: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(deserialize_with = "dec_f64", rename = "origQty")]
    pub orig_qty: f64,
    #[serde(deserialize_with = "dec_f64", rename = "executedQty")]
    pub executed_qty: f64,
    #[serde(deserialize_with = "dec_f64")]
    pub price: f64,
    #[serde(default, deserialize_with = "dec_f64", rename = "avgPrice")]
    pub avg_price: f64,
    #[serde(default, rename = "updateTime")]
    pub update_ms: i64,
}

/// One trade as the core reads it: a raw fill of `<symbol>@trade` (the live tape,
/// [`StreamEvent::Trade`]) or of `GET /fapi/v3/trades` (the last-hour history, via
/// [`RawTradeRow`]).
///
/// Measured 01.10 the `aggTrade` stream and the `aggTrades` rows (which the core no longer reads)
/// carried the same fields under the same names, and the stream added `e`, `E` and `s`, which is
/// why `symbol` defaults. The raw rows of `trades` name theirs differently (`RawTradeRow`); the
/// `trade` frame of the stream is read as it is, with `a` absent.
#[derive(Debug, Clone, Deserialize)]
pub struct AggTrade {
    #[serde(default, rename = "s")]
    pub symbol: String,
    /// The trade id, the paging key of `historicalTrades` (set from [`RawTradeRow`]). A live `trade`
    /// frame has no `a` (its id is `t`, which the core does not read): 0 there, so nothing may key
    /// on it for live prints.
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

/// One row of `GET /fapi/v3/trades` and `/fapi/v3/historicalTrades`, the raw fills:
/// `{"id","price","qty","quoteQty","time","isBuyerMaker"}`. Read as an [`AggTrade`] (`id` is the
/// trade id, the paging key of `historicalTrades`).
#[derive(Debug, Clone, Deserialize)]
pub struct RawTradeRow {
    /// Without an id (0) the paging back stops: a row that cannot be paged from fails nothing.
    #[serde(default)]
    pub id: i64,
    #[serde(default, deserialize_with = "str_f64")]
    pub price: f64,
    #[serde(default, deserialize_with = "str_f64")]
    pub qty: f64,
    #[serde(default)]
    pub time: i64,
    #[serde(default, rename = "isBuyerMaker")]
    pub buyer_is_maker: bool,
}

impl From<RawTradeRow> for AggTrade {
    fn from(r: RawTradeRow) -> Self {
        Self {
            symbol: String::new(),
            id: r.id,
            price: r.price,
            qty: r.qty,
            time_ms: r.time,
            buyer_is_maker: r.buyer_is_maker,
        }
    }
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

/// `<symbol>@depth@100ms`: the levels that changed, a zero quantity removing
/// one, chained by update ids (`book.rs` stitches them to a [`DepthSnapshot`]).
///
/// Measured 01.10: `U`/`u` are one sequence across all symbols, `pu` is the
/// previous event's `u` (0 breaks in 72 events), and even a quiet market sends
/// an event about every 150 ms; BTCUSDT carries 24 levels an event on average.
#[derive(Debug, Clone, Deserialize)]
pub struct Depth {
    #[serde(default, rename = "s")]
    pub symbol: String,
    #[serde(default, rename = "U")]
    pub first_id: i64,
    #[serde(default, rename = "u")]
    pub last_id: i64,
    #[serde(default, rename = "pu")]
    pub prev_id: i64,
    #[serde(default, rename = "b")]
    pub bids: Vec<[Dec; 2]>,
    #[serde(default, rename = "a")]
    pub asks: Vec<[Dec; 2]>,
}

/// `GET /fapi/v1/depth`: the book as of `lastUpdateId`, bids best first.
#[derive(Debug, Clone, Deserialize)]
pub struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_id: i64,
    #[serde(default)]
    pub bids: Vec<[Dec; 2]>,
    #[serde(default)]
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
    /// QUOTE turnover (USDT) — the one the screener's retained 5m candles
    /// carry (`candles5m`). NaN on a REST row that carries none.
    #[serde(default, deserialize_with = "str_f64", rename = "q")]
    pub quote_volume: f64,
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
    #[serde(default, deserialize_with = "int_i64", rename = "T")]
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
    /// `<symbol>@trade`: one fill. The stream the core reads, because `aggTrade` merges the fills
    /// of one taker order at one price (measured 02.10: 11-37 % fewer prints on BTCUSDT and
    /// SOLUSDT than the exchange's raw tape in the same window). The fields the core reads are
    /// those of [`AggTrade`] under the same names, so it is decoded as one (`a` defaults to 0).
    #[serde(rename = "trade")]
    Trade(AggTrade),
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

/// One event of the account's user-data stream (`/ws/<listenKey>`), which
/// sends bare events, not combined-stream envelopes.
///
/// The core does not build its account from these: `ACCOUNT_UPDATE` carries no
/// available balance, and an order placed or cancelled moves the free margin
/// without one (docs). Each event is a reason to re-read the account now
/// (`account.rs`); what it says is read only for the journal. Every field
/// therefore defaults, an event type the core does not name is `Other`, and
/// a frame that does not decode at all is read as `Other` too
/// (`account::user_event`): a changed shape costs a line of the journal,
/// never the wake-up.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "e")]
pub enum UserEvent {
    #[serde(rename = "ACCOUNT_UPDATE")]
    Account(AccountUpdate),
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    Order(Box<OrderUpdate>),
    #[serde(rename = "MARGIN_CALL")]
    MarginCall,
    /// The key behind the open stream expired; no more events on it until a
    /// new key is used (docs).
    #[serde(rename = "listenKeyExpired")]
    Expired,
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct AccountUpdate {
    #[serde(default, rename = "a")]
    pub update: AccountUpdateData,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct AccountUpdateData {
    /// `ORDER`, `FUNDING_FEE`, `DEPOSIT`, …
    #[serde(default, rename = "m")]
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct OrderUpdate {
    #[serde(default, rename = "o")]
    pub order: OrderEvent,
}

/// The order inside an `ORDER_TRADE_UPDATE`, as the exchange wrote it:
/// quantities and prices stay the decimal strings they arrived as. Every field
/// defaults, so an odd one costs no more than a line of the journal; the
/// report the order model reads is parsed from them strictly
/// (`OrderUpdate::from_event`), and an unreadable one is not a report.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct OrderEvent {
    #[serde(default, rename = "s")]
    pub symbol: String,
    #[serde(default, rename = "c")]
    pub client_id: String,
    #[serde(default, rename = "i")]
    pub id: i64,
    #[serde(default, rename = "S")]
    pub side: String,
    #[serde(default, rename = "o")]
    pub kind: String,
    /// Execution type: `NEW`, `TRADE`, `CANCELED`, `EXPIRED`, `CALCULATED`.
    #[serde(default, rename = "x")]
    pub execution: String,
    /// Order status: `NEW`, `PARTIALLY_FILLED`, `FILLED`, …
    #[serde(default, rename = "X")]
    pub status: String,
    #[serde(default, rename = "q")]
    pub qty: String,
    #[serde(default, rename = "p")]
    pub price: String,
    /// Filled so far.
    #[serde(default, rename = "z")]
    pub filled: String,
    /// Price of the last fill.
    #[serde(default, rename = "L")]
    pub last_price: String,
    /// Average fill price.
    #[serde(default, rename = "ap")]
    pub avg_price: String,
    /// Transaction time, ms.
    #[serde(default, rename = "T")]
    pub time_ms: i64,
    /// Commission of this execution (a `TRADE` event), its asset, and the
    /// execution's trade id.
    #[serde(default, rename = "n")]
    pub commission: String,
    #[serde(default, rename = "N")]
    pub commission_asset: String,
    #[serde(default, rename = "t")]
    pub trade_id: i64,
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
        // Cell 7 (measured 01.10: all 12 cells present on every row). Not a
        // reason to drop the bar if it ever goes missing: a CoinCard row does
        // not read it, and a screener candle without it still has its range.
        // NaN rather than 0, so that `Candles5m::seed` can tell "no figure"
        // from "nothing traded" and keep what the tape counted.
        quote_volume: num(7).unwrap_or(f64::NAN),
    })
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    /// The docs' payloads (`aster-finance-futures-api-v3.md`, "User Data
    /// Streams"), cut short.
    #[test]
    fn user_events_decode_and_an_unknown_one_is_other() {
        let acc = r#"{"e":"ACCOUNT_UPDATE","E":1564745798939,"T":1564745798938,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"122624.12345678","cw":"100.12345678","bc":"50.12345678"}],"P":[{"s":"BTCUSDT","pa":"0","ep":"0.00000","cr":"200","up":"0","mt":"isolated","iw":"0.00000000","ps":"BOTH"}]}}"#;
        match serde_json::from_str::<UserEvent>(acc).unwrap() {
            UserEvent::Account(a) => assert_eq!(a.update.reason, "ORDER"),
            e => panic!("{e:?}"),
        }
        let ord = r#"{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"T":1568879465650,"o":{"s":"BTCUSDT","c":"TEST","S":"SELL","o":"TRAILING_STOP_MARKET","f":"GTC","q":"0.001","p":"0","ap":"0","sp":"7103.04","x":"NEW","X":"NEW","i":8886774,"l":"0","z":"0","L":"0","T":1568879465651,"t":0,"m":false,"R":false,"ps":"LONG","rp":"0"}}"#;
        match serde_json::from_str::<UserEvent>(ord).unwrap() {
            UserEvent::Order(o) => {
                let o = o.order;
                assert_eq!(
                    (o.symbol.as_str(), o.id, o.side.as_str(), o.status.as_str()),
                    ("BTCUSDT", 8886774, "SELL", "NEW")
                );
                assert_eq!(
                    (o.qty.as_str(), o.kind.as_str()),
                    ("0.001", "TRAILING_STOP_MARKET")
                );
            }
            e => panic!("{e:?}"),
        }
        let exp = r#"{"e":"listenKeyExpired","E":1576653824250}"#;
        assert!(matches!(
            serde_json::from_str::<UserEvent>(exp).unwrap(),
            UserEvent::Expired
        ));
        let cfg = r#"{"e":"ACCOUNT_CONFIG_UPDATE","E":1611646737479,"T":1611646737476,"ac":{"s":"BTCUSDT","l":25}}"#;
        assert!(matches!(
            serde_json::from_str::<UserEvent>(cfg).unwrap(),
            UserEvent::Other
        ));
    }

    /// The frames below are the ones the exchange sent on 01.10, cut short.
    #[test]
    fn the_four_stream_shapes_decode() {
        // The raw tape's frame, as measured on 02.10.
        let agg = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1790870843144,"T":1790870842950,"s":"BTCUSDT","t":148352617,"p":"84249.3","q":"0.016","X":"MARKET","m":true}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::Trade(t)),
            ..
        } = serde_json::from_str(agg).unwrap()
        else {
            panic!("trade")
        };
        assert_eq!(
            (t.symbol.as_str(), t.price, t.time_ms),
            ("BTCUSDT", 84249.3, 1790870842950)
        );
        assert_eq!(t.signed_qty(), -0.016, "buyer the maker: a sell");

        let depth = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":577800763009,"u":577800765321,"pu":577800762691,"b":[["84249.3","0.492"],["84248.5","0.083"]],"a":[["84249.4","1.154"]]}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::Depth(d)),
            ..
        } = serde_json::from_str(depth).unwrap()
        else {
            panic!("depth")
        };
        assert_eq!(d.bids.len(), 2);
        assert_eq!((d.asks[0][0].0, d.asks[0][1].0), (84249.4, 1.154));
        assert_eq!(
            (d.first_id, d.last_id, d.prev_id),
            (577800763009, 577800765321, 577800762691)
        );

        let snap: DepthSnapshot = serde_json::from_str(
            r#"{"lastUpdateId":577800763983,"E":1790875796240,"T":1790875796200,"bids":[["84249.3","0.492"]],"asks":[["84249.4","1.154"],["84250.0","0"]]}"#,
        )
        .unwrap();
        assert_eq!(
            (snap.last_id, snap.bids.len(), snap.asks.len()),
            (577800763983, 1, 2)
        );

        let kline = r#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1790870820000,"T":1790870879999,"s":"BTCUSDT","i":"1m","f":1,"L":2,"o":"84259.8","c":"84249.3","h":"84259.8","l":"84242.3","v":"3.502","n":10,"x":false,"q":"295045.4887","V":"2.903","Q":"244579.6424","B":"0"}}}"#;
        let Envelope {
            data: StreamData::One(StreamEvent::Kline(k)),
            ..
        } = serde_json::from_str(kline).unwrap()
        else {
            panic!("kline")
        };
        assert_eq!(
            (
                k.kline.interval.as_str(),
                k.kline.volume,
                k.kline.quote_volume
            ),
            ("1m", 3.502, 295045.4887)
        );

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
            (k.open_ms, k.high, k.volume, k.quote_volume),
            (1661299200000, 21899.0, 1073.265, 2.3e7)
        );
        assert!(kline_row(&row[..3]).is_none(), "a short row is no bar");
    }

    #[test]
    fn account_rows_read_the_documented_v3_shapes() {
        // `/fapi/v3/balance` and the hedge-mode `/fapi/v3/positionRisk`
        // examples of the v3 docs, trimmed to the fields that matter.
        let b: Vec<Balance> = serde_json::from_str(
            r#"[{"accountAlias":"SgsR","asset":"USDT","balance":"122607.35137903",
                "crossWalletBalance":"23.72469206","crossUnPnl":"0.84683141",
                "availableBalance":"23.72469206","maxWithdrawAmount":"23.72469206",
                "marginAvailable":true,"updateTime":1617939110373}]"#,
        )
        .unwrap();
        assert_eq!(
            (b[0].asset.as_str(), b[0].balance, b[0].available),
            ("USDT", 122607.35137903, 23.72469206)
        );
        let p: Vec<PositionRisk> = serde_json::from_str(
            r#"[{"entryPrice":"6563.66500","marginType":"isolated","positionAmt":"20.000",
                 "symbol":"BTCUSDT","unRealizedProfit":"2316.83423560","positionSide":"LONG"},
                {"entryPrice":"0.00000","positionAmt":"-10.000","symbol":"BTCUSDT",
                 "unRealizedProfit":"-1156.46711780","positionSide":"SHORT"}]"#,
        )
        .unwrap();
        assert_eq!((p[0].amount, p[0].entry_price), (20.0, 6563.665));
        assert_eq!((p[1].amount, p[1].unrealized), (-10.0, -1156.4671178));
    }

    #[test]
    fn leverage_is_read_leniently_from_the_position_rows_and_the_brackets() {
        let rows: Vec<PositionRisk> = serde_json::from_str(
            r#"[{"symbol":"A","positionAmt":"1","entryPrice":"1","unRealizedProfit":"0","leverage":"21"},
                {"symbol":"B","positionAmt":"1","entryPrice":"1","unRealizedProfit":"0","leverage":7},
                {"symbol":"C","positionAmt":"1","entryPrice":"1","unRealizedProfit":"0"},
                {"symbol":"D","positionAmt":"1","entryPrice":"1","unRealizedProfit":"0","leverage":"x"},
                {"symbol":"E","positionAmt":"1","entryPrice":"1","unRealizedProfit":"0","leverage":"0"}]"#,
        )
        .unwrap();
        let got: Vec<Option<i32>> = rows.iter().map(|r| r.leverage).collect();
        assert_eq!(got, [Some(21), Some(7), None, None, None]);

        let set: LeverageSet = serde_json::from_str(
            r#"{"leverage":21,"maxNotionalValue":"1000000","symbol":"BTCUSDT"}"#,
        )
        .unwrap();
        assert_eq!((set.leverage, set.symbol.as_str()), (21, "BTCUSDT"));

        let b: Vec<SymbolBrackets> = serde_json::from_str(
            r#"[{"symbol":"BTCUSDT","brackets":[
                 {"bracket":2,"initialLeverage":50,"notionalCap":50000,"notionalFloor":10000,"maintMarginRatio":0.01,"cum":0.0},
                 {"bracket":1,"initialLeverage":125,"notionalCap":10000,"notionalFloor":0,"maintMarginRatio":0.004,"cum":0.0}]},
                {"symbol":"X","brackets":[]}]"#,
        )
        .unwrap();
        assert_eq!(
            b[0].max_leverage(),
            Some(125),
            "the highest, whatever the order"
        );
        assert_eq!(b[1].max_leverage(), None);
    }

    #[test]
    fn raw_trade_rows_read_as_prints_signed_by_the_aggressor() {
        let rows: Vec<RawTradeRow> = serde_json::from_str(
            r#"[{"id":1017104,"price":"0.2887000","qty":"346","quoteQty":"99.89","time":1790937178650,"isBuyerMaker":true},
                {"id":1017105,"price":"0.2888000","qty":"20","quoteQty":"5.77","time":1790937181000,"isBuyerMaker":false}]"#,
        )
        .unwrap();
        let t: Vec<AggTrade> = rows.into_iter().map(AggTrade::from).collect();
        assert_eq!(
            (t[0].id, t[0].price, t[0].time_ms, t[0].signed_qty()),
            (1017104, 0.2887, 1790937178650, -346.0),
            "the buyer the maker: a sell"
        );
        assert_eq!(t[1].signed_qty(), 20.0);
    }

    #[test]
    fn a_malformed_account_number_fails_the_answer_instead_of_reading_zero() {
        for bad in [
            r#"[{"symbol":"BTCUSDT","positionAmt":"abc","entryPrice":"1","unRealizedProfit":"0"}]"#,
            r#"[{"symbol":"BTCUSDT","positionAmt":"NaN","entryPrice":"1","unRealizedProfit":"0"}]"#,
            r#"[{"symbol":"BTCUSDT","positionAmt":null,"entryPrice":"1","unRealizedProfit":"0"}]"#,
            r#"[{"symbol":"BTCUSDT","entryPrice":"1","unRealizedProfit":"0"}]"#,
        ] {
            assert!(
                serde_json::from_str::<Vec<PositionRisk>>(bad).is_err(),
                "{bad} would show an open position as flat"
            );
        }
        let bare: Vec<PositionRisk> = serde_json::from_str(
            r#"[{"symbol":"X","positionAmt":-2,"entryPrice":1.5,"unRealizedProfit":0}]"#,
        )
        .unwrap();
        assert_eq!((bare[0].amount, bare[0].entry_price), (-2.0, 1.5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An integer where 0 means «absent» that arrives as `null`, a string, a float, a bool,
    /// `[]` or `{}` costs that field, not the document: one odd row of a 767-row frame must not
    /// blank the funding of the rest.
    #[test]
    fn odd_integers_cost_the_field_not_the_document() {
        let rows: Vec<PremiumIndex> = serde_json::from_str(
            r#"[{"symbol":"A","lastFundingRate":"0.0001","nextFundingTime":null},
                {"symbol":"B","lastFundingRate":"0.0002","nextFundingTime":"1790000000000"},
                {"symbol":"C","lastFundingRate":"0.0003","nextFundingTime":1.79e12},
                {"symbol":"D","lastFundingRate":"0.0004","nextFundingTime":true},
                {"symbol":"E","lastFundingRate":"0.0005","nextFundingTime":1790000000000},
                {"symbol":"F","lastFundingRate":"0.0006","nextFundingTime":[1,2]},
                {"symbol":"G","lastFundingRate":"0.0007","nextFundingTime":{"x":1}},
                {"symbol":"H","lastFundingRate":"0.0008","nextFundingTime":"abc"}]"#,
        )
        .expect("the document decodes");
        let times: Vec<i64> = rows.iter().map(|r| r.next_funding_time_ms).collect();
        assert_eq!(
            times,
            [
                0,
                1_790_000_000_000,
                1_790_000_000_000,
                0,
                1_790_000_000_000,
                0,
                0,
                0
            ]
        );
        let marks: Vec<MarkPriceUpdate> = serde_json::from_str(
            r#"[{"s":"A","p":"1","r":"0","T":"abc"},{"s":"B","p":"2","r":"0","T":-5}]"#,
        )
        .expect("the frame decodes");
        assert_eq!(
            marks
                .iter()
                .map(|m| m.next_funding_time_ms)
                .collect::<Vec<_>>(),
            [0, -5]
        );
    }

    /// An identifier, a time that orders data, a precision stay strict: a garbled one fails the
    /// frame instead of being read as trade 0, a bar of 1970 or a price with no decimals.
    #[test]
    fn identifiers_and_times_stay_strict() {
        let trade = |a: &str, t: &str| {
            serde_json::from_str::<AggTrade>(&format!(
                r#"{{"s":"X","a":{a},"p":"1","q":"1","T":{t},"m":false}}"#
            ))
        };
        assert!(trade("7", "1790000000000").is_ok());
        assert!(trade(r#""oops""#, "1790000000000").is_err());
        assert!(trade("7", "null").is_err());
    }
}
