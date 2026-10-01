//! Engine API: `TEngineRequest` parser and `TEngineResponse` builder plus the
//! method-specific payload writers the Init spine needs.
//!
//! Mirrors `commands::engine_request` (client builder), `engine_api::response`
//! (client parser), `engine_api::base_check`/`auth_check` and `market::*`.

use std::io::Write;

use flate2::write::DeflateEncoder;
use flate2::Compression;

pub use crate::commands::engine_api::EngineMethod;
pub use crate::commands::market::BaseCurrency;

use super::market_data::delphi_days;
use super::{read_str, write_str, BaseHeader, BASE_HEADER_SIZE};

const REQUEST_CMD_ID: u8 = 2;
const RESPONSE_CMD_ID: u8 = 1;
/// Deflate response bodies from this size on; smaller ones ride the transport SynLZ.
const COMPRESS_FROM: usize = 1024;

/// Parsed `TEngineRequest` (client -> server, `Command::API`).
#[derive(Debug, Clone)]
pub struct EngineRequest {
    pub uid: u64,
    pub method: EngineMethod,
    pub market_name: String,
    pub market_names: Vec<String>,
    pub params: Vec<u8>,
}

impl EngineRequest {
    pub fn parse(payload: &[u8]) -> Option<Self> {
        let hdr = BaseHeader::parse(payload)?;
        if hdr.cmd_id != REQUEST_CMD_ID {
            return None;
        }
        let mut pos = BASE_HEADER_SIZE;
        let method = EngineMethod::from_byte(*payload.get(pos)?);
        pos += 1;
        let market_name = read_str(payload, &mut pos)?;
        let count = read_i32(payload, &mut pos)?;
        if count < 0 || count as usize > payload.len() {
            return None;
        }
        let mut market_names = Vec::with_capacity(count as usize);
        for _ in 0..count {
            market_names.push(read_str(payload, &mut pos)?);
        }
        let params_size = read_i32(payload, &mut pos)?;
        if params_size < 0 || pos + params_size as usize > payload.len() {
            return None;
        }
        let params = payload[pos..pos + params_size as usize].to_vec();
        Some(Self {
            uid: hdr.uid,
            method,
            market_name,
            market_names,
            params,
        })
    }
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    let v = i32::from_le_bytes(data.get(*pos..*pos + 4)?.try_into().unwrap());
    *pos += 4;
    Some(v)
}

/// `TEngineResponse` payload for `Command::API`.
pub fn response_ok(request_uid: u64, method: EngineMethod, data: &[u8]) -> Vec<u8> {
    response(request_uid, method, true, 0, "", data)
}

pub fn response_err(request_uid: u64, method: EngineMethod, code: i32, msg: &str) -> Vec<u8> {
    response(request_uid, method, false, code, msg, &[])
}

fn response(
    request_uid: u64,
    method: EngineMethod,
    success: bool,
    error_code: i32,
    error_msg: &str,
    data: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 24 + error_msg.len() + data.len());
    BaseHeader::write(&mut out, RESPONSE_CMD_ID, rand::random());
    out.extend_from_slice(&request_uid.to_le_bytes());
    out.push(method.to_byte());
    out.push(u8::from(success));
    out.extend_from_slice(&error_code.to_le_bytes());
    write_str(&mut out, error_msg);
    let compressed = (data.len() >= COMPRESS_FROM).then(|| deflate(data));
    let body = compressed.as_deref().unwrap_or(data);
    out.push(u8::from(compressed.is_some()));
    out.extend_from_slice(&(body.len() as i32).to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn deflate(data: &[u8]) -> Vec<u8> {
    let mut enc = DeflateEncoder::new(Vec::with_capacity(data.len() / 2), Compression::default());
    enc.write_all(data).expect("Vec write");
    enc.finish().expect("Vec finish")
}

/// `emk_BaseCheck` body: server identity (`MoonProtoEngineServer.pas:244-273`).
pub struct ServerInfo<'a> {
    pub bot_id: i64,
    pub server_name: &'a str,
    /// MoonBot `ExchangeCode` ordinal; values outside the known range read as unknown.
    pub exchange_code: u8,
    pub exchange_name: &'a str,
    /// `ExchangeTypeMask` bits: 0x01 spot, 0x02 futures.
    pub exchange_type_mask: u8,
    pub base_currency_name: &'a str,
    pub base_currency_code: BaseCurrency,
    pub server_version: i32,
    pub moonproto_version: i32,
}

pub fn write_server_info(info: &ServerInfo<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&info.bot_id.to_le_bytes());
    write_str(&mut out, info.server_name);
    out.push(info.exchange_code);
    write_str(&mut out, info.exchange_name);
    out.push(info.exchange_type_mask);
    write_str(&mut out, ""); // dex_name
    write_str(&mut out, info.base_currency_name);
    out.push(info.base_currency_code.to_byte());
    out.extend_from_slice(&info.server_version.to_le_bytes());
    out.extend_from_slice(&info.moonproto_version.to_le_bytes());
    out
}

/// `emk_AuthCheck` body (`MoonProtoEngine.pas:605-639`), mandatory prefix plus
/// `recvd_max_payload` and an empty DEX table.
pub fn write_auth_check(account_id: &str, recvd_max_payload: i32) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + account_id.len());
    out.extend_from_slice(&0i64.to_le_bytes()); // binance_account_id
    write_str(&mut out, ""); // btc_address
    out.extend_from_slice(&0i32.to_le_bytes()); // spot_ref
    out.push(0); // is_sub_account
    write_str(&mut out, account_id);
    out.extend_from_slice(&recvd_max_payload.to_le_bytes());
    out.push(0); // known_dexes count
    out.push(0); // hl_dex_market
    out.push(0); // hl_spot_market
    out
}

/// Funding on one market: the rate and when it is next charged.
///
/// One type rather than two fields because the terminal's absence test is the
/// TIME, not the rate — a rate of exactly zero is a real answer between charges
/// (measured 01.10: 97 of 595 Aster symbols sit at zero right now), while a
/// missing time means "this venue charges no funding"
/// (`moon-core/src/market/source/mod.rs`, `funding_from_wire`). Carrying them
/// apart invites a row with a rate and no time, which reads on the terminal as
/// no funding at all while the number is right there.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Funding {
    /// Rate in PERCENT, which is the unit the wire carries.
    ///
    /// Aster's `lastFundingRate` is a FRACTION (`0.00008979` on BTCUSDT, 01.10)
    /// and must be multiplied by 100 before it gets here. The terminal reads
    /// this field straight into its funding column and says so twice, both
    /// times after being burnt: *"the value arrives in percent"*
    /// (`market/screener.rs`) and *"The protocol's own doc comment claims
    /// `0.0001 = 0.01%`; the two live screens say otherwise, and the screens
    /// win"* (`market/source/mod.rs`). A fraction written here shows BTCUSDT's
    /// 0.009 % as 0.00009 % — a funding of effectively nothing on every market.
    pub rate_pct: f64,
    /// Unix milliseconds of the next charge, **UTC**.
    ///
    /// [`MarketSpec::write`] converts it to the Delphi days the wire wants;
    /// moonproto's reader then adds the client's own zone offset, so UTC in is
    /// what makes the terminal's countdown right (`apply_delphi_local_funding_shift`).
    pub time_ms: i64,
}

/// One market row of `emk_GetMarketsList` (`MoonProtoSerialization.pas:42-98`).
///
/// `m_index` is the row position in the list.
///
/// This struct arrived from TInvestCore with its whole Binance-shaped half
/// written as zero/empty, because MOEX has no funding, no price band, no
/// settlement currency and no 1000-aliases. **Aster has all of them**, so on
/// this exchange those zeros were not placeholders but statements — and
/// statements no type change could flag, which is why `PLAN.md` tracks them as
/// M0 work rather than cleanup ("Находка M0").
///
/// The zeros that REMAIN are the ones Aster genuinely does not publish:
/// `bn_max_notional` (the exchange has no such filter), `bn_max_value`,
/// `bn_iceberg`/`bn_iceberg_parts`, `bn_margin_table_id`, `bn_only_isolated`
/// (not expressed in `exchangeInfo`), and the per-side `bid_*`/`ask_*`
/// multipliers. The last ones are a deliberate refusal: Aster's `PERCENT_PRICE`
/// does carry an extra `ltMultiplierUp`/`Down` pair, but what it bounds is
/// undocumented and measured 01.10 it is not a per-side band — it equals the
/// plain pair on 504 of 596 symbols, diverges on 68, and arrives as `0` on 24
/// including BTCUSDT. Writing it into a per-side slot would put a fabricated
/// bound on the money path.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketSpec {
    /// Server-side symbol (`bn_market_name`), key for indexed streams and subscriptions.
    pub symbol: String,
    /// Displayed instrument (`market_currency`), the base asset: `BTC` of
    /// `BTCUSDT`, `1000SHIB` of `1000SHIBUSDT`.
    ///
    /// Spelled exactly as the exchange does, because this is the token the
    /// terminal MATCHES BY TEXT against a strategy's coin list and stores in
    /// its reports (`moon-core/src/market/source/mod.rs`, `MarketLabel::coin`).
    pub currency: String,
    /// Contract-free identity of the same coin (`market_currency_canonic`):
    /// `BTC`, and `SHIB` for `1000SHIB`.
    ///
    /// THE answer to "is this the same coin on another exchange", and the
    /// terminal takes it whole rather than deriving it: no rule over a market
    /// name can fold `1000BONK`, `1kBONK` and `BONK` into one coin, so every
    /// core is expected to fold its own (measured there across 21 live cores —
    /// `market_currency` splits BONK into four groups, `canonic` into one).
    pub currency_canonic: String,
    /// Human name (`market_currency_long`), e.g. `Bitcoin`.
    pub currency_long: String,
    /// Quote currency (`base_currency`), e.g. `USDT`.
    ///
    /// Must be non-empty on a linear market: an empty quote is how the terminal
    /// recognises a COIN-margined one, and paired with a contract size of 1 it
    /// would send a dollar figure through as a coin quantity
    /// (`market/source/mod.rs`, `quote_is_absent`).
    pub base_currency: String,
    pub base_currency_code: BaseCurrency,
    /// Settlement currency (`futures_type`), `USDT` for a USDⓈ-M perpetual.
    ///
    /// `EMPTY` here is what the terminal reads as **spot**
    /// (`Market::listed_type`, `feed/assets.rs`) — which is what an Aster core
    /// said for as long as this field was inherited from MOEX.
    pub futures_type: BaseCurrency,
    /// Display name (`market_name`), the exchange symbol: `BTCUSDT`.
    pub market_name: String,
    /// The `1000`-multiplier alias this market is listed under, or empty.
    ///
    /// Measured 01.10: 12 of the 596 USDT symbols carry it (`1000SHIB`,
    /// `1000PEPE`, …), and the exchange puts the prefix in `baseAsset` itself.
    /// Nothing in the terminal or in moonproto READS `leading1000`/`k1000`
    /// today — moon-core's own `coin_naming` channel exists precisely because
    /// their meaning is undocumented — so they are filled to the one reading
    /// the names allow and the fold that IS consumed goes to
    /// [`Self::currency_canonic`].
    pub leading1000: String,
    /// Multiplier of that alias: 1000 on those 12 markets, **1** elsewhere.
    ///
    /// One, not zero: upstream's own fixtures carry `k1000: 1` for an ordinary
    /// market, and a factor of zero is the one value that cannot be a factor.
    ///
    /// It describes the LISTING, and no price or quantity in this row has been
    /// divided or multiplied by it: an exchange of this dialect publishes every
    /// filter in the alias unit already (measured 01.10, `1000SHIBUSDT` prices
    /// from 0.00016 with a quantity step of 1 — a lot of 1000 coins). A reader
    /// that rescales by this field would be off by a thousand, in money.
    pub k1000: i32,
    pub price_precision: i32,
    pub quantity_precision: i32,
    pub tick_size: f64,
    /// Quantity step (`bn_step_size`): Aster's `LOT_SIZE.stepSize`. Not a lot —
    /// a size on Aster is a decimal quantity.
    pub step_size: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    pub min_notional: f64,
    /// `PRICE_FILTER.minPrice`/`maxPrice` (BTCUSDT: 1 … 1 000 000). Zero means
    /// the filter did not arrive, never "no bound".
    pub min_price: f64,
    pub max_price: f64,
    /// `PERCENT_PRICE.multiplierUp`/`Down`: an order price must sit inside
    /// `[mark × down, mark × up]`.
    ///
    /// Per symbol, and not the one constant `PLAN.md` first recorded — measured
    /// 01.10 over all 596 USDT perpetuals of one such venue: 1.10/0.90 on 405,
    /// 1.05/0.95 on 151, 1.02/0.98 on 21 (BTCUSDT among them), and 19 more
    /// across three narrower pairs. Zero means the filter did not arrive.
    pub multiplier_up: f64,
    pub multiplier_down: f64,
    pub max_leverage: i32,
    /// 24-hour turnover in the quote currency, as the terminal's screener
    /// column reads it.
    pub volume: f64,
    /// Settlement instant for a dated contract, `None` for a vanilla
    /// perpetual.
    ///
    /// `Option` rather than Aster's year-2101 sentinel or a zero: this is what
    /// `PanicSellDelisted` is keyed on, and "settles in 75 years" and "does not
    /// settle" must not be the same value to the one caller who has to tell
    /// them apart. Measured 01.10: 19 of 596 carry a real date, and they are
    /// exactly the `SETTLING` ones.
    pub delivery_time_ms: Option<i64>,
    /// Funding, or `None` when the exchange published none for this symbol.
    ///
    /// Measured 01.10: `/fapi/v1/premiumIndex` answers for 595 of the 596 USDT
    /// perpetuals — `MBLUSDT` has no row at all — so the absent case is live,
    /// not theoretical.
    pub funding: Option<Funding>,
    /// Whether this is the exchange's reference BTC market, which MoonBot's
    /// `Delta_BTC_*` keys and the terminal's core-PnL counter read
    /// (`moon-core/src/feed/types.rs`).
    ///
    /// Derived per row from "base is BTC, quote is the core's quote currency",
    /// which on a venue of this dialect can hold for one symbol only — a
    /// perpetual is keyed by base+quote. It is not checked across the list,
    /// because a check would hide the anomaly rather than report it: a catalog
    /// with two such markets is a catalog to look at, not to quietly thin.
    pub is_btc_market: bool,
    pub status_trading: bool,
}

/// Next funding time as the wire wants it: Delphi days, UTC, zero for absent
/// ([`MarketSpec::funding_time_days`] says why the guard is the whole point).
/// Shared by the market row and the funded price row, which carry the same pair.
fn funding_time_days(funding: Option<Funding>) -> f64 {
    match funding {
        Some(f) if f.time_ms > 0 => delphi_days(f.time_ms),
        _ => 0.0,
    }
}

impl MarketSpec {
    /// Next funding time as the wire wants it: Delphi days, UTC, and **zero for
    /// absent**.
    ///
    /// The guard is the whole function. `delphi_days(0)` is not zero but
    /// 25569.0 — the Delphi spelling of 1970-01-01 — and the terminal's absence
    /// test is `time > 0`, so an unguarded conversion turns "this market has no
    /// funding" into "it was last charged 56 years ago" and the countdown shows
    /// a figure instead of nothing.
    fn funding_time_days(&self) -> f64 {
        funding_time_days(self.funding)
    }

    fn write(&self, out: &mut Vec<u8>) {
        write_str(out, &self.symbol);
        write_str(out, &self.currency);
        write_str(out, &self.currency); // bn_market_currency
        write_str(out, &self.base_currency);
        write_str(out, &self.currency_long);
        write_str(out, &self.currency_canonic);
        write_str(out, &self.market_name);
        write_str(out, &self.market_name); // market_name_mb_classic
        write_str(
            out,
            if self.status_trading {
                "TRADING"
            } else {
                "BREAK"
            },
        );
        write_str(out, &self.leading1000);

        for v in [
            self.price_precision,
            self.quantity_precision,
            self.max_leverage,
            self.k1000,
            0, // bn_iceberg_parts
            0, // bn_margin_table_id
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        // A vanilla perpetual writes 0, not Aster's year-2101 sentinel: the
        // sentinel is a date to every reader downstream, and the one reader
        // that matters acts on "is there a settlement".
        out.extend_from_slice(&self.delivery_time_ms.unwrap_or(0).to_le_bytes());

        for v in [
            self.tick_size,
            self.step_size,
            self.min_qty,
            self.max_qty,
            self.min_notional,
            0.0, // bn_max_notional: Aster publishes no MAX_NOTIONAL filter
            1.0, // bn_contract_size: linear market, quantity is coins
            self.min_price,
            self.max_price,
            0.0, // bn_max_value
            self.multiplier_up,
            self.multiplier_down,
            0.0,          // bid_multiplier_up
            0.0,          // bid_multiplier_down
            0.0,          // ask_multiplier_up
            0.0,          // ask_multiplier_down
            self.max_qty, // int_bn_max_qty
            self.funding.map_or(0.0, |f| f.rate_pct),
            self.funding_time_days(),
            self.volume,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }

        out.push(u8::from(self.is_btc_market));
        out.push(u8::from(self.status_trading));
        // `has_1000_prefix_alias` is derived, not carried: two fields saying the
        // same thing is a row that can claim an alias and name none.
        out.push(u8::from(!self.leading1000.is_empty()));
        out.push(0); // bn_iceberg
        out.push(0); // bn_only_isolated: not expressed in `exchangeInfo`
        out.push(self.futures_type.to_byte());
    }
}

/// `emk_GetMarketsList` body: `count + markets + corr_count(0)`.
pub fn write_markets_list(markets: &[MarketSpec]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + markets.len() * 320);
    out.extend_from_slice(&(markets.len() as i32).to_le_bytes());
    for m in markets {
        m.write(&mut out);
    }
    out.extend_from_slice(&0i32.to_le_bytes());
    out
}

/// `emk_GetMarketsIndexes` body: `count:i32 + bn_market_name[count]` in mIndex order.
pub fn write_markets_indexes(names: &[&str]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + names.len() * 12);
    out.extend_from_slice(&(names.len() as i32).to_le_bytes());
    for name in names {
        write_str(&mut out, name);
    }
    out
}

/// `emk_QueryHedgeMode`: one Delphi boolean.
pub fn write_hedge_mode(hedge: bool) -> Vec<u8> {
    vec![u8::from(hedge)]
}

/// `emk_CheckAPIExpirationTime` meaning "no expiration":
/// `server_local_time:f64=0, days_left:i32=0, remaining_days:f64=0`.
pub fn write_no_api_expiration() -> Vec<u8> {
    vec![0u8; 20]
}

/// `emk_UpdateTransferAssets` with no rows.
pub fn write_no_transfer_assets() -> Vec<u8> {
    0i32.to_le_bytes().to_vec()
}

/// One `emk_UpdateMarketsList` row (`WriteMarketPricesToStream`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceRow {
    pub m_index: u16,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
}

/// `emk_UpdateMarketsList` body without funding or corr markets.
pub fn write_markets_prices(rows: &[PriceRow]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + rows.len() * 27);
    out.push(0); // send_funding
    out.extend_from_slice(&(rows.len() as i32).to_le_bytes());
    for r in rows {
        out.extend_from_slice(&r.m_index.to_le_bytes());
        out.extend_from_slice(&r.bid.to_le_bytes());
        out.extend_from_slice(&r.ask.to_le_bytes());
        out.extend_from_slice(&r.last.to_le_bytes()); // mark_price
        out.push(u8::from(r.last > 0.0)); // mark_price_found
    }
    out.push(0); // send_corr_markets
    out
}

/// One price row plus the market's current funding, for
/// [`write_markets_prices_funded`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FundedPriceRow {
    pub row: PriceRow,
    /// `None` is sent as rate 0 and time 0, which the terminal reads as "no
    /// funding" — the right statement for a market the exchange stopped naming.
    pub funding: Option<Funding>,
}

/// `emk_UpdateMarketsList` body WITH funding (`send_funding = 1`).
///
/// The catalog carries funding only once per session, and a funding charge
/// moves the next-charge time every few hours: without this the terminal's
/// countdown runs to a moment already past. With the flag set, every row
/// carries the pair and the client overwrites both on every row
/// (`state/markets/prices.rs`), so a market whose funding went away is sent as
/// zeros rather than left out — the same reason [`write_markets_prices`] sends
/// a zero bid instead of omitting the row.
pub fn write_markets_prices_funded(rows: &[FundedPriceRow]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + rows.len() * 43);
    out.push(1); // send_funding
    out.extend_from_slice(&(rows.len() as i32).to_le_bytes());
    for FundedPriceRow { row: r, funding } in rows {
        out.extend_from_slice(&r.m_index.to_le_bytes());
        out.extend_from_slice(&r.bid.to_le_bytes());
        out.extend_from_slice(&r.ask.to_le_bytes());
        out.extend_from_slice(&funding.map_or(0.0, |f| f.rate_pct).to_le_bytes());
        out.extend_from_slice(&funding_time_days(*funding).to_le_bytes());
        out.extend_from_slice(&r.last.to_le_bytes()); // mark_price
        out.push(u8::from(r.last > 0.0)); // mark_price_found
    }
    out.push(0); // send_corr_markets
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::engine_api::{
        parse_auth_check_response, parse_base_check_response, parse_engine_response,
    };
    use crate::commands::engine_request;
    use crate::commands::market::parse_markets_list_response;
    use crate::state::MarketsState;

    /// A MOEX-shaped row, kept so this file stays diff-comparable with
    /// TInvestCore's copy: every field Aster answers and MOEX does not is the
    /// neutral value there, and that is the correct row for that exchange — not
    /// an unfilled one.
    fn spec(symbol: &str) -> MarketSpec {
        MarketSpec {
            symbol: symbol.into(),
            currency: symbol.into(),
            currency_canonic: symbol.into(),
            currency_long: format!("{symbol} long"),
            base_currency: "RUB".into(),
            base_currency_code: BaseCurrency::RUB,
            futures_type: BaseCurrency::EMPTY,
            market_name: format!("{symbol}RUB"),
            leading1000: String::new(),
            k1000: 1,
            price_precision: 2,
            quantity_precision: 0,
            tick_size: 0.01,
            step_size: 10.0,
            min_qty: 10.0,
            max_qty: 1e9,
            min_notional: 0.0,
            min_price: 0.0,
            max_price: 0.0,
            multiplier_up: 0.0,
            multiplier_down: 0.0,
            max_leverage: 1,
            volume: 123.0,
            delivery_time_ms: None,
            funding: None,
            is_btc_market: false,
            status_trading: true,
        }
    }

    #[test]
    fn parses_upstream_engine_request() {
        let raw = engine_request::set_leverage("SBER", 5);
        let req = EngineRequest::parse(&raw).expect("parse");
        assert_eq!(req.method, EngineMethod::SetLeverage);
        assert_eq!(req.market_name, "SBER");
        assert!(req.market_names.is_empty());
        assert_eq!(req.params, 5i32.to_le_bytes());

        let raw = engine_request::subscribe_order_book(&["SBER", "GAZP"]);
        let req = EngineRequest::parse(&raw).expect("parse");
        assert_eq!(req.market_names, vec!["SBER", "GAZP"]);
    }

    #[test]
    fn response_round_trips_through_upstream_parser_with_and_without_deflate() {
        let small = response_ok(7, EngineMethod::BaseCheck, b"abc");
        let parsed = parse_engine_response(&small).expect("parse");
        assert_eq!(parsed.request_uid, 7);
        assert_eq!(parsed.method, EngineMethod::BaseCheck);
        assert!(parsed.success);
        assert_eq!(parsed.data, b"abc");

        let big = vec![0x5Au8; 5000];
        let resp = response_ok(9, EngineMethod::GetMarketsList, &big);
        assert!(resp.len() < big.len() / 4);
        let parsed = parse_engine_response(&resp).expect("parse");
        assert_eq!(parsed.data, big);

        let err =
            parse_engine_response(&response_err(3, EngineMethod::AuthCheck, 401, "nope")).unwrap();
        assert!(!err.success);
        assert_eq!((err.error_code, err.error_msg.as_str()), (401, "nope"));
    }

    #[test]
    fn server_info_and_auth_check_parse_upstream() {
        let info = parse_base_check_response(&write_server_info(&ServerInfo {
            bot_id: 42,
            server_name: "Astercore",
            exchange_code: 221,
            exchange_name: "Aster",
            exchange_type_mask: 0x02,
            base_currency_name: "USDT",
            base_currency_code: BaseCurrency::USDT,
            server_version: 1,
            moonproto_version: 4,
        }));
        assert_eq!(info.bot_id, Some(42));
        assert_eq!(info.exchange_name.as_deref(), Some("Aster"));
        assert_eq!(info.base_currency_name.as_deref(), Some("USDT"));
        assert_eq!(info.base_currency_code, Some(BaseCurrency::USDT));
        assert_eq!(info.moonproto_version, Some(4));

        let auth = parse_auth_check_response(&write_auth_check("acc-1", 65000)).expect("auth");
        assert_eq!(auth.account_id, "acc-1");
        assert_eq!(auth.recvd_max_payload, Some(65000));
        assert!(auth.known_dexes.is_empty());
    }

    #[test]
    fn empty_post_init_bodies_parse_upstream() {
        use crate::commands::engine_api::{
            parse_api_expiration_time_response, parse_query_hedge_mode_response,
            parse_update_transfer_assets_response,
        };

        assert_eq!(
            parse_query_hedge_mode_response(&write_hedge_mode(false)),
            Some(false)
        );
        let exp = parse_api_expiration_time_response(&write_no_api_expiration()).expect("exp");
        assert!(exp.time().is_none());
        assert!(
            parse_update_transfer_assets_response(&write_no_transfer_assets())
                .expect("assets")
                .is_empty()
        );
    }

    /// A market row shaped like Aster's own BTCUSDT, as `exchangeInfo` and
    /// `premiumIndex` gave it on 01.10.
    ///
    /// The `spec()` fixture above is deliberately left in its inherited MOEX
    /// shape so that this file stays diff-comparable with TInvestCore's copy —
    /// that is how a codec fix crosses between the two cores. But the only
    /// round-trip test then pinned `RUB`, a currency this exchange cannot have,
    /// so an Aster row gets its own.
    fn aster_spec() -> MarketSpec {
        MarketSpec {
            symbol: "BTCUSDT".into(),
            currency: "BTC".into(),
            currency_canonic: "BTC".into(),
            currency_long: "Bitcoin".into(),
            base_currency: "USDT".into(),
            base_currency_code: BaseCurrency::USDT,
            futures_type: BaseCurrency::USDT,
            market_name: "BTCUSDT".into(),
            leading1000: String::new(),
            k1000: 1,
            price_precision: 1,
            quantity_precision: 3,
            tick_size: 0.1,
            step_size: 0.001,
            min_qty: 0.001,
            max_qty: 1000.0,
            min_notional: 5.0,
            min_price: 1.0,
            max_price: 1_000_000.0,
            multiplier_up: 1.02,
            multiplier_down: 0.98,
            max_leverage: 20,
            volume: 658_458_631.25,
            delivery_time_ms: None,
            // `lastFundingRate` 0.00008979 as a fraction, times 100.
            funding: Some(Funding {
                rate_pct: 0.008_979,
                time_ms: 1_790_870_400_000,
            }),
            is_btc_market: true,
            status_trading: true,
        }
    }

    #[test]
    fn an_aster_market_row_round_trips_through_the_upstream_parser() {
        let list = write_markets_list(&[aster_spec()]);
        let parsed = parse_markets_list_response(&list, super::super::PROTO_CMD_VER).expect("list");
        let m = &parsed.markets[0];
        assert_eq!(m.bn_market_name, "BTCUSDT");
        assert_eq!(m.base_currency, "USDT");
        assert_eq!(m.bn_tick_size, 0.1);
        assert_eq!(m.bn_step_size, 0.001);
        assert_eq!(m.bn_min_qty, 0.001);
        assert_eq!(m.bn_min_notional, 5.0);
        assert!(m.status_trading);

        // Every one of these was a zero inherited from a MOEX core, and every
        // one of them is a statement to the terminal (`PLAN.md`, "Находка M0").
        assert_eq!(m.futures_type, BaseCurrency::USDT); // not EMPTY => not spot
        assert_eq!(m.market_currency_canonic, "BTC");
        assert_eq!((m.bn_min_price, m.bn_max_price), (1.0, 1_000_000.0));
        assert_eq!((m.bn_multiplier_up, m.bn_multiplier_down), (1.02, 0.98));
        assert_eq!(m.funding_rate, 0.008_979);
        assert!(m.is_btc_market);
        assert_eq!(m.bn_delivery_time, 0);
        assert_eq!(m.k1000, 1);
        assert!(!m.has_1000_prefix_alias);

        // The funding instant is written in UTC and the upstream reader moves it
        // onto this machine's wall clock while parsing
        // (`apply_delphi_local_funding_shift`), which is exactly what the
        // terminal undoes before it counts down to the charge. So the
        // round-trip is checked against the shifted value, not the raw one.
        let shift = crate::commands::candles::current_local_time_shift_minutes();
        let expected = delphi_days(1_790_870_400_000) + shift.round() / 1440.0;
        assert!(
            (m.funding_time - expected).abs() < 1e-9,
            "{}",
            m.funding_time
        );
    }

    /// The two absences the exchange really produces, and the one conversion
    /// that must not happen on them.
    #[test]
    fn a_market_without_funding_sends_zero_rather_than_1970() {
        let mut spec = aster_spec();
        // `MBLUSDT`, measured 01.10: in `exchangeInfo`, absent from
        // `premiumIndex` — 595 of 596 symbols have a row and this one does not.
        spec.symbol = "MBLUSDT".into();
        spec.funding = None;
        let list = write_markets_list(&[spec]);
        let parsed = parse_markets_list_response(&list, super::super::PROTO_CMD_VER).expect("list");
        let m = &parsed.markets[0];
        // Zero, not `delphi_days(0)` = 25569.0: the terminal's absence test is
        // `funding_time > 0`, so the Delphi spelling of 1970 would read as a
        // real charge and show a countdown for a market that has none.
        assert_eq!(m.funding_time, 0.0);
        assert_eq!(m.funding_rate, 0.0);
    }

    /// A `1000`-multiplier market: the alias fields filled and the coin folded.
    #[test]
    fn a_1000_alias_market_names_its_multiplier_and_folds_its_coin() {
        let mut spec = aster_spec();
        spec.symbol = "1000SHIBUSDT".into();
        spec.market_name = "1000SHIBUSDT".into();
        spec.currency = "1000SHIB".into();
        spec.currency_canonic = "SHIB".into();
        spec.leading1000 = "1000SHIB".into();
        spec.k1000 = 1000;
        spec.is_btc_market = false;
        let list = write_markets_list(&[spec]);
        let parsed = parse_markets_list_response(&list, super::super::PROTO_CMD_VER).expect("list");
        let m = &parsed.markets[0];
        // The token a strategy's coin list is matched against stays as the
        // exchange spells it; the cross-exchange identity is the folded one.
        assert_eq!(m.market_currency, "1000SHIB");
        assert_eq!(m.market_currency_canonic, "SHIB");
        assert_eq!(m.leading1000, "1000SHIB");
        assert_eq!(m.k1000, 1000);
        assert!(m.has_1000_prefix_alias);
    }

    /// A dated contract: `SETTLING` with a real delivery date, which is what
    /// `PanicSellDelisted` acts on.
    #[test]
    fn a_settling_market_carries_its_delivery_instant() {
        let mut spec = aster_spec();
        spec.symbol = "TONUSDT".into();
        spec.market_name = "TONUSDT".into();
        spec.is_btc_market = false;
        spec.status_trading = false;
        spec.delivery_time_ms = Some(1_781_859_600_000);
        let list = write_markets_list(&[spec]);
        let parsed = parse_markets_list_response(&list, super::super::PROTO_CMD_VER).expect("list");
        let m = &parsed.markets[0];
        assert_eq!(m.bn_delivery_time, 1_781_859_600_000);
        assert!(!m.status_trading);
    }

    #[test]
    fn markets_list_and_prices_parse_upstream() {
        let list = write_markets_list(&[spec("SBER"), spec("GAZP")]);
        let parsed = parse_markets_list_response(&list, super::super::PROTO_CMD_VER).expect("list");
        assert_eq!(parsed.markets.len(), 2);
        assert!(parsed.corr_markets.is_empty());
        let m = &parsed.markets[1];
        assert_eq!(m.bn_market_name, "GAZP");
        assert_eq!(m.market_name, "GAZPRUB");
        assert_eq!(m.base_currency, "RUB");
        assert_eq!(m.bn_step_size, 10.0);
        // A MOEX core has no settlement currency to name, so `EMPTY` — read as
        // SPOT — is the right answer for this row. On Aster it was not: see
        // `an_aster_market_row_round_trips_through_the_upstream_parser`.
        assert_eq!(m.futures_type, BaseCurrency::EMPTY);
        assert!(m.status_trading);

        let mut st = MarketsState::new();
        st.apply_markets_list_payload(&list, super::super::PROTO_CMD_VER)
            .expect("state list");
        let prices = write_markets_prices(&[PriceRow {
            m_index: 1,
            bid: 100.0,
            ask: 100.5,
            last: 100.2,
        }]);
        st.apply_markets_prices_payload(&prices)
            .expect("state prices");
        let gazp = st.get("GAZP").expect("GAZP");
        assert_eq!(gazp.with(|m| (m.price.bid, m.price.ask)), (100.0, 100.5));

        // The funded form: the same row plus the funding pair, which the
        // client overwrites on every row it receives — so a market without
        // funding must arrive as zeros, and a funded one as percent and a
        // UTC instant the reader then shifts into its own zone.
        let funded = write_markets_prices_funded(&[
            FundedPriceRow {
                row: PriceRow {
                    m_index: 0,
                    bid: 10.0,
                    ask: 10.5,
                    last: 10.2,
                },
                funding: Some(Funding {
                    rate_pct: 0.008_979,
                    time_ms: 1_790_870_400_000,
                }),
            },
            FundedPriceRow {
                row: PriceRow {
                    m_index: 1,
                    bid: 101.0,
                    ask: 101.5,
                    last: 0.0,
                },
                funding: None,
            },
        ]);
        let ev = st
            .apply_markets_prices_payload(&funded)
            .expect("funded prices");
        assert!(matches!(
            ev,
            crate::state::MarketsEvent::PricesUpdated {
                count: 2,
                included_funding: true,
                ..
            }
        ));
        let sber = st.get("SBER").expect("SBER");
        let (rate, time, mark, bid) = sber.with(|m| {
            (
                m.funding_rate,
                m.funding_time,
                m.price.mark_price,
                m.price.bid,
            )
        });
        assert_eq!((rate, mark, bid), (0.008_979, 10.2, 10.0));
        assert!(
            time > delphi_days(1_790_870_400_000) - 1.0,
            "a 2026 instant, not 1970"
        );
        let gazp = st.get("GAZP").expect("GAZP");
        assert_eq!(
            gazp.with(|m| (m.funding_rate, m.funding_time, m.price.bid)),
            (0.0, 0.0, 101.0),
            "no funding is zeros, never the Delphi 1970"
        );

        let names =
            crate::commands::market::parse_markets_indexes_response(&write_markets_indexes(&[
                "SBER", "GAZP",
            ]))
            .expect("indexes");
        assert_eq!(names, ["SBER", "GAZP"]);
    }
}
