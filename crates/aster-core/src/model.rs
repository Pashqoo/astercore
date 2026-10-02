//! The core's market catalog: Aster's `exchangeInfo` turned into the rows the
//! terminal is sent in `GetMarketsList`.
//!
//! One decision shapes this file. The terminal computes `currency_usd_rate` by
//! short-circuiting on a USD stablecoin (`moon-core/src/symbol/mod.rs`:
//! `is_usd_stable` lists `USDT`) *before* it looks for any market, so an Aster
//! core needs **no synthetic rate market at all** — where TInvestCore had to
//! invent `RUBUSDT` or have every manual order rejected. Verified against the
//! terminal's source, not assumed.

use moonproto::server::codec::engine::{
    BaseCurrency, FundedPriceRow, Funding as WireFunding, MarketSpec, PriceRow,
};

use crate::aster::json::{BookTicker, ExchangeInfo, Filter, PremiumIndex, SymbolInfo, Ticker24h};

/// Quote currency this core trades.
///
/// Measured 01.10: of 613 symbols, 596 are quoted in `USDT`, 15 in `USD1` and
/// 2 in `U`. Only `USDT` is taken: a core reports one `base_currency` to the
/// terminal, and `moonproto`'s `BaseCurrency` enum has no member for the other
/// two, so they could not be spelled on the wire even if we wanted them
/// (`PLAN.md` §10.5).
pub const QUOTE: &str = "USDT";

/// The same quote currency as the wire's one-byte enum.
///
/// Beside [`QUOTE`] because the two must name the same currency: the terminal
/// reads the string for display and the ordinal for its money path, and a core
/// whose two disagree is a core whose orders are priced in one currency and
/// sized in another. Changing the quote means changing both, here, once.
pub const QUOTE_CODE: BaseCurrency = BaseCurrency::USDT;

/// A perpetual whose `deliveryDate` is this is vanilla — the value is a
/// sentinel for "never" (year 2101), not a date. 577 of 596 carry it.
const NO_DELIVERY_MS: i64 = 4_133_404_800_000;

/// The prefix Aster puts on a symbol whose quantity counts 1000 coins.
const ALIAS_1000: &str = "1000";

/// Highest leverage this core will ever claim for a market.
///
/// Only a sanity bound on arithmetic over an exchange-reported percent: the
/// real figure per market comes from `/fapi/v1/leverageBracket` in M2. Measured
/// 01.10 the catalog implies 2× to 20× and nothing near this.
const MAX_LEVERAGE: i32 = 125;

/// Funding on a market, in the units the EXCHANGE states them.
///
/// Deliberately not the wire's units: `rate` is a fraction here and percent
/// there, and the conversion happens once, in [`spec_of`]. Two types rather
/// than one shared struct is what keeps that conversion from being a `* 100.0`
/// somebody can drop — the compiler will not let a fraction reach the field
/// documented as percent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Funding {
    /// `lastFundingRate`, a fraction: `0.00008979` is 0.009 % per charge.
    pub rate: f64,
    /// `nextFundingTime`, unix milliseconds, UTC, and expected to be > 0.
    ///
    /// Not an invariant the type enforces — the struct is plain data — but one
    /// every write path in this module keeps: [`Catalog::apply_premium_index`] stores
    /// `None` for a row without a charge time rather than a rate beside a zero,
    /// which is the shape the terminal reads as no funding at all while the
    /// rate sits right there.
    pub next_ms: i64,
}

/// What kind of underlying a market has, for MoonBot's `MarketTags`.
///
/// Aster is not a pure crypto venue, which is the single most surprising fact
/// about its catalog: it lists stock, forex and commodity perpetuals beside
/// coins. TInvestCore's tags were `shares, futures, etf, bonds`; these are the
/// same idea over the taxonomy Aster actually publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tag {
    Crypto,
    Stock,
    Forex,
    Commodities,
    Etf,
    Meme,
    Ai,
    /// Aster's own "major coin" marker (`underlyingSubType: ["Top"]`).
    Top,
    /// Real-world asset, `USD1-RWA` / `AOS2`.
    Rwa,
    /// Trading has not opened yet (`pre-launch`, or `status` `PENDING_TRADING`).
    PreLaunch,
}

impl Tag {
    /// The spelling a strategy file uses. Lowercase, like TInvestCore's.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Crypto => "crypto",
            Self::Stock => "stock",
            Self::Forex => "forex",
            Self::Commodities => "commodities",
            Self::Etf => "etf",
            Self::Meme => "meme",
            Self::Ai => "ai",
            Self::Top => "top",
            Self::Rwa => "rwa",
            Self::PreLaunch => "prelaunch",
        }
    }
}

impl Tag {
    /// Every tag, in the order the journal and the editor list them.
    pub const ALL: [Tag; 10] = [
        Tag::Crypto,
        Tag::Stock,
        Tag::Forex,
        Tag::Commodities,
        Tag::Etf,
        Tag::Meme,
        Tag::Ai,
        Tag::Top,
        Tag::Rwa,
        Tag::PreLaunch,
    ];

    fn bit(self) -> u16 {
        1 << (self as u16)
    }
}

/// The classes a strategy's screener picks from (MoonBot's `MarketTags`),
/// ported from TInvestCore over Aster's own taxonomy ([`Tag`]). A market
/// carries several tags (`DOGEUSDT`: meme and crypto), and it is in the set
/// when one of its tags is and none of its tags is excluded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MarketTags {
    include: u16,
    exclude: u16,
}

impl MarketTags {
    /// Choices of the terminal's `MarketTags` combo: one class per item,
    /// spelled the way [`Self::parse`] expects. A combination or a `!tag`
    /// exclusion still parses when typed into a strategy file.
    pub const PICKLIST: &'static str =
        "crypto|stock|forex|commodities|etf|meme|ai|top|rwa|prelaunch";

    /// MoonBot tag-filter syntax, case-insensitive: `crypto`, `meme, ai` or
    /// `!stock` (only exclusions start from every class). Empty text is the
    /// empty set; an unknown tag is the error, so a typo never widens the set.
    pub fn parse(text: &str) -> Result<Self, String> {
        let (mut include, mut exclude) = (0u16, 0u16);
        for token in text.split([',', ' ', ';']).filter(|t| !t.is_empty()) {
            let (negated, name) = match token.strip_prefix('!') {
                Some(rest) => (true, rest),
                None => (false, token),
            };
            let Some(tag) = Tag::ALL
                .iter()
                .find(|t| t.name().eq_ignore_ascii_case(name))
            else {
                return Err(token.to_string());
            };
            if negated {
                exclude |= tag.bit();
            } else {
                include |= tag.bit();
            }
        }
        if include == 0 && exclude != 0 {
            include = Tag::ALL.iter().fold(0, |acc, t| acc | t.bit());
        }
        Ok(Self { include, exclude })
    }

    pub fn is_empty(self) -> bool {
        self.include & !self.exclude == 0
    }

    /// A market with these tags is in the set.
    pub fn matches(self, tags: &[Tag]) -> bool {
        let bits = tags.iter().fold(0, |acc, t| acc | t.bit());
        bits & self.include != 0 && bits & self.exclude == 0
    }

    /// The tag names the parser knows, for log lines.
    pub fn known() -> String {
        Tag::ALL.map(Tag::name).join(", ")
    }
}

/// One market of the catalog.
#[derive(Debug, Clone)]
pub struct Market {
    /// Exchange symbol and subscription key: `BTCUSDT`.
    pub symbol: String,
    /// Base asset: `BTC`.
    pub base: String,
    pub quote: String,
    /// Human name; Aster fills it for stock perps and leaves it empty for coins.
    pub long_name: String,

    pub price_precision: i32,
    pub quantity_precision: i32,
    pub tick_size: f64,
    /// `LOT_SIZE.stepSize` — the order-size step. Not a lot: on Aster a size is
    /// a decimal quantity, and TInvestCore's lot arithmetic has no counterpart.
    pub step_size: f64,
    pub min_qty: f64,
    pub max_qty: f64,
    /// `MARKET_LOT_SIZE.maxQty`, which is lower than `max_qty` (BTCUSDT: 120
    /// against 1000). A MARKET order sized past it is refused.
    pub market_max_qty: f64,
    /// `MIN_NOTIONAL.notional` — the floor in USDT (5 on every symbol measured).
    pub min_notional: f64,
    pub min_price: f64,
    pub max_price: f64,
    /// `PERCENT_PRICE`, per symbol: an order price must sit within
    /// `[mark × down, mark × up]`.
    ///
    /// Measured 01.10 over all 596 USDT perpetuals, and NOT the "±2 %" the plan
    /// first recorded from BTCUSDT alone: 1.10/0.90 on 405 symbols, 1.05/0.95
    /// on 151, 1.02/0.98 on 21, 1.15/0.85 on 8, 1.03/0.97 on 7, 1.04/0.96 on 4
    /// — 596 in all. BTCUSDT is one of the 21, which is how the tightest band
    /// in the catalog came to be taken for the common one. The spread matters
    /// for how a position can be closed (`PLAN.md`, open decision 2): ±10 %
    /// still admits a marketable limit through the book, ±2 % does not.
    pub multiplier_up: f64,
    pub multiplier_down: f64,
    pub max_num_orders: i64,
    /// Conditional orders allowed on the exchange for this symbol; 10.
    pub max_num_algo_orders: i64,
    /// Minimum distance a stop trigger must keep, as a fraction.
    pub trigger_protect: f64,
    pub market_take_bound: f64,
    pub maint_margin_percent: f64,
    /// Initial margin, percent. The ceiling it implies — `100 /
    /// required_margin_percent`, so 5.0 means 20× — is only an upper bound, not
    /// this account's leverage: the real figure comes from
    /// `/fapi/v1/leverageBracket` and `ACCOUNT_CONFIG_UPDATE`, which is why this
    /// carries the percent rather than a `leverage` field that would read as
    /// authoritative.
    pub required_margin_percent: f64,
    pub liquidation_fee: f64,

    /// `TRADING` right now. The whole of TInvestCore's trading-schedule gate
    /// reduces to this plus [`Market::has_sessions`].
    pub trading: bool,
    /// The underlying trades in sessions rather than round the clock, taken
    /// from the venue its `channel` names.
    ///
    /// **Not `tradingMode`, which is not a static property at all.** Two live
    /// snapshots of `exchangeInfo` three hours apart on 01.10: at 10:29 UTC
    /// `tradingMode == 1` on 121 of the 596 USDT symbols, at 13:30 UTC on 20 —
    /// all 101 `nasdaq` symbols had flipped to 0, which is the moment the US
    /// market opens (09:30 ET). So the flag follows the clock, and in a
    /// direction nothing here has established; reading it as "has sessions"
    /// made this catalog's own journal line swing from 121 to 20 between two
    /// runs of the same build.
    ///
    /// `channel` is the fact that holds still: measured 01.10, 124 of the 596
    /// carry an exchange venue — `nasdaq` 101, forex 9, `hkstock` 7, `krstock`
    /// 5, `astock` 2 — and every one of those underlyings has sessions by its
    /// nature. Note that **forex never carried `tradingMode == 1` in either
    /// snapshot** although forex plainly has sessions, which is the second
    /// reason the flag cannot be the signal. What `tradingMode` actually means
    /// is an open question for the entry gate (`PLAN.md`), to be measured
    /// across a day rather than guessed from a name.
    pub has_sessions: bool,
    /// Real settlement date, or `None` for a vanilla perpetual.
    pub delivery_ms: Option<i64>,
    pub tags: Vec<Tag>,

    /// 24-hour turnover in USDT, once `ticker/24hr` has been read.
    ///
    /// `Option`, not a zero: "not measured yet" and "traded nothing" are
    /// different facts, and ranking a class on an unfilled window is the
    /// mistake TInvestCore names in its own pool rules. A convention that a
    /// zero means unknown is a convention every future caller can forget; a
    /// `None` is one the compiler makes them handle.
    pub quote_volume_24h: Option<f64>,
    /// Last traded price, from the same call and `None` for the same reason.
    pub last_price: Option<f64>,
    /// Top of book, from `ticker/bookTicker` and `None` until it is read.
    ///
    /// Both sides or neither: a book row carries the two together, and half a
    /// top of book is a quote nobody can act on. Measured 01.10 the answer
    /// covers the `TRADING` symbols and no others, so a `SETTLING` or
    /// `PENDING_TRADING` market keeps `None` for as long as it is listed, and
    /// [`Catalog::prices`] sends it to the terminal as a zero — which is how
    /// both sides of that wire spell "no quote".
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    /// Mark price from `premiumIndex` at startup and `!markPrice@arr` every
    /// 3 s after; `None` until read, and cleared while the mark stream is dead
    /// (`engine::judge_streams`).
    ///
    /// The exchange's own reference price: `PERCENT_PRICE`, the margin and the
    /// liquidation price are all computed against it, so it is what the
    /// terminal's band and risk columns mean. Not the last trade, and not
    /// derived from the book — the two differ on a thin market, which is
    /// exactly where the difference matters.
    pub mark_price: Option<f64>,
    /// Funding, from the same two sources as the mark price, and `None` when the exchange
    /// published no row for this symbol — measured 01.10, `MBLUSDT` is such a
    /// market. Not a zero: a rate of exactly zero is a real answer between
    /// charges on 97 of the 595 rows.
    pub funding: Option<Funding>,
    /// The market's trade stream is alive: the session that carries its
    /// `aggTrade` answers (`stream_health`). True from the start, as a stream
    /// still opening counts as alive; the engine keeps it current. Orders read
    /// it (`fresh`), as TInvestCore's did.
    pub feed_fresh: bool,
}

impl Market {
    /// Whether this is the market MoonBot's `Delta_BTC_*` keys refer to.
    ///
    /// On MOEX there was no BTC at all, so TInvestCore re-read those keys as
    /// `Delta_MOEX_*` against `IMOEXF`. Here the original meaning is available:
    /// measured 01.10, exactly one USDT market has `BTC` as its base.
    pub fn is_btc_reference(&self) -> bool {
        self.base == "BTC"
    }

    /// The coin behind a `1000`-multiplier listing: `Some("SHIB")` for
    /// `1000SHIB`, `None` for an ordinary market.
    ///
    /// Measured 01.10 this matches exactly 12 of the 596 USDT symbols —
    /// `1000SHIB`, `1000PEPE`, `1000FLOKI`, `1000BONK`, `1000CHEEMS`,
    /// `1000LUNC`, `1000SATS`, `1000WOJAK`, `1000NEX`, `1000XEC`, `1000RATS`,
    /// `1000CAT` — and Aster puts the prefix in `baseAsset` itself, not only in
    /// the symbol.
    ///
    /// A rule over the name, which is exactly what `moon-core/coin_naming.rs`
    /// says no rule can settle in general: `1000SATS` may be a real ticker
    /// rather than a multiplier. It is folded anyway, deliberately, because the
    /// terminal measured what cores DO across 21 live venues and built its
    /// cross-exchange identity on it — "the core already folds `1000BONK`,
    /// `1kBONK` and `BONK` to one `BONK`, `1000SATS` to `SATS`"
    /// (`market/source/mod.rs`). Folding differently would put this core alone
    /// in its own identity group. The next letter must be a letter, so a coin
    /// genuinely named `1000` or `10001` is not folded.
    ///
    /// The alias is a NAME, never a scale factor applied here: Aster already
    /// publishes every filter in the alias unit. Measured 01.10,
    /// `1000SHIBUSDT` carries `PRICE_FILTER` 0.00016 … 2000 and `LOT_SIZE`
    /// step 1 — prices and sizes for a lot of 1000 SHIB, not for one. Rescaling
    /// anything by [`ALIAS_1000`] would be the thousandfold error, not the fix
    /// for one.
    pub fn alias_1000(&self) -> Option<&str> {
        let coin = self.base.strip_prefix(ALIAS_1000)?;
        coin.chars()
            .next()
            .filter(char::is_ascii_alphabetic)
            .map(|_| coin)
    }

    /// Highest leverage the INSTRUMENT allows, from its initial-margin percent.
    ///
    /// `requiredMarginPercent` 5.0 means 5 % of the notional must be posted, so
    /// 20×. Measured 01.10 over the 596 USDT perpetuals: 33.33 % on 358 (3×),
    /// 5 % on 123 (20×), 50 % on 74 (2×), 20 % on 32, 25 % on 5, 10 % on 4.
    ///
    /// This is the instrument's ceiling, **not** this account's leverage, which
    /// needs the signed `/fapi/v1/leverageBracket` call M2 brings. It goes on
    /// the wire because the field there is a maximum (`max_leverage`, which the
    /// terminal shows as the market's cap) and because the alternative is a 0
    /// or a 1 — a statement that this venue has no leverage at all. A missing
    /// or unreadable percent gives 1 for the same reason the sizing rules
    /// refuse rather than guess: 1× cannot over-promise.
    pub fn max_leverage(&self) -> i32 {
        let p = self.required_margin_percent;
        if !p.is_finite() || p <= 0.0 {
            return 1;
        }
        // The epsilon keeps an exact ratio exact: 100 / 33.33 is 3.0003 and
        // floors to 3, but 100 / 20.0 is 4.999999999999999 on some inputs and
        // would floor to 4 without it. Same shape as TInvestCore's
        // `max_leverage`, which learnt it on 0.1428 -> 7.
        ((100.0 / p + 1e-9).floor() as i32).clamp(1, MAX_LEVERAGE)
    }

    /// Whether this market can be sized at all.
    ///
    /// A symbol whose `LOT_SIZE` filter did not arrive has `step_size == 0`, and
    /// with it `min_qty`, `max_qty` and `min_notional` are zero too — so every
    /// check below would pass vacuously and the core would send an order no
    /// filter had bounded. Measured 01.10 this does not happen (0 of 596 USDT
    /// symbols lack the filter), but the degenerate path must refuse rather
    /// than wave an unbounded order through.
    pub fn sizable(&self) -> bool {
        // All three, not just the step. Checking the step alone left a market
        // whose `maxQty` came through as 0 reading as sizable, and the ceiling
        // test below is written `ceiling > 0.0 &&` — so that market shipped an
        // unbounded size, which is the exact vacuous pass this guard exists to
        // stop. `tick_size` is here too because an order needs a price on the
        // grid as much as a size on it. Measured 01.10: all 596 USDT symbols
        // carry a complete `LOT_SIZE`, so this refuses nothing that is real.
        self.step_size > 0.0
            && self.step_size.is_finite()
            && self.max_qty > 0.0
            && self.max_qty.is_finite()
            && self.tick_size > 0.0
            && self.tick_size.is_finite()
    }

    /// Round a quantity down to the exchange's step.
    ///
    /// Down, never nearest: rounding up can cross `maxQty`, a balance, or a
    /// position's remaining size, and the exchange refuses the whole order
    /// rather than clamping it.
    ///
    /// Returns `None` when the market has no usable step — see [`Market::sizable`].
    pub fn floor_qty(&self, qty: f64) -> Option<f64> {
        if !self.sizable() || !qty.is_finite() || qty < 0.0 {
            return None;
        }
        let steps = qty / self.step_size;
        // MEASURED, not defensive: over this catalog's nine real step sizes,
        // 714 of ~18000 exact multiples come out of this division just under
        // their integer — `0.29 / 0.01` is 28.999999999999996 — so a plain
        // `.floor()` drops a whole step. On `0.29` that is a 3.4% short order,
        // and no later rounding recovers it because `28 * 0.01` is already
        // 0.28. So a quotient within a relative epsilon of an integer is
        // snapped to it BEFORE flooring. The tolerance is relative, so it
        // holds at 0.001 and at 100000 alike.
        let near = steps.round();
        // The tolerance is relative so it holds at every magnitude, but CAPPED:
        // `1e-9 * steps` passes 0.5 once `steps` exceeds 5e8, and from there
        // every quotient would snap to the NEAREST integer — returning more
        // than was asked for and breaking the floor contract above. That is
        // reachable on live data, not theoretical: 1000SATSUSDT has
        // `maxQty 60000000000` with `stepSize 1`, which is 6e10 steps, 120×
        // past the threshold. A thousandth of a step is far more than binary
        // division error ever needs and far less than a step.
        let tol = (1e-9 * near.abs().max(1.0)).min(1e-3);
        let steps = if (steps - near).abs() <= tol {
            near
        } else {
            steps.floor()
        };
        if steps < 0.0 {
            return None;
        }
        // The step count is now exact, so the product carries only the
        // multiplication's own error. It is removed by snapping to the step's
        // own decimal count — derived from the step rather than taken from the
        // separate `quantityPrecision` field, which an absent value would
        // silently make 0 and turn every fractional size into nothing.
        Some(round_to(
            steps * self.step_size,
            decimals_of(self.step_size),
        ))
    }

    /// Size a notional (USDT) into a quantity this exchange will accept, or say
    /// why it cannot be done.
    ///
    /// This is where TInvestCore's `floor(size / (price × lot))` lands, minus
    /// the lot: the checks that follow are the exchange's own filters, and
    /// skipping any of them means the refusal arrives from Aster instead, one
    /// round trip later and counted against the order budget.
    ///
    /// `kind` is required rather than defaulted because `MARKET_LOT_SIZE` caps
    /// a market order lower than `LOT_SIZE` caps a limit one (BTCUSDT: 120
    /// against 1000). A single ceiling would be wrong for one of the two, and a
    /// parameter is the only shape a caller cannot forget to consider.
    pub fn size_from_notional(
        &self,
        notional: f64,
        price: f64,
        kind: OrderKind,
    ) -> Result<f64, SizeError> {
        if !price.is_finite() || price <= 0.0 {
            return Err(SizeError::NoPrice);
        }
        if !notional.is_finite() || notional <= 0.0 {
            return Err(SizeError::NoNotional);
        }
        let Some(qty) = self.floor_qty(notional / price) else {
            return Err(SizeError::NotSizable);
        };
        if qty <= 0.0 || qty < self.min_qty {
            return Err(SizeError::BelowMinQty {
                qty,
                min_qty: self.min_qty,
            });
        }
        let ceiling = self.max_qty_for(kind);
        if ceiling > 0.0 && qty > ceiling {
            return Err(SizeError::AboveMaxQty {
                qty,
                max_qty: ceiling,
                kind,
            });
        }
        // Checked on the ROUNDED quantity, not on the notional asked for:
        // rounding down is what can drop a request that was above the floor
        // below it, and that is precisely the case the exchange refuses.
        //
        // The comparison carries a relative tolerance for the same reason the
        // step count does: `qty * price` can land a hair under a figure that is
        // mathematically the limit, and refusing an order that is exactly large
        // enough is a refusal the exchange would not have made.
        let got = qty * price;
        if self.min_notional > 0.0 && got < self.min_notional * (1.0 - 1e-9) {
            return Err(SizeError::BelowMinNotional {
                notional: got,
                min_notional: self.min_notional,
            });
        }
        Ok(qty)
    }
}

/// Which ceiling applies to a size, because Aster caps the two differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    /// Bounded by `LOT_SIZE.maxQty` (BTCUSDT: 1000).
    Limit,
    /// Bounded by `MARKET_LOT_SIZE.maxQty` (BTCUSDT: 120).
    Market,
}

impl OrderKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Limit => "LOT_SIZE",
            Self::Market => "MARKET_LOT_SIZE",
        }
    }
}

impl Market {
    /// The quantity ceiling that applies to `kind`.
    ///
    /// Falls back to the limit ceiling when `MARKET_LOT_SIZE` did not arrive:
    /// the tighter of the two is the safe side, and a zero here would mean "no
    /// ceiling at all".
    pub fn max_qty_for(&self, kind: OrderKind) -> f64 {
        match kind {
            OrderKind::Limit => self.max_qty,
            OrderKind::Market if self.market_max_qty > 0.0 => self.market_max_qty,
            OrderKind::Market => self.max_qty,
        }
    }
}

/// What the order model (`orders.rs`, ported from TInvestCore) reads of a
/// market. Its "lot" is one `stepSize` of quantity: whole lots are then whole
/// steps, which is exactly the grid `LOT_SIZE` admits, and the model's integer
/// arithmetic carries over without a rounding of its own.
impl Market {
    /// `price` at the nearest tick (MoonBot rounds every order price this way).
    pub fn nearest(&self, price: f64) -> f64 {
        round_tick(price, self.tick_size)
    }

    /// `price` on the tick grid, rounded up or down.
    pub fn snap(&self, price: f64, up: bool) -> f64 {
        let tick = self.tick_size;
        if tick <= 0.0 {
            return price;
        }
        // The tolerance scales with the step count — a fixed 1e-9 sinks below
        // a float's own error once price / tick nears 1e8 — and is capped well
        // under half a step, so it never turns a ceil or floor into a round.
        let x = price / tick;
        let eps = (1e-9 * x.abs().max(1.0)).min(1e-3);
        let steps = if up {
            (x - eps).ceil()
        } else {
            (x + eps).floor()
        };
        quantize(steps, tick)
    }

    /// The quantity of one lot: one step.
    pub fn lot(&self) -> f64 {
        self.step_size
    }

    /// USDT notional of one lot at `price`. Linear USDT contracts: a unit of
    /// quantity is worth its price, with no multiplier between them.
    pub fn lot_value(&self, price: f64) -> f64 {
        price * self.step_size
    }

    /// The last trade price, 0 while none is known.
    pub fn last(&self) -> f64 {
        self.last_price.unwrap_or(0.0)
    }

    /// The market's prices are current: its trade stream is alive.
    pub fn fresh(&self) -> bool {
        self.feed_fresh
    }

    /// A limit `spread` through the book: below the bid for a sale, above the
    /// ask for a purchase, the last price standing in for a missing side.
    /// `None` without any price.
    pub fn marketable(&self, sell: bool, spread: f64) -> Option<f64> {
        let side = if sell { self.bid } else { self.ask };
        let base = side.filter(|p| *p > 0.0).unwrap_or(self.last());
        if base <= 0.0 {
            return None;
        }
        let raw = base * if sell { 1.0 - spread } else { 1.0 + spread };
        Some(self.snap(raw, !sell))
    }

    /// The band `PERCENT_PRICE` allows a limit in: the mark price times
    /// `multiplierDown` / `multiplierUp`. `None` while the mark or the
    /// multipliers are unknown.
    pub fn band(&self) -> Option<(f64, f64)> {
        let mark = self.mark_price.filter(|p| *p > 0.0)?;
        (self.multiplier_down > 0.0 && self.multiplier_up > 0.0).then(|| {
            (
                self.snap(mark * self.multiplier_down, true),
                self.snap(mark * self.multiplier_up, false),
            )
        })
    }

    /// `price` pinned inside the band, unchanged while the band is unknown.
    pub fn within_limits(&self, price: f64) -> f64 {
        match self.band() {
            Some((down, up)) => price.clamp(down, up),
            None => price,
        }
    }

    /// `price` when it lies inside the band (or the band is unknown), 0 past
    /// it: a strategy entry beyond the band is not placed — pinned to the edge
    /// it would sit nearer the market than the strategy asked (TInvestCore,
    /// by request, 21.09). Exits and manual orders are pinned instead.
    pub fn inside_limits(&self, price: f64) -> f64 {
        match self.band() {
            Some((down, up)) => {
                let eps = self.tick_size.max(0.0) * 1e-6;
                if price < down - eps || price > up + eps {
                    0.0
                } else {
                    price
                }
            }
            None => price,
        }
    }

    /// Short entries may go out: a perpetual shorts as it longs.
    pub fn shortable(&self, _now: i64) -> bool {
        true
    }

    /// The `PERCENT_PRICE` band set to exactly `down..up`, for tests: a mark
    /// of 1 with the bounds as multipliers.
    #[cfg(test)]
    pub(crate) fn set_limits(&mut self, down: f64, up: f64, _now: i64) {
        self.mark_price = Some(1.0);
        self.multiplier_down = down;
        self.multiplier_up = up;
    }

    /// The best bid, 0 while unknown (the strategies' convention).
    pub fn bid_px(&self) -> f64 {
        self.bid.unwrap_or(0.0)
    }

    /// The best ask, 0 while unknown.
    pub fn ask_px(&self) -> f64 {
        self.ask.unwrap_or(0.0)
    }

    /// Strategies may act on this market: it is `TRADING` and its prices are
    /// current. Otherwise entries come off and exits hold.
    pub fn live(&self) -> bool {
        self.trading && self.fresh()
    }
}

/// `price` at the nearest multiple of `tick` (unchanged for a zero tick).
pub fn round_tick(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    quantize((price / tick).round(), tick)
}

/// `value` as the exchange must read it on a grid of `step`: rounded to the
/// grid and printed at the step's decimals (`0.001` → `"0.016"`), never in
/// an exponent and never with a float's tail.
pub fn on_grid(value: f64, step: f64) -> String {
    let d = decimals_of(step).max(0) as usize;
    format!("{:.*}", d, round_tick(value, step))
}

/// `steps × tick` printed clean at the tick's decimals.
fn quantize(steps: f64, tick: f64) -> f64 {
    let scale = 10f64.powi(decimals_of(tick));
    (steps * tick * scale).round() / scale
}

/// Why a notional could not become an order size. Every arm carries the numbers
/// the journal line needs, so the refusal can be read without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SizeError {
    NoPrice,
    NoNotional,
    /// The market arrived without a usable `LOT_SIZE` step, so no filter bounds
    /// it and no size may be derived. See [`Market::sizable`].
    NotSizable,
    BelowMinQty {
        qty: f64,
        min_qty: f64,
    },
    AboveMaxQty {
        qty: f64,
        max_qty: f64,
        kind: OrderKind,
    },
    BelowMinNotional {
        notional: f64,
        min_notional: f64,
    },
}

impl std::fmt::Display for SizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPrice => write!(f, "no price"),
            Self::NoNotional => write!(f, "no notional"),
            Self::NotSizable => write!(f, "no LOT_SIZE step for this market"),
            Self::BelowMinQty { qty, min_qty } => {
                write!(f, "qty {qty} below minQty {min_qty}")
            }
            Self::AboveMaxQty { qty, max_qty, kind } => {
                write!(f, "qty {qty} above {} maxQty {max_qty}", kind.name())
            }
            Self::BelowMinNotional {
                notional,
                min_notional,
            } => write!(f, "notional {notional} below MIN_NOTIONAL {min_notional}"),
        }
    }
}

/// Round to `digits` decimal places, half away from zero.
fn round_to(v: f64, digits: i32) -> f64 {
    if !(0..=12).contains(&digits) {
        return v;
    }
    let f = 10f64.powi(digits);
    (v * f).round() / f
}

/// How many decimal places a step size carries: `0.001` -> 3, `1` -> 0.
///
/// Derived from the step itself rather than read from `quantityPrecision`,
/// because that is a separate field and an absent one deserializes to 0 — which
/// would quietly turn every fractional size into nothing. The step is the thing
/// the grid is actually made of, so it cannot disagree with itself.
fn decimals_of(step: f64) -> i32 {
    if !step.is_finite() || step <= 0.0 {
        return 0;
    }
    // Walk the grid up rather than formatting and counting characters: a step
    // arrives as a float here, and `0.0010` and `0.001` are the same number.
    let mut d = 0;
    let mut scaled = step;
    // The threshold is absolute and far below any real step. Scaled by the
    // value, `1e-9` made the test `1e-9 > 1e-9` for a step of 1e-9 itself,
    // which exited at once and rounded every size to a whole unit.
    while d < 12 && (scaled - scaled.round()).abs() > 1e-12 {
        scaled *= 10.0;
        d += 1;
    }
    d
}

/// The whole catalog, plus what the log line says about it.
///
/// `markets` is private because [`Catalog::get`] binary-searches it: a public
/// field would let any caller append out of order and turn a lookup into a
/// silent `None`. [`Catalog::build`] is the only way in, and it sorts.
#[derive(Debug, Default)]
pub struct Catalog {
    markets: Vec<Market>,
    /// Symbols `exchangeInfo` listed that this core does not carry, by reason.
    pub skipped_quote: usize,
    pub skipped_not_a_perpetual: usize,
    /// Markets whose `LOT_SIZE` filter did not arrive, so they cannot be sized
    /// ([`Market::sizable`]). Measured 01.10 this is 0; it is counted rather
    /// than asserted because a filter the catalog shipped without is a fact the
    /// journal should state, not a panic.
    pub unsizable: usize,
    /// `channel` values this build does not know, with how many carried each.
    /// A new venue appearing on Aster would otherwise fall silently into the
    /// crypto fallback.
    pub unknown_channels: Vec<(String, usize)>,
}

impl Catalog {
    /// Build the catalog from one `exchangeInfo` answer.
    ///
    /// Non-`TRADING` symbols are **kept**, not dropped: `SETTLING` is where a
    /// position still needs an exit, and a market missing from the catalog is a
    /// market the terminal cannot show or close. Trading is gated by
    /// [`Market::trading`] instead, which is the same separation TInvestCore
    /// drew between its pool and its entry gates.
    ///
    /// `contractType` is accepted both as `PERPETUAL` and as **empty**. Measured
    /// 01.10: the only 5 symbols with an empty one are exactly the 5
    /// `PENDING_TRADING` ones, and the catalog contains no dated contract type
    /// at all — so rejecting the empty value excluded nothing but pre-launch
    /// perpetuals, which is the opposite of what the check was for. Anything
    /// else named is rejected, so a real dated future would still be kept out.
    pub fn build(info: &ExchangeInfo) -> Self {
        let mut out = Self::default();
        let mut unknown: Vec<(String, usize)> = Vec::new();
        for s in &info.symbols {
            if s.quote_asset != QUOTE {
                out.skipped_quote += 1;
                continue;
            }
            if !matches!(s.contract_type.as_str(), "PERPETUAL" | "") {
                out.skipped_not_a_perpetual += 1;
                continue;
            }
            if !is_known_channel(&s.channel) {
                match unknown.iter_mut().find(|(c, _)| *c == s.channel) {
                    Some((_, n)) => *n += 1,
                    None => unknown.push((s.channel.clone(), 1)),
                }
            }
            let m = market_of(s);
            if !m.sizable() {
                out.unsizable += 1;
            }
            out.markets.push(m);
        }
        out.markets.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        unknown.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out.unknown_channels = unknown;
        out
    }

    /// Every market, in symbol order.
    pub fn markets(&self) -> &[Market] {
        &self.markets
    }

    /// Markets open for trading right now.
    pub fn trading(&self) -> impl Iterator<Item = &Market> {
        self.markets.iter().filter(|m| m.trading)
    }

    /// Whether the trade stream of `symbol` is alive (`stream_health`).
    pub fn set_feed_fresh(&mut self, symbol: &str, fresh: bool) {
        if let Ok(i) = self
            .markets
            .binary_search_by(|m| m.symbol.as_str().cmp(symbol))
        {
            self.markets[i].feed_fresh = fresh;
        }
    }

    /// The last trade of `symbol`, from the tape: what orders read as the
    /// market's price (`Market::last`). The startup value is the 24-hour
    /// ticker's; from the first trade on it is live.
    pub fn set_last(&mut self, symbol: &str, price: f64) {
        if !(price.is_finite() && price > 0.0) {
            return;
        }
        if let Ok(i) = self
            .markets
            .binary_search_by(|m| m.symbol.as_str().cmp(symbol))
        {
            self.markets[i].last_price = Some(price);
        }
    }

    /// A catalog of these markets, for tests that need no `exchangeInfo`.
    #[cfg(test)]
    pub(crate) fn of(mut markets: Vec<Market>) -> Self {
        markets.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        Self {
            markets,
            ..Self::default()
        }
    }

    /// A market to change in place, for tests: the symbol must stay as it is,
    /// or [`Catalog::get`] loses it.
    #[cfg(test)]
    pub(crate) fn get_mut(&mut self, symbol: &str) -> Option<&mut Market> {
        let i = self
            .markets
            .binary_search_by(|m| m.symbol.as_str().cmp(symbol))
            .ok()?;
        Some(&mut self.markets[i])
    }

    pub fn get(&self, symbol: &str) -> Option<&Market> {
        self.markets
            .binary_search_by(|m| m.symbol.as_str().cmp(symbol))
            .ok()
            .map(|i| &self.markets[i])
    }

    /// Merge one `ticker/24hr` answer: turnover and last price per market.
    ///
    /// Matched by symbol, never zipped: the answer measured 608 rows against
    /// this catalog's 596, because it covers quotes this core does not carry.
    /// Returns how many markets were filled, so the journal can say it instead
    /// of the core silently ranking a pool on unfilled windows.
    pub fn apply_tickers(&mut self, rows: &[Ticker24h]) -> usize {
        let mut filled = 0;
        for r in rows {
            if let Ok(i) = self
                .markets
                .binary_search_by(|m| m.symbol.as_str().cmp(&r.symbol))
            {
                self.markets[i].quote_volume_24h = Some(r.quote_volume);
                self.markets[i].last_price = Some(r.last_price);
                filled += 1;
            }
        }
        filled
    }

    /// Merge one `premiumIndex` answer: the funding pair and the mark price per
    /// market. Called with the startup REST answer, with every `!markPrice@arr`
    /// frame after it (the same complete set, measured), and with an empty
    /// slice when that stream dies — which clears both, on purpose.
    ///
    /// Same matching rule and same reason, with the asymmetry the other way
    /// round as well: measured 01.10 the answer has 766 rows for 596 markets
    /// AND still leaves `MBLUSDT` without one. The markets it does not name
    /// keep `funding: None` and `mark_price: None`.
    ///
    /// Named after the answer rather than after one of its two fields: both the
    /// funding pair and the mark price come from this one call, and a caller
    /// that read the name as "funding only" would go looking for a second
    /// request that does not exist.
    ///
    /// Returns how many markets ended up WITH funding — the figure the
    /// `catalog:` line reports — not how many rows matched. The mark-price
    /// coverage is counted separately by [`Catalog::summary`], because the two
    /// differ: a row with a mark price and no charge time fills one and not the
    /// other.
    pub fn apply_premium_index(&mut self, rows: &[PremiumIndex]) -> usize {
        // Cleared first for the same reason as the book, and it is the same
        // kind of answer: the exchange's current word on every market it
        // publishes these numbers for. A market that drops out of it has no
        // mark price and no funding, rather than yesterday's.
        for m in &mut self.markets {
            m.mark_price = None;
            m.funding = None;
        }
        let mut filled = 0;
        for r in rows {
            let Ok(i) = self
                .markets
                .binary_search_by(|m| m.symbol.as_str().cmp(&r.symbol))
            else {
                continue;
            };
            // Zero is not a mark price: it is what an absent or unreadable
            // field decodes to (`json::str_f64`), and a zero reference price
            // would read on the terminal as a market priced at nothing.
            self.markets[i].mark_price = (r.mark_price > 0.0).then_some(r.mark_price);
            // A row without a charge time is not funding, whatever rate it
            // carries: the terminal's own absence test is the time, so keeping
            // it would show nothing on screen while counting as funding in the
            // journal — wrong in both directions at once. It CLEARS what was
            // there rather than being skipped, because this answer is the
            // exchange's current word on the market: a later merge that no
            // longer names a charge time means there is none, not that the last
            // one still stands. Measured 01.10 all 595 rows carry a time; the
            // branch is for the row that does not.
            self.markets[i].funding = (r.next_funding_time_ms > 0).then_some(Funding {
                rate: r.last_funding_rate,
                next_ms: r.next_funding_time_ms,
            });
            filled += usize::from(self.markets[i].funding.is_some());
        }
        filled
    }

    /// Merge one `ticker/bookTicker` answer: the top of book per market.
    ///
    /// The answer is the exchange's COMPLETE word on what is quoted, not a
    /// patch: measured 01.10 it carries a row for every one of the 589
    /// `TRADING` symbols and for nothing else. So every quote is dropped first
    /// and only this answer's rows are stored — a market that falls out of a
    /// later answer (it stops trading, it is delisted) loses its quote instead
    /// of keeping the last one it ever had. Merging in place would leave that
    /// market quoted at a price nobody is offering, for as long as the process
    /// runs.
    ///
    /// A row is a quote only if both sides are positive and the bid is not
    /// above the ask. Half a top of book is not one, and a crossed pair cannot
    /// come from one book — both sides of a row are read off the same one — so
    /// it is garbage from a partial or reordered answer rather than a market to
    /// act on.
    ///
    /// Returns how many markets ended up quoted, which the `catalog:` line and
    /// the `prices:` line report: the difference from the catalog size is the
    /// markets that are not `TRADING`, and a sudden drop there is the exchange
    /// thinning out.
    ///
    /// An EMPTY answer therefore clears every quote. That is the same statement
    /// as any other answer — nothing is quoted — and it is also how the
    /// refresher reports an outage it has given up on (`prices.rs`), which is
    /// the one way a core that cannot reach the exchange can stop presenting
    /// old prices as current.
    pub fn apply_book(&mut self, rows: &[BookTicker]) -> usize {
        for m in &mut self.markets {
            m.bid = None;
            m.ask = None;
        }
        let mut filled = 0;
        for r in rows {
            let Ok(i) = self
                .markets
                .binary_search_by(|m| m.symbol.as_str().cmp(&r.symbol))
            else {
                continue;
            };
            if r.bid_price > 0.0 && r.ask_price > 0.0 && r.bid_price <= r.ask_price {
                let m = &mut self.markets[i];
                m.bid = Some(r.bid_price);
                m.ask = Some(r.ask_price);
                filled += 1;
            }
        }
        filled
    }

    /// Market symbols in `m_index` order — the body of `GetMarketsIndexes`.
    ///
    /// The same order as [`Catalog::specs`] by construction (both walk the one
    /// sorted vector), which is the whole contract of that message: the
    /// terminal maps these names onto the indexes every later indexed packet
    /// uses.
    pub fn symbols(&self) -> Vec<&str> {
        self.markets.iter().map(|m| m.symbol.as_str()).collect()
    }

    /// Position of a symbol in that order, or `None` for a market this core
    /// does not carry.
    pub fn index_of_symbol(&self, symbol: &str) -> Option<u16> {
        self.markets
            .binary_search_by(|m| m.symbol.as_str().cmp(symbol))
            .ok()
            .map(|i| i as u16)
    }

    /// [`Catalog::index_of_symbol`] for text an operator typed — a strategy's
    /// `CoinsWhiteList`/`CoinsBlackList`, the terminal's global black list —
    /// where the case is nobody's contract. Aster spells every symbol in
    /// upper case, so the upper-cased spelling is the other candidate. MoonBot
    /// lists name coins, not symbols (`btc, eth`, `1000pepe` in its own
    /// strategy files), so a word that is no symbol is tried as a coin: the
    /// one market whose base asset it is, or whose folded `1000` coin
    /// ([`Market::alias_1000`], what the terminal is told as the canonic
    /// currency: `pepe` for `1000PEPEUSDT`).
    pub fn index_of_symbol_ci(&self, symbol: &str) -> Option<u16> {
        let up = symbol.to_ascii_uppercase();
        self.index_of_symbol(symbol)
            .or_else(|| self.index_of_symbol(&up))
            .or_else(|| {
                let mut by_base = self.bases(&up);
                let first = by_base.next()?;
                by_base.next().is_none().then_some(first)
            })
    }

    /// Whether an inexact spelling is refused because two markets answer to
    /// it: two markets on one coin. Not seen on Aster: the catalog holds only
    /// `USDT`-quoted markets, and their bases are unique (checked 02.10
    /// against `exchangeInfo`). Kept for the screener's log line, which tells
    /// «ambiguous» from «unknown».
    pub fn symbol_is_ambiguous(&self, symbol: &str) -> bool {
        self.bases(&symbol.to_ascii_uppercase()).nth(1).is_some()
    }

    /// Markets whose base asset or folded `1000` coin is `up`.
    fn bases<'a>(&'a self, up: &'a str) -> impl Iterator<Item = u16> + 'a {
        self.iter()
            .filter(move |(_, m)| {
                m.base.eq_ignore_ascii_case(up)
                    || m.alias_1000().is_some_and(|c| c.eq_ignore_ascii_case(up))
            })
            .map(|(i, _)| i)
    }

    /// The market at `m_index`.
    pub fn at(&self, idx: u16) -> Option<&Market> {
        self.markets.get(usize::from(idx))
    }

    /// Markets with their `m_index`.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &Market)> {
        self.markets.iter().enumerate().map(|(i, m)| (i as u16, m))
    }

    pub fn len(&self) -> usize {
        self.markets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    /// The market at `m_index`, for tests that set prices on it.
    #[cfg(test)]
    pub(crate) fn at_mut(&mut self, idx: u16) -> Option<&mut Market> {
        self.markets.get_mut(usize::from(idx))
    }

    /// The price rows of `UpdateMarketsList`, in `m_index` order: one row per
    /// market, every time.
    ///
    /// A market with no quote is sent as **zeros**, not left out. Leaving it
    /// out does not clear anything on the other side — the terminal's client
    /// overwrites only the rows it receives (`state/markets/prices.rs`), so an
    /// omitted market keeps whatever it was last told, which is precisely the
    /// stale quote this core must not leave standing. A zero, by contrast, is
    /// read as absent on both sides of that boundary: the terminal passes the
    /// bid through its own `positive()` filter (`moon-core/src/market/source/
    /// read.rs`) and the client's price-mean and chart-step arithmetic is
    /// guarded by an epsilon, so a zero row shows no price and poisons no
    /// delta window — verified in their source, not assumed.
    ///
    /// One reader there is NOT guarded, and it is harmless: the client
    /// recomputes `price.min_lot_size = max(step × mid, min_notional)` on every
    /// row, so a zero collapses it to the market's `MIN_NOTIONAL` — 5 USDT on
    /// this venue, which is the floor it would have anyway — and the next
    /// quoted row restores it. It is also what the contract test reads to tell
    /// "the row arrived as zeros" from "no row arrived at all", since the
    /// client's own default for an untouched market is a zero everywhere.
    ///
    /// Measured 01.10 this is 596 rows of which 589 carry a quote; the other
    /// seven are the `SETTLING` and `PENDING_TRADING` markets, which the
    /// exchange quotes no book for and the terminal must therefore show no
    /// price for.
    ///
    /// The mark price rides along when it is known; the price-row writer
    /// carries a `found` flag beside it, and a zero there says "no mark price"
    /// rather than "zero".
    pub fn prices(&self) -> Vec<PriceRow> {
        self.markets
            .iter()
            .enumerate()
            .map(|(i, m)| PriceRow {
                m_index: i as u16,
                bid: m.bid.unwrap_or(0.0),
                ask: m.ask.unwrap_or(0.0),
                last: m.mark_price.unwrap_or(0.0),
            })
            .collect()
    }

    /// [`Catalog::prices`] with each market's current funding beside its row —
    /// the body of `UpdateMarketsList` from M1 on.
    ///
    /// The catalog row carries funding once per session, and every charge
    /// moves the next-charge time: without these rows the terminal counts down
    /// to a moment already past. A market without funding goes out as zeros,
    /// which overwrite what the terminal held — the same reason a market
    /// without a quote is a zero row and not a missing one.
    pub fn funded_prices(&self) -> Vec<FundedPriceRow> {
        self.prices()
            .into_iter()
            .zip(&self.markets)
            .map(|(row, m)| FundedPriceRow {
                row,
                funding: wire_funding(m.funding),
            })
            .collect()
    }

    /// The catalog as `GetMarketsList` rows, in `m_index` order.
    ///
    /// The order is this list's order and every later indexed message
    /// (`GetMarketsIndexes`, price rows, subscriptions) refers to the same
    /// positions, which is why it is taken from the sorted `markets` once and
    /// never rebuilt per call.
    pub fn specs(&self) -> Vec<MarketSpec> {
        self.markets.iter().map(spec_of).collect()
    }

    /// The `catalog:` journal line — the observation that says M0 worked.
    pub fn summary(&self) -> String {
        let total = self.markets.len();
        let trading = self.markets.iter().filter(|m| m.trading).count();
        let sessions = self.markets.iter().filter(|m| m.has_sessions).count();
        let delivering = self
            .markets
            .iter()
            .filter(|m| m.delivery_ms.is_some())
            .count();
        let mut tags: Vec<_> = [
            Tag::Crypto,
            Tag::Stock,
            Tag::Forex,
            Tag::Commodities,
            Tag::Etf,
            Tag::Meme,
            Tag::Ai,
            Tag::Top,
            Tag::Rwa,
            Tag::PreLaunch,
        ]
        .into_iter()
        .map(|t| {
            let n = self.markets.iter().filter(|m| m.tags.contains(&t)).count();
            (t, n)
        })
        .filter(|(_, n)| *n > 0)
        .collect();
        tags.sort_by_key(|a| std::cmp::Reverse(a.1));
        let tags: Vec<String> = tags
            .iter()
            .map(|(t, n)| format!("{} {n}", t.name()))
            .collect();
        // Coverage of the two merged answers, counted rather than assumed: a
        // market without turnover cannot be ranked in a pool and a market
        // without funding shows none in the terminal, and both are per-symbol
        // facts (measured 01.10: `premiumIndex` answers for 595 of 596).
        let funded = self.markets.iter().filter(|m| m.funding.is_some()).count();
        let turnover = self
            .markets
            .iter()
            .filter(|m| m.quote_volume_24h.is_some())
            .count();
        let aliases = self
            .markets
            .iter()
            .filter(|m| m.alias_1000().is_some())
            .count();
        // The two the price rows of `UpdateMarketsList` are built from. Counted
        // here for the same reason as funding and turnover: a market with no
        // top of book goes into those rows as a zero and the terminal shows no
        // price for it — a fact the journal must state rather than leave to be
        // noticed on screen.
        let quoted = self
            .markets
            .iter()
            .filter(|m| m.bid.is_some() && m.ask.is_some())
            .count();
        let marked = self
            .markets
            .iter()
            .filter(|m| m.mark_price.is_some())
            .count();
        let mut line = format!(
            "catalog: {total} {QUOTE} perpetuals, {trading} trading, \
             {sessions} with sessions, {delivering} with a delivery date, \
             {aliases} 1000-aliases; funding {funded}/{total}, \
             turnover {turnover}/{total}, quoted {quoted}/{total}, \
             marks {marked}/{total}; \
             {}; skipped {} on quote, {} not a perpetual",
            tags.join(", "),
            self.skipped_quote,
            self.skipped_not_a_perpetual
        );
        // Both of these are silent degradations unless the line names them: an
        // unsizable market refuses every order, and an unknown channel takes
        // the crypto fallback for a venue nobody has looked at.
        if self.unsizable > 0 {
            line.push_str(&format!("; {} UNSIZABLE (no LOT_SIZE)", self.unsizable));
        }
        if !self.unknown_channels.is_empty() {
            let chans: Vec<String> = self
                .unknown_channels
                .iter()
                .map(|(c, n)| format!("{c:?} {n}"))
                .collect();
            line.push_str(&format!("; unknown channels: {}", chans.join(", ")));
        }
        line
    }
}

/// One catalog market as the terminal's `GetMarketsList` row.
///
/// The one place the exchange's units become the wire's units, and this core
/// has exactly one such conversion: the funding fraction becomes a percent.
/// Nothing else is rescaled at all — a linear USDⓈ-M market's quantity is
/// coins, so `bn_contract_size` stays 1 and the terminal's own money path reads
/// it that way (`quote_is_absent` + `contract_size == 1` =>
/// `MarketQuantityUnit::Coins`).
///
/// Three fields are intentionally not what Aster hands over:
/// - `delivery_time_ms` drops the year-2101 sentinel for `None`;
/// - `volume` is the QUOTE turnover (USDT), which is what MoonBot's volume
///   filters and the terminal's screener column mean — the base turnover goes
///   only into deep-history chart rows (`PLAN.md`, "Объёмы");
/// - `max_leverage` is the instrument's ceiling derived from the margin
///   percent, until M2's signed brackets give the account's own figure.
fn spec_of(m: &Market) -> MarketSpec {
    let alias = m.alias_1000();
    MarketSpec {
        symbol: m.symbol.clone(),
        currency: m.base.clone(),
        // The folded coin for the 12 alias markets, the base asset otherwise.
        currency_canonic: alias.unwrap_or(&m.base).to_string(),
        currency_long: m.long_name.clone(),
        base_currency: m.quote.clone(),
        base_currency_code: QUOTE_CODE,
        // Settled in the quote currency — a USDⓈ-M perpetual. `EMPTY` here is
        // read as spot, and the catalog only ever holds `QUOTE` markets
        // (`Catalog::build` skips the rest), so the settlement currency is the
        // quote currency by construction rather than by coincidence.
        futures_type: QUOTE_CODE,
        market_name: m.symbol.clone(),
        leading1000: alias.map(|_| m.base.clone()).unwrap_or_default(),
        k1000: alias.map_or(1, |_| 1000),
        price_precision: m.price_precision,
        quantity_precision: m.quantity_precision,
        tick_size: m.tick_size,
        step_size: m.step_size,
        min_qty: m.min_qty,
        max_qty: m.max_qty,
        min_notional: m.min_notional,
        min_price: m.min_price,
        max_price: m.max_price,
        multiplier_up: m.multiplier_up,
        multiplier_down: m.multiplier_down,
        max_leverage: m.max_leverage(),
        // Zero where no ticker has been read yet: the wire has no "unknown"
        // for this field, and the terminal already reads 0 as "no figure".
        volume: m.quote_volume_24h.unwrap_or(0.0),
        delivery_time_ms: m.delivery_ms,
        funding: wire_funding(m.funding),
        is_btc_market: m.is_btc_reference(),
        status_trading: m.trading,
    }
}

/// The exchange's funding in the wire's units — the single conversion this
/// file exists to keep in one place, shared by the catalog row and the price
/// rows so the two can never disagree on the unit.
fn wire_funding(f: Option<Funding>) -> Option<WireFunding> {
    f.map(|f| WireFunding {
        // Fraction -> percent; see `WireFunding::rate_pct`.
        rate_pct: f.rate * 100.0,
        time_ms: f.next_ms,
    })
}

/// Whether `channel` is a value [`tags_of`] knows how to classify.
///
/// `{}` and the empty string are Aster's two spellings of "none" (measured:
/// 347 and 133 of 613), so they are known values, not unknown ones.
fn is_known_channel(channel: &str) -> bool {
    matches!(
        channel,
        "" | "{}" | "nasdaq" | "forex" | "hkstock" | "krstock" | "astock"
    )
}

/// Whether the underlying behind `channel` trades in sessions.
///
/// Every value except Aster's two spellings of "none" is an exchange or a
/// market that closes — so an UNKNOWN channel counts as session-bearing too.
/// That is the safe side of the one gate this feeds: treating a new venue's
/// stock as round-the-clock crypto would let an entry through while its market
/// is shut, while treating it as session-bearing only costs a gate that waits
/// for a schedule nobody has configured yet. `Catalog::build` names the unknown
/// channel in the `catalog:` line either way.
fn has_sessions(channel: &str) -> bool {
    !matches!(channel, "" | "{}")
}

fn market_of(s: &SymbolInfo) -> Market {
    let mut m = Market {
        symbol: s.symbol.clone(),
        base: s.base_asset.clone(),
        quote: s.quote_asset.clone(),
        long_name: s.name.clone(),
        price_precision: s.price_precision,
        quantity_precision: s.quantity_precision,
        tick_size: 0.0,
        step_size: 0.0,
        min_qty: 0.0,
        max_qty: 0.0,
        market_max_qty: 0.0,
        min_notional: 0.0,
        min_price: 0.0,
        max_price: 0.0,
        multiplier_up: 0.0,
        multiplier_down: 0.0,
        max_num_orders: 0,
        max_num_algo_orders: 0,
        trigger_protect: s.trigger_protect,
        market_take_bound: s.market_take_bound,
        maint_margin_percent: s.maint_margin_percent,
        required_margin_percent: s.required_margin_percent,
        liquidation_fee: s.liquidation_fee,
        trading: s.status == "TRADING",
        has_sessions: has_sessions(&s.channel),
        delivery_ms: (s.delivery_date_ms != NO_DELIVERY_MS && s.delivery_date_ms > 0)
            .then_some(s.delivery_date_ms),
        tags: tags_of(s),
        quote_volume_24h: None,
        last_price: None,
        bid: None,
        ask: None,
        mark_price: None,
        funding: None,
        feed_fresh: true,
    };
    for f in &s.filters {
        match f {
            Filter::Price {
                tick_size,
                min_price,
                max_price,
            } => {
                m.tick_size = *tick_size;
                m.min_price = *min_price;
                m.max_price = *max_price;
            }
            Filter::LotSize {
                step_size,
                min_qty,
                max_qty,
            } => {
                m.step_size = *step_size;
                m.min_qty = *min_qty;
                m.max_qty = *max_qty;
            }
            Filter::MarketLotSize { max_qty, .. } => m.market_max_qty = *max_qty,
            Filter::MinNotional { notional } => m.min_notional = *notional,
            Filter::PercentPrice {
                multiplier_up,
                multiplier_down,
            } => {
                m.multiplier_up = *multiplier_up;
                m.multiplier_down = *multiplier_down;
            }
            Filter::MaxNumOrders { limit } => m.max_num_orders = *limit,
            Filter::MaxNumAlgoOrders { limit } => m.max_num_algo_orders = *limit,
            Filter::Other => {}
        }
    }
    m
}

/// Derive MoonBot tags from Aster's `channel` and `underlyingSubType`.
///
/// `channel` is the venue the underlying really trades on and is the stronger
/// signal; `underlyingSubType` is Aster's own labelling and overlaps with it
/// (a Nasdaq name carries both `nasdaq` and `STOCK`). `Crypto` is the fallback
/// rather than a label Aster publishes — it is what is left when nothing else
/// matched, which is why it is decided last.
fn tags_of(s: &SymbolInfo) -> Vec<Tag> {
    /// Append unless already present: a Nasdaq name carries both `nasdaq` and
    /// `STOCK`, and the tag must come out once.
    ///
    /// A free function rather than a closure because the fallback below has to
    /// READ the set after it has been written to, which a closure holding the
    /// mutable borrow forbids.
    fn push(tags: &mut Vec<Tag>, t: Tag) {
        if !tags.contains(&t) {
            tags.push(t);
        }
    }
    let mut tags = Vec::new();

    match s.channel.as_str() {
        "nasdaq" | "hkstock" | "krstock" | "astock" => push(&mut tags, Tag::Stock),
        "forex" => push(&mut tags, Tag::Forex),
        _ => {}
    }
    for sub in &s.underlying_sub_type {
        // Case-insensitive: Aster mixes `STOCK` with `Meme` and `Top`.
        match sub.to_ascii_lowercase().as_str() {
            "stock" | "semiconductor" => push(&mut tags, Tag::Stock),
            "commodities" => push(&mut tags, Tag::Commodities),
            "etf" => push(&mut tags, Tag::Etf),
            "meme" => push(&mut tags, Tag::Meme),
            "ai" => push(&mut tags, Tag::Ai),
            "top" => push(&mut tags, Tag::Top),
            "usd1-rwa" | "aos2" => push(&mut tags, Tag::Rwa),
            "pre-launch" => push(&mut tags, Tag::PreLaunch),
            _ => {}
        }
    }
    if s.status == "PENDING_TRADING" {
        push(&mut tags, Tag::PreLaunch);
    }
    // `Etf` belongs in this list beside the other non-coin classes: an index
    // ETF perpetual is not crypto. Measured 01.10 all 6 `["ETF"]`-only symbols
    // also carry `channel: nasdaq`, so today they pick up `Stock` and never
    // reach the fallback — but that is an accident of Aster's labelling, not a
    // rule, and the next ETF listed without a channel would come out crypto.
    if !tags.iter().any(|t| {
        matches!(
            t,
            Tag::Stock | Tag::Forex | Tag::Commodities | Tag::Rwa | Tag::Etf
        )
    }) {
        push(&mut tags, Tag::Crypto);
    }
    tags
}

/// Markets for the tests of the ported strategy modules.
#[cfg(test)]
pub(crate) mod fixtures {
    use super::{Catalog, Market, Tag};

    /// A market with no prices, no band and the given grid.
    pub(crate) fn market(symbol: &str, base: &str, tick: f64, step: f64) -> Market {
        Market {
            symbol: symbol.into(),
            base: base.into(),
            quote: "USDT".into(),
            long_name: String::new(),
            price_precision: super::decimals_of(tick),
            quantity_precision: super::decimals_of(step),
            tick_size: tick,
            step_size: step,
            min_qty: step,
            max_qty: 1_000_000.0,
            market_max_qty: 1_000_000.0,
            min_notional: 5.0,
            min_price: tick,
            max_price: 1_000_000.0,
            multiplier_up: 0.0,
            multiplier_down: 0.0,
            max_num_orders: 200,
            max_num_algo_orders: 10,
            trigger_protect: 0.02,
            market_take_bound: 0.02,
            maint_margin_percent: 2.5,
            required_margin_percent: 5.0,
            liquidation_fee: 0.025,
            trading: true,
            has_sessions: false,
            delivery_ms: None,
            tags: vec![Tag::Crypto],
            quote_volume_24h: None,
            last_price: None,
            bid: None,
            ask: None,
            mark_price: None,
            funding: None,
            feed_fresh: true,
        }
    }

    /// `BTCUSDT` at index 0 and the given `(symbol, base, tick, step)`
    /// markets after it; the symbols must sort after `BTCUSDT` and in the
    /// order given, or the indexes the caller counts on move.
    pub(crate) fn sber_catalog_of(rest: &[(&str, &str, f64, f64)]) -> Catalog {
        let mut btc = market("BTCUSDT", "BTC", 0.1, 0.001);
        btc.tags = vec![Tag::Top, Tag::Crypto];
        let mut markets = vec![btc];
        markets.extend(rest.iter().map(|&(s, b, t, st)| market(s, b, t, st)));
        let c = Catalog::of(markets);
        debug_assert!(rest
            .iter()
            .enumerate()
            .all(|(i, (s, ..))| c.index_of_symbol(s) == Some(i as u16 + 1)));
        c
    }

    /// TInvestCore's two-market test model on Aster's catalog: `BTCUSDT` at
    /// index 0 (where TInvestCore had its service market; here it is the
    /// delta reference) and its SBER fixture at index 1 — keyed `u-sber`, so
    /// it sorts after `BTCUSDT`, with a lot of 10 and a tick of 0.01.
    pub(crate) fn sber_catalog() -> Catalog {
        let mut btc = market("BTCUSDT", "BTC", 0.1, 0.001);
        btc.tags = vec![Tag::Top, Tag::Crypto];
        Catalog::of(vec![btc, market("u-sber", "SBER", 0.01, 10.0)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btc() -> Market {
        // BTCUSDT as `exchangeInfo` gave it on 01.10.
        Market {
            symbol: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            long_name: String::new(),
            price_precision: 1,
            quantity_precision: 3,
            tick_size: 0.1,
            step_size: 0.001,
            min_qty: 0.001,
            max_qty: 1000.0,
            market_max_qty: 120.0,
            min_notional: 5.0,
            min_price: 1.0,
            max_price: 1_000_000.0,
            multiplier_up: 1.02,
            multiplier_down: 0.98,
            max_num_orders: 200,
            max_num_algo_orders: 10,
            trigger_protect: 0.02,
            market_take_bound: 0.02,
            maint_margin_percent: 2.5,
            required_margin_percent: 5.0,
            liquidation_fee: 0.025,
            trading: true,
            has_sessions: false,
            delivery_ms: None,
            tags: vec![Tag::Top, Tag::Crypto],
            quote_volume_24h: None,
            last_price: None,
            bid: None,
            ask: None,
            mark_price: None,
            funding: None,
            feed_fresh: true,
        }
    }

    /// MoonBot's tag filter over Aster's taxonomy: a market is in when one of
    /// its tags is and none is excluded; only exclusions start from all; an
    /// unknown tag is an error, never a wider set.
    #[test]
    fn market_tags_include_any_and_exclude_any() {
        let doge = [Tag::Meme, Tag::Crypto];
        let nvda = [Tag::Stock];
        let t = |s: &str| MarketTags::parse(s).unwrap();
        assert!(t("crypto").matches(&doge) && !t("crypto").matches(&nvda));
        assert!(t("Meme, STOCK").matches(&doge) && t("meme;stock").matches(&nvda));
        assert!(!t("crypto, !meme").matches(&doge), "an excluded tag wins");
        assert!(t("!meme").matches(&nvda) && !t("!meme").matches(&doge));
        assert!(t("").is_empty() && !t("").matches(&doge));
        assert_eq!(MarketTags::parse("crypto, fx"), Err("fx".to_string()));
        for name in MarketTags::PICKLIST.split('|') {
            assert!(MarketTags::parse(name).is_ok(), "{name}");
        }
    }

    /// What the trader typed into `CoinsWhiteList` on 02.10 (`btc, eth`) found
    /// nothing, and both MoonShot strategies stood without a market: MoonBot
    /// lists name coins. A coin is its one market; a symbol still wins.
    #[test]
    fn typed_market_is_a_symbol_or_a_coin_in_any_case() {
        use fixtures::market;
        let c = Catalog::of(vec![
            market("1000PEPEUSDT", "1000PEPE", 0.0000001, 1.0),
            market("BTCUSDT", "BTC", 0.1, 0.001),
            market("ETHUSDT", "ETH", 0.01, 0.001),
        ]);
        for typed in ["BTCUSDT", "btcusdt", "BTC", "btc", "Btc"] {
            assert_eq!(c.index_of_symbol_ci(typed), Some(1), "{typed}");
        }
        assert_eq!(c.index_of_symbol_ci("eth"), Some(2));
        assert_eq!(c.index_of_symbol_ci("1000pepe"), Some(0));
        assert_eq!(c.index_of_symbol_ci("pepe"), Some(0), "the folded coin");
        for typed in ["", "nope", "USDT", "bt"] {
            assert_eq!(c.index_of_symbol_ci(typed), None, "{typed}");
            assert!(!c.symbol_is_ambiguous(typed), "{typed}");
        }
        // Two markets on one base is no answer, and said so.
        let mut usdc = market("BTCUSDC", "BTC", 0.1, 0.001);
        usdc.quote = "USDC".into();
        let two = Catalog::of(vec![market("BTCUSDT", "BTC", 0.1, 0.001), usdc]);
        assert_eq!(two.index_of_symbol_ci("btc"), None);
        assert!(two.symbol_is_ambiguous("btc"));
        assert_eq!(two.index_of_symbol_ci("btcusdc"), Some(0));
    }

    #[test]
    fn floor_qty_snaps_down_to_the_step() {
        let m = btc();
        assert_eq!(m.floor_qty(0.0019), Some(0.001));
        assert_eq!(m.floor_qty(0.001), Some(0.001));
        assert_eq!(m.floor_qty(0.0009), Some(0.0));
        assert_eq!(m.floor_qty(1.7), Some(1.7));
        // Negative and non-finite are not sizes.
        assert_eq!(m.floor_qty(-1.0), None);
        assert_eq!(m.floor_qty(f64::NAN), None);
    }

    #[test]
    fn floor_qty_does_not_lose_a_whole_step_to_the_division() {
        // MEASURED over this catalog's nine real step sizes: 714 of ~18000
        // exact multiples come out of `qty / step` just under their integer, so
        // a plain `.floor()` drops a step. These are the smallest cases of
        // each, and before the quotient was snapped the first answered 0.28 —
        // a 3.4% short order, which no later rounding recovers.
        let mut m = btc();
        m.step_size = 0.01;
        m.min_qty = 0.01;
        for (asked, want) in [(0.29, 0.29), (0.58, 0.58), (1.16, 1.16), (2.05, 2.05)] {
            assert_eq!(m.floor_qty(asked), Some(want), "step 0.01, qty {asked}");
        }
        // Still a floor, not a round: a value genuinely between two steps goes
        // down, and the snap must not pull it up.
        assert_eq!(m.floor_qty(0.299), Some(0.29));
        assert_eq!(m.floor_qty(0.2999999), Some(0.29));

        m.step_size = 0.1;
        m.min_qty = 0.1;
        assert_eq!(m.floor_qty(2.9), Some(2.9));
        assert_eq!(m.floor_qty(0.7), Some(0.7));
        assert_eq!(m.floor_qty(0.79), Some(0.7));
    }

    #[test]
    fn step_decimals_come_from_the_step_not_a_separate_field() {
        // Aster spells the same step both ways (`0.0010` and `0.001` both occur
        // in the live catalog), and `quantityPrecision` is a field that can be
        // absent — which would default to 0 and turn every fractional size into
        // nothing. The grid is derived from the step instead.
        assert_eq!(decimals_of(0.001), 3);
        assert_eq!(decimals_of(0.01), 2);
        assert_eq!(decimals_of(0.1), 1);
        assert_eq!(decimals_of(1.0), 0);
        assert_eq!(decimals_of(0.0), 0);

        let mut m = btc();
        m.quantity_precision = 0; // as an absent field would deserialize
        assert_eq!(m.floor_qty(0.0019), Some(0.001));
    }

    #[test]
    fn an_unsizable_market_refuses_instead_of_sizing_unbounded() {
        // No LOT_SIZE filter leaves step, minQty, maxQty and MIN_NOTIONAL all
        // at 0, so every check below would pass vacuously. Measured 01.10 this
        // does not occur, but the degenerate path must not wave an order
        // through that no filter bounded.
        let mut m = btc();
        m.step_size = 0.0;
        m.min_qty = 0.0;
        m.max_qty = 0.0;
        m.market_max_qty = 0.0;
        m.min_notional = 0.0;
        assert!(!m.sizable());
        assert_eq!(m.floor_qty(1.0), None);
        assert_eq!(
            m.size_from_notional(1000.0, 83_633.8, OrderKind::Limit),
            Err(SizeError::NotSizable)
        );
    }

    #[test]
    fn market_orders_use_the_tighter_market_ceiling() {
        // BTCUSDT caps a limit order at 1000 and a market order at 120. One
        // ceiling would be wrong for one of the two, which is why the kind is
        // a parameter rather than a default.
        let m = btc();
        assert_eq!(m.max_qty_for(OrderKind::Limit), 1000.0);
        assert_eq!(m.max_qty_for(OrderKind::Market), 120.0);
        let price = 100.0;
        // 500 units: fine as a limit, over the market ceiling.
        assert_eq!(
            m.size_from_notional(50_000.0, price, OrderKind::Limit),
            Ok(500.0)
        );
        assert!(matches!(
            m.size_from_notional(50_000.0, price, OrderKind::Market),
            Err(SizeError::AboveMaxQty {
                kind: OrderKind::Market,
                ..
            })
        ));
        // A market with no MARKET_LOT_SIZE falls back to the limit ceiling
        // rather than to "no ceiling".
        let mut no_mkt = m.clone();
        no_mkt.market_max_qty = 0.0;
        assert_eq!(no_mkt.max_qty_for(OrderKind::Market), 1000.0);
    }

    #[test]
    fn notional_sizes_down_and_names_every_refusal() {
        let m = btc();
        let price = 83_633.8;
        // $1000 at that price is 0.011956… -> 0.011.
        assert_eq!(
            m.size_from_notional(1000.0, price, OrderKind::Limit),
            Ok(0.011)
        );
        // $5 is exactly MIN_NOTIONAL, but one step is 0.001 = $83.6, so the
        // rounded size is 0 and the floor is reported as minQty, not notional.
        assert!(matches!(
            m.size_from_notional(5.0, price, OrderKind::Limit),
            Err(SizeError::BelowMinQty { .. })
        ));
        assert_eq!(
            m.size_from_notional(0.0, price, OrderKind::Limit),
            Err(SizeError::NoNotional)
        );
        assert_eq!(
            m.size_from_notional(100.0, 0.0, OrderKind::Limit),
            Err(SizeError::NoPrice)
        );
    }

    #[test]
    fn min_notional_is_checked_after_rounding_not_before() {
        // A market whose step is coarse enough that rounding drops a request
        // from above MIN_NOTIONAL to below it. Asking the exchange instead
        // costs a round trip and an order-budget slot for a certain refusal.
        let mut m = btc();
        m.step_size = 1.0;
        m.quantity_precision = 0;
        m.min_qty = 1.0;
        m.min_notional = 10.0;
        // 1.9 units at $6 is $11.40 asked, but 1 unit at $6 is $6 placed.
        assert!(matches!(
            m.size_from_notional(11.4, 6.0, OrderKind::Limit),
            Err(SizeError::BelowMinNotional { .. })
        ));
        // And an order that lands ON the floor is not refused for a rounding
        // hair. The pair is SEARCHED FOR, not guessed: `3 * 0.15` in f64 is
        // 0.44999999999999996, while the exchange's own `"0.45"` parses to
        // 0.45 — so `got < min_notional` holds on a notional that is exactly
        // the limit, and the core would refuse an order Aster would have taken.
        // (A first attempt at this test asserted a pair that turned out not to
        // fall short at all, and passed for the wrong reason.)
        let mut tol = btc();
        tol.step_size = 1.0;
        tol.min_qty = 1.0;
        tol.max_qty = 1000.0;
        tol.tick_size = 0.01;
        tol.min_notional = 0.45;
        // Read through the struct so the comparison is not a constant clippy
        // can fold — as a guard it has to run, or the test can start passing
        // because the pair stopped falling short rather than because the
        // tolerance works.
        let price = 0.15;
        let qty = 3.0;
        assert!(
            qty * price < tol.min_notional,
            "this pair must really fall short, or the tolerance is untested"
        );
        assert_eq!(
            tol.size_from_notional(tol.min_notional, price, OrderKind::Limit),
            Ok(qty)
        );
    }

    #[test]
    fn btc_is_the_delta_reference_and_nothing_else_is() {
        assert!(btc().is_btc_reference());
        let mut eth = btc();
        eth.symbol = "ETHUSDT".into();
        eth.base = "ETH".into();
        assert!(!eth.is_btc_reference());
    }

    fn sym(symbol: &str, channel: &str, sub: &[&str], status: &str, mode: i32) -> SymbolInfo {
        SymbolInfo {
            symbol: symbol.into(),
            base_asset: symbol.trim_end_matches("USDT").into(),
            quote_asset: "USDT".into(),
            margin_asset: "USDT".into(),
            status: status.into(),
            contract_type: "PERPETUAL".into(),
            price_precision: 2,
            quantity_precision: 3,
            delivery_date_ms: NO_DELIVERY_MS,
            onboard_date_ms: 0,
            name: String::new(),
            channel: channel.into(),
            underlying_sub_type: sub.iter().map(|s| (*s).into()).collect(),
            trading_mode: mode,
            order_types: Vec::new(),
            time_in_force: Vec::new(),
            maint_margin_percent: 2.5,
            required_margin_percent: 5.0,
            liquidation_fee: 0.025,
            market_take_bound: 0.02,
            trigger_protect: 0.02,
            filters: Vec::new(),
        }
    }

    /// An `exchangeInfo` answer carrying just these symbols.
    fn info(symbols: Vec<SymbolInfo>) -> ExchangeInfo {
        ExchangeInfo {
            server_time_ms: 0,
            timezone: "UTC".into(),
            rate_limits: Vec::new(),
            symbols,
        }
    }

    fn ticker(symbol: &str, last_price: f64, quote_volume: f64) -> Ticker24h {
        Ticker24h {
            symbol: symbol.into(),
            last_price,
            quote_volume,
            volume: 0.0,
            price_change_percent: 0.0,
            count: 0,
        }
    }

    /// A funding row with no mark price, which is the shape that keeps the
    /// funding assertions about funding alone; the mark price has its own test.
    fn premium(symbol: &str, rate: f64, next_ms: i64) -> PremiumIndex {
        PremiumIndex {
            symbol: symbol.into(),
            last_funding_rate: rate,
            next_funding_time_ms: next_ms,
            mark_price: 0.0,
        }
    }

    /// A row that carries a mark price and no funding.
    fn premium_marked(symbol: &str, mark: f64) -> PremiumIndex {
        PremiumIndex {
            symbol: symbol.into(),
            last_funding_rate: 0.0,
            next_funding_time_ms: 0,
            mark_price: mark,
        }
    }

    fn book(symbol: &str, bid: f64, ask: f64) -> BookTicker {
        BookTicker {
            symbol: symbol.into(),
            bid_price: bid,
            ask_price: ask,
        }
    }

    #[test]
    fn taxonomy_follows_the_measured_catalog() {
        // Plain coin: `{}` is the literal string Aster uses for "no channel".
        assert_eq!(
            tags_of(&sym("ADAUSDT", "{}", &[], "TRADING", 0)),
            [Tag::Crypto]
        );
        // A Nasdaq name carries both markers and must not come out twice.
        assert_eq!(
            tags_of(&sym("AAPLUSDT", "nasdaq", &["STOCK"], "TRADING", 1)),
            [Tag::Stock]
        );
        assert_eq!(
            tags_of(&sym(
                "NVDAUSDT",
                "nasdaq",
                &["STOCK", "Semiconductor"],
                "TRADING",
                1
            )),
            [Tag::Stock]
        );
        // Meme is still crypto: the fallback is decided on the hard classes
        // only, so a coin keeps `crypto` alongside its flavour.
        assert_eq!(
            tags_of(&sym("1000PEPEUSDT", "", &["Meme"], "TRADING", 0)),
            [Tag::Meme, Tag::Crypto]
        );
        // Gold trades on the forex channel and is labelled a commodity: both
        // are hard classes, so it is not crypto.
        assert_eq!(
            tags_of(&sym("XAUUSDT", "forex", &["Commodities"], "TRADING", 1)),
            [Tag::Forex, Tag::Commodities]
        );
        // Pre-launch arrives two ways and must collapse to one tag.
        assert_eq!(
            tags_of(&sym("NEWUSDT", "", &["pre-launch"], "PENDING_TRADING", 0)),
            [Tag::PreLaunch, Tag::Crypto]
        );
    }

    #[test]
    fn non_trading_markets_stay_in_the_catalog() {
        // A position on a settling or pre-launch market still needs an exit,
        // and a market absent from the catalog is one the terminal cannot show
        // or close. Trading is gated by `Market::trading`, not by membership.
        let info = ExchangeInfo {
            server_time_ms: 0,
            timezone: "UTC".into(),
            rate_limits: Vec::new(),
            symbols: vec![
                sym("BTCUSDT", "", &["Top"], "TRADING", 0),
                sym("TONUSDT", "", &[], "SETTLING", 0),
                // The 5 PENDING_TRADING symbols measured on 01.10 carry an
                // EMPTY contractType, and the catalog holds no dated contract
                // type at all — so rejecting the empty value would have dropped
                // only pre-launch perpetuals, the opposite of the check's point.
                {
                    let mut s = sym("MBLUSDT", "", &[], "PENDING_TRADING", 0);
                    s.contract_type = String::new();
                    s
                },
                // Wrong quote: not this core's market.
                {
                    let mut s = sym("XUSD1", "", &[], "TRADING", 0);
                    s.quote_asset = "USD1".into();
                    s
                },
                // A NAMED non-perpetual type is still kept out.
                {
                    let mut s = sym("BTCUSDT_260327", "", &[], "TRADING", 0);
                    s.contract_type = "CURRENT_QUARTER".into();
                    s
                },
            ],
        };
        let cat = Catalog::build(&info);
        assert_eq!(cat.markets().len(), 3);
        assert_eq!(cat.skipped_quote, 1);
        assert_eq!(cat.skipped_not_a_perpetual, 1);
        assert_eq!(cat.trading().count(), 1);
        assert!(cat.get("TONUSDT").is_some_and(|m| !m.trading));
        // Pre-launch is present, not trading, and tagged as such.
        let pending = cat.get("MBLUSDT").expect("pre-launch kept");
        assert!(!pending.trading);
        assert!(pending.tags.contains(&Tag::PreLaunch));
        // Sorted, so `get` may binary-search.
        assert!(cat.get("BTCUSDT").is_some());
        assert!(cat.get("NOPE").is_none());
        // Every one of these came without filters, so none may be sized.
        assert_eq!(cat.unsizable, 3);
    }

    #[test]
    fn the_funding_fraction_becomes_a_percent_exactly_once() {
        let mut m = btc();
        // `lastFundingRate` as the exchange answered it on 01.10.
        m.funding = Some(Funding {
            rate: 0.000_089_79,
            next_ms: 1_790_870_400_000,
        });
        let f = spec_of(&m).funding.expect("funding");
        // 0.009 %, which is what the terminal's column prints verbatim. A
        // fraction left unconverted reads as 0.00009 % — no funding anywhere;
        // a second multiplication reads as 0.9 %, a rate no venue charges.
        assert!((f.rate_pct - 0.008_979).abs() < 1e-12, "{}", f.rate_pct);
        assert_eq!(f.time_ms, 1_790_870_400_000);
    }

    #[test]
    fn a_market_the_exchange_published_no_funding_for_sends_none() {
        // `MBLUSDT`, measured 01.10: in `exchangeInfo`, absent from
        // `premiumIndex`. A zero rate here would be indistinguishable from the
        // 97 markets that really do sit at zero between charges.
        assert!(spec_of(&btc()).funding.is_none());
    }

    #[test]
    fn a_1000_alias_is_folded_for_identity_and_kept_for_matching() {
        let mut m = btc();
        m.symbol = "1000SHIBUSDT".into();
        m.base = "1000SHIB".into();
        let s = spec_of(&m);
        // The token strategy coin lists match against, as the exchange spells it.
        assert_eq!(s.currency, "1000SHIB");
        // The cross-exchange identity, folded like every other core folds it.
        assert_eq!(s.currency_canonic, "SHIB");
        assert_eq!(s.leading1000, "1000SHIB");
        assert_eq!(s.k1000, 1000);

        let plain = spec_of(&btc());
        assert_eq!(plain.currency_canonic, "BTC");
        assert!(plain.leading1000.is_empty());
        // One, not zero: zero is the one value a multiplier cannot be.
        assert_eq!(plain.k1000, 1);
    }

    #[test]
    fn a_coin_that_merely_starts_with_a_digit_is_not_an_alias() {
        let mut m = btc();
        // Neither of these is a 1000-multiplier listing, and Aster has no
        // symbol of this shape — the guard is here so a future one does not
        // silently lose a thousandfold in its identity.
        for base in ["1000", "10001", "1INCH"] {
            m.base = base.into();
            assert_eq!(m.alias_1000(), None, "{base}");
            assert_eq!(spec_of(&m).k1000, 1, "{base}");
        }
    }

    #[test]
    fn max_leverage_comes_from_the_margin_percent_and_never_over_promises() {
        let mut m = btc();
        // The whole measured distribution of 01.10, in order of how many
        // symbols carry it: 33.33 % on 358, 5 % on 123, 50 % on 74, 20 % on 32,
        // 25 % on 5, 10 % on 4.
        for (percent, expected) in [
            (33.33, 3),
            (5.0, 20),
            (50.0, 2),
            (20.0, 5),
            (25.0, 4),
            (10.0, 10),
        ] {
            m.required_margin_percent = percent;
            assert_eq!(m.max_leverage(), expected, "{percent}");
        }
        // A missing or unreadable percent claims 1×, which cannot over-promise.
        for percent in [0.0, -1.0, f64::NAN] {
            m.required_margin_percent = percent;
            assert_eq!(m.max_leverage(), 1, "{percent}");
        }
    }

    #[test]
    fn a_usdt_perpetual_is_not_listed_as_spot() {
        let s = spec_of(&btc());
        // `EMPTY` is what the terminal reads as spot (`Market::listed_type`),
        // and it is what this encoder inherited from a MOEX core.
        assert_eq!(s.futures_type, BaseCurrency::USDT);
        assert_eq!(s.base_currency, "USDT");
        assert!(s.is_btc_market);
        // The band and the price bounds, which decide whether an order price is
        // acceptable at all, must reach the terminal as the exchange states them.
        assert_eq!((s.multiplier_down, s.multiplier_up), (0.98, 1.02));
        assert_eq!((s.min_price, s.max_price), (1.0, 1_000_000.0));
    }

    #[test]
    fn the_delivery_sentinel_never_reaches_the_wire() {
        let vanilla = Catalog::build(&info(vec![sym("BTCUSDT", "{}", &[], "TRADING", 0)]));
        assert_eq!(spec_of(&vanilla.markets()[0]).delivery_time_ms, None);

        // `TONUSDT` as measured 01.10: `SETTLING`, with a real date.
        let mut dated = sym("TONUSDT", "{}", &[], "SETTLING", 0);
        dated.delivery_date_ms = 1_781_859_600_000;
        let cat = Catalog::build(&info(vec![dated]));
        let s = spec_of(&cat.markets()[0]);
        assert_eq!(s.delivery_time_ms, Some(1_781_859_600_000));
        // Still in the catalog, and still not tradable: a position in a
        // settling market needs an exit more than an absent market does.
        assert!(!s.status_trading);
    }

    #[test]
    fn merged_answers_are_matched_by_symbol_not_zipped_by_position() {
        // The real asymmetry of 01.10: `ticker/24hr` answers 608 rows and
        // `premiumIndex` 766 for a 596-market catalog, while `MBLUSDT` is in
        // the catalog and in neither answer.
        let mut cat = Catalog::build(&info(vec![
            sym("BTCUSDT", "{}", &[], "TRADING", 0),
            sym("MBLUSDT", "{}", &[], "TRADING", 0),
        ]));
        let filled = cat.apply_tickers(&[
            ticker("ETHUSD1", 1.0, 7.0), // a quote this core does not carry
            ticker("BTCUSDT", 83_906.0, 658_458_631.25),
        ]);
        assert_eq!(filled, 1);
        let filled = cat.apply_premium_index(&[
            premium("GNSUSD", 0.001, 1), // an index symbol, not a market
            premium("BTCUSDT", 0.000_089_79, 1_790_870_400_000),
        ]);
        assert_eq!(filled, 1);

        let btc = cat.get("BTCUSDT").expect("BTCUSDT");
        assert_eq!(btc.quote_volume_24h, Some(658_458_631.25));
        assert_eq!(btc.last_price, Some(83_906.0));
        assert!(btc.funding.is_some());

        let mbl = cat.get("MBLUSDT").expect("MBLUSDT");
        // Nothing borrowed from the neighbouring row, which is exactly what a
        // positional merge would have done here.
        assert_eq!(mbl.quote_volume_24h, None);
        assert_eq!(mbl.funding, None);
        assert_eq!(spec_of(mbl).volume, 0.0);
        assert!(cat.summary().contains("funding 1/2"), "{}", cat.summary());
    }

    /// The price rows are what `UpdateMarketsList` answers every 2 s, and two rules
    /// decide them: one row per market whatever its state, and a quote only
    /// where the exchange published one.
    #[test]
    fn price_rows_cover_every_market_and_quote_only_the_quoted_ones() {
        let mut cat = Catalog::build(&info(vec![
            sym("AAAUSDT", "{}", &[], "TRADING", 0),
            sym("BBBUSDT", "{}", &[], "SETTLING", 0),
            sym("CCCUSDT", "{}", &[], "TRADING", 0),
        ]));
        // What the exchange answered on 01.10: a row per TRADING symbol and
        // none for the others.
        assert_eq!(
            cat.apply_book(&[
                book("CCCUSDT", 9.0, 9.5),
                book("AAAUSDT", 1.0, 1.5),
                book("ZZZUSD1", 7.0, 7.5), // a quote this core does not carry
            ]),
            2
        );
        cat.apply_premium_index(&[PremiumIndex {
            symbol: "AAAUSDT".into(),
            last_funding_rate: 0.0001,
            next_funding_time_ms: 1_790_870_400_000,
            mark_price: 1.25,
        }]);

        let rows = cat.prices();
        assert_eq!(rows.len(), 3, "every market is sent, quoted or not");
        // Indexes are positions in the catalog, not in the answer.
        assert_eq!((rows[0].m_index, rows[0].bid, rows[0].ask), (0, 1.0, 1.5));
        assert_eq!(rows[0].last, 1.25, "the mark price rides the same row");
        assert_eq!(
            (rows[1].m_index, rows[1].bid, rows[1].ask, rows[1].last),
            (1, 0.0, 0.0, 0.0),
            "an unquoted market is sent as zeros, which both sides read as absent"
        );
        assert_eq!((rows[2].m_index, rows[2].bid, rows[2].ask), (2, 9.0, 9.5));
        assert_eq!(
            rows[2].last, 0.0,
            "a market with no mark price says so, and `mark_price_found` is 0"
        );
        // `GetMarketsIndexes` must agree with those positions or every later
        // indexed packet lands on the wrong market.
        assert_eq!(cat.symbols(), ["AAAUSDT", "BBBUSDT", "CCCUSDT"]);
        assert_eq!(cat.index_of_symbol("CCCUSDT"), Some(2));
        assert_eq!(cat.index_of_symbol("NOSUCHUSDT"), None);
        assert!(
            cat.summary().contains("quoted 2/3") && cat.summary().contains("marks 1/3"),
            "{}",
            cat.summary()
        );
    }

    /// The answer is the exchange's whole word, so what it stops naming stops
    /// being quoted. Keeping the old row is how a delisted market would go on
    /// showing a price nobody is offering for as long as the core runs.
    #[test]
    fn a_market_missing_from_a_later_answer_loses_its_quote() {
        let mut cat = Catalog::build(&info(vec![
            sym("AAAUSDT", "{}", &[], "TRADING", 0),
            sym("BBBUSDT", "{}", &[], "TRADING", 0),
        ]));
        assert_eq!(
            cat.apply_book(&[book("AAAUSDT", 1.0, 1.5), book("BBBUSDT", 2.0, 2.5)]),
            2
        );
        // BBBUSDT went `SETTLING` between two answers.
        assert_eq!(cat.apply_book(&[book("AAAUSDT", 1.0, 1.5)]), 1);
        let bbb = cat.get("BBBUSDT").expect("BBBUSDT");
        assert_eq!((bbb.bid, bbb.ask), (None, None));
        assert_eq!(cat.prices()[1].bid, 0.0, "and the terminal is told so");

        // The same rule under the premium answer: a market that drops out of it
        // has no mark price, not the last one it had.
        cat.apply_premium_index(&[premium_marked("AAAUSDT", 1.25)]);
        cat.apply_premium_index(&[premium_marked("BBBUSDT", 2.25)]);
        assert_eq!(cat.get("AAAUSDT").expect("AAAUSDT").mark_price, None);
        assert_eq!(cat.get("BBBUSDT").expect("BBBUSDT").mark_price, Some(2.25));

        // An empty answer is the same statement about every market, and it is
        // how the refresher reports an outage it has given up on.
        assert_eq!(cat.apply_book(&[]), 0);
        assert!(cat.prices().iter().all(|r| r.bid == 0.0 && r.ask == 0.0));
    }

    /// Both sides of a row come off one book, so a crossed pair is garbage from
    /// a partial or reordered answer — not a market to quote.
    #[test]
    fn a_crossed_or_half_quote_is_not_a_quote() {
        let mut cat = Catalog::build(&info(vec![sym("AAAUSDT", "{}", &[], "TRADING", 0)]));
        for (bid, ask) in [(1.0, 0.0), (0.0, 1.5), (1.6, 1.5)] {
            assert_eq!(
                cat.apply_book(&[book("AAAUSDT", bid, ask)]),
                0,
                "{bid}/{ask}"
            );
            let m = cat.get("AAAUSDT").expect("AAAUSDT");
            assert_eq!((m.bid, m.ask), (None, None), "{bid}/{ask}");
        }
        // The equal pair is a real one-tick market, not a crossed book.
        assert_eq!(cat.apply_book(&[book("AAAUSDT", 1.5, 1.5)]), 1);
    }

    /// Zero is how an absent or unreadable `markPrice` decodes, and a market
    /// priced at nothing is not a reference price.
    #[test]
    fn a_zero_mark_price_is_absent_rather_than_zero() {
        let mut cat = Catalog::build(&info(vec![sym("AAAUSDT", "{}", &[], "TRADING", 0)]));
        cat.apply_premium_index(&[premium("AAAUSDT", 0.0001, 1_790_870_400_000)]);
        assert_eq!(cat.get("AAAUSDT").expect("AAAUSDT").mark_price, None);
        assert!(cat.summary().contains("marks 0/1"), "{}", cat.summary());
    }

    #[test]
    fn sessions_follow_the_venue_channel_and_not_the_live_trading_mode() {
        // `tradingMode` is passed as the live flag said at each moment, and
        // none of it may move `has_sessions`: measured 01.10, all 101 `nasdaq`
        // symbols flipped from 1 to 0 three hours apart while remaining
        // Nasdaq stocks.
        for (channel, mode, expected) in [
            ("nasdaq", 1, true),
            ("nasdaq", 0, true),
            // Forex carried 0 in both live snapshots, and forex has sessions.
            ("forex", 0, true),
            ("hkstock", 0, true),
            // An unknown venue counts as session-bearing: the safe side of the
            // gate, and the `catalog:` line names it besides.
            ("newstock", 0, true),
            ("{}", 1, false),
            ("", 0, false),
        ] {
            let m = market_of(&sym("XUSDT", channel, &[], "TRADING", mode));
            assert_eq!(m.has_sessions, expected, "{channel} mode {mode}");
        }
    }

    #[test]
    fn a_funding_row_without_a_charge_time_is_not_funding() {
        let mut cat = Catalog::build(&info(vec![sym("BTCUSDT", "{}", &[], "TRADING", 0)]));
        // A rate with no time: the terminal would show no funding whatever the
        // rate says, so storing it would be a figure in the journal and nothing
        // on screen.
        assert_eq!(
            cat.apply_premium_index(&[premium("BTCUSDT", 0.000_1, 0)]),
            0
        );
        assert_eq!(cat.get("BTCUSDT").expect("BTCUSDT").funding, None);
        assert!(cat.summary().contains("funding 0/1"), "{}", cat.summary());

        // The same row with a time is kept.
        assert_eq!(
            cat.apply_premium_index(&[premium("BTCUSDT", 0.000_1, 1_790_870_400_000)]),
            1
        );
        assert!(cat.get("BTCUSDT").expect("BTCUSDT").funding.is_some());
    }

    #[test]
    fn an_unknown_channel_is_counted_rather_than_silently_crypto() {
        // A new venue on Aster would otherwise take the crypto fallback with
        // nothing said. The tag still falls back — there is nothing better to
        // guess — but the journal line names the value.
        let info = ExchangeInfo {
            server_time_ms: 0,
            timezone: "UTC".into(),
            rate_limits: Vec::new(),
            symbols: vec![
                sym("ADAUSDT", "{}", &[], "TRADING", 0),
                sym("XXXUSDT", "lse", &[], "TRADING", 1),
                sym("YYYUSDT", "lse", &[], "TRADING", 1),
            ],
        };
        let cat = Catalog::build(&info);
        assert_eq!(cat.unknown_channels, vec![("lse".to_string(), 2)]);
        assert!(cat.summary().contains("unknown channels: \"lse\" 2"));
        // The two known spellings of "none" are not unknown.
        assert!(is_known_channel(""));
        assert!(is_known_channel("{}"));
    }
}
