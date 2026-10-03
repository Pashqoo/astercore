//! MoonShot engine: MoonBot's algorithm as read off its logs (13.09 entries,
//! 12.09 exits) on core orders. For every checked strategy × universe market
//! a ladder of `OrdersCount` entries sits `MShotPrice` % below the market
//! (above it for `Short`) inside the observed entry corridor (see `far_distance`)
//! and follows the price; the first fill drops the other entries and the
//! position gets a `SellPrice` exit with PriceDown and a bot-side stop.
//! DropsDetection, MoonStrike and MoonHook strategies share the gates and the
//! exits; their entries go out once per detected drop (`drops.rs`), strike
//! (`strike.rs`) or hook (`hook.rs`) instead of following the price — a
//! MoonHook's then follow it inside the corridor its detect sized.
//! Pure decisions: reads `Strategies`, `Orders`, `Catalog`, `Windows`;
//! returns `Cmd`s the engine turns into order calls.
//!
//! Ported from TInvestCore. What changed on the way: the BTC deltas are
//! `BTCUSDT`'s and the market delta is the traded markets' mean (TInvestCore
//! read both off the MOEX index); a market trades while it is `TRADING`,
//! round the clock — the MOEX schedules and their open/close gates have no
//! counterpart; a fired stop of a live order closes with a MARKET order
//! (`PLAN.md`, «Открытые решения» п. 2) unless an allowed drop asks for a
//! limit; money is USDT.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use moonproto::server::codec::trade::{status, StartOrder};
use moonproto::{FieldValue, StrategyFields, StrategyKind, StrategySchema, StrategySnapshot};

use crate::bvsv;
use crate::drops::Tapes;
use crate::guards;
use crate::hook;
use crate::model::{Catalog as Model, Market, MarketTags};
use crate::orders::{reason, CoreOrder, Leg, Orders};
use crate::screener::{self, DynList};
use crate::stops::{self, exit_price, Fired};
use crate::strategies::{Strategies, MARKET_TAGS};
use crate::strike::{self, Tracks};
use crate::windows::Windows;

/// The market whose moves drive `MShotAddBTC*Delta` and `Delta_BTC_*`.
pub const BTC_SYMBOL: &str = "BTCUSDT";
/// No entries of a strategy on a market this long after one was refused. (-4140/-4141 are not
/// a cooldown: the market is closed to entries for `CLOSED_MARKET_MS`, `engine.rs`.) A refusal
/// that repeats until someone changes something — precision, the minimum notional, leverage —
/// is asked again a minute on.
const REJECT_COOLDOWN_MS: i64 = 60_000;
/// The same after a refusal for margin (`-2019`, `-2018`, `-4051`): on Aster margin is free
/// again the moment an order is cancelled or filled, and MoonBot tries again in ~5.6 s (its
/// 12.09 log). Until 03.10 it was the minute above, TInvestCore's, for T-Invest's error budget.
const MARGIN_COOLDOWN_MS: i64 = 10_000;

/// How long a refused entry keeps its strategy off the market ([`REJECT_COOLDOWN_MS`],
/// [`MARGIN_COOLDOWN_MS`]).
fn reject_cooldown_ms(msg: &str) -> i64 {
    use crate::aster::rest::msg_has_code;
    if [-2019, -2018, -4051].iter().any(|&c| msg_has_code(msg, c)) {
        MARGIN_COOLDOWN_MS
    } else {
        REJECT_COOLDOWN_MS
    }
}
/// Entry halts after a rate refusal that names no `Retry-After`: a ban (418) is two minutes at
/// least (API docs, «IP Limits»), `-1015` is the `ORDERS` 300 / 10 s window, a 429 or `-1003`
/// the `REQUEST_WEIGHT` minute.
const BAN_HALT_MS: i64 = 120_000;
const ORDER_BURST_HALT_MS: i64 = 10_000;
const RATE_HALT_MS: i64 = 60_000;

/// How long the exchange's rate refusal halts entries: its `Retry-After` when the answer named
/// one, else by the refusal — HTTP 418, `-1015` (too many new orders), HTTP 429 or `-1003
/// TOO_MANY_REQUESTS`. `None`: not a rate refusal. `rest::Error` reads `api {status}/{code}: …`.
pub fn rate_halt_ms(msg: &str) -> Option<i64> {
    use crate::aster::rest::{msg_has_code, msg_has_status, msg_retry_after_ms};
    let fallback = if msg_has_status(msg, 418) {
        BAN_HALT_MS
    } else if msg_has_code(msg, -1015) {
        ORDER_BURST_HALT_MS
    } else if msg_has_status(msg, 429) || msg_has_code(msg, -1003) {
        RATE_HALT_MS
    } else {
        return None;
    };
    Some(msg_retry_after_ms(msg).map_or(fallback, |ms| ms.max(1_000)))
}

/// The exchange refused a call for its rate ([`rate_halt_ms`]).
pub fn rate_limited(msg: &str) -> bool {
    rate_halt_ms(msg).is_some()
}
const CANCEL_RETRY_MS: i64 = 5_000;
/// After a restart, entries restored on a market wait this long for its
/// first live trade of this run (`Market::live_price`; the startup ticker's price is not one)
/// or a fresh book (`Market::book_fresh`); a market with neither (halted, feed
/// lost) withdraws them. Before 03.10 only a trade counted — TInvestCore's rule,
/// where the broker's quotes outlived a halt — and a thin coin's restored ladder
/// came off and went back on at its first trade.
const RESTORE_PRICE_WAIT_MS: i64 = 5 * 60_000;
/// A fired stop's exit follows the book at most this often.
const STOP_CHASE_MS: i64 = 2_000;
/// Failed exchange calls back off per order: 1 → 2 → … → 30 s.
const RETRY_BASE_MS: i64 = 1_000;
const RETRY_MAX_MS: i64 = 30_000;
/// A budget under one lot opens one lot while the lot costs at most this many
/// budgets; a dearer lot skips the entry (BTZ6 21.09: 7100 USDT on 1000).
const LOT_OVER_BUDGET: f64 = 1.5;
const PING_WINDOW_MS: i64 = 60_000;
const PING_MIN_SAMPLES: usize = 3;

/// What the engine must do for the strategies.
#[derive(Debug, Clone, PartialEq)]
pub enum Cmd {
    /// Place an entry (`Orders::start`).
    Start {
        order: StartOrder,
        tier: usize,
    },
    /// Move a leg; on `Leg::Sell` from BuyDone this places the exit.
    /// `reason`: the `SellReason` code of an exit move (`orders::reason`).
    /// `market`: a fired stop of a live order that only has to cross the
    /// book — it goes at MARKET instead (`PLAN.md`, «Открытые решения» п. 2)
    /// and `Orders` follows it as a panic; `price` is the limit it replaces.
    Move {
        order: u64,
        leg: Leg,
        price: f64,
        reason: u8,
        market: bool,
    },
    /// Reprice an entry and recalculate lots from the tier's USDT budget.
    /// `planned`: a new exit price for it, 0 = keep the planned ratio
    /// (MoonHook with `HookSellFixed` off re-prices its exit off the entry's
    /// new price instead).
    MoveEntry {
        order: u64,
        price: f64,
        size: f64,
        planned: f64,
    },
    /// Cancel an entry.
    Cancel {
        order: u64,
    },
    /// The bot stop for the order image (`price` 0 = none): the terminal
    /// shows SL:ON and the chart line from the image alone once the entry
    /// filled. `Orders::watch` never fires on it, `manage_exit` does.
    Stop {
        order: u64,
        price: f64,
        spread: f64,
    },
    /// A deleted strategy's position takes its last bot stop as the order's own stop, which
    /// `Orders::watch` fires on (`Orders::adopt_bot_stop`).
    AdoptStop {
        order: u64,
    },
    /// Detect fact for the terminal (row + chart mark).
    Detect {
        market: String,
        strategy_id: u64,
        is_short: bool,
        msg: String,
    },
    Log(String),
}

/// `MShotUsePrice`: the market price the ladder is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Anchor {
    Bid,
    Ask,
    Trade,
}

impl Anchor {
    fn parse(v: &str) -> Self {
        match v.trim().to_ascii_uppercase().as_str() {
            "ASK" => Self::Ask,
            "TRADE" => Self::Trade,
            _ => Self::Bid,
        }
    }

    /// The anchor price for a side; the last trade when the book side is
    /// unavailable. A short mirrors the two sides of the book: an entry it
    /// opens rests above the ask the way a long's rests below the bid, so
    /// `BID` means "the near side" and reads the ask for a short. Measuring
    /// a short from the bid would fold the whole spread into every distance
    /// it computes, which on a wide book is more than the distance itself.
    fn price(self, m: &Market, short: bool) -> f64 {
        let side = match (self, short) {
            (Self::Bid, false) | (Self::Ask, true) => m.bid_px(),
            (Self::Ask, false) | (Self::Bid, true) => m.ask_px(),
            (Self::Trade, _) => 0.0,
        };
        if side > 0.0 {
            side
        } else {
            m.last()
        }
    }
}

/// Strategy kinds the engine runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    MoonShot,
    Drops,
    Strike,
    Hook,
    /// Exits of the terminal's hand trades; never enters.
    Manual,
}

/// `MStrikeDirection` / `HookDirection`: the sides a strategy trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Both,
    Long,
    Short,
}

impl Direction {
    fn parse(v: &str) -> Self {
        match v.trim().to_ascii_lowercase().as_str() {
            "both" => Self::Both,
            "onlyshort" => Self::Short,
            _ => Self::Long,
        }
    }
}

/// Typed strategy fields (schema defaults for the ones not sent).
#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    pub kind: Kind,
    pub short: bool,
    pub emulator: bool,
    pub anchor: Anchor,
    pub white: Vec<String>,
    pub black: Vec<String>,
    /// `MarketTags`: the classes the screener picks from when the white list
    /// is empty; `Err` holds the first unknown tag.
    pub market_tags: Result<MarketTags, String>,
    pub max_active: i64,
    pub max_markets: i64,
    pub max_ping: i64,
    pub ping_cooldown_min: f64,
    /// No entries on a market this many seconds after a losing trade on it.
    pub penalty: f64,
    /// `MaxOrdersPerMarket` (0 = no limit): the strategy's live orders on
    /// one market.
    pub max_per_market: i64,
    /// `MaxPosition`, USDT (0 = off): no entries on a market whose position
    /// (every core order of the mode) cost this much.
    pub max_position: f64,
    /// `CheckFreeBalance`: an entry larger than the free money is not placed.
    pub check_free_balance: bool,
    /// `MinFreeBalance`, USDT: signal kinds place nothing below it (MoonShot
    /// not, as in MoonBot).
    pub min_free_balance: f64,
    /// `CancelBuyAfterSell`: a sale of the strategy on a market withdraws
    /// its entries there placed before it (the rest of the grid).
    pub cancel_after_sell: bool,
    /// The Filters tab as the list of checks an entry passes, in the order
    /// they are asked: the volume bounds, then the deltas, then `FilterBy`.
    /// `None` = `IgnoreFilters`, or nothing set.
    pub deltas: Option<DeltaFilters>,
    /// `WorkingTime`: when the strategy works (`Err`: not understood — it
    /// does not work).
    pub working_time: Result<Option<WorkWindow>, String>,
    /// `PreventWorkingUntil`, Unix s: stopped until then (0 = no).
    pub prevent_until: f64,
    /// `PenaltyTime`, s: off the market after 3 losing trades in a row on it
    /// or a manual order there.
    pub penalty_time: f64,
    /// `GlobalDetectPenalty`, s: no new ladder or signal on a market this
    /// long after another strategy's.
    pub global_detect_penalty: f64,
    /// `TotalLoss`, USDT (0 = off): no entries once the strategy's closed
    /// deals lost this much over the auto-stop window.
    pub total_loss: f64,
    /// The Sessions tab (`None` = `IgnoreSession`).
    pub session: Option<guards::SessionRule>,
    /// `Dyn_Refresh`: how often this strategy's pool is recomputed, seconds;
    /// clamped to `screener::REFRESH_MIN_MS` … `REFRESH_MAX_MS` (30 s … 1 h)
    /// and snapped to a grid shared by every strategy of the same period
    /// (`MoonShot::tick`).
    pub dyn_refresh: f64,
    /// MoonBot's dynamic white and black lists. `DynWL_*` ranks the class
    /// and keeps its head, and that head IS the pool now that the volume
    /// bounds have moved to the entry filters; `DynBL_*` is subtracted
    /// (`screener`).
    pub dyn_wl: DynList,
    pub dyn_bl: DynList,
    pub order_size: f64,
    /// Entries older than this many seconds are cancelled and re-placed (0 = never).
    pub auto_cancel: f64,
    pub price: f64,
    pub price_min: f64,
    pub add_15m: f64,
    pub add_1h: f64,
    pub add_3h: f64,
    /// `MShotAddBTCDelta` / `MShotAddBTC5mDelta`: shares of BTC's 1 h and
    /// 5 m ranges added to the ladder's distance.
    pub add_btc: f64,
    pub add_btc_5m: f64,
    pub add_distance: f64,
    pub raise_wait: f64,
    pub replace_delay: f64,
    pub orders_count: i64,
    pub step: f64,
    pub size_step: f64,
    pub expand: f64,
    /// `MShotRepeatAfterBuy`: a fill grants one more entry on the market when
    /// the price is `repeat_profit` % past the mean entry within `repeat_wait`
    /// s of the fill; it goes out `repeat_delay` s later.
    pub repeat: bool,
    pub repeat_profit: f64,
    pub repeat_wait: f64,
    pub repeat_delay: f64,
    pub sell_price: f64,
    pub pd_timer: f64,
    pub pd_delay: f64,
    pub pd_pct: f64,
    pub pd_relative: bool,
    pub pd_drop: f64,
    /// The `Stops` section (`stops.rs`).
    pub stops: stops::Config,
    /// DropsDetection: window, average and current-price samples, the drop
    /// that fires, the hourly-low condition, the detector's price as the
    /// entry base (see `drops.rs`).
    pub drops_max_time: f64,
    pub drops_price_ma: f64,
    pub drops_last_ma: i64,
    pub drops_delta: f64,
    pub drops_is_low: bool,
    pub drops_use_last: bool,
    /// `buyPrice`: tier 0 of a DropsDetection entry, % from the base price.
    pub buy_price: f64,
    /// `NextDetectPenalty`: no new signal on the market this many seconds
    /// after the last one.
    pub detect_penalty: f64,
    /// MoonStrike (see `strike.rs`): depth %, its `Add*` shifts per % of the
    /// 15 m / 1 h ranges and of the market's hourly delta, the USDT volume floor,
    /// the delay before the entry (ms), the entry level (% of the depth from
    /// the extreme, or with `strike_relative` off % from the price before
    /// the strike), the exit level (% of the depth from the extreme), the
    /// rebound wait and the sides.
    pub strike_depth: f64,
    pub strike_add_15m: f64,
    pub strike_add_1h: f64,
    pub strike_add_market: f64,
    pub strike_volume: f64,
    pub strike_delay_ms: i64,
    pub strike_level: f64,
    pub strike_relative: bool,
    pub strike_sell_level: f64,
    pub strike_wait_dip: bool,
    /// MoonHook (see `hook.rs`): the frame the move is measured in (s), the
    /// depth that fires and its cap, the mean-price reference, the turnover
    /// the frame must have traded, the rollback that must have held (% of the
    /// move, its cap and how long in ms), the fall before the move (% of the
    /// depth), where tier 0 sits and how wide its corridor is (both % of the
    /// move), the re-place delays, the exit level (% of the way from the entry
    /// back to the start of the move) and whether a moved entry re-plans it,
    /// and the ladder repeated after a profitable sale.
    pub hook_frame: f64,
    pub hook_depth: f64,
    pub hook_depth_max: f64,
    pub hook_anti_pump: bool,
    pub hook_min_volume: f64,
    pub hook_rollback: f64,
    pub hook_rollback_max: f64,
    pub hook_rollback_wait: i64,
    pub hook_drop_min: f64,
    pub hook_drop_max: f64,
    pub hook_initial: f64,
    pub hook_distance: f64,
    pub hook_replace_delay: f64,
    pub hook_raise_wait: f64,
    pub hook_sell_level: f64,
    pub hook_sell_fixed: bool,
    pub hook_repeat: bool,
    pub hook_repeat_profit: f64,
    pub direction: Direction,
}

impl Params {
    pub fn from_snapshot(s: &StrategySnapshot, schema: &StrategySchema) -> Self {
        let f = |name: &str| field(&s.fields, schema, name);
        let num = |name: &str| match f(name) {
            Some(FieldValue::Double(v)) => v,
            Some(FieldValue::Int32(v)) => f64::from(v),
            Some(FieldValue::Int64(v)) => v as f64,
            _ => 0.0,
        };
        let flag = |name: &str| matches!(f(name), Some(FieldValue::Bool(true)));
        let kind = match s.kind() {
            StrategyKind::DROPS => Kind::Drops,
            StrategyKind::MOON_STRIKE => Kind::Strike,
            StrategyKind::MOON_HOOK => Kind::Hook,
            StrategyKind::MANUAL => Kind::Manual,
            _ => Kind::MoonShot,
        };
        let text = |name: &str| match f(name) {
            Some(FieldValue::String(v)) => v,
            _ => String::new(),
        };
        let list = |name: &str| match f(name) {
            Some(FieldValue::String(v)) => v
                .split([',', ' ', ';'])
                .filter(|t| !t.is_empty())
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        };
        // DropsDetection buys long only, off the ask (`buyPriceLastTrade`:
        // the last trade, `DropsUseLastPrice`: the detector's price);
        // MoonStrike measures the book (`strike.rs`), MoonHook the trade tape
        // its detect is built from (`hook.rs`), so it needs no book.
        let direction = Direction::parse(&text(match kind {
            Kind::Hook => "HookDirection",
            _ => "MStrikeDirection",
        }));
        let anchor = match kind {
            Kind::MoonShot => Anchor::parse(&text("MShotUsePrice")),
            Kind::Drops if flag("buyPriceLastTrade") || flag("DropsUseLastPrice") => Anchor::Trade,
            Kind::Drops => Anchor::Ask,
            Kind::Strike => Anchor::Bid,
            Kind::Hook | Kind::Manual => Anchor::Trade,
        };
        Self {
            kind,
            short: match kind {
                Kind::MoonShot => flag("Short"),
                Kind::Drops | Kind::Manual => false,
                Kind::Strike | Kind::Hook => direction == Direction::Short,
            },
            emulator: flag("EmulatorMode"),
            anchor,
            white: list("CoinsWhiteList"),
            black: list("CoinsBlackList"),
            market_tags: MarketTags::parse(&text(MARKET_TAGS)),
            max_active: num("MaxActiveOrders") as i64,
            max_markets: num("MaxMarkets") as i64,
            max_ping: num("MaxPing") as i64,
            ping_cooldown_min: num("PingCooldown"),
            penalty: num("TradePenaltyTime"),
            deltas: DeltaFilters::read(&|n| num(n), &|n| text(n), &|n| flag(n)),
            working_time: WorkWindow::parse(&text("WorkingTime")),
            prevent_until: {
                // Unix seconds; a value in milliseconds (13 digits) is read as such.
                let v = seconds(num("PreventWorkingUntil"));
                if v > 1e11 {
                    v / 1000.0
                } else {
                    v
                }
            },
            penalty_time: seconds(num("PenaltyTime")),
            global_detect_penalty: seconds(num("GlobalDetectPenalty")),
            total_loss: num("TotalLoss").abs(),
            session: (!flag("IgnoreSession")).then(|| {
                // `SessionLevelsUSDT` off: the levels are % of `OrderSize`.
                let scale = if flag("SessionLevelsUSDT") {
                    1.0
                } else {
                    num("OrderSize") / 100.0
                };
                guards::SessionRule {
                    min: -(num("SessionStratMin") * scale).abs(),
                    max: (num("SessionStratMax") * scale).abs(),
                    penalty_ms: secs_ms(num("SessionPenaltyTime").max(0.0)),
                    reset_on_minus: flag("SessionResetOnMinus"),
                }
            }),
            dyn_refresh: num("Dyn_Refresh"),
            dyn_wl: DynList::new(
                &text("DynWL_SortBy"),
                flag("DynWL_SortDesc"),
                num("DynWL_Count") as i64,
            ),
            dyn_bl: DynList::new(
                &text("DynBL_SortBy"),
                flag("DynBL_SortDesc"),
                num("DynBL_Count") as i64,
            ),
            order_size: num("OrderSize"),
            auto_cancel: num("AutoCancelBuy"),
            cancel_after_sell: flag("CancelBuyAfterSell"),
            max_per_market: num("MaxOrdersPerMarket").max(0.0) as i64,
            max_position: num("MaxPosition").abs(),
            check_free_balance: flag("CheckFreeBalance"),
            min_free_balance: num("MinFreeBalance").max(0.0),
            price: num("MShotPrice"),
            price_min: num("MShotPriceMin"),
            add_15m: num("MShotAdd15minDelta"),
            add_1h: num("MShotAddHourlyDelta"),
            add_3h: num("MShotAdd3hDelta"),
            add_btc: num("MShotAddBTCDelta"),
            add_btc_5m: num("MShotAddBTC5mDelta"),
            add_distance: num("MShotAddDistance"),
            raise_wait: num("MShotRaiseWait"),
            replace_delay: num("MShotReplaceDelay"),
            orders_count: num("OrdersCount") as i64,
            step: num("BuyPriceStep"),
            size_step: num("OrderSizeStep"),
            expand: num("MShotExpand"),
            repeat: flag("MShotRepeatAfterBuy"),
            repeat_profit: num("MShotRepeatIfProfit"),
            repeat_wait: num("MShotRepeatWait"),
            repeat_delay: num("MShotRepeatDelay"),
            sell_price: num("SellPrice"),
            pd_timer: num("PriceDownTimer"),
            pd_delay: num("PriceDownDelay"),
            pd_pct: num("PriceDownPercent"),
            pd_relative: flag("PriceDownRelative"),
            pd_drop: num("PriceDownAllowedDrop"),
            stops: stops::Config::read(&num, &flag, &text),
            drops_max_time: num("DropsMaxTime"),
            drops_price_ma: num("DropsPriceMA"),
            drops_last_ma: num("DropsLastPriceMA") as i64,
            drops_delta: num("DropsPriceDelta"),
            drops_is_low: flag("DropsPriceIsLow"),
            drops_use_last: flag("DropsUseLastPrice"),
            buy_price: num("buyPrice"),
            detect_penalty: num("NextDetectPenalty"),
            strike_depth: num("MStrikeDepth"),
            strike_add_15m: num("MStrikeAdd15minDelta"),
            strike_add_1h: num("MStrikeAddHourlyDelta"),
            strike_add_market: num("MStrikeAddMarketDelta"),
            strike_volume: num("MStrikeVolume"),
            strike_delay_ms: num("MStrikeBuyDelay") as i64,
            strike_level: num("MStrikeBuyLevel"),
            strike_relative: flag("MStrikeBuyRelative"),
            strike_sell_level: num("MStrikeSellLevel"),
            strike_wait_dip: flag("MStrikeWaitDip"),
            hook_frame: num("HookTimeFrame"),
            hook_depth: num("HookDetectDepth"),
            hook_depth_max: num("HookDetectDepthMax"),
            hook_anti_pump: flag("HookAntiPump"),
            hook_min_volume: num("HookDetectMinVolume"),
            hook_rollback: num("HookPriceRollBack"),
            hook_rollback_max: num("HookPriceRollBackMax"),
            hook_rollback_wait: num("HookRollBackWait") as i64,
            hook_drop_min: num("HookDropMin"),
            hook_drop_max: num("HookDropMax"),
            hook_initial: num("HookInitialPrice"),
            hook_distance: num("HookPriceDistance"),
            hook_replace_delay: num("HookReplaceDelay"),
            hook_raise_wait: num("HookRaiseWait"),
            hook_sell_level: num("HookSellLevel"),
            hook_sell_fixed: flag("HookSellFixed"),
            hook_repeat: flag("HookRepeatAfterSell"),
            hook_repeat_profit: num("HookRepeatIfProfit"),
            direction,
        }
    }

    /// Whether the strategy trades `short`'s side.
    fn allows(&self, short: bool) -> bool {
        match (self.kind, self.direction) {
            (Kind::Strike | Kind::Hook, Direction::Both) => true,
            (Kind::Strike | Kind::Hook, Direction::Long) => !short,
            (Kind::Strike | Kind::Hook, Direction::Short) => short,
            _ => short == self.short,
        }
    }

    /// Whether it may enter `short`'s side on `m` at `now`: its own
    /// direction, and a short only where the exchange allows one.
    fn trades(&self, m: &Market, short: bool, now: i64) -> bool {
        self.allows(short) && (!short || m.shortable(now))
    }

    /// USDT size of a new entry of tier `i` at `price`: its budget, or one lot
    /// when that is dearer but within `LOT_OVER_BUDGET`; none past that.
    fn entry_size(&self, i: usize, m: &Market, price: f64) -> Option<f64> {
        let (budget, lot) = (self.tier_size(i), smallest_order(m, price));
        // A market whose step did not arrive has no lot to size by: no entry, said by the caller.
        if lot.is_nan() || lot <= 0.0 {
            return None;
        }
        if lot <= budget {
            Some(budget)
        } else if lot <= budget * LOT_OVER_BUDGET {
            Some(lot)
        } else {
            None
        }
    }

    /// USDT budget of ladder tier `i`: `OrderSize` grown by `OrderSizeStep` % a tier.
    fn tier_size(&self, i: usize) -> f64 {
        // A step that shrinks the tiers can take the size to zero: that tier has no budget
        // (`entry_size` gives no order for it), and it is not the biggest one.
        (self.order_size * (1.0 + i as f64 * self.size_step / 100.0)).max(0.0)
    }

    /// The sides it enters: its own direction.
    fn sides(&self) -> impl Iterator<Item = bool> + '_ {
        [false, true]
            .into_iter()
            .filter(|&short| self.allows(short))
    }
}

pub(crate) fn field(
    fields: &StrategyFields,
    schema: &StrategySchema,
    name: &str,
) -> Option<FieldValue> {
    fields
        .get(name)
        .cloned()
        .or_else(|| schema.field(name).and_then(|f| f.default_value.clone()))
}

/// Per-order runtime notes (dropped with the order).
#[derive(Default)]
struct Memo {
    /// Stable ladder slot; a pending cancellation still occupies it.
    entry_tier: Option<usize>,
    /// Since when the entry has been outside its corridor (0 = inside), on
    /// which side, and the extreme of `last` seen meanwhile — the reference
    /// the entry is re-placed from (MoonBot's «Min. Ask»).
    wait_since: i64,
    wait_near: bool,
    wait_ref: f64,
    /// A move refused for being over the lot budget has been said (once, not every pass).
    lot_said: bool,
    /// The position's stops.
    stops: stops::State,
    cancel_at: i64,
    fails: u32,
    fail_at: i64,
    detected: bool,
    /// Entry logged as taken off by a `FilterCheck` (`Gate` or an opposite
    /// position on its market).
    filtered: bool,
    /// Repeat after buy granted by this position: when the new entry may go
    /// out (0 = not granted) and whether it did.
    repeat_at: i64,
    repeat_used: bool,
}

#[derive(Default)]
struct StratRt {
    universe: Vec<u16>,
    universe_at: i64,
    universe_rev: u64,
    /// The screener problem last logged (`None` = none in force): logged when
    /// it appears or changes, not on every recompute.
    screen_problem: Option<String>,
    /// The same, for list entries that name no market
    /// (`screener::unknown_symbols`). Its own memo, because it is a warning
    /// beside a working pool and must not be silenced by a problem, nor
    /// silence one.
    screen_unknown: Option<String>,
    /// Of the pool, how many markets each filter was the first to refuse when
    /// the pool was last recomputed — the check's own name and its count, in
    /// the order the checks are asked. A pool of 100 that trades 12 is a
    /// mystery without it, now that the filters no longer shrink the pool.
    filtered: Vec<(&'static str, usize)>,
    /// The picture last said about what the filters hold back (`None` = they
    /// hold nothing back, the quiet state, which says nothing): which checks
    /// hold something, and how much of the pool they hold between them, in
    /// tenths.
    ///
    /// The pool's own size is in it as well: it is pinned to `DynWL_Count`
    /// and does not wobble, so it costs no chatter, and without it a pool
    /// that grew from 40 to 300 under an unchanged filter picture would leave
    /// the journal's last word saying 40.
    ///
    /// Deliberately not the counts themselves. Those move on every recompute
    /// — 28, 28, 29 — and a line per strategy per minute would bury a journal
    /// kept for three days under numbers nobody reads; the page carries them
    /// live, and this says when the picture actually changed.
    pool_said: Option<(Vec<&'static str>, usize, usize)>,
    /// A pool past `screener::POOL_WIDE` has been reported.
    wide_said: bool,
    ping_halt_until: i64,
}

/// What one pass measures a market against; the screener reads it too.
pub struct Ctx<'a> {
    pub(crate) model: &'a Model,
    pub(crate) win: &'a Windows,
    pub(crate) now: i64,
    /// `BTC_SYMBOL` market, when in the catalog.
    pub(crate) btc: Option<u16>,
    /// The traded markets' mean hourly delta, % ([`market_delta`]); `None`
    /// while too few markets have an hour of history.
    pub(crate) market_delta: Option<f64>,
}

impl<'a> Ctx<'a> {
    /// What a pass measures against, for a caller outside the pass — the
    /// page's helper asks the screener and the filters the same questions the
    /// pass asks, and it must ask them in the same context, BTC and the
    /// market included: a `Delta_BTC` judged against a missing market is a
    /// different answer from the one the strategy gets.
    pub fn new(model: &'a Model, win: &'a Windows, now: i64, market_delta: Option<f64>) -> Self {
        Self {
            model,
            win,
            now,
            btc: model.index_of_symbol(BTC_SYMBOL),
            market_delta,
        }
    }
}

/// Markets [`market_delta`] needs before its mean is a market's and not a
/// handful of names'.
const MARKET_DELTA_MIN: usize = 20;

/// The mean hourly delta of the `TRADING` markets with an hour of history,
/// % — MoonBot's «market» delta (`Delta_Market_*`, the auto-start tab's
/// market panic). `None` below [`MARKET_DELTA_MIN`] markets: after a start
/// the windows fill from the warm-up and the tape, and a mean over the first
/// few would be one coin's move called the market's.
pub fn market_delta(model: &Model, win: &Windows, now: i64) -> Option<f64> {
    let (mut sum, mut n) = (0.0, 0usize);
    for (idx, m) in model.iter().filter(|(_, m)| m.live()) {
        if !win.covers(idx, now, 60) {
            continue;
        }
        if let Some(d) = win.delta(idx, m.last(), now, 60).filter(|d| d.is_finite()) {
            sum += d;
            n += 1;
        }
    }
    (n >= MARKET_DELTA_MIN).then(|| sum / n as f64)
}

#[derive(Default)]
pub struct MoonShot {
    memo: HashMap<u64, Memo>,
    rt: HashMap<u64, StratRt>,
    /// The warm-up is still filling `Windows` (`Engine::set_warming`).
    ///
    /// Every volume key reads an honest zero from an empty window, so a
    /// `DailyVol` ranking taken now ties the whole class at zero and keeps
    /// whichever markets the catalog happened to list first — a pool by
    /// accident, subscribed and sampled, and swapped out wholesale one
    /// recompute later. The black list ranks the same way and bans on the
    /// same tie, so EITHER list makes a pool a ranked one. A pool with no
    /// ranking at all does not wait, since nothing about it is measured.
    /// `false` by default, which is what a core with no feed and every test
    /// means.
    warming: bool,
    rate_halt_until: i64,
    /// `(at, rtt ms)` of recent API calls.
    pings: VecDeque<(i64, i64)>,
    /// Close time of the last losing trade per strategy × market:
    /// `TradePenaltyTime` keeps that strategy off the market for a while.
    /// A manual order's loss penalizes no strategy, and one strategy's loss
    /// neither holds nor takes off another's entries.
    loss_at: HashMap<(u64, u16), i64>,
    /// Refused orders: when, and how long the refusal keeps the strategy off
    /// the market (`reject_cooldown_ms`). Kept no longer than the longest.
    refusals: HashMap<u64, (i64, i64)>,
    /// `CheckFreeBalance` / `MinFreeBalance` budget.
    funds: Funds,
    /// The auto-start tab's work window (`set_work_window`).
    work_window: Option<WorkWindow>,
    /// `GlobalFilterPenalty`: until when a strategy stays off after its
    /// index filter failed.
    filter_penalty: HashMap<u64, i64>,
    /// Cost of the positions per market × emulator mode (`MaxPosition`),
    /// rebuilt each pass.
    position_cost: HashMap<(u16, bool), f64>,
    /// The terminal's global black list (`set_black_list`): no strategy
    /// trades these markets.
    black_list: HashSet<String>,
    /// `PenaltyTime` from the report (`set_guards`): when a third loss in a
    /// row closed per strategy × market, the last manual deal per market ×
    /// emulator mode; and the live manual orders' placement per market ×
    /// mode (a pass rebuilds it).
    streaks: HashMap<(u64, String), i64>,
    manual_deals: HashMap<(String, bool), i64>,
    manual_at: HashMap<(u16, bool), i64>,
    /// `GlobalDetectPenalty`: last detect or new ladder per market and whose.
    market_detect: HashMap<u16, (i64, u64)>,
    /// Loss guards from the report (`set_guards`).
    totals: HashMap<u64, f64>,
    sessions: HashMap<(u64, String), i64>,
    /// Until when restored entries wait for their market's first trade.
    price_wait_until: i64,
    /// The account's user-data stream is not open (from the start until its
    /// first session, and between sessions): a fill would go unseen, so no
    /// entries anywhere but the emulator's.
    fills_unseen: bool,
    /// Markets the exchange takes no new positions on (`set_closed_markets`).
    closed: HashSet<String>,
    /// The core-wide emulator mode (`emu_mode`): every strategy trades in
    /// the emulator, as one with `EmulatorMode` does.
    emulator: bool,
    /// Price samples of the DropsDetection universes.
    tapes: Tapes,
    /// Last DropsDetection / MoonStrike signal per (strategy, market):
    /// `NextDetectPenalty`.
    detect_at: HashMap<(u64, u16), i64>,
    /// (strategy, market uid) whose ladder entry was skipped for a lot far
    /// over the budget: logged once, until an entry there starts again.
    over_lot: HashSet<(u64, String)>,
    /// (strategy, market uid) whose ladder entry was skipped past the
    /// exchange bound of its side: logged once, until an entry there starts again.
    past_band: HashSet<(u64, String)>,
    /// EMAs and strikes of the MoonStrike universes.
    tracks: Tracks,
    /// MoonStrike signals waiting for `MStrikeBuyDelay` / `MStrikeWaitDip`.
    signals: HashMap<(u64, u16), Signal>,
    /// Trade tapes of the MoonHook universes.
    hook_tape: hook::Tracks,
    /// The detect a MoonHook's live entries were placed from.
    hooks: HashMap<(u64, u16), Hook>,
    /// Buy and sell volume of the markets a BV/SV stop watches.
    bv: bvsv::Tapes,
    /// Markets a `FastStopLoss` strategy trades, and their lowest and highest
    /// trade since the last pass.
    fast_markets: HashSet<u16>,
    swings: HashMap<u16, (f64, f64)>,
}

/// The band `manage_entries` keeps an entry in: its side, tier 0's distance
/// from the anchor and the half-width of its band (both %), the growth per
/// tier, the waits before a re-place and the kind name the log line carries.
#[derive(Debug, Clone, Copy)]
struct Corridor {
    short: bool,
    dist: f64,
    width: f64,
    expand: f64,
    /// Entries stay where they were put — MoonHook with an explicit
    /// `HookPriceDistance` 0, which the schema no longer defaults to
    /// (`strategies::schema_fields`) so that standing still is a choice the
    /// strategy file records rather than the silence of a missing line.
    follow: bool,
    /// The far bound keeps MoonShot's observed hysteresis (`far_distance`);
    /// MoonHook's band is the symmetric one its FAQ describes.
    far_capped: bool,
    replace_delay: f64,
    raise_wait: f64,
    kind: &'static str,
    /// MoonHook with `HookSellFixed` off: a moved entry re-plans its exit
    /// off the new price instead of keeping the ratio it was placed with.
    hook: Option<Hook>,
}

/// MoonHook state of one strategy × market: the detect its entries were
/// placed from. It outlives them, so every tier keeps the corridor its
/// detect sized, and goes once the strategy has nothing left on the market.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Hook {
    short: bool,
    at: i64,
    /// Where the move started (`hook::Detect.reference`): the exit is priced
    /// off it for as long as the detect lives.
    reference: f64,
    /// Tier 0's distance from the price at the detect and the corridor's
    /// half-width, both %.
    dist: f64,
    width: f64,
    /// `HookRepeatAfterSell` grants one more ladder; spent here.
    repeated: bool,
}

impl Hook {
    fn corridor(&self, p: &Params) -> Corridor {
        Corridor {
            short: self.short,
            dist: self.dist,
            width: self.width,
            expand: 0.0,
            follow: p.hook_distance > 0.0,
            far_capped: false,
            replace_delay: p.hook_replace_delay,
            raise_wait: p.hook_raise_wait,
            kind: "MoonHook",
            hook: (!p.hook_sell_fixed).then_some(*self),
        }
    }

    /// The exit planned for an entry at `entry`: `HookSellLevel` % of the way
    /// from it back to where the move started — 100 % is that start itself.
    /// At least a tick in profit.
    fn planned(&self, p: &Params, m: &Market, entry: f64) -> f64 {
        let tick = m.tick_size.max(f64::EPSILON);
        // `HookSellLevel` % of the way from the entry back to where the move
        // started, as a share of the entry; a short divides, as every MoonBot
        // exit does. Both `HookSellFixed` settings price it the same (logs
        // 20.09: `fixedNO ip0/ip30`, 61 identical detects, 0 shared targets —
        // so it is not an absolute level of the move); the flag only decides
        // whether a moved entry re-plans it (`Hook::corridor`).
        // Signed, not absolute: a corridor that chased the entry past the
        // start of the move has no way back left, so the exit sits a tick
        // off the entry and stays there instead of growing again.
        let back = if self.short {
            entry - self.reference
        } else {
            self.reference - entry
        };
        let pct = p.hook_sell_level * back.max(0.0) / entry;
        let sell = m.nearest(exit_price(self.short, entry, pct));
        if self.short {
            sell.min(entry - tick)
        } else {
            sell.max(entry + tick)
        }
    }
}

/// Why a strategy takes no entries on a market this pass; a live entry it
/// takes off for it is logged once (`MoonShot::filter_check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// The strategy is stopped or unchecked.
    Stopped,
    /// Entries are halted everywhere: fills unseen, the error budget, the
    /// ping.
    Halted,
    /// The market left the strategy's pool.
    Universe,
    /// `MaxMarkets` is taken by other markets.
    MarketSlots,
    /// The market is not in normal trading.
    NotTrading,
    /// No price seen yet.
    NoPrice,
    /// The entry's own side is no longer traded: the direction was edited,
    /// or the exchange does not short the market.
    Side,
    /// Another entry of the strategy on the market lost its side; the rest
    /// come off with it, so the edited strategy starts from nothing.
    OtherSide,
    /// A MoonShot short ladder on a market the exchange does not short.
    Unshortable,
    /// A buy on the market was refused at `at`.
    Refused { at: i64 },
    /// `TradePenaltyTime`: a trade on the market closed at a loss at `at`.
    Penalty { at: i64 },
    /// Filters / Delta that cannot be judged (`why`).
    Filter { why: &'static str },
    /// Filters / Delta: `what` % (in hundredths) is outside its range.
    Delta { what: &'static str, hundredths: i64 },
    /// Filters / Volume: `what` turnover of turnover is outside its range.
    Volume { what: &'static str, turnover: i64 },
    /// Filters / Base: the market's leverage is outside `MinLeverage..MaxLeverage`.
    Leverage { leverage: i32 },
    /// `GlobalFilterPenalty` after a failed BTC / market filter, until `until`.
    FilterPenalty { until: i64 },
    /// `PenaltyTime`: 3 losing trades in a row (or a manual order, `manual`)
    /// on the market at `at`.
    PenaltyTime { at: i64, manual: bool },
    /// Outside `WorkingTime` (or the auto-start work window), or a
    /// `WorkingTime` that is not understood (`why`).
    WorkingTime { why: &'static str },
    /// `PreventWorkingUntil` (Unix s).
    Prevented { until: i64 },
    /// `MaxPosition`: the market's position cost `cost` USDT.
    MaxPosition { cost: i64 },
    /// `TotalLoss`: the strategy's closed deals lost `loss` USDT.
    TotalLoss { loss: i64 },
    /// A minus session of the strategy on the market, until `until`.
    Session { until: i64 },
    /// `UseBV_SV_Stop`: the ratio would fire the stop on every side it enters.
    BvSv,
}

impl Gate {
    /// Whether a live entry comes off with this gate, or only new ones are
    /// held back.
    ///
    /// The Filters tab does NOT withdraw — a deliberate departure from
    /// MoonBot (28.09), which takes the entry off for any of them. A filter
    /// says "this market is not interesting right now", while an entry
    /// already on the book is a decision taken when it was interesting:
    /// a market whose hourly turnover dips under the floor for one pass would
    /// otherwise have its whole grid cancelled and re-placed on the next tick
    /// up. Everything else here is about whether the strategy may hold an
    /// order on this market at all — stopped, halted, out of the pool, not
    /// trading, holding the opposite position — and those do take it
    /// off. The cost of the departure is real and was accepted: an entry on a
    /// market that has gone thin stays until `AutoCancelBuy` and can fill
    /// into that thin book.
    fn withdraws(self) -> bool {
        !matches!(
            self,
            Self::Filter { .. }
                | Self::Delta { .. }
                | Self::Volume { .. }
                | Self::Leverage { .. }
                | Self::FilterPenalty { .. }
        )
    }

    fn reason(self, market: &str, now: i64) -> String {
        let ago = |at: i64| (now - at).max(0) / 1000;
        match self {
            Self::Stopped => "The strategy is stopped".into(),
            Self::Halted => "Entries are halted".into(),
            Self::Universe => format!("Market {market} doesnt match strategy markets list"),
            Self::MarketSlots => "MaxMarkets reached".into(),
            Self::NotTrading => format!("Market {market} is not in normal trading"),
            Self::NoPrice => format!("No price of {market} yet"),
            Self::Side => "The strategy no longer trades this side".into(),
            Self::OtherSide => "Another entry of the strategy no longer trades its side".into(),
            Self::Unshortable => format!("The exchange does not short {market}"),
            Self::Refused { at } => format!("A buy of {market} was refused {} sec. ago", ago(at)),
            Self::Penalty { at } => format!(
                "TradePenaltyTime: a trade on {market} closed at a loss {} sec. ago",
                ago(at)
            ),
            Self::BvSv => format!("BV/SV of {market} is under BV_SV_Ratio"),
            Self::PenaltyTime { at, manual } => format!(
                "PenaltyTime: {} on {market} {} sec. ago",
                if manual {
                    "a manual order"
                } else {
                    "3 losing trades in a row"
                },
                ago(at)
            ),
            Self::WorkingTime { why } => why.into(),
            Self::Filter { why } => why.into(),
            Self::Delta { what, hundredths } => {
                format!("{what} {:.2}% is out of range", hundredths as f64 / 100.0)
            }
            Self::Volume { what, turnover } => format!("{what} {turnover} USDT is out of range"),
            Self::Leverage { leverage } => {
                format!("Leverage {leverage}x of {market} is out of range")
            }
            Self::FilterPenalty { until } => format!(
                "GlobalFilterPenalty: {} sec. left",
                (until - now).max(0) / 1000
            ),
            Self::Prevented { until } => format!("PreventWorkingUntil {until}"),
            Self::MaxPosition { cost } => {
                format!("MaxPosition: the position on {market} cost {cost} USDT")
            }
            Self::TotalLoss { loss } => {
                format!("TotalLoss: the strategy lost {loss} USDT")
            }
            Self::Session { until } => format!(
                "Minus session on {market}: no entries for {} sec.",
                (until - now).max(0) / 1000
            ),
        }
    }
}

/// A detected strike whose entries have not gone out yet.
#[derive(Debug, Clone, Copy)]
struct Signal {
    short: bool,
    at: i64,
    /// Past it the signal is dropped: its prices are old (a halt, a gate or
    /// an unchecked strategy kept it from being served).
    expires: i64,
    reference: f64,
    /// The strike's extreme, still measured while the signal waits.
    extreme: f64,
}

impl MoonShot {
    /// Runtime notes for orders restored after a restart, from what they
    /// carry: an entry that filled was already announced; a placed exit may
    /// have granted its repeat already (`repeat_grant` runs on SellSet only),
    /// so it counts as spent; an exit placed for the bot stop keeps chasing
    /// (else PriceDown would move it back once the price recovered).
    pub fn restore(&mut self, orders: &Orders, now: i64) {
        self.price_wait_until = now + RESTORE_PRICE_WAIT_MS;
        for o in orders.iter().filter(|o| o.strategy_id != 0) {
            let memo = self.memo.entry(o.id).or_default();
            memo.detected = is_committed(o);
            memo.repeat_used = o.status == status::SELL_SET;
            if o.status == status::SELL_SET {
                if let Some(fired) = Fired::of_reason(o.exit_reason()) {
                    memo.stops = stops::State::restored(fired, now, o.bot_stop().0);
                }
            }
        }
    }

    pub fn set_fills_seen(&mut self, seen: bool) {
        self.fills_unseen = !seen;
    }

    pub fn set_emulator(&mut self, on: bool) {
        self.emulator = on;
        self.funds.emulator = on;
    }

    /// Markets the exchange refused new positions on (`-4140`/`-4141`), as the engine learned
    /// them: a real entry is not started there again, instead of failing once a minute.
    pub fn set_closed_markets(&mut self, closed: &HashSet<String>) {
        self.closed.clone_from(closed);
    }

    /// A detect of the strategy on the market (`NextDetectPenalty`), seen by
    /// the others as well (`GlobalDetectPenalty`).
    fn mark_detect(&mut self, key: (u64, u16), now: i64) {
        self.detect_at.insert(key, now);
        self.market_detect.insert(key.1, (now, key.0));
    }

    /// Another strategy detected on the market within `GlobalDetectPenalty`.
    fn detected_by_other(&self, strategy_id: u64, idx: u16, p: &Params, now: i64) -> bool {
        p.global_detect_penalty > 0.0
            && self.market_detect.get(&idx).is_some_and(|&(at, by)| {
                by != strategy_id && now - at < secs_ms(p.global_detect_penalty)
            })
    }

    /// Free USDT on the account (`CheckFreeBalance`, `MinFreeBalance`);
    /// `None` until the account is read.
    ///
    /// `None` after a figure was known means the account went unreadable (the reader withdraws
    /// its snapshot after a minute of refusals): the last figure stays for the sums, and the
    /// entries that check the balance wait for a fresh one instead of running unchecked.
    pub fn set_free_balance(&mut self, usdt: Option<f64>) {
        match usdt {
            Some(_) => {
                self.funds.budget = usdt;
                self.funds.lost = false;
            }
            None => self.funds.lost = self.funds.budget.is_some(),
        }
    }

    /// The Filters tab on the market: the first check it fails, and
    /// `GlobalFilterPenalty` armed when that check was the index one.
    ///
    /// This gate does not withdraw a live entry (`Gate::withdraws`) — it
    /// holds back new ones.
    fn delta_gate(
        &mut self,
        strategy_id: u64,
        p: &Params,
        idx: u16,
        cx: &Ctx,
        now: i64,
    ) -> Option<Gate> {
        let f = p.deltas.as_ref()?;
        if let Some(&until) = self.filter_penalty.get(&strategy_id).filter(|&&u| u > now) {
            return Some(Gate::FilterPenalty { until });
        }
        let (gate, _, index) = f.refused(idx, cx)?;
        if index && f.penalty_ms > 0 {
            self.filter_penalty.insert(strategy_id, now + f.penalty_ms);
        }
        Some(gate)
    }

    /// The auto-start tab's work window (`None` = always).
    pub fn set_work_window(&mut self, window: Option<WorkWindow>) {
        self.work_window = window;
    }

    /// Whether the warm-up is still filling `Windows` (see the field).
    pub fn set_warming(&mut self, on: bool) {
        self.warming = on;
    }

    /// The strategy's parameters with the terminal's global black list folded
    /// into its own, which is what the screener is actually run with. The
    /// global list joins the strategy's before the ranking, so a dynamic list
    /// backfills past a listed market; a listed ticker also takes its
    /// class-suffixed twin (`SBER_TQBR`).
    ///
    /// `Cow`: the common case is an empty global list, and the pass must not
    /// clone a `Params` per strategy to discover that.
    fn blacked<'a>(black: &HashSet<String>, p: &'a Params, model: &Model) -> Cow<'a, Params> {
        if black.is_empty() {
            return Cow::Borrowed(p);
        }
        // A catalog collision names the second market `T_CLASS` after the
        // first took `T`: only a listed `T` that names a market (a symbol or
        // a coin) takes its twins.
        let twins: Vec<String> = black
            .iter()
            .filter(|t| model.index_of_symbol_ci(t).is_some())
            .map(|t| format!("{}_", t.to_uppercase()))
            .collect();
        let mut q = p.clone();
        q.black.extend(
            model
                .iter()
                .map(|(_, m)| &m.symbol)
                .filter(|sym| {
                    let up = sym.to_uppercase();
                    black.contains(&up) || twins.iter().any(|t| up.starts_with(t))
                })
                .cloned(),
        );
        // A coin the operator typed (`btc`) bans its market the way the
        // strategy's own black list does (`Catalog::index_of_symbol_ci`).
        q.black.extend(
            black
                .iter()
                .filter_map(|t| model.index_of_symbol_ci(t))
                .filter_map(|i| model.at(i))
                .map(|m| m.symbol.clone()),
        );
        Cow::Owned(q)
    }

    /// Every market of the catalog with the seat this strategy's screener
    /// gives it, and the pool it is measured against — the page's strategy
    /// helper (`control::Screen`).
    ///
    /// Asked with the strategy's own live pool as `prev`, so the boundary's
    /// hold (`screener::KEEP_BAND`) is the one the strategy is running under
    /// and not an idealised re-sort. The global black list is folded in the
    /// same way the pass folds it, so a market the terminal banned shows as
    /// banned here too instead of as a market the strategy would watch.
    pub fn screen(&self, p: &Params, cx: &Ctx, prev: &HashSet<u16>) -> Vec<screener::Seen> {
        screener::explain(&Self::blacked(&self.black_list, p, cx.model), cx, prev)
    }

    /// The markets a strategy watched at its last recompute, for the helper's
    /// `prev`. Empty for a strategy that never ran.
    pub fn pool_set(&self, id: u64) -> HashSet<u16> {
        self.rt
            .get(&id)
            .map_or_else(HashSet::new, |rt| rt.universe.iter().copied().collect())
    }

    /// The global black list; a change re-screens every universe at once.
    pub fn set_black_list(&mut self, symbols: HashSet<String>) {
        if symbols != self.black_list {
            self.black_list = symbols;
            for rt in self.rt.values_mut() {
                rt.universe_at = 0;
            }
        }
    }

    /// The loss guards' state for the next passes: realized profit per
    /// strategy over the `TotalLoss` window, the minus sessions and the
    /// `PenaltyTime` marks.
    pub fn set_guards(&mut self, g: guards::View) {
        self.totals = g.totals;
        self.sessions = g.sessions;
        self.streaks = g.streaks;
        self.manual_deals = g.manual;
    }

    /// Bind a new core order before any exchange report can trigger a pass.
    pub fn on_entry_started(&mut self, order: u64, tier: usize) {
        self.memo.entry(order).or_default().entry_tier = Some(tier);
    }

    /// One pass over every strategy: exits and stops are managed for all
    /// strategy orders; entries only for running, checked strategies.
    pub fn tick(
        &mut self,
        st: &Strategies,
        orders: &Orders,
        model: &Model,
        win: &Windows,
        market_delta: Option<f64>,
        now: i64,
    ) -> Vec<Cmd> {
        let mut cmds = Vec::new();
        let cx = Ctx::new(model, win, now, market_delta);
        let mut by_strat: HashMap<u64, HashMap<u16, Vec<&CoreOrder>>> = HashMap::new();
        let mut live = HashSet::new();
        // Open positions per market and mode by direction (any order, manual
        // too): a strategy does not enter against one. The account nets only
        // real positions; the emulator's never meet them.
        let mut positions: HashMap<(u16, bool), (bool, bool)> = HashMap::new();
        self.manual_at.clear();
        self.position_cost.clear();
        for o in orders.iter() {
            if !status::is_terminal(o.status) {
                live.insert(o.id);
            }
            let Some(idx) = model.index_of_symbol(&o.uid) else {
                continue;
            };
            if o.strategy_id != 0 && o.status == status::SELL_DONE && is_loss(o) {
                let at = self.loss_at.entry((o.strategy_id, idx)).or_default();
                *at = (*at).max(o.record().sell.close_ms);
            }
            // A manual order the exchange took (not refused) holds the market.
            // A pending one has not reached it yet: its cooldown starts when
            // the trigger crosses and the entry actually goes out.
            let hand = o.strategy_id == 0 || o.hand;
            if hand && o.status != status::BUY_FAIL && !o.is_pending() {
                let at = self.manual_at.entry((idx, o.emulator)).or_default();
                *at = (*at).max(o.record().buy.create_ms);
            }
            if is_committed(o) {
                let (long, short) = positions.entry((idx, o.emulator)).or_default();
                *if o.is_short { short } else { long } = true;
                *self.position_cost.entry((idx, o.emulator)).or_default() += o.record().buy.spent;
            }
            if o.strategy_id != 0 {
                by_strat
                    .entry(o.strategy_id)
                    .or_default()
                    .entry(idx)
                    .or_default()
                    .push(o);
            }
        }
        // Strategy x market pairs with a LIVE order on them: a MoonHook
        // detect must not be collected under its own entries. Terminal orders
        // linger in `Orders` for a while and must not keep it — a detect that
        // outlived its tape would re-ladder at prices taken before the halt.
        let engaged: HashSet<(u64, u16)> = by_strat
            .iter()
            .flat_map(|(&id, markets)| {
                markets.iter().filter_map(move |(&idx, os)| {
                    os.iter()
                        .any(|o| !status::is_terminal(o.status))
                        .then_some((id, idx))
                })
            })
            .collect();
        self.memo.retain(|id, _| live.contains(id));
        self.refusals
            .retain(|_, &mut (at, _)| now - at < REJECT_COOLDOWN_MS);
        self.rt
            .retain(|id, _| st.list().iter().any(|s| s.strategy_id == *id));
        let ping = self.ping_median(now);
        let running = st.running();
        let warming = self.warming;
        let mut sampled = HashSet::new();
        let mut tracked = HashSet::new();
        let mut hooked = HashSet::new();
        let mut bv_watched = HashSet::new();
        let mut fast = HashSet::new();
        for s in st.list() {
            let p = Params::from_snapshot(s, st.schema());
            let mine = by_strat.remove(&s.strategy_id).unwrap_or_default();
            let active = running && s.checked;
            // The emulator's orders never reach the exchange: its error budget,
            // ping and order stream hold only real entries.
            let emulated = p.emulator || self.emulator;
            // Read before `rt` borrows `self.rt`: the counting sweep below
            // reports the penalty as the reason when it is the live one.
            let penalized = self
                .filter_penalty
                .get(&s.strategy_id)
                .is_some_and(|&until| until > now);
            let rt = self.rt.entry(s.strategy_id).or_default();
            // `Dyn_Refresh` on a grid shared by every strategy of the same
            // period, not a stopwatch per strategy: N strategies then move the
            // subscription sets once, in one pass, instead of at N moments the
            // coordinator's 300 ms debounce cannot merge.
            let period = ((p.dyn_refresh * 1000.0) as i64)
                .clamp(screener::REFRESH_MIN_MS, screener::REFRESH_MAX_MS);
            // A Manual strategy watches no pool: only its hand trades, and
            // what its filters used to hold back goes with the pool — a
            // breakdown against no markets is a page saying nothing true.
            if p.kind == Kind::Manual {
                rt.universe.clear();
                rt.filtered.clear();
                rt.pool_said = None;
                rt.wide_said = false;
            } else if active
                && !(warming && (p.dyn_wl.on() || p.dyn_bl.on()))
                && (now / period != rt.universe_at / period || rt.universe_rev != s.last_date)
            {
                // A `WorkingTime` not understood is told once, like a bad
                // screener: the strategy trades nothing meanwhile.
                let problem = screener::problem(&p)
                    .or_else(|| p.working_time.clone().err())
                    .or_else(|| p.deltas.as_ref().and_then(|d| d.problem).map(String::from));
                if problem != rt.screen_problem {
                    if let Some(text) = &problem {
                        cmds.push(Cmd::Log(format!("{}: {text}", label(s))));
                    }
                    rt.screen_problem = problem;
                }
                // Not a problem: the rest of the list is still a pool. Said
                // on the strategy's own params, before the global black list
                // joins them below.
                let unknown = screener::unknown_symbols(&p, model);
                if unknown != rt.screen_unknown {
                    if let Some(text) = &unknown {
                        cmds.push(Cmd::Log(format!("{}: {text}", label(s))));
                    }
                    rt.screen_unknown = unknown;
                }
                // The global list joins the strategy's own before the ranking,
                // so a dynamic list backfills past a listed market. A listed
                // ticker also takes its class-suffixed twin (`SBER_TQBR`).
                let prev: HashSet<u16> = rt.universe.iter().copied().collect();
                rt.universe =
                    screener::universe(&Self::blacked(&self.black_list, &p, model), &cx, &prev);
                rt.universe_at = now;
                rt.universe_rev = s.last_date;
                // What the filters would refuse of the pool right now. The
                // pass asks each market this anyway (`delta_gate`); asking
                // once more here, on the recompute grid, is what turns "pool
                // 100" into "pool 100, 88 of them filtered and why".
                rt.filtered = filter_counts(&p, &rt.universe, &cx, penalized);
                let refused: usize = rt.filtered.iter().map(|&(_, n)| n).sum();
                let held: Vec<&'static str> = rt
                    .filtered
                    .iter()
                    .filter(|&&(_, n)| n > 0)
                    .map(|&(what, _)| what)
                    .collect();
                let shape = (refused > 0).then(|| {
                    (
                        held.clone(),
                        refused * 10 / rt.universe.len().max(1),
                        rt.universe.len(),
                    )
                });
                if shape != rt.pool_said {
                    // Back to nothing held back is news too, once.
                    let text = if refused > 0 {
                        let by: Vec<String> = rt
                            .filtered
                            .iter()
                            .filter(|&&(_, n)| n > 0)
                            .map(|&(what, n)| format!("{what} {n}"))
                            .collect();
                        format!(
                            "pool {}, {refused} filtered ({})",
                            rt.universe.len(),
                            by.join(", ")
                        )
                    } else {
                        format!("pool {}, nothing filtered", rt.universe.len())
                    };
                    cmds.push(Cmd::Log(format!("{}: {text}", label(s))));
                    rt.pool_said = shape;
                }
                // `DynWL_Count` is the only bound on a class pool now, and it
                // has no ceiling of its own: an operator who types 2000 gets
                // 2000 markets subscribed, sampled every pass and swept every
                // recompute. That is the operator's call, but it is not
                // allowed to be a silent one.
                let wide = rt.universe.len() > screener::POOL_WIDE;
                if wide != rt.wide_said {
                    if wide {
                        cmds.push(Cmd::Log(format!(
                            "{}: pool {} markets — DynWL_Count is its only bound, and every {} \
                             of them cost a market-data stream of their own",
                            label(s),
                            rt.universe.len(),
                            screener::POOL_WIDE
                        )));
                    }
                    rt.wide_said = wide;
                }
            }
            if active && p.kind == Kind::Drops {
                for &idx in &rt.universe {
                    match model.at(idx) {
                        Some(m) if m.live() && m.live_price() => {
                            self.tapes.sample(idx, now, m.last())
                        }
                        _ => self.tapes.reset(idx),
                    }
                    sampled.insert(idx);
                }
            }
            if active && p.kind == Kind::Hook {
                for &idx in &rt.universe {
                    match model.at(idx) {
                        Some(m) if m.live() => self.hook_tape.sample(idx, now),
                        _ => self.hook_tape.reset(idx),
                    }
                    hooked.insert(idx);
                }
            }
            if active && p.kind == Kind::Strike {
                for &idx in &rt.universe {
                    match model.at(idx) {
                        Some(m) if m.live() && m.live_price() => {
                            // A side the book does not give falls back to the
                            // last trade, as the anchor does: the long's
                            // reference is the bid, the short's the ask.
                            let bid = if m.bid_px() > 0.0 {
                                m.bid_px()
                            } else {
                                m.last()
                            };
                            let ask = if m.ask_px() > 0.0 {
                                m.ask_px()
                            } else {
                                m.last()
                            };
                            self.tracks.sample(idx, now, bid, ask);
                        }
                        _ => self.tracks.reset(idx),
                    }
                    tracked.insert(idx);
                }
            }
            let slow = |m: &i64| !emulated && p.max_ping > 0 && *m >= p.max_ping;
            if let Some(med) = ping.filter(slow) {
                if now >= rt.ping_halt_until {
                    rt.ping_halt_until = now + (p.ping_cooldown_min * 60_000.0) as i64;
                    cmds.push(Cmd::Log(format!(
                        "{}: API ping {med} ms >= {} ms, entries halted for {:.0} min",
                        label(s),
                        p.max_ping,
                        p.ping_cooldown_min
                    )));
                }
            }
            let entries_ok = active
                && (emulated
                    || (!self.fills_unseen
                        && now >= self.rate_halt_until
                        && now >= rt.ping_halt_until));
            // The universe can hold every share that passes the filters, so
            // the per-market gate asks a set, not a list.
            let watched: HashSet<u16> = if entries_ok {
                rt.universe.iter().copied().collect()
            } else {
                HashSet::new()
            };
            let mut markets: BTreeSet<u16> = watched.iter().copied().collect();
            markets.extend(mine.keys().copied());
            // A pending order (status `NONE`, waiting for its trigger) holds
            // its slot too: it is the strategy's, and the entry behind it goes
            // out without asking again.
            let alive = |o: &CoreOrder| {
                o.is_pending()
                    || matches!(
                        o.status,
                        status::BUY_SET | status::BUY_DONE | status::SELL_SET
                    )
            };
            // `MaxActiveOrders` counts the strategy's live orders (pending and
            // in position, MoonBot FAQ); 0 = no limit. Only new entries wait.
            let active_orders = mine.values().flatten().filter(|o| alive(o)).count() as i64;
            let mut slots = if p.max_active > 0 {
                (p.max_active - active_orders).max(0)
            } else {
                i64::MAX
            };
            // `MaxMarkets` counts the markets the strategy STANDS on (an entry
            // placed or a position open), not the ones it watches; 0 = no
            // limit. A market it already stands on keeps its slot, a new one
            // needs a free one — and a detect that finds none is dropped, not
            // queued (MoonBot: the slot opens for the next detect after it).
            let mut engaged_markets: HashSet<u16> = mine
                .iter()
                .filter(|(_, os)| os.iter().any(|o| alive(o)))
                .map(|(&idx, _)| idx)
                .collect();
            for idx in markets {
                let Some(m) = model.at(idx) else {
                    continue;
                };
                if let Some(b) = p.stops.bvsv {
                    self.bv.watch(idx, b.kind, b.n, now);
                    bv_watched.insert(idx);
                }
                if p.stops.fast {
                    fast.insert(idx);
                }
                let os = mine.get(&idx).map_or(&[][..], Vec::as_slice);
                let free_market_slot =
                    p.max_markets <= 0 || (engaged_markets.len() as i64) < p.max_markets;
                let gate = if !active {
                    Some(Gate::Stopped)
                } else if !entries_ok {
                    Some(Gate::Halted)
                } else if !watched.contains(&idx) {
                    Some(Gate::Universe)
                } else if !(free_market_slot || engaged_markets.contains(&idx)) {
                    Some(Gate::MarketSlots)
                } else if !m.live()
                    || (!(p.emulator || self.emulator) && self.closed.contains(&m.symbol))
                {
                    Some(Gate::NotTrading)
                } else if !(m.live_price() || m.book_fresh(now) || now < self.price_wait_until) {
                    Some(Gate::NoPrice)
                } else if self.bvsv_holds(&p, idx, now) {
                    Some(Gate::BvSv)
                } else {
                    None
                };
                let held = positions.get(&(idx, emulated)).copied().unwrap_or_default();
                let before = cmds.len();
                // `MaxOrdersPerMarket`: the strategy's live orders here cap
                // the new ones on top of `MaxActiveOrders`.
                let here = os.iter().filter(|o| alive(o)).count() as i64;
                let mut local = if p.max_per_market > 0 {
                    slots.min((p.max_per_market - here).max(0))
                } else {
                    slots
                };
                let granted = local;
                self.tick_market(
                    s, &p, idx, m, os, gate, m.trading, held, &mut local, &cx, &mut cmds,
                );
                slots -= granted - local;
                // An entry started on this market takes its slot right here:
                // `Orders` learns of it only after the pass, so the markets
                // behind it in the same pass would otherwise take it too.
                if cmds[before..]
                    .iter()
                    .any(|c| matches!(c, Cmd::Start { .. }))
                {
                    engaged_markets.insert(idx);
                }
            }
        }
        self.tapes.retain(|idx| sampled.contains(&idx));
        self.tracks.retain(|idx| tracked.contains(&idx));
        self.hook_tape.retain(|idx| hooked.contains(&idx));
        self.bv.retain(|idx| bv_watched.contains(&idx));
        self.fast_markets = fast;
        self.swings.clear();
        let listed = |id: u64| st.list().iter().any(|s| s.strategy_id == id);
        self.detect_at.retain(|&(id, _), _| listed(id));
        self.loss_at.retain(|&(id, _), _| listed(id));
        self.market_detect.retain(|_, &mut (_, by)| listed(by));
        self.filter_penalty.retain(|&id, _| listed(id));
        // A signal lives with its market's strike track (a market that left
        // the universe or normal trading drops it) and not past its expiry.
        let tracks = &self.tracks;
        self.signals
            .retain(|&(id, idx), sig| listed(id) && tracks.contains(idx) && now <= sig.expires);
        // A hook lives with its market's tape: one that left the universe or
        // normal trading no longer prices the corridor it sized.
        let tape = &self.hook_tape;
        self.hooks
            .retain(|&key, _| listed(key.0) && (tape.contains(key.1) || engaged.contains(&key)));
        // Orders of deleted strategies: entries come off — pending ones too,
        // or the trigger would open a position under a strategy that no
        // longer exists — positions stay manual (a deleted Manual strategy's
        // are the trader's own: they stay).
        let entries: Vec<&CoreOrder> = by_strat
            .values()
            .flat_map(|markets| markets.values().flatten().copied())
            .filter(|o| (o.status == status::BUY_SET || o.is_pending()) && !o.hand)
            .collect();
        // An unreadable strategy file leaves the list empty: that is not a deletion of every
        // strategy, and nothing is cancelled or adopted on its account.
        if !st.unreadable() {
            self.cancel_entries(&entries, now, &mut cmds);
        }
        // Their positions are nobody's but the core's watch: the stop the strategy had drawn
        // is made a real one, or it would stay a picture that never fires.
        for (&sid, markets) in &by_strat {
            // An unreadable strategy file leaves the list empty: that is not a deletion.
            if listed(sid) || st.unreadable() {
                continue;
            }
            for o in markets.values().flatten() {
                if o.holds_position() && !o.hand && o.bot_stop().0 > 0.0 {
                    cmds.push(Cmd::AdoptStop { order: o.id });
                }
            }
        }
        cmds
    }

    /// Markets whose order book the ladders need: the universe of every
    /// active strategy anchored at the bid or ask (entries are placed there
    /// before any core order subscribes the market).
    pub fn book_markets(&self, st: &Strategies) -> BTreeSet<u16> {
        let mut out = BTreeSet::new();
        if !st.running() {
            return out;
        }
        for s in st.list().iter().filter(|s| s.checked) {
            let p = Params::from_snapshot(s, st.schema());
            if p.anchor == Anchor::Trade {
                continue;
            }
            if let Some(rt) = self.rt.get(&s.strategy_id) {
                out.extend(rt.universe.iter().copied());
            }
        }
        out
    }

    /// Markets whose trading status gates the strategies: the universe of
    /// every active strategy (their live orders' markets are the caller's).
    pub fn status_markets(&self, st: &Strategies) -> BTreeSet<u16> {
        let mut out = BTreeSet::new();
        if !st.running() {
            return out;
        }
        for s in st
            .list()
            .iter()
            .filter(|s| s.checked && s.kind() != StrategyKind::MANUAL)
        {
            if let Some(rt) = self.rt.get(&s.strategy_id) {
                out.extend(rt.universe.iter().copied());
            }
        }
        out
    }

    /// Pool size of every checked strategy as its last recompute left it (the
    /// `load:` line): a stopped strategy keeps the pool it had, it no longer
    /// recomputes; one that never ran has none.
    pub fn pools(&self, st: &Strategies) -> Vec<(String, usize)> {
        st.list()
            .iter()
            .filter(|s| s.checked)
            .map(|s| {
                let n = self
                    .rt
                    .get(&s.strategy_id)
                    .map_or(0, |rt| rt.universe.len());
                (label(s), n)
            })
            .collect()
    }

    /// The pool of one strategy and what its filters refused of it at the
    /// last recompute, for the page: only the checks that refused anything,
    /// in the order they are asked. A strategy that never ran has neither.
    pub fn pool_filters(&self, id: u64) -> (usize, Vec<(&'static str, usize)>) {
        self.rt.get(&id).map_or((0, Vec::new()), |rt| {
            (
                rt.universe.len(),
                rt.filtered
                    .iter()
                    .copied()
                    .filter(|&(_, n)| n > 0)
                    .collect(),
            )
        })
    }

    /// An exchange trade (`turnover` of turnover, `buy` = the aggressor
    /// bought) for the MoonStrike and MoonHook detects and the stops; returns
    /// whether it extends a strike, so the strategies look at once. A hook
    /// is judged on the ordinary pass, not per trade.
    pub fn on_trade(&mut self, idx: u16, now: i64, price: f64, turnover: f64, buy: bool) -> bool {
        self.bv.trade(idx, now, turnover, buy);
        if self.fast_markets.contains(&idx) {
            let (lo, hi) = self.swings.entry(idx).or_insert((price, price));
            *lo = lo.min(price);
            *hi = hi.max(price);
        }
        self.hook_tape.trade(idx, now, price, turnover);
        self.tracks.trade(idx, now, price, turnover)
    }

    /// `UseBV_SV_Stop` also keeps entries off while the ratio would fire the
    /// stop on every side the strategy enters (MoonBot FAQ: else the
    /// position would be sold at once).
    fn bvsv_holds(&self, p: &Params, idx: u16, now: i64) -> bool {
        let Some(b) = p.stops.bvsv else {
            return false;
        };
        let Some(v) = self.bv.volumes(idx, b.kind, b.n, now) else {
            return false;
        };
        p.sides()
            .all(|short| b.ratio_of(short, v).is_some_and(|r| r < b.ratio))
    }

    /// Whether `short`'s side may be entered on `m`: its direction, the
    /// exchange, and the BV/SV ratio of that side (a two-sided strategy picks
    /// its side at the detect, past the per-market gate).
    fn enters(&self, p: &Params, m: &Market, idx: u16, short: bool, now: i64) -> bool {
        p.trades(m, short, now)
            && !p.stops.bvsv.is_some_and(|b| {
                self.bv
                    .volumes(idx, b.kind, b.n, now)
                    .and_then(|v| b.ratio_of(short, v))
                    .is_some_and(|r| r < b.ratio)
            })
    }

    /// What the stops of `p` read of market `idx` beyond its book.
    fn exit_feed(&self, p: &Params, idx: u16, m: &Market, cx: &Ctx) -> ExitFeed {
        let c = &p.stops;
        ExitFeed {
            delta_1m: if c.add_1m != 0.0 {
                cx.win.delta(idx, m.last(), cx.now, 1).unwrap_or(0.0)
            } else {
                0.0
            },
            swing: c.fast.then(|| self.swings.get(&idx).copied()).flatten(),
            volumes: c
                .bvsv
                .and_then(|b| self.bv.volumes(idx, b.kind, b.n, cx.now)),
        }
    }

    /// An exchange call for `order` failed: back the order off; a rate limit
    /// halts entries everywhere.
    pub fn on_failed(&mut self, order: u64, msg: &str, now: i64) {
        if let Some(halt) = rate_halt_ms(msg) {
            self.on_error_budget(now, halt);
        }
        let memo = self.memo.entry(order).or_default();
        memo.fails += 1;
        memo.fail_at = now;
        // Kept apart from the memo, which goes with the order once it is
        // final — and a refused entry is final at once.
        self.refusals.insert(order, (now, reject_cooldown_ms(msg)));
    }

    /// The exchange's rate limit: no entries anywhere for `halt_ms` ([`rate_halt_ms`]), so
    /// the calls waiting out the window are not joined by new ones.
    pub fn on_error_budget(&mut self, now: i64, halt_ms: i64) {
        self.rate_halt_until = self.rate_halt_until.max(now.saturating_add(halt_ms));
    }

    /// Round-trip time of one API call.
    pub fn on_ping(&mut self, ms: i64, now: i64) {
        self.pings.push_back((now, ms));
        while self
            .pings
            .front()
            .is_some_and(|&(at, _)| at < now - PING_WINDOW_MS)
        {
            self.pings.pop_front();
        }
    }

    pub fn ping_median(&self, now: i64) -> Option<i64> {
        let mut v: Vec<i64> = self
            .pings
            .iter()
            .filter(|&&(at, _)| at >= now - PING_WINDOW_MS)
            .map(|&(_, ms)| ms)
            .collect();
        if v.len() < PING_MIN_SAMPLES {
            return None;
        }
        v.sort_unstable();
        Some(v[v.len() / 2])
    }

    #[allow(clippy::too_many_arguments)]
    fn tick_market(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        idx: u16,
        m: &Market,
        os: &[&CoreOrder],
        gate: Option<Gate>,
        settled: bool,
        held: (bool, bool),
        slots: &mut i64,
        cx: &Ctx,
        cmds: &mut Vec<Cmd>,
    ) {
        let now = cx.now;
        let commits: Vec<&CoreOrder> = os.iter().copied().filter(|o| is_committed(o)).collect();
        for o in &commits {
            let rec = o.record();
            let memo = self.memo.entry(o.id).or_default();
            if !memo.detected {
                memo.detected = true;
                let d = m.price_precision as usize;
                cmds.push(Cmd::Log(format!(
                    "{}: Buy order DONE! FILL: {:.0}% Quantity: {} Avg.Price: {:.d$} (strategy <{}>)",
                    o.market,
                    if rec.buy.quantity > 0.0 {
                        rec.buy.filled / rec.buy.quantity * 100.0
                    } else {
                        100.0
                    },
                    rec.buy.filled,
                    rec.buy.mean_price,
                    label(s)
                )));
                cmds.push(Cmd::Detect {
                    market: o.market.clone(),
                    strategy_id: s.strategy_id,
                    is_short: o.is_short,
                    msg: format!(
                        "{} {} @ {:.d$}",
                        if o.is_short { "sold" } else { "bought" },
                        rec.buy.filled,
                        rec.buy.mean_price
                    ),
                });
            }
            if matches!(o.status, status::BUY_DONE | status::SELL_SET) {
                let feed = self.exit_feed(p, idx, m, cx);
                self.manage_exit(s, o, p, m, settled, feed, now, cmds);
            }
        }
        // A Manual strategy's entries are the trader's: no gate, no ladder.
        if p.kind == Kind::Manual {
            return;
        }
        let entries: Vec<&CoreOrder> = os
            .iter()
            .copied()
            .filter(|o| o.status == status::BUY_SET)
            .collect();
        // The strategy's own pending orders wait in the core and are on no
        // book: they are not entries to move or re-price. But a gate must
        // take them back — a stopped strategy must not fire one later — and
        // they hold the market for a new detect the way an entry does.
        let pending: Vec<&CoreOrder> = os.iter().copied().filter(|o| o.is_pending()).collect();
        // `CancelBuyAfterSell`: the grid's rest comes off once a sale of the
        // strategy here closed after it was placed.
        let sold_at = os
            .iter()
            .filter(|o| o.status == status::SELL_DONE)
            .map(|o| o.record().sell.close_ms)
            .max()
            .unwrap_or(0);
        let (stale, entries): (Vec<&CoreOrder>, Vec<&CoreOrder>) = entries
            .into_iter()
            .partition(|o| p.cancel_after_sell && o.record().buy.create_ms < sold_at);
        let first = stale
            .iter()
            .filter(|o| self.memo.get(&o.id).is_none_or(|memo| memo.cancel_at == 0))
            .count();
        if first > 0 {
            cmds.push(Cmd::Log(format!(
                "{}: CancelBuyAfterSell: {} entries of the grid cancelled after the sale (strategy <{}>)",
                m.symbol,
                first,
                label(s)
            )));
        }
        self.cancel_entries(&stale, now, cmds);
        // Replace cannot change exchange direction. Cancel the old side and
        // wait for its final report before starting the edited strategy; a
        // short entry on a market the exchange does not short comes off too.
        if entries.iter().any(|o| !p.trades(m, o.is_short, now)) {
            let (lost, rest): (Vec<&CoreOrder>, Vec<&CoreOrder>) =
                entries.iter().partition(|o| !p.trades(m, o.is_short, now));
            self.filter_check(s, &lost, Gate::Side, now, cmds);
            self.filter_check(s, &rest, Gate::OtherSide, now, cmds);
            self.cancel_entries(&entries, now, cmds);
            return;
        }
        // A two-sided strategy's entry whose own side BV/SV now holds comes
        // off; the other side's stays (a one-sided one is gated per market).
        let (bv_held, entries): (Vec<&CoreOrder>, Vec<&CoreOrder>) = entries
            .into_iter()
            .partition(|o| !self.enters(p, m, idx, o.is_short, now));
        if !bv_held.is_empty() {
            self.filter_check(s, &bv_held, Gate::BvSv, now, cmds);
            self.cancel_entries(&bv_held, now, cmds);
        }
        // An open position of the other side (any order, manual too) on the
        // market: a MoonStrike or MoonHook trading both sides judges each of
        // its entries.
        let against = |short: bool| if short { held.0 } else { held.1 };
        let opposite = if matches!(p.kind, Kind::Strike | Kind::Hook) {
            entries.iter().any(|o| against(o.is_short))
        } else {
            against(p.short)
        };
        let refused = os
            .iter()
            .filter(|o| o.status == status::BUY_FAIL)
            .filter(|o| {
                let cooldown = self
                    .refusals
                    .get(&o.id)
                    .map_or(REJECT_COOLDOWN_MS, |&(_, ms)| ms);
                now - o.record().buy.close_ms < cooldown
            })
            .map(|o| o.record().buy.close_ms)
            .max();
        let penalized = self
            .loss_at
            .get(&(s.strategy_id, idx))
            .copied()
            .filter(|&at| now - at < secs_ms(p.penalty));
        let within = |at: i64| at > 0 && now - at < secs_ms(p.penalty_time);
        let symbol = m.symbol.clone();
        let streak = self
            .streaks
            .get(&(s.strategy_id, symbol.clone()))
            .copied()
            .filter(|&at| within(at))
            .map(|at| Gate::PenaltyTime { at, manual: false });
        // Manual trades of the mode this strategy trades in.
        let emulated = p.emulator || self.emulator;
        let manual = self
            .manual_at
            .get(&(idx, emulated))
            .copied()
            .into_iter()
            .chain(self.manual_deals.get(&(symbol, emulated)).copied())
            .max()
            .filter(|&at| within(at))
            .map(|at| Gate::PenaltyTime { at, manual: true });
        let delta = self.delta_gate(s.strategy_id, p, idx, cx, now);
        let hours = match (&p.working_time, self.work_window) {
            (Err(_), _) => Some(Gate::WorkingTime {
                why: "WorkingTime is not understood",
            }),
            (Ok(Some(w)), _) if !w.contains(now) => Some(Gate::WorkingTime {
                why: "Outside WorkingTime",
            }),
            (_, Some(w)) if !w.contains(now) => Some(Gate::WorkingTime {
                why: "Outside the auto-start work time",
            }),
            _ => None,
        };
        let prevented = (p.prevent_until > 0.0 && (now / 1000) < p.prevent_until as i64).then_some(
            Gate::Prevented {
                until: p.prevent_until as i64,
            },
        );
        let cost = self
            .position_cost
            .get(&(idx, p.emulator || self.emulator))
            .copied()
            .unwrap_or(0.0);
        let over = (p.max_position > 0.0 && cost >= p.max_position).then_some(Gate::MaxPosition {
            cost: cost.round() as i64,
        });
        let total = self.totals.get(&s.strategy_id).copied().unwrap_or(0.0);
        let lost = (p.total_loss > 0.0 && total <= -p.total_loss).then_some(Gate::TotalLoss {
            loss: (-total).round() as i64,
        });
        let session = p
            .session
            .and_then(|_| self.sessions.get(&(s.strategy_id, m.symbol.clone())))
            .copied()
            .filter(|&until| until > now)
            .map(|until| Gate::Session { until });
        if opposite {
            for o in &entries {
                let memo = self.memo.entry(o.id).or_default();
                if !memo.filtered {
                    memo.filtered = true;
                    cmds.push(Cmd::Log(format!(
                        "{}: FilterCheck: market no longer meets the conditions. Market {} has \
                         opened {} position ! (strategy <{}>)",
                        o.market,
                        o.market,
                        if o.is_short { "Long" } else { "Short" },
                        label(s)
                    )));
                }
            }
        }
        // A MoonShot short ladder on a market the exchange does not short.
        let unshortable = p.kind == Kind::MoonShot && !p.trades(m, p.short, now);
        let gate = gate
            .or(refused.map(|at| Gate::Refused { at }))
            .or(penalized.map(|at| Gate::Penalty { at }))
            .or(hours)
            .or(prevented)
            .or(delta)
            .or(streak)
            .or(manual)
            .or(over)
            .or(lost)
            .or(session)
            .or(unshortable.then_some(Gate::Unshortable));
        // A gate of the Filters tab holds new entries back without taking the
        // live ones off (`Gate::withdraws`), and writes no `FilterCheck`
        // line, because that line says an entry was withdrawn. What it leaves
        // behind is a market the strategy manages exactly as before —
        // corridor, `AutoCancelBuy`, exits — with no slot to start anything
        // new, which is the state a market with `MaxActiveOrders` spent is
        // already in. An entry frozen instead would never expire.
        let mut spent = 0;
        let mut held_back = false;
        if opposite || gate.is_some() {
            if opposite || gate.is_none_or(Gate::withdraws) {
                // The opposite position said so above.
                if let Some(gate) = gate.filter(|_| !opposite) {
                    self.filter_check(s, &entries, gate, now, cmds);
                }
                self.cancel_entries(&entries, now, cmds);
                self.cancel_entries(&pending, now, cmds);
                // A MoonStrike signal does not outlast the gate.
                self.signals.remove(&(s.strategy_id, idx));
                return;
            }
            held_back = true;
        }
        // The caller reads back what this market spent, so the zeroed budget
        // must be a place of its own: writing 0 into the real one would
        // charge the strategy for entries it never started.
        let slots = if held_back { &mut spent } else { slots };
        let busy = !entries.is_empty() || !commits.is_empty() || !pending.is_empty();
        match p.kind {
            Kind::Drops => {
                self.drops_entries(s, p, idx, m, &entries, busy, slots, cx, cmds);
                return;
            }
            Kind::Strike => {
                self.strike_entries(s, p, idx, m, &entries, busy, held, slots, cx, cmds);
                return;
            }
            Kind::Hook => {
                self.hook_entries(s, p, idx, m, os, &entries, busy, held, slots, cx, cmds);
                return;
            }
            // Returned before the entries (above).
            Kind::Manual => return,
            Kind::MoonShot => {}
        }
        // MoonShot's corridor: tier 0 `MShotPrice` % away (shifted by the
        // delta windows) with a band down to `MShotPriceMin`, both growing
        // `MShotExpand` % a tier. `MShotPrice` 0 = no ladder, whatever the
        // shift would have added.
        let (eff, eff_min) = eff_params(p, idx, m.last(), cx);
        let corridor = Corridor {
            short: p.short,
            dist: if p.price > 0.0 { eff } else { 0.0 },
            width: (eff - eff_min).max(0.0),
            expand: p.expand,
            follow: true,
            far_capped: true,
            replace_delay: p.replace_delay,
            raise_wait: p.raise_wait,
            kind: "MoonShot",
            hook: None,
        };
        // No trade yet since the start: the ladder waits with what it has
        // (entries restored from the previous run stay put, see
        // `RESTORE_PRICE_WAIT_MS`, for as long as the book is fresh); the
        // startup ticker's price would move it on nothing new. Withdrawals
        // above and below need no price.
        let priced = m.live_price();
        if commits.is_empty() {
            if !priced {
                return;
            }
            // A new ladder is this strategy's detect on the market.
            let fresh = entries.is_empty();
            let allow = !(fresh && self.detected_by_other(s.strategy_id, idx, p, now));
            if self.manage_entries(s, p, m, &entries, corridor, allow, slots, now, cmds) && fresh {
                self.market_detect.insert(idx, (now, s.strategy_id));
            }
            return;
        }
        if !p.repeat {
            self.cancel_entries(&entries, now, cmds);
            return;
        }
        // Repeat after buy: the ladder the fill came from comes down (a
        // partial fill counts as filling now), entries placed after it follow
        // the corridor, and a fresh one may go out on a grant. Grants do not
        // take `MaxActiveOrders` slots (MoonBot FAQ).
        let fill_at = commits
            .iter()
            .map(|o| match o.record().buy.close_ms {
                0 => now,
                at => at,
            })
            .max()
            .unwrap_or(now);
        let (stale, fresh): (Vec<&CoreOrder>, Vec<&CoreOrder>) = entries
            .iter()
            .partition(|o| is_committed(o) || o.record().buy.create_ms < fill_at);
        self.cancel_entries(&stale, now, cmds);
        if !priced {
            return;
        }
        let grant = self.repeat_grant(p, &commits, m, now);
        // Grants do not take `MaxActiveOrders` slots, but they are still new
        // entries: a market the entry filters just refused gets none, or the
        // hold-back above would have one door left open — the one that opens
        // right after a fill, on exactly the market that has gone thin.
        // Nothing is spent, so the grant survives until the filter clears.
        let mut unlimited = if held_back { 0 } else { i64::MAX };
        if self.manage_entries(
            s,
            p,
            m,
            &fresh,
            corridor,
            grant.is_some(),
            &mut unlimited,
            now,
            cmds,
        ) {
            if let Some(memo) = grant.and_then(|id| self.memo.get_mut(&id)) {
                memo.repeat_used = true;
            }
        }
    }

    /// `MShotRepeatAfterBuy`: a position whose exit is placed grants one new
    /// entry when, within `MShotRepeatWait` s of its fill, the price is
    /// `MShotRepeatIfProfit` % past the mean entry; the entry may go out
    /// `MShotRepeatDelay` s later. Returns the granting order once due.
    fn repeat_grant(
        &mut self,
        p: &Params,
        commits: &[&CoreOrder],
        m: &Market,
        now: i64,
    ) -> Option<u64> {
        let mut due = None;
        for o in commits {
            let rec = o.record();
            let memo = self.memo.entry(o.id).or_default();
            if memo.repeat_used {
                continue;
            }
            if memo.repeat_at == 0 {
                let k = if o.is_short { -1.0 } else { 1.0 };
                let mean = rec.buy.mean_price;
                let profit = k * (m.last() - mean) >= mean * p.repeat_profit / 100.0;
                if o.status != status::SELL_SET
                    || now - rec.buy.close_ms > secs_ms(p.repeat_wait)
                    || m.last() <= 0.0
                    || !profit
                {
                    continue;
                }
                memo.repeat_at = now + secs_ms(p.repeat_delay);
            }
            if now >= memo.repeat_at && due.is_none() {
                due = Some(o.id);
            }
        }
        due
    }

    /// MoonBot's `FilterCheck`: an entry the strategy takes off because the
    /// market no longer passes `gate` says why, once.
    fn filter_check(
        &mut self,
        s: &StrategySnapshot,
        entries: &[&CoreOrder],
        gate: Gate,
        now: i64,
        cmds: &mut Vec<Cmd>,
    ) {
        for o in entries {
            // `cancel_entries` leaves an entry still being placed alone.
            if o.record().buy.exchange_id == 0 {
                continue;
            }
            let memo = self.memo.entry(o.id).or_default();
            if !memo.filtered {
                memo.filtered = true;
                cmds.push(Cmd::Log(format!(
                    "{}: FilterCheck: market no longer meets the conditions. {} (strategy <{}>)",
                    o.market,
                    gate.reason(&o.market, now),
                    label(s)
                )));
            }
        }
    }

    fn cancel_entries(&mut self, entries: &[&CoreOrder], now: i64, cmds: &mut Vec<Cmd>) {
        for o in entries {
            // A pending order has no exchange id because it never went out:
            // it is cancelled in the core, not on the book.
            if !o.is_pending() && o.record().buy.exchange_id == 0 {
                continue; // still being placed
            }
            let memo = self.memo.entry(o.id).or_default();
            if now - memo.cancel_at < CANCEL_RETRY_MS {
                continue;
            }
            memo.cancel_at = now;
            cmds.push(Cmd::Cancel { order: o.id });
        }
    }

    /// Ladder (MoonBot), shared by MoonShot and by the entries of a MoonHook
    /// detect (`Hook::corridor`): tier `i` is placed `dist_i` % from the
    /// anchor and left alone inside `[dist − width, dist + width]`. Outside it
    /// the entry is re-placed from the extreme of the anchor seen while waiting:
    /// at once (or after `c.replace_delay` — `MShotReplaceDelay` /
    /// `HookReplaceDelay`) when the price came too close («DOWN»), after
    /// `c.raise_wait` (`MShotRaiseWait` / `HookRaiseWait`) when it ran away
    /// («UP»). Entries older than `AutoCancelBuy` are cancelled; surplus
    /// tiers come off. A corridor that does not `follow` only ages its entries
    /// out. Missing tiers — a cancelled one included — are placed only with
    /// `allow_new`, which a MoonHook detect never asks for: its ladder goes
    /// out once. Returns whether any tier was placed.
    #[allow(clippy::too_many_arguments)]
    fn manage_entries(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        m: &Market,
        entries: &[&CoreOrder],
        c: Corridor,
        allow_new: bool,
        slots: &mut i64,
        now: i64,
        cmds: &mut Vec<Cmd>,
    ) -> bool {
        if c.dist <= 0.0 || p.order_size <= 0.0 {
            return false;
        }
        // `MShotUsePrice` picks the anchor; the delta windows keep the trade.
        let last = p.anchor.price(m, c.short);
        let n = p.orders_count.max(1) as usize;
        let dist = |i: usize| i as f64 * p.step.abs() + c.dist * factor(c.expand, i);
        let width = |i: usize| c.width.max(0.0) * factor(c.expand, i);
        // A tier past the exchange bound of its side (a BUY above `mark × up`, a
        // SELL below `mark × down`) prices nothing (0): it is not placed, and a
        // placed one is not moved past the limit.
        let raw = |i: usize, reference: f64| m.nearest(entry_price(c.short, reference, dist(i)));
        let target = |i: usize, reference: f64| m.inside_limits(raw(i, reference), !c.short);
        let tick = m.tick_size.max(f64::EPSILON);

        // Keep each order's original slot while other orders are cancelled,
        // replaced or pinned to the exchange bound. Rank only unbound entries
        // once (e.g. an entry started externally with a strategy id).
        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| {
            gap(c.short, last, a.record().buy.price)
                .total_cmp(&gap(c.short, last, b.record().buy.price))
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut occupied: BTreeSet<usize> = entries
            .iter()
            .filter_map(|o| self.memo.get(&o.id).and_then(|memo| memo.entry_tier))
            .collect();
        let mut tiers = Vec::with_capacity(sorted.len());
        for o in sorted {
            let memo = self.memo.entry(o.id).or_default();
            let tier = *memo.entry_tier.get_or_insert_with(|| {
                let tier = (0..).find(|i| !occupied.contains(i)).expect("vacant tier");
                occupied.insert(tier);
                tier
            });
            tiers.push((tier, o));
        }
        tiers.sort_by_key(|&(tier, _)| tier);
        let surplus: Vec<_> = tiers
            .iter()
            .filter(|(tier, _)| *tier >= n)
            .map(|&(_, o)| o)
            .collect();
        self.cancel_entries(&surplus, now, cmds);

        let mut started = false;
        for i in 0..n {
            if occupied.contains(&i) {
                continue;
            }
            if !allow_new || *slots <= 0 {
                break;
            }
            let key = (s.strategy_id, m.symbol.clone());
            let price = target(i, last);
            if price <= 0.0 {
                let raw = raw(i, last);
                if raw > 0.0 && self.past_band.insert(key) {
                    cmds.push(past_band_log(s, m, c.kind, c.short, i, raw));
                }
                continue;
            }
            // Sized at the price the order will actually rest at: on the tick grid, where the lot
            // count is taken (`Orders::target_entry` floors size / (rounded price × lot)).
            let rested = m.nearest(price);
            let Some(size) = p.entry_size(i, m, if rested > 0.0 { rested } else { price }) else {
                if self.over_lot.insert(key) {
                    cmds.push(over_lot_log(s, p, m, i, price));
                }
                continue;
            };
            self.over_lot.remove(&key);
            self.past_band.remove(&key);
            if !self.funds.take(s, p, m, size, cmds) {
                continue;
            }
            *slots -= 1;
            started = true;
            cmds.push(Cmd::Start {
                tier: i,
                order: StartOrder {
                    market: m.symbol.clone(),
                    is_short: c.short,
                    use_market_stop: false,
                    strategy_id: s.strategy_id,
                    size,
                    price,
                    planned_sell: 0.0,
                    stops: None,
                },
            });
            cmds.push(Cmd::Log(format!(
                "Starting new {} market: {} (strategy <{}>) tier {i}: price {price}; size {size}",
                c.kind,
                m.symbol,
                label(s)
            )));
        }

        for (i, o) in tiers.into_iter().filter(|(tier, _)| *tier < n) {
            if self.auto_cancel(s, p, o, now, cmds) {
                continue;
            }
            if !c.follow {
                continue;
            }
            let rec = o.record();
            let memo = self.memo.entry(o.id).or_default();
            // A move deferred in `Orders` is already on its way (see manage_exit).
            let Some(current) = o.heading(Leg::Buy) else {
                continue;
            };
            let g = gap(c.short, last, current);
            let (d, w) = (dist(i), width(i));
            let near = g < d - w;
            let far = if c.far_capped {
                far_distance(d, d - w)
            } else {
                d + w
            };
            if !near && g <= far {
                memo.wait_since = 0;
                continue;
            }
            if memo.wait_since == 0 || memo.wait_near != near {
                memo.wait_since = now;
                memo.wait_near = near;
                memo.wait_ref = last;
            } else if c.short {
                memo.wait_ref = memo.wait_ref.max(last);
            } else {
                memo.wait_ref = memo.wait_ref.min(last);
            }
            let wait = secs_ms(if near { c.replace_delay } else { c.raise_wait });
            if now - memo.wait_since < wait || rec.buy.exchange_id == 0 || backoff(memo, now) {
                continue;
            }
            let reference = memo.wait_ref;
            let price = target(i, reference);
            if price <= 0.0 || (price - current).abs() < tick {
                continue;
            }
            // The moved entry is sized by the same rule as a new one: a lot dearer than the
            // budget by more than `LOT_OVER_BUDGET` is not placed — the order stays where it is.
            // Sized at the price it will rest at, on the tick grid (where `target_entry` takes
            // the lot count), as a new entry is.
            let rested = m.nearest(price);
            let Some(size) = p.entry_size(i, m, if rested > 0.0 { rested } else { price }) else {
                if !std::mem::replace(&mut memo.lot_said, true) {
                    cmds.push(over_lot_log(s, p, m, i, price));
                }
                continue;
            };
            memo.lot_said = false;
            // A move to a bigger size (a dearer lot at the new price) spends the difference:
            // it goes through the balance check like a new entry.
            // Only when a whole further lot is bought: the budget is not the notional of the
            // lots placed (that is the budget floored to lots), and the leftover is not spent.
            let one_lot = m.lot_value(if rested > 0.0 { rested } else { price });
            let more = size - rec.buy.notional;
            if one_lot > 0.0 && more >= one_lot && !self.funds.take(s, p, m, more, cmds) {
                continue;
            }
            memo.wait_since = 0;
            cmds.push(Cmd::MoveEntry {
                order: o.id,
                price,
                size,
                planned: c.hook.map_or(0.0, |h| h.planned(p, m, price)),
            });
            let delay = if near {
                format!(" delay: {wait}ms;")
            } else {
                String::new()
            };
            cmds.push(Cmd::Log(format!(
                "{}: {} order replacing {} on cur. price: {last}{delay} Min. Ask: {reference} \
                 PriceMin: {:.2}% Price: {:.2}% tier {i}: {} -> {price} (strategy <{}>)",
                o.market,
                c.kind,
                if near { "DOWN" } else { "UP" },
                d - w,
                d,
                current,
                label(s)
            )));
        }
        started
    }

    /// `AutoCancelBuy`: an entry older than it (from its placement or its
    /// last manual move) is cancelled; returns whether it is.
    fn auto_cancel(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        o: &CoreOrder,
        now: i64,
        cmds: &mut Vec<Cmd>,
    ) -> bool {
        let rec = o.record();
        let age = now - rec.buy.create_ms.max(o.entry_moved_at);
        if p.auto_cancel <= 0.0 || rec.buy.exchange_id == 0 || age < secs_ms(p.auto_cancel) {
            return false;
        }
        let memo = self.memo.entry(o.id).or_default();
        if now - memo.cancel_at >= CANCEL_RETRY_MS {
            memo.cancel_at = now;
            cmds.push(Cmd::Cancel { order: o.id });
            cmds.push(Cmd::Log(format!(
                "{}: Auto cancel buy order activated since {} sec. (strategy <{}>)",
                o.market,
                age / 1000,
                label(s)
            )));
        }
        true
    }

    /// DropsDetection entries: a drop of `DropsPriceDelta` % (and, with
    /// `DropsPriceIsLow`, the price at its hourly low) on a market where the
    /// strategy holds no order puts out `OrdersCount` entries at once, tier
    /// `i` `buyPrice − i·|BuyPriceStep|` % from the base price; they stay
    /// until filled or `AutoCancelBuy`, each fill gets its own exit. The next
    /// signal waits `NextDetectPenalty` s.
    #[allow(clippy::too_many_arguments)]
    fn drops_entries(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        idx: u16,
        m: &Market,
        entries: &[&CoreOrder],
        busy: bool,
        slots: &mut i64,
        cx: &Ctx,
        cmds: &mut Vec<Cmd>,
    ) {
        let now = cx.now;
        // `DropsPriceDelta` 0 = off: any flat market would fire every pass.
        if !self.signal_ready(s, p, idx, m, entries, busy, *slots, now, cmds)
            || p.drops_delta <= 0.0
        {
            return;
        }
        let key = (s.strategy_id, idx);
        let Some(d) = self.tapes.drop_at(
            idx,
            now,
            p.drops_max_time,
            p.drops_price_ma,
            p.drops_last_ma,
            m.last(),
        ) else {
            return;
        };
        if d.pct < p.drops_delta {
            return;
        }
        let low = cx
            .win
            .extremes(idx, now, 60)
            .is_some_and(|(lo, _)| m.last() <= lo);
        if p.drops_is_low && !low {
            return;
        }
        self.mark_detect(key, now);
        // DropsDetection only ever buys (`is_short: false` below).
        let base = if p.drops_use_last {
            d.current
        } else {
            p.anchor.price(m, false)
        };
        let dp = m.price_precision as usize;
        cmds.push(Cmd::Detect {
            market: m.symbol.clone(),
            strategy_id: s.strategy_id,
            is_short: false,
            msg: format!("drop {:.2}%", d.pct),
        });
        cmds.push(Cmd::Log(format!(
            "{}: DropsDetection: drop {:.2}% in {:.0} s High: {:.dp$} LastPrice: {:.dp$} \
             PriceIsLow: {low} (strategy <{}>)",
            m.symbol,
            d.pct,
            p.drops_max_time,
            d.high,
            d.current,
            label(s)
        )));
        for i in 0..p.orders_count.max(1) as usize {
            let pct = p.buy_price - i as f64 * p.step.abs();
            let price = band_entry(
                s,
                m,
                "Drops",
                false,
                i,
                m.nearest(base * (1.0 + pct / 100.0)),
                cmds,
            );
            start_tier(
                s,
                p,
                m,
                "Drops",
                false,
                i,
                price,
                0.0,
                slots,
                &mut self.funds,
                cmds,
            );
        }
    }

    /// Common to the signal strategies: nothing before the market's first
    /// trade since the start (restored entries wait, as MoonShot's ladder
    /// does); `AutoCancelBuy` on the entries; a new signal only on a market
    /// where the strategy holds no order, with a slot free, and
    /// `NextDetectPenalty` s after the last one.
    #[allow(clippy::too_many_arguments)]
    fn signal_ready(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        idx: u16,
        m: &Market,
        entries: &[&CoreOrder],
        busy: bool,
        slots: i64,
        now: i64,
        cmds: &mut Vec<Cmd>,
    ) -> bool {
        if !m.live_price() {
            return false;
        }
        for o in entries {
            self.auto_cancel(s, p, o, now, cmds);
        }
        !busy
            && slots > 0
            && p.order_size > 0.0
            && !self
                .detect_at
                .get(&(s.strategy_id, idx))
                .is_some_and(|&at| now - at < secs_ms(p.detect_penalty))
            && !self.detected_by_other(s.strategy_id, idx, p, now)
            // Checked before the detect: a signal the money cannot serve
            // must not spend its `NextDetectPenalty`.
            && self.funds.can(p, m)
    }

    /// MoonStrike entries: a strike of `MStrikeDepth` % (plus its `Add*`
    /// shifts) with `MStrikeVolume` turnover on an allowed side becomes a
    /// signal; after `MStrikeBuyDelay` ms (the strike still measured) and,
    /// with `MStrikeWaitDip`, a trade back against the strike within 10 s,
    /// `OrdersCount` entries go out at `MStrikeBuyLevel`, tier `i` further
    /// `i·|BuyPriceStep|` %, each planning its exit `MStrikeSellLevel` % of the
    /// depth from the extreme (MoonBot's precomputed «SellPrice»).
    #[allow(clippy::too_many_arguments)]
    fn strike_entries(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        idx: u16,
        m: &Market,
        entries: &[&CoreOrder],
        busy: bool,
        held: (bool, bool),
        slots: &mut i64,
        cx: &Ctx,
        cmds: &mut Vec<Cmd>,
    ) {
        let now = cx.now;
        let key = (s.strategy_id, idx);
        if let Some(mut sig) = self.signals.get(&key).copied() {
            if let Some(k) = self.tracks.strike(idx, sig.short) {
                sig.extreme = if sig.short {
                    sig.extreme.max(k.extreme)
                } else {
                    sig.extreme.min(k.extreme)
                };
            }
            let dip = !p.strike_wait_dip || self.tracks.rebound_at(idx, sig.short) > sig.at;
            if p.strike_wait_dip && !dip && now - sig.at > strike::WAIT_DIP_MS {
                self.signals.remove(&key);
                cmds.push(Cmd::Log(format!(
                    "{}: MoonStrike: no dip in {} s, no order (strategy <{}>)",
                    m.symbol,
                    strike::WAIT_DIP_MS / 1000,
                    label(s)
                )));
                return;
            }
            if now - sig.at < p.strike_delay_ms || !dip {
                self.signals.insert(key, sig);
                return;
            }
            self.signals.remove(&key);
            // The gates may have closed meanwhile: the signal is spent.
            if m.live_price() && *slots > 0 && !busy && self.enters(p, m, idx, sig.short, now) {
                self.strike_orders(s, p, m, sig, slots, cmds);
            }
            return;
        }
        if !self.signal_ready(s, p, idx, m, entries, busy, *slots, now, cmds)
            || p.strike_depth <= 0.0
        {
            return;
        }
        let shift = cx.win.range(idx, m.last(), now, 15) * p.strike_add_15m
            + cx.win.range(idx, m.last(), now, 60) * p.strike_add_1h
            + cx.market_delta
                .map_or(0.0, |d| d.abs() * p.strike_add_market);
        let need = p.strike_depth + shift;
        for short in [false, true] {
            // No entry against an open position of the other side.
            let against = if short { held.0 } else { held.1 };
            if !self.enters(p, m, idx, short, now) || against {
                continue;
            }
            let Some(k) = self.tracks.strike(idx, short) else {
                continue;
            };
            if k.depth < need || k.turnover < p.strike_volume {
                continue;
            }
            self.mark_detect(key, now);
            let d = m.price_precision as usize;
            cmds.push(Cmd::Detect {
                market: m.symbol.clone(),
                strategy_id: s.strategy_id,
                is_short: short,
                msg: format!("strike {:.2}%", k.depth),
            });
            cmds.push(Cmd::Log(format!(
                "{}: MoonStrike {}: {:.d$} {}: {:.d$} Depth: {:.2}% (need {need:.2}%) \
                 StrikeVol: {:.0} USDT (strategy <{}>)",
                m.symbol,
                if short { "LastASK" } else { "LastBID" },
                k.reference,
                if short { "max.Price" } else { "min.Price" },
                k.extreme,
                k.depth,
                k.turnover,
                label(s)
            )));
            let wait = if p.strike_wait_dip {
                strike::WAIT_DIP_MS
            } else {
                strike::TICK_MS
            };
            let sig = Signal {
                short,
                at: now,
                expires: now + p.strike_delay_ms.max(0) + wait,
                reference: k.reference,
                extreme: k.extreme,
            };
            if p.strike_delay_ms > 0 || p.strike_wait_dip {
                self.signals.insert(key, sig);
            } else {
                self.strike_orders(s, p, m, sig, slots, cmds);
            }
            return;
        }
    }

    /// The entries of a strike signal and their planned exits.
    fn strike_orders(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        m: &Market,
        sig: Signal,
        slots: &mut i64,
        cmds: &mut Vec<Cmd>,
    ) {
        let tick = m.tick_size.max(f64::EPSILON);
        let d = m.price_precision as usize;
        for i in 0..p.orders_count.max(1) as usize {
            let (take, _, depth, sell) = strike_prices(p, sig, i);
            let price = band_entry(s, m, "MoonStrike", sig.short, i, m.nearest(take), cmds);
            // A level past −100 % or past the exchange bound prices nothing
            // (`start_tier` skips it too).
            if price <= 0.0 {
                continue;
            }
            // The exit is at least a tick in profit.
            let planned = if sig.short {
                m.nearest(sell).min(price - tick)
            } else {
                m.nearest(sell).max(price + tick)
            };
            cmds.push(Cmd::Log(format!(
                "{}: MoonStrike {}{}: {:.d$} {}: {:.d$} (take {take:.d$}) Depth: {depth:.1}% \
                 BuyPrice: {price:.d$} sell {:+.1}% SellPrice: {planned:.d$} (strategy <{}>)",
                m.symbol,
                if sig.short { "SHORT " } else { "" },
                if sig.short { "LastASK" } else { "LastBID" },
                sig.reference,
                if sig.short { "max.Price" } else { "min.Price" },
                sig.extreme,
                depth * p.strike_sell_level / 100.0,
                label(s)
            )));
            start_tier(
                s,
                p,
                m,
                "MoonStrike",
                sig.short,
                i,
                price,
                planned,
                slots,
                &mut self.funds,
                cmds,
            );
        }
    }

    /// MoonHook entries: inside `HookTimeFrame` a move of `HookDetectDepth` %
    /// whose price has come `HookPriceRollBack` % of it back and held there
    /// `HookRollBackWait` ms puts out `OrdersCount` entries — tier 0 at
    /// `HookInitialPrice` % of the move, tier `i` `i·|BuyPriceStep|` %
    /// further from the price. They then follow the price inside a corridor
    /// `HookPriceDistance` % of the move wide, as MoonShot's do, and each
    /// plans its exit off `HookSellLevel`. The detect outlives them, so
    /// `HookRepeatAfterSell` can put the same ladder out once more. With
    /// `HookDirection` open both ways the deeper of the two moves is the
    /// one taken, not whichever side is judged first.
    #[allow(clippy::too_many_arguments)]
    fn hook_entries(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        idx: u16,
        m: &Market,
        os: &[&CoreOrder],
        entries: &[&CoreOrder],
        busy: bool,
        held: (bool, bool),
        slots: &mut i64,
        cx: &Ctx,
        cmds: &mut Vec<Cmd>,
    ) {
        let now = cx.now;
        let key = (s.strategy_id, idx);
        if let Some(h) = self.hooks.get(&key).copied() {
            if !entries.is_empty() {
                self.manage_entries(s, p, m, entries, h.corridor(p), false, slots, now, cmds);
                return;
            }
            // A position of this detect is still open; its exit is managed
            // with every other one.
            if busy {
                return;
            }
            if p.hook_repeat
                && !h.repeated
                && p.trades(m, h.short, now)
                && os
                    .iter()
                    .any(|o| sold_in_profit(o, h.at, p.hook_repeat_profit))
            {
                // BV/SV holds the repeat: the hook waits for it.
                if !self.enters(p, m, idx, h.short, now) {
                    return;
                }
                let h = Hook {
                    repeated: true,
                    ..h
                };
                self.hooks.insert(key, h);
                cmds.push(Cmd::Log(format!(
                    "{}: MoonHook: repeat after sell (strategy <{}>)",
                    m.symbol,
                    label(s)
                )));
                hook_orders(s, p, m, h, slots, &mut self.funds, cmds);
                return;
            }
            self.hooks.remove(&key);
        }
        // `HookDetectDepth` 0 = off: a flat market would fire every pass.
        if !self.signal_ready(s, p, idx, m, entries, busy, *slots, now, cmds) || p.hook_depth <= 0.0
        {
            return;
        }
        // MoonBot's own floor on top of `NextDetectPenalty`: a second detect
        // is not possible inside a frame, or the strategy spams them.
        let frame = secs_ms(p.hook_frame);
        if self.detect_at.get(&key).is_some_and(|&at| now - at < frame) {
            return;
        }
        // Both sides are judged before one is taken, and the deeper move
        // wins: one frame can hold a rise and a fall that both qualify, and
        // taking whichever came first in the loop made a spike read as its
        // own mirror. A tie keeps the long — the order they were judged in
        // before.
        let mut best: Option<(bool, hook::Detect)> = None;
        for short in [false, true] {
            // No entry against an open position of the other side.
            let against = if short { held.0 } else { held.1 };
            if !self.enters(p, m, idx, short, now) || against {
                continue;
            }
            let Some(d) = self
                .hook_tape
                .detect(idx, now, frame, p.hook_anti_pump, short)
            else {
                continue;
            };
            if !within(d.depth, p.hook_depth, p.hook_depth_max)
                || !within(d.rollback, p.hook_rollback, p.hook_rollback_max)
                || !within(d.drop_ratio, p.hook_drop_min, p.hook_drop_max)
                || d.turnover < p.hook_min_volume
            {
                continue;
            }
            // The rollback counts only once it has held: `HookRollBackWait`
            // drops the one that was a single print.
            let level = d.level(p.hook_rollback);
            if self.hook_tape.held_beyond(idx, now, level, short) < p.hook_rollback_wait {
                continue;
            }
            match best {
                Some((_, b)) if b.depth >= d.depth => {}
                _ => best = Some((short, d)),
            }
        }
        let Some((short, d)) = best else {
            return;
        };
        let anchor = p.anchor.price(m, short);
        let base = d.level(p.hook_initial);
        let dist = gap(short, anchor, base);
        let dp = m.price_precision as usize;
        self.mark_detect(key, now);
        // `HookInitialPrice` past the rollback would price the entry on
        // the wrong side of the market: MoonBot takes no detect then.
        if dist <= 0.0 {
            cmds.push(Cmd::Log(format!(
                "{}: MoonHook: order {base:.dp$} is past the price {anchor:.dp$}, \
                 no detect (strategy <{}>)",
                m.symbol,
                label(s)
            )));
            return;
        }
        cmds.push(Cmd::Detect {
            market: m.symbol.clone(),
            strategy_id: s.strategy_id,
            is_short: short,
            msg: format!("hook {:.2}%", d.depth),
        });
        cmds.push(Cmd::Log(format!(
            "{}: MoonHook {}{}: {:.dp$} {}: {:.dp$} Depth: {:.2}% RollBack: {:.0}% \
             HookDrop: {:.0}% Vol: {:.0} USDT (strategy <{}>)",
            m.symbol,
            if short { "SHORT " } else { "" },
            if short { "min.Price" } else { "max.Price" },
            d.reference,
            if short { "max.Price" } else { "min.Price" },
            d.extreme,
            d.depth,
            d.rollback,
            d.drop_ratio,
            d.turnover,
            label(s)
        )));
        let h = Hook {
            short,
            at: now,
            reference: d.reference,
            dist,
            width: d.depth * p.hook_distance / 100.0,
            repeated: false,
        };
        self.hooks.insert(key, h);
        hook_orders(s, p, m, h, slots, &mut self.funds, cmds);
    }

    /// Exit of an open position: placed at `SellPrice` from the mean entry
    /// (a planned exit — MoonStrike — at its planned distance instead),
    /// lowered by PriceDown (`price_down`), replaced by a marketable close
    /// when one of the strategy's stops fires (`stops.rs`). The stops watch
    /// the side of the book the exit would hit (bid for a long, ask for a
    /// short; `last` without a book) and are not live for `StopLossDelay` s
    /// after the fill.
    #[allow(clippy::too_many_arguments)]
    fn manage_exit(
        &mut self,
        s: &StrategySnapshot,
        o: &CoreOrder,
        p: &Params,
        m: &Market,
        settled: bool,
        feed: ExitFeed,
        now: i64,
        cmds: &mut Vec<Cmd>,
    ) {
        let rec = o.record();
        // Orders::watch owns a manual Panic / ClosePosition until it ends.
        if rec.panic {
            return;
        }
        let entry = if rec.buy.mean_price > 0.0 {
            rec.buy.mean_price
        } else {
            rec.buy.price
        };
        if entry <= 0.0 {
            return;
        }
        let short = o.is_short;
        let tick = m.tick_size.max(f64::EPSILON);
        let has_exit = o.status == status::SELL_SET;
        // A planned exit (MoonStrike) keeps its distance from the fill, as
        // `Orders` placed it; PriceDown steps from there.
        let sell_pct = match o.planned_ratio() {
            r if r > 0.0 => (if short { 1.0 / r } else { r } - 1.0) * 100.0,
            _ => p.sell_price,
        };
        let memo = self.memo.entry(o.id).or_default();
        // `(0, 0)` = no stop, the order's default: nothing goes out for a
        // stop-less strategy.
        let line = memo.stops.shown(&p.stops, short, entry);
        if (line.price, line.spread) != o.bot_stop() {
            if line.price > 0.0 && memo.stops.announce(line.what) {
                let d = m.price_precision as usize;
                let from = if line.what == "Trailing" {
                    "peak"
                } else {
                    "buyPrice"
                };
                cmds.push(Cmd::Log(format!(
                    "{}: {} applied ({from} {:.d$} stop {:+.2}% => {:.d$}) (strategy <{}>)",
                    o.market,
                    line.what,
                    line.from,
                    line.pct,
                    line.price,
                    label(s)
                )));
            }
            cmds.push(Cmd::Stop {
                order: o.id,
                price: line.price,
                spread: line.spread,
            });
        }
        // Outside `TRADING` (settling, a halt) the exit stays as it is. On
        // stale prices
        // (`Market::fresh`) only the exit priced off the entry may go out; the
        // stops and PriceDown wait for the stream.
        if backoff(memo, now) || !settled {
            return;
        }
        let fresh = m.fresh();
        // A move deferred in `Orders` is already on its way: judge against
        // it, or every pass repeats the move and its log line. No move while
        // a plain Cancel of the exit is in flight (`Orders` would drop it).
        let heading = o.heading(Leg::Sell);
        let mv = |price: f64, reason: u8| Cmd::Move {
            order: o.id,
            leg: Leg::Sell,
            price,
            reason,
            market: false,
        };
        let (side, book) = if short {
            ("ASK", m.ask_px())
        } else {
            ("BID", m.bid_px())
        };
        let px = if book > 0.0 { book } else { m.last() };
        if let Some(fired) = memo.stops.fired {
            if !fresh {
                return;
            }
            let Some((price, _, _)) =
                memo.stops
                    .exit(&p.stops, None, m, short, entry, px, feed.delta_1m)
            else {
                return;
            };
            let behind = heading
                .is_some_and(|exit| (if short { price - exit } else { exit - price }) >= tick);
            if !has_exit
                || (behind && rec.sell.exchange_id != 0 && now - memo.stops.moved >= STOP_CHASE_MS)
            {
                memo.stops.moved = now;
                cmds.push(mv(price, fired.reason()));
            }
            return;
        }
        if fresh && px > 0.0 {
            let pass = stops::Pass {
                short,
                entry,
                held_ms: now - rec.buy.close_ms,
                px,
                swing: feed.swing,
                volumes: feed.volumes,
                now,
            };
            if let Some(t) = memo.stops.judge(&p.stops, &pass) {
                memo.stops.fired = Some(t.fired);
                memo.stops.moved = now;
                let base = memo.stops.base(&p.stops, entry);
                if let Some((price, spread, limit)) =
                    memo.stops
                        .exit(&p.stops, Some(&t), m, short, entry, px, feed.delta_1m)
                {
                    cmds.push(Cmd::Log(fired_line(
                        s, o, p, m, &t, side, base, price, spread,
                    )));
                    // A live stop that only has to cross the book goes at
                    // MARKET (`PLAN.md`, «Открытые решения» п. 2), in the
                    // emulator too: Orders follows it as a panic from here on.
                    // One that must stay a limit is chased as before.
                    cmds.push(Cmd::Move {
                        order: o.id,
                        leg: Leg::Sell,
                        price,
                        reason: t.fired.reason(),
                        market: !limit,
                    });
                }
                return;
            }
        }
        if !has_exit {
            let price = m.within_limits(m.nearest(exit_price(short, entry, sell_pct)), short);
            if price > 0.0 {
                cmds.push(mv(price, reason::SELL_PRICE));
                cmds.push(Cmd::Log(format!(
                    "{}: Using (strategy <{}>) SELL price: {sell_pct:.2}% = {price}",
                    o.market,
                    label(s),
                )));
            }
            return;
        }
        let Some(exit) = heading else {
            return;
        };
        if !fresh {
            return;
        }
        if rec.sell.exchange_id == 0 || p.pd_timer <= 0.0 || p.pd_pct <= 0.0 {
            return;
        }
        let (elapsed, timer, delay) = (
            now - rec.sell.create_ms,
            secs_ms(p.pd_timer),
            // Zero means continue at the minimum interval (MoonBot FAQ),
            // not a single step. The engine's cadence may coalesce steps.
            secs_ms(p.pd_delay).max(330),
        );
        if elapsed < timer {
            return;
        }
        let steps = 1 + (elapsed - timer) / delay;
        let mut floor = m.nearest(exit_price(short, entry, p.pd_drop));
        // A planned exit nearer the entry than `PriceDownAllowedDrop` is
        // already past the floor: PriceDown never moves it away.
        if o.planned_ratio() > 0.0 {
            let start = m.nearest(exit_price(short, entry, sell_pct));
            floor = if short {
                floor.max(start)
            } else {
                floor.min(start)
            };
        }
        let price = m.within_limits(
            price_down(p, m, short, entry, sell_pct, steps, floor),
            short,
        );
        if price <= 0.0 || (price - exit).abs() < tick {
            return;
        }
        cmds.push(mv(price, reason::AUTO_PRICE_DOWN));
        cmds.push(Cmd::Log(format!(
            "{}: Auto Sell Replacing ({}: {} => Perc: {} AllowedDrop: {floor} NewP: {price})",
            o.market,
            if p.pd_relative {
                "PriceDownRelative"
            } else {
                "PriceDown"
            },
            exit,
            p.pd_pct,
        )));
    }
}

/// The log line of a stop that fired; stop 1's is MoonBot's own wording.
#[allow(clippy::too_many_arguments)]
fn fired_line(
    s: &StrategySnapshot,
    o: &CoreOrder,
    p: &Params,
    m: &Market,
    t: &stops::Trigger,
    side: &str,
    base: f64,
    price: f64,
    spread: f64,
) -> String {
    let d = m.price_precision as usize;
    let (market, strat, spread) = (&o.market, label(s), spread * 100.0);
    match t.fired {
        Fired::Stop if t.immediate => format!(
            "{market}: Immediate StopLoss: sell price is [actual buy - StopSpread%]: \
             {base:.d$} - {spread:.2}% = {price:.d$} (strategy <{strat}>)"
        ),
        Fired::Stop | Fired::Stop3 => {
            let what = if t.fired == Fired::Stop {
                "StopLoss"
            } else {
                "StopLoss3"
            };
            format!(
                "{market}: {what} AutoActivated on price drop: {side} = {:.d$} LastPrice = {:.d$} \
                 BuyPrice = {base:.d$}; {what} fixed: {:.d$} spread: {spread:.2}% => {price:.d$} \
                 (strategy <{strat}>)",
                t.px,
                m.last(),
                t.line
            )
        }
        Fired::Trailing => {
            format!(
            "{market}: Trailing AutoActivated: {side} = {:.d$} LastPrice = {:.d$} Peak = {:.d$}; \
             Trailing line: {:.d$} spread: {spread:.2}% => {price:.d$} (strategy <{strat}>)",
            t.px, m.last(), t.peak, t.line
        )
        }
        Fired::BvSv => format!(
            "{market}: BV/SV Stop AutoActivated: BV/SV = {:.2} < {:.2} spread: {spread:.2}% => \
             {price:.d$} (strategy <{strat}>)",
            t.ratio,
            p.stops.bvsv.map_or(0.0, |b| b.ratio)
        ),
    }
}

/// What the stops read of a market beyond its book (`stops::Pass`).
#[derive(Debug, Default, Clone, Copy)]
struct ExitFeed {
    /// The 1-minute move, % (`StopSpreadAdd1mDelta`).
    delta_1m: f64,
    /// Lowest and highest trade since the last pass (`FastStopLoss`).
    swing: Option<(f64, f64)>,
    /// `(bought, sold)` turnover of the BV/SV window.
    volumes: Option<(f64, f64)>,
}

/// Exit after `steps` PriceDown steps from the exit's `sell` % (`SellPrice`,
/// or a planned exit's distance), at a tick and not past `floor`
/// (`PriceDownAllowedDrop`): each step takes `PriceDownPercent` % of the
/// placed exit, or with `PriceDownRelative` that share of what is left to
/// the entry, from the rounded previous price as MoonBot does (ETH_short
/// 15.09: 2370.77 → 2380.25 → 2385.94 → 2389.36 → 2391.41 → …; MON 19.09:
/// 0.025290 → 0.025280 → 0.025270).
fn price_down(
    p: &Params,
    m: &Market,
    short: bool,
    entry: f64,
    sell: f64,
    steps: i64,
    floor: f64,
) -> f64 {
    let cap = |x: f64| if short { x.min(floor) } else { x.max(floor) };
    let keep = 1.0 - p.pd_pct / 100.0;
    let mut price = m.nearest(exit_price(short, entry, sell));
    for _ in 0..steps {
        // Absolute steps take `PriceDownPercent` of the placed exit (MoonBot
        // 7.71 log 19.09: 0.025290 → p 0.025277 → 0.025280); relative ones
        // that share of what is left to the entry.
        let next = if p.pd_relative {
            cap(m.nearest(entry + (price - entry) * keep))
        } else {
            let k = if short { 2.0 - keep } else { keep };
            cap(m.nearest(price * k))
        };
        if next == price || next == floor {
            return next;
        }
        price = next;
    }
    price
}

/// MoonStrike tier `i` (MoonBot 7.71 log 19.09, ST_20/25/30 long and short):
/// the take price — `MStrikeBuyLevel` % from the strike's reference, which is
/// the LastBidEMA for a long and the LastAskEMA for a short (a short divides:
/// `reference / (1 + L/100)`), or with `MStrikeBuyRelative` that share of the
/// depth from the extreme — `i·|BuyPriceStep|` % further; the bottom (the
/// farther of the extreme and the take), its depth % from that same
/// reference, and the exit `depth × MStrikeSellLevel/100` % from the bottom
/// (a short divides).
/// Unrounded: `(take, bottom, depth %, sell)`.
fn strike_prices(p: &Params, sig: Signal, i: usize) -> (f64, f64, f64, f64) {
    let (reference, short) = (sig.reference, sig.short);
    let take0 = if p.strike_relative {
        // FAQ: 0 = at the extreme, 50 = mid-strike.
        sig.extreme + (reference - sig.extreme) * p.strike_level / 100.0
    } else {
        exit_price(short, reference, p.strike_level)
    };
    let take = exit_price(short, take0, -(i as f64) * p.step.abs());
    let bottom = if short {
        take.max(sig.extreme)
    } else {
        take.min(sig.extreme)
    };
    let depth = (bottom / reference - 1.0).abs() * 100.0;
    let sell = exit_price(short, bottom, depth * p.strike_sell_level / 100.0);
    (take, bottom, depth, sell)
}

/// The ladder of a MoonHook detect: tier `i` sits `i·|BuyPriceStep|` %
/// further from the price than tier 0, each with its own planned exit.
fn hook_orders(
    s: &StrategySnapshot,
    p: &Params,
    m: &Market,
    h: Hook,
    slots: &mut i64,
    funds: &mut Funds,
    cmds: &mut Vec<Cmd>,
) {
    let anchor = p.anchor.price(m, h.short);
    for i in 0..p.orders_count.max(1) as usize {
        let dist = h.dist + i as f64 * p.step.abs();
        let raw = m.nearest(entry_price(h.short, anchor, dist));
        let price = band_entry(s, m, "MoonHook", h.short, i, raw, cmds);
        if price <= 0.0 {
            continue;
        }
        let planned = h.planned(p, m, price);
        start_tier(
            s, p, m, "MoonHook", h.short, i, price, planned, slots, funds, cmds,
        );
    }
}

/// A finished sale of this strategy on the market, closed after `since` and
/// at or past `pct` % of profit against its mean entry (`HookRepeatIfProfit`).
fn sold_in_profit(o: &CoreOrder, since: i64, pct: f64) -> bool {
    let rec = o.record();
    if o.status != status::SELL_DONE || rec.sell.close_ms <= since {
        return false;
    }
    let (buy, sell) = (rec.buy.mean_price, rec.sell.mean_price);
    if buy <= 0.0 || sell <= 0.0 {
        return false;
    }
    let k = if o.is_short { -1.0 } else { 1.0 };
    k * (sell - buy) / buy * 100.0 >= pct
}

/// What one entry filter reads off the market.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Metric {
    /// USDT turnover of the clock's last 24 hours (`vol24`).
    Vol24,
    /// USDT turnover of the last `minutes` of the clock.
    Turnover(i64),
    /// Signed % of the price at the start of a `minutes` window
    /// (`Windows::delta`).
    Delta(i64),
    /// The market's highest leverage ([`Market::max_leverage`]).
    Leverage,
    /// The hourly delta of `BTCUSDT`, in %.
    BtcDelta,
    /// The traded markets' mean hourly delta, in % ([`market_delta`]).
    MarketDelta,
    /// A screener sort key in its own unit (`FilterBy`).
    Key(screener::SortKey),
}

/// One check of the Filters tab: a corridor on one metric. A bound of 0 is no
/// bound on that side and both 0 is no check at all (MoonBot's «0 = not
/// checked»), so a `Check` that exists always has something to say.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Check {
    /// What the log line calls it.
    pub what: &'static str,
    pub metric: Metric,
    pub lo: f64,
    pub hi: f64,
}

/// What one [`Check`] says about one market.
///
/// [`Judged::Waiting`] is the state a green/red picture has no room for and
/// the gate has always had: a check whose window the market's history does not
/// cover yet is SKIPPED, not failed. Painted as a refusal it would accuse a
/// market the strategy is about to enter; painted as a pass it would promise a
/// corridor nobody measured.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Judged {
    /// Inside the corridor, with the value read.
    Pass(f64),
    /// Outside it.
    Refused(f64),
    /// Not measurable yet — a window the market's history does not reach
    /// after a start, a key with no opening price.
    Waiting,
    /// The check cannot be asked at all; the text is the reason.
    Broken(&'static str),
}

impl Check {
    /// Turnover, or a signed percentage: the two read differently in a log
    /// line, and `FilterBy` can be either.
    pub fn turnover(&self) -> bool {
        match self.metric {
            Metric::Vol24 | Metric::Turnover(_) => true,
            Metric::Delta(_) | Metric::Leverage | Metric::BtcDelta | Metric::MarketDelta => false,
            Metric::Key(k) => k.turnover(),
        }
    }

    /// A leverage in `x`, which is neither of the other two readings.
    pub fn leverage(&self) -> bool {
        matches!(self.metric, Metric::Leverage)
    }

    /// The market's value for this check. `Ok(None)` = not measurable yet —
    /// a window the market's history does not cover after a start, a key with
    /// no opening price — and the check waits rather than refusing. `Err` is
    /// a check that cannot be asked at all, with the reason; the caller turns
    /// that into the gate, because the same answer also reaches the page,
    /// which has no use for a `Gate`.
    fn value(&self, idx: u16, cx: &Ctx) -> Result<Option<f64>, &'static str> {
        let last = |i: u16| cx.model.at(i).map_or(0.0, |m| m.last());
        Ok(match self.metric {
            // Turnover has no "not measured": an empty window is an honest
            // zero turnover, the same reading the screener's keys take.
            Metric::Vol24 => Some(cx.win.vol24(idx, cx.now)),
            Metric::Turnover(minutes) => Some(cx.win.turnover(idx, cx.now, minutes)),
            // A market's leverage is known from the catalog; an index outside it
            // reads 0, which a `MinLeverage` refuses and no `MaxLeverage` does.
            Metric::Leverage => Some(
                cx.model
                    .at(idx)
                    .map_or(0.0, |m| f64::from(m.max_leverage())),
            ),
            Metric::Delta(minutes) => cx
                .win
                .covers(idx, cx.now, minutes)
                .then(|| cx.win.delta(idx, last(idx), cx.now, minutes))
                .flatten(),
            Metric::BtcDelta => {
                let Some(btc) = cx.btc else {
                    return Err("Delta_BTC: BTCUSDT is not in the catalog");
                };
                cx.win
                    .covers(btc, cx.now, 60)
                    .then(|| cx.win.delta(btc, last(btc), cx.now, 60))
                    .flatten()
            }
            Metric::MarketDelta => cx.market_delta,
            Metric::Key(key) => key.value(idx, cx),
        })
    }

    /// This check's verdict on one market. The entry gate
    /// ([`DeltaFilters::refused`]) and the page's column per check
    /// ([`DeltaFilters::judge_all`]) both come through here, so a market the
    /// page paints as held back is the market the entry is refused on.
    pub fn judge(&self, idx: u16, cx: &Ctx) -> Judged {
        match self.value(idx, cx) {
            Err(why) => Judged::Broken(why),
            Ok(None) => Judged::Waiting,
            Ok(Some(v)) if v < self.lo || v > self.hi => Judged::Refused(v),
            Ok(Some(v)) => Judged::Pass(v),
        }
    }

    /// The gate for a value outside the corridor, in the metric's own unit.
    fn refuse(&self, v: f64) -> Gate {
        if self.leverage() {
            Gate::Leverage {
                leverage: v.round() as i32,
            }
        } else if self.turnover() {
            Gate::Volume {
                what: self.what,
                turnover: v.round() as i64,
            }
        } else {
            Gate::Delta {
                what: self.what,
                hundredths: (v * 100.0).round() as i64,
            }
        }
    }
}

/// MoonBot's Filters tab as the checks an entry passes.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaFilters {
    /// In the order they are asked, which is the order they are declared in
    /// [`DeltaFilters::read`]: the volume bounds, the leverage corridor, the
    /// market's own deltas, BTC and the market, `FilterBy`. The order decides the log line, because the
    /// FIRST check a market fails is the reason it gets no entry — so the
    /// coarsest question ("is this market traded at all") comes before the
    /// finest ("is its two-hour move inside the corridor").
    pub checks: Vec<Check>,
    pub penalty_ms: i64,
    /// A setting that cannot be judged: the strategy does not enter.
    pub problem: Option<&'static str>,
}

impl DeltaFilters {
    fn read(
        num: &dyn Fn(&str) -> f64,
        text: &dyn Fn(&str) -> String,
        flag: &dyn Fn(&str) -> bool,
    ) -> Option<Self> {
        // `IgnoreFilters` is the whole tab; `IgnoreVolume`, `IgnoreBase` (the
        // leverage corridor only) and `IgnoreDelta` are its boxes, as in MoonBot. They used to share one `if`, so
        // switching the deltas off switched the volume bounds off with them —
        // which nobody asked for and nothing said.
        if flag("IgnoreFilters") {
            return None;
        }
        // Both 0 = off; one side 0 = no bound on that side (MoonBot's
        // «0 = not checked», as for the volume bounds).
        let range = |lo: &str, hi: &str| {
            let (lo, hi) = (num(lo), num(hi));
            (lo.is_finite() && hi.is_finite() && (lo != 0.0 || hi != 0.0)).then_some((
                if lo == 0.0 { f64::NEG_INFINITY } else { lo },
                if hi == 0.0 { f64::INFINITY } else { hi },
            ))
        };
        let mut problem = None;
        let minutes = |t: &str| -> Option<i64> {
            let t = t.trim().to_ascii_lowercase();
            let (n, unit) = t.split_at(t.find(|c: char| !c.is_ascii_digit())?);
            let n: i64 = n.parse().ok()?;
            match unit {
                "m" => Some(n),
                "h" => Some(n * 60),
                _ => None,
            }
        };
        let mut checks: Vec<Check> = Vec::new();
        // Filters / Volume. These bound the entry, not the pool: the pool is
        // the dynamic white list's business (`screener`), so a market that
        // goes quiet keeps its place and its detect and only stops being
        // entered.
        if !flag("IgnoreVolume") {
            for (what, metric, lo, hi) in [
                ("Volume 24h", Metric::Vol24, "MinVolume", "MaxVolume"),
                (
                    "Volume 1h",
                    Metric::Turnover(60),
                    "MinHourlyVolume",
                    "MaxHourlyVolume",
                ),
            ] {
                if let Some((lo, hi)) = range(lo, hi) {
                    checks.push(Check {
                        what,
                        metric,
                        lo,
                        hi,
                    });
                }
            }
        }
        // Filters / Base: the market's leverage. `MinLeverage` 1 is MoonBot's
        // default and means every market (a leverage is never under 1), and
        // `MaxLeverage` 0 is «not limited», so neither default makes a check.
        // The figure is the account's own highest leverage once the brackets
        // have been read, the instrument's ceiling before (and without an
        // account), which is what the terminal shows for the market too — so a
        // market can change sides when the brackets land, and, as for every
        // filter, a standing entry is not withdrawn for it (`Gate::withdraws`).
        if !flag("IgnoreBase") {
            let (lo, hi) = (num("MinLeverage"), num("MaxLeverage"));
            if lo.is_finite() && hi.is_finite() && (lo > 1.0 || hi > 0.0) {
                checks.push(Check {
                    what: "Leverage",
                    metric: Metric::Leverage,
                    lo: if lo > 1.0 { lo } else { f64::NEG_INFINITY },
                    hi: if hi > 0.0 { hi } else { f64::INFINITY },
                });
            }
        }
        // Filters / Delta, `FilterBy` included: it lives in the same box of
        // the terminal's tab and goes off with the same switch.
        if !flag("IgnoreDelta") {
            for (what, mins, lo, hi) in [
                ("Delta_3h", Some(180), "Delta_3h_Min", "Delta_3h_Max"),
                ("Delta_24h", Some(1440), "Delta_24h_Min", "Delta_24h_Max"),
                (
                    "Delta2",
                    minutes(&text("Delta2_Type")),
                    "Delta2_Min",
                    "Delta2_Max",
                ),
                (
                    "Delta3",
                    minutes(&text("Delta3_Type")),
                    "Delta3_Min",
                    "Delta3_Max",
                ),
            ] {
                match (mins, range(lo, hi)) {
                    (Some(mins), Some((lo, hi))) => checks.push(Check {
                        what,
                        metric: Metric::Delta(mins),
                        lo,
                        hi,
                    }),
                    (None, Some(_)) => {
                        problem =
                            Some("Delta2_Type / Delta3_Type is not understood (1m, 15m, 1h, 24h…)")
                    }
                    _ => {}
                }
            }
            // MoonBot's two market-wide corridors: BTC's hourly delta and the
            // traded markets' mean one.
            for (what, metric, lo_key, hi_key) in [
                (
                    "BTC 1h delta",
                    Metric::BtcDelta,
                    "Delta_BTC_Min",
                    "Delta_BTC_Max",
                ),
                (
                    "Market 1h delta",
                    Metric::MarketDelta,
                    "Delta_Market_Min",
                    "Delta_Market_Max",
                ),
            ] {
                if let Some((lo, hi)) = range(lo_key, hi_key) {
                    checks.push(Check {
                        what,
                        metric,
                        lo,
                        hi,
                    });
                }
            }
            if let Some((key, (lo, hi))) = screener::SortKey::parse(&text("FilterBy"))
                .ok()
                .zip(range("FilterMin", "FilterMax"))
            {
                checks.push(Check {
                    what: "FilterBy",
                    metric: Metric::Key(key),
                    lo,
                    hi,
                });
            }
        }
        if checks.is_empty() && problem.is_none() {
            return None;
        }
        Some(Self {
            checks,
            penalty_ms: secs_ms(seconds(num("GlobalFilterPenalty"))),
            problem,
        })
    }

    /// The first check `idx` fails: the gate to report, which check it was
    /// (`None` for a setting that cannot be judged at all), and whether it
    /// was a market-wide one (BTC, the market) — that one earns
    /// `GlobalFilterPenalty`, because a market outside its corridor is the
    /// strategy's business, not this
    /// market's.
    fn refused(&self, idx: u16, cx: &Ctx) -> Option<(Gate, Option<usize>, bool)> {
        if let Some(why) = self.problem {
            return Some((Gate::Filter { why }, None, false));
        }
        for (i, c) in self.checks.iter().enumerate() {
            match c.judge(idx, cx) {
                Judged::Broken(why) => return Some((Gate::Filter { why }, Some(i), false)),
                Judged::Refused(v) => {
                    let global = matches!(c.metric, Metric::BtcDelta | Metric::MarketDelta);
                    return Some((c.refuse(v), Some(i), global));
                }
                // History shorter than the window (after a start): it waits.
                Judged::Waiting | Judged::Pass(_) => {}
            }
        }
        None
    }

    /// Every check's verdict on one market, in the order they are asked: the
    /// page's column per check.
    ///
    /// The entry gate ([`Self::refused`]) walks the same [`Check::judge`] one
    /// at a time and stops at the first that is neither a pass nor a wait, so
    /// the first such column here IS the reason the entry is refused. It is a
    /// separate walk rather than this list's own first element on purpose:
    /// the gate runs per market per pass and must not allocate for it.
    pub fn judge_all(&self, idx: u16, cx: &Ctx) -> Vec<Judged> {
        self.checks.iter().map(|c| c.judge(idx, cx)).collect()
    }
}

/// Of `pool`, how many markets each filter of `p` is the FIRST to refuse: the
/// same question the pass asks per market (`MoonShot::delta_gate`), asked over
/// the whole pool for the count the log line and the page report.
///
/// `penalized` is `GlobalFilterPenalty` already armed for this strategy. Two
/// states refuse the whole pool at once and belong to no single check — that
/// one, and a setting that cannot be judged — and each is reported under its
/// own name rather than left out: counted as nothing, they would have the
/// page say "nothing filtered" about a strategy that is entering nothing.
///
/// Reading the penalty rather than arming it is the point of taking it as an
/// argument: a counting sweep that armed it would silence the strategy over
/// markets it was only counting.
fn filter_counts(
    p: &Params,
    pool: &[u16],
    cx: &Ctx,
    penalized: bool,
) -> Vec<(&'static str, usize)> {
    let Some(f) = p.deltas.as_ref() else {
        return Vec::new();
    };
    if penalized {
        return vec![("GlobalFilterPenalty", pool.len())];
    }
    if f.problem.is_some() {
        // A short label, not the sentence: this reaches a table column on the
        // page. The sentence itself is already a log line of its own
        // (`StratRt::screen_problem`).
        return vec![("Filters misconfigured", pool.len())];
    }
    let mut counts: Vec<(&'static str, usize)> = f.checks.iter().map(|c| (c.what, 0)).collect();
    for &idx in pool {
        if let Some((_, Some(i), _)) = f.refused(idx, cx) {
            counts[i].1 += 1;
        }
    }
    counts
}

/// The trader's clock, which MoonBot reads its work windows on (the
/// terminal's auto-start window, `WorkingTime`) — the venue's UTC would shift
/// every window by three hours.
const TRADER_UTC_OFFSET_MS: i64 = crate::clock::TRADER_OFFSET_MS;

/// A time window of the day on the trader's clock, minutes since
/// midnight, or of the hour, minutes past it; `from > to` wraps over
/// midnight (or the hour's end).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkWindow {
    Daily {
        from: u32,
        to: u32,
    },
    Hourly {
        from: u32,
        to: u32,
    },
    /// Never (an auto-start window that is not a time of day).
    Closed,
}

impl WorkWindow {
    /// MoonBot's `WorkingTime`: `HH:MM-HH:MM` every day, `MM-MM` every hour,
    /// empty = always.
    pub fn parse(text: &str) -> Result<Option<Self>, String> {
        let t: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        if t.is_empty() {
            return Ok(None);
        }
        let bad = || format!("WorkingTime {text:?}: expected HH:MM-HH:MM or MM-MM");
        let (a, b) = t.split_once('-').ok_or_else(bad)?;
        let hm = |v: &str| -> Option<u32> {
            let (h, m) = v.split_once(':')?;
            let (h, m): (u32, u32) = (h.parse().ok()?, m.parse().ok()?);
            (h < 24 && m < 60).then_some(h * 60 + m)
        };
        let mm = |v: &str| v.parse::<u32>().ok().filter(|&m| m < 60);
        match (a.contains(':'), b.contains(':')) {
            (true, true) => Ok(Some(Self::Daily {
                from: hm(a).ok_or_else(bad)?,
                to: hm(b).ok_or_else(bad)?,
            })),
            (false, false) => Ok(Some(Self::Hourly {
                from: mm(a).ok_or_else(bad)?,
                to: mm(b).ok_or_else(bad)?,
            })),
            _ => Err(bad()),
        }
    }

    /// The auto-start tab's window: day fractions (0.0 = midnight); `None`
    /// when they are not fractions of a day.
    pub fn of_day_fractions(from: f64, to: f64) -> Option<Self> {
        let minute = |f: f64| {
            (f.is_finite() && (0.0..=1.0).contains(&f))
                .then(|| ((f * 1440.0).round() as u32).min(1439))
        };
        Some(Self::Daily {
            from: minute(from)?,
            to: minute(to)?,
        })
    }

    pub fn contains(self, now_ms: i64) -> bool {
        let minute_of_day =
            ((now_ms + TRADER_UTC_OFFSET_MS).rem_euclid(86_400_000) / 60_000) as u32;
        let (at, from, to) = match self {
            Self::Daily { from, to } => (minute_of_day, from, to),
            Self::Hourly { from, to } => (minute_of_day % 60, from, to),
            Self::Closed => return false,
        };
        // `from == to` is the whole day (hour), not one minute of it.
        if from == to {
            true
        } else if from < to {
            at >= from && at <= to
        } else {
            at >= from || at <= to
        }
    }
}

/// `CheckFreeBalance` / `MinFreeBalance`: the account's free USDT
/// (`availableBalance`) as of its last snapshot, less what the entries placed
/// since took (the snapshot already excludes the margin its live orders
/// block). An entry is charged its whole notional, not its margin: the
/// account's leverage per market is read (`Account::leverage`) but not used here,
/// and charging the notional errs on the side of not spending. The emulator is not checked.
#[derive(Debug, Default)]
struct Funds {
    budget: Option<f64>,
    /// The balance was known and is not any more.
    lost: bool,
    /// The core-wide emulator mode.
    emulator: bool,
    /// Strategy × market told it is short of money (logged once).
    short: HashSet<(u64, String)>,
}

impl Funds {
    fn applies(&self, p: &Params, m: &Market) -> bool {
        let _ = m;
        !(p.emulator || self.emulator) && self.budget.is_some()
    }

    fn refusal(&self, p: &Params, m: &Market, size: f64) -> Option<String> {
        let free = self.budget.filter(|_| self.applies(p, m))?;
        if self.lost
            && (p.check_free_balance || (p.kind != Kind::MoonShot && p.min_free_balance > 0.0))
        {
            return Some("the account is unreadable: the free balance is unknown".into());
        }
        if p.kind != Kind::MoonShot && p.min_free_balance > 0.0 && free < p.min_free_balance {
            return Some(format!(
                "free balance {free:.2} USDT < MinFreeBalance {}",
                p.min_free_balance
            ));
        }
        (p.check_free_balance && size > free)
            .then(|| format!("free balance {free:.2} USDT < {size:.2} (CheckFreeBalance)"))
    }

    /// A signal's first tier could be served (its real size at the last
    /// price: a lot dearer than `OrderSize` counts).
    fn can(&self, p: &Params, m: &Market) -> bool {
        let size = p.entry_size(0, m, m.last()).unwrap_or(p.order_size);
        self.refusal(p, m, size).is_none()
    }

    /// An entry of `size` USDT may go out: spends it; else logs once why not.
    /// Nothing refunds a cancelled or refused entry: the budget is the
    /// exchange's word (`set_free_balance`), read again after any order
    /// report (the account reader's woken read), and until then it errs on
    /// the side of not spending.
    fn take(
        &mut self,
        s: &StrategySnapshot,
        p: &Params,
        m: &Market,
        size: f64,
        cmds: &mut Vec<Cmd>,
    ) -> bool {
        let key = (s.strategy_id, m.symbol.clone());
        if let Some(why) = self.refusal(p, m, size) {
            if self.short.insert(key) {
                cmds.push(Cmd::Log(format!(
                    "{}: entry not placed: {why} (strategy <{}>)",
                    m.symbol,
                    label(s)
                )));
            }
            return false;
        }
        self.short.remove(&key);
        if self.applies(p, m) {
            if let Some(free) = self.budget.as_mut() {
                *free -= size;
            }
        }
        true
    }
}

/// One entry of a signal ladder (`label`: the strategy kind in the log); none
/// without a price or a free `MaxActiveOrders` slot.
#[allow(clippy::too_many_arguments)]
fn start_tier(
    s: &StrategySnapshot,
    p: &Params,
    m: &Market,
    kind: &str,
    short: bool,
    tier: usize,
    price: f64,
    planned_sell: f64,
    slots: &mut i64,
    funds: &mut Funds,
    cmds: &mut Vec<Cmd>,
) {
    if *slots <= 0 || price <= 0.0 {
        return;
    }
    let Some(size) = p.entry_size(tier, m, price) else {
        cmds.push(over_lot_log(s, p, m, tier, price));
        return;
    };
    if !funds.take(s, p, m, size, cmds) {
        return;
    }
    *slots -= 1;
    cmds.push(Cmd::Start {
        tier,
        order: StartOrder {
            market: m.symbol.clone(),
            is_short: short,
            use_market_stop: false,
            strategy_id: s.strategy_id,
            size,
            price,
            planned_sell,
            stops: None,
        },
    });
    cmds.push(Cmd::Log(format!(
        "Starting new {kind} market: {} (strategy <{}>) tier {tier}: price {price}; size {size}",
        m.symbol,
        label(s)
    )));
}

/// The smallest order the exchange takes at `price`, USDT: whole steps, at
/// least one, and at least `MIN_NOTIONAL` — a tier sized under the floor would
/// be refused with the whole budget already spent on it.
fn smallest_order(m: &Market, price: f64) -> f64 {
    let lot = m.lot_value(price);
    if lot <= 0.0 {
        return lot;
    }
    // Whole lots that clear both the notional floor and `LOT_SIZE.minQty` (a market can
    // have `minQty` above one step).
    let for_notional = (m.min_notional / lot - 1e-9).ceil();
    let for_qty = if m.step_size > 0.0 {
        (m.min_qty / m.step_size - 1e-9).ceil()
    } else {
        0.0
    };
    for_notional.max(for_qty).max(1.0) * lot
}

/// The entry of `tier` skipped: the smallest order at `price` is past
/// `LOT_OVER_BUDGET` budgets of the tier.
fn over_lot_log(s: &StrategySnapshot, p: &Params, m: &Market, tier: usize, price: f64) -> Cmd {
    Cmd::Log(format!(
        "{}: the smallest order {:.2} USDT exceeds the budget {:.2} USDT (strategy <{}>) tier {tier}: no entry",
        m.symbol,
        smallest_order(m, price),
        p.tier_size(tier),
        label(s)
    ))
}

/// The entry of `tier` at `price` past what `PERCENT_PRICE` lets its side
/// carry is not placed: a BUY above `mark × multiplierUp` (`limit`), a SELL
/// below `mark × multiplierDown`.
fn past_band_log(
    s: &StrategySnapshot,
    m: &Market,
    kind: &str,
    short: bool,
    tier: usize,
    price: f64,
) -> Cmd {
    let (down, up) = m.band().unwrap_or_default();
    let (side, rule, limit) = if short {
        ("SELL", "below", down)
    } else {
        ("BUY", "above", up)
    };
    Cmd::Log(format!(
        "{}: {kind} tier {tier} {side} price {price} is {rule} the exchange limit {limit} \
         (strategy <{}>): no entry",
        m.symbol,
        label(s)
    ))
}

/// A signal entry at `price` that its side may carry, else 0 and a log line.
fn band_entry(
    s: &StrategySnapshot,
    m: &Market,
    kind: &str,
    short: bool,
    tier: usize,
    price: f64,
    cmds: &mut Vec<Cmd>,
) -> f64 {
    let inside = m.inside_limits(price, !short);
    if inside <= 0.0 && price > 0.0 {
        cmds.push(past_band_log(s, m, kind, short, tier, price));
    }
    inside
}

/// `v` within `[min, max]`; a bound of 0 is no bound (MoonBot's filters).
pub(crate) fn within(v: f64, min: f64, max: f64) -> bool {
    v >= min && (max <= 0.0 || v <= max)
}

/// Corridor bounds of tier 0 with the delta shift: high/low ranges of the market
/// (15 m / 1 h / 3 h) and of BTC (1 h / 5 m) times
/// their coefficients move both bounds; the far bound `MShotAddDistance` %
/// more (TMB `effParams`, MoonBot FAQ).
fn eff_params(p: &Params, idx: u16, last: f64, cx: &Ctx) -> (f64, f64) {
    let (win, now) = (cx.win, cx.now);
    let mut shift = 0.0;
    if p.add_3h != 0.0 || p.add_1h != 0.0 || p.add_15m != 0.0 {
        shift += win.range(idx, last, now, 180) * p.add_3h
            + win.range(idx, last, now, 60) * p.add_1h
            + win.range(idx, last, now, 15) * p.add_15m;
    }
    if p.add_btc != 0.0 || p.add_btc_5m != 0.0 {
        if let Some(ix) = cx.btc {
            let live = cx.model.at(ix).map_or(0.0, |m| m.last());
            shift += win.range(ix, live, now, 60) * p.add_btc
                + win.range(ix, live, now, 5) * p.add_btc_5m;
        }
    }
    let eff = (p.price + shift * (1.0 + p.add_distance / 100.0)).max(0.0);
    (eff, (p.price_min + shift).min(eff).max(0.0))
}

/// Far-side hysteresis reconstructed from the 12–15 September v7.70 logs.
/// The extra distance cannot exceed the near distance: (11,10) -> 12,
/// (0.30,0.10) -> 0.40, (0.90,0.85) -> 0.95. The closed implementation
/// is unavailable; these observed regimes, not universal parity, are tested.
fn far_distance(distance: f64, near: f64) -> f64 {
    distance + (distance - near).max(0.0).min(near.max(0.0))
}

/// Geometric corridor factor of tier `i`: `(1 + MShotExpand/100)^i`.
fn factor(expand: f64, i: usize) -> f64 {
    if expand == 0.0 {
        1.0
    } else {
        (1.0 + expand / 100.0).powi(i as i32)
    }
}

/// Distance of an entry from the market, % (positive on the entry's side).
fn gap(short: bool, last: f64, price: f64) -> f64 {
    if last <= 0.0 {
        return 0.0;
    }
    if short {
        (price - last) / last * 100.0
    } else {
        (last - price) / last * 100.0
    }
}

fn entry_price(short: bool, last: f64, dist: f64) -> f64 {
    if short {
        last * (1.0 + dist / 100.0)
    } else {
        last * (1.0 - dist / 100.0)
    }
}

/// Whether the order is still inside its failure back-off.
fn backoff(memo: &mut Memo, now: i64) -> bool {
    if memo.fails == 0 {
        return false;
    }
    if now - memo.fail_at >= 2 * RETRY_MAX_MS {
        memo.fails = 0;
        return false;
    }
    let wait = (RETRY_BASE_MS << (memo.fails.min(6) - 1)).min(RETRY_MAX_MS);
    now - memo.fail_at < wait
}

/// Filled entry (whole or part): the market belongs to this position now.
fn is_committed(o: &CoreOrder) -> bool {
    o.holds_position()
}

/// Closed trade that lost money (mean exit against mean entry, fees aside).
fn is_loss(o: &CoreOrder) -> bool {
    // A position that left the account unseen was booked at the core's guess of the price: no
    // penalty is started on a guess.
    if o.exit_source() == crate::reports::ExitSource::Outside {
        return false;
    }
    let rec = o.record();
    let (entry, exit) = (rec.buy.mean_price, rec.sell.mean_price);
    entry > 0.0
        && exit > 0.0
        && if o.is_short {
            exit > entry
        } else {
            exit < entry
        }
}

/// A time setting in seconds: finite and not negative (else 0 = off).
fn seconds(v: f64) -> f64 {
    if v.is_finite() {
        v.max(0.0)
    } else {
        0.0
    }
}

fn secs_ms(s: f64) -> i64 {
    // Capped at ten years: a setting is text a person typed, `as i64` saturates, and the sums
    // this feeds (`now + …`) must not wrap.
    if s.is_nan() {
        return 0;
    }
    (s.min(315_360_000.0) * 1000.0) as i64
}

/// The strategy's own name, or `#id` when it has none — the terminal's own
/// fallback, and what the log lines and the web page show.
pub(crate) fn label(s: &StrategySnapshot) -> String {
    match s.fields.get_string("StrategyName") {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => format!("#{}", s.strategy_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No previous pool: the plain top-`DynWL_Count`, which is what a first
    /// recompute sees and what these tests mean unless they say otherwise
    /// (`screener::DynList::head` gives a market already in the pool a hold
    /// on the boundary, and an empty `prev` has none to give).
    fn none() -> HashSet<u16> {
        HashSet::new()
    }

    /// The schema's `Dyn_Refresh`, in ms: a pass this far apart lands in the
    /// next grid cell and recomputes the pool.
    const REFRESH_MS: i64 = screener::REFRESH_DEFAULT_S as i64 * 1_000;
    use crate::drops;
    use crate::model::fixtures;
    use crate::strike;
    use crate::trading::{ExecStatus, OrderUpdate};
    use moonproto::server::codec::strat::{self as strat_codec, Snapshot};
    use moonproto::StrategyKind;

    /// TInvestCore's test model on Aster's catalog: `BTCUSDT` (0), SBER (1)
    /// and SGAZP (2) with a 0.01 tick and lots of 10, STAR (3) like the
    /// Binance market of the 12.09 log: tick 0.00001, lot 1. Keyed so they
    /// sort in that order (`u1-sber` …); the terminal names them `SBER` ….
    fn model() -> Model {
        let mut m = fixtures::sber_catalog_of(&[
            ("SBER", "SBER", 0.01, 10.0),
            ("SGAZP", "SGAZP", 0.01, 10.0),
            ("STAR", "STAR", 0.00001, 1.0),
        ]);
        m.at_mut(1).unwrap().last_price = Some(300.0);
        m.at_mut(2).unwrap().last_price = Some(120.0);
        m.at_mut(3).unwrap().last_price = Some(0.0614);
        // BTCUSDT stands where TInvestCore's service market stood, and like it
        // is nobody's candidate: Aster's «top» alone, not a coin of the class.
        m.at_mut(0).unwrap().tags = vec![crate::model::Tag::Top];
        // STAR's tests size entries of 1 USDT, under Aster's 5 USDT floor:
        // they replay MoonBot's prices, which the size does not move.
        m.at_mut(3).unwrap().min_notional = 0.0;
        m
    }

    fn strategies(fields: &[(&str, FieldValue)], checked: bool) -> Strategies {
        strategies_of(StrategyKind::MOON_SHOT, fields, checked)
    }

    fn strategies_of(
        kind: StrategyKind,
        fields: &[(&str, FieldValue)],
        checked: bool,
    ) -> Strategies {
        let mut st = Strategies::new(None, 0);
        let mut f = StrategyFields::new();
        f.insert("StrategyName", FieldValue::String("shot".into()));
        // TInvestCore's tests ran on MoonBot's default, real orders; this
        // core's schema defaults to the emulator, so they say it.
        f.insert("EmulatorMode", FieldValue::Bool(false));
        for (k, v) in fields {
            f.insert(*k, v.clone());
        }
        let s = StrategySnapshot::new(7, 1, 10, checked, kind, "", f);
        st.apply_snapshot(&Snapshot {
            server_epoch: 1,
            client_max_last_date: 10,
            full: true,
            data: strat_codec::encode_batch(st.schema(), &[s], std::iter::empty()),
            folders_last_modified: 1,
        });
        st.set_running(true);
        st
    }

    /// A refusal for margin keeps the strategy off the market 10 s — margin
    /// frees the moment an order goes — any other refusal a minute, since it
    /// repeats until someone changes something.
    #[test]
    fn a_margin_refusal_cools_down_shorter_than_the_rest() {
        assert_eq!(
            reject_cooldown_ms("api 400/-2019: Margin is insufficient."),
            MARGIN_COOLDOWN_MS
        );
        assert_eq!(reject_cooldown_ms("api 400/-4051: x"), MARGIN_COOLDOWN_MS);
        assert_eq!(
            reject_cooldown_ms("api 400/-4164: Order's notional must be no smaller than 5"),
            REJECT_COOLDOWN_MS
        );
        assert_eq!(reject_cooldown_ms("order rejected"), REJECT_COOLDOWN_MS);
    }

    /// The exchange's rate limit halts entries as long as it says, and a
    /// later, shorter signal never cuts a halt in force.
    #[test]
    fn error_budget_halts_entries_without_shortening() {
        let mut shot = MoonShot::default();
        shot.on_failed(7, "api 429/-1003: Too many requests.", 1_000);
        assert_eq!(shot.rate_halt_until, 1_000 + RATE_HALT_MS);
        shot.rate_halt_until = 200_000;
        shot.on_failed(7, "api 400/-1015: Too many new orders.", 2_000);
        assert_eq!(shot.rate_halt_until, 200_000);
        // A ban names its length, and it is waited out to the end.
        let mut shot = MoonShot::default();
        shot.on_failed(
            7,
            "api 418/-1003: Way too many requests. (retry after 600 s)",
            1_000,
        );
        assert_eq!(shot.rate_halt_until, 601_000);
        let mut shot = MoonShot::default();
        shot.on_failed(7, "api 418/-1003: banned", 1_000);
        assert_eq!(shot.rate_halt_until, 1_000 + BAN_HALT_MS);
        let mut shot = MoonShot::default();
        shot.on_failed(7, "api 400/-1015: Too many new orders.", 1_000);
        assert_eq!(shot.rate_halt_until, 1_000 + ORDER_BURST_HALT_MS);
        let mut shot = MoonShot::default();
        shot.on_failed(7, "api 400/-2019: Margin is insufficient.", 1_000);
        assert_eq!(shot.rate_halt_until, 0, "a refusal is not a rate limit");
    }

    /// An exit move cancels the live exit `old`: its final report posts the
    /// remainder, which the exchange acknowledges as `new`.
    fn settle_exit_move(orders: &mut Orders, old: &str, new: &str, lots: i64, now: i64) {
        use crate::orders::Action;
        let mut done = report("", old, ExecStatus::Cancelled, lots, 0, 0.0);
        done.unary = true;
        let fx = orders.apply(&done, now);
        let key = fx
            .actions
            .iter()
            .find_map(|a| match a {
                Action::Post { key, .. } => Some(key.clone()),
                _ => None,
            })
            .expect("exit reposted");
        orders.apply(&report(&key, new, ExecStatus::New, lots, 0, 0.0), now);
    }

    fn report(key: &str, ex: &str, st: ExecStatus, req: i64, done: i64, avg: f64) -> OrderUpdate {
        OrderUpdate {
            exchange_id: ex.into(),
            request_id: key.into(),
            uid: "SBER".into(),
            status: st,
            sell: false,
            is_market: false,
            lots_requested: req,
            lots_executed: done,
            price: 0.0,
            avg_price: avg,
            unary: true,
            time_ms: 0,
            message: String::new(),
        }
    }

    /// Runs `cmds` through `Orders` like the engine; returns idempotency keys
    /// of posted legs in order. An entry replace is acknowledged at once under
    /// the same exchange id, so the tests keep naming orders by their first
    /// id. An exit move only cancels: `settle_exit_move` completes it.
    fn apply(
        shot: &mut MoonShot,
        orders: &mut Orders,
        model: &Model,
        cmds: &[Cmd],
        now: i64,
    ) -> Vec<String> {
        use crate::orders::Action;
        let mut keys = Vec::new();
        for c in cmds {
            let fx = match c {
                Cmd::Start { order, tier } => {
                    let idx = model.index_of_symbol(&order.market).unwrap();
                    let fx = orders.start(0, order, model.at(idx).unwrap(), now);
                    if let Some(&id) = fx.changed.first() {
                        shot.on_entry_started(id, *tier);
                    }
                    fx
                }
                Cmd::Move {
                    order,
                    leg,
                    price,
                    reason,
                    ..
                } => {
                    if *leg == Leg::Sell {
                        orders.set_sell_reason(*order, *reason);
                    }
                    orders.target(*order, *leg, *price, None)
                }
                Cmd::MoveEntry {
                    order,
                    price,
                    size,
                    planned,
                } => {
                    let fx = orders.target_entry(*order, *price, *size);
                    orders.replan_exit(*order, *price, *planned);
                    fx
                }
                Cmd::Cancel { order } => orders.cancel_buy(*order, now),
                Cmd::Stop {
                    order,
                    price,
                    spread,
                } => orders.set_bot_stop(*order, *price, *spread),
                Cmd::AdoptStop { order } => orders.adopt_bot_stop(*order),
                _ => continue,
            };
            for a in fx.actions {
                match a {
                    Action::Post { key, .. } => keys.push(key),
                    Action::Replace {
                        key,
                        exchange_id,
                        lots,
                        ..
                    } => {
                        orders.apply(
                            &report(&key, &exchange_id, ExecStatus::New, lots, 0, 0.0),
                            now,
                        );
                    }
                    _ => {}
                }
            }
        }
        keys
    }

    fn starts(cmds: &[Cmd]) -> Vec<(f64, f64)> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Start { order, .. } => Some((order.price, order.size)),
                _ => None,
            })
            .collect()
    }

    fn moves(cmds: &[Cmd]) -> Vec<(u64, Leg, f64)> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Move {
                    order, leg, price, ..
                } => Some((*order, *leg, *price)),
                Cmd::MoveEntry { order, price, .. } => Some((*order, Leg::Buy, *price)),
                _ => None,
            })
            .collect()
    }

    fn cancels(cmds: &[Cmd]) -> Vec<u64> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Cancel { order } => Some(*order),
                _ => None,
            })
            .collect()
    }

    /// A pending order waits in the core with its strategy's id and no
    /// exchange order. Deleting the strategy takes it off like a resting
    /// entry, or the trigger would open a position under a strategy that no
    /// longer exists; a listed strategy's pending order stays armed.
    #[test]
    fn a_pending_order_of_a_deleted_strategy_is_cancelled() {
        use FieldValue::String as Str;
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let st = strategies(&[("CoinsWhiteList", Str("SBER".into()))], true);
        let mut orders = Orders::new();
        let pending = |strategy_id| StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id,
            size: 3000.0,
            price: 290.0,
            planned_sell: 0.0,
            stops: None,
        };
        let t = 1_000_000;
        let gone = orders
            .start_pending(0, &pending(99), model.at(1).unwrap(), t)
            .changed[0];
        let kept = orders
            .start_pending(0, &pending(7), model.at(1).unwrap(), t)
            .changed[0];
        assert!(orders.get(gone).unwrap().is_pending() && orders.get(kept).unwrap().is_pending());
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1000);
        assert_eq!(cancels(&cmds), [gone]);
    }

    fn has_log(cmds: &[Cmd], text: &str) -> bool {
        cmds.iter()
            .any(|c| matches!(c, Cmd::Log(l) if l.contains(text)))
    }

    fn check_auto_cancel_ladder(short: bool, cancellation_order: [usize; 3]) {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let mut model = model();
        let m = model.at_mut(1).unwrap();
        m.last_price = Some(75_000.0);
        // TInvestCore's lot of one contract at 0.008469 RUB a point: a step
        // worth the same 635 the ladder was sized in.
        (m.step_size, m.min_qty) = (0.008469, 0.008469);
        m.quantity_precision = 6;
        m.tick_size = 0.1;
        m.price_precision = 1;
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("Short", Bool(short)),
                ("OrderSize", Double(2000.0)),
                ("OrdersCount", Int32(3)),
                ("OrderSizeStep", Double(0.0)),
                ("BuyPriceStep", Double(0.3)),
                ("MShotPrice", Double(0.6)),
                ("MShotPriceMin", Double(0.5)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let mut now = 1_000_000;
        let first = shot.tick(&st, &orders, &model, &win, sched, now);
        let initial = starts(&first);
        let keys = apply(&mut shot, &mut orders, &model, &first, now);
        for (i, key) in keys.iter().enumerate() {
            let mut update = report(key, &format!("{}", 800_000 + i), ExecStatus::New, 3, 0, 0.0);
            update.sell = short;
            orders.apply(&update, now + i as i64);
        }
        now += 90_000;
        let cancel = shot.tick(&st, &orders, &model, &win, sched, now);
        assert_eq!(cancels(&cancel).len(), 3);
        apply(&mut shot, &mut orders, &model, &cancel, now);
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, now + 50)).is_empty());
        let mut renewed = Vec::new();
        // The sequential worker and exchange acknowledge one cancellation at a time.
        // Run the next strategy pass after each report, as CoreHandler does.
        for (i, cancelled) in cancellation_order.into_iter().enumerate() {
            now += 100;
            let mut update = report(
                &keys[cancelled],
                &format!("{}", 800_000 + cancelled),
                ExecStatus::Cancelled,
                3,
                0,
                0.0,
            );
            update.sell = short;
            orders.apply(&update, now);
            let commands = shot.tick(&st, &orders, &model, &win, sched, now);
            assert_eq!(starts(&commands), [initial[cancelled]]);
            renewed.extend(starts(&commands));
            let new_keys = apply(&mut shot, &mut orders, &model, &commands, now);
            for (j, key) in new_keys.iter().enumerate() {
                let mut update = report(
                    key,
                    &format!("{}", 810_000 + i * 10 + j),
                    ExecStatus::New,
                    3,
                    0,
                    0.0,
                );
                update.sell = short;
                orders.apply(&update, now);
            }
        }
        println!("short={short}; initial={initial:?}; renewed={renewed:?}");
        let mut expected: Vec<_> = initial.iter().map(|(price, _)| *price).collect();
        let mut actual: Vec<_> = orders
            .iter()
            .filter(|o| o.status == status::BUY_SET)
            .map(|o| o.record().buy.price)
            .collect();
        expected.sort_by(f64::total_cmp);
        actual.sort_by(f64::total_cmp);
        assert_eq!(
            actual, expected,
            "auto-cancel at an unchanged price must preserve the three ladder levels"
        );
    }

    #[test]
    fn review_auto_cancel_long_ladder_keeps_distinct_tiers() {
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            check_auto_cancel_ladder(false, order);
        }
    }

    #[test]
    fn review_auto_cancel_short_ladder_keeps_distinct_tiers() {
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            check_auto_cancel_ladder(true, order);
        }
    }

    fn stepped_ladder(count: i32) -> Strategies {
        use FieldValue::{Double, Int32, String as Str};
        strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(6000.0)),
                ("OrdersCount", Int32(count)),
                ("OrderSizeStep", Double(50.0)),
                ("BuyPriceStep", Double(0.3)),
                ("MShotPrice", Double(0.6)),
                ("MShotPriceMin", Double(0.5)),
                ("AutoCancelBuy", Double(0.0)),
                ("MShotReplaceDelay", Double(0.0)),
            ],
            true,
        )
    }

    #[test]
    fn missing_middle_tier_preserves_its_budget_and_other_orders() {
        let model = model();
        let st = stepped_ladder(3);
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let now = 1_000_000;
        let commands = shot.tick(&st, &orders, &model, &win, sched, now);
        let initial = starts(&commands);
        let keys = apply(&mut shot, &mut orders, &model, &commands, now);
        for (i, key) in keys.iter().enumerate() {
            orders.apply(
                &report(
                    key,
                    &format!("{}", 899_999 + i),
                    ExecStatus::New,
                    i as i64 + 2,
                    0,
                    0.0,
                ),
                now,
            );
        }
        orders.apply(
            &report(&keys[1], "900000", ExecStatus::Cancelled, 3, 0, 0.0),
            now + 1,
        );
        let commands = shot.tick(&st, &orders, &model, &win, sched, now + 2);
        assert_eq!(starts(&commands), [initial[1]]);
        assert_eq!(initial[1], (297.3, 9000.0));
        assert!(moves(&commands).is_empty());
        assert!(cancels(&commands).is_empty());
        apply(&mut shot, &mut orders, &model, &commands, now + 2);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, now + 3)
            .is_empty());
    }

    #[test]
    fn entries_past_the_band_wait_until_it_opens() {
        // A BUY is bounded from above only (`mark × multiplierUp`): the ladder's
        // tiers above the ceiling wait, the ones under it go.
        let mut model = model();
        model.at_mut(1).unwrap().set_limits(250.0, 296.0, 0);
        let st = stepped_ladder(3);
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let now = 1_000_000;
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, now)).is_empty());
        model.at_mut(1).unwrap().set_limits(250.0, 297.5, 0);
        assert_eq!(
            starts(&shot.tick(&st, &orders, &model, &win, sched, now + 1000)),
            [(297.3, 9000.0), (296.4, 12000.0)]
        );
        model.at_mut(1).unwrap().set_limits(250.0, 350.0, 0);
        assert_eq!(
            starts(&shot.tick(&st, &orders, &model, &win, sched, now + 2000)),
            [(298.2, 6000.0), (297.3, 9000.0), (296.4, 12000.0)]
        );
    }

    #[test]
    fn changing_order_count_waits_for_the_removed_tier_cancellation() {
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let now = 1_000_000;
        let commands = shot.tick(&stepped_ladder(3), &orders, &model, &win, sched, now);
        let keys = apply(&mut shot, &mut orders, &model, &commands, now);
        for (i, key) in keys.iter().enumerate() {
            orders.apply(
                &report(
                    key,
                    &format!("{}", 899_999 + i),
                    ExecStatus::New,
                    i as i64 + 2,
                    0,
                    0.0,
                ),
                now,
            );
        }
        let far = orders
            .iter()
            .find(|o| o.record().buy.price == 296.4)
            .unwrap()
            .id;
        let commands = shot.tick(&stepped_ladder(2), &orders, &model, &win, sched, now + 1);
        assert_eq!(cancels(&commands), [far]);
        apply(&mut shot, &mut orders, &model, &commands, now + 1);
        assert!(
            starts(&shot.tick(&stepped_ladder(3), &orders, &model, &win, sched, now + 2))
                .is_empty()
        );
        orders.apply(
            &report(&keys[2], "900001", ExecStatus::Cancelled, 4, 0, 0.0),
            now + 3,
        );
        let commands = shot.tick(&stepped_ladder(3), &orders, &model, &win, sched, now + 4);
        assert_eq!(starts(&commands), [(296.4, 12000.0)]);
    }

    #[test]
    fn params_take_schema_defaults() {
        let st = strategies(&[("MShotPrice", FieldValue::Double(1.5))], true);
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert_eq!((p.price, p.price_min, p.orders_count), (1.5, 0.6, 1));
        // Dollars, not MoonBot's 1000 of roubles: a strategy nobody tuned must not size a
        // thousand-dollar entry.
        assert_eq!(p.order_size, 10.0);
        // The default lives in the schema, shared by every kind: no kind reads a thousand.
        assert!(matches!(
            st.schema().field("OrderSize").and_then(|f| f.default_value.clone()),
            Some(FieldValue::Double(v)) if v == 10.0
        ));
        assert_eq!(
            (p.stops.level, p.max_active, p.sell_price),
            (Some(-2.0), 5, 2.0)
        );
        assert_eq!(
            (p.auto_cancel, p.replace_delay, p.add_distance, p.penalty),
            (90.0, 0.1, 0.0, 30.0)
        );
        assert!(p.stops.spread == 0.4 && (p.stops.stop_spread() - 0.004).abs() < 1e-12);
        assert!(p.white.is_empty() && !p.short && p.max_ping == 0 && !p.pd_relative);
        assert!(!p.repeat && p.repeat_wait == 5.0 && p.stops.delay == 0.0);
        // A file written before the rest of the `Stops` section keeps
        // trading as it did: every added switch is off, the spread does not
        // grow and the chase may go to −50 %.
        let c = &p.stops;
        assert!(!c.fast && !c.market && !c.fixed && c.ema == 0 && c.add_1m == 0.0);
        assert!(
            c.second.is_none() && c.third.is_none() && c.trailing.is_none() && c.bvsv.is_none()
        );
        assert_eq!(c.allowed_drop, -50.0);
        // A file written before the dynamic lists reads them off, on
        // MoonBot's own defaults (`Dyn_Refresh=61`, `Last2hDelta`, `YES`, 0).
        assert_eq!(
            (p.dyn_refresh, p.dyn_wl.count, p.dyn_bl.count),
            (61.0, 0, 0)
        );
        assert_eq!(p.dyn_wl.by, Ok(screener::SortKey::Last2h));
        assert_eq!(p.dyn_bl.by, Ok(screener::SortKey::Last2h));
        assert!(p.dyn_wl.desc && p.dyn_bl.desc);
    }

    /// ETH_short of the 15.09 log: SellPrice 1 % is `entry / 1.01`, and
    /// PriceDownRelative 40 % walks the rounded price towards the entry down
    /// to `AllowedDrop` 0.045 %.
    #[test]
    fn short_exit_divides_and_price_down_steps_from_the_rounded_price() {
        use FieldValue::{Bool, Double};
        let model = model();
        let m = model.at(1).unwrap();
        let st = strategies(
            &[
                ("SellPrice", Double(1.0)),
                ("PriceDownPercent", Double(40.0)),
                ("PriceDownRelative", Bool(true)),
                ("PriceDownAllowedDrop", Double(0.045)),
            ],
            true,
        );
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        let entry = 2394.48;
        assert_eq!(m.nearest(exit_price(true, entry, 1.0)), 2370.77);
        assert_eq!(m.nearest(exit_price(true, 2399.57, 1.0)), 2375.81);
        let floor = m.nearest(exit_price(true, entry, 0.045));
        assert_eq!(floor, 2393.40);
        let steps: Vec<f64> = (1..=7)
            .map(|k| price_down(&p, m, true, entry, p.sell_price, k, floor))
            .collect();
        assert_eq!(
            steps,
            [2380.25, 2385.94, 2389.36, 2391.41, 2392.64, 2393.38, 2393.40]
        );
        // The long keeps its multiplication.
        assert_eq!(m.nearest(exit_price(false, 296.01, 2.0)), 301.93);
    }

    /// `TotalLoss` holds every entry of the strategy; a minus session only
    /// its market, and it withdraws the entries standing there.
    #[test]
    fn total_loss_and_minus_session_hold_entries() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
                ("TotalLoss", Double(100.0)),
                ("IgnoreSession", Bool(false)),
                ("SessionStratMin", Double(-50.0)),
                ("SessionPenaltyTime", Double(60.0)),
            ],
            true,
        );
        let id = st.list()[0].strategy_id;
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert_eq!(p.total_loss, 100.0);
        assert_eq!(
            p.session.map(|r| (r.min, r.penalty_ms)),
            Some((-50.0, 60_000))
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t = 1_000_000;

        // Lost 150 over the window: no ladder.
        shot.set_guards(guards::View {
            totals: HashMap::from([(id, -150.0)]),
            ..guards::View::default()
        });
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        // 50 only: the ladder goes out.
        shot.set_guards(guards::View {
            totals: HashMap::from([(id, -50.0)]),
            ..guards::View::default()
        });
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(starts(&cmds).len(), 1);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t);
        // A minus session on SBER withdraws it.
        shot.set_guards(guards::View {
            sessions: HashMap::from([((id, "SBER".to_string()), t + 60_000)]),
            ..guards::View::default()
        });
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1_000);
        assert_eq!(cancels(&cmds).len(), 1);
        assert!(has_log(&cmds, "Minus session on SBER"));
        // Once it ends, the market is open again.
        shot.set_guards(guards::View::default());
        assert!(cancels(&shot.tick(&st, &orders, &model, &win, sched, t + 2_000)).is_empty());
    }

    /// `CancelBuyAfterSell`: a sale of the strategy on the market withdraws
    /// its entries there placed before the sale; off, they stay.
    #[test]
    fn cancel_buy_after_sell_withdraws_the_rest_of_the_grid() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        for on in [true, false] {
            let st = strategies(
                &[
                    ("CoinsWhiteList", Str("SBER".into())),
                    ("OrderSize", Double(3000.0)),
                    ("MShotPrice", Double(1.0)),
                    ("MShotPriceMin", Double(0.5)),
                    ("MShotAdd15minDelta", Double(0.0)),
                    ("MShotAddHourlyDelta", Double(0.0)),
                    ("OrdersCount", Int32(1)),
                    ("CancelBuyAfterSell", Bool(on)),
                ],
                true,
            );
            let id = st.list()[0].strategy_id;
            let (win, sched) = (Windows::default(), None::<f64>);
            let sber = model.at(1).unwrap();
            let mut orders = Orders::new();
            let entry = |price| StartOrder {
                market: "SBER".into(),
                is_short: false,
                use_market_stop: false,
                strategy_id: id,
                size: 3000.0,
                price,
                planned_sell: 310.0,
                stops: None,
            };
            let key_of = |fx: &crate::orders::Effects| match &fx.actions[0] {
                crate::orders::Action::Post { key, .. } => key.clone(),
                other => panic!("{other:?}"),
            };
            // B rests at 297; A fills at 296 and sells at 310.
            let fx = orders.start(0, &entry(297.0), sber, 1_000_000);
            let b = fx.changed[0];
            orders.apply(
                &report(&key_of(&fx), "900007", ExecStatus::New, 1, 0, 0.0),
                1_000_001,
            );
            let fx = orders.start(0, &entry(296.0), sber, 1_000_000);
            let fx = orders.apply(
                &report(&key_of(&fx), "900006", ExecStatus::Filled, 1, 1, 296.0),
                1_002_000,
            );
            let mut sold = report(&key_of(&fx), "900008", ExecStatus::Filled, 1, 1, 310.0);
            sold.sell = true;
            orders.apply(&sold, 1_003_000);
            let mut shot = MoonShot::default();
            let cmds = shot.tick(&st, &orders, &model, &win, sched, 1_004_000);
            assert_eq!(cancels(&cmds).contains(&b), on, "{cmds:?}");
            assert_eq!(has_log(&cmds, "CancelBuyAfterSell: 1 entries"), on);
        }
    }

    /// A Manual strategy never enters and never gates: the trader's entry
    /// stays even while the strategies are stopped.
    #[test]
    fn manual_strategy_leaves_the_traders_entries_alone() {
        use FieldValue::Double;
        let model = model();
        // Unchecked: a MoonShot's gate (Stopped) would withdraw the entry.
        let st = strategies_of(StrategyKind::MANUAL, &[("SellPrice", Double(2.0))], false);
        let id = st.list()[0].strategy_id;
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert_eq!(p.kind, Kind::Manual);
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let entry = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: id,
            size: 3000.0,
            price: 290.0,
            planned_sell: 0.0,
            stops: None,
        };
        let t = 1_000_000;
        let fx = orders.start(0, &entry, model.at(1).unwrap(), t);
        let key = match &fx.actions[0] {
            crate::orders::Action::Post { key, .. } => key.clone(),
            other => panic!("{other:?}"),
        };
        orders.apply(&report(&key, "910001", ExecStatus::New, 1, 0, 0.0), t);
        let cmds = MoonShot::default().tick(&st, &orders, &model, &win, sched, t + 60_000);
        assert!(
            cancels(&cmds).is_empty() && starts(&cmds).is_empty(),
            "{cmds:?}"
        );
    }

    /// Filters / Delta: the 3 h delta must lie in [0.5, 1000] %: a falling
    /// market gets no ladder, a rising one does; IgnoreFilters turns it off;
    /// MOEX and market ranges intersect on the index.
    #[test]
    fn delta_filters_gate_the_ladder() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let sched = None::<f64>;
        let t = 50_000_000_000;
        let run = |open: f64, extra: &[(&str, FieldValue)]| {
            let mut win = Windows::default();
            win.push(1, t - 179 * 60_000, open, 10.0);
            win.push(1, t - 1_000, 300.0, 10.0);
            let mut f = vec![
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MShotAdd3hDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
                ("Delta_3h_Min", Double(0.5)),
                ("Delta_3h_Max", Double(1000.0)),
            ];
            f.extend(extra.iter().cloned());
            let st = strategies(&f, true);
            starts(&MoonShot::default().tick(&st, &Orders::new(), &model, &win, sched, t)).len()
        };
        assert_eq!(run(310.0, &[]), 0, "fell 3.2 % in 3 h");
        assert_eq!(run(290.0, &[]), 1, "rose 3.4 % in 3 h");
        assert_eq!(run(310.0, &[("IgnoreFilters", Bool(true))]), 1);
        assert_eq!(run(310.0, &[("IgnoreDelta", Bool(true))]), 1);
        // A type that cannot be read keeps the strategy out.
        assert_eq!(
            run(
                290.0,
                &[
                    ("Delta2_Type", Str("2d".into())),
                    ("Delta2_Min", Double(1.0))
                ]
            ),
            0
        );
        let f = DeltaFilters::read(
            &|n| match n {
                "Delta_BTC_Min" => -10.0,
                "Delta_BTC_Max" => 10.0,
                "Delta_Market_Min" => -2.0,
                "Delta_Market_Max" => 20.0,
                "GlobalFilterPenalty" => 60.0,
                _ => 0.0,
            },
            &|_| String::new(),
            &|_| false,
        )
        .unwrap();
        // BTC's corridor and the market's are two checks, in that order, and
        // nothing else was set: no volume bound, no `FilterBy`.
        assert_eq!(
            f.checks,
            [
                Check {
                    what: "BTC 1h delta",
                    metric: Metric::BtcDelta,
                    lo: -10.0,
                    hi: 10.0,
                },
                Check {
                    what: "Market 1h delta",
                    metric: Metric::MarketDelta,
                    lo: -2.0,
                    hi: 20.0,
                }
            ]
        );
        assert_eq!(f.penalty_ms, 60_000);
    }

    #[test]
    fn working_time_parses_and_wraps() {
        // On the trader's clock: 10:30 there is 07:30 UTC.
        let at = |h: i64, m: i64| (h * 60 + m) * 60_000 - TRADER_UTC_OFFSET_MS + 86_400_000;
        let w = WorkWindow::parse("10:30 - 16:45").unwrap().unwrap();
        assert!(w.contains(at(10, 30)) && w.contains(at(16, 45)));
        assert!(!w.contains(at(10, 29)) && !w.contains(at(16, 46)));
        let night = WorkWindow::parse("22:00-02:00").unwrap().unwrap();
        assert!(
            night.contains(at(23, 0)) && night.contains(at(1, 0)) && !night.contains(at(12, 0))
        );
        let hourly = WorkWindow::parse("05-35").unwrap().unwrap();
        assert!(hourly.contains(at(13, 20)) && !hourly.contains(at(13, 40)));
        assert_eq!(WorkWindow::parse(""), Ok(None));
        assert!(WorkWindow::parse("10:30").is_err() && WorkWindow::parse("25:00-26:00").is_err());
        assert!(WorkWindow::parse("10:00-35").is_err());
        let day = WorkWindow::of_day_fractions(0.0, 0.9999).unwrap();
        assert!(day.contains(at(23, 58)));
        assert_eq!(WorkWindow::of_day_fractions(f64::NAN, 0.5), None);
        assert!(!WorkWindow::Closed.contains(at(12, 0)));
        // from == to: the whole day.
        assert!(WorkWindow::parse("10:00-10:00")
            .unwrap()
            .unwrap()
            .contains(at(3, 0)));
    }

    /// Outside `WorkingTime`, with a `WorkingTime` not understood, or before
    /// `PreventWorkingUntil`: no ladder; inside: the ladder goes out.
    #[test]
    fn working_time_and_prevent_until_gate_the_ladder() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        // 12:00 on the trader's clock, some day.
        let t = 20_000 * 86_400_000 + 12 * 3_600_000 - TRADER_UTC_OFFSET_MS;
        let run = |extra: &[(&str, FieldValue)]| {
            let mut f = vec![
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
            ];
            f.extend(extra.iter().cloned());
            let st = strategies(&f, true);
            starts(&MoonShot::default().tick(&st, &Orders::new(), &model, &win, sched, t)).len()
        };
        assert_eq!(run(&[("WorkingTime", Str("10:00-14:00".into()))]), 1);
        assert_eq!(run(&[("WorkingTime", Str("14:00-18:00".into()))]), 0);
        assert_eq!(run(&[("WorkingTime", Str("ten to two".into()))]), 0);
        let until = (t / 1000 + 60) as f64;
        assert_eq!(run(&[("PreventWorkingUntil", Double(until))]), 0);
        assert_eq!(run(&[("PreventWorkingUntil", Double(until - 120.0))]), 1);
    }

    /// Exposure limits: `MaxOrdersPerMarket` caps the ladder, `CheckFreeBalance`
    /// drops the tiers the free money does not cover, `MinFreeBalance` does
    /// not apply to MoonShot, `MaxPosition` holds a market whose position
    /// cost that much.
    #[test]
    fn exposure_limits_cap_the_ladder() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let base = [
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("MShotPrice", Double(1.0)),
            ("MShotPriceMin", Double(0.5)),
            ("MShotAdd15minDelta", Double(0.0)),
            ("MShotAddHourlyDelta", Double(0.0)),
            ("OrdersCount", Int32(2)),
            ("BuyPriceStep", Double(-1.0)),
        ];
        let with = |extra: &[(&str, FieldValue)]| {
            let mut f = base.to_vec();
            f.extend(extra.iter().cloned());
            strategies(&f, true)
        };
        let t = 1_000_000;
        let run = |st: &Strategies, free: Option<f64>, orders: &Orders| {
            let mut shot = MoonShot::default();
            shot.set_free_balance(free);
            shot.tick(st, orders, &model, &win, sched, t)
        };
        let none = Orders::new();
        assert_eq!(starts(&run(&with(&[]), None, &none)).len(), 2);
        let st = with(&[("MaxOrdersPerMarket", Int32(1))]);
        assert_eq!(starts(&run(&st, None, &none)).len(), 1);
        // 4000 free: the 3000 tier fits, the 3750 one does not.
        let st = with(&[("CheckFreeBalance", Bool(true))]);
        let cmds = run(&st, Some(4000.0), &none);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        assert!(has_log(
            &cmds,
            "entry not placed: free balance 1000.00 USDT < 3750.00"
        ));
        // Unknown balance: nothing is dropped; MinFreeBalance spares MoonShot.
        assert_eq!(starts(&run(&st, None, &none)).len(), 2);
        let st = with(&[("MinFreeBalance", Double(10_000.0))]);
        assert_eq!(starts(&run(&st, Some(4000.0), &none)).len(), 2);
        // A manual position of 10 × 300 = 3000 USDT on SBER: over MaxPosition 2000.
        let st = with(&[("MaxPosition", Double(2000.0))]);
        let mut orders = Orders::new();
        let manual = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 3000.0,
            price: 300.0,
            planned_sell: 0.0,
            stops: None,
        };
        let fx = orders.start(0, &manual, model.at(1).unwrap(), t - 60_000);
        let key = match &fx.actions[0] {
            crate::orders::Action::Post { key, .. } => key.clone(),
            other => panic!("{other:?}"),
        };
        orders.apply(
            &report(&key, "910003", ExecStatus::Filled, 1, 1, 300.0),
            t - 59_000,
        );
        assert!(starts(&run(&st, None, &orders)).is_empty());
        assert_eq!(starts(&run(&with(&[]), None, &orders)).len(), 2);
    }

    /// The price the startup ticker seeded is not one to enter at: the ladder waits for the
    /// market's first live trade, then goes.
    #[test]
    fn a_seeded_price_holds_the_ladder_until_a_live_trade() {
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        model.at_mut(1).unwrap().price_seeded = true;
        let (win, sched) = (Windows::default(), None::<f64>);
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
            ],
            true,
        );
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let held = shot.tick(&st, &orders, &model, &win, sched, 1_000_000);
        assert!(starts(&held).is_empty());
        model.at_mut(1).unwrap().price_seeded = false;
        let mut shot = MoonShot::default();
        assert_eq!(
            starts(&shot.tick(&st, &orders, &model, &win, sched, 1_001_000)).len(),
            1
        );
    }

    /// The account went unreadable after a balance was known: the entries that check the
    /// balance wait, instead of running unchecked as when nothing was ever read; the ones that
    /// do not check go on, and a fresh figure releases the waiting ones.
    #[test]
    fn a_lost_balance_holds_the_checked_entries_and_a_fresh_one_frees_them() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut base = vec![
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("MShotPrice", Double(1.0)),
            ("MShotPriceMin", Double(0.5)),
            ("MShotAdd15minDelta", Double(0.0)),
            ("MShotAddHourlyDelta", Double(0.0)),
            ("OrdersCount", Int32(1)),
        ];
        let unchecked = strategies(&base, true);
        base.push(("CheckFreeBalance", Bool(true)));
        let checked = strategies(&base, true);
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let mut t = 1_000_000;
        let mut run = |shot: &mut MoonShot, st: &Strategies| {
            t += 1_000;
            shot.tick(st, &orders, &model, &win, sched, t)
        };
        shot.set_free_balance(Some(4000.0));
        assert_eq!(starts(&run(&mut shot, &checked)).len(), 1);
        shot.set_free_balance(None);
        let held = run(&mut shot, &checked);
        assert!(starts(&held).is_empty());
        assert!(has_log(&held, "the account is unreadable"));
        assert_eq!(starts(&run(&mut shot, &unchecked)).len(), 1);
        shot.set_free_balance(Some(4000.0));
        assert_eq!(starts(&run(&mut shot, &checked)).len(), 1);
    }

    /// `PenaltyTime` after a manual order on the market, and
    /// `GlobalDetectPenalty` after another strategy's detect there: no ladder.
    #[test]
    fn manual_order_and_another_detect_hold_the_ladder() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
                ("PenaltyTime", Double(60.0)),
                ("GlobalDetectPenalty", Double(30.0)),
            ],
            true,
        );
        let id = st.list()[0].strategy_id;
        let (win, sched) = (Windows::default(), None::<f64>);
        let t = 1_000_000;
        // Another strategy's ladder 10 s ago: held; our own detect is not.
        let mut shot = MoonShot::default();
        shot.market_detect.insert(1, (t - 10_000, id + 1));
        assert!(starts(&shot.tick(&st, &Orders::new(), &model, &win, sched, t)).is_empty());
        shot.market_detect.insert(1, (t - 10_000, id));
        assert_eq!(
            starts(&shot.tick(&st, &Orders::new(), &model, &win, sched, t)).len(),
            1
        );
        // A manual order on SBER 20 s ago: no ladder for PenaltyTime (60 s).
        let mut orders = Orders::new();
        let manual = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 3000.0,
            price: 290.0,
            planned_sell: 0.0,
            stops: None,
        };
        orders.start(0, &manual, model.at(1).unwrap(), t - 20_000);
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert!(starts(&cmds).is_empty());
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 41_000);
        assert_eq!(starts(&cmds).len(), 1);
    }

    /// The terminal's global black list takes the market out of every
    /// universe at once: its entries come off, none go out while listed.
    #[test]
    fn global_black_list_takes_the_market_out() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(1)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t = 1_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t);
        // Listed within the same Dyn_Refresh period: re-screened at once.
        shot.set_black_list(HashSet::from(["SBER".to_string()]));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1_000);
        assert_eq!(cancels(&cmds).len(), 1);
        assert!(starts(&cmds).is_empty());
        // Another market listed: SBER stays in the universe.
        let mut fresh = MoonShot::default();
        fresh.set_black_list(HashSet::from(["SGAZP".to_string()]));
        let cmds = fresh.tick(&st, &Orders::new(), &model, &win, sched, t);
        assert_eq!(starts(&cmds).len(), 1);
        // Typed as the trader types it, in lower case: still banned.
        let mut typed = MoonShot::default();
        typed.set_black_list(HashSet::from(["sber".to_string()]));
        let cmds = typed.tick(&st, &Orders::new(), &model, &win, sched, t);
        assert!(starts(&cmds).is_empty());
    }

    #[test]
    fn ladder_corridor_exit_price_down_stop_and_restart() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER, XXXX".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MShotRaiseWait", Double(5.0)),
                ("MShotReplaceDelay", Double(0.0)),
                ("AutoCancelBuy", Double(0.0)),
                ("OrdersCount", Int32(2)),
                ("BuyPriceStep", Double(-1.0)),
                ("SellPrice", Double(2.0)),
                ("PriceDownTimer", Double(10.0)),
                ("PriceDownDelay", Double(5.0)),
                ("PriceDownPercent", Double(0.5)),
                ("PriceDownAllowedDrop", Double(0.5)),
                ("StopLoss", Double(-3.0)),
                ("TradePenaltyTime", Double(10.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let mut model = model;
        let mut t = 1_000_000;

        // Two tiers at `Price`: 1 % and 2 % below 300.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(starts(&cmds), [(297.0, 3000.0), (294.0, 3750.0)]);
        assert!(has_log(
            &cmds,
            "Starting new MoonShot market: SBER (strategy <shot>)"
        ));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        let ids: Vec<u64> = {
            let mut v: Vec<u64> = orders.iter().map(|o| o.id).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(ids.len(), 2);
        for (k, ex) in keys.iter().zip(["900000", "900001"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t);
        }
        // Inside the corridors ([0.5, 1.5] and [1.5, 2.5]): nothing to do.
        t += 1000;
        assert!(shot.tick(&st, &orders, &model, &win, sched, t).is_empty());

        // Market up: entries too far, raised only after RaiseWait (5 s) from
        // the lowest price seen meanwhile (306 → 305 → 306: 305).
        model.at_mut(1).unwrap().last_price = Some(306.0);
        t += 1000;
        assert!(moves(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        model.at_mut(1).unwrap().last_price = Some(305.0);
        t += 2000;
        assert!(moves(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        model.at_mut(1).unwrap().last_price = Some(306.0);
        t += 3000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        let mv = moves(&cmds);
        assert_eq!(mv.len(), 2);
        assert_eq!((mv[0].1, mv[0].2, mv[1].2), (Leg::Buy, 301.95, 298.9));
        assert!(has_log(
            &cmds,
            "replacing UP on cur. price: 306 Min. Ask: 305"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t);
        // Market down through the corridor floor: moved at once (ReplaceDelay 0).
        model.at_mut(1).unwrap().last_price = Some(299.0);
        t += 1000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        let mv = moves(&cmds);
        assert_eq!(mv.len(), 2);
        assert_eq!((mv[0].2, mv[1].2), (296.01, 293.02));
        assert!(has_log(
            &cmds,
            "replacing DOWN on cur. price: 299 delay: 0ms;"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t);

        // Tier 0 fills: the other entry is cancelled, the exit goes 2 % above
        // the fill (nearest tick), the terminal gets a detect.
        let tier0 = mv[0].0;
        let tier1 = mv[1].0;
        assert_eq!(orders.get(tier0).unwrap().status, status::BUY_SET);
        orders.apply(&report("", "900000", ExecStatus::Filled, 1, 1, 296.01), t);
        t += 1000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(cancels(&cmds), [tier1]);
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Cmd::Detect { strategy_id: 7, .. })));
        assert!(has_log(
            &cmds,
            "Buy order DONE! FILL: 100% Quantity: 10 Avg.Price: 296.01"
        ));
        assert_eq!(moves(&cmds), [(tier0, Leg::Sell, 301.93)]);
        assert!(has_log(
            &cmds,
            "Using (strategy <shot>) SELL price: 2.00% = 301.93"
        ));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        let reason_of = |orders: &Orders| orders.get(tier0).unwrap().record().sell_reason;
        assert_eq!(orders.get(tier0).unwrap().status, status::SELL_SET);
        assert_eq!(reason_of(&orders), reason::SELL_PRICE);
        orders.apply(&report(&keys[0], "900002", ExecStatus::New, 1, 0, 0.0), t);
        // Cancel is not repeated within 5 s.
        t += 1000;
        assert!(shot.tick(&st, &orders, &model, &win, sched, t).is_empty());
        orders.apply(&report("", "900001", ExecStatus::Cancelled, 1, 0, 0.0), t);

        // PriceDown: after 10 s the exit drops 0.5 % of itself, after 15 s
        // again: 301.93 → 300.42 → 298.92.
        t += 10_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(moves(&cmds), [(tier0, Leg::Sell, 300.42)]);
        assert!(has_log(
            &cmds,
            "Auto Sell Replacing (PriceDown: 301.93 => Perc: 0.5 AllowedDrop: 297.49 NewP: 300.42)"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t);
        settle_exit_move(&mut orders, "900002", "900003", 1, t);
        t += 5000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(moves(&cmds), [(tier0, Leg::Sell, 298.92)]);
        apply(&mut shot, &mut orders, &model, &cmds, t);
        settle_exit_move(&mut orders, "900003", "900004", 1, t);
        assert_eq!(reason_of(&orders), reason::AUTO_PRICE_DOWN);

        // Stop at -3 % (287.13): a break must hold 2 s, then a limit
        // `StopLossSpread` (0.4 %) through the last price.
        model.at_mut(1).unwrap().last_price = Some(287.0);
        t += 1000;
        assert!(moves(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        t += 2000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(moves(&cmds), [(tier0, Leg::Sell, 285.85)]);
        assert!(has_log(
            &cmds,
            "StopLoss AutoActivated on price drop: BID = 287.00 LastPrice = 287.00 BuyPrice = 296.01; \
             StopLoss fixed: 287.13 spread: 0.40% => 285.85"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t);
        settle_exit_move(&mut orders, "900004", "900005", 1, t);
        assert_eq!(reason_of(&orders), reason::STOP_LOSS);
        // The close fills at a loss: TradePenaltyTime (10 s) keeps the market
        // quiet, then a fresh ladder goes out.
        orders.apply(&report("", "900005", ExecStatus::Filled, 1, 1, 285.85), t);
        model.at_mut(1).unwrap().last_price = Some(288.0);
        t += 1000;
        assert!(shot.tick(&st, &orders, &model, &win, sched, t).is_empty());
        t += 9000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(starts(&cmds).len(), 2);
        assert_eq!(starts(&cmds)[0].0, 285.12);

        // An entry refused for margin keeps the market quiet for
        // `MARGIN_COOLDOWN_MS`, and no longer.
        apply(&mut shot, &mut orders, &model, &cmds, t);
        let failed = orders
            .iter()
            .find(|o| o.status == status::BUY_SET)
            .unwrap()
            .id;
        let margin = "api 400/-2019: Margin is insufficient.";
        orders.fail(failed, Leg::Buy, crate::orders::Op::Post, margin, t);
        shot.on_failed(failed, margin, t);
        let refused = t;
        t += 1000;
        assert!(shot.tick(&st, &orders, &model, &win, sched, t).is_empty());
        t = refused + MARGIN_COOLDOWN_MS - 1;
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        t = refused + MARGIN_COOLDOWN_MS;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert!(!starts(&cmds).is_empty(), "{cmds:?}");
    }

    /// LONG8-like settings of the 13.09 log: `MShotReplaceDelay` 1 s waits
    /// and re-places from the lowest price of the wait; `AutoCancelBuy`
    /// cancels an old entry and the next pass starts a new one.
    fn drops(extra: &[(&str, FieldValue)]) -> Strategies {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let mut fields = vec![
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("DropsMaxTime", Int32(10)),
            ("DropsPriceMA", Int32(2)),
            ("DropsLastPriceMA", Int32(1)),
            ("DropsPriceDelta", Double(1.0)),
            ("buyPrice", Double(-0.5)),
            ("buyPriceLastTrade", Bool(true)),
            ("OrdersCount", Int32(2)),
            ("BuyPriceStep", Double(-1.0)),
            ("AutoCancelBuy", Double(20.0)),
            ("NextDetectPenalty", Double(30.0)),
            ("SellPrice", Double(2.0)),
            ("UseStopLoss", Bool(false)),
        ];
        fields.extend(extra.iter().cloned());
        strategies_of(StrategyKind::DROPS, &fields, true)
    }

    /// Ticks every 2 s at `prices` (one tape sample each) from `t`; returns
    /// the commands of all passes and the time after them.
    fn drops_ticks(
        shot: &mut MoonShot,
        st: &Strategies,
        orders: &Orders,
        model: &mut Model,
        win: &Windows,
        prices: &[f64],
        mut t: i64,
    ) -> (Vec<Cmd>, i64) {
        let sched = None::<f64>;
        let mut cmds = Vec::new();
        for &p in prices {
            model.at_mut(1).unwrap().last_price = Some(p);
            cmds.extend(shot.tick(st, orders, model, win, sched, t));
            t += drops::SAMPLE_MS;
        }
        (cmds, t)
    }

    /// A 1.35 % drop inside the 10 s window puts out both tiers from the
    /// last trade at once; they stay after a fill (each fill gets its own
    /// exit) until `AutoCancelBuy`; no new signal while orders live.
    #[test]
    fn drops_detect_ladder_exit_and_auto_cancel() {
        let st = drops(&[]);
        let (mut model, win) = (model(), Windows::default());
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let start = 1_000_000;
        // Four flat samples do not span the window yet; no signal.
        let (cmds, t) = drops_ticks(
            &mut shot,
            &st,
            &orders,
            &mut model,
            &win,
            &[300.0; 4],
            start,
        );
        assert!(starts(&cmds).is_empty());
        let (cmds, mut t) = drops_ticks(&mut shot, &st, &orders, &mut model, &win, &[296.0], t);
        assert_eq!(starts(&cmds), [(294.52, 3000.0), (291.56, 3750.0)]);
        assert!(cmds.iter().any(
            |c| matches!(c, Cmd::Detect { strategy_id: 7, is_short: false, msg, .. } if msg == "drop 1.35%")
        ));
        assert!(has_log(&cmds, "SBER: DropsDetection: drop 1.35% in 10 s"));
        assert!(has_log(&cmds, "Starting new Drops market: SBER"));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        for (k, ex) in keys.iter().zip(["900000", "900001"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t);
        }
        let placed = t;
        // The drop still holds: no second signal while the entries live.
        t += 1000;
        let cmds = shot.tick(&st, &orders, &model, &win, None, t);
        assert!(starts(&cmds).is_empty());

        // Tier 0 fills: its exit goes out 2 % above the fill, tier 1 stays.
        orders.apply(&report("", "900000", ExecStatus::Filled, 1, 1, 294.52), t);
        t += 1000;
        let cmds = shot.tick(&st, &orders, &model, &win, None, t);
        let tier0 = orders
            .iter()
            .find(|o| o.status == status::BUY_DONE)
            .unwrap()
            .id;
        assert!(cancels(&cmds).is_empty());
        assert_eq!(moves(&cmds), [(tier0, Leg::Sell, 300.41)]);
        apply(&mut shot, &mut orders, &model, &cmds, t);

        // AutoCancelBuy 20 s after placement lifts tier 1 — not after a
        // restart before the market's first trade.
        let tier1 = orders
            .iter()
            .find(|o| o.status == status::BUY_SET)
            .unwrap()
            .id;
        let mut restarted = MoonShot::default();
        restarted.restore(&orders, placed + 20_000);
        model.at_mut(1).unwrap().last_price = Some(0.0);
        let cmds = restarted.tick(&st, &orders, &model, &win, None, placed + 20_000);
        assert!(cancels(&cmds).is_empty());
        model.at_mut(1).unwrap().last_price = Some(296.0);
        t = placed + 19_000;
        let cmds = shot.tick(&st, &orders, &model, &win, None, t);
        assert!(cancels(&cmds).is_empty());
        t = placed + 20_000;
        let cmds = shot.tick(&st, &orders, &model, &win, None, t);
        assert_eq!(cancels(&cmds), [tier1]);
        assert!(has_log(
            &cmds,
            "SBER: Auto cancel buy order activated since 20 sec."
        ));
    }

    /// `DropsPriceIsLow` wants the price at its hourly low; `NextDetectPenalty`
    /// holds the next signal after one whose entries are gone.
    #[test]
    fn drops_hourly_low_and_next_detect_penalty() {
        let mut model = model();
        let start = 1_000_000;
        let mut win = Windows::default();
        win.push(1, start - 30 * 60_000, 290.0, 10.0);
        let st = drops(&[("DropsPriceIsLow", FieldValue::Bool(true))]);
        let mut shot = MoonShot::default();
        let orders = Orders::new();
        let (cmds, _) = drops_ticks(
            &mut shot,
            &st,
            &orders,
            &mut model,
            &win,
            &[300.0, 300.0, 300.0, 300.0, 296.0],
            start,
        );
        assert!(starts(&cmds).is_empty(), "296 is above the hourly low 290");

        let mut model = self::model();
        let win = Windows::default();
        let st = drops(&[]);
        let mut shot = MoonShot::default();
        let (cmds, t) = drops_ticks(
            &mut shot,
            &st,
            &orders,
            &mut model,
            &win,
            &[300.0, 300.0, 300.0, 300.0, 296.0],
            start,
        );
        assert_eq!(starts(&cmds).len(), 2);
        // The entries were never placed (no orders kept): the drop holds, but
        // the next signal waits 30 s from the first.
        let (cmds, t) = drops_ticks(&mut shot, &st, &orders, &mut model, &win, &[296.0; 10], t);
        assert!(starts(&cmds).is_empty());
        let late = start + 8_000 + 30_000;
        assert!(t < late);
        let (cmds, _) = drops_ticks(
            &mut shot,
            &st,
            &orders,
            &mut model,
            &win,
            &[300.0, 300.0, 300.0, 300.0, 296.0],
            late,
        );
        assert_eq!(starts(&cmds).len(), 2);
    }

    fn strike(extra: &[(&str, FieldValue)]) -> Strategies {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let mut fields = vec![
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("MStrikeDepth", Double(1.0)),
            ("MStrikeBuyLevel", Double(-2.0)),
            ("MStrikeBuyRelative", Bool(false)),
            ("MStrikeSellLevel", Double(80.0)),
            ("OrdersCount", Int32(1)),
            ("AutoCancelBuy", Double(10.0)),
            ("NextDetectPenalty", Double(4.0)),
            ("UseStopLoss", Bool(false)),
        ];
        fields.extend(extra.iter().cloned());
        strategies_of(StrategyKind::MOON_STRIKE, &fields, true)
    }

    /// Three 2 s ticks at bid 300 / ask 300.1 from `t`: both EMAs settle.
    fn strike_ticks(shot: &mut MoonShot, st: &Strategies, model: &mut Model, t: i64) {
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask, m.last_price) = (Some(300.0), Some(300.1), Some(300.0));
        for k in 0..3 {
            let cmds = shot.tick(
                st,
                &Orders::new(),
                model,
                &Windows::default(),
                None,
                t + k * strike::TICK_MS,
            );
            assert!(starts(&cmds).is_empty());
        }
    }

    /// MoonBot 7.71 log 19.09, ZAMA (tick 0.00001 like STAR): ST_30 long at
    /// 21:20:33 — reference 0.085945, strike low 0.085080, BuyLevel −30 →
    /// take 0.060161, depth 30 %, sell +24 % = 0.074600; ST_25 short at
    /// 21:17:50 — reference 0.084228, strike high 0.085070, BuyLevel −25 →
    /// take 0.11230 (÷ 0.75), depth 33.3 %, sell +26.7 % = 0.088661
    /// (÷ 1.2667).
    ///
    /// Each side takes its reference from its own side of the book: the long
    /// from the bid, the short from the ask (24.09). MoonBot's line labels
    /// both «LastBID», which on a Binance book a hundredth of a percent wide
    /// cannot tell the two apart — and the one thing it does settle, the
    /// arithmetic from the reference to the take and the exit, is what this
    /// replays and what stays identical.
    #[test]
    fn strike_prices_replay_the_moonbot_log() {
        use FieldValue::{Double, String as Str};
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let t = 1_000_000;
        let now = t + 2 * strike::TICK_MS + 100;
        let run = |level: f64, bid: f64, ask: f64, trade: f64| {
            let st = strike(&[
                ("CoinsWhiteList", Str("STAR".into())),
                ("MStrikeDirection", Str("Both".into())),
                ("MStrikeBuyLevel", Double(level)),
                ("OrderSize", Double(10.0)),
            ]);
            let mut model = model();
            let mut shot = MoonShot::default();
            let m = model.at_mut(3).unwrap();
            (m.bid, m.ask, m.last_price) = (Some(bid), Some(ask), Some(bid));
            for k in 0..3 {
                shot.tick(&st, &orders, &model, &win, sched, t + k * strike::TICK_MS);
            }
            assert!(shot.on_trade(3, now, trade, 20_000.0, true));
            shot.tick(&st, &orders, &model, &win, sched, now + 10)
        };
        // A tick-wide book: the long's reference is the bid it quotes.
        let long = run(-30.0, 0.085945, 0.085955, 0.085080);
        assert_eq!(planned(&long), [(false, 0.06016, 0.0746)]);
        // The log shows the strike's own low, the take beside it.
        assert!(has_log(
            &long,
            "min.Price: 0.08508 (take 0.06016) Depth: 30.0%"
        ));
        // 0.085070 / 0.084228 is 0.9997 %: the log rounds the reference to
        // six digits; a trade a tick higher clears the 1 % bar the same way.
        let short = run(-25.0, 0.084218, 0.084228, 0.085080);
        assert_eq!(planned(&short), [(true, 0.1123, 0.08866)]);
        assert!(has_log(&short, "SHORT LastASK: 0.08423"));
        // Past −100 % a long takes nothing: no order, no line.
        let none = run(-120.0, 0.085945, 0.085955, 0.085080);
        assert!(starts(&none).is_empty() && !has_log(&none, "(take"));
    }

    /// MoonBot 7.71 log 19.09, MON (513): SELL 0.30 % of 0.025210 = 0.025290,
    /// PriceDown 0.05 % steps from the placed exit: 0.025290 → p 0.025277 →
    /// 0.025280 → p 0.025267 → 0.025270; the floor (AllowedDrop 0.025221)
    /// caps it.
    #[test]
    fn price_down_steps_from_the_placed_exit_as_the_moonbot_log() {
        let model = model();
        let star = model.at(3).unwrap();
        let mut p = Params::from_snapshot(
            &strategies(&[], true).list()[0],
            strategies(&[], true).schema(),
        );
        (p.pd_pct, p.pd_relative) = (0.05, false);
        let (entry, floor) = (0.02521, star.nearest(0.025221));
        let steps: Vec<f64> = (1..=4)
            .map(|k| price_down(&p, star, false, entry, 0.30, k, floor))
            .collect();
        assert_eq!(steps, [0.02528, 0.02527, 0.02526, 0.02525]);
        assert_eq!(price_down(&p, star, false, entry, 0.30, 40, floor), 0.02522);
    }

    fn planned(cmds: &[Cmd]) -> Vec<(bool, f64, f64)> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Start { order, .. } => Some((order.is_short, order.price, order.planned_sell)),
                _ => None,
            })
            .collect()
    }

    /// Every field a hook detect needs except `HookPriceDistance`, which the
    /// two helpers below either pin or leave to the schema.
    fn hook_fields() -> Vec<(&'static str, FieldValue)> {
        use FieldValue::{Bool, Double, Int32, String as Str};
        vec![
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("HookTimeFrame", Double(4.0)),
            ("HookDetectDepth", Double(1.0)),
            ("HookAntiPump", Bool(false)),
            ("HookPriceRollBack", Double(20.0)),
            ("HookInitialPrice", Double(10.0)),
            ("HookSellLevel", Double(80.0)),
            ("HookSellFixed", Bool(true)),
            ("OrdersCount", Int32(1)),
            ("AutoCancelBuy", Double(0.0)),
            ("NextDetectPenalty", Double(0.0)),
            ("UseStopLoss", Bool(false)),
        ]
    }

    /// A hook with its corridor pinned off: every test that predates the
    /// schema default reads a standing entry, and one that wants a corridor
    /// overrides the value from `extra` (the later pair wins).
    fn hook(extra: &[(&str, FieldValue)]) -> Strategies {
        let mut fields = hook_fields();
        fields.push(("HookPriceDistance", FieldValue::Double(0.0)));
        fields.extend(extra.iter().cloned());
        strategies_of(StrategyKind::MOON_HOOK, &fields, true)
    }

    /// A hook with no `HookPriceDistance` field at all — what the terminal
    /// saves when the operator leaves the box alone, and what the strategy
    /// file then holds.
    fn hook_bare(extra: &[(&str, FieldValue)]) -> Strategies {
        let mut fields = hook_fields();
        fields.extend(extra.iter().cloned());
        strategies_of(StrategyKind::MOON_HOOK, &fields, true)
    }

    /// One fall on SBER: flat 300 for a second, 1 % down to 297, a fifth of
    /// it back to 297.60. The first pass starts the tape, so a detect taken
    /// at `t + 5000` has a whole 4 s frame behind it.
    fn hook_fall(shot: &mut MoonShot, st: &Strategies, model: &mut Model, t: i64) {
        let (win, sched) = (Windows::default(), None::<f64>);
        shot.tick(st, &Orders::new(), model, &win, sched, t);
        for (at, price, turnover) in [
            (3_000, 300.0, 100_000.0),
            (3_500, 300.0, 100_000.0),
            (4_100, 297.0, 200_000.0),
            (4_600, 297.6, 50_000.0),
        ] {
            shot.on_trade(1, t + at, price, turnover, true);
            model.at_mut(1).unwrap().last_price = Some(price);
        }
    }

    /// The detect puts tier 0 at `HookInitialPrice` (10 %) of the move above
    /// its low and plans the exit `HookSellLevel` (80 %) of the way from that
    /// entry back to where the move started; the fill gets it, not `SellPrice`.
    #[test]
    fn hook_detect_ladder_and_planned_exit() {
        let st = hook(&[]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let mut orders = Orders::new();
        let t = 1_000_000;
        hook_fall(&mut shot, &st, &mut model, t);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 5_000);
        // 297 + 10 % of 3.00 = 297.30, and the exit is `HookSellLevel` (80 %)
        // of the way from there back to where the move started: 80 % of
        // (300 − 297.30) / 297.30 = 0.7265 % → 299.46. MoonBot 7.71, AKE
        // 20.09 13:14:29.955 (`HOOK fixedNO ip30`, level 80): High 0.090355,
        // entry 0.089716, logged `SellPrice: 0.57%` = 80 % of 0.7123 %.
        assert_eq!(planned(&cmds), [(false, 297.3, 299.46)]);
        assert_eq!(starts(&cmds), [(297.3, 3000.0)]);
        assert!(has_log(
            &cmds,
            "SBER: MoonHook max.Price: 300.00 min.Price: 297.00 Depth: 1.01% RollBack: 20% \
             HookDrop: 0% Vol: 450000 USDT"
        ));
        assert!(cmds
            .iter()
            .any(|c| matches!(c, Cmd::Detect { is_short: false, msg, .. } if msg == "hook 1.01%")));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 5_000);
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
            t + 5_010,
        );
        orders.apply(
            &report("", "900000", ExecStatus::Filled, 1, 1, 297.3),
            t + 5_020,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 6_000);
        let o = orders.iter().next().unwrap();
        assert_eq!(o.status, status::SELL_SET, "Orders placed the planned exit");
        assert_eq!(o.record().sell.price, 299.46);
        assert!(moves(&cmds).is_empty(), "no SellPrice exit on top");
    }

    /// The unhappy branches of the same fall: a rollback that has not held
    /// `HookRollBackWait`, one outside `HookPriceRollBackMax`, a move under
    /// `HookDetectDepth` and one over `HookDetectDepthMax` all pass silently.
    #[test]
    fn hook_detect_needs_depth_and_a_rollback_that_held() {
        use FieldValue::{Double, Int32};
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let t = 1_000_000;
        let run = |st: &Strategies| {
            let mut model = model();
            let mut shot = MoonShot::default();
            hook_fall(&mut shot, st, &mut model, t);
            shot.tick(st, &orders, &model, &win, sched, t + 5_000)
        };
        assert_eq!(starts(&run(&hook(&[]))).len(), 1, "the reference fall");
        assert!(
            starts(&run(&hook(&[("HookRollBackWait", Int32(1_000))]))).is_empty(),
            "297.60 has held 900 ms, not a second"
        );
        assert!(
            starts(&run(&hook(&[("HookPriceRollBack", Double(25.0))]))).is_empty(),
            "the price came back a fifth, not a quarter"
        );
        assert!(
            starts(&run(&hook(&[("HookPriceRollBackMax", Double(10.0))]))).is_empty(),
            "a rollback past the cap"
        );
        assert!(
            starts(&run(&hook(&[("HookDetectDepth", Double(1.5))]))).is_empty(),
            "a 1 % move is not deep enough"
        );
        assert!(
            starts(&run(&hook(&[("HookDetectDepthMax", Double(0.5))]))).is_empty(),
            "a move past the depth cap"
        );
        assert!(
            starts(&run(&hook(&[("HookDetectMinVolume", Double(500_000.0))]))).is_empty(),
            "the frame traded 450 000 USDT"
        );
        assert!(
            starts(&run(&hook(&[("HookDropMin", Double(50.0))]))).is_empty(),
            "nothing fell before the move"
        );
        // `HookInitialPrice` past the rollback prices the entry on the wrong
        // side of the market: MoonBot takes no detect then.
        let cmds = run(&hook(&[("HookInitialPrice", Double(30.0))]));
        assert!(starts(&cmds).is_empty());
        assert!(has_log(
            &cmds,
            "SBER: MoonHook: order 297.90 is past the price 297.60, no detect"
        ));
    }

    /// A hook that never mentions `HookPriceDistance` follows the price:
    /// the schema default is what a terminal box left alone means, and a
    /// missing line in the strategy file is that default rather than a lost
    /// value. This is the whole guard on the default — the helpers above pin
    /// the field, so nothing else here would notice it going back to 0, and
    /// with 0 the silence and the setting are indistinguishable.
    #[test]
    fn a_hook_that_never_mentions_the_corridor_still_follows_the_price() {
        let bare = hook_bare(&[]);
        assert_eq!(
            Params::from_snapshot(&bare.list()[0], bare.schema()).hook_distance,
            15.0,
            "the schema default reaches the engine through `field`'s fallback"
        );
        let moves = |st: &Strategies| {
            let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
            let mut shot = MoonShot::default();
            let mut orders = Orders::new();
            let t = 1_000_000;
            hook_fall(&mut shot, st, &mut model, t);
            let cmds = shot.tick(st, &orders, &model, &win, sched, t + 5_000);
            let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 5_000);
            orders.apply(
                &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
                t + 5_010,
            );
            // Tier 0 rests at 297.30 (`HookInitialPrice` 10 % of the 3.00
            // fall), 0.1008 % under the 297.60 anchor, and the band is that
            // ∓ 1.0101·15/100 = 0.1515 pp. 305.00 is 2.52 % off — outside
            // every reading of that band, so this pins the follow, not the
            // width (`hook_corridor_follows_the_price_and_replans_the_exit`
            // pins the width).
            model.at_mut(1).unwrap().last_price = Some(305.0);
            shot.tick(st, &orders, &model, &win, sched, t + 6_000)
                .iter()
                .any(|c| matches!(c, Cmd::MoveEntry { .. }))
        };
        assert!(moves(&bare), "no corridor line = the schema's corridor");
        assert!(
            !moves(&hook(&[])),
            "an explicit 0 still stands still, and reaches the file to say so"
        );
    }

    /// `HookPriceDistance` gives the entry a corridor a share of the move
    /// wide, as MoonShot's: the price leaving it upwards re-places the entry
    /// at the same distance. With `HookSellFixed` off the moved entry re-plans
    /// its exit off the new price (299.10 → 299.38) instead of keeping the
    /// ratio it was placed with.
    #[test]
    fn hook_corridor_follows_the_price_and_replans_the_exit() {
        use FieldValue::{Bool, Double};
        let st = hook(&[
            ("HookInitialPrice", Double(-50.0)),
            ("HookPriceDistance", Double(20.0)),
            ("HookSellFixed", Bool(false)),
        ]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let mut orders = Orders::new();
        let t = 1_000_000;
        hook_fall(&mut shot, &st, &mut model, t);
        // 297 − 50 % of 3.00 = 295.50, 0.7056 % under 297.60; the exit is
        // 80 % of the way back to 300 from there: 1.2183 % → 299.10.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 5_000);
        assert_eq!(planned(&cmds), [(false, 295.5, 299.1)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 5_000);
        let id = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
            t + 5_010,
        );
        // The move is 1.0101 % (3.00 off its low of 297), so the band is
        // 0.7056 % ∓ 1.0101·20/100 = 0.5036 %…0.9077 %. At 297.00 the entry
        // is 0.5051 % under the price — inside by 0.0014 pp, where the old
        // base (depth 1.00 %, near bound 0.5056 %) would have moved it down.
        // The sliver is thin on purpose: it is what pins the width formula.
        model.at_mut(1).unwrap().last_price = Some(297.0);
        assert!(
            shot.tick(&st, &orders, &model, &win, sched, t + 5_500)
                .is_empty(),
            "the near bound follows the depth measured off the low"
        );
        // Still inside: 295.50 is 0.6723 % under 297.50.
        model.at_mut(1).unwrap().last_price = Some(297.5);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 6_000)
            .is_empty());
        model.at_mut(1).unwrap().last_price = Some(299.0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 7_000);
        assert_eq!(
            cmds.iter()
                .filter_map(|c| match c {
                    Cmd::MoveEntry {
                        order,
                        price,
                        planned,
                        ..
                    } => Some((*order, *price, *planned)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            // The entry moved up to 296.89, so its exit is re-planned from
            // there: 80 % of (300 − 296.89) / 296.89 = 0.8380 % → 299.38.
            [(id, 296.89, 299.38)]
        );
        assert!(has_log(&cmds, "SBER: MoonHook order replacing UP"));
        apply(&mut shot, &mut orders, &model, &cmds, t + 7_000);
        assert_eq!(orders.get(id).unwrap().record().planned_sell, 299.38);
    }

    /// A corridor that chased the entry past the start of the move leaves no
    /// way back: the exit sits one tick off the entry instead of growing
    /// again as the gap to the start reopens on the far side.
    #[test]
    fn hook_exit_stops_at_the_entry_once_it_passes_the_start() {
        use FieldValue::{Bool, Double};
        let st = hook(&[
            ("HookInitialPrice", Double(-50.0)),
            ("HookPriceDistance", Double(20.0)),
            ("HookSellFixed", Bool(false)),
        ]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let mut orders = Orders::new();
        let t = 1_000_000;
        hook_fall(&mut shot, &st, &mut model, t);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 5_000);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 5_000);
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
            t + 5_010,
        );
        // 303.00 puts tier 0 at 300.86 — above the 300.00 the fall started
        // from, so the exit is one tick away, not 80 % of the 0.286 % that
        // now separates the entry from that start again (301.55).
        model.at_mut(1).unwrap().last_price = Some(303.0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 6_000);
        assert_eq!(
            cmds.iter()
                .filter_map(|c| match c {
                    Cmd::MoveEntry { price, planned, .. } => Some((*price, *planned)),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            [(300.86, 300.87)]
        );
    }

    /// One frame holding a fall and a deeper rise: the deeper move is the
    /// one taken. 305 → 300 is a 1.67 % fall with the price 120 % back,
    /// 300 → 310 a 3.33 % rise 40 % back, and both clear their gates — the
    /// long used to win for being judged first, so a spike up came back as
    /// a buy (KCHEP, DATA 23.09).
    #[test]
    fn hook_takes_the_deeper_of_the_two_sides() {
        use FieldValue::String as Str;
        let st = hook(&[("HookDirection", Str("Both".into()))]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let orders = Orders::new();
        let t = 1_000_000;
        shot.tick(&st, &orders, &model, &win, sched, t);
        for (at, price, turnover) in [
            (3_000, 305.0, 100_000.0),
            (3_500, 300.0, 100_000.0),
            (4_100, 310.0, 200_000.0),
            (4_600, 306.0, 50_000.0),
        ] {
            shot.on_trade(1, t + at, price, turnover, true);
            model.at_mut(1).unwrap().last_price = Some(price);
        }
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 5_000);
        assert_eq!(
            planned(&cmds)
                .iter()
                .map(|&(short, price, _)| (short, price))
                .collect::<Vec<_>>(),
            [(true, 309.0)]
        );
        assert!(has_log(
            &cmds,
            "SBER: MoonHook SHORT min.Price: 300.00 max.Price: 310.00 Depth: 3.33%"
        ));
    }

    /// `HookDirection` opens the mirror side: a 1 % rise to 303, a fifth of
    /// it back, sells at `HookInitialPrice` of the move above its high with
    /// the exit `HookSellLevel` of the way back to where the rise started.
    #[test]
    fn hook_short_detect_mirrors_the_fall() {
        use FieldValue::String as Str;
        let st = hook(&[("HookDirection", Str("OnlyShort".into()))]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let orders = Orders::new();
        let t = 1_000_000;
        shot.tick(&st, &orders, &model, &win, sched, t);
        for (at, price, turnover) in [
            (3_000, 300.0, 100_000.0),
            (3_500, 300.0, 100_000.0),
            (4_100, 303.0, 200_000.0),
            (4_600, 302.4, 50_000.0),
        ] {
            shot.on_trade(1, t + at, price, turnover, true);
            model.at_mut(1).unwrap().last_price = Some(price);
        }
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 5_000);
        // 80 % of the way from 302.70 back to the 300.00 the rise started
        // from is 0.7136 %, and a short divides: 302.70 / 1.007136 = 300.56.
        assert_eq!(planned(&cmds), [(true, 302.7, 300.56)]);
        assert!(has_log(
            &cmds,
            "SBER: MoonHook SHORT min.Price: 300.00 max.Price: 303.00 Depth: 1.00% RollBack: 20%"
        ));
    }

    /// `HookRepeatAfterSell`: a sale that closed at or past
    /// `HookRepeatIfProfit` puts the same detect's ladder out once more; the
    /// detect is spent afterwards, and without the flag it goes at the sale.
    #[test]
    fn hook_repeats_the_ladder_after_a_profitable_sale() {
        use crate::orders::Action;
        use FieldValue::{Bool, Double};
        let (win, sched) = (Windows::default(), None::<f64>);
        let t = 1_000_000;
        let run = |st: &Strategies, profit: f64| {
            let mut model = model();
            let mut shot = MoonShot::default();
            let mut orders = Orders::new();
            hook_fall(&mut shot, st, &mut model, t);
            let cmds = shot.tick(st, &orders, &model, &win, sched, t + 5_000);
            let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 5_000);
            orders.apply(
                &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
                t + 5_010,
            );
            let fx = orders.apply(
                &report("", "900000", ExecStatus::Filled, 1, 1, 297.3),
                t + 5_020,
            );
            let exit = fx
                .actions
                .iter()
                .find_map(|a| match a {
                    Action::Post { key, .. } => Some(key.clone()),
                    _ => None,
                })
                .expect("the planned exit is posted on the fill");
            orders.apply(
                &report(&exit, "900001", ExecStatus::New, 1, 0, 0.0),
                t + 5_030,
            );
            orders.apply(
                &report(
                    "",
                    "900001",
                    ExecStatus::Filled,
                    1,
                    1,
                    297.3 * (1.0 + profit / 100.0),
                ),
                t + 5_040,
            );
            let mut out = Vec::new();
            for k in 1..4 {
                out.push(starts(&shot.tick(
                    st,
                    &orders,
                    &model,
                    &win,
                    sched,
                    t + 5_000 + k * 1_000,
                )));
            }
            out
        };
        let on = hook(&[("HookRepeatAfterSell", Bool(true))]);
        assert_eq!(
            run(&on, 0.8),
            [vec![(297.3, 3000.0)], vec![], vec![]],
            "one repeat, then the detect is spent"
        );
        assert_eq!(
            run(&hook(&[]), 0.8),
            [vec![], vec![], vec![]],
            "no repeat without the flag"
        );
        let picky = hook(&[
            ("HookRepeatAfterSell", Bool(true)),
            ("HookRepeatIfProfit", Double(1.0)),
        ]);
        assert_eq!(
            run(&picky, 0.8),
            [vec![], vec![], vec![]],
            "0.8 % is under HookRepeatIfProfit"
        );
    }

    /// MAGEP, 21.09 18:13:16–18:13:47 MSK: the exchange trades (`GetLastTrades`,
    /// ms from 18:13:40, price, lots) in exchange order.
    const MAGEP_TAPE: &[(i64, f64, f64)] = &[
        (-23465, 2.64, 1.0),
        (3091, 2.635, 2.0),
        (3091, 2.63, 15.0),
        (3091, 2.63, 1.0),
        (3091, 2.625, 2.0),
        (3091, 2.62, 50.0),
        (3091, 2.615, 2.0),
        (3091, 2.61, 15.0),
        (3091, 2.61, 58.0),
        (3091, 2.605, 2.0),
        (3091, 2.605, 50.0),
        (3091, 2.6, 1.0),
        (3091, 2.6, 1.0),
        (3091, 2.6, 2.0),
        (3091, 2.6, 10.0),
        (3091, 2.595, 2.0),
        (3091, 2.595, 2.0),
        (3091, 2.595, 50.0),
        (3091, 2.59, 2.0),
        (3091, 2.59, 2.0),
        (3091, 2.59, 2.0),
        (3091, 2.59, 5.0),
        (3091, 2.585, 2.0),
        (3091, 2.585, 2.0),
        (3091, 2.585, 10.0),
        (3091, 2.585, 10.0),
        (3091, 2.585, 10.0),
        (3091, 2.585, 132.0),
        (3091, 2.58, 1.0),
        (3091, 2.58, 2.0),
        (3091, 2.58, 50.0),
        (3091, 2.58, 375.0),
        (3091, 2.575, 58.0),
        (3091, 2.575, 5.0),
        (3091, 2.575, 2.0),
        (3091, 2.575, 2.0),
        (3091, 2.575, 2.0),
        (3091, 2.575, 2.0),
        (3091, 2.575, 111.0),
        (3091, 2.57, 2.0),
        (3091, 2.57, 2.0),
        (3091, 2.57, 15.0),
        (3091, 2.57, 53.0),
        (3091, 2.565, 2.0),
        (3091, 2.565, 2.0),
        (3091, 2.565, 2.0),
        (3091, 2.565, 50.0),
        (3091, 2.56, 2.0),
        (3091, 2.56, 2.0),
        (3091, 2.56, 2.0),
        (3103, 2.64, 1.0),
        (4384, 2.6, 1.0),
        (4384, 2.59, 1.0),
        (4384, 2.59, 3.0),
        (4384, 2.58, 5.0),
        (4384, 2.58, 1.0),
        (4384, 2.575, 3.0),
        (4384, 2.56, 5.0),
        (4384, 2.56, 5.0),
        (4384, 2.56, 1.0),
        (5764, 2.575, 1.0),
        (5764, 2.57, 4.0),
        (5764, 2.57, 1.0),
        (5764, 2.57, 8.0),
        (5764, 2.565, 5.0),
        (5764, 2.565, 5.0),
        (5764, 2.56, 4.0),
        (5764, 2.555, 2.0),
        (5764, 2.555, 2.0),
        (5764, 2.555, 50.0),
        (5764, 2.55, 2.0),
        (5764, 2.55, 2.0),
        (5764, 2.55, 2.0),
        (5764, 2.55, 2.0),
        (5764, 2.545, 25.0),
        (5764, 2.545, 2.0),
        (5764, 2.545, 2.0),
        (5764, 2.545, 2.0),
        (5764, 2.545, 10.0),
        (5764, 2.545, 50.0),
        (5764, 2.54, 19.0),
        (6079, 2.555, 1.0),
        (6079, 2.55, 6.0),
        (6079, 2.55, 5.0),
        (6079, 2.55, 5.0),
        (6079, 2.545, 6.0),
        (6079, 2.54, 1.0),
        (6079, 2.54, 5.0),
        (6079, 2.54, 130.0),
        (6079, 2.535, 72.0),
        (6079, 2.53, 50.0),
        (6079, 2.525, 26.0),
        (6079, 2.52, 100.0),
        (6079, 2.52, 1.0),
        (6079, 2.52, 2.0),
        (6079, 2.52, 156.0),
        (6079, 2.515, 1.0),
        (6079, 2.515, 50.0),
        (6079, 2.51, 1.0),
        (6079, 2.51, 26.0),
        (6079, 2.505, 20.0),
        (6079, 2.5, 325.0),
        (6079, 2.49, 40.0),
        (6079, 2.485, 151.0),
        (6079, 2.48, 410.0),
        (6482, 2.515, 344.0),
        (7421, 2.535, 1.0),
        (7421, 2.525, 2.0),
    ];

    /// The MAGEP spike of 21.09 through `hook shares` (frame 10 s, anti-pump,
    /// depth 1 %, rollback 80 % held 100 ms, entry at the low, corridor 1 % of
    /// the move), passes every 100 ms, trades arriving 30 ms late. The detect
    /// prices the entry at the low, 2.56, but its distance off the one-lot buy
    /// at 2.64 that followed the sweep: 3.03 %, which the 0.01 % corridor then
    /// holds under every new low — the entry never meets a trade, as in the
    /// core's log (2.56 → 2.48 → 2.465 → 2.425 → 2.405, back up to 2.45) and
    /// as MoonBot's corridor does (`Buffer` off the price at the detect).
    /// At 47.42 a position of another strategy closes at a loss: its
    /// `TradePenaltyTime` is its own, so the hook's entry stays (in the core
    /// of 21.09 it was taken off, silently).
    #[test]
    fn hook_replays_the_magep_spike_of_21_09() {
        use FieldValue::{Bool, Double, Int32};
        let st = hook(&[
            ("OrderSize", Double(1000.0)),
            ("HookTimeFrame", Double(10.0)),
            ("HookAntiPump", Bool(true)),
            ("HookPriceRollBack", Double(80.0)),
            ("HookRollBackWait", Int32(100)),
            ("HookInitialPrice", Double(0.0)),
            ("HookPriceDistance", Double(1.0)),
            ("AutoCancelBuy", Double(60.0)),
        ]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        {
            let m = model.at_mut(1).unwrap();
            m.tick_size = 0.005;
            m.price_precision = 3;
            m.last_price = Some(2.64);
        }
        let mut shot = MoonShot::default();
        let mut orders = Orders::new();
        let t0 = 1_000_000_000;
        const LATE: i64 = 30;
        // A long of another strategy (MoonStrike), bought at 2.575.
        let manual = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 9,
            size: 772.5,
            price: 2.575,
            planned_sell: 0.0,
            stops: None,
        };
        let fx = orders.start(0, &manual, model.at(1).unwrap(), t0 - 30_000);
        let other = fx.changed[0];
        let key = fx
            .actions
            .iter()
            .find_map(|a| match a {
                crate::orders::Action::Post { key, .. } => Some(key.clone()),
                _ => None,
            })
            .expect("the long is posted");
        orders.apply(
            &report(&key, "910003", ExecStatus::New, 30, 0, 0.0),
            t0 - 30_000,
        );
        orders.apply(
            &report("", "910003", ExecStatus::Filled, 30, 30, 2.575),
            t0 - 30_000,
        );

        let mut fed = 0;
        let mut entry: Option<(u64, f64)> = None;
        let mut prices = Vec::new();
        let mut logs = Vec::new();
        let mut now = t0 - 25_000;
        while now <= t0 + 9_000 {
            while fed < MAGEP_TAPE.len() && t0 + MAGEP_TAPE[fed].0 + LATE <= now {
                let (at, price, lots) = MAGEP_TAPE[fed];
                // A print at the entry's own price need not reach it in the
                // queue (44.38: 11 lots at 2.56 a second after it was placed);
                // one under it would have filled it.
                if let Some((_, p)) = entry {
                    assert!(
                        price >= p,
                        "a trade at {price} went through the entry at {p}"
                    );
                }
                shot.on_trade(1, t0 + at + LATE, price, price * lots * 10.0, true);
                model.at_mut(1).unwrap().last_price = Some(price);
                fed += 1;
            }
            if now == t0 + 7_600 {
                // The other long's stop fills at 2.528 on the 47.42 prints,
                // reported after the pass that moved the entry up to 2.45.
                let fx = orders.target(other, Leg::Sell, 2.475, None);
                let exit = fx
                    .actions
                    .iter()
                    .find_map(|a| match a {
                        crate::orders::Action::Post { key, .. } => Some(key.clone()),
                        _ => None,
                    })
                    .expect("the stop is posted");
                let mut new = report(&exit, "910004", ExecStatus::New, 30, 0, 0.0);
                new.sell = true;
                orders.apply(&new, now);
                let mut done = report("", "910004", ExecStatus::Filled, 30, 30, 2.528);
                done.sell = true;
                orders.apply(&done, now);
            }
            let cmds = shot.tick(&st, &orders, &model, &win, sched, now);
            for c in &cmds {
                match c {
                    Cmd::Start { order, .. } => prices.push(order.price),
                    Cmd::MoveEntry { price, .. } => prices.push(*price),
                    Cmd::Log(l) => logs.push(l.clone()),
                    _ => {}
                }
            }
            let keys = apply(&mut shot, &mut orders, &model, &cmds, now);
            for key in keys {
                orders.apply(&report(&key, "910002", ExecStatus::New, 39, 0, 0.0), now);
            }
            entry = orders
                .iter()
                .find(|o| o.strategy_id == 7 && o.status == status::BUY_SET)
                .map(|o| (o.id, o.heading(Leg::Buy).unwrap_or(o.record().buy.price)));
            if cancels(&cmds)
                .iter()
                .any(|&id| Some(id) == entry.map(|e| e.0))
            {
                orders.apply(
                    &report("", "910002", ExecStatus::Cancelled, 39, 0, 0.0),
                    now,
                );
                entry = None;
            }
            now += 100;
        }
        assert!(logs.iter().any(|l| l.contains(
            "SBER: MoonHook max.Price: 2.589 min.Price: 2.560 Depth: 1.13% RollBack: 277%"
        )));
        // The core's log has one more step down (2.425): the 46.08 sweep
        // reached it in two parts. The ends are the same.
        assert_eq!(prices[..3], [2.56, 2.48, 2.465]);
        assert_eq!(prices.iter().copied().fold(f64::MAX, f64::min), 2.405);
        assert_eq!(prices.last(), Some(&2.45));
        assert_eq!(
            entry.map(|e| e.1),
            Some(2.45),
            "another strategy's loss leaves the entry"
        );
        assert!(!logs.iter().any(|l| l.contains("FilterCheck")), "{logs:#?}");
    }

    /// Trades 1.03 % under LastBidEMA: the entry goes out 2 % under the price
    /// before the strike with its exit planned 80 % of its depth (2 %) above
    /// it; the fill's exit keeps that distance, not `SellPrice`.
    #[test]
    fn strike_long_entry_and_planned_exit() {
        let st = strike(&[]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let mut orders = Orders::new();
        let t = 1_000_000;
        strike_ticks(&mut shot, &st, &mut model, t);
        let now = t + 2 * strike::TICK_MS + 100;
        assert!(!shot.on_trade(1, now, 300.0, 1_000.0, true));
        assert!(shot.on_trade(1, now, 297.2, 1_000.0, true));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now);
        assert!(starts(&cmds).is_empty(), "0.93 % is not deep enough");
        assert!(shot.on_trade(1, now + 10, 296.9, 1_000.0, true));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 20);
        // Depth of the bottom (the take 294.00): 2 %; exit 80 % of it = +1.6 %.
        assert_eq!(planned(&cmds), [(false, 294.0, 298.7)]);
        assert!(has_log(
            &cmds,
            "SBER: MoonStrike LastBID: 300.00 min.Price: 296.90 Depth: 1.03%"
        ));
        assert!(cmds.iter().any(
            |c| matches!(c, Cmd::Detect { is_short: false, msg, .. } if msg == "strike 1.03%")
        ));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, now + 20);
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0),
            now + 30,
        );
        orders.apply(
            &report("", "900000", ExecStatus::Filled, 1, 1, 294.0),
            now + 40,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 1_000);
        let o = orders.iter().next().unwrap();
        assert_eq!(o.status, status::SELL_SET, "Orders placed the planned exit");
        assert_eq!(o.record().sell.price, 298.7);
        assert!(moves(&cmds).is_empty(), "no SellPrice exit on top");
    }

    /// BTZ6 21.09: one contract (~7100 USDT) against an OrderSize of
    /// 1000 went out as one lot, seven budgets. A lot is rounded up to only
    /// within `LOT_OVER_BUDGET` of the budget; a dearer one skips the entry.
    #[test]
    fn strike_entry_skips_a_lot_far_over_the_budget() {
        use FieldValue::Double;
        let entry = |size: f64| {
            let st = strike(&[("OrderSize", Double(size))]);
            let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
            let mut shot = MoonShot::default();
            let t = 1_000_000;
            strike_ticks(&mut shot, &st, &mut model, t);
            let now = t + 2 * strike::TICK_MS + 100;
            assert!(shot.on_trade(1, now + 10, 296.9, 1_000.0, true));
            shot.tick(&st, &Orders::new(), &model, &win, sched, now + 20)
        };
        // SBER lot 10 at the take 294.00 = 2940 USDT.
        let cmds = entry(1_000.0);
        assert!(starts(&cmds).is_empty(), "2940 USDT is 2.9 budgets");
        assert!(has_log(
            &cmds,
            "SBER: the smallest order 2940.00 USDT exceeds the budget 1000.00 USDT"
        ));
        assert_eq!(starts(&entry(2_000.0)), [(294.0, 2940.0)]);
    }

    /// Aster's `MIN_NOTIONAL`: a tier budget under the exchange's floor is
    /// not a smaller order, it is a refused one — the smallest order the
    /// exchange takes is what the budget is measured against.
    #[test]
    fn the_smallest_order_is_the_exchange_floor_not_one_step() {
        let mut m = fixtures::market("XUSDT", "X", 0.0001, 1.0);
        m.min_notional = 5.0;
        // One step at 0.5 is 0.5 USDT; the floor needs ten of them.
        assert!((smallest_order(&m, 0.5) - 5.0).abs() < 1e-9);
        // A step worth more than the floor is its own minimum.
        assert!((smallest_order(&m, 80.0) - 80.0).abs() < 1e-9);
        // `LOT_SIZE.minQty` above the lots the notional floor needs wins.
        m.min_qty = 12.0;
        assert!((smallest_order(&m, 0.5) - 6.0).abs() < 1e-9);
        m.min_qty = 0.0;
        m.min_notional = 0.0;
        assert!((smallest_order(&m, 0.5) - 0.5).abs() < 1e-9);
    }

    /// Both sides: a strike up above LastAskEMA sells; the entry waits
    /// `MStrikeBuyDelay` and, with `MStrikeWaitDip`, a trade back down; the
    /// relative level 0 sells at the strike's high, measured until then.
    #[test]
    fn strike_short_waits_for_delay_and_dip() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let st = strike(&[
            ("MStrikeDirection", Str("Both".into())),
            ("MStrikeBuyDelay", Int32(500)),
            ("MStrikeWaitDip", Bool(true)),
            ("MStrikeBuyRelative", Bool(true)),
            ("MStrikeBuyLevel", Double(0.0)),
        ]);
        let (mut model, win, sched) = (model(), Windows::default(), None::<f64>);
        let mut shot = MoonShot::default();
        let orders = Orders::new();
        let t = 1_000_000;
        strike_ticks(&mut shot, &st, &mut model, t);
        let now = t + 2 * strike::TICK_MS + 100;
        shot.on_trade(1, now, 300.1, 1_000.0, true);
        assert!(shot.on_trade(1, now + 1, 303.2, 1_000.0, true));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 10);
        // The short reads the ask side of the same book, a tick above.
        assert!(has_log(
            &cmds,
            "SBER: MoonStrike LastASK: 300.10 max.Price: 303.20"
        ));
        assert!(starts(&cmds).is_empty(), "the delay");
        shot.on_trade(1, now + 100, 303.5, 1_000.0, true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 600);
        assert!(starts(&cmds).is_empty(), "no dip yet");
        shot.on_trade(1, now + 700, 303.0, 1_000.0, true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 800);
        // Take and bottom at the high 303.5 (1.13 % over LastASK), exit
        // 0.91 % under it.
        assert_eq!(planned(&cmds), [(true, 303.5, 300.77)]);

        // A market leaving normal trading drops a waiting signal: no order
        // from its old prices once it trades again.
        let mut shot = MoonShot::default();
        strike_ticks(&mut shot, &st, &mut model, t);
        shot.on_trade(1, now, 300.1, 1_000.0, true);
        shot.on_trade(1, now + 1, 303.2, 1_000.0, true);
        shot.tick(&st, &orders, &model, &win, sched, now + 10);
        assert_eq!(shot.signals.len(), 1);
        model.at_mut(1).unwrap().trading = false;
        shot.tick(&st, &orders, &model, &win, sched, now + 100);
        assert!(shot.signals.is_empty());
        model.at_mut(1).unwrap().trading = true;
        shot.on_trade(1, now + 200, 303.0, 1_000.0, true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 900);
        assert!(starts(&cmds).is_empty());

        // An entry halt (a rate limit) keeps the market out of the pass while its
        // track lives on: the signal expires and never fires after the halt.
        let mut shot = MoonShot::default();
        strike_ticks(&mut shot, &st, &mut model, t);
        shot.on_trade(1, now, 300.1, 1_000.0, true);
        shot.on_trade(1, now + 1, 303.2, 1_000.0, true);
        shot.tick(&st, &orders, &model, &win, sched, now + 10);
        assert_eq!(shot.signals.len(), 1);
        shot.on_error_budget(now + 20, RATE_HALT_MS);
        shot.on_trade(1, now + 200, 303.0, 1_000.0, true);
        shot.tick(&st, &orders, &model, &win, sched, now + 11_000);
        assert!(shot.signals.is_empty());
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 61_000);
        assert!(starts(&cmds).is_empty());

        // Without a dip for 10 s the signal is dropped.
        let mut shot = MoonShot::default();
        strike_ticks(&mut shot, &st, &mut model, t);
        shot.on_trade(1, now, 300.1, 1_000.0, true);
        shot.on_trade(1, now + 1, 303.2, 1_000.0, true);
        shot.tick(&st, &orders, &model, &win, sched, now + 10);
        shot.on_trade(1, now + 100, 303.5, 1_000.0, true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 10_100);
        assert!(has_log(&cmds, "MoonStrike: no dip in 10 s, no order"));
        shot.on_trade(1, now + 10_200, 303.0, 1_000.0, true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, now + 10_300);
        assert!(starts(&cmds).is_empty());
    }

    #[test]
    fn replace_delay_and_auto_cancel() {
        use FieldValue::{Double, String as Str};
        let mut model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MShotReplaceDelay", Double(1.0)),
                ("AutoCancelBuy", Double(30.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 2_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t0);

        // Price comes within PriceMin: the move waits 1 s and tracks the low.
        model.at_mut(1).unwrap().last_price = Some(298.0);
        assert!(moves(&shot.tick(&st, &orders, &model, &win, sched, t0 + 1000)).is_empty());
        model.at_mut(1).unwrap().last_price = Some(297.4);
        assert!(moves(&shot.tick(&st, &orders, &model, &win, sched, t0 + 1500)).is_empty());
        model.at_mut(1).unwrap().last_price = Some(298.2);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 2100);
        assert_eq!(moves(&cmds), [(id, Leg::Buy, 294.43)]);
        assert!(has_log(
            &cmds,
            "replacing DOWN on cur. price: 298.2 delay: 1000ms; Min. Ask: 297.4"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t0 + 2100);
        // Back inside the corridor: quiet until the entry turns 30 s old.
        model.at_mut(1).unwrap().last_price = Some(297.4);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 29_000)
            .is_empty());
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 30_000);
        assert_eq!(cancels(&cmds), [id]);
        assert!(has_log(
            &cmds,
            "Auto cancel buy order activated since 30 sec. (strategy <shot>)"
        ));
        assert!(moves(&cmds).is_empty());
        // Not repeated while the cancel is in flight; the cancelled entry is
        // replaced by a fresh one at `Price` from the current price.
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 31_000)
            .is_empty());
        orders.apply(
            &report("", "900000", ExecStatus::Cancelled, 1, 0, 0.0),
            t0 + 31_500,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 32_000);
        assert_eq!(starts(&cmds), [(294.43, 3000.0)]);
        assert!(has_log(&cmds, "Starting new MoonShot market"));
    }

    /// The STAR trade of the 12.09 log: entry 0.05465, SellPrice 8 %,
    /// PriceDownRelative 20 % every 3 s after 1 s, floor +0.2 %.
    #[test]
    fn price_down_relative_follows_the_moonbot_log() {
        use FieldValue::{Bool, Double, String as Str};
        let model = model();
        let star = model.at(3).unwrap();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("STAR".into())),
                ("OrderSize", Double(1.0)),
                ("MShotPrice", Double(11.0)),
                ("MShotPriceMin", Double(10.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("SellPrice", Double(8.0)),
                ("PriceDownTimer", Double(1.0)),
                ("PriceDownDelay", Double(3.0)),
                ("PriceDownPercent", Double(20.0)),
                ("PriceDownRelative", Bool(true)),
                ("PriceDownAllowedDrop", Double(0.2)),
                ("UseStopLoss", Bool(false)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 3_000_000;
        // The entry sits 11 % under 0.0614 and fills at its price.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(starts(&cmds), [(0.05465, 1.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        let mut fill = report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 0.05465);
        fill.uid = "STAR".into();
        orders.apply(&fill, t0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 0.05902)]);
        assert!(has_log(
            &cmds,
            "Using (strategy <shot>) SELL price: 8.00% = 0.05902"
        ));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        let mut new = report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0);
        new.uid = "STAR".into();
        orders.apply(&new, t0 + 500);
        // 1 s after the fill the first step, then every 3 s: each takes 20 %
        // of the remaining profit; the floor is the entry +0.2 %.
        let mut expect = |at: i64, price: f64| {
            let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + at);
            assert_eq!(moves(&cmds), [(id, Leg::Sell, price)], "at {at} ms");
            assert!(has_log(&cmds, "Auto Sell Replacing (PriceDownRelative:"));
            apply(&mut shot, &mut orders, &model, &cmds, t0 + at);
        };
        expect(1_000, 0.05815);
        expect(4_000, 0.05745);
        expect(7_000, 0.05689);
        expect(10_000, 0.05644);
        expect(100_000, 0.05476);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 103_000)
            .is_empty());
        assert_eq!(star.nearest(0.05465 * 1.002), 0.05476);
    }

    /// A PriceDown step that falls while the previous step's Cancel waits for
    /// the exit's final report is handed to `Orders` once: later passes see
    /// it deferred and neither repeat the move nor the log line (up to 130
    /// lines a step).
    #[test]
    fn price_down_step_behind_a_cancel_in_flight_is_sent_once() {
        use crate::orders::Action;
        use FieldValue::{Bool, Double, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("STAR".into())),
                ("OrderSize", Double(1.0)),
                ("MShotPrice", Double(11.0)),
                ("MShotPriceMin", Double(10.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("SellPrice", Double(8.0)),
                ("PriceDownTimer", Double(1.0)),
                ("PriceDownDelay", Double(3.0)),
                ("PriceDownPercent", Double(20.0)),
                ("PriceDownRelative", Bool(true)),
                ("PriceDownAllowedDrop", Double(0.2)),
                ("UseStopLoss", Bool(false)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 3_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        let mut fill = report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 0.05465);
        fill.uid = "STAR".into();
        orders.apply(&fill, t0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        let mut new = report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0);
        new.uid = "STAR".into();
        orders.apply(&new, t0 + 500);
        // Step 1 goes out and stays unanswered.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1_000);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 0.05815)]);
        let fx = orders.target(id, Leg::Sell, 0.05815, None);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Cancel { exchange_id, .. }] if exchange_id == "900001"),
            "cancel expected: {:?}",
            fx.actions
        );
        // Step 2 falls behind it: deferred once, then quiet.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 4_000);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 0.05745)]);
        assert!(has_log(&cmds, "(PriceDownRelative: 0.05815 =>"));
        assert!(orders
            .target(id, Leg::Sell, 0.05745, None)
            .actions
            .is_empty());
        for at in [4_300, 4_600, 5_000] {
            let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + at);
            assert!(cmds.is_empty(), "at {at} ms: {cmds:?}");
        }
        // The final report releases the deferred step.
        let mut done = report(&keys[0], "900001", ExecStatus::Cancelled, 1, 0, 0.0);
        done.uid = "STAR".into();
        done.unary = true;
        let fx = orders.apply(&done, t0 + 5_100);
        assert!(
            matches!(fx.actions.as_slice(), [Action::Post { price: Some(p), .. }] if *p == 0.05745),
            "{:?}",
            fx.actions
        );
        let [Action::Post { key, .. }] = fx.actions.as_slice() else {
            unreachable!()
        };
        let mut new = report(key, "900002", ExecStatus::New, 1, 0, 0.0);
        new.uid = "STAR".into();
        orders.apply(&new, t0 + 5_150);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 5_200)
            .is_empty());
        // A plain Cancel in flight drops moves: none is sent at the next step.
        orders.cancel(id, Leg::Sell);
        assert_eq!(orders.get(id).unwrap().heading(Leg::Sell), None);
        for at in [7_000, 7_300] {
            let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + at);
            assert!(cmds.is_empty(), "at {at} ms: {cmds:?}");
        }
    }

    /// The entry ladder the same way: a corridor move deferred behind a
    /// Replace in flight is not re-sent every pass (`ReplaceDelay` 0).
    #[test]
    fn entry_move_behind_a_replace_in_flight_is_sent_once() {
        use crate::orders::Action;
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MShotReplaceDelay", Double(0.0)),
                ("OrdersCount", Int32(1)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t = 1_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        let id = orders.iter().next().unwrap().id;
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t);
        // Market down: the entry follows and its Replace stays unanswered.
        model.at_mut(1).unwrap().last_price = Some(298.0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1_000);
        assert_eq!(moves(&cmds), [(id, Leg::Buy, 295.02)]);
        let fx = orders.target_entry(id, 295.02, 3000.0);
        let [Action::Replace { key, lots, .. }] = fx.actions.as_slice() else {
            panic!("replace expected: {:?}", fx.actions);
        };
        let (key, lots) = (key.clone(), *lots);
        // Down again: deferred once, then quiet.
        model.at_mut(1).unwrap().last_price = Some(296.0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 2_000);
        assert_eq!(moves(&cmds), [(id, Leg::Buy, 293.04)]);
        assert!(has_log(&cmds, "tier 0: 295.02 -> 293.04"));
        assert!(orders.target_entry(id, 293.04, 3000.0).actions.is_empty());
        for at in [2_100, 2_200, 3_000] {
            let cmds = shot.tick(&st, &orders, &model, &win, sched, t + at);
            assert!(cmds.is_empty(), "at {at} ms: {cmds:?}");
        }
        let fx = orders.apply(
            &report(&key, "900000", ExecStatus::New, lots, 0, 0.0),
            t + 3_100,
        );
        assert!(
            matches!(fx.actions.as_slice(), [Action::Replace { price, .. }] if *price == 293.04)
        );
    }

    #[test]
    fn gates_stop_entries_but_keep_exits() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let mut st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(3)),
                ("MaxActiveOrders", Int32(2)),
            ],
            true,
        );
        let t = 5_000_000;
        // MaxActiveOrders caps the strategy's live orders: 2 of 3 tiers go out
        // and stay out (the cap never cancels what is already placed).
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(starts(&cmds).len(), 2);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        let mut ids: Vec<u64> = orders.iter().map(|o| o.id).collect();
        ids.sort_unstable();
        for (k, ex) in keys.iter().zip(["900000", "900001"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t);
        }
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 500)
            .is_empty());
        // Stopped strategies take their entries down, each saying why.
        st.set_running(false);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1000);
        let mut down = cancels(&cmds);
        down.sort_unstable();
        assert_eq!((cmds.len(), &down), (4, &ids));
        assert!(has_log(
            &cmds,
            "SBER: FilterCheck: market no longer meets the conditions. The strategy is stopped"
        ));
        for ex in ["900000", "900001"] {
            orders.apply(&report("", ex, ExecStatus::Cancelled, 1, 0, 0.0), t);
        }
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 2000)
            .is_empty());
        // A rate limit halts entries for a minute even when running.
        st.set_running(true);
        shot.on_failed(ids[0], "api 429/-1003: Too many requests", t + 2000);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 3000)
            .is_empty());
        assert_eq!(
            starts(&shot.tick(&st, &orders, &model, &win, sched, t + 63_000)).len(),
            2
        );
    }

    /// Strategies trade only while the market is `TRADING` (TInvestCore,
    /// 18.09, on MOEX's statuses): a market that stops takes the entries off
    /// and holds the exit where it is.
    #[test]
    fn strategies_trade_only_in_normal_trading() {
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(2)),
                ("SellPrice", Double(2.0)),
            ],
            true,
        );
        let t = 5_000_000;
        let mut shot = MoonShot::default();
        model.at_mut(1).unwrap().trading = false;
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, t)).is_empty());
        assert_eq!(
            shot.status_markets(&st).into_iter().collect::<Vec<_>>(),
            [1]
        );

        model.at_mut(1).unwrap().trading = true;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1000);
        assert_eq!(starts(&cmds).len(), 2);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 1000);
        for (k, ex) in keys.iter().zip(["900000", "900001"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t + 1000);
        }
        orders.apply(
            &report("", "900000", ExecStatus::Filled, 1, 1, 300.0),
            t + 1500,
        );

        // Status off before the exit went out: no exit, the ladder comes off.
        model.at_mut(1).unwrap().trading = false;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 2000);
        assert!(moves(&cmds).is_empty() && starts(&cmds).is_empty());
        assert_eq!(cancels(&cmds).len(), 1);

        model.at_mut(1).unwrap().trading = true;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 3000);
        assert_eq!(moves(&cmds).len(), 1, "the exit goes out with the session");
    }

    /// 18.09 restart: until a market's first trade after the start there is no
    /// price, and the restored ladders came off and went out again (114
    /// cancels and posts, HTTP 429). Without a price the ladder waits; a
    /// stopped strategy still withdraws its entries.
    #[test]
    fn restored_ladder_waits_for_the_first_price() {
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(3)),
            ],
            true,
        );
        let t = 5_000_000;
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        for (k, ex) in keys.iter().zip(["900000", "900001", "900002"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t);
        }

        let mut shot = MoonShot::default();
        shot.restore(&orders, t + 1000);
        model.at_mut(1).unwrap().last_price = Some(0.0);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 1000)
            .is_empty());
        model.at_mut(1).unwrap().last_price = Some(300.0);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t + 2000)
            .is_empty());

        // Past the wait, a market the exchange still quotes keeps them: a thin
        // coin may go minutes without a trade.
        model.at_mut(1).unwrap().last_price = Some(0.0);
        let after = t + 1000 + RESTORE_PRICE_WAIT_MS;
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask, m.book_ms) = (Some(299.0), Some(301.0), after - 1000);
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, after)
            .is_empty());
        // A market with neither a trade nor a fresh book withdraws them.
        model.at_mut(1).unwrap().book_ms = after - 20_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, after);
        assert_eq!(cancels(&cmds).len(), 3);
    }

    /// A restored fill without a price yet: the rest of its ladder still
    /// comes off (no repeat after buy), which needs no price.
    #[test]
    fn restored_fill_withdraws_its_ladder_without_a_price() {
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("OrdersCount", Int32(3)),
            ],
            true,
        );
        let t = 5_000_000;
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        for (k, ex) in keys.iter().zip(["900000", "900001", "900002"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t);
        }
        orders.apply(&report("", "900000", ExecStatus::Filled, 1, 1, 300.0), t);

        let mut shot = MoonShot::default();
        shot.restore(&orders, t + 1000);
        model.at_mut(1).unwrap().last_price = Some(0.0);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1000);
        assert_eq!(cancels(&cmds).len(), 2);
    }

    /// 15.09 log, 21:37: `MShotRepeatAfterBuy` starts a new shot on the
    /// market right after the fill's exit is placed, `MaxActiveOrders` 1
    /// notwithstanding; the next fill grants the next one.
    #[test]
    fn repeat_after_buy_pyramids_past_max_active_orders() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MaxActiveOrders", Int32(1)),
                ("MShotRepeatAfterBuy", Bool(true)),
                ("UseStopLoss", Bool(false)),
                ("PriceDownTimer", Double(0.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 6_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let first = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0 + 1000,
        );
        // BuyDone: the exit goes out, no repeat before it is placed.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1500);
        assert_eq!(moves(&cmds), [(first, Leg::Sell, 302.94)]);
        assert!(starts(&cmds).is_empty());
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 1500);
        orders.apply(
            &report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0),
            t0 + 1500,
        );
        // SellSet with `last` (300) at or above the mean (297): one repeat,
        // although the strategy already holds its one active order.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1600);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        assert!(has_log(&cmds, "Starting new MoonShot market: SBER"));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 1600);
        let second = orders.iter().map(|o| o.id).max().unwrap();
        orders.apply(
            &report(&keys[0], "900002", ExecStatus::New, 1, 0, 0.0),
            t0 + 1600,
        );
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 2600)
            .is_empty());
        // The repeat fills: its own exit and its own grant (chain 120 → 122 → 123).
        orders.apply(
            &report("", "900002", ExecStatus::Filled, 1, 1, 297.0),
            t0 + 3000,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 3500);
        assert_eq!(moves(&cmds), [(second, Leg::Sell, 302.94)]);
        assert!(cancels(&cmds).is_empty());
        apply(&mut shot, &mut orders, &model, &cmds, t0 + 3500);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 3600);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        assert_eq!(
            orders
                .iter()
                .filter(|o| o.status == status::SELL_SET)
                .count(),
            2
        );
    }

    /// 15.09 log, 21:52: a short strategy's entry comes off while the market
    /// holds a long position (a manual one here) and returns once it is sold.
    #[test]
    fn opposite_position_filters_the_market() {
        use FieldValue::{Bool, Double, String as Str};
        let model = model();
        let sber = model.at(1).unwrap();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(5000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("Short", Bool(true)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 7_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(starts(&cmds), [(303.0, 5000.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let entry = orders.iter().next().unwrap().id;
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t0);
        // A manual long fills on the same market.
        let fx = orders.start(
            0,
            &StartOrder {
                market: "SBER".into(),
                is_short: false,
                use_market_stop: false,
                strategy_id: 0,
                size: 3000.0,
                price: 299.0,
                planned_sell: 0.0,
                stops: None,
            },
            sber,
            t0 + 500,
        );
        let long = orders.iter().map(|o| o.id).max().unwrap();
        let crate::orders::Action::Post { key, .. } = &fx.actions[0] else {
            panic!("{:?}", fx.actions);
        };
        orders.apply(
            &report(key, "900001", ExecStatus::Filled, 1, 1, 299.0),
            t0 + 500,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1000);
        assert_eq!(cancels(&cmds), [entry]);
        assert!(has_log(
            &cmds,
            "SBER: FilterCheck: market no longer meets the conditions. Market SBER has opened Long position !"
        ));
        // Logged once, not re-placed while the position is open.
        orders.apply(
            &report("", "900000", ExecStatus::Cancelled, 1, 0, 0.0),
            t0 + 1500,
        );
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 2000)
            .is_empty());
        // The long is sold: the short starts again.
        let fx = orders.target(long, Leg::Sell, 305.0, None);
        let crate::orders::Action::Post { key, .. } = &fx.actions[0] else {
            panic!("{:?}", fx.actions);
        };
        orders.apply(
            &report(key, "900002", ExecStatus::Filled, 1, 1, 305.0),
            t0 + 3000,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 4000);
        assert_eq!(starts(&cmds), [(303.0, 5000.0)]);
    }

    /// 15.09 log: `StopLoss` +0.2 (past the entry), `StopLossDelay` 3 — the
    /// bid is under the stop from the fill on, the stop fires 3 s after it
    /// with a limit `StopLossSpread` under the bid.
    #[test]
    fn stop_loss_delay_arms_after_the_fill() {
        use FieldValue::{Double, String as Str};
        let mut model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("StopLoss", Double(0.2)),
                ("StopLossDelay", Double(3.0)),
                ("StopLossSpread", Double(1.0)),
                ("PriceDownTimer", Double(0.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 8_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0,
        );
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask, m.last_price) = (Some(296.5), Some(296.6), Some(296.8));
        // The exit is placed; the stop (297.59) stays quiet for 3 s.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 302.94)]);
        assert!(!has_log(&cmds, "StopLoss AutoActivated"));
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        orders.apply(
            &report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0),
            t0 + 500,
        );
        assert!(shot
            .tick(&st, &orders, &model, &win, sched, t0 + 2900)
            .is_empty());
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 3000);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 293.53)]);
        assert!(has_log(
            &cmds,
            "StopLoss AutoActivated on price drop: BID = 296.50 LastPrice = 296.80 BuyPrice = 297.00; \
             StopLoss fixed: 297.59 spread: 1.00% => 293.53"
        ));
    }

    /// M4: a silent trade stream makes the market's prices stale and a
    /// silent order state stream hides fills. No entries then; the exit
    /// priced off the entry still goes out, the bot stop waits for the stream.
    #[test]
    fn stale_streams_hold_entries_and_the_stop_but_not_the_exit() {
        use FieldValue::{Double, String as Str};
        let mut model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("StopLoss", Double(0.2)),
                ("StopLossDelay", Double(3.0)),
                ("StopLossSpread", Double(1.0)),
                ("PriceDownTimer", Double(0.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 8_000_000;
        model.at_mut(1).unwrap().feed_fresh = false;
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, t0)).is_empty());
        model.at_mut(1).unwrap().feed_fresh = true;
        shot.set_fills_seen(false);
        assert!(starts(&shot.tick(&st, &orders, &model, &win, sched, t0)).is_empty());
        shot.set_fills_seen(true);

        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0,
        );
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask, m.last_price, m.feed_fresh) = (Some(296.5), Some(296.6), Some(296.8), false);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        assert_eq!(
            moves(&cmds),
            [(id, Leg::Sell, 302.94)],
            "exit on stale prices"
        );
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        orders.apply(
            &report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0),
            t0 + 500,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 3000);
        assert!(moves(&cmds).is_empty(), "no stop on a stale bid");

        // The 2 s confirmation starts over on current prices.
        model.at_mut(1).unwrap().feed_fresh = true;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 3100);
        assert!(moves(&cmds).is_empty());
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 5100);
        assert_eq!(moves(&cmds), [(id, Leg::Sell, 293.53)]);
    }

    /// The strategy's stop reaches the order image once the entry fills: the
    /// terminal shows SL:ON and draws the chart line from that section alone
    /// (before the fill it reads the strategy's `UseStopLoss` itself). A
    /// manual stop, the one `Orders::watch` fires on, takes the slot over.
    #[test]
    fn bot_stop_is_published_in_the_image_after_the_fill() {
        use FieldValue::{Double, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("StopLoss", Double(0.2)),
                ("StopLossDelay", Double(3.0)),
                ("StopLossSpread", Double(1.0)),
                ("PriceDownTimer", Double(0.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 8_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        assert_eq!(orders.get(id).unwrap().record().stop, None);
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        assert!(has_log(
            &cmds,
            "SBER: StopLoss applied (buyPrice 297.00 stop +0.20% => 297.59) (strategy <shot>)"
        ));
        apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        let (stop, spread) = orders.get(id).unwrap().record().stop.expect("stop shown");
        assert!(
            (stop - 297.594).abs() < 1e-9 && spread == 1.0,
            "{stop} {spread}"
        );
        // Unchanged on the next pass: no command, no image bump.
        let rev = orders.get(id).unwrap().record().rev;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1000);
        assert!(!has_log(&cmds, "StopLoss applied"));
        apply(&mut shot, &mut orders, &model, &cmds, t0 + 1000);
        assert_eq!(orders.get(id).unwrap().record().rev, rev);
        // The manual stop wins the image slot.
        orders.set_stops(id, true, true, 290.0, 0.0);
        assert_eq!(orders.get(id).unwrap().record().stop, Some((290.0, 0.0)));
        orders.set_stops(id, false, false, 0.0, 0.0);
        let (stop, _) = orders
            .get(id)
            .unwrap()
            .record()
            .stop
            .expect("bot stop back");
        assert!((stop - 297.594).abs() < 1e-9);
    }

    /// `UseStopLoss` off: nothing is shown after the fill either.
    #[test]
    fn no_bot_stop_leaves_the_image_stop_empty() {
        use FieldValue::{Bool, Double, String as Str};
        let model = model();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("UseStopLoss", Bool(false)),
                ("PriceDownTimer", Double(0.0)),
            ],
            true,
        );
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 8_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let id = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0,
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
        assert!(!has_log(&cmds, "StopLoss applied"));
        assert!(!cmds.iter().any(|c| matches!(c, Cmd::Stop { .. })));
        apply(&mut shot, &mut orders, &model, &cmds, t0 + 500);
        assert_eq!(orders.get(id).unwrap().record().stop, None);
    }

    #[test]
    fn screener_universe_follows_market_tags() {
        use crate::model::Tag;
        use FieldValue::{Int32, String as Str};
        let mut model = model();
        // GAZP a stock perpetual.
        model.at_mut(2).unwrap().tags = vec![Tag::Stock];
        let mut win = Windows::default();
        let now = 9_000_000_000;
        win.push(1, now - 60_000, 300.0, 10.0);
        win.push(2, now - 60_000, 120.0, 400.0);
        let cx = Ctx {
            model: &model,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        // A class pool is what the dynamic white list says it is, so every
        // class strategy here carries one; 2 makes the cut visible.
        let pick = |tags: Option<&str>, white: &str| {
            let mut fields = vec![
                ("DynWL_SortBy", Str("DailyVol".into())),
                ("DynWL_Count", Int32(2)),
                ("CoinsWhiteList", Str(white.into())),
            ];
            if let Some(t) = tags {
                fields.push((MARKET_TAGS, Str(t.into())));
            }
            let st = strategies(&fields, true);
            screener::universe(
                &Params::from_snapshot(&st.list()[0], st.schema()),
                &cx,
                &none(),
            )
        };
        // A strategy without the field: every class (`all`, the schema's
        // default), so the stock perpetual is in beside the coins — three
        // markets, a count of two. The coins alone are `crypto`: STAR never
        // traded, and with the volume bounds gone from the screener a silent
        // market is still a market of the class — it is watched and simply not
        // entered (`the_volume_bounds_are_an_and_not_a_choice`).
        assert_eq!(pick(None, ""), [2, 1]);
        assert_eq!(pick(Some("all"), ""), [2, 1]);
        assert_eq!(pick(Some("crypto"), ""), [1, 3]);
        assert_eq!(pick(Some("stock"), ""), [2]);
        assert_eq!(
            pick(Some("Crypto, STOCK"), ""),
            [2, 1],
            "three markets, a count of two: the head of the ranking"
        );
        assert_eq!(pick(Some("!crypto, !top"), ""), [2]);
        assert_eq!(pick(Some("etf; forex"), ""), [] as [u16; 0]);
        // Empty tags or a typo: nothing, never every class.
        assert_eq!(pick(Some(""), ""), [] as [u16; 0]);
        assert_eq!(pick(Some("stocks"), ""), [] as [u16; 0]);
        // A white list alone decides, whatever the tags.
        assert_eq!(pick(Some("stock"), "SBER"), [1]);
        assert_eq!(pick(Some("stocks"), "SBER"), [1]);
        // A class with no count is a pool of the whole class: refused, with
        // a line of its own, rather than silently watching 596 markets.
        let st = strategies(&[(MARKET_TAGS, Str("crypto".into()))], true);
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert!(screener::problem(&p).is_some_and(|t| t.contains("DynWL_Count 0")));
        assert!(screener::universe(&p, &cx, &none()).is_empty());
    }

    /// Both lists are text somebody typed by hand — the terminal's global
    /// black list has read them case-blind since it was written
    /// (`ui::black_list` upper-cases it) — so these two read them the same
    /// way. TInvestCore learnt it on 29.09 from `BTCUSDperpA`, which nobody
    /// types right from memory; Aster spells every symbol in upper case, and
    /// a list typed in lower case must still name its market.
    #[test]
    fn the_lists_read_a_symbol_however_it_was_typed() {
        use FieldValue::String as Str;
        let model = model();
        let win = Windows::default();
        let cx = Ctx {
            model: &model,
            win: &win,
            now: 9_000_000_000,
            btc: None,
            market_delta: None,
        };
        let params = |white: &str, black: &str| {
            let st = strategies(
                &[
                    ("CoinsWhiteList", Str(white.into())),
                    ("CoinsBlackList", Str(black.into())),
                ],
                true,
            );
            Params::from_snapshot(&st.list()[0], st.schema())
        };
        let pool =
            |white: &str, black: &str| screener::universe(&params(white, black), &cx, &none());
        let btc = model.index_of_symbol("BTCUSDT").unwrap();
        let sber = model.index_of_symbol("SBER").unwrap();
        assert_eq!(pool("BTCUSDT", ""), [btc], "the catalog's own spelling");
        assert_eq!(pool("btcusdt", ""), [btc], "whispered");
        assert_eq!(pool("BtcUsdt", ""), [btc], "mixed");
        // The black list is read the same way, or a market would be let back
        // in by the case of the letters that banned it.
        assert_eq!(pool("BTCUSDT, SBER", "btcusdt"), [sber]);
        // And a list entry that names no market of the catalog still holds
        // nothing — the change is about spelling, not about inventing markets.
        assert_eq!(pool("SBER", "XXXX"), [sber]);
        // The page reads the seats off the same two lists: a market the black
        // list holds says so by name, whichever case it was banned in.
        let seat = |sym: &str, white: &str, black: &str| {
            let idx = model.index_of_symbol(sym).unwrap();
            screener::explain(&params(white, black), &cx, &none())
                .into_iter()
                .find(|s| s.idx == idx)
                .unwrap()
                .seat
        };
        assert_eq!(seat("BTCUSDT", "btcusdt", ""), screener::Seat::Pool);
        assert_eq!(
            seat("BTCUSDT", "BTCUSDT, SBER", "btcusdt"),
            screener::Seat::Black
        );
    }

    /// A word in a list that names no market used to hold nothing and say
    /// nothing: the pool came up short and no line anywhere said which word
    /// was wrong. It is a warning and never a `problem` — the markets that DO
    /// exist are still the operator's pool, and a strategy must not stop
    /// trading four markets over a fifth that was mistyped.
    #[test]
    fn a_list_entry_that_names_no_market_is_said_out_loud() {
        use FieldValue::String as Str;
        let model = model();
        let win = Windows::default();
        let cx = Ctx {
            model: &model,
            win: &win,
            now: 9_000_000_000,
            btc: None,
            market_delta: None,
        };
        let params = |white: &str, black: &str| {
            let st = strategies(
                &[
                    ("CoinsWhiteList", Str(white.into())),
                    ("CoinsBlackList", Str(black.into())),
                ],
                true,
            );
            Params::from_snapshot(&st.list()[0], st.schema())
        };
        let said =
            |white: &str, black: &str| screener::unknown_symbols(&params(white, black), &model);

        assert_eq!(said("SBER", ""), None);
        assert_eq!(
            said("sber, sGaZp", ""),
            None,
            "the case is not what makes a market unknown"
        );
        assert_eq!(
            said("SBER, NOPE", ""),
            Some("CoinsWhiteList: no such market in the catalog: NOPE".to_string())
        );
        assert_eq!(
            said("SBER, NOPE, NOPE", "GONE"),
            Some(
                "CoinsWhiteList: no such market in the catalog: NOPE; \
                 CoinsBlackList: no such market in the catalog: GONE"
                    .to_string()
            ),
            "each word once, and the field that holds it is named"
        );

        // And none of it changes what the strategy does: no `problem`, and
        // the market that does exist is still the pool.
        let p = params("SBER, NOPE", "");
        assert!(screener::problem(&p).is_none(), "a warning, not a refusal");
        assert_eq!(
            screener::universe(&p, &cx, &none()),
            [model.index_of_symbol("SBER").unwrap()]
        );

        // No two Aster markets share a symbol's upper case or a coin, so no
        // spelling is ambiguous (TInvestCore had `SiZ5` beside `SIZ5`).
        assert!(!model.symbol_is_ambiguous("sber"));
    }

    /// The page's helper and the strategy must never disagree about which
    /// markets are watched: what `explain` seats in the pool IS what
    /// `universe` returned, and every other seat names a different reason.
    /// A helper that answered this question a second time would sooner or
    /// later contradict the strategy it is helping to write.
    #[test]
    fn explain_seats_the_pool_universe_returned() {
        use crate::model::Tag;
        use FieldValue::{Int32, String as Str};
        let mut model = model();
        model.at_mut(2).unwrap().tags = vec![Tag::Stock];
        let mut win = Windows::default();
        let now = 9_000_000_000;
        win.push(1, now - 60_000, 300.0, 10.0);
        win.push(2, now - 60_000, 120.0, 400.0);
        let cx = Ctx::new(&model, &win, now, None);
        let seats = |fields: &[(&str, FieldValue)]| {
            let st = strategies(fields, true);
            let p = Params::from_snapshot(&st.list()[0], st.schema());
            let pool = screener::universe(&p, &cx, &none());
            let seen = screener::explain(&p, &cx, &none());
            // Every market of the catalog is accounted for exactly once, and
            // the pool seats are the pool.
            let seated: Vec<u16> = seen
                .iter()
                .filter(|s| s.seat == screener::Seat::Pool)
                .map(|s| s.idx)
                .collect();
            let mut want = pool.clone();
            want.sort_unstable();
            let mut got = seated.clone();
            got.sort_unstable();
            assert_eq!(got, want, "seats disagree with the pool for {fields:?}");
            seen
        };
        // A class pool of one: SBER and STAR are coins, the count is 1, so
        // one is watched and the other is ranked out — not «another class».
        let seen = seats(&[
            (MARKET_TAGS, Str("crypto".into())),
            ("DynWL_SortBy", Str("DailyVol".into())),
            ("DynWL_Count", Int32(1)),
        ]);
        let of = |sym: &str| {
            let idx = model.index_of_symbol(sym).unwrap();
            *seen.iter().find(|s| s.idx == idx).unwrap()
        };
        assert_eq!(of("SBER").seat, screener::Seat::Pool);
        assert_eq!(of("SBER").rank, Some(1));
        assert_eq!(of("SGAZP").seat, screener::Seat::OtherClass, "a stock");
        // STAR never traded: a coin of the class, ranked at zero turnover and
        // past the count — watched by nothing, refused by nobody.
        assert_eq!(of("STAR").seat, screener::Seat::Ranked);
        assert_eq!(of("STAR").rank, Some(2));
        // The white list names the markets: what it does not name is «not in
        // the white list», and what a black list holds says so by name.
        let seen = seats(&[
            ("CoinsWhiteList", Str("SBER, STAR".into())),
            ("CoinsBlackList", Str("STAR".into())),
        ]);
        let of = |sym: &str| {
            let idx = model.index_of_symbol(sym).unwrap();
            *seen.iter().find(|s| s.idx == idx).unwrap()
        };
        assert_eq!(of("SBER").seat, screener::Seat::Pool);
        assert_eq!(of("STAR").seat, screener::Seat::Black);
        assert_eq!(of("SGAZP").seat, screener::Seat::NotListed);
        assert_eq!(of("SBER").rank, None, "no dynamic list, no ranking");
        // The two dynamic lists overlap, and the order they are read in is
        // the order the pool is built in: the white list's head first, the
        // black list after. STAR is ranked past a count of 1 AND would be in
        // the black list's head — it left by the ranking, and an operator
        // sent to `DynBL_Count` would be sent to the wrong control.
        let seen = seats(&[
            (MARKET_TAGS, Str("crypto".into())),
            ("DynWL_SortBy", Str("DailyVol".into())),
            ("DynWL_Count", Int32(1)),
            ("DynBL_SortBy", Str("DailyVol".into())),
            ("DynBL_SortDesc", FieldValue::Bool(false)),
            ("DynBL_Count", Int32(1)),
        ]);
        let of = |sym: &str| {
            let idx = model.index_of_symbol(sym).unwrap();
            *seen.iter().find(|s| s.idx == idx).unwrap()
        };
        assert_eq!(
            of("STAR").seat,
            screener::Seat::Ranked,
            "the ranking, not the black list"
        );
        assert_eq!(of("SBER").seat, screener::Seat::Pool);
        // A screener that cannot produce a pool seats nothing anywhere else:
        // every market reads «no pool», not «another class».
        let st = strategies(&[(MARKET_TAGS, Str("crypto".into()))], true);
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert!(screener::problem(&p).is_some());
        assert!(screener::explain(&p, &cx, &none())
            .iter()
            .all(|s| s.seat == screener::Seat::NoPool));
    }

    /// The column the page paints red and the check the entry gate stops at
    /// are the same check. They are two walks over `Check::judge`, and the
    /// day they stop agreeing the page starts explaining refusals that never
    /// happened.
    #[test]
    fn the_page_and_the_gate_stop_at_the_same_check() {
        use FieldValue::Double;
        let model = model();
        let mut win = Windows::default();
        let now = 9_000_000_000;
        // SBER: 10 ₽ of turnover in the last minute, so both volume bounds
        // have something to read and the 24 h one is the first to refuse.
        win.push(1, now - 60_000, 300.0, 10.0);
        let cx = Ctx::new(&model, &win, now, None);
        let st = strategies(
            &[
                ("MinVolume", Double(1_000_000.0)),
                ("MinHourlyVolume", Double(1_000_000.0)),
                ("Delta_3h_Min", Double(-5.0)),
                ("Delta_3h_Max", Double(5.0)),
            ],
            true,
        );
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        let f = p.deltas.as_ref().expect("the filters are on");
        assert_eq!(f.checks.len(), 3, "two volume bounds and one delta");
        let judged = f.judge_all(1, &cx);
        let first = judged
            .iter()
            .position(|j| matches!(j, Judged::Refused(_) | Judged::Broken(_)));
        assert_eq!(first, Some(0), "the 24 h turnover is 10 ₽");
        assert_eq!(f.refused(1, &cx).map(|(_, i, _)| i), Some(first));
        // The third check waits rather than refusing: three hours of history
        // the window does not have is not a market outside its corridor.
        assert_eq!(judged[2], Judged::Waiting);
        // A market with no turnover at all reads an honest zero, never a wait.
        assert!(matches!(f.judge_all(3, &cx)[0], Judged::Refused(v) if v == 0.0));
    }

    /// MoonBot's `MinLeverage` / `MaxLeverage` (Filters / Base): a market whose
    /// leverage is outside the corridor is refused, the defaults (1 and 0) and
    /// `IgnoreBase` / `IgnoreFilters` make no check at all.
    #[test]
    fn leverage_corridor_refuses_markets_outside_it() {
        use FieldValue::{Bool, Int32};
        let mut model = model();
        model.at_mut(1).unwrap().bracket_leverage = Some(50);
        model.at_mut(2).unwrap().bracket_leverage = Some(10);
        let win = Windows::default();
        let cx = Ctx::new(&model, &win, 9_000_000_000, None);
        let checks = |fields: &[(&str, FieldValue)]| {
            let st = strategies(fields, true);
            Params::from_snapshot(&st.list()[0], st.schema()).deltas
        };
        assert!(checks(&[]).is_none(), "the defaults filter nothing");
        let min20 = checks(&[("MinLeverage", Int32(20))]).expect("a floor is a check");
        assert_eq!(min20.checks.len(), 1);
        assert!(min20.refused(1, &cx).is_none(), "50x clears a floor of 20");
        let (gate, i, global) = min20.refused(2, &cx).expect("10x is under 20");
        assert_eq!(
            (gate, i, global),
            (Gate::Leverage { leverage: 10 }, Some(0), false)
        );
        assert!(!gate.withdraws());
        let max30 = checks(&[("MaxLeverage", Int32(30))]).expect("a ceiling is a check");
        assert!(matches!(
            max30.refused(1, &cx),
            Some((Gate::Leverage { leverage: 50 }, _, _))
        ));
        assert!(
            max30.refused(2, &cx).is_none(),
            "0 is no ceiling, 10x is under 30"
        );
        assert!(checks(&[("MinLeverage", Int32(20)), ("IgnoreBase", Bool(true))]).is_none());
        assert!(checks(&[("MinLeverage", Int32(20)), ("IgnoreFilters", Bool(true))]).is_none());
    }

    #[test]
    fn market_tags_problem_logged_once() {
        use FieldValue::String as Str;
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let st = strategies(&[(MARKET_TAGS, Str("crypto, fx".into()))], true);
        let t0 = 9_000_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert!(has_log(&cmds, "MarketTags: unknown tag <fx>"));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + REFRESH_MS);
        assert!(!has_log(&cmds, "MarketTags"));
        // A new problem logs without a new revision (`last_date` stays put,
        // as for a file without `LastEditDate`).
        let st = strategies(&[(MARKET_TAGS, Str(String::new()))], true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 2 * REFRESH_MS);
        assert!(has_log(&cmds, "no MarketTags"));
    }

    /// The warning rides the same «say it once» memo as a screener problem,
    /// and rides its OWN: it appears when the word changes, not on every
    /// recompute — a line pushed 1.2 times a second would bury the journal it
    /// was added to — and fixing the word takes it away again.
    #[test]
    fn an_unknown_symbol_is_logged_once() {
        use FieldValue::String as Str;
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 9_000_000_000;
        let st = strategies(&[("CoinsWhiteList", Str("SBER, NOPE".into()))], true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert!(has_log(&cmds, "no such market in the catalog: NOPE"));
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + REFRESH_MS);
        assert!(
            !has_log(&cmds, "no such market"),
            "said once, not every recompute"
        );
        // The word fixed: nothing more to say, and nothing said.
        let st = strategies(&[("CoinsWhiteList", Str("SBER".into()))], true);
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 2 * REFRESH_MS);
        assert!(!has_log(&cmds, "no such market"));
    }

    #[test]
    fn screener_universe_and_delta_shift() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let mut win = Windows::default();
        let now = 9_000_000_000;
        // SGAZP traded 50k turnover in the last hour, SBER 900 (below the floor).
        win.push(2, now - 60_000, 120.0, 400.0);
        win.push(1, now - 60_000, 300.0, 3.0);
        let cx = Ctx {
            model: &model,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        let st = strategies(
            &[
                ("DynWL_SortBy", Str("DailyVol".into())),
                ("DynWL_Count", Int32(1)),
            ],
            true,
        );
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert_eq!(screener::universe(&p, &cx, &none()), [2]);
        // No white list and no count: nothing.
        let st = strategies(&[], true);
        assert!(screener::universe(
            &Params::from_snapshot(&st.list()[0], st.schema()),
            &cx,
            &none()
        )
        .is_empty());
        // A volume bound no longer touches the pool, white list or class:
        // SBER is thin this hour and stays watched, and what the bound does
        // instead is in `the_volume_bounds_are_an_and_not_a_choice`.
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER SGAZP".into())),
                ("MinVolume", Double(1000.0)),
            ],
            true,
        );
        assert_eq!(
            screener::universe(
                &Params::from_snapshot(&st.list()[0], st.schema()),
                &cx,
                &none()
            ),
            [1, 2]
        );
        let st = strategies(&[("CoinsWhiteList", Str("SBER SGAZP".into()))], true);
        assert_eq!(
            screener::universe(
                &Params::from_snapshot(&st.list()[0], st.schema()),
                &cx,
                &none()
            ),
            [1, 2]
        );

        // 15 m excursion: SBER low 300 → last 306 is +2 %; 0.1 per % shifts
        // both corridor bounds by 0.2 %; `MShotAddDistance` 50 % moves the
        // far bound half again as much.
        let st = strategies(&[("MShotAddHourlyDelta", Double(0.0))], true);
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        let (eff, eff_min) = eff_params(&p, 1, 306.0, &cx);
        assert!((eff - 1.1).abs() < 1e-9 && (eff_min - 0.8).abs() < 1e-9);
        assert_eq!(eff_params(&p, 2, 0.0, &cx), (0.9, 0.6));
        let far = Params {
            add_distance: 50.0,
            ..p
        };
        let (eff, eff_min) = eff_params(&far, 1, 306.0, &cx);
        assert!((eff - 1.2).abs() < 1e-9 && (eff_min - 0.8).abs() < 1e-9);
        assert!((factor(10.0, 2) - 1.21).abs() < 1e-9 && factor(0.0, 5) == 1.0);
    }

    /// The day bound and the hourly one are an AND, not a choice, and they
    /// bound the ENTRY rather than the pool: a market that fails one of them
    /// keeps its place in the pool — its subscription, its detect, its live
    /// orders — and simply gets no new entry.
    ///
    /// The hourly window is a slice of the day's, so the day sum is never the
    /// smaller of the two; the only way to fail the day floor while clearing
    /// the hourly one is a day floor set above it, and that case is here too.
    #[test]
    fn the_volume_bounds_are_an_and_not_a_choice() {
        use FieldValue::{Bool, Double, String as Str};
        let model = model();
        let hour = 60 * 60_000;
        let now = 9_000_000_000;
        let mut win = Windows::default();
        // SBER traded 300k turnover three hours ago and nothing since: the day
        // is full, the hour is empty. SGAZP traded 60k a minute ago: both hold.
        // STAR's 6 turnover clear neither.
        win.push(1, now - 3 * hour, 300.0, 1_000.0);
        win.push(2, now - 60_000, 120.0, 500.0);
        win.push(3, now - 60_000, 0.0614, 100.0);
        assert_eq!(win.vol24(1, now), 300_000.0);
        assert_eq!(win.turnover(1, now, 60), 0.0);
        assert_eq!(win.vol24(2, now), 60_000.0);
        assert_eq!(win.turnover(2, now, 60), 60_000.0);
        let cx = Ctx {
            model: &model,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        let params = |fields: &[(&str, FieldValue)]| {
            let st = strategies(fields, true);
            Params::from_snapshot(&st.list()[0], st.schema())
        };
        // Why the market gets no entry, or `None` if it does.
        let refused = |fields: &[(&str, FieldValue)], idx: u16| -> Option<String> {
            let p = params(fields);
            p.deltas
                .as_ref()
                .and_then(|f| f.refused(idx, &cx))
                .map(|(gate, _, _)| gate.reason("M", now))
        };
        // Day floor alone: SBER's empty hour is nobody's business.
        let day = [("MinVolume", Double(50_000.0))];
        assert_eq!(refused(&day, 1), None);
        assert_eq!(refused(&day, 2), None);
        assert_eq!(
            refused(&day, 3).as_deref(),
            Some("Volume 24h 6 USDT is out of range"),
            "turnover are reported as turnover, not as a percentage"
        );
        // Both floors: SBER clears the day and fails the hour. Under an OR
        // the full day would have bought it the silent hour.
        let both = [
            ("MinVolume", Double(50_000.0)),
            ("MinHourlyVolume", Double(10_000.0)),
        ];
        assert_eq!(
            refused(&both, 1).as_deref(),
            Some("Volume 1h 0 USDT is out of range")
        );
        assert_eq!(refused(&both, 2), None);
        // The other way round, and it pins the order too: SGAZP fails the day,
        // which is asked first, so that is what the line says.
        let thin_day = [
            ("MinVolume", Double(100_000.0)),
            ("MinHourlyVolume", Double(10_000.0)),
        ];
        assert_eq!(
            refused(&thin_day, 1).as_deref(),
            Some("Volume 1h 0 USDT is out of range")
        );
        assert_eq!(
            refused(&thin_day, 2).as_deref(),
            Some("Volume 24h 60000 USDT is out of range"),
            "a busy hour does not buy a thin day"
        );
        // The ceilings are read on the same AND, with no floor beside them.
        let hourly_cap = [("MaxHourlyVolume", Double(10_000.0))];
        assert_eq!(refused(&hourly_cap, 1), None);
        assert!(refused(&hourly_cap, 2).is_some());
        let day_cap = [("MaxVolume", Double(100_000.0))];
        assert!(refused(&day_cap, 1).is_some());
        assert_eq!(refused(&day_cap, 2), None);
        // `IgnoreVolume` is the volume half's own switch: it silences these
        // and leaves the deltas alone, where one shared switch used to take
        // both. `IgnoreFilters` still takes the whole tab.
        let with_delta: Vec<(&str, FieldValue)> = vec![
            ("MinVolume", Double(50_000.0)),
            ("Delta_3h_Min", Double(1.0)),
            ("IgnoreVolume", Bool(true)),
        ];
        let p = params(&with_delta);
        assert_eq!(
            p.deltas.as_ref().map(|f| f.checks.len()),
            Some(1),
            "the delta survives its neighbour's switch"
        );
        assert_eq!(p.deltas.as_ref().unwrap().checks[0].what, "Delta_3h");
        let mut off = with_delta.clone();
        off.push(("IgnoreFilters", Bool(true)));
        assert!(params(&off).deltas.is_none());
        // And the pool does not move for any of it: both markets stay watched
        // while one of them is refused.
        let mut listed: Vec<(&str, FieldValue)> =
            vec![("CoinsWhiteList", Str("SBER SGAZP".into()))];
        listed.extend(both);
        let p = params(&listed);
        assert_eq!(screener::universe(&p, &cx, &none()), [1, 2]);
        // …which is exactly what the page and the log line report.
        assert_eq!(
            filter_counts(&p, &[1, 2], &cx, false),
            [("Volume 24h", 0), ("Volume 1h", 1)]
        );
    }

    /// `MaxMarkets` caps the markets the strategy STANDS on, not the pool the
    /// screener watches: the whole pool is watched, entries go out on as many
    /// markets as there are slots, and a freed slot passes to the next market
    /// — a detect taken while every slot was busy is simply gone.
    #[test]
    fn max_markets_caps_the_markets_not_the_pool() {
        use FieldValue::{Double, Int32, String as Str};
        let mut model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER SGAZP STAR".into())),
                ("OrderSize", Double(3000.0)),
                ("OrdersCount", Int32(1)),
                ("MaxMarkets", Int32(1)),
                ("MaxActiveOrders", Int32(10)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
            ],
            true,
        );
        let cx = Ctx {
            model: &model,
            win: &win,
            now: 5_000_000,
            btc: None,
            market_delta: None,
        };
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        assert_eq!(
            screener::universe(&p, &cx, &none()),
            [1, 2, 3],
            "the pool is not truncated"
        );

        let markets = |cmds: &[Cmd]| -> Vec<String> {
            cmds.iter()
                .filter_map(|c| match c {
                    Cmd::Start { order, .. } => Some(order.market.clone()),
                    _ => None,
                })
                .collect()
        };
        let mut shot = MoonShot::default();
        let t = 5_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t);
        assert_eq!(markets(&cmds), ["SBER"], "one slot, one market");
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t);
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t);

        // The slot is taken: the other markets of the pool stay watched and
        // unentered, however many passes go by.
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 1_000);
        assert!(markets(&cmds).is_empty());

        // The entry is gone and SBER stopped trading normally: the freed slot
        // goes to the next market of the pool, not nowhere.
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Cancelled, 1, 0, 0.0),
            t + 2_000,
        );
        model.at_mut(1).unwrap().trading = false;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t + 3_000);
        assert_eq!(markets(&cmds), ["SGAZP"]);
        model.at_mut(1).unwrap().trading = true;

        // 0 = no limit: every market of the pool is entered at once.
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER SGAZP STAR".into())),
                ("OrderSize", Double(3000.0)),
                ("OrdersCount", Int32(1)),
                ("MaxMarkets", Int32(0)),
                ("MaxActiveOrders", Int32(10)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
            ],
            true,
        );
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &Orders::new(), &model, &win, sched, t);
        assert_eq!(markets(&cmds), ["SBER", "SGAZP", "STAR"]);
    }

    fn review_live_position(
        fields: &[(&str, FieldValue)],
    ) -> (Model, Strategies, Orders, MoonShot, u64) {
        use FieldValue::{Double, String as Str};
        let m = model();
        let mut f = vec![
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("MShotAdd15minDelta", Double(0.0)),
            ("MShotAddHourlyDelta", Double(0.0)),
        ];
        f.extend_from_slice(fields);
        let st = strategies(&f, true);
        let mut os = Orders::new();
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_000_000);
        let keys = apply(&mut shot, &mut os, &m, &cmds, 1_000_000);
        let id = os.iter().next().unwrap().id;
        os.apply(
            &report(&keys[0], "910000", ExecStatus::Filled, 1, 1, 300.0),
            1_000_001,
        );
        let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_000_002);
        let keys = apply(&mut shot, &mut os, &m, &cmds, 1_000_002);
        os.apply(
            &report(&keys[0], "910008", ExecStatus::New, 1, 0, 0.0),
            1_000_003,
        );
        (m, st, os, shot, id)
    }

    #[test]
    fn review_price_down_must_not_undo_manual_panic_exit() {
        let (m, st, mut os, mut shot, id) =
            review_live_position(&[("PriceDownTimer", FieldValue::Double(1.0))]);
        let fx = os.set_panic(id, true, m.at(1).unwrap(), 1_002_000);
        assert!(matches!(
            fx.actions.as_slice(),
            [crate::orders::Action::Cancel { .. }]
        ));
        assert!(os.get(id).unwrap().heading(Leg::Sell).unwrap() < 300.0);
        // Neither while the Cancel is out nor after the panic exit is posted.
        for settled in [false, true] {
            if settled {
                settle_exit_move(&mut os, "910008", "910009", 1, 1_002_001);
            }
            let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_002_002);
            assert!(
                !moves(&cmds)
                    .iter()
                    .any(|(oid, leg, p)| *oid == id && *leg == Leg::Sell && *p > 300.0),
                "strategy undoes Panic: {cmds:?}"
            );
        }
    }

    #[test]
    fn review_changing_short_must_cancel_old_direction_entry() {
        use FieldValue::{Bool, Double, String as Str};
        let m = model();
        let mut os = Orders::new();
        let mut shot = MoonShot::default();
        let fields = [
            ("CoinsWhiteList", Str("SBER".into())),
            ("OrderSize", Double(3000.0)),
            ("MShotReplaceDelay", Double(0.0)),
        ];
        let st = strategies(&fields, true);
        let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_000_000);
        let keys = apply(&mut shot, &mut os, &m, &cmds, 1_000_000);
        let id = os.iter().next().unwrap().id;
        os.apply(
            &report(&keys[0], "910000", ExecStatus::New, 1, 0, 0.0),
            1_000_001,
        );
        let mut edited = fields.to_vec();
        edited.push(("Short", Bool(true)));
        let st = strategies(&edited, true);
        let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_001_000);
        assert!(
            cancels(&cmds).contains(&id),
            "old long order is moved above market instead: {cmds:?}"
        );
    }

    #[test]
    fn review_price_down_must_compare_exchange_capped_price() {
        let (mut m, st, mut os, mut shot, id) =
            review_live_position(&[("PriceDownTimer", FieldValue::Double(1.0))]);
        // A SELL is floored by `mark × multiplierDown` and free above it.
        m.at_mut(1).unwrap().set_limits(306.0, 400.0, 0);
        os.reprice(id, Leg::Sell, 306.0);
        let cmds = shot.tick(&st, &os, &m, &Windows::default(), None, 1_002_000);
        assert!(
            moves(&cmds).is_empty(),
            "target will be capped back to 306: {cmds:?}"
        );
    }

    // Review-only checks. This file is inserted into a disposable copy.
    fn reference_params() -> (StrategySnapshot, Params) {
        let st = strategies(&[], true);
        let s = st.list()[0].clone();
        let mut p = Params::from_snapshot(&s, st.schema());
        p.add_15m = 0.0;
        p.add_1h = 0.0;
        p.add_3h = 0.0;
        p.add_btc = 0.0;
        p.add_btc_5m = 0.0;
        p.add_distance = 0.0;
        p.auto_cancel = 0.0;
        p.raise_wait = 0.0;
        p.replace_delay = 0.0;
        p.stops.level = None;
        p.price = 1.0;
        p.price_min = 0.5;
        p.order_size = 6000.0;
        p.orders_count = 1;
        (s, p)
    }

    fn reference_order(
        m: &Model,
        short: bool,
        price: f64,
        size: f64,
        now: i64,
        filled: bool,
    ) -> Orders {
        use crate::orders::Action;
        let mut orders = Orders::new();
        let fx = orders.start(
            42,
            &StartOrder {
                market: "SBER".into(),
                is_short: short,
                use_market_stop: false,
                strategy_id: 7,
                size,
                price,
                planned_sell: 0.0,
                stops: None,
            },
            m.at(1).unwrap(),
            now,
        );
        let (key, lots) = match &fx.actions[0] {
            Action::Post { key, lots, .. } => (key.clone(), *lots),
            other => panic!("{other:?}"),
        };
        orders.apply(
            &report(
                &key,
                "910005",
                if filled {
                    ExecStatus::Filled
                } else {
                    ExecStatus::New
                },
                lots,
                if filled { lots } else { 0 },
                if filled { price } else { 0.0 },
            ),
            now,
        );
        orders
    }

    /// Restored orders: a filled one is announced and its repeat spent, a stop
    /// exit keeps chasing; an unfilled entry keeps both for its own fill.
    #[test]
    fn restore_derives_memo_from_the_orders() {
        let m = model();
        let t = 1_000_000;
        let mut filled = reference_order(&m, false, 300.0, 6000.0, t, true);
        filled.set_sell_reason(42, reason::STOP_LOSS);
        filled.target(42, Leg::Sell, 290.0, None);
        assert_eq!(filled.get(42).unwrap().status, status::SELL_SET);
        let mut shot = MoonShot::default();
        shot.restore(&filled, t);
        let memo = &shot.memo[&42];
        assert!(memo.detected && memo.repeat_used);
        assert_eq!((memo.stops.fired, memo.stops.moved), (Some(Fired::Stop), t));

        let waiting = reference_order(&m, false, 300.0, 6000.0, t, false);
        let mut shot = MoonShot::default();
        shot.restore(&waiting, t);
        let memo = &shot.memo[&42];
        assert!(!memo.detected && !memo.repeat_used && memo.stops.fired.is_none());
    }

    fn reference_manage_entries(
        shot: &mut MoonShot,
        s: &StrategySnapshot,
        p: &Params,
        orders: &Orders,
        m: &Model,
        now: i64,
    ) -> Vec<Cmd> {
        let win = Windows::default();
        let cx = Ctx {
            model: m,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        let market = m.at(1).unwrap();
        let (eff, eff_min) = eff_params(p, 1, market.last(), &cx);
        let corridor = Corridor {
            short: p.short,
            dist: if p.price > 0.0 { eff } else { 0.0 },
            width: (eff - eff_min).max(0.0),
            expand: p.expand,
            follow: true,
            far_capped: true,
            replace_delay: p.replace_delay,
            raise_wait: p.raise_wait,
            kind: "MoonShot",
            hook: None,
        };
        let mut cmds = Vec::new();
        shot.manage_entries(
            s,
            p,
            market,
            &[orders.get(42).unwrap()],
            corridor,
            false,
            &mut 0,
            now,
            &mut cmds,
        );
        cmds
    }

    #[test]
    fn reference_far_boundary_matches_15_september_eth_event() {
        // LOG_2026-09-15.log:1650,1742,1744. Original moved 2466.88 -> 2469.42.
        let (s, mut p) = reference_params();
        p.price = 0.30;
        p.price_min = 0.10;
        let mut m = model();
        // A lot worth a few USDT, as on the real ETH market (the fixture's lot of 10 at this
        // price is 25 000 USDT, and a move is no longer sized past the budget).
        m.at_mut(1).unwrap().step_size = 0.001;
        m.at_mut(1).unwrap().min_qty = 0.001;
        m.at_mut(1).unwrap().last_price = Some(2476.85);
        let orders = reference_order(&m, false, 2466.88, 100_000.0, 1_000_000, false);
        let cmds =
            reference_manage_entries(&mut MoonShot::default(), &s, &p, &orders, &m, 1_001_000);
        assert_eq!(
            moves(&cmds),
            [(42, Leg::Buy, 2469.42)],
            "The original repositions at 0.4025%, but the clone waits for 0.5%"
        );
    }

    /// A move is sized by the rule of a new entry: where the smallest order is dearer than the
    /// budget by more than `LOT_OVER_BUDGET`, the entry stays where it is (it used to be moved
    /// at `max(budget, lot)` — here a lot of 25 000 USDT against a budget of 6000).
    #[test]
    fn a_move_to_a_lot_far_over_the_budget_is_not_made() {
        let (s, mut p) = reference_params();
        p.price = 0.30;
        p.price_min = 0.10;
        let mut m = model();
        m.at_mut(1).unwrap().last_price = Some(2476.85);
        let orders = reference_order(&m, false, 2466.88, 100_000.0, 1_000_000, false);
        let cmds =
            reference_manage_entries(&mut MoonShot::default(), &s, &p, &orders, &m, 1_001_000);
        assert!(moves(&cmds).is_empty(), "{:?}", moves(&cmds));
        assert!(
            cmds.iter()
                .any(|c| matches!(c, Cmd::Log(l) if l.contains("exceeds the budget"))),
            "said once"
        );
    }

    #[test]
    fn reference_automatic_reprice_preserves_order_budget() {
        // LOG_2026-09-13.log:23,31 changes both quantity and price to keep ~4950 USDT.
        let (s, p) = reference_params();
        let mut m = model();
        let mut orders = reference_order(&m, false, 297.0, 6000.0, 1_000_000, false);
        m.at_mut(1).unwrap().last_price = Some(400.0);
        let cmds =
            reference_manage_entries(&mut MoonShot::default(), &s, &p, &orders, &m, 1_001_000);
        assert_eq!(moves(&cmds), [(42, Leg::Buy, 396.0)]);
        apply(&mut MoonShot::default(), &mut orders, &m, &cmds, 1_001_000);
        let rec = orders.get(42).unwrap().record();
        let actual_budget = rec.buy.quantity * rec.buy.price;
        assert!(
            actual_budget <= p.order_size,
            "actual {actual_budget}, configured {}",
            p.order_size
        );
    }

    #[test]
    fn reference_zero_price_down_delay_continues_after_first_step() {
        // faqen.tsv:1366: delay=0 continues at the minimum interval, it does not stop.
        let (s, mut p) = reference_params();
        p.sell_price = 2.0;
        p.pd_timer = 1.0;
        p.pd_delay = 0.0;
        p.pd_pct = 0.5;
        p.pd_relative = false;
        p.pd_drop = 0.0;
        let m = model();
        let t = 1_000_000;
        let mut orders = reference_order(&m, false, 300.0, 6000.0, t, true);
        let keys = apply(
            &mut MoonShot::default(),
            &mut orders,
            &m,
            &[Cmd::Move {
                order: 42,
                leg: Leg::Sell,
                price: 306.0,
                reason: reason::SELL_PRICE,
                market: false,
            }],
            t,
        );
        orders.apply(&report(&keys[0], "910007", ExecStatus::New, 2, 0, 0.0), t);
        let mut shot = MoonShot::default();
        let mut first = Vec::new();
        shot.manage_exit(
            &s,
            orders.get(42).unwrap(),
            &p,
            m.at(1).unwrap(),
            true,
            ExitFeed::default(),
            t + 1000,
            &mut first,
        );
        assert_eq!(moves(&first), [(42, Leg::Sell, 304.47)]);
        apply(&mut shot, &mut orders, &m, &first, t + 1000);
        let mut second = Vec::new();
        shot.manage_exit(
            &s,
            orders.get(42).unwrap(),
            &p,
            m.at(1).unwrap(),
            true,
            ExitFeed::default(),
            t + 2000,
            &mut second,
        );
        assert!(
            moves(&second)
                .iter()
                .any(|(_, leg, price)| *leg == Leg::Sell && *price < 304.47),
            "delay=0 must continue reducing: {second:?}"
        );
    }

    #[test]
    fn reference_delta_uses_window_range_described_in_faq() {
        // faqen.tsv:1050,1287. Documented delta is (high/low - 1)*100.
        let (_, mut p) = reference_params();
        p.add_1h = 0.1;
        let m = model();
        let now = 10_000_000;
        let mut win = Windows::default();
        win.seed(1, now - 60_000, 100.0, 90.0, 110.0, 1.0);
        let cx = Ctx {
            model: &m,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        let actual = eff_params(&p, 1, 100.0, &cx);
        let expected_shift = (110.0 / 90.0 - 1.0) * 100.0 * p.add_1h;
        assert!(
            (actual.0 - (p.price + expected_shift)).abs() < 1e-9,
            "actual {actual:?}, expected shift {expected_shift}"
        );
    }

    #[test]
    fn reference_manual_entry_move_resets_auto_cancel_clock() {
        // faqen.tsv:1137: manual reposition resets the auto-cancel countdown.
        let (s, mut p) = reference_params();
        p.auto_cancel = 90.0;
        let m = model();
        let t = 1_000_000;
        let mut orders = reference_order(&m, false, 297.0, 6000.0, t, false);
        // Same price-only target path as a manual TargetBuy with an unchanged size.
        let fx = orders.target_manual(42, Leg::Buy, 296.99, None, t + 89_000);
        let crate::orders::Action::Replace { key, lots, .. } = &fx.actions[0] else {
            panic!("{fx:?}");
        };
        orders.apply(
            &report(key, "910006", ExecStatus::New, *lots, 0, 0.0),
            t + 89_000,
        );
        let cmds =
            reference_manage_entries(&mut MoonShot::default(), &s, &p, &orders, &m, t + 91_000);
        assert!(
            cancels(&cmds).is_empty(),
            "manual move was just 2 seconds ago: {cmds:?}"
        );
    }

    #[test]
    fn reference_relative_price_down_replays_67_original_steps() {
        let (_, mut p) = reference_params();
        p.pd_relative = true;
        let mut m = model();
        let market = m.at_mut(1).unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/moonshot-price-down.json"))
                .unwrap();
        let rows = fixture["price_down"].as_array().unwrap();
        assert_eq!(rows.len(), 67);
        let mut failures = Vec::new();
        for r in rows {
            let num = |key: &str| r[key].as_f64().unwrap();
            let short = r["short"].as_bool().unwrap();
            let (entry, initial, tick) = (num("entry"), num("initial"), num("tick"));
            market.tick_size = tick;
            p.sell_price = if short {
                (entry / initial - 1.0) * 100.0
            } else {
                (initial / entry - 1.0) * 100.0
            };
            p.pd_pct = num("percent");
            let predicted = price_down(
                &p,
                market,
                short,
                entry,
                p.sell_price,
                r["steps"].as_i64().unwrap(),
                market.nearest(num("floor")),
            );
            let actual = num("actual");
            if (predicted - actual).abs() > tick * 0.01 {
                failures.push(format!(
                    "{}:{}: predicted {predicted}, actual {actual}",
                    r["file"], r["line"]
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {} mismatches: {failures:?}",
            failures.len(),
            rows.len()
        );
    }

    #[test]
    fn far_hysteresis_covers_observed_narrow_and_wide_corridors() {
        for (price, near, far) in [
            (0.30, 0.10, 0.40),
            (0.90, 0.85, 0.95),
            (1.10, 1.05, 1.15),
            (1.25, 1.20, 1.30),
            (11.0, 10.0, 12.0),
        ] {
            assert!((far_distance(price, near) - far).abs() < 1e-9);
        }
        let (s, mut p) = reference_params();
        p.short = true;
        p.price = 0.3;
        p.price_min = 0.1;
        let mut m = model();
        // A lot worth a few dozen USDT, as on the real BTC market.
        m.at_mut(1).unwrap().step_size = 0.0001;
        m.at_mut(1).unwrap().min_qty = 0.0001;
        m.at_mut(1).unwrap().last_price = Some(76894.70);
        m.at_mut(1).unwrap().tick_size = 0.1;
        let os = reference_order(&m, true, 77203.20, 1_000_000.0, 1_000_000, false);
        let cmds = reference_manage_entries(&mut MoonShot::default(), &s, &p, &os, &m, 1_001_000);
        assert_eq!(moves(&cmds), [(42, Leg::Buy, 77125.40)]);
    }

    /// `MShotUsePrice`: the entry is measured from the chosen book side
    /// (bid by default), from the last trade with `Trade` or without a book.
    #[test]
    fn use_price_anchors_entry_at_bid_ask_or_trade() {
        use FieldValue::{Double, String as Str};
        let mut model = model();
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask, m.last_price) = (Some(299.0), Some(301.0), Some(300.0));
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let entry = |model: &Model, use_price: Option<&str>| {
            let mut fields = vec![
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotPriceMin", Double(0.5)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
            ];
            if let Some(v) = use_price {
                fields.push(("MShotUsePrice", Str(v.into())));
            }
            let st = strategies(&fields, true);
            let cmds = MoonShot::default().tick(&st, &orders, model, &win, sched, 1_000_000);
            starts(&cmds)[0].0
        };
        assert_eq!(entry(&model, None), 296.01);
        assert_eq!(entry(&model, Some("bid")), 296.01);
        assert_eq!(entry(&model, Some("ASK")), 297.99);
        assert_eq!(entry(&model, Some("Trade")), 297.0);
        let m = model.at_mut(1).unwrap();
        (m.bid, m.ask) = (None, None);
        assert_eq!(entry(&model, None), 297.0);
    }

    /// Bid/ask-anchored strategies need the books of their universe before
    /// any core order subscribes the market; `Trade` ones do not.
    #[test]
    fn book_markets_follow_bid_ask_universes() {
        use FieldValue::String as Str;
        let model = model();
        let (win, sched) = (Windows::default(), None::<f64>);
        let orders = Orders::new();
        let mut shot = MoonShot::default();
        let mut st = strategies(&[("CoinsWhiteList", Str("SGAZP".into()))], true);
        shot.tick(&st, &orders, &model, &win, sched, 1_000_000);
        assert_eq!(shot.book_markets(&st), BTreeSet::from([2]));
        st.set_running(false);
        assert!(shot.book_markets(&st).is_empty());
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SGAZP".into())),
                ("MShotUsePrice", Str("Trade".into())),
            ],
            true,
        );
        let mut shot = MoonShot::default();
        shot.tick(&st, &orders, &model, &win, sched, 1_000_000);
        assert!(shot.book_markets(&st).is_empty());
    }
    /// M6: the dynamic lists ARE the pool of a class strategy — the white one
    /// ranks the class and keeps the head of its sort, the black one is taken
    /// the same way and subtracted, and an unknown key stops the strategy
    /// instead of guessing a ranking.
    #[test]
    fn dynamic_lists_keep_the_head_and_subtract_the_black_one() {
        use FieldValue::{Bool, Int32, String as Str};
        let model = model();
        let mut win = Windows::default();
        let now = 9_000_000_000;
        let hour = 60 * 60_000;
        // One trade an hour back opens each window; the model's `last` closes
        // it. SBER 250 -> 300 (+20%), SGAZP 120 -> 120 (0%), STAR 0.07 ->
        // 0.0614 (-12%). Day turnover ranks them STAR, SBER, SGAZP.
        win.push(1, now - hour + 1_000, 250.0, 100.0);
        win.push(2, now - hour + 1_000, 120.0, 100.0);
        win.push(3, now - hour + 1_000, 0.07, 1_000_000.0);
        let cx = Ctx {
            model: &model,
            win: &win,
            now,
            btc: None,
            market_delta: None,
        };
        // The base is a white list wide enough to hold the whole class, so
        // the cases below vary one thing at a time; `fields` wins over it.
        let pool = |fields: &[(&str, FieldValue)]| {
            let mut all: Vec<(&str, FieldValue)> = vec![
                (MARKET_TAGS, Str("crypto".into())),
                ("DynWL_SortBy", Str("DailyVol".into())),
                ("DynWL_Count", Int32(9)),
            ];
            all.extend(fields.iter().cloned());
            let st = strategies(&all, true);
            screener::universe(
                &Params::from_snapshot(&st.list()[0], st.schema()),
                &cx,
                &none(),
            )
        };
        assert_eq!(pool(&[]), [3, 1, 2], "the ranking's own order");
        let by_hour = |white: bool, count: i32, desc: bool| -> Vec<(&'static str, FieldValue)> {
            let (c, by, order) = if white {
                ("DynWL_Count", "DynWL_SortBy", "DynWL_SortDesc")
            } else {
                ("DynBL_Count", "DynBL_SortBy", "DynBL_SortDesc")
            };
            vec![
                (c, Int32(count)),
                (by, Str("Last1hDelta".into())),
                (order, Bool(desc)),
            ]
        };
        // The head of the sort, in the sort's order: it is the strategy's
        // order of preference, not the turnover's.
        assert_eq!(pool(&by_hour(true, 2, true)), [1, 2]);
        assert_eq!(pool(&by_hour(true, 2, false)), [3, 2]);
        assert_eq!(
            pool(&by_hour(true, 9, true)),
            [1, 2, 3],
            "count past the pool"
        );
        // The black list takes its own head off the filtered pool.
        assert_eq!(pool(&by_hour(false, 1, true)), [3, 2]);
        // Both: the white head minus the black one.
        let mut both = by_hour(true, 2, true);
        both.extend(by_hour(false, 1, true));
        assert_eq!(pool(&both), [2]);
        // A key this core cannot answer: no markets, never a guessed order.
        assert!(pool(&[
            ("DynWL_Count", Int32(2)),
            ("DynWL_SortBy", Str("MarkPrice".into())),
        ])
        .is_empty());
        // A market the key cannot measure (no window to open from) is a
        // candidate for neither list: STAR keeps its turnover but loses its
        // delta, so a top-2 by the hour is SBER and SGAZP alone.
        let mut quiet = Windows::default();
        quiet.push(1, now - hour + 1_000, 250.0, 100.0);
        quiet.push(2, now - hour + 1_000, 120.0, 100.0);
        quiet.seed(3, now - hour + 1_000, 0.0, 0.06, 0.08, 70_000.0);
        let cx = Ctx {
            model: &model,
            win: &quiet,
            now,
            btc: None,
            market_delta: None,
        };
        let st = strategies(&by_hour(true, 3, true), true);
        assert_eq!(
            screener::universe(
                &Params::from_snapshot(&st.list()[0], st.schema()),
                &cx,
                &none()
            ),
            [1, 2],
            "the unmeasured market is not ranked as a flat one"
        );
        // The same key with the list off is nobody's business — and a white
        // list is what "off" needs, because it is its own bound where a class
        // has none.
        assert_eq!(
            pool(&[
                ("CoinsWhiteList", Str("SBER SGAZP STAR".into())),
                ("DynWL_Count", Int32(0)),
                ("DynWL_SortBy", Str("MarkPrice".into())),
            ]),
            [1, 2, 3],
            "the list's order, since no list ranked it"
        );
    }

    /// A filter of the Filters tab holds the NEXT entry back and leaves the
    /// live ones where they are: no cancel, and no `FilterCheck` line, which
    /// would claim a withdrawal that did not happen. The market keeps its
    /// place in the pool throughout — it is the entry that is gated, not the
    /// watching.
    ///
    /// This is the deliberate departure from MoonBot, which takes the entry
    /// off for any filter (`Gate::withdraws`).
    #[test]
    fn a_filter_holds_the_next_entry_back_and_leaves_the_live_one() {
        use FieldValue::{Double, Int32, String as Str};
        let model = model();
        let sched = None::<f64>;
        let mut orders = Orders::new();
        let hour = 60 * 60_000;
        let t = 9_000_000_000;
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("OrdersCount", Int32(2)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MinHourlyVolume", Double(10_000.0)),
            ],
            true,
        );
        // Busy: 300k turnover inside the hour. Quiet: the same 300k, but three
        // hours ago — the day is full and the hour is empty.
        let mut busy = Windows::default();
        busy.push(1, t - 60_000, 300.0, 1_000.0);
        let mut quiet = Windows::default();
        quiet.push(1, t - 3 * hour, 300.0, 1_000.0);
        let starts = |cmds: &[Cmd]| {
            cmds.iter()
                .filter(|c| matches!(c, Cmd::Start { .. }))
                .count()
        };

        // The hour is empty: nothing starts, and the market is watched all
        // the same — a pool of one, with the filter holding all of it.
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &quiet, sched, t);
        assert_eq!(starts(&cmds), 0);
        assert_eq!(shot.pool_filters(7), (1, vec![("Volume 1h", 1)]));
        assert!(has_log(&cmds, "pool 1, 1 filtered (Volume 1h 1)"));

        // The hour fills: the ladder goes out.
        let cmds = shot.tick(&st, &orders, &model, &busy, sched, t + 1_000);
        assert_eq!(starts(&cmds), 2);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t + 1_000);
        for (k, ex) in keys.iter().zip(["900000", "900001"]) {
            orders.apply(&report(k, ex, ExecStatus::New, 1, 0, 0.0), t + 1_000);
        }

        // The hour empties again: both entries stay on the book, nothing is
        // cancelled, and nothing claims a withdrawal.
        let cmds = shot.tick(&st, &orders, &model, &quiet, sched, t + 2_000);
        assert!(
            !cmds.iter().any(|c| matches!(c, Cmd::Cancel { .. })),
            "{cmds:#?}"
        );
        assert!(!has_log(&cmds, "FilterCheck"));
        assert_eq!(
            orders
                .iter()
                .filter(|o| o.status == status::BUY_SET)
                .count(),
            2
        );

        // A gate that is NOT a filter still takes them off, so the entries
        // above stayed because of the filter and not because nothing here
        // cancels at all.
        let mut stopped = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("OrdersCount", Int32(2)),
                ("MinHourlyVolume", Double(10_000.0)),
            ],
            true,
        );
        stopped.set_running(false);
        let cmds = shot.tick(&stopped, &orders, &model, &quiet, sched, t + 3_000);
        assert_eq!(
            cmds.iter()
                .filter(|c| matches!(c, Cmd::Cancel { .. }))
                .count(),
            2
        );
        assert!(has_log(&cmds, "FilterCheck"));
    }

    /// The repeat-after-buy grant is a new entry too, so a market the
    /// filters refuse does not get one — and the grant is not spent by the
    /// refusal, it waits for the filter to clear.
    ///
    /// Grants do not take `MaxActiveOrders` slots, which is why this path
    /// carries a budget of its own and needed telling about the hold-back
    /// separately; before the diff every filter withdrew, so this door was
    /// never reached with one in force.
    #[test]
    fn a_filter_holds_back_the_repeat_after_buy_grant() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let model = model();
        let sched = None::<f64>;
        let mut orders = Orders::new();
        let hour = 60 * 60_000;
        let t0 = 9_000_000_000;
        let st = strategies(
            &[
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotPrice", Double(1.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("MaxActiveOrders", Int32(1)),
                ("MShotRepeatAfterBuy", Bool(true)),
                ("UseStopLoss", Bool(false)),
                ("PriceDownTimer", Double(0.0)),
                ("MinHourlyVolume", Double(10_000.0)),
            ],
            true,
        );
        let mut busy = Windows::default();
        busy.push(1, t0 - 60_000, 300.0, 1_000.0);
        let mut quiet = Windows::default();
        quiet.push(1, t0 - 3 * hour, 300.0, 1_000.0);
        let mut shot = MoonShot::default();

        let cmds = shot.tick(&st, &orders, &model, &busy, sched, t0);
        assert_eq!(starts(&cmds), [(297.0, 3000.0)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        let first = orders.iter().next().unwrap().id;
        orders.apply(
            &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
            t0 + 1_000,
        );
        // The fill's exit goes out; the grant comes with it being placed.
        let cmds = shot.tick(&st, &orders, &model, &busy, sched, t0 + 1_500);
        assert_eq!(moves(&cmds), [(first, Leg::Sell, 302.94)]);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0 + 1_500);
        orders.apply(
            &report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0),
            t0 + 1_500,
        );

        // The hour goes empty before the grant is served: no new entry, and
        // the exit standing over the position is not touched.
        let cmds = shot.tick(&st, &orders, &model, &quiet, sched, t0 + 1_600);
        assert!(starts(&cmds).is_empty(), "{cmds:#?}");
        assert!(cancels(&cmds).is_empty(), "{cmds:#?}");
        // The hour fills again and the grant is still there to be served.
        let cmds = shot.tick(&st, &orders, &model, &busy, sched, t0 + 1_700);
        assert_eq!(
            starts(&cmds),
            [(297.0, 3000.0)],
            "the refusal held the grant, it did not spend it"
        );
    }

    /// A ranked pool waits for the warm-up. Every volume key reads an honest
    /// zero out of an empty window, so a `DailyVol` ranking taken before the
    /// bars land ties the whole class and keeps whichever markets the catalog
    /// listed first — subscribed, sampled, and swapped out wholesale one
    /// recompute later. A written white list measures nothing and does not
    /// wait.
    #[test]
    fn a_ranked_pool_waits_for_the_warm_up() {
        use FieldValue::{Int32, String as Str};
        let model = model();
        let (sched, orders) = (None::<f64>, Orders::new());
        let mut win = Windows::default();
        let t0 = 9_000_000_000;
        win.push(1, t0 - 60_000, 300.0, 100.0);
        let ranked = strategies(
            &[
                ("DynWL_SortBy", Str("DailyVol".into())),
                ("DynWL_Count", Int32(2)),
            ],
            true,
        );
        let listed = strategies(&[("CoinsWhiteList", Str("SBER SGAZP".into()))], true);
        let pool = |st: &Strategies, warming: bool| -> usize {
            let mut shot = MoonShot::default();
            shot.set_warming(warming);
            shot.tick(st, &orders, &model, &win, sched, t0);
            shot.pool_filters(7).0
        };
        assert_eq!(pool(&ranked, true), 0, "no ranking off an empty window");
        assert_eq!(pool(&ranked, false), 2);
        assert_eq!(
            pool(&listed, true),
            2,
            "a list the operator wrote needs no measurement"
        );
    }

    /// A market already in the pool keeps its place until it falls a fifth
    /// past `DynWL_Count`, and it pays for that with the slot of the
    /// WORST-ranked newcomer, never a better one — so the top of the ranking
    /// is never held out and the pool is never longer than the count.
    ///
    /// Without the hold the pool trades its own boundary: the markets around
    /// rank `Count` swap on every recompute, and a market that leaves loses
    /// its book subscription, its hook tape and its strike track.
    #[test]
    fn the_pool_holds_its_members_against_the_boundary() {
        use FieldValue::{Int32, String as Str};
        let tickers: Vec<String> = (1..=8).map(|i| format!("M{i}")).collect();
        let specs: Vec<(&str, &str, f64, f64)> = tickers
            .iter()
            .map(|t| (t.as_str(), t.as_str(), 0.01, 1.0))
            .collect();
        let mut model = fixtures::sber_catalog_of(&specs);
        // BTCUSDT sits at index 0 as TInvestCore's service market did, and
        // is no candidate of this pool.
        model.at_mut(0).unwrap().tags = vec![crate::model::Tag::Top];
        let now = 9_000_000_000;
        // A count of 5 gives a band of 6: 5 + ⌈5/5⌉.
        let st = strategies(
            &[
                ("DynWL_SortBy", Str("DailyVol".into())),
                ("DynWL_Count", Int32(5)),
            ],
            true,
        );
        let p = Params::from_snapshot(&st.list()[0], st.schema());
        // Turnover straight off the quantity: price 1 makes the USDT sum
        // the number written here.
        let pool = |vols: &[f64], prev: &[u16]| -> Vec<u16> {
            let mut win = Windows::default();
            for (i, v) in vols.iter().enumerate() {
                win.push(i as u16 + 1, now - 60_000, 1.0, *v);
            }
            let cx = Ctx {
                model: &model,
                win: &win,
                now,
                btc: None,
                market_delta: None,
            };
            screener::universe(&p, &cx, &prev.iter().copied().collect())
        };
        // A first recompute has nobody to hold: the plain head of the sort.
        let first = pool(&[80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0], &[]);
        assert_eq!(first, [1, 2, 3, 4, 5]);
        // M1 falls to rank 6 — inside the band, so it stays, and what it
        // costs is M6: the worst-ranked market that was not in the pool.
        let held = pool(&[25.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0], &first);
        assert_eq!(held, [2, 3, 4, 5, 1], "rank 6 of a top 5 is still in");
        assert_eq!(held.len(), 5, "the count is a hard ceiling, not a target");
        // Rank 7 is past the band: it leaves, and M6 takes the slot back.
        assert_eq!(
            pool(&[15.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0], &first),
            [2, 3, 4, 5, 6]
        );
        // The hold never keeps the top out: M8 jumps to first and is in the
        // same pass, while M1 (rank 7 now) is the one that goes.
        assert_eq!(
            pool(&[25.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 99.0], &held),
            [8, 2, 3, 4, 5]
        );
    }

    /// The unknown key reaches the terminal's log, once, like an unknown tag.
    #[test]
    fn an_unknown_sort_key_is_logged_once() {
        use FieldValue::{Int32, String as Str};
        let model = model();
        let mut win = Windows::default();
        let (orders, sched) = (Orders::new(), None::<f64>);
        let t0 = 9_000_000_000;
        win.push(1, t0 - 60_000, 300.0, 100.0);
        let st = strategies(
            &[
                ("DynWL_Count", Int32(2)),
                ("DynWL_SortBy", Str("MarkPrice".into())),
            ],
            true,
        );
        let mut shot = MoonShot::default();
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        assert!(has_log(&cmds, "DynWL_SortBy: unknown sort key <MarkPrice>"));
        assert!(
            shot.book_markets(&st).is_empty(),
            "no markets while it holds"
        );
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + REFRESH_MS);
        assert!(
            !has_log(&cmds, "DynWL_SortBy"),
            "logged once, not every pass"
        );
    }

    /// M6: `Dyn_Refresh` is the recompute period — floored at 30 s and landing
    /// on a grid every strategy of that period shares, so N strategies move the
    /// subscription sets in one pass instead of at N moments of their own.
    #[test]
    fn dyn_refresh_floors_the_period_and_lands_on_a_shared_grid() {
        use FieldValue::Int32;
        let model = model();
        let mut win = Windows::default();
        let (orders, sched) = (Orders::new(), None::<f64>);
        // A cell boundary of both the 30 s floor and the 61 s default.
        let t0 = 9_150_000_000;
        win.push(1, t0 - 60_000, 300.0, 100.0);
        let sizes = |shot: &MoonShot, st: &Strategies| -> Vec<usize> {
            shot.pools(st).into_iter().map(|(_, n)| n).collect()
        };
        // Below the floor: 1 s is read as 30 s.
        let st = strategies(
            &[("DynWL_Count", Int32(50)), ("Dyn_Refresh", Int32(1))],
            true,
        );
        let mut shot = MoonShot::default();
        shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(sizes(&shot, &st), [1]);
        win.push(2, t0 + 1_000, 120.0, 100.0);
        shot.tick(&st, &orders, &model, &win, sched, t0 + 29_000);
        assert_eq!(sizes(&shot, &st), [1], "same 30 s cell: no recompute");
        shot.tick(&st, &orders, &model, &win, sched, t0 + 30_000);
        assert_eq!(sizes(&shot, &st), [2], "the next cell recomputes");
        // Milliseconds typed where seconds belong: the ceiling keeps the pool
        // from freezing for good (`(f64 * 1000.0) as i64` saturates, and an
        // unclamped period stops `now / period` from ever changing).
        let mut win = Windows::default();
        win.push(1, t0 - 60_000, 300.0, 100.0);
        let st = strategies(
            &[("DynWL_Count", Int32(50)), ("Dyn_Refresh", Int32(86_400))],
            true,
        );
        let mut shot = MoonShot::default();
        shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(sizes(&shot, &st), [1]);
        win.push(2, t0 + 1_000, 120.0, 100.0);
        shot.tick(&st, &orders, &model, &win, sched, t0 + 3_600_000);
        assert_eq!(sizes(&shot, &st), [2], "an hour is the longest it may hold");
        // Two strategies of the same period, last recomputed at different
        // moments, still move together on the grid's next cell.
        let mut win = Windows::default();
        win.push(1, t0 - 60_000, 300.0, 100.0);
        let st = two_strategies(&[("DynWL_Count", Int32(50))]);
        let mut shot = MoonShot::default();
        shot.tick(&st, &orders, &model, &win, sched, t0);
        assert_eq!(sizes(&shot, &st), [1, 1]);
        shot.rt.get_mut(&8).unwrap().universe_at = t0 + 20_000;
        win.push(2, t0 + 1_000, 120.0, 100.0);
        shot.tick(&st, &orders, &model, &win, sched, t0 + 61_000);
        assert_eq!(
            sizes(&shot, &st),
            [2, 2],
            "a stopwatch each would have held the second one back"
        );
    }

    /// Two MoonShot strategies (ids 7 and 8) sharing the same fields.
    fn two_strategies(fields: &[(&str, FieldValue)]) -> Strategies {
        let mut st = Strategies::new(None, 0);
        let snap = |id: u64, name: &str| {
            let mut f = StrategyFields::new();
            f.insert("StrategyName", FieldValue::String(name.into()));
            for (k, v) in fields {
                f.insert(*k, v.clone());
            }
            StrategySnapshot::new(id, 1, 10, true, StrategyKind::MOON_SHOT, "", f)
        };
        st.apply_snapshot(&Snapshot {
            server_epoch: 1,
            client_max_last_date: 10,
            full: true,
            data: strat_codec::encode_batch(
                st.schema(),
                &[snap(7, "A"), snap(8, "B")],
                std::iter::empty(),
            ),
            folders_last_modified: 1,
        });
        st.set_running(true);
        st
    }

    /// A long SBER position filled at 297.00 at `t0` under the base fields
    /// and `extra` (no PriceDown, `StopLossSpread` 1 %), and the pass at
    /// `t0 + 500` that places its exit (its commands are returned).
    struct StopFx {
        st: Strategies,
        model: Model,
        orders: Orders,
        shot: MoonShot,
        t0: i64,
        first: Vec<Cmd>,
    }

    impl StopFx {
        fn new(extra: &[(&str, FieldValue)]) -> Self {
            use FieldValue::{Double, String as Str};
            let mut fields = vec![
                ("CoinsWhiteList", Str("SBER".into())),
                ("OrderSize", Double(3000.0)),
                ("MShotAdd15minDelta", Double(0.0)),
                ("MShotAddHourlyDelta", Double(0.0)),
                ("StopLossSpread", Double(1.0)),
                ("PriceDownTimer", Double(0.0)),
            ];
            fields.extend(extra.iter().cloned());
            let mut model = model();
            let st = strategies(&fields, true);
            let mut orders = Orders::new();
            let mut shot = MoonShot::default();
            let t0 = 8_000_000;
            let (win, sched) = (Windows::default(), None::<f64>);
            let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
            let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
            orders.apply(
                &report(&keys[0], "900000", ExecStatus::Filled, 1, 1, 297.0),
                t0,
            );
            let m = model.at_mut(1).unwrap();
            (m.bid, m.ask, m.last_price) = (Some(297.0), Some(297.1), Some(297.0));
            let first = shot.tick(&st, &orders, &model, &win, sched, t0 + 500);
            let keys = apply(&mut shot, &mut orders, &model, &first, t0 + 500);
            orders.apply(
                &report(&keys[0], "900001", ExecStatus::New, 1, 0, 0.0),
                t0 + 500,
            );
            Self {
                st,
                model,
                orders,
                shot,
                t0,
                first,
            }
        }

        /// A pass `dt` ms after the fill with the bid at `bid` (the ask a
        /// tick above, the last trade at the bid).
        fn at(&mut self, dt: i64, bid: f64) -> Vec<Cmd> {
            let m = self.model.at_mut(1).unwrap();
            (m.bid, m.ask, m.last_price) = (Some(bid), Some(bid + 0.01), Some(bid));
            let (win, sched) = (Windows::default(), None::<f64>);
            let now = self.t0 + dt;
            let cmds = self
                .shot
                .tick(&self.st, &self.orders, &self.model, &win, sched, now);
            apply(&mut self.shot, &mut self.orders, &self.model, &cmds, now);
            cmds
        }
    }

    /// The order kind of every exit move: `true` = MARKET.
    fn exit_kinds(cmds: &[Cmd]) -> Vec<bool> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Move {
                    leg: Leg::Sell,
                    market,
                    ..
                } => Some(*market),
                _ => None,
            })
            .collect()
    }

    /// Aster: a fired stop of a live order goes at MARKET (`PLAN.md`,
    /// «Открытые решения» п. 2) — unless its exit is held back by the
    /// allowed drop, which only a limit can keep. An emulated order goes at
    /// MARKET as well (since 03.10; before, it was a limit the emulator
    /// filled at its own price). The price beside it is
    /// the limit TInvestCore would have placed: the band check and the image
    /// read it, and the chase falls back to it.
    #[test]
    fn a_live_stop_goes_at_market_unless_a_limit_must_hold_it() {
        use FieldValue::Double;
        let fire = |extra: &[(&str, FieldValue)], emulated: bool| {
            let mut fields = vec![("StopLoss", Double(-1.0)), ("StopLossSpread", Double(5.0))];
            fields.extend(extra.iter().cloned());
            let mut fx = StopFx::new(&fields);
            if emulated {
                let id = fx.orders.iter().next().unwrap().id;
                fx.orders.set_emulator(id);
            }
            fx.at(1_000, 293.0);
            exit_kinds(&fx.at(3_000, 293.0))
        };
        assert_eq!(fire(&[], false), [true], "a live stop crosses at market");
        assert_eq!(fire(&[], true), [true], "and so does an emulated one");
        assert_eq!(
            fire(&[("AllowedDrop", Double(-2.0))], false),
            [false],
            "the allowed drop holds the exit back: a limit"
        );
    }

    /// The market delta is the mean hourly move of the live markets with an
    /// hour of history, and nothing below `MARKET_DELTA_MIN` of them.
    #[test]
    fn the_market_delta_is_the_mean_of_the_live_markets() {
        let specs: Vec<(String, String)> = (0..MARKET_DELTA_MIN + 1)
            .map(|i| (format!("C{i:02}"), format!("C{i:02}")))
            .collect();
        let rows: Vec<(&str, &str, f64, f64)> = specs
            .iter()
            .map(|(s, b)| (s.as_str(), b.as_str(), 0.01, 1.0))
            .collect();
        let mut model = fixtures::sber_catalog_of(&rows);
        let mut win = Windows::default();
        let now = 9_000_000_000;
        // Every coin from 100 an hour back to 102 now (+2 %), BTCUSDT none.
        for i in 1..=rows.len() as u16 {
            win.push(i, now - 3_600_000, 100.0, 1.0);
            model.at_mut(i).unwrap().last_price = Some(102.0);
        }
        let d = market_delta(&model, &win, now).unwrap();
        assert!((d - 2.0).abs() < 1e-9, "{d}");
        // A market that stopped trading, or whose tape died, is not the market.
        model.at_mut(1).unwrap().trading = false;
        model.at_mut(2).unwrap().feed_fresh = false;
        assert_eq!(market_delta(&model, &win, now), None, "under the minimum");
    }

    fn exit_moves(cmds: &[Cmd]) -> Vec<(f64, u8)> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Move {
                    leg: Leg::Sell,
                    price,
                    reason,
                    ..
                } => Some((*price, *reason)),
                _ => None,
            })
            .collect()
    }

    /// Lines sent to the order image, to 0.001.
    fn shown_stops(cmds: &[Cmd]) -> Vec<f64> {
        cmds.iter()
            .filter_map(|c| match c {
                Cmd::Stop { price, .. } => Some((price * 1000.0).round() / 1000.0),
                _ => None,
            })
            .collect()
    }

    /// FAQ: after `TimeToSwitch2Stop` s, once the price is past
    /// `PriceToSwitch2Stop`, stop 1 moves to `SecondStopLoss` — a breakeven stop.
    #[test]
    fn second_stop_moves_the_line_to_breakeven() {
        use FieldValue::{Bool, Double, Int32};
        let mut fx = StopFx::new(&[
            ("UseSecondStop", Bool(true)),
            ("TimeToSwitch2Stop", Int32(5)),
            ("PriceToSwitch2Stop", Double(1.0)),
            ("SecondStopLoss", Double(0.3)),
        ]);
        assert_eq!(shown_stops(&fx.first), [291.06]);
        // +1.2 % but before its time: stop 1 stays.
        assert!(shown_stops(&fx.at(3_000, 300.5)).is_empty());
        fx.at(5_000, 300.5);
        let cmds = fx.at(6_000, 299.0);
        assert_eq!(shown_stops(&cmds), [297.891]);
        assert!(has_log(
            &cmds,
            "SBER: StopLoss2 applied (buyPrice 297.00 stop +0.30% => 297.89)"
        ));
        assert!(exit_moves(&fx.at(7_000, 297.5)).is_empty(), "armed");
        let cmds = fx.at(9_000, 297.5);
        assert_eq!(exit_moves(&cmds), [(294.52, reason::STOP_LOSS)]);
        assert!(has_log(
            &cmds,
            "StopLoss fixed: 297.89 spread: 1.00% => 294.52"
        ));
    }

    /// FAQ: with `UseTakeProfit` the trailing line appears once the price
    /// reaches `TakeProfit`, then follows the best price at `TrailingPercent`;
    /// its exit crosses the book by `TrailingSpread`, sold as `Trailing`.
    #[test]
    fn trailing_starts_at_the_take_profit_and_follows_the_peak() {
        use FieldValue::{Bool, Double};
        let mut fx = StopFx::new(&[
            ("UseStopLoss", Bool(false)),
            ("UseTrailing", Bool(true)),
            ("TrailingPercent", Double(-0.5)),
            ("TrailingSpread", Double(0.5)),
            ("UseTakeProfit", Bool(true)),
            ("TakeProfit", Double(1.0)),
        ]);
        assert!(shown_stops(&fx.first).is_empty());
        fx.at(1_000, 299.0);
        assert!(shown_stops(&fx.at(1_500, 299.5)).is_empty(), "under +1 %");
        fx.at(2_000, 300.0);
        let cmds = fx.at(3_000, 302.0);
        assert_eq!(shown_stops(&cmds), [298.5]);
        assert!(has_log(
            &cmds,
            "SBER: Trailing applied (peak 300.00 stop -0.50% => 298.50)"
        ));
        let cmds = fx.at(4_000, 300.3);
        assert_eq!(shown_stops(&cmds), [300.49]);
        assert!(!has_log(&cmds, "Trailing applied"), "a move is not logged");
        assert!(exit_moves(&cmds).is_empty(), "armed");
        let cmds = fx.at(6_000, 300.3);
        assert_eq!(exit_moves(&cmds), [(298.79, reason::TRAILING)]);
        assert!(has_log(
            &cmds,
            "SBER: Trailing AutoActivated: BID = 300.30 LastPrice = 300.30 Peak = 302.00; \
             Trailing line: 300.49 spread: 0.50% => 298.79 (strategy <shot>)"
        ));
    }

    /// FAQ: the BV/SV stop sells once bought-to-sold over the last
    /// `BV_SV_TradesN` trades falls under `BV_SV_Ratio`, and keeps new
    /// entries off while it would.
    #[test]
    fn bvsv_stop_sells_and_holds_entries() {
        use FieldValue::{Bool, Double, Int32, String as Str};
        let bv = [
            ("UseStopLoss", Bool(false)),
            ("UseBV_SV_Stop", Bool(true)),
            ("BV_SV_Kind", Str("TradesCount".into())),
            ("BV_SV_TradesN", Int32(4)),
            ("BV_SV_Ratio", Double(0.75)),
        ];
        let mut fx = StopFx::new(&bv);
        let t = fx.t0 + 600;
        for buy in [true, false, false] {
            fx.shot.on_trade(1, t, 297.0, 100.0, buy);
        }
        assert!(exit_moves(&fx.at(1_000, 297.0)).is_empty(), "3 of 4 trades");
        fx.shot.on_trade(1, t, 297.0, 100.0, false);
        let cmds = fx.at(2_000, 297.0);
        assert_eq!(exit_moves(&cmds), [(294.03, reason::BV_SV_STOP)]);
        assert!(has_log(
            &cmds,
            "SBER: BV/SV Stop AutoActivated: BV/SV = 0.33 < 0.75 spread: 1.00% => 294.03"
        ));

        // The same tape takes a waiting entry off.
        use FieldValue::String as S;
        let mut fields = vec![
            ("CoinsWhiteList", S("SBER".into())),
            ("OrderSize", Double(3000.0)),
        ];
        fields.extend(bv.iter().cloned());
        let (model, st) = (model(), strategies(&fields, true));
        let (win, sched) = (Windows::default(), None::<f64>);
        let mut orders = Orders::new();
        let mut shot = MoonShot::default();
        let t0 = 8_000_000;
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0);
        let keys = apply(&mut shot, &mut orders, &model, &cmds, t0);
        orders.apply(&report(&keys[0], "900000", ExecStatus::New, 1, 0, 0.0), t0);
        for buy in [false, false, false, true] {
            shot.on_trade(1, t0, 300.0, 100.0, buy);
        }
        let cmds = shot.tick(&st, &orders, &model, &win, sched, t0 + 1_000);
        assert_eq!(cancels(&cmds).len(), 1);
        assert!(has_log(&cmds, "BV/SV of SBER is under BV_SV_Ratio"));
    }

    /// FAQ: `FastStopLoss` fires on a trade through the line between passes,
    /// without the book confirmation; the plain stop does not see it.
    #[test]
    fn fast_stop_fires_on_a_trade_spike() {
        use FieldValue::{Bool, Double};
        for fast in [false, true] {
            let mut fx = StopFx::new(&[("StopLoss", Double(-1.0)), ("FastStopLoss", Bool(fast))]);
            fx.shot.on_trade(1, fx.t0 + 700, 293.9, 1_000.0, false);
            let cmds = fx.at(1_000, 297.0);
            if fast {
                assert_eq!(exit_moves(&cmds), [(294.03, reason::STOP_LOSS)]);
                assert!(has_log(
                    &cmds,
                    "StopLoss AutoActivated on price drop: BID = 293.90"
                ));
            } else {
                assert!(exit_moves(&cmds).is_empty());
            }
        }
    }

    /// `StopLossEMA` 10: a dip the plain stop fires on stays above the line
    /// on average.
    #[test]
    fn stop_ema_smooths_a_dip() {
        use FieldValue::{Double, String as Str};
        for ema in ["0", "10"] {
            let mut fx =
                StopFx::new(&[("StopLoss", Double(-1.0)), ("StopLossEMA", Str(ema.into()))]);
            fx.at(1_000, 290.0);
            fx.at(2_000, 290.0);
            let fired = !exit_moves(&fx.at(3_100, 290.0)).is_empty();
            assert_eq!(fired, ema == "0", "StopLossEMA {ema}");
        }
    }

    /// `AllowedDrop`: a fired stop's exit goes no lower than its level;
    /// `UseMarketOrder` puts it at the band's edge instead of the spread.
    #[test]
    fn allowed_drop_floors_the_stop_exit_and_market_takes_the_band() {
        use FieldValue::{Bool, Double};
        let fire = |extra: &[(&str, FieldValue)]| {
            let mut fields = vec![("StopLoss", Double(-1.0)), ("StopLossSpread", Double(5.0))];
            fields.extend(extra.iter().cloned());
            let mut fx = StopFx::new(&fields);
            let m = fx.model.at_mut(1).unwrap();
            m.set_limits(270.0, 330.0, 0);
            fx.at(1_000, 293.0);
            exit_moves(&fx.at(3_000, 293.0))
        };
        assert_eq!(fire(&[]), [(278.35, reason::STOP_LOSS)]);
        assert_eq!(
            fire(&[("AllowedDrop", Double(-2.0))]),
            [(291.06, reason::STOP_LOSS)]
        );
        assert_eq!(
            fire(&[("UseMarketOrder", Bool(true))]),
            [(270.0, reason::STOP_LOSS)]
        );
    }

    /// FAQ: the third stop is a line of its own; once it fires its exit goes
    /// no lower than `AllowedDrop3` until the price passes the main stop,
    /// then `AllowedDrop` rules.
    #[test]
    fn third_stop_keeps_its_own_drop_until_the_main_line() {
        use FieldValue::{Bool, Double, Int32};
        let mut fx = StopFx::new(&[
            ("StopLoss", Double(-3.0)),
            ("AllowedDrop", Double(-10.0)),
            ("UseStopLoss3", Bool(true)),
            ("TimeToSwitchStop3", Int32(0)),
            ("PriceToSwitchStop3", Double(0.5)),
            ("StopLoss3", Double(0.2)),
            ("AllowedDrop3", Double(-0.5)),
        ]);
        fx.at(1_000, 299.0);
        let cmds = fx.at(2_000, 297.3);
        assert_eq!(shown_stops(&cmds), [297.594]);
        assert!(has_log(
            &cmds,
            "SBER: StopLoss3 applied (buyPrice 297.00 stop +0.20% => 297.59)"
        ));
        let cmds = fx.at(4_000, 297.3);
        assert_eq!(exit_moves(&cmds), [(295.52, reason::STOP_LOSS)]);
        assert!(has_log(&cmds, "StopLoss3 AutoActivated"));
        settle_exit_move(&mut fx.orders, "900001", "900002", 1, fx.t0 + 4_000);
        // A restart keeps the third stop's drop: reason 7 is shared.
        fx.shot = MoonShot::default();
        fx.shot.restore(&fx.orders, fx.t0 + 4_000);
        assert!(
            exit_moves(&fx.at(6_000, 294.0)).is_empty(),
            "held at AllowedDrop3"
        );
        let cmds = fx.at(8_000, 287.0);
        assert_eq!(exit_moves(&cmds), [(284.13, reason::STOP_LOSS)]);
    }

    /// FAQ: `FastStopLoss`, no delay and a positive `StopLoss` sell at once
    /// at the entry less `StopLossSpread`.
    #[test]
    fn immediate_stop_sells_off_the_entry() {
        use FieldValue::{Bool, Double};
        let fx = StopFx::new(&[
            ("FastStopLoss", Bool(true)),
            ("StopLoss", Double(10.0)),
            ("StopLossSpread", Double(2.0)),
        ]);
        assert_eq!(exit_moves(&fx.first), [(291.06, reason::STOP_LOSS)]);
        assert!(has_log(
            &fx.first,
            "SBER: Immediate StopLoss: sell price is [actual buy - StopSpread%]: \
             297.00 - 2.00% = 291.06 (strategy <shot>)"
        ));
    }
}
