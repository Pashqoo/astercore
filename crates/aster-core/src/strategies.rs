//! Strategy list owned by the core: the schema (MoonShot, DropsDetection,
//! MoonStrike, MoonHook), the terminal's snapshot sync (per-strategy rollback guard,
//! list order, folder tree), checked flags and the global run state.
//! Persisted as MoonBot text after every change (`strategy_file`).
//!
//! Ported from TInvestCore. Two departures: the BTC fields carry MoonBot's own
//! names and meaning again (`Delta_BTC_*`, `MShotAddBTCDelta`,
//! `MShotAddBTC5mDelta` — TInvestCore read them off the MOEX index), and
//! `EmulatorMode` defaults ON, the safe side for a strategy made without it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use moonproto::server::codec::strat::{self, ui, CheckedItem, SchemaField, Snapshot};
use moonproto::{FieldValue, StrategyKind, StrategySchema, StrategySnapshot};

use crate::bvsv;
use crate::model::MarketTags;
use crate::screener;
use crate::strategy_file;

pub const KIND_MOONSHOT: (u8, &str) = (StrategyKind::MOON_SHOT.ordinal(), "MoonShot");
pub const KIND_DROPS: (u8, &str) = (StrategyKind::DROPS.ordinal(), "DropsDetection");
pub const KIND_STRIKE: (u8, &str) = (StrategyKind::MOON_STRIKE.ordinal(), "MoonStrike");
pub const KIND_HOOK: (u8, &str) = (StrategyKind::MOON_HOOK.ordinal(), "MoonHook");
/// MoonBot's `Manual`: no entries of its own; the exit settings of the
/// terminal's hand trades while `use_manual_strategy` names it.
pub const KIND_MANUAL: (u8, &str) = (StrategyKind::MANUAL.ordinal(), "Manual");
pub const SELL_PRICE: &str = "SellPrice";
/// MoonBot's per-strategy Telegram switches: messages about its detects, and
/// reports of its closed deals — read by the Telegram reports, which come
/// with M4; until then they are kept and do nothing.
pub const REPORT_DETECTS: &str = "ReportToTelegram";
pub const REPORT_TRADES: &str = "ReportTradesToTelegram";
pub const MARKET_TAGS: &str = "MarketTags";

/// What the page's strategy helper shows, card by card: every field that
/// decides WHICH markets a strategy is about and whether it may enter one of
/// them — the screener and the Filters tab — and nothing else. What a
/// strategy does once it is in a market has an editor already, in the
/// terminal; a helper that offered it too would be a second one, and the two
/// would disagree about which is the strategy.
///
/// `Dyn_Refresh` is deliberately absent: it says how often the pool is
/// recomputed, not what is in it, and a control that moves nothing on the
/// page would read as one that does.
///
/// These are schema names, and `screen_fields_are_in_the_schema` keeps them
/// so: a name that drifted would show an empty control and screen the catalog
/// against the schema default instead of what the page displays.
pub const SCREEN_FIELDS: &[(&str, &[&str])] = &[
    ("Pool", &[MARKET_TAGS, "CoinsWhiteList", "CoinsBlackList"]),
    (
        "Dynamic white list",
        &["DynWL_SortBy", "DynWL_SortDesc", "DynWL_Count"],
    ),
    (
        "Dynamic black list",
        &["DynBL_SortBy", "DynBL_SortDesc", "DynBL_Count"],
    ),
    // `IgnoreFilters` is the whole tab, both halves of it, so it gets a card
    // of its own: sitting in the volume box it read as the volume box's
    // switch, next to the one that really is (`IgnoreVolume`).
    ("Filters: the whole tab", &["IgnoreFilters"]),
    (
        "Filters / Volume",
        &[
            "IgnoreVolume",
            "MinVolume",
            "MaxVolume",
            "MinHourlyVolume",
            "MaxHourlyVolume",
        ],
    ),
    (
        "Filters / Base",
        &["IgnoreBase", "MinLeverage", "MaxLeverage"],
    ),
    (
        "Filters / Delta",
        &[
            "IgnoreDelta",
            "Delta_3h_Min",
            "Delta_3h_Max",
            "Delta_24h_Min",
            "Delta_24h_Max",
            "Delta2_Type",
            "Delta2_Min",
            "Delta2_Max",
            "Delta3_Type",
            "Delta3_Min",
            "Delta3_Max",
            "Delta_BTC_Min",
            "Delta_BTC_Max",
            "Delta_Market_Min",
            "Delta_Market_Max",
            "FilterBy",
            "FilterMin",
            "FilterMax",
        ],
    ),
];

const SHOT: &[u8] = &[KIND_MOONSHOT.0];
const DROPS: &[u8] = &[KIND_DROPS.0];
const STRIKE: &[u8] = &[KIND_STRIKE.0];
const HOOK: &[u8] = &[KIND_HOOK.0];
const SIGNALS: &[u8] = &[KIND_DROPS.0, KIND_STRIKE.0, KIND_HOOK.0];
/// MoonStrike and MoonHook price their exit from the detect, not by `SellPrice`.
const BY_SELL_PRICE: &[u8] = &[KIND_MOONSHOT.0, KIND_DROPS.0, KIND_MANUAL.0];

/// Fields the engine (`moonshot.rs`) and the terminal UI read; names, types
/// and defaults are MoonBot 7.52's (demo `MOONSHOT-1`, `DROPS-1` for the
/// DropsDetection ones, `ST_5` for MoonStrike) where MoonBot has the field
/// — `HookPriceDistance` is the single deliberate exception, see its line —
/// TMB's otherwise (`MShotExpand`, `PingCooldown`). A field shows for the
/// kinds that read it (`SchemaField::kinds`).
/// Editor sections follow MoonBot's (`Moonterminal/assets/param_deps.toml`;
/// TMB-only fields sit next to their MoonBot kin). Volume filters are USDT
/// turnover over the core's own tick windows (24 h / 1 h), 0 = no bound.
fn schema_fields() -> Vec<SchemaField> {
    use FieldValue::{Bool, Double, Int32, String as Str};
    let f = |name, default, ui, section| SchemaField {
        name,
        default,
        ui,
        section,
        picklist: None,
        kinds: &[],
    };
    let only = |kinds, field| SchemaField { kinds, ..field };
    let s = |v: &str| Str(v.to_string());
    vec![
        // Main
        f("StrategyName", s(""), ui::EDIT, None),
        f("SignalType", s(KIND_MOONSHOT.1), ui::COMBO, None),
        f("Comment", s(""), ui::EDIT, None),
        // ON, unlike MoonBot: a strategy made or pasted without the field
        // must not trade real money by default (Astercore, 01.10).
        f("EmulatorMode", Bool(true), ui::CHECKBOX, None),
        f("SoundAlert", Bool(true), ui::CHECKBOX, None),
        f("SoundKind", s("ding1"), ui::EDIT, None),
        f("KeepAlert", Int32(20), ui::EDIT, None),
        f("AddToChart", Int32(1), ui::EDIT, None),
        f("KeepInChart", Int32(180), ui::EDIT, None),
        // MoonBot's Telegram switches, per strategy (demo defaults: trades
        // yes, detects no). The core reads them when it has a chat to report
        // to (the Telegram reports, M4); the terminal shows them in Main, as its own
        // `param_deps.toml` places them.
        f(REPORT_DETECTS, Bool(false), ui::CHECKBOX, None),
        f(REPORT_TRADES, Bool(true), ui::CHECKBOX, None),
        f("CoinsWhiteList", s(""), ui::EDIT, Some("Filters")),
        f("CoinsBlackList", s(""), ui::EDIT, None),
        // Classes the screener picks from when the white list is empty —
        // Aster's taxonomy (`model::Tag`), `!tag` excludes. The combo offers
        // one class per item (`MarketTags::PICKLIST`) so the editor cannot
        // misspell a tag; a combination or a `!tag` still parses when a
        // strategy file carries one. The default is `all`.
        SchemaField {
            name: MARKET_TAGS,
            default: s("all"),
            ui: ui::COMBO,
            section: None,
            picklist: Some(MarketTags::PICKLIST),
            kinds: &[],
        },
        f("MaxPing", Int32(0), ui::EDIT, Some("Filters / Ping")),
        f("PingCooldown", Double(5.0), ui::EDIT, None),
        f(
            "TradePenaltyTime",
            Double(30.0),
            ui::EDIT,
            Some("Filters / Time"),
        ),
        // MoonBot: off the market after 3 losses in a row or a manual order
        // on it; one strategy's detect holds the others for a while.
        f("WorkingTime", s(""), ui::EDIT, None),
        f("PreventWorkingUntil", Double(0.0), ui::EDIT, None),
        f("PenaltyTime", Double(0.0), ui::EDIT, None),
        f("GlobalDetectPenalty", Double(0.0), ui::EDIT, None),
        only(
            SIGNALS,
            f("NextDetectPenalty", Double(30.0), ui::EDIT, None),
        ),
        // MoonBot's loss guards (`guards.rs`); all off by default.
        f(
            "MaxPosition",
            Double(0.0),
            ui::EDIT,
            Some("Filters / Price/Position"),
        ),
        f("TotalLoss", Double(0.0), ui::EDIT, None),
        f("IgnoreSession", Bool(true), ui::CHECKBOX, Some("Sessions")),
        f("SessionLevelsUSDT", Bool(true), ui::CHECKBOX, None),
        f("SessionStratMax", Double(0.0), ui::EDIT, None),
        f("SessionStratMin", Double(0.0), ui::EDIT, None),
        f("SessionPenaltyTime", Double(0.0), ui::EDIT, None),
        f("SessionResetOnMinus", Bool(false), ui::CHECKBOX, None),
        // MoonBot's Filters / Delta: Min/Max % of the window's signed delta,
        // both 0 = off; `Delta_BTC_*` is BTCUSDT's hourly delta and
        // `Delta_Market_*` the traded markets' mean hourly delta.
        f(
            "IgnoreFilters",
            Bool(false),
            ui::CHECKBOX,
            Some("Filters / Delta"),
        ),
        f("IgnoreDelta", Bool(false), ui::CHECKBOX, None),
        f("Delta_3h_Min", Double(0.0), ui::EDIT, None),
        f("Delta_3h_Max", Double(0.0), ui::EDIT, None),
        f("Delta_24h_Min", Double(0.0), ui::EDIT, None),
        f("Delta_24h_Max", Double(0.0), ui::EDIT, None),
        f("Delta2_Type", s("1h"), ui::EDIT, None),
        f("Delta2_Min", Double(0.0), ui::EDIT, None),
        f("Delta2_Max", Double(0.0), ui::EDIT, None),
        f("Delta3_Type", s("15m"), ui::EDIT, None),
        f("Delta3_Min", Double(0.0), ui::EDIT, None),
        f("Delta3_Max", Double(0.0), ui::EDIT, None),
        f("Delta_BTC_Min", Double(0.0), ui::EDIT, None),
        f("Delta_BTC_Max", Double(0.0), ui::EDIT, None),
        f("Delta_Market_Min", Double(0.0), ui::EDIT, None),
        f("Delta_Market_Max", Double(0.0), ui::EDIT, None),
        SchemaField {
            name: "FilterBy",
            default: s("Last2hDelta"),
            ui: ui::COMBO,
            section: None,
            picklist: Some(screener::SortKey::PICKLIST),
            kinds: &[],
        },
        f("FilterMin", Double(0.0), ui::EDIT, None),
        f("FilterMax", Double(0.0), ui::EDIT, None),
        f("GlobalFilterPenalty", Double(0.0), ui::EDIT, None),
        // USDT turnover bounds, 0 = no bound on that side. They gate the
        // ENTRY on a market of the pool, pass by pass, and no longer decide
        // what the pool is (`moonshot::DeltaFilters`, `screener`): a market
        // that goes quiet for an hour keeps its place, its subscription and
        // its detect, and only stops being entered. `MinVolume`/`MaxVolume`
        // read the last 24 hours the market traded, the hourly pair the last
        // hour of the clock.
        // The Filters tab's two halves have a switch each, as in MoonBot:
        // `IgnoreVolume` opens this box, `IgnoreDelta` the delta one above.
        // They shared one switch before, so turning the deltas off turned the
        // volume bounds off with them — and a switch declared up there would
        // show in the wrong box, since a field's section runs until the next
        // one names its own.
        f(
            "IgnoreVolume",
            Bool(false),
            ui::CHECKBOX,
            Some("Filters / Volume"),
        ),
        f("MinVolume", Double(0.0), ui::EDIT, None),
        f("MaxVolume", Double(0.0), ui::EDIT, None),
        f("MinHourlyVolume", Double(0.0), ui::EDIT, None),
        f("MaxHourlyVolume", Double(0.0), ui::EDIT, None),
        // MoonBot's Filters / Base, the part this core reads: the market's
        // leverage corridor. `MinLeverage` 1 and `MaxLeverage` 0 (no limit) are
        // MoonBot's defaults and filter nothing. The box's other fields
        // (`BinanceTokenTags`, `MarkPriceMin`, …) are not read here.
        f(
            "IgnoreBase",
            Bool(false),
            ui::CHECKBOX,
            Some("Filters / Base"),
        ),
        f("MinLeverage", Int32(1), ui::EDIT, None),
        f("MaxLeverage", Int32(0), ui::EDIT, None),
        // MoonBot's dynamic lists, and with the volume bounds gone from the
        // screener they ARE the pool of a class strategy: sort the class by
        // one key and keep the first `DynWL_Count` markets, take
        // `DynBL_Count` the same way and subtract them. `Count` 0 = the list
        // is off, which is what a strategy saved before these fields reads as
        // — and a class strategy with no count has no pool at all, which its
        // log line says (`screener::problem`). The sort combo offers the keys
        // this core can answer (`SortKey::PICKLIST`): the Binance-only ones
        // and `24h-Delta` are not among them.
        f(
            "Dyn_Refresh",
            Int32(screener::REFRESH_DEFAULT_S),
            ui::EDIT,
            Some("Dynamic White/Black List"),
        ),
        SchemaField {
            name: "DynWL_SortBy",
            default: s("Last2hDelta"),
            ui: ui::COMBO,
            section: None,
            picklist: Some(screener::SortKey::PICKLIST),
            kinds: &[],
        },
        f("DynWL_SortDesc", Bool(true), ui::CHECKBOX, None),
        f("DynWL_Count", Int32(0), ui::EDIT, None),
        SchemaField {
            name: "DynBL_SortBy",
            default: s("Last2hDelta"),
            ui: ui::COMBO,
            section: None,
            picklist: Some(screener::SortKey::PICKLIST),
            kinds: &[],
        },
        f("DynBL_SortDesc", Bool(true), ui::CHECKBOX, None),
        f("DynBL_Count", Int32(0), ui::EDIT, None),
        only(
            SHOT,
            f("Short", Bool(false), ui::CHECKBOX, Some("Buy conditions")),
        ),
        f("MaxActiveOrders", Int32(5), ui::EDIT, None),
        f("MaxMarkets", Int32(5), ui::EDIT, None),
        // USDT per entry. MoonBot's 1000 is in the quote of the venue it was written for (roubles
        // on MOEX); here it would be a thousand dollars on a strategy nobody has tuned yet. Aster's
        // `MIN_NOTIONAL` is 5 USDT.
        f("OrderSize", Double(10.0), ui::EDIT, None),
        f("AutoCancelBuy", Double(90.0), ui::EDIT, None),
        f("CancelBuyAfterSell", Bool(false), ui::CHECKBOX, None),
        // MoonBot's exposure limits; 0 / off = no limit (MoonBot's own
        // default MaxOrdersPerMarket 1 would cut the ladders saved before).
        f("MaxOrdersPerMarket", Int32(0), ui::EDIT, None),
        f("CheckFreeBalance", Bool(false), ui::CHECKBOX, None),
        f("MinFreeBalance", Double(0.0), ui::EDIT, None),
        only(DROPS, f("buyPrice", Double(-2.0), ui::EDIT, None)),
        only(
            DROPS,
            f("buyPriceLastTrade", Bool(false), ui::CHECKBOX, None),
        ),
        only(
            SHOT,
            f(
                "MShotPrice",
                Double(0.9),
                ui::EDIT,
                Some("Strategy settings"),
            ),
        ),
        only(SHOT, f("MShotPriceMin", Double(0.6), ui::EDIT, None)),
        SchemaField {
            name: "MShotUsePrice",
            default: s("BID"),
            ui: ui::COMBO,
            section: None,
            picklist: Some("BID|ASK|Trade"),
            kinds: SHOT,
        },
        only(SHOT, f("MShotAdd15minDelta", Double(0.1), ui::EDIT, None)),
        only(SHOT, f("MShotAddHourlyDelta", Double(0.1), ui::EDIT, None)),
        only(SHOT, f("MShotAdd3hDelta", Double(0.0), ui::EDIT, None)),
        only(SHOT, f("MShotAddBTCDelta", Double(0.0), ui::EDIT, None)),
        only(SHOT, f("MShotAddBTC5mDelta", Double(0.0), ui::EDIT, None)),
        only(SHOT, f("MShotAddDistance", Double(0.0), ui::EDIT, None)),
        only(SHOT, f("MShotRaiseWait", Double(20.0), ui::EDIT, None)),
        only(SHOT, f("MShotReplaceDelay", Double(0.1), ui::EDIT, None)),
        only(
            SHOT,
            f("MShotRepeatAfterBuy", Bool(false), ui::CHECKBOX, None),
        ),
        only(SHOT, f("MShotRepeatIfProfit", Double(0.0), ui::EDIT, None)),
        only(SHOT, f("MShotRepeatWait", Double(5.0), ui::EDIT, None)),
        only(SHOT, f("MShotRepeatDelay", Double(0.0), ui::EDIT, None)),
        only(
            DROPS,
            f(
                "DropsMaxTime",
                Int32(60),
                ui::EDIT,
                Some("Strategy settings"),
            ),
        ),
        only(DROPS, f("DropsPriceMA", Int32(2), ui::EDIT, None)),
        only(DROPS, f("DropsLastPriceMA", Int32(2), ui::EDIT, None)),
        only(DROPS, f("DropsPriceDelta", Double(1.0), ui::EDIT, None)),
        only(DROPS, f("DropsPriceIsLow", Bool(false), ui::CHECKBOX, None)),
        only(
            DROPS,
            f("DropsUseLastPrice", Bool(false), ui::CHECKBOX, None),
        ),
        only(
            STRIKE,
            f(
                "MStrikeDepth",
                Double(1.0),
                ui::EDIT,
                Some("Strategy settings"),
            ),
        ),
        only(STRIKE, f("MStrikeVolume", Double(0.0), ui::EDIT, None)),
        only(
            STRIKE,
            f("MStrikeAddHourlyDelta", Double(0.0), ui::EDIT, None),
        ),
        only(
            STRIKE,
            f("MStrikeAdd15minDelta", Double(0.0), ui::EDIT, None),
        ),
        only(
            STRIKE,
            f("MStrikeAddMarketDelta", Double(0.0), ui::EDIT, None),
        ),
        only(STRIKE, f("MStrikeBuyDelay", Int32(0), ui::EDIT, None)),
        only(STRIKE, f("MStrikeBuyLevel", Double(-5.0), ui::EDIT, None)),
        only(
            STRIKE,
            f("MStrikeBuyRelative", Bool(false), ui::CHECKBOX, None),
        ),
        only(STRIKE, f("MStrikeSellLevel", Double(80.0), ui::EDIT, None)),
        SchemaField {
            name: "MStrikeDirection",
            default: s("OnlyLong"),
            ui: ui::COMBO,
            section: None,
            picklist: Some("Both|OnlyLong|OnlyShort"),
            kinds: STRIKE,
        },
        only(STRIKE, f("MStrikeWaitDip", Bool(false), ui::CHECKBOX, None)),
        only(
            HOOK,
            f(
                "HookTimeFrame",
                Double(10.0),
                ui::EDIT,
                Some("Strategy settings"),
            ),
        ),
        only(HOOK, f("HookDetectDepth", Double(5.0), ui::EDIT, None)),
        only(HOOK, f("HookDetectDepthMax", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookAntiPump", Bool(true), ui::CHECKBOX, None)),
        only(HOOK, f("HookDetectMinVolume", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookPriceRollBack", Double(1.0), ui::EDIT, None)),
        only(HOOK, f("HookPriceRollBackMax", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookRollBackWait", Int32(0), ui::EDIT, None)),
        only(HOOK, f("HookDropMin", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookDropMax", Double(0.0), ui::EDIT, None)),
        SchemaField {
            name: "HookDirection",
            default: s("OnlyLong"),
            ui: ui::COMBO,
            section: None,
            picklist: Some("Both|OnlyLong|OnlyShort"),
            kinds: HOOK,
        },
        only(HOOK, f("HookInitialPrice", Double(20.0), ui::EDIT, None)),
        // The one deliberate departure from MoonBot's table: its own default
        // is 0, an entry that stands still until `AutoCancelBuy` (22 of the
        // 39 demo hooks run that way). A zero default cannot be seen: it
        // equals the value, so the terminal never sends the field
        // (`strategy_serializer::writer`) and `strategy_file::render` writes
        // only what arrived — the strategy file then holds no
        // `HookPriceDistance` line at all, and a hook whose entries never
        // follow the price looks exactly like one that does. 15 is the demo
        // packs' own second choice; an entry meant to stand still now says
        // so with an explicit 0, which does reach the file.
        only(HOOK, f("HookPriceDistance", Double(15.0), ui::EDIT, None)),
        only(HOOK, f("HookReplaceDelay", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookRaiseWait", Double(0.0), ui::EDIT, None)),
        only(HOOK, f("HookSellLevel", Double(80.0), ui::EDIT, None)),
        only(HOOK, f("HookSellFixed", Bool(true), ui::CHECKBOX, None)),
        only(
            HOOK,
            f("HookRepeatAfterSell", Bool(false), ui::CHECKBOX, None),
        ),
        only(HOOK, f("HookRepeatIfProfit", Double(0.0), ui::EDIT, None)),
        f("OrdersCount", Int32(1), ui::EDIT, Some("Multiple Orders")),
        f("BuyPriceStep", Double(-1.5), ui::EDIT, None),
        f("OrderSizeStep", Double(25.0), ui::EDIT, None),
        only(SHOT, f("MShotExpand", Double(0.0), ui::EDIT, None)),
        only(
            BY_SELL_PRICE,
            f(SELL_PRICE, Double(2.0), ui::EDIT, Some("Sell order")),
        ),
        f("PriceDownTimer", Double(70.0), ui::EDIT, None),
        f("PriceDownDelay", Double(20.0), ui::EDIT, None),
        f("PriceDownPercent", Double(0.3), ui::EDIT, None),
        f("PriceDownRelative", Bool(false), ui::CHECKBOX, None),
        f("PriceDownAllowedDrop", Double(0.4), ui::EDIT, None),
        // Stops (`stops.rs`). Defaults of the fields added on 21.09 are the
        // demo strategies' (`Binance-USDT-strat.txt`) with every switch off
        // and no spread growth, so a strategy saved before them trades as it
        // did; the liquidation and delisting fields are kept and not yet read
        // (a debt on Aster, where they could work — `PLAN.md`).
        f("UseStopLoss", Bool(true), ui::CHECKBOX, Some("Stops")),
        f("FastStopLoss", Bool(false), ui::CHECKBOX, None),
        f("UseMarketOrder", Bool(false), ui::CHECKBOX, None),
        SchemaField {
            name: "StopLossEMA",
            default: s("0"),
            ui: ui::COMBO,
            section: None,
            picklist: Some("0|3|5|10"),
            kinds: &[],
        },
        f("StopLossDelay", Double(0.0), ui::EDIT, None),
        f("StopLoss", Double(-2.0), ui::EDIT, None),
        f("StopLossSpread", Double(0.4), ui::EDIT, None),
        f("StopSpreadAdd1mDelta", Double(0.0), ui::EDIT, None),
        f("AllowedDrop", Double(-50.0), ui::EDIT, None),
        f("DontSellBelowLiq", Bool(false), ui::CHECKBOX, None),
        f("StopAboveLiq", Double(0.0), ui::EDIT, None),
        f("StopLossFixed", Bool(false), ui::CHECKBOX, None),
        f("UseSecondStop", Bool(false), ui::CHECKBOX, None),
        f("TimeToSwitch2Stop", Int32(1800), ui::EDIT, None),
        f("PriceToSwitch2Stop", Double(0.0), ui::EDIT, None),
        f("SecondStopLoss", Double(-1.0), ui::EDIT, None),
        f("UseStopLoss3", Bool(false), ui::CHECKBOX, None),
        f("TimeToSwitchStop3", Int32(30), ui::EDIT, None),
        f("PriceToSwitchStop3", Double(-10.0), ui::EDIT, None),
        f("StopLoss3", Double(0.0), ui::EDIT, None),
        f("AllowedDrop3", Double(-14.0), ui::EDIT, None),
        f("UseTrailing", Bool(false), ui::CHECKBOX, None),
        f("TrailingPercent", Double(-1.0), ui::EDIT, None),
        f("TrailingSpread", Double(2.0), ui::EDIT, None),
        f("TrailingEMA", Int32(0), ui::EDIT, None),
        f("UseTakeProfit", Bool(true), ui::CHECKBOX, None),
        f("TakeProfit", Double(0.5), ui::EDIT, None),
        f("UseBV_SV_Stop", Bool(false), ui::CHECKBOX, None),
        SchemaField {
            name: "BV_SV_Kind",
            default: s("TradesCount"),
            ui: ui::COMBO,
            section: None,
            picklist: Some(bvsv::Kind::PICKLIST),
            kinds: &[],
        },
        f("BV_SV_TradesN", Int32(100), ui::EDIT, None),
        f("BV_SV_Ratio", Double(0.75), ui::EDIT, None),
        f("BV_SV_Reverse", Bool(false), ui::CHECKBOX, None),
        f("BV_SV_TakeProfit", Double(-1.0), ui::EDIT, None),
        f("PanicSellDelisted", Bool(false), ui::CHECKBOX, None),
    ]
}

pub struct Strategies {
    schema: StrategySchema,
    schema_blob: Vec<u8>,
    /// Terminal order.
    list: Vec<StrategySnapshot>,
    /// Every folder path including parents and empty folders.
    folders: Vec<String>,
    /// Date of the list order (`TStratSnapshot.ServerEpoch`).
    last_modified: u64,
    folders_last_modified: i64,
    running: bool,
    file: Option<PathBuf>,
    /// The file exists but could not be read (permissions, not UTF-8, a filesystem hiccup): the
    /// empty list is not the file's content, so [`Self::save`] must not write over it.
    unreadable: bool,
    /// The file as this run found it is copied to `<file>.prev` before the first write: a block
    /// the parser skipped (`strategy_file`) is gone from the file at that write.
    backed_up: std::cell::Cell<bool>,
}

impl Strategies {
    /// Loads `file` when it exists; epochs come from its mtime so a client
    /// reorder made while the core was down still wins.
    pub fn new(file: Option<PathBuf>, now: i64) -> Self {
        let schema_blob = strat::build_schema_blob(
            &[
                KIND_MOONSHOT,
                KIND_DROPS,
                KIND_STRIKE,
                KIND_HOOK,
                KIND_MANUAL,
            ],
            &schema_fields(),
        );
        let schema = strat::parse_schema(&schema_blob).expect("own schema parses");
        let mut this = Self {
            schema,
            schema_blob,
            list: Vec::new(),
            folders: Vec::new(),
            last_modified: now as u64,
            folders_last_modified: now,
            running: false,
            file,
            unreadable: false,
            backed_up: std::cell::Cell::new(false),
        };
        let Some(path) = this.file.as_deref() else {
            return this;
        };
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return this,
            Err(e) => {
                log::error!(
                    "strategies: cannot read {}: {e}; starting with an empty list and NOT \
                     saving over the file until the core is restarted with a readable one",
                    path.display()
                );
                this.unreadable = true;
                return this;
            }
        };
        let (list, folders) = strategy_file::parse(&text, &this.schema);
        // Text that parses to nothing is a foreign or damaged file, not an empty list: it is
        // kept as it is until the operator looks at it.
        if list.is_empty() && folders.is_empty() && !text.trim().is_empty() {
            log::error!(
                "strategies: {} has content but no strategy could be read from it; starting \
                 with an empty list and NOT saving over the file",
                path.display()
            );
            this.unreadable = true;
            return this;
        }
        for s in &list {
            add_folder(&mut this.folders, &s.path);
        }
        for f in &folders {
            add_folder(&mut this.folders, f);
        }
        this.list = list;
        let mtime = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(now, |d| d.as_millis() as i64);
        this.last_modified = mtime as u64;
        this.folders_last_modified = mtime;
        log::info!(
            "strategies: {} loaded from {}",
            this.list.len(),
            path.display()
        );
        this
    }

    /// The strategy file could not be read at start: the empty list is not the trader's.
    pub fn unreadable(&self) -> bool {
        self.unreadable
    }

    pub fn schema_blob(&self) -> &[u8] {
        &self.schema_blob
    }

    pub fn schema(&self) -> &StrategySchema {
        &self.schema
    }

    pub fn list(&self) -> &[StrategySnapshot] {
        &self.list
    }

    pub fn folders(&self) -> &[String] {
        &self.folders
    }

    /// A listed strategy's boolean field, its schema default when the
    /// strategy itself does not carry it (a file written before the field).
    /// `None` when there is no such strategy any more — the caller decides
    /// what a deleted strategy's deal is worth reporting.
    pub fn flag(&self, strategy_id: u64, name: &str) -> Option<bool> {
        let s = &self.list[self.position(strategy_id)?];
        let value = s.fields.get(name).cloned().or_else(|| {
            self.schema
                .field(name)
                .and_then(|f| f.default_value.clone())
        });
        Some(matches!(value, Some(FieldValue::Bool(true))))
    }

    /// MoonBot kind name (`SignalType`) of a listed strategy.
    pub fn kind_name(&self, strategy_id: u64) -> Option<&str> {
        let s = &self.list[self.position(strategy_id)?];
        self.schema.kind_name_for_strategy_kind(s.kind())
    }

    pub fn running(&self) -> bool {
        self.running
    }

    pub fn set_running(&mut self, on: bool) {
        self.running = on;
    }

    /// Terminal snapshot: newer revisions replace ours (`last_date` and
    /// `strategy_ver` both `>=` keep the current one), unknown ids are added,
    /// Full adopts a newer order and folder tree. Returns `(touched, rejected)`:
    /// touched ids are echoed (a stale revision comes back as the core's copy),
    /// rejected ones (a kind outside the schema) are deleted at the terminal.
    pub fn apply_snapshot(&mut self, snap: &Snapshot) -> (Vec<u64>, Vec<u64>) {
        let Some((incoming, paths)) = strat::decode_batch(&snap.data, &self.schema) else {
            log::warn!(
                "strategies: undecodable snapshot ({} bytes)",
                snap.data.len()
            );
            return (Vec::new(), Vec::new());
        };
        let mut touched = Vec::with_capacity(incoming.len());
        let mut rejected = Vec::new();
        for s in incoming {
            if self.schema.kind_name_for_strategy_kind(s.kind()).is_none() {
                log::warn!(
                    "strategies: {} has unsupported kind {}, deleting",
                    s.strategy_id,
                    s.kind().ordinal()
                );
                rejected.push(s.strategy_id);
                continue;
            }
            touched.push(s.strategy_id);
            add_folder(&mut self.folders, &s.path);
            match self.position(s.strategy_id) {
                Some(i)
                    if self.list[i].last_date >= s.last_date
                        && self.list[i].strategy_ver >= s.strategy_ver =>
                {
                    log::debug!("strategies: {} kept (older revision sent)", s.strategy_id);
                }
                Some(i) => self.list[i] = s,
                None => self.list.push(s),
            }
        }
        if snap.full {
            if snap.server_epoch > self.last_modified {
                let rank: HashMap<u64, usize> =
                    touched.iter().enumerate().map(|(i, &id)| (id, i)).collect();
                self.list
                    .sort_by_key(|s| rank.get(&s.strategy_id).copied().unwrap_or(usize::MAX));
                self.last_modified = snap.server_epoch;
            }
            if snap.folders_last_modified > self.folders_last_modified {
                let mut folders = Vec::new();
                for p in paths
                    .iter()
                    .map(AsRef::as_ref)
                    .chain(self.list.iter().map(|s| &*s.path))
                {
                    add_folder(&mut folders, p);
                }
                self.folders = folders;
                self.folders_last_modified = snap.folders_last_modified;
            }
        }
        self.save();
        (touched, rejected)
    }

    /// `TStratDelete`: a strategy by id and/or an empty folder by path.
    /// Returns whether anything changed.
    pub fn delete(&mut self, strategy_id: u64, folder: &str, now: i64) -> bool {
        let mut changed = false;
        if let Some(i) = self.position(strategy_id) {
            self.list.remove(i);
            changed = true;
        }
        if !folder.is_empty()
            && self.folders.iter().any(|f| f.eq_ignore_ascii_case(folder))
            && !self.list.iter().any(|s| within(&s.path, folder))
        {
            self.folders.retain(|f| !within(f, folder));
            self.folders_last_modified = now.max(self.folders_last_modified + 1);
            changed = true;
        }
        if changed {
            self.save();
        }
        changed
    }

    /// Checked flags from a checked-sync or start/stop command.
    pub fn set_checked(&mut self, items: &[CheckedItem]) {
        for it in items {
            if let Some(i) = self.position(it.strategy_id) {
                self.list[i].checked = it.checked;
            }
        }
        if !items.is_empty() {
            self.save();
        }
    }

    /// `TStratSellPriceUpdate`: a new revision the clients learn by snapshot.
    pub fn set_sell_price(&mut self, strategy_id: u64, price: f64, now: i64) -> bool {
        let Some(i) = self.position(strategy_id) else {
            return false;
        };
        let s = &mut self.list[i];
        s.fields.insert(SELL_PRICE, FieldValue::Double(price));
        s.last_date = (now as u64).max(s.last_date + 1);
        self.save();
        true
    }

    /// Full snapshot: the whole list, its order date and the folder tree.
    pub fn full_payload(&self, uid: u64) -> Vec<u8> {
        self.payload(uid, &self.list, true)
    }

    /// Partial snapshot with the listed strategies (those still present).
    pub fn partial_payload(&self, uid: u64, ids: &[u64]) -> Option<Vec<u8>> {
        let rows: Vec<StrategySnapshot> = self
            .list
            .iter()
            .filter(|s| ids.contains(&s.strategy_id))
            .cloned()
            .collect();
        (!rows.is_empty()).then(|| self.payload(uid, &rows, false))
    }

    fn payload(&self, uid: u64, rows: &[StrategySnapshot], full: bool) -> Vec<u8> {
        let folders = full
            .then(|| self.folders.iter().map(String::as_str))
            .into_iter()
            .flatten();
        strat::snapshot(
            uid,
            &Snapshot {
                server_epoch: if full { self.last_modified } else { 0 },
                client_max_last_date: rows.iter().map(|s| s.last_date).max().unwrap_or(0),
                full,
                data: strat::encode_batch(&self.schema, rows, folders),
                folders_last_modified: if full { self.folders_last_modified } else { 0 },
            },
        )
    }

    fn position(&self, strategy_id: u64) -> Option<usize> {
        self.list.iter().position(|s| s.strategy_id == strategy_id)
    }

    fn save(&self) {
        let Some(path) = &self.file else {
            return;
        };
        if self.unreadable {
            log::error!(
                "strategies: {} was unreadable at start, the change is kept in memory only",
                path.display()
            );
            return;
        }
        let text = strategy_file::render(&self.list, &self.folders, &self.schema);
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if !self.backed_up.get() && path.exists() {
            let with = |suffix: &str| {
                let mut o = path.as_os_str().to_owned();
                o.push(suffix);
                PathBuf::from(o)
            };
            let (prev, older, fresh) = (with(".prev"), with(".prev2"), with(".prev.new"));
            // Copied beside first and moved into place: a copy that fails partway must not
            // become `.prev`. Two generations cover one restart after a lossy run — no more.
            match std::fs::copy(path, &fresh) {
                Ok(_) => {
                    let _ = std::fs::rename(&prev, &older);
                    match std::fs::rename(&fresh, &prev) {
                        Ok(()) => self.backed_up.set(true),
                        Err(e) => log::warn!("strategies: backup of {}: {e}", path.display()),
                    }
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&fresh);
                    log::warn!("strategies: backup of {}: {e}", path.display());
                }
            }
        }
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let written = std::fs::File::create(&tmp)
            .and_then(|mut f| {
                use std::io::Write;
                f.write_all(text.as_bytes())?;
                f.sync_all()
            })
            .and_then(|()| std::fs::rename(&tmp, path));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            log::error!("strategies: write {}: {e}", path.display());
        }
    }
}

/// `path` equals `folder` or lies below it (case-insensitive, `/`-separated).
fn within(path: &str, folder: &str) -> bool {
    path.get(..folder.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(folder))
        && (path.len() == folder.len() || path.as_bytes()[folder.len()] == b'/')
}

/// Registers `path` and its parents once (first spelling wins).
fn add_folder(folders: &mut Vec<String>, path: &str) {
    // The prefix is built from the segments, not cut out of `path` by their lengths: a leading
    // or doubled `/` shifts every such index, and one into a multi-byte character panics.
    let mut prefix = String::new();
    for seg in path.split('/').filter(|seg| !seg.is_empty()) {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(seg);
        if !folders.iter().any(|f| f.eq_ignore_ascii_case(&prefix)) {
            folders.push(prefix.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonproto::{StrategyFields, StrategyKind};

    /// Every control the helper offers stands for a real schema field. A name
    /// that drifted would be a control the page draws empty and the core
    /// ignores — the operator would move it and watch nothing change.
    #[test]
    fn screen_fields_are_in_the_schema() {
        let fields = schema_fields();
        let mut seen: Vec<&str> = Vec::new();
        for (card, names) in SCREEN_FIELDS {
            for name in *names {
                assert!(
                    fields.iter().any(|f| f.name == *name),
                    "{card}: {name} is not a schema field"
                );
                assert!(!seen.contains(name), "{name} is offered twice");
                seen.push(name);
            }
        }
        // The two the screener cannot work without: a helper that lost them
        // would still draw a table, and it would be the table of a pool the
        // operator cannot change.
        assert!(seen.contains(&MARKET_TAGS) && seen.contains(&"DynWL_Count"));
    }

    fn shot(id: u64, ver: i32, date: u64, path: &str, size: f64) -> StrategySnapshot {
        let mut fields = StrategyFields::new();
        fields.insert("StrategyName", FieldValue::String(format!("S{id}")));
        fields.insert("OrderSize", FieldValue::Double(size));
        StrategySnapshot::new(id, ver, date, false, StrategyKind::MOON_SHOT, path, fields)
    }

    fn snap(
        st: &Strategies,
        rows: &[StrategySnapshot],
        full: bool,
        epoch: u64,
        folders: i64,
    ) -> Snapshot {
        Snapshot {
            server_epoch: epoch,
            client_max_last_date: 0,
            full,
            data: strat::encode_batch(st.schema(), rows, ["Empty"]),
            folders_last_modified: folders,
        }
    }

    #[test]
    fn rollback_guard_order_and_folders() {
        let mut st = Strategies::new(None, 1_000);
        let a = shot(1, 1, 100, "Shots", 500.0);
        let b = shot(2, 1, 100, "", 700.0);
        let foreign = StrategySnapshot::new(
            3,
            0,
            100,
            false,
            StrategyKind::from_ordinal(1),
            "",
            StrategyFields::new(),
        );
        let rows = [a.clone(), b.clone(), foreign];
        let (touched, rejected) = st.apply_snapshot(&snap(&st, &rows, true, 2_000, 2_000));
        assert_eq!((touched, rejected), (vec![1, 2], vec![3]));
        assert_eq!(st.list().len(), 2);
        assert!(st.folders().iter().any(|f| f == "Empty"));
        assert!(st.folders().iter().any(|f| f == "Shots"));

        // Same revision with other fields: kept.
        let stale = shot(1, 1, 100, "Shots", 1.0);
        st.apply_snapshot(&snap(&st, &[stale], false, 0, 0));
        assert_eq!(st.list()[0].fields.get_double("OrderSize"), Some(500.0));
        // Newer date, same ver (the terminal's edit): replaced.
        let edit = shot(1, 1, 101, "Shots", 900.0);
        st.apply_snapshot(&snap(&st, &[edit], false, 0, 0));
        assert_eq!(st.list()[0].fields.get_double("OrderSize"), Some(900.0));

        // Older Full does not reorder; newer one does.
        st.apply_snapshot(&snap(&st, &[b.clone(), a.clone()], true, 1_500, 0));
        assert_eq!(st.list()[0].strategy_id, 1);
        st.apply_snapshot(&snap(&st, &[b, a], true, 3_000, 0));
        assert_eq!(st.list()[0].strategy_id, 2);

        // Folder delete refuses an occupied folder, removes an empty one.
        assert!(!st.delete(0, "Shots", 4_000));
        assert!(st.delete(0, "Empty", 4_000));
        assert!(!st.folders().iter().any(|f| f == "Empty"));
        assert!(st.delete(2, "", 4_001));
        assert_eq!(st.list().len(), 1);
        assert!(st.partial_payload(1, &[2]).is_none());
        assert!(st.partial_payload(1, &[1]).is_some());
    }

    /// A file pasted together by hand can hold one strategy id twice: it is listed once.
    #[test]
    fn a_strategy_id_twice_in_the_file_is_listed_once() {
        let dir = std::env::temp_dir().join(format!("aster-dupid-{}", std::process::id()));
        let file = dir.join("strategies.txt");
        let _ = std::fs::remove_dir_all(&dir);
        let mut st = Strategies::new(Some(file.clone()), 1_000);
        let a = shot(7, 3, 5_000, "A", 250.0);
        st.apply_snapshot(&snap(&st, &[a], true, 2_000, 2_000));
        let text = std::fs::read_to_string(&file).unwrap();
        std::fs::write(&file, format!("{text}{text}")).unwrap();
        let again = Strategies::new(Some(file.clone()), 9_000);
        assert_eq!(again.list().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persists_and_reloads() {
        let dir = std::env::temp_dir().join(format!("aster-strat-{}", std::process::id()));
        let file = dir.join("strategies.txt");
        let _ = std::fs::remove_file(&file);
        let mut st = Strategies::new(Some(file.clone()), 1_000);
        let mut a = shot(7, 3, 5_000, "A/B", 250.0);
        a.checked = true;
        a.fields.insert("SoundAlert", FieldValue::Bool(false));
        st.apply_snapshot(&snap(&st, &[a.clone()], true, 2_000, 2_000));
        st.set_checked(&[CheckedItem {
            strategy_id: 7,
            checked: true,
        }]);

        let again = Strategies::new(Some(file.clone()), 9_000);
        assert_eq!(again.list().len(), 1);
        let s = &again.list()[0];
        assert_eq!((s.strategy_id, s.strategy_ver, s.last_date), (7, 3, 5_000));
        assert!(s.checked);
        assert_eq!(s.path.as_ref(), "A/B");
        assert_eq!(s.fields, a.fields);
        for f in ["A", "A/B", "Empty"] {
            assert!(again.folders().iter().any(|x| x == f), "folder {f}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Folder paths with a leading or doubled `/`, or with multi-byte names, register the right
    /// parents and do not panic.
    #[test]
    fn add_folder_registers_the_parents_of_any_spelling() {
        let mut folders = Vec::new();
        add_folder(&mut folders, "/Тест//Вложенная/");
        assert_eq!(folders, ["Тест", "Тест/Вложенная"]);
        // ASCII case folds (the first spelling wins); other alphabets compare as written.
        add_folder(&mut folders, "a/B");
        add_folder(&mut folders, "A/b/c");
        assert_eq!(folders, ["Тест", "Тест/Вложенная", "a", "a/B", "A/b/c"]);
    }

    /// A file that cannot be read (here: not UTF-8) is not the empty list: the first change from
    /// the terminal must not write over it.
    #[test]
    fn unreadable_file_is_not_overwritten() {
        let dir = std::env::temp_dir().join(format!("aster-unreadable-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("strategies.txt");
        let bytes = [0xC8u8, 0xE3, 0xF0, 0xE0, 0x0D, 0x0A];
        std::fs::write(&file, bytes).unwrap();
        let mut st = Strategies::new(Some(file.clone()), 1_000);
        assert!(st.list().is_empty());
        st.set_checked(&[CheckedItem {
            strategy_id: 1,
            checked: true,
        }]);
        st.save();
        assert_eq!(std::fs::read(&file).unwrap(), bytes);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What the core itself writes for an empty list reads back as empty and stays savable.
    #[test]
    fn own_empty_file_stays_savable() {
        let dir = std::env::temp_dir().join(format!("aster-empty-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let file = dir.join("strategies.txt");
        let st = Strategies::new(Some(file.clone()), 1_000);
        st.save();
        let again = Strategies::new(Some(file.clone()), 2_000);
        assert!(again.list().is_empty());
        assert!(!again.unreadable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// DropsDetection is accepted, sees its own fields and not MoonShot's,
    /// and survives the file under its MoonBot name.
    #[test]
    fn drops_kind_fields_and_file() {
        let names = |kind| -> Vec<String> {
            let st = Strategies::new(None, 0);
            st.schema()
                .editor_sections_for_strategy_kind(kind)
                .iter()
                .flat_map(|s| s.fields.iter().map(|f| f.name.to_string()))
                .collect()
        };
        let drops = names(StrategyKind::DROPS);
        let shot = names(StrategyKind::MOON_SHOT);
        for f in [
            "DropsPriceDelta",
            "buyPrice",
            "NextDetectPenalty",
            "SellPrice",
        ] {
            assert!(drops.iter().any(|n| n == f), "{f} for Drops");
        }
        for f in ["MShotPrice", "Short", "MShotUsePrice"] {
            assert!(!drops.iter().any(|n| n == f), "{f} hidden for Drops");
            assert!(shot.iter().any(|n| n == f), "{f} for MoonShot");
        }
        assert!(!shot.iter().any(|n| n == "DropsPriceDelta"));
        let strike = names(StrategyKind::MOON_STRIKE);
        for f in [
            "MStrikeDepth",
            "MStrikeDirection",
            "NextDetectPenalty",
            "PriceDownTimer",
        ] {
            assert!(strike.iter().any(|n| n == f), "{f} for MoonStrike");
        }
        for f in ["SellPrice", "MShotPrice", "DropsPriceDelta", "Short"] {
            assert!(!strike.iter().any(|n| n == f), "{f} hidden for MoonStrike");
        }

        let dir = std::env::temp_dir().join(format!("aster-drops-{}", std::process::id()));
        let file = dir.join("strategies.txt");
        let _ = std::fs::remove_file(&file);
        let mut st = Strategies::new(Some(file.clone()), 1_000);
        let mut fields = StrategyFields::new();
        fields.insert("StrategyName", FieldValue::String("D".into()));
        fields.insert("DropsPriceDelta", FieldValue::Double(1.5));
        let d = StrategySnapshot::new(5, 1, 2_000, true, StrategyKind::DROPS, "", fields);
        let (touched, rejected) = st.apply_snapshot(&snap(&st, &[d], true, 2_000, 2_000));
        assert_eq!((touched, rejected), (vec![5], vec![]));
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(text.contains("SignalType=DropsDetection"));
        let again = Strategies::new(Some(file), 9_000);
        let s = &again.list()[0];
        assert_eq!(s.kind(), StrategyKind::DROPS);
        assert_eq!(s.fields.get_double("DropsPriceDelta"), Some(1.5));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A strategies file pasted from MoonBot keeps its `Delta_BTC_*` and
    /// `MShotAddBTCDelta`: on Aster they mean what they mean in MoonBot, so
    /// they are schema fields under their own names, not renamed ones.
    #[test]
    fn moonbot_btc_fields_are_read_under_their_own_names() {
        let dir = std::env::temp_dir().join(format!("aster-btc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("strategies.txt");
        std::fs::write(
            &file,
            "##Begin_Strategy\n   Active=-1\n   FVersion=1\n   FIntID=9\n   \
             LastEditDate=5000\n   SignalType=MoonShot\n   StrategyName=A\n   \
             Delta_BTC_Min=-5\n   Delta_BTC_Max=5\n   MShotAddBTCDelta=0.2\n##End_Strategy\n",
        )
        .unwrap();
        let st = Strategies::new(Some(file), 1_000);
        let s = &st.list()[0];
        assert_eq!(s.fields.get_double("Delta_BTC_Min"), Some(-5.0));
        assert_eq!(s.fields.get_double("Delta_BTC_Max"), Some(5.0));
        assert_eq!(s.fields.get_double("MShotAddBTCDelta"), Some(0.2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deleting_unrelated_folder_handles_utf8_boundaries() {
        let mut st = Strategies::new(None, 1000);
        let rows = [shot(1, 1, 1001, "Русские/Акции", 500.0)];
        let snapshot = Snapshot {
            server_epoch: 2000,
            client_max_last_date: 1001,
            full: true,
            data: strat::encode_batch(st.schema(), &rows, ["A", "Я/Пустая"]),
            folders_last_modified: 2000,
        };
        st.apply_snapshot(&snapshot);
        assert!(st.delete(0, "A", 3000));
        assert!(st.delete(0, "Я", 3001));
        assert!(!st.delete(0, "Русские", 3002));
        assert_eq!(st.list().len(), 1);
        assert!(!st.folders().iter().any(|f| f.starts_with('Я')));
    }
}
