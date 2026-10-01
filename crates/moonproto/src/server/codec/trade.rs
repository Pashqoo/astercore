//! `Command::Order` server side. Mirrors `commands::trade::order_v2`: parses
//! the terminal's `TOrderCommand` (47) and builds the canonical order state
//! (`TOrderImage` 41 / `TOrdersSnapshot` 43 / `TOrderNotFound` 46).

use super::BaseHeader;
use crate::commands::trade::{StopSettings, OFL_IMMUNE, OFL_PANIC_ON};

const CMD_ORDER_IMAGE: u8 = 41;
const CMD_ORDERS_SNAPSHOT: u8 = 43;
/// `TOrderStatusRequest`; `uid == 0` asks for the whole snapshot.
pub const CMD_ORDER_STATUS_REQUEST: u8 = 45;
const CMD_ORDER_NOT_FOUND: u8 = 46;
pub const CMD_ORDER_COMMAND: u8 = 47;

/// `OrderWorkerStatus` ordinals.
pub mod status {
    pub const NONE: u8 = 0;
    pub const BUY_FAIL: u8 = 1;
    pub const BUY_SET: u8 = 2;
    pub const BUY_CANCEL: u8 = 3;
    pub const BUY_DONE: u8 = 4;
    pub const SELL_FAIL: u8 = 5;
    pub const SELL_SET: u8 = 6;
    pub const SELL_CANCEL: u8 = 7;
    pub const SELL_DONE: u8 = 8;

    pub fn is_terminal(s: u8) -> bool {
        matches!(
            s,
            BUY_FAIL | BUY_CANCEL | SELL_FAIL | SELL_CANCEL | SELL_DONE
        )
    }
}

const STATE_SIZE: usize = 342;
const ALL_SECTIONS: u16 = 0x1fff;
const NAME_MAX: usize = 64;
/// Description flags (`ODF_*`): an emulator order, a short.
const DESC_EMULATOR: u8 = 1;
const DESC_IS_SHORT: u8 = 2;
/// `OrderType` ordinals of the two legs.
const ORDER_TYPE_SELL: u8 = 0;
const ORDER_TYPE_BUY: u8 = 1;
/// `OrderSubType` ordinals.
const SUB_TYPE_LIMIT: u8 = 0;
const SUB_TYPE_MARKET: u8 = 5;

// Canonical state offsets (see `order_v2::ORDER_SECTION_OFFSET`).
const OFS_STATUS: usize = 0;
const OFS_STRAT_ID: usize = 1;
/// Sell reason byte of the FLAGS section (MoonBot `SellReason` code, 0 = unset).
const OFS_SELL_REASON: usize = 9;
/// Order flags byte of the FLAGS section (`OFL_*`).
const OFS_FLAGS: usize = 10;
const OFS_BUY_TARGET: usize = 11;
const OFS_SELL_TARGET: usize = 28;
const OFS_BUY_EXEC: usize = 37;
const OFS_BUY_PLACEMENT: usize = 70;
const OFS_BUY_SLOW: usize = 133;
const OFS_SELL_EXEC: usize = 153;
const OFS_SELL_PLACEMENT: usize = 186;
const OFS_SELL_SLOW: usize = 249;
const OFS_STOPS: usize = 269;
const OFS_PLANNED: usize = 333;
const OFS_MARKET_STOP: usize = 341;

/// New-order request (`Start` 10 / `StartPending` 18; for the latter `price`
/// is the trigger).
#[derive(Debug, Clone, PartialEq)]
pub struct StartOrder {
    pub market: String,
    pub is_short: bool,
    pub use_market_stop: bool,
    pub strategy_id: u64,
    /// Notional in the core base currency.
    pub size: f64,
    pub price: f64,
    pub planned_sell: f64,
}

/// `TOrderCommand` bodies the core acts on; everything else is `Other`.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderCommand {
    TargetBuy {
        order_id: u64,
        price: f64,
        size: f64,
    },
    TargetSell {
        order_id: u64,
        price: f64,
    },
    CancelBuy {
        order_id: u64,
    },
    CancelSell {
        order_id: u64,
    },
    PendingCancel {
        order_id: u64,
    },
    /// `StopSettings` of one order: `sl_level` is a price when `sl_fixed`, a
    /// % below the entry otherwise; `*_spread` is the % the exit limit crosses
    /// the book; `trail_level` is a % below the best price (a price distance
    /// when `trail_fixed`); `tp` is a price.
    Stops {
        order_id: u64,
        sl_on: bool,
        sl_fixed: bool,
        sl_level: f64,
        sl_spread: f64,
        trail_on: bool,
        trail_fixed: bool,
        trail_level: f64,
        trail_spread: f64,
        tp_on: bool,
        tp: f64,
    },
    /// Panic sell on/off for one order.
    Panic {
        order_id: u64,
        enabled: bool,
    },
    Start(StartOrder),
    StartPending(StartOrder),
    /// `mode` 0 = close (`flag` = market sell), 1 = limit close, 2/3 = split.
    ClosePosition {
        market: String,
        mode: u8,
        flag: bool,
    },
    /// Panic sell on for every open position of the core.
    PanicSellAll,
    /// «Move all» of one leg on a market (`sells`: the exits, else the
    /// entries): `kind` 0 = by `move_kind` to `price` for a `side`
    /// (0 both, 1 long, 2 short), 1 = the price zone `[price, max]`, 2 = by
    /// `price` percent.
    MoveAll {
        market: String,
        sells: bool,
        kind: u8,
        move_kind: u8,
        side: u8,
        price: f64,
        max: f64,
    },
    /// Immune for clicks: the order stays out of «Move all».
    Immune {
        order_id: u64,
        enabled: bool,
    },
    Other(u8),
}

impl OrderCommand {
    /// Parse the body that follows `BaseHeader` (Delphi zero-tail reads).
    pub fn parse(body: &[u8]) -> Self {
        let mut r = Reader(body);
        let opcode = r.u8();
        match opcode {
            0 => Self::TargetBuy {
                order_id: r.u64(),
                price: r.f64(),
                size: r.f64(),
            },
            1 => Self::TargetSell {
                order_id: r.u64(),
                price: r.f64(),
            },
            3 => Self::Stops {
                order_id: r.u64(),
                sl_on: r.u8() != 0,
                sl_fixed: r.u8() != 0,
                sl_level: r.f64(),
                sl_spread: r.f64(),
                trail_on: r.u8() != 0,
                trail_fixed: r.u8() != 0,
                trail_level: r.f64(),
                trail_spread: r.f64(),
                tp_on: r.u8() != 0,
                tp: r.f64(),
            },
            5 => Self::Panic {
                order_id: r.u64(),
                enabled: r.u8() & 1 != 0,
            },
            7 => Self::CancelBuy { order_id: r.u64() },
            8 => Self::CancelSell { order_id: r.u64() },
            9 => Self::PendingCancel { order_id: r.u64() },
            10 | 18 => {
                let market = r.short_string();
                let flags = r.u8();
                let start = StartOrder {
                    market,
                    is_short: flags & 1 != 0,
                    use_market_stop: flags & 2 != 0,
                    strategy_id: r.u64(),
                    size: r.f64(),
                    price: r.f64(),
                    planned_sell: r.f64(),
                };
                if opcode == 10 {
                    Self::Start(start)
                } else {
                    Self::StartPending(start)
                }
            }
            14 => Self::ClosePosition {
                market: r.short_string(),
                mode: r.u8(),
                flag: r.u8() & 1 != 0,
            },
            17 => Self::PanicSellAll,
            6 => Self::Immune {
                order_id: r.u64(),
                enabled: r.u8() & 1 != 0,
            },
            11 => {
                let market = r.short_string();
                let sells = r.u8() == 0;
                let kind = r.u8();
                let (move_kind, side, price, max) = match kind {
                    0 => {
                        let move_kind = r.u8();
                        let side = r.u8();
                        (move_kind, side, r.f64(), 0.0)
                    }
                    1 => {
                        let side = r.u8();
                        let min = r.f64();
                        (0, side, min, r.f64())
                    }
                    _ => (0, 0, r.f64(), 0.0),
                };
                Self::MoveAll {
                    market,
                    sells,
                    kind,
                    move_kind,
                    side,
                    price,
                    max,
                }
            }
            other => Self::Other(other),
        }
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0u8; N];
        let n = N.min(self.0.len());
        out[..n].copy_from_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        out
    }

    fn u8(&mut self) -> u8 {
        self.take::<1>()[0]
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn f64(&mut self) -> f64 {
        f64::from_le_bytes(self.take())
    }

    fn short_string(&mut self) -> String {
        let len = (self.u8() as usize).min(self.0.len());
        let s = String::from_utf8_lossy(&self.0[..len]).into_owned();
        self.0 = &self.0[len..];
        s
    }
}

/// One exchange leg of a core order as the terminal shows it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LegState {
    /// Exchange order id folded into `int_id`.
    pub exchange_id: i64,
    /// Order price (`actual_price`).
    pub price: f64,
    /// Units ordered / filled so far.
    pub quantity: f64,
    pub filled: f64,
    pub mean_price: f64,
    /// Notional in base currency at placement (buy target `size`) and spent.
    pub notional: f64,
    pub spent: f64,
    pub create_ms: i64,
    pub open_ms: i64,
    pub close_ms: i64,
    pub is_market: bool,
    pub opened: bool,
    pub closed: bool,
    pub canceled: bool,
}

/// Everything the canonical image carries for one core order.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord<'a> {
    pub id: u64,
    pub rev: u64,
    pub market: &'a str,
    pub is_short: bool,
    pub status: u8,
    pub strategy_id: u64,
    pub buy: LegState,
    pub sell: LegState,
    pub planned_sell: f64,
    pub use_market_stop: bool,
    /// Armed stop-loss as `(price, spread %)`; shown as a fixed SL.
    pub stop: Option<(f64, f64)>,
    /// Armed trailing stop as `(level %, spread %)`.
    pub trailing: Option<(f64, f64)>,
    /// Take-profit price (the trailing stop's start).
    pub take_profit: Option<f64>,
    pub panic: bool,
    /// «Immune for clicks»: «Move all» by kind leaves it alone.
    pub immune: bool,
    /// MoonBot `SellReason` code of the exit (0 = unset).
    pub sell_reason: u8,
    /// An emulator order: the terminal marks it `(E)` and leaves it out of
    /// the real-position checks.
    pub emulator: bool,
}

impl OrderRecord<'_> {
    fn state(&self) -> [u8; STATE_SIZE] {
        let mut s = [0u8; STATE_SIZE];
        s[OFS_STATUS] = self.status;
        put_u64(&mut s, OFS_STRAT_ID, self.strategy_id);
        s[OFS_SELL_REASON] = self.sell_reason;
        if self.panic {
            s[OFS_FLAGS] |= OFL_PANIC_ON;
        }
        if self.immune {
            s[OFS_FLAGS] |= OFL_IMMUNE;
        }
        put_f64(&mut s, OFS_BUY_TARGET, self.buy.price);
        put_f64(&mut s, OFS_BUY_TARGET + 8, self.buy.notional);
        put_f64(&mut s, OFS_SELL_TARGET, self.sell.price);
        put_leg(
            &mut s,
            &self.buy,
            ORDER_TYPE_BUY,
            OFS_BUY_EXEC,
            OFS_BUY_PLACEMENT,
            OFS_BUY_SLOW,
        );
        put_leg(
            &mut s,
            &self.sell,
            ORDER_TYPE_SELL,
            OFS_SELL_EXEC,
            OFS_SELL_PLACEMENT,
            OFS_SELL_SLOW,
        );
        if self.stop.is_some() || self.trailing.is_some() || self.take_profit.is_some() {
            let mut stops = StopSettings::disabled();
            if let Some((price, spread)) = self.stop {
                stops = stops.with_stop_loss_fixed(price, spread);
            }
            if let Some((level, spread)) = self.trailing {
                stops = stops.with_trailing_percent(level, spread);
            }
            if let Some(price) = self.take_profit {
                stops = stops.with_take_profit_price(price);
            }
            let mut bytes = Vec::with_capacity(48);
            stops.write_to(&mut bytes);
            s[OFS_STOPS..OFS_STOPS + bytes.len()].copy_from_slice(&bytes);
        }
        put_f64(&mut s, OFS_PLANNED, self.planned_sell);
        s[OFS_MARKET_STOP] = u8::from(self.use_market_stop);
        s
    }

    /// `state_rev + description + mask + all sections`.
    fn write_body(&self, out: &mut Vec<u8>) {
        write_uleb(out, self.rev);
        let name = self.market.as_bytes();
        let name = &name[..name.len().min(NAME_MAX)];
        out.push(name.len() as u8);
        out.extend_from_slice(name);
        let mut flags = 0;
        if self.is_short {
            flags |= DESC_IS_SHORT;
        }
        if self.emulator {
            flags |= DESC_EMULATOR;
        }
        out.push(flags);
        out.extend_from_slice(&ALL_SECTIONS.to_le_bytes());
        out.extend_from_slice(&self.state());
    }
}

fn put_u64(s: &mut [u8], at: usize, v: u64) {
    s[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn put_i64(s: &mut [u8], at: usize, v: i64) {
    s[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn put_f64(s: &mut [u8], at: usize, v: f64) {
    s[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn put_leg(
    s: &mut [u8],
    leg: &LegState,
    order_type: u8,
    exec: usize,
    placement: usize,
    slow: usize,
) {
    put_f64(s, exec, leg.quantity - leg.filled); // quantity_remaining
    put_f64(s, exec + 8, leg.filled); // actual_q
    put_f64(s, exec + 16, leg.filled * leg.mean_price); // total_btc
    put_f64(s, exec + 24, leg.mean_price);
    s[exec + 32] = u8::from(leg.filled > 0.0 && leg.filled < leg.quantity); // partial_done

    put_i64(s, placement, leg.exchange_id);
    put_f64(s, placement + 8, leg.price);
    put_i64(s, placement + 16, leg.open_ms);
    put_f64(s, placement + 24, leg.quantity);
    // `quantity_base` is units too: the terminal sizes chart labels and the
    // ORDERS column from it (`size × price`), the notional goes to `buy_size`.
    put_f64(s, placement + 32, leg.quantity);
    put_i64(s, placement + 40, leg.close_ms);
    put_i64(s, placement + 48, leg.create_ms);
    s[placement + 57] = order_type;
    s[placement + 58] = if leg.is_market {
        SUB_TYPE_MARKET
    } else {
        SUB_TYPE_LIMIT
    };
    s[placement + 59] = 1; // leverage
    s[placement + 60] = u8::from(leg.opened);
    s[placement + 61] = u8::from(leg.closed);
    s[placement + 62] = u8::from(leg.canceled);

    put_f64(s, slow, leg.spent);
}

fn write_uleb(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// `TOrderImage` (41): full state of one order, `uid` = order id.
pub fn order_image(rec: &OrderRecord<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 80 + STATE_SIZE);
    BaseHeader::write(&mut out, CMD_ORDER_IMAGE, rec.id);
    rec.write_body(&mut out);
    out
}

/// `TOrdersSnapshot` (43) covering the whole id range.
pub fn orders_snapshot(uid: u64, recs: &[OrderRecord<'_>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 16 + recs.len() * (90 + STATE_SIZE));
    BaseHeader::write(&mut out, CMD_ORDERS_SNAPSHOT, uid);
    out.extend_from_slice(&0u64.to_le_bytes()); // from_uid
    out.extend_from_slice(&0u64.to_le_bytes()); // range_end_uid (0 = open)
    for rec in recs {
        out.extend_from_slice(&rec.id.to_le_bytes());
        rec.write_body(&mut out);
    }
    out
}

/// `TOrderNotFound` (46) for a status request about an unknown order.
pub fn order_not_found(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE);
    BaseHeader::write(&mut out, CMD_ORDER_NOT_FOUND, uid);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::trade::{
        build_order_command, OrderCommandPayload, OrderSubType, OrderType, OrderWorkerStatus,
        TradeCommand,
    };

    fn parse_cmd(payload: OrderCommandPayload) -> (u64, OrderCommand) {
        let raw = build_order_command(0x55, payload);
        let hdr = BaseHeader::parse(&raw).unwrap();
        assert_eq!(hdr.cmd_id, CMD_ORDER_COMMAND);
        (
            hdr.uid,
            OrderCommand::parse(&raw[super::super::BASE_HEADER_SIZE..]),
        )
    }

    #[test]
    fn parses_upstream_order_commands() {
        let (uid, cmd) = parse_cmd(OrderCommandPayload::Start {
            market_name: "SBER".into(),
            is_short: true,
            use_market_stop: true,
            strategy_id: 9,
            size: 3000.0,
            price: 310.5,
            planned_sell_price: 320.0,
        });
        assert_eq!(uid, 0x55);
        assert_eq!(
            cmd,
            OrderCommand::Start(StartOrder {
                market: "SBER".into(),
                is_short: true,
                use_market_stop: true,
                strategy_id: 9,
                size: 3000.0,
                price: 310.5,
                planned_sell: 320.0,
            })
        );
        let (_, cmd) = parse_cmd(OrderCommandPayload::StartPending {
            market_name: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 1.0,
            trigger_price: 300.0,
            planned_sell_price: 0.0,
        });
        assert!(matches!(cmd, OrderCommand::StartPending(s) if s.price == 300.0 && !s.is_short));
        let (_, cmd) = parse_cmd(OrderCommandPayload::TargetBuy {
            order_id: 7,
            price: 1.5,
            size: 2.5,
        });
        assert_eq!(
            cmd,
            OrderCommand::TargetBuy {
                order_id: 7,
                price: 1.5,
                size: 2.5
            }
        );
        let (_, cmd) = parse_cmd(OrderCommandPayload::TargetSell {
            order_id: 7,
            price: 1.5,
        });
        assert_eq!(
            cmd,
            OrderCommand::TargetSell {
                order_id: 7,
                price: 1.5
            }
        );
        for (payload, expected) in [
            (
                OrderCommandPayload::CancelBuy { order_id: 1 },
                OrderCommand::CancelBuy { order_id: 1 },
            ),
            (
                OrderCommandPayload::CancelSell { order_id: 2 },
                OrderCommand::CancelSell { order_id: 2 },
            ),
            (
                OrderCommandPayload::PendingCancel { order_id: 3 },
                OrderCommand::PendingCancel { order_id: 3 },
            ),
            (
                OrderCommandPayload::ClosePosition {
                    market_name: "SBER".into(),
                    mode: 0,
                    flag: true,
                },
                OrderCommand::ClosePosition {
                    market: "SBER".into(),
                    mode: 0,
                    flag: true,
                },
            ),
            (
                OrderCommandPayload::Panic {
                    order_id: 4,
                    enabled: true,
                },
                OrderCommand::Panic {
                    order_id: 4,
                    enabled: true,
                },
            ),
            (
                OrderCommandPayload::Stops {
                    order_id: 5,
                    stops: StopSettings::disabled()
                        .with_stop_loss_percent(1.5, 0.4)
                        .with_take_profit_price(330.0),
                },
                OrderCommand::Stops {
                    order_id: 5,
                    sl_on: true,
                    sl_fixed: false,
                    sl_level: 1.5,
                    sl_spread: 0.4,
                    trail_on: false,
                    trail_fixed: false,
                    trail_level: 0.0,
                    trail_spread: 0.0,
                    tp_on: true,
                    tp: 330.0,
                },
            ),
            (
                OrderCommandPayload::Stops {
                    order_id: 6,
                    stops: StopSettings::disabled().with_trailing_percent(1.0, 0.5),
                },
                OrderCommand::Stops {
                    order_id: 6,
                    sl_on: false,
                    sl_fixed: false,
                    sl_level: 0.0,
                    sl_spread: 0.0,
                    trail_on: true,
                    trail_fixed: false,
                    trail_level: 1.0,
                    trail_spread: 0.5,
                    tp_on: false,
                    tp: 0.0,
                },
            ),
            (
                OrderCommandPayload::PanicSellAll,
                OrderCommand::PanicSellAll,
            ),
            (
                OrderCommandPayload::Immune {
                    order_id: 9,
                    enabled: true,
                },
                OrderCommand::Immune {
                    order_id: 9,
                    enabled: true,
                },
            ),
            (
                OrderCommandPayload::MoveAllPercent {
                    market_name: "SBER".into(),
                    leg: 1,
                    percent: -0.5,
                },
                OrderCommand::MoveAll {
                    market: "SBER".into(),
                    sells: false,
                    kind: 2,
                    move_kind: 0,
                    side: 0,
                    price: -0.5,
                    max: 0.0,
                },
            ),
            (
                OrderCommandPayload::MoveAllKind {
                    market_name: "SBER".into(),
                    leg: 0,
                    move_kind: 5,
                    side: 1,
                    price: 310.0,
                },
                OrderCommand::MoveAll {
                    market: "SBER".into(),
                    sells: true,
                    kind: 0,
                    move_kind: 5,
                    side: 1,
                    price: 310.0,
                    max: 0.0,
                },
            ),
            (
                OrderCommandPayload::MoveAllZone {
                    market_name: "SBER".into(),
                    side: 2,
                    min_price: 300.0,
                    max_price: 305.0,
                },
                OrderCommand::MoveAll {
                    market: "SBER".into(),
                    sells: true,
                    kind: 1,
                    move_kind: 0,
                    side: 2,
                    price: 300.0,
                    max: 305.0,
                },
            ),
        ] {
            assert_eq!(parse_cmd(payload).1, expected);
        }
        assert_eq!(
            OrderCommand::parse(&[10, 9, b'S']),
            OrderCommand::Start(StartOrder {
                market: "S".into(),
                is_short: false,
                use_market_stop: false,
                strategy_id: 0,
                size: 0.0,
                price: 0.0,
                planned_sell: 0.0,
            }),
            "zero-tail reads like Delphi"
        );
    }

    fn record() -> OrderRecord<'static> {
        OrderRecord {
            id: 0x1234,
            rev: 300,
            market: "SBER",
            is_short: true,
            status: status::BUY_DONE,
            strategy_id: 5,
            buy: LegState {
                exchange_id: 987_654_321,
                price: 310.5,
                quantity: 20.0,
                filled: 20.0,
                mean_price: 310.4,
                notional: 6210.0,
                spent: 6208.0,
                create_ms: 1_700_000_000_000,
                open_ms: 1_700_000_001_000,
                close_ms: 1_700_000_002_000,
                is_market: false,
                opened: true,
                closed: true,
                canceled: false,
            },
            sell: LegState {
                price: 320.0,
                quantity: 20.0,
                filled: 5.0,
                mean_price: 320.0,
                is_market: true,
                ..LegState::default()
            },
            planned_sell: 320.0,
            use_market_stop: true,
            stop: Some((305.5, 1.5)),
            immune: true,
            trailing: None,
            take_profit: None,
            panic: true,
            sell_reason: 7,
            emulator: false,
        }
    }

    #[test]
    fn image_materializes_upstream() {
        let raw = order_image(&record());
        let Some(TradeCommand::OrderImage(img)) = TradeCommand::parse(&raw) else {
            panic!("not an image");
        };
        assert_eq!(
            (img.header.uid, img.state_rev, img.section_mask),
            (0x1234, 300, ALL_SECTIONS)
        );
        assert_eq!(img.desc.market_name(), "SBER");
        assert!(img.desc.is_short() && !img.desc.emulator());
        let s = &img.state;
        // FLAGS: StopLoss reason, panic on; STOPS: a fixed stop-loss with its spread.
        assert_eq!(
            (s.read_u8(9), s.read_u8(10)),
            (7, OFL_PANIC_ON | OFL_IMMUNE)
        );
        let stops = s.stops();
        assert!(stops.stop_loss_enabled() && stops.stop_loss_fixed());
        assert_eq!(
            (stops.stop_loss_level(), stops.stop_loss_spread()),
            (305.5, 1.5)
        );
        assert!(!stops.trailing_enabled() && !stops.take_profit_enabled());
        let mut plain = record();
        plain.stop = None;
        plain.panic = false;
        plain.immune = false;
        plain.emulator = true;
        let Some(TradeCommand::OrderImage(img)) = TradeCommand::parse(&order_image(&plain)) else {
            panic!("not an image");
        };
        assert!(img.desc.is_short() && img.desc.emulator());
        assert_eq!(img.state.read_u8(10), 0);
        assert!(!img.state.stops().stop_loss_enabled());
        // Trailing and take profit share the STOPS section with the stop-loss.
        let mut trailed = record();
        trailed.trailing = Some((1.2, 0.3));
        trailed.take_profit = Some(330.0);
        let Some(TradeCommand::OrderImage(img)) = TradeCommand::parse(&order_image(&trailed))
        else {
            panic!("not an image");
        };
        let t = img.state.stops();
        assert!(t.stop_loss_enabled() && t.trailing_enabled() && !t.trailing_fixed());
        assert_eq!((t.trailing_level(), t.trailing_spread()), (1.2, 0.3));
        assert!(t.take_profit_enabled());
        assert_eq!(t.take_profit(), 330.0);
        assert_eq!(
            OrderWorkerStatus::from_byte(s.read_u8(0)),
            OrderWorkerStatus::BuyDone
        );
        assert_eq!(s.read_u64(1), 5);
        assert_eq!((s.read_f64(11), s.read_f64(19)), (310.5, 6210.0));
        assert_eq!(s.read_f64(28), 320.0);
        // buy exec: remaining, filled, total, mean, partial
        assert_eq!(
            (s.read_f64(37), s.read_f64(45), s.read_f64(61)),
            (0.0, 20.0, 310.4)
        );
        assert_eq!(s.read_f64(53), 20.0 * 310.4);
        assert_eq!(s.read_u8(69), 0);
        // buy placement
        assert_eq!(s.read_i64(70), 987_654_321);
        assert_eq!((s.read_f64(78), s.read_i64(86)), (310.5, 1_700_000_001_000));
        assert_eq!((s.read_f64(94), s.read_f64(102)), (20.0, 20.0));
        assert_eq!(
            (s.read_i64(110), s.read_i64(118)),
            (1_700_000_002_000, 1_700_000_000_000)
        );
        assert_eq!(OrderType::from_byte(s.read_u8(127)), OrderType::Buy);
        assert_eq!(OrderSubType::from_byte(s.read_u8(128)), OrderSubType::Limit);
        assert_eq!(
            (
                s.read_u8(129),
                s.read_u8(130),
                s.read_u8(131),
                s.read_u8(132)
            ),
            (1, 1, 1, 0)
        );
        assert_eq!(s.read_f64(133), 6208.0);
        // sell exec/placement: partial market sell
        assert_eq!(
            (s.read_f64(153), s.read_f64(161), s.read_u8(185)),
            (15.0, 5.0, 1)
        );
        assert_eq!(OrderType::from_byte(s.read_u8(243)), OrderType::Sell);
        assert_eq!(
            OrderSubType::from_byte(s.read_u8(244)),
            OrderSubType::Market
        );
        assert_eq!((s.read_f64(333), s.read_u8(341)), (320.0, 1));
        assert!(!s.is_terminal());
    }

    #[test]
    fn snapshot_and_not_found_parse_upstream() {
        let rec = record();
        let mut second = record();
        second.id = 0x99;
        second.status = status::SELL_DONE;
        let raw = orders_snapshot(4, &[rec, second]);
        let Some(TradeCommand::OrdersSnapshot(s)) = TradeCommand::parse(&raw) else {
            panic!("not a snapshot");
        };
        assert_eq!((s.header.uid, s.from_uid, s.range_end_uid), (4, 0, 0));
        assert_eq!(s.records.len(), 2);
        assert_eq!(
            (s.records[0].order_id, s.records[1].order_id),
            (0x1234, 0x99)
        );
        assert_eq!(s.records[1].state_rev, 300);
        assert!(s.records[1].state.is_terminal());

        let empty = orders_snapshot(1, &[]);
        assert!(
            matches!(TradeCommand::parse(&empty), Some(TradeCommand::OrdersSnapshot(s)) if s.records.is_empty())
        );
        assert!(matches!(
            TradeCommand::parse(&order_not_found(77)),
            Some(TradeCommand::OrderNotFound(h)) if h.uid == 77
        ));
        assert!(status::is_terminal(status::BUY_FAIL) && !status::is_terminal(status::SELL_SET));
    }
}
