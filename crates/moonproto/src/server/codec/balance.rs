//! `Command::Balance` server side. Mirrors `commands::balance`.

use super::BaseHeader;

/// Full balance snapshot (CmdId 3).
const CMD_BALANCE_FULL: u8 = 3;
/// Client asks for a full refresh (CmdId 5).
pub const CMD_REQUEST_REFRESH: u8 = 5;
/// Client answers a digest mismatch with its own digest (CmdId 8); the core
/// sends no digests, so this only ever means "resend the snapshot".
pub const CMD_DIGEST: u8 = 8;

/// `OrderType` ordinal for `pos_dir`.
const POS_DIR_SELL: u8 = 0;
const POS_DIR_BUY: u8 = 1;

/// `TBalanceItem` field bits the core fills (Delphi order, 22 fields).
const F_POS_SIZE: u32 = 1 << 2;
const F_POS_PRICE: u32 = 1 << 3;
const F_POS_DIR: u32 = 1 << 5;
const F_ASSET_BALANCE: u32 = 1 << 14;
const F_ASSET_BALANCE_FULL: u32 = 1 << 15;
const F_LEVERAGE: u32 = 1 << 20;
const F_POSITION_TYPE: u32 = 1 << 21;
/// `PositionType` ordinals.
const MARGIN_CROSS: u8 = 0;
const MARGIN_ISOLATED: u8 = 1;

/// One market row of the full balance.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BalanceItem<'a> {
    pub market: &'a str,
    /// Free units of the asset and free + blocked.
    pub asset_balance: f64,
    pub asset_balance_full: f64,
    /// Leverage of the market, `0` when it is not stated: the flag is then left out and the
    /// terminal reads its default, which it shows as unknown.
    pub leverage: i32,
    /// A row that carries the leverage and nothing else: no position, no balance. The terminal
    /// takes a market with neither as flat, which is what it is.
    pub leverage_only: bool,
    /// Isolated margin on the market, cross otherwise. Sent with the leverage, which is when the
    /// terminal reads it (its screener's «200 / 200 Cross»); left out, it reads cross.
    pub isolated: bool,
    /// Open derivative position: signed size (long > 0) and entry price.
    pub pos_size: f64,
    pub pos_price: f64,
}

/// `TBalanceFull`: account totals in the core base currency plus market rows.
/// Rows carry no hash: the core never sends balance digests.
pub fn balance_full(
    uid: u64,
    epoch: u16,
    free: f64,
    locked: f64,
    full: f64,
    items: &[BalanceItem<'_>],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 38 + items.len() * 64);
    BaseHeader::write(&mut out, CMD_BALANCE_FULL, uid);
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&free.to_le_bytes()); // btc_balance_total
    out.extend_from_slice(&locked.to_le_bytes()); // btc_balance_locked
    out.extend_from_slice(&full.to_le_bytes()); // btc_balance_full
    out.extend_from_slice(&0f64.to_le_bytes()); // special_coin_balance
    out.extend_from_slice(&(items.len() as i32).to_le_bytes());
    for item in items {
        super::write_str(&mut out, item.market);
        out.extend_from_slice(&0u64.to_le_bytes()); // balance_hash
        let stated = if item.leverage > 0 {
            F_LEVERAGE | F_POSITION_TYPE
        } else {
            0
        };
        let held = if item.leverage_only {
            0
        } else {
            F_POS_SIZE | F_POS_PRICE | F_POS_DIR | F_ASSET_BALANCE | F_ASSET_BALANCE_FULL
        };
        out.extend_from_slice(&(held | stated).to_le_bytes());
        // Values follow the bit order of the flags (Delphi field order).
        if !item.leverage_only {
            out.extend_from_slice(&item.pos_size.abs().to_le_bytes());
            out.extend_from_slice(&item.pos_price.to_le_bytes());
            out.push(if item.pos_size < 0.0 {
                POS_DIR_SELL
            } else {
                POS_DIR_BUY
            });
            out.extend_from_slice(&item.asset_balance.to_le_bytes());
            out.extend_from_slice(&item.asset_balance_full.to_le_bytes());
        }
        if item.leverage > 0 {
            out.extend_from_slice(&item.leverage.to_le_bytes());
            out.push(if item.isolated {
                MARGIN_ISOLATED
            } else {
                MARGIN_CROSS
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::balance::parse_balance;
    use crate::commands::trade::OrderType;

    /// The margin type goes with the leverage, and reads back as the terminal reads it.
    #[test]
    fn the_margin_type_rides_with_the_leverage() {
        use crate::commands::market::PositionType;
        let rows = [
            BalanceItem {
                market: "BTCUSDT",
                leverage: 150,
                leverage_only: true,
                isolated: true,
                ..BalanceItem::default()
            },
            BalanceItem {
                market: "ETHUSDT",
                leverage: 20,
                leverage_only: true,
                ..BalanceItem::default()
            },
            BalanceItem {
                market: "SOLUSDT",
                pos_size: 1.0,
                pos_price: 150.0,
                leverage: 10,
                isolated: true,
                ..BalanceItem::default()
            },
        ];
        let raw = balance_full(1, 1, 0.0, 0.0, 0.0, &rows);
        let parsed =
            parse_balance(raw[0], &raw[super::super::BASE_HEADER_SIZE..]).expect("balance");
        let types: Vec<_> = parsed
            .items
            .iter()
            .map(|i| (i.market_name.as_str(), i.leverage_x, i.position_type))
            .collect();
        assert_eq!(
            types,
            [
                ("BTCUSDT", 150, PositionType::Isolated),
                ("ETHUSDT", 20, PositionType::Cross),
                ("SOLUSDT", 10, PositionType::Isolated),
            ]
        );
    }

    #[test]
    fn full_balance_rows_parse_upstream() {
        let rows = [
            BalanceItem {
                market: "SBER",
                asset_balance: 20.0,
                asset_balance_full: 30.0,
                ..BalanceItem::default()
            },
            BalanceItem {
                market: "SiZ5",
                pos_size: -2.0,
                pos_price: 84_000.0,
                ..BalanceItem::default()
            },
        ];
        let raw = balance_full(1, 7, 100.0, 5.0, 12_345.0, &rows);
        let parsed =
            parse_balance(raw[0], &raw[super::super::BASE_HEADER_SIZE..]).expect("balance");
        assert_eq!(parsed.epoch, 7);
        assert_eq!(
            (
                parsed.btc_balance_total,
                parsed.btc_balance_locked,
                parsed.btc_balance_full
            ),
            (100.0, 5.0, 12_345.0)
        );
        assert_eq!(parsed.items.len(), 2);
        let sber = &parsed.items[0];
        assert_eq!(sber.market_name, "SBER");
        assert_eq!((sber.asset_balance, sber.asset_balance_full), (20.0, 30.0));
        assert_eq!((sber.pos_size, sber.leverage_x), (0.0, 1));
        let si = &parsed.items[1];
        assert_eq!((si.pos_size, si.pos_price), (2.0, 84_000.0));
        assert_eq!(si.pos_dir, OrderType::Sell);
        assert_eq!(sber.pos_dir, OrderType::Buy);

        let empty = balance_full(1, 7, 0.0, 0.0, 0.0, &[]);
        assert!(parse_balance(3, &empty[super::super::BASE_HEADER_SIZE..])
            .unwrap()
            .items
            .is_empty());
    }

    #[test]
    fn leverage_rides_with_a_position_or_alone_and_is_left_out_when_unknown() {
        let rows = [
            BalanceItem {
                market: "BTCUSDT",
                pos_size: -0.5,
                pos_price: 60_000.0,
                leverage: 20,
                ..BalanceItem::default()
            },
            BalanceItem {
                market: "ETHUSDT",
                leverage: 7,
                leverage_only: true,
                ..BalanceItem::default()
            },
            BalanceItem {
                market: "SOLUSDT",
                pos_size: 3.0,
                pos_price: 150.0,
                ..BalanceItem::default()
            },
        ];
        let raw = balance_full(1, 1, 0.0, 0.0, 0.0, &rows);
        let parsed =
            parse_balance(raw[0], &raw[super::super::BASE_HEADER_SIZE..]).expect("balance");
        let by = |name: &str| {
            parsed
                .items
                .iter()
                .find(|i| i.market_name == name)
                .expect(name)
        };
        let btc = by("BTCUSDT");
        assert_eq!(
            (btc.pos_size, btc.pos_price, btc.leverage_x),
            (0.5, 60_000.0, 20)
        );
        assert_eq!(btc.pos_dir, OrderType::Sell);
        let eth = by("ETHUSDT");
        assert_eq!(
            (eth.leverage_x, eth.pos_size, eth.asset_balance),
            (7, 0.0, 0.0)
        );
        let sol = by("SOLUSDT");
        assert_eq!(
            (sol.pos_size, sol.leverage_x),
            (3.0, 1),
            "unknown stays the default"
        );
    }
}
