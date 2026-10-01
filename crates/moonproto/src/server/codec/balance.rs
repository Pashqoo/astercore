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

/// One market row of the full balance.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BalanceItem<'a> {
    pub market: &'a str,
    /// Free units of the asset and free + blocked.
    pub asset_balance: f64,
    pub asset_balance_full: f64,
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
        let flags = F_POS_SIZE | F_POS_PRICE | F_POS_DIR | F_ASSET_BALANCE | F_ASSET_BALANCE_FULL;
        out.extend_from_slice(&flags.to_le_bytes());
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
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::balance::parse_balance;
    use crate::commands::trade::OrderType;

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
}
