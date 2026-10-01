//! `Command::UI` server side: ClientSettings / SharedConfig echo and runtime
//! state. Mirrors `commands::ui` builders/parser.

use super::strat::{self, CheckedItem};
use super::BaseHeader;
use crate::commands::ui::{AutoStartConfig, AutoStartConfig2, ClientSettingsCommand, UICommand};
use crate::shared_config::{gzip_compress, serialize_payload, SharedConfig};

pub const CMD_CLIENT_SETTINGS: u8 = 1;
pub const CMD_SETTINGS_REQUEST: u8 = 2;
pub const CMD_STRAT_START_STOP: u8 = 3;
pub const CMD_STRAT_START_STOP_V2: u8 = 4;
const CMD_NEW_MARKET_NOTIFY: u8 = 8;
pub const CMD_RUNTIME_STATE: u8 = 20;
const CMD_KERNEL_LICENSE_STATE: u8 = 22;
pub const CMD_KERNEL_LICENSE_STATE_REQUEST: u8 = 23;
const CMD_PROFIT_STATE: u8 = 24;
pub const CMD_SHARED_CONFIG: u8 = 28;
pub const CMD_SHARED_CONFIG_REQUEST: u8 = 29;
const CMD_TELEGRAM_STATE: u8 = 36;
/// The terminal's Telegram controls: refresh (37) through logout (48). Each is
/// fire-and-forget; the core answers every one with its full state (36).
pub const CMD_TELEGRAM_REFRESH: u8 = 37;
pub const CMD_TELEGRAM_LOGOUT: u8 = 48;

/// `TClientSettings` with every field at its default.
pub fn default_client_settings(uid: u64) -> Vec<u8> {
    let cmd = ClientSettingsCommand {
        uid,
        sign_orders: true,
        ..ClientSettingsCommand::default()
    };
    crate::commands::ui::build_client_settings(&cmd)
}

/// What the terminal's toolbar asks for manual orders that carry no
/// strategy: the take profit it shows and the price-drop stop (MoonBot applies
/// both from its `cfg` when the order names neither).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ManualDefaults {
    /// Effective take profit, % (0 = none).
    pub take_profit_pct: f64,
    /// Stop distance below the entry, % (0 = off): `price_drop_level` is
    /// negative on the wire and only counts with `panic_if_price_drop`.
    pub stop_pct: f64,
    /// Core-wide emulator mode (`emu_mode`): manual orders, and strategies
    /// without `EmulatorMode`, trade in the emulator.
    pub emulator: bool,
    /// «Use the manual strategy» (`use_manual_strategy`): its id.
    pub manual_strategy: Option<u64>,
    /// The toolbar's trailing stop for manual orders (`trailing_stop`), %
    /// back from the best price (0 = off) …
    pub trailing_pct: f64,
    /// … started once the price is this % past the entry (`use_g_take_profit`,
    /// 0 = at once).
    pub trailing_from_pct: f64,
}

/// Manual-order defaults of an inbound `TClientSettings` payload.
pub fn manual_defaults(payload: &[u8]) -> Option<ManualDefaults> {
    let UICommand::ClientSettings(c) = UICommand::parse(payload)? else {
        return None;
    };
    Some(ManualDefaults {
        take_profit_pct: c.effective_take_profit_percent().max(0.0),
        stop_pct: if c.panic_if_price_drop {
            f64::from(c.price_drop_level).abs()
        } else {
            0.0
        },
        emulator: c.emu_mode,
        manual_strategy: (c.use_manual_strategy && c.manual_strategy_id != 0)
            .then_some(c.manual_strategy_id),
        trailing_pct: if c.trailing_stop && c.trailing_drop.is_finite() {
            f64::from(c.trailing_drop).abs()
        } else {
            0.0
        },
        trailing_from_pct: if c.use_g_take_profit && c.g_take_profit.is_finite() {
            c.g_take_profit.abs()
        } else {
            0.0
        },
    })
}

/// Auto-start rules (`as_cfg`, `as_cfg2`) of an inbound `TClientSettings`
/// payload.
pub fn auto_start(payload: &[u8]) -> Option<(AutoStartConfig, AutoStartConfig2)> {
    let UICommand::ClientSettings(c) = UICommand::parse(payload)? else {
        return None;
    };
    Some((c.auto_start_config(), c.auto_start_config2()))
}

/// Global coin black list of an inbound `TClientSettings` payload: the
/// permanent list (empty while its checkbox is off) and the temporary one as
/// `(symbol, remaining days)`.
pub fn black_list(payload: &[u8]) -> Option<(Vec<String>, Vec<(String, f64)>)> {
    let UICommand::ClientSettings(c) = UICommand::parse(payload)? else {
        return None;
    };
    let permanent = if c.use_coins_black_list {
        c.coins_black_list_text
            .split([',', ';', ' ', '\n', '\r', '\t'])
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };
    let temporary = c
        .temp_blacklist_entries()
        .map(|e| (e.symbol.trim().to_uppercase(), e.remaining_days()))
        .collect();
    Some((permanent, temporary))
}

/// The same `TClientSettings` payload with its temporary black list replaced
/// by `rows` (symbol, remaining time): an echo then carries what is left now,
/// not what was left when the terminal sent it.
pub fn with_temp_black_list(
    payload: &[u8],
    rows: Vec<(String, std::time::Duration)>,
) -> Option<Vec<u8>> {
    let UICommand::ClientSettings(mut c) = UICommand::parse(payload)? else {
        return None;
    };
    c.set_temp_blacklist_entries(rows);
    Some(crate::commands::ui::build_client_settings(&c))
}

/// Same command, new uid: the client does not compare uids, but each send is a
/// distinct wire command.
pub fn with_uid(payload: &[u8], uid: u64) -> Vec<u8> {
    let mut out = payload.to_vec();
    if out.len() >= super::BASE_HEADER_SIZE {
        out[3..11].copy_from_slice(&uid.to_le_bytes());
    }
    out
}

/// gzip(MBSP) of `SharedConfig::default()`.
pub fn default_shared_config_blob() -> Vec<u8> {
    let plain = serialize_payload(&SharedConfig::default()).expect("default shared config");
    gzip_compress(&plain).expect("gzip default shared config")
}

/// `TSharedConfig` (CmdId 28): header + `len:u32` + gzip blob.
pub fn shared_config_payload(uid: u64, blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 4 + blob.len());
    BaseHeader::write(&mut out, CMD_SHARED_CONFIG, uid);
    out.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    out.extend_from_slice(blob);
    out
}

/// Blob carried by an inbound `TSharedConfig` (after the header).
pub fn shared_config_blob(payload: &[u8]) -> Option<&[u8]> {
    let body = payload.get(super::BASE_HEADER_SIZE..)?;
    let len = u32::from_le_bytes(body.get(..4)?.try_into().unwrap()) as usize;
    body.get(4..4 + len)
}

/// `TStratStartStopCommand` (3) / `V2` (4) body: `(is_start, checked items)`;
/// V1 carries no items.
pub fn parse_strat_start_stop(cmd_id: u8, body: &[u8]) -> Option<(bool, Vec<CheckedItem>)> {
    let is_start = *body.first()? != 0;
    let items = match cmd_id {
        CMD_STRAT_START_STOP_V2 => strat::parse_checked_items(body, &mut 1)?,
        _ => Vec::new(),
    };
    Some((is_start, items))
}

/// `TNewMarketNotifyCommand` (CmdId 8, header only): the terminal re-reads
/// `GetMarketsList` at once, past its 30 s listing-refresh throttle, and copies
/// each known market's fields from it (the 24 h `volume` among them).
pub fn new_market_notify(uid: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE);
    BaseHeader::write(&mut out, CMD_NEW_MARKET_NOTIFY, uid);
    out
}

/// `TRuntimeStateCommand` (CmdId 20).
pub fn runtime_state(uid: u64, is_started: bool, auto_detect_active: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 2);
    BaseHeader::write(&mut out, CMD_RUNTIME_STATE, uid);
    out.push(u8::from(is_started));
    out.push(u8::from(auto_detect_active));
    out
}

/// `TProfitStateCommand` (CmdId 24): report profit counters shown beside the
/// AutoStart loss caps — the total and the last hour's profit and trade count.
pub fn profit_state(
    uid: u64,
    total: f64,
    trades: i32,
    hour_total: f64,
    hour_trades: i32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 24);
    BaseHeader::write(&mut out, CMD_PROFIT_STATE, uid);
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&trades.to_le_bytes());
    out.extend_from_slice(&hour_total.to_le_bytes());
    out.extend_from_slice(&hour_trades.to_le_bytes());
    out
}

/// `TKernelLicenseState` (CmdId 22): paid core, no MoonBot add-on features.
/// Layout: paid u8, reg_id i32, order_count i32, 7 feature bools, news_valid_until f64,
/// news_trial_used u8, arb_active u8, arb_valid_until f64, 3 credit i32, can_use_watcher u8.
pub fn kernel_license_state(uid: u64, paid: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 48);
    BaseHeader::write(&mut out, CMD_KERNEL_LICENSE_STATE, uid);
    out.push(u8::from(paid));
    out.extend_from_slice(&[0u8; 4 + 4 + 7 + 8 + 1 + 1 + 8 + 12 + 1]);
    out
}

/// `TTelegramStateCommand` (CmdId 36): `len u32 + JSON object`, the core's
/// Telegram service state as `crate::state::TelegramState` reads it. The
/// client accepts only an object, and a missing optional field means
/// unavailable, not unchanged.
pub fn telegram_state(uid: u64, json: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 4 + json.len());
    BaseHeader::write(&mut out, CMD_TELEGRAM_STATE, uid);
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend_from_slice(json.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::ui::{build_shared_config_blob, UICommand};

    #[test]
    fn telegram_state_parses() {
        let json = r#"{"enabled":false,"service_online":false,"state_supported":false,"setup_error":"none here"}"#;
        match UICommand::parse(&telegram_state(3, json)) {
            Some(UICommand::TelegramState(s)) => {
                assert!(!s.enabled && !s.service_online && !s.state_supported);
                assert_eq!(s.setup_error.as_deref(), Some("none here"));
                assert!(s.service.is_none());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn profit_state_parses() {
        match UICommand::parse(&profit_state(2, 12.5, 3, -1.25, 1)) {
            Some(UICommand::ProfitState(p)) => {
                assert_eq!(
                    (
                        p.rep_total_profit,
                        p.rep_total_trades,
                        p.rep_trades_total,
                        p.rep_count_trades
                    ),
                    (12.5, 3, -1.25, 1)
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn new_market_notify_parses() {
        assert!(matches!(
            UICommand::parse(&new_market_notify(7)),
            Some(UICommand::NewMarketNotify(_))
        ));
    }

    #[test]
    fn kernel_license_state_parses() {
        match UICommand::parse(&kernel_license_state(2, true)) {
            Some(UICommand::KernelLicenseState(k)) => {
                assert!(k.paid_version && !k.can_use_watcher && k.moon_credits == 0);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    use crate::shared_config::{gzip_decompress, parse_payload};

    #[test]
    fn client_settings_default_parses_and_uid_rewrites() {
        let raw = with_uid(&default_client_settings(1), 42);
        match UICommand::parse(&raw) {
            Some(UICommand::ClientSettings(s)) => {
                assert_eq!(s.uid, 42);
                assert!(s.sign_orders);
                assert!(s.temp_bl_symbols.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn black_list_reads_the_checked_list_and_the_temporary_rows() {
        use crate::commands::ui::build_client_settings;
        use std::time::Duration;
        let mut cmd = ClientSettingsCommand::default();
        cmd.coins_black_list_text = "sber, GAZP;;VTBR ".into();
        cmd.set_temp_blacklist_entries([("aflt", Duration::from_secs(43_200))]);
        // The list counts only while its checkbox is on; temporary rows always.
        let (perm, temp) = black_list(&build_client_settings(&cmd)).unwrap();
        assert!(perm.is_empty());
        assert_eq!(temp, [("AFLT".to_string(), 0.5)]);
        cmd.use_coins_black_list = true;
        let (perm, _) = black_list(&build_client_settings(&cmd)).unwrap();
        assert_eq!(perm, ["SBER", "GAZP", "VTBR"]);
    }

    #[test]
    fn temp_black_list_is_rewritten_with_what_is_left() {
        use crate::commands::ui::build_client_settings;
        use std::time::Duration;
        let mut cmd = ClientSettingsCommand::default();
        cmd.coins_black_list_text = "SBER".into();
        cmd.set_temp_blacklist_entries([("AFLT", Duration::from_secs(86_400))]);
        let payload = build_client_settings(&cmd);
        let out =
            with_temp_black_list(&payload, vec![("AFLT".into(), Duration::from_secs(21_600))])
                .unwrap();
        let (_, temp) = black_list(&out).unwrap();
        assert_eq!(temp, [("AFLT".to_string(), 0.25)]);
        // Everything else survives the rewrite.
        let UICommand::ClientSettings(back) = UICommand::parse(&out).unwrap() else {
            panic!("not settings");
        };
        assert_eq!(back.coins_black_list_text, "SBER");
    }

    #[test]
    fn manual_defaults_read_take_profit_and_armed_stop() {
        use crate::commands::ui::build_client_settings;
        assert_eq!(
            manual_defaults(&default_client_settings(1)),
            Some(ManualDefaults::default())
        );
        let mut cmd = ClientSettingsCommand::default();
        cmd.set_main_take_profit_percent(2.0);
        cmd.price_drop_level = -1.5;
        // The level alone is not a stop: the toolbar's SL toggle is `panic_if_price_drop`.
        let off = manual_defaults(&build_client_settings(&cmd)).unwrap();
        assert_eq!((off.take_profit_pct, off.stop_pct), (2.0, 0.0));
        cmd.panic_if_price_drop = true;
        let on = manual_defaults(&build_client_settings(&cmd)).unwrap();
        assert_eq!((on.take_profit_pct, on.stop_pct), (2.0, 1.5));
        assert!(!on.emulator);
        cmd.emu_mode = true;
        assert!(
            manual_defaults(&build_client_settings(&cmd))
                .unwrap()
                .emulator
        );
    }

    #[test]
    fn shared_config_default_round_trips_and_inbound_blob_extracts() {
        let blob = default_shared_config_blob();
        match UICommand::parse(&shared_config_payload(3, &blob)) {
            Some(UICommand::SharedConfig(c)) => {
                assert_eq!(c.data, blob);
                parse_payload(&gzip_decompress(&c.data).unwrap()).expect("mbsp");
            }
            other => panic!("unexpected {other:?}"),
        }
        let inbound = build_shared_config_blob(9, &blob);
        assert_eq!(shared_config_blob(&inbound), Some(blob.as_slice()));
    }

    #[test]
    fn strat_start_stop_parses_upstream_builders() {
        use crate::commands::strat::StratCheckedItem;
        use crate::commands::ui::{build_strat_start_stop, build_strat_start_stop_v2};
        let items = [StratCheckedItem {
            strategy_id: 7,
            checked: true,
        }];
        let v2 = build_strat_start_stop_v2(1, true, &items);
        let (start, parsed) =
            parse_strat_start_stop(v2[0], &v2[super::super::BASE_HEADER_SIZE..]).unwrap();
        assert!(start);
        assert_eq!(
            parsed,
            vec![CheckedItem {
                strategy_id: 7,
                checked: true
            }]
        );
        let v1 = build_strat_start_stop(1, false);
        let (start, parsed) =
            parse_strat_start_stop(v1[0], &v1[super::super::BASE_HEADER_SIZE..]).unwrap();
        assert!(!start && parsed.is_empty());
    }

    #[test]
    fn runtime_state_parses() {
        match UICommand::parse(&runtime_state(1, true, false)) {
            Some(UICommand::RuntimeState(r)) => assert!(r.is_started && !r.auto_detect_active),
            other => panic!("unexpected {other:?}"),
        }
    }
}
