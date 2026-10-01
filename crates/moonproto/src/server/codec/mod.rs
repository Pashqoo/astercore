//! Server-side wire codecs: parse what the client builds, build what the
//! client parses. Each file mirrors one upstream `commands::*` module.

pub mod balance;
pub mod engine;
pub mod log;
pub mod market_data;
pub mod report;
pub mod strat;
pub mod trade;
pub mod ui;

pub const PROTO_CMD_VER: u16 = crate::commands::registry::CURRENT_PROTO_CMD_VER;

pub(crate) fn write_str(out: &mut Vec<u8>, s: &str) {
    crate::commands::registry::write_string(out, s);
}

pub(crate) fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    crate::commands::registry::read_string(data, pos)
}

/// `TBaseCommand` header shared by Engine/Strat/UI/Balance payloads:
/// `cmd_id:u8 + ver:u16 + uid:u64`.
pub struct BaseHeader {
    pub cmd_id: u8,
    pub ver: u16,
    pub uid: u64,
}

pub const BASE_HEADER_SIZE: usize = 11;

impl BaseHeader {
    pub fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < BASE_HEADER_SIZE {
            return None;
        }
        Some(Self {
            cmd_id: payload[0],
            ver: u16::from_le_bytes([payload[1], payload[2]]),
            uid: u64::from_le_bytes(payload[3..11].try_into().unwrap()),
        })
    }

    pub(crate) fn write(out: &mut Vec<u8>, cmd_id: u8, uid: u64) {
        out.push(cmd_id);
        out.extend_from_slice(&PROTO_CMD_VER.to_le_bytes());
        out.extend_from_slice(&uid.to_le_bytes());
    }
}
