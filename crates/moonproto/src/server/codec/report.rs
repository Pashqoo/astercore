//! Reports replication over `Command::Order` (cmd ids 32..50): schema, sync
//! pages, live row upserts/deletes, soft-delete echo and the alive map.
//! The field set belongs to the caller; this module only encodes it.
//! Mirrors `commands::report` / `state::report`.

use super::BaseHeader;
use crate::commands::report::{
    RepAliveMapRequest, RepCheckRowsRequest, RepSetRowsDeleted, RepSyncRequest,
};
use crate::compression::synlz_compress;

const CMD_ROW_UPSERT: u8 = 32;
const CMD_ROW_DELETE: u8 = 33;
pub const CMD_SYNC_REQUEST: u8 = 34;
pub const CMD_SCHEMA_REQUEST: u8 = 37;
const CMD_SCHEMA: u8 = 38;
const CMD_SYNC_PAGE: u8 = 39;
pub const CMD_CHECK_ROWS_REQUEST: u8 = 40;
pub const CMD_SET_ROWS_DELETED: u8 = 48;
pub const CMD_ALIVE_MAP_REQUEST: u8 = 49;
const CMD_ALIVE_MAP: u8 = 50;

const SCHEMA_FORMAT_VERSION: u8 = 1;
/// Longest text value the client accepts (`REPORT_TEXT_MAX_BYTES`).
const TEXT_MAX_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Integer = 1,
    Float = 2,
    Text = 3,
}

/// One report column: name, wire kind and the SQLite declaration the client
/// uses in `ALTER TABLE ADD COLUMN` (so no `PRIMARY KEY`).
#[derive(Debug, Clone, Copy)]
pub struct Field {
    pub name: &'static str,
    pub kind: FieldKind,
    pub sql: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
}

/// `TRepSchema` (CmdId 38): header + `len:u32` + synlz(ver, count u16,
/// {str8 name, kind u8, str8 sql}). Field index = position in `fields`.
pub fn schema_payload(uid: u64, fields: &[Field]) -> Vec<u8> {
    let mut plain = Vec::with_capacity(8 + fields.len() * 32);
    plain.push(SCHEMA_FORMAT_VERSION);
    plain.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for f in fields {
        str8(&mut plain, f.name);
        plain.push(f.kind as u8);
        str8(&mut plain, f.sql);
    }
    let blob = synlz_compress(&plain);
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 4 + blob.len());
    BaseHeader::write(&mut out, CMD_SCHEMA, uid);
    out.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    out.extend_from_slice(&blob);
    out
}

/// Encoded report row: `count u16` + `(index u16, kind u8, value)` per field.
/// Text is `u16 len + utf8`, cut to the client's limit on a char boundary.
pub fn encode_row(out: &mut Vec<u8>, values: &[(u16, Value)]) {
    out.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for (index, value) in values {
        out.extend_from_slice(&index.to_le_bytes());
        match value {
            Value::Int(v) => {
                out.push(FieldKind::Integer as u8);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(FieldKind::Float as u8);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Text(s) => {
                out.push(FieldKind::Text as u8);
                let mut end = s.len().min(TEXT_MAX_BYTES);
                while !s.is_char_boundary(end) {
                    end -= 1;
                }
                out.extend_from_slice(&(end as u16).to_le_bytes());
                out.extend_from_slice(&s.as_bytes()[..end]);
            }
        }
    }
}

/// `TRepSyncPage` (CmdId 39). `rows` is the concatenation of `row_count`
/// encoded rows, each carrying `newRecID`, in increasing rec_id order;
/// `last_rec_id` is the last row's id (0 when empty). `epoch` must be non-zero.
pub fn sync_page(
    request_uid: u64,
    epoch: i32,
    last_rec_id: i64,
    max_rec_id: i64,
    row_count: u16,
    rows: &[u8],
) -> Vec<u8> {
    let blob = if row_count == 0 {
        Vec::new()
    } else {
        synlz_compress(rows)
    };
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 34 + blob.len());
    BaseHeader::write(&mut out, CMD_SYNC_PAGE, request_uid);
    out.extend_from_slice(&request_uid.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&last_rec_id.to_le_bytes());
    out.extend_from_slice(&max_rec_id.to_le_bytes());
    out.extend_from_slice(&row_count.to_le_bytes());
    out.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    out.extend_from_slice(&blob);
    out
}

/// `TRepRowUpsert` (CmdId 32): `rec_id i64` + `len u32` + one uncompressed row.
pub fn row_upsert(uid: u64, rec_id: i64, row: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 12 + row.len());
    BaseHeader::write(&mut out, CMD_ROW_UPSERT, uid);
    out.extend_from_slice(&rec_id.to_le_bytes());
    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
    out.extend_from_slice(row);
    out
}

/// `TRepRowDelete` (CmdId 33): the row does not exist.
pub fn row_delete(uid: u64, rec_id: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 8);
    BaseHeader::write(&mut out, CMD_ROW_DELETE, uid);
    out.extend_from_slice(&rec_id.to_le_bytes());
    out
}

/// `TRepAliveMap` (CmdId 50): header + request_uid + epoch + covered_up_to +
/// `len:u32` + RleLZ(bitmap). Bit `rec_id - 1` (LSB first) = row alive;
/// `bitmap` must be exactly `ceil(up_to / 8)` bytes.
pub fn alive_map(request_uid: u64, epoch: i32, up_to_rec_id: i64, bitmap: &[u8]) -> Vec<u8> {
    let data = if bitmap.is_empty() {
        Vec::new()
    } else {
        rlelz_compress(bitmap)
    };
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 24 + data.len());
    BaseHeader::write(&mut out, CMD_ALIVE_MAP, request_uid);
    out.extend_from_slice(&request_uid.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&up_to_rec_id.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(&data);
    out
}

/// `(from_rec_id, depth_days)` of a `TRepSyncRequest`.
pub fn sync_request(payload: &[u8]) -> Option<(i64, u16)> {
    let r = RepSyncRequest::read(&mut &payload[..])?;
    Some((r.from_rec_id, r.depth_days))
}

/// rec ids of a `TRepCheckRowsRequest`.
pub fn check_rows(payload: &[u8]) -> Option<Vec<i64>> {
    Some(RepCheckRowsRequest::read(&mut &payload[..])?.rec_ids)
}

/// `up_to_rec_id` of an inbound `TRepAliveMapRequest`.
pub fn alive_map_request_up_to(payload: &[u8]) -> Option<i64> {
    Some(RepAliveMapRequest::read(&mut &payload[..])?.up_to_rec_id)
}

/// A `TRepSetRowsDeleted` selection: inclusive ranges (reversed = empty, as
/// SQL `BETWEEN`) and single ids. The echo to clients is the same payload.
#[derive(Debug, Clone, PartialEq)]
pub struct RowsDeleted {
    pub deleted: bool,
    pub ranges: Vec<(i64, i64)>,
    pub singles: Vec<i64>,
}

impl RowsDeleted {
    pub fn contains(&self, rec_id: i64) -> bool {
        self.ranges.iter().any(|&(a, b)| a <= rec_id && rec_id <= b)
            || self.singles.contains(&rec_id)
    }
}

pub fn set_rows_deleted(payload: &[u8]) -> Option<RowsDeleted> {
    let r = RepSetRowsDeleted::read(&mut &payload[..])?;
    Some(RowsDeleted {
        deleted: r.deleted,
        ranges: r.ranges,
        singles: r.singles,
    })
}

/// mORMot `TAlgoRleLZ` container without the RLE pass: `varuint(len) + 0 + SynLZ`.
fn rlelz_compress(plain: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + plain.len());
    let mut len = plain.len() as u32;
    while len >= 0x80 {
        out.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    out.push(len as u8);
    out.push(0);
    out.extend_from_slice(&synlz_compress(plain));
    out
}

fn str8(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::report::{
        build_alive_map_request, build_check_rows_request, build_set_rows_deleted,
        build_sync_request,
    };
    use crate::commands::trade::TradeCommand;
    use crate::compression::{rlelz_decompress_exact, synlz_decompress};
    use crate::state::{
        ReportAliveMapRequest, ReportEvent, ReportRecIdRange, ReportReplicationState,
        ReportRowsDeleted, ReportSchema,
    };
    use std::sync::Arc;

    const FIELDS: [Field; 4] = [
        Field {
            name: "newRecID",
            kind: FieldKind::Integer,
            sql: "INTEGER",
        },
        Field {
            name: "Coin",
            kind: FieldKind::Text,
            sql: "TEXT",
        },
        Field {
            name: "ProfitBTC",
            kind: FieldKind::Float,
            sql: "REAL",
        },
        Field {
            name: "deleted",
            kind: FieldKind::Integer,
            sql: "INT default 0",
        },
    ];

    fn row(rec_id: i64, coin: &str, profit: f64) -> Vec<u8> {
        let mut out = Vec::new();
        encode_row(
            &mut out,
            &[
                (0, Value::Int(rec_id)),
                (1, Value::Text(coin.into())),
                (2, Value::Float(profit)),
                (3, Value::Int(0)),
            ],
        );
        out
    }

    /// Upstream replication state after applying our schema.
    fn with_schema() -> (ReportReplicationState, Arc<ReportSchema>) {
        let mut state = ReportReplicationState::default();
        let (mut out, mut controls) = (Vec::new(), Vec::new());
        let Some(TradeCommand::ReportSchema(wire)) =
            TradeCommand::parse(&schema_payload(3, &FIELDS))
        else {
            panic!("schema");
        };
        assert!(state.apply_schema(wire, &mut out, &mut controls));
        match out.pop() {
            Some(ReportEvent::Schema(s)) => (state, s),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn schema_parses_upstream() {
        let (_, s) = with_schema();
        let coin = s.field_by_name("Coin").expect("Coin");
        assert_eq!((coin.index, coin.sql_spec.as_ref()), (1, "TEXT"));
        assert_eq!(s.field_by_name("deleted").unwrap().index, 3);
    }

    #[test]
    fn sync_page_rows_parse_upstream() {
        let mut rows = row(1, "SBER", 12.5);
        rows.extend(row(2, "ГАЗП", -3.0));
        match TradeCommand::parse(&sync_page(9, 7, 2, 5, 2, &rows)) {
            Some(TradeCommand::ReportSyncPage(p)) => {
                assert_eq!(
                    (
                        p.request_uid,
                        p.epoch,
                        p.last_rec_id,
                        p.max_rec_id,
                        p.row_count
                    ),
                    (9, 7, 2, 5, 2)
                );
                assert_eq!(synlz_decompress(&p.blob).unwrap(), rows);
            }
            other => panic!("unexpected {other:?}"),
        }
        match TradeCommand::parse(&sync_page(9, 7, 0, 0, 0, &[])) {
            Some(TradeCommand::ReportSyncPage(p)) => assert!(p.blob.is_empty()),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn live_rows_parse_upstream_through_the_schema() {
        let (mut state, s) = with_schema();
        let (mut out, mut controls) = (Vec::new(), Vec::new());
        let Some(TradeCommand::ReportRowUpsert(u)) =
            TradeCommand::parse(&row_upsert(0, 4, &row(4, "ГАЗП", 1.25)))
        else {
            panic!("upsert");
        };
        assert!(state.apply_live_upsert(u.rec_id, &u.row, &mut out, &mut controls));
        match out.as_slice() {
            [ReportEvent::RowUpsert(r)] => {
                assert_eq!(r.rec_id, 4);
                assert_eq!(r.text_by_name(&s, "Coin"), Some("ГАЗП"));
                assert_eq!(r.float_by_name(&s, "ProfitBTC"), Some(1.25));
            }
            other => panic!("unexpected {other:?}"),
        }
        match TradeCommand::parse(&row_delete(0, 6)) {
            Some(TradeCommand::ReportRowDelete(d)) => assert_eq!(d.rec_id, 6),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn long_text_is_cut_on_a_char_boundary() {
        let mut out = Vec::new();
        encode_row(&mut out, &[(1, Value::Text("ж".repeat(5000)))]);
        let len = u16::from_le_bytes([out[5], out[6]]) as usize;
        assert_eq!(len, 8192);
        assert!(std::str::from_utf8(&out[7..7 + len]).is_ok());
    }

    #[test]
    fn requests_parse_from_upstream_builders() {
        assert_eq!(sync_request(&build_sync_request(5, 11, 30)), Some((11, 30)));
        assert_eq!(
            check_rows(&build_check_rows_request(5, &[3, 9])),
            Some(vec![3, 9])
        );
        let change = ReportRowsDeleted::new(true, [ReportRecIdRange::new(2, 4)], [7]);
        let parsed = set_rows_deleted(&build_set_rows_deleted(5, &change)).unwrap();
        assert!(parsed.deleted);
        assert!(parsed.contains(3) && parsed.contains(7) && !parsed.contains(5));
        let req = build_alive_map_request(
            4,
            ReportAliveMapRequest {
                epoch: 1,
                up_to_rec_id: 1000,
            },
        );
        assert_eq!(alive_map_request_up_to(&req), Some(1000));
    }

    #[test]
    fn alive_map_round_trips_bitmap() {
        let bitmap = [0b0000_0101u8, 0b1000_0000];
        match TradeCommand::parse(&alive_map(4, 1, 16, &bitmap)) {
            Some(TradeCommand::ReportAliveMap(m)) => {
                assert_eq!((m.request_uid, m.epoch, m.covered_up_to), (4, 1, 16));
                assert_eq!(rlelz_decompress_exact(&m.data, 2).unwrap(), bitmap);
            }
            other => panic!("unexpected {other:?}"),
        }
        match TradeCommand::parse(&alive_map(5, 1, 0, &[])) {
            Some(TradeCommand::ReportAliveMap(m)) => assert!(m.data.is_empty()),
            other => panic!("unexpected {other:?}"),
        }
    }
}
