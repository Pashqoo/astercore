//! `Command::Strat` server side. Mirrors `commands::strat`,
//! `commands::strategy_schema` and `commands::strategy_serializer`: parses the
//! terminal's snapshot / delete / sell-price / checked-sync commands, builds the
//! schema (8), snapshots (2), delete echo (3), checked echo (6) and runtime
//! state (10), and wraps the upstream `TStrategySerializer` for the app.

use std::io::Write;
use std::sync::Arc;

use flate2::write::DeflateEncoder;
use flate2::Compression;

use super::{read_str, write_str, BaseHeader, BASE_HEADER_SIZE};
use crate::commands::strategy_schema::{StrategySchema, SCHEMA_FORMAT_VERSION};
use crate::commands::strategy_serializer::{
    parse_strategy_batch_with_schema, FieldValue, StrategyBatchBuilder, StrategySnapshot,
};

pub const CMD_SNAPSHOT_REQUEST: u8 = 1;
pub const CMD_SNAPSHOT: u8 = 2;
pub const CMD_DELETE: u8 = 3;
pub const CMD_SELL_PRICE_UPDATE: u8 = 4;
pub const CMD_CHECKED_SYNC: u8 = 5;
const CMD_CHECKED_ECHO: u8 = 6;
pub const CMD_SCHEMA_REQUEST: u8 = 7;
const CMD_SCHEMA: u8 = 8;
const CMD_DETECT_SIGNAL: u8 = 9;
const CMD_RUNTIME_STATE: u8 = 10;

/// UI widget bits of a schema field's flags byte.
pub mod ui {
    pub const EDIT: u8 = 0;
    pub const CHECKBOX: u8 = 1;
    pub const COMBO: u8 = 2;
}

const LA_COMMENT: u8 = 1;
const FLAG_HAS_STATIC: u8 = 0x10;
const FLAG_DEFAULT_NZ: u8 = 0x40;

// ----- schema ----------------------------------------------------------------

/// Schema field. `default` carries both the wire type and the default value
/// (a zero default is written as "none").
pub struct SchemaField {
    pub name: &'static str,
    pub default: FieldValue,
    pub ui: u8,
    /// Starts a new editor section titled so (`Comment` layout marker).
    pub section: Option<&'static str>,
    /// Pipe-separated choices of a `COMBO` field (`FLAG_HAS_STATIC`).
    pub picklist: Option<&'static str>,
    /// Kind ordinals the field is visible for; empty = every exported kind.
    pub kinds: &'static [u8],
}

/// Raw-deflate `TStratSchema.Data` (`StrategySchemaBuilder.pas:BuildStrategySchemaBlob`).
pub fn build_schema_blob(kinds: &[(u8, &str)], fields: &[SchemaField]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(64 + fields.len() * 24);
    raw.push(SCHEMA_FORMAT_VERSION);
    raw.push(kinds.len() as u8);
    for (ordinal, name) in kinds {
        raw.push(*ordinal);
        str8(&mut raw, name);
    }
    raw.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    let vis_bytes = kinds.len().div_ceil(8);
    for f in fields {
        str8(&mut raw, f.name);
        raw.push(f.default.type_id_inner());
        let mut flags = f.ui;
        if f.section.is_some() {
            flags |= LA_COMMENT << 2;
        }
        if !f.default.is_zero() {
            flags |= FLAG_DEFAULT_NZ;
        }
        if f.picklist.is_some() {
            flags |= FLAG_HAS_STATIC;
        }
        raw.push(flags);
        if let Some(title) = f.section {
            str8(&mut raw, title);
        }
        if !f.default.is_zero() {
            write_raw_value(&mut raw, &f.default);
        }
        // Bit `i` = visible for `kinds[i]`.
        let mut vis = vec![0u8; vis_bytes];
        for (i, (ordinal, _)) in kinds.iter().enumerate() {
            if f.kinds.is_empty() || f.kinds.contains(ordinal) {
                vis[i / 8] |= 1 << (i % 8);
            }
        }
        raw.extend_from_slice(&vis);
        if let Some(list) = f.picklist {
            write_str(&mut raw, list);
        }
    }
    deflate(&raw)
}

/// Decoded view of a blob built by `build_schema_blob` (the serializer needs it).
pub fn parse_schema(blob: &[u8]) -> Option<StrategySchema> {
    StrategySchema::parse_compressed(blob)
}

/// `TStratSchema` payload: header + `size:u32` + deflate blob.
pub fn schema_payload(uid: u64, blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 4 + blob.len());
    BaseHeader::write(&mut out, CMD_SCHEMA, uid);
    out.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    out.extend_from_slice(blob);
    out
}

/// Schema default value layout (`strategy_schema::read_raw_value_by_type_id`).
fn write_raw_value(out: &mut Vec<u8>, v: &FieldValue) {
    match v {
        FieldValue::Bool(b) => out.push(u8::from(*b)),
        FieldValue::Byte(b) => out.push(*b),
        FieldValue::Word(w) => out.extend_from_slice(&w.to_le_bytes()),
        FieldValue::Int32(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt32(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Int64(i) => out.extend_from_slice(&i.to_le_bytes()),
        FieldValue::UInt64(u) => out.extend_from_slice(&u.to_le_bytes()),
        FieldValue::Single(f) => out.extend_from_slice(&f.to_le_bytes()),
        FieldValue::Double(d) => out.extend_from_slice(&d.to_le_bytes()),
        FieldValue::String(s) => write_str(out, s),
    }
}

fn str8(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

fn deflate(raw: &[u8]) -> Vec<u8> {
    let mut enc = DeflateEncoder::new(Vec::with_capacity(raw.len()), Compression::default());
    enc.write_all(raw).expect("Vec write");
    enc.finish().expect("Vec finish")
}

// ----- TStrategySerializer batches ------------------------------------------

/// Strategies and the folder dictionary of a raw-deflate serializer payload.
/// Fields are validated against `schema` the way the core's RTTI reader does.
pub fn decode_batch(
    data: &[u8],
    schema: &StrategySchema,
) -> Option<(Vec<StrategySnapshot>, Vec<Arc<str>>)> {
    let batch = parse_strategy_batch_with_schema(data, Some(schema))?;
    Some((batch.strategies, batch.paths))
}

/// Raw-deflate serializer payload: `folders` are declared in the path
/// dictionary even when no strategy lives there (empty folders survive Full).
pub fn encode_batch<'a>(
    schema: &StrategySchema,
    strategies: &[StrategySnapshot],
    folders: impl IntoIterator<Item = &'a str>,
) -> Vec<u8> {
    let mut builder = StrategyBatchBuilder::new(schema);
    for path in folders {
        builder.path_index(path);
    }
    for s in strategies {
        builder.write_strategy(s);
    }
    builder.finalize()
}

// ----- commands from the terminal --------------------------------------------

/// `TStratSnapshot` (2) body.
pub struct Snapshot {
    pub server_epoch: u64,
    pub client_max_last_date: u64,
    pub full: bool,
    pub data: Vec<u8>,
    /// Only in Full snapshots; 0 when absent.
    pub folders_last_modified: i64,
}

/// `(strategy_id, checked)` item of checked-sync / start-stop commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedItem {
    pub strategy_id: u64,
    pub checked: bool,
}

/// Body after `BaseHeader`.
pub fn parse_snapshot(body: &[u8]) -> Option<Snapshot> {
    let mut pos = 0;
    let server_epoch = read_u64(body, &mut pos)?;
    let client_max_last_date = read_u64(body, &mut pos)?;
    let size = u32::from_le_bytes(body.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let full = *body.get(pos)? != 0;
    pos += 1;
    let data = body.get(pos..pos + size)?.to_vec();
    pos += size;
    let folders_last_modified = match body.get(pos..pos + 8) {
        Some(tail) if full => i64::from_le_bytes(tail.try_into().ok()?),
        _ => 0,
    };
    Some(Snapshot {
        server_epoch,
        client_max_last_date,
        full,
        data,
        folders_last_modified,
    })
}

/// `TStratDelete` (3) body: `(strategy_id, folder_path)`.
pub fn parse_delete(body: &[u8]) -> Option<(u64, String)> {
    let mut pos = 0;
    let id = read_u64(body, &mut pos)?;
    let path = if pos < body.len() {
        read_str(body, &mut pos)?
    } else {
        String::new()
    };
    Some((id, path))
}

/// `TStratSellPriceUpdate` (4) body: `(strategy_id, sell_price)`.
pub fn parse_sell_price(body: &[u8]) -> Option<(u64, f64)> {
    let mut pos = 0;
    let id = read_u64(body, &mut pos)?;
    let price = f64::from_le_bytes(body.get(pos..pos + 8)?.try_into().ok()?);
    Some((id, price))
}

/// `TStratCheckedSync` (5) body: items (the trailing `is_delta` is ignored —
/// the core applies every listed item either way).
pub fn parse_checked_sync(body: &[u8]) -> Option<Vec<CheckedItem>> {
    let mut pos = 0;
    parse_checked_items(body, &mut pos)
}

/// `cnt:u16 + (id:u64, checked:u8)[]`, shared with UI start/stop V2.
pub(crate) fn parse_checked_items(body: &[u8], pos: &mut usize) -> Option<Vec<CheckedItem>> {
    let count = u16::from_le_bytes(body.get(*pos..*pos + 2)?.try_into().ok()?) as usize;
    *pos += 2;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let strategy_id = read_u64(body, pos)?;
        let checked = *body.get(*pos)? != 0;
        *pos += 1;
        items.push(CheckedItem {
            strategy_id,
            checked,
        });
    }
    Some(items)
}

fn read_u64(d: &[u8], pos: &mut usize) -> Option<u64> {
    let v = u64::from_le_bytes(d.get(*pos..*pos + 8)?.try_into().ok()?);
    *pos += 8;
    Some(v)
}

// ----- commands to the terminal ----------------------------------------------

/// `TStratSnapshot` (2): `data` from `encode_batch`; Full carries the folder
/// tree date.
pub fn snapshot(uid: u64, snap: &Snapshot) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 29 + snap.data.len());
    BaseHeader::write(&mut out, CMD_SNAPSHOT, uid);
    out.extend_from_slice(&snap.server_epoch.to_le_bytes());
    out.extend_from_slice(&snap.client_max_last_date.to_le_bytes());
    out.extend_from_slice(&(snap.data.len() as u32).to_le_bytes());
    out.push(u8::from(snap.full));
    out.extend_from_slice(&snap.data);
    if snap.full {
        out.extend_from_slice(&snap.folders_last_modified.to_le_bytes());
    }
    out
}

/// `TStratDelete` (3) echo.
pub fn delete(uid: u64, strategy_id: u64, folder_path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 10 + folder_path.len());
    BaseHeader::write(&mut out, CMD_DELETE, uid);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    write_str(&mut out, folder_path);
    out
}

/// `TStratCheckedSync` (5) forwarded to the other clients.
pub fn checked_sync(uid: u64, items: &[CheckedItem]) -> Vec<u8> {
    let mut out = checked_list(CMD_CHECKED_SYNC, uid, items);
    out.push(1);
    out
}

/// `TStratCheckedEcho` (6): acknowledges the sender's delta.
pub fn checked_echo(uid: u64, items: &[CheckedItem]) -> Vec<u8> {
    checked_list(CMD_CHECKED_ECHO, uid, items)
}

fn checked_list(cmd: u8, uid: u64, items: &[CheckedItem]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 3 + items.len() * 9);
    BaseHeader::write(&mut out, cmd, uid);
    out.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for it in items {
        out.extend_from_slice(&it.strategy_id.to_le_bytes());
        out.push(u8::from(it.checked));
    }
    out
}

/// `TDetectSignalCommand` (9), regular detect (kind 0): a row in the
/// terminal's Detects panel and a chart mark; sound / keep-in-chart come from
/// the strategy's own fields on the client.
pub fn detect_signal(
    uid: u64,
    market: &str,
    strategy_id: u64,
    is_short: bool,
    msg: &str,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 15 + market.len() + msg.len());
    BaseHeader::write(&mut out, CMD_DETECT_SIGNAL, uid);
    write_str(&mut out, market);
    out.extend_from_slice(&strategy_id.to_le_bytes());
    out.push(u8::from(is_short));
    out.push(0); // kind: regular detect
    out.push(0); // reserved
    write_str(&mut out, msg);
    out
}

/// `TStratRuntimeState` (CmdId 10).
pub fn runtime_state(uid: u64, strategies_running: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(BASE_HEADER_SIZE + 1);
    BaseHeader::write(&mut out, CMD_RUNTIME_STATE, uid);
    out.push(u8::from(strategies_running));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::strat::{
        build_checked_sync, build_delete, build_schema_request, build_sell_price_update,
        build_snapshot, StratCheckedItem, StratCommand,
    };
    use crate::commands::strategy_schema::StrategyFieldLayout;
    use crate::commands::strategy_serializer::{StrategyFields, StrategyKind};

    fn schema_blob() -> Vec<u8> {
        build_schema_blob(
            &[(6, "MoonShot")],
            &[
                SchemaField {
                    name: "StrategyName",
                    default: FieldValue::String(String::new()),
                    ui: ui::EDIT,
                    section: None,
                    picklist: None,
                    kinds: &[],
                },
                SchemaField {
                    name: "OrderSize",
                    default: FieldValue::Double(1000.0),
                    ui: ui::EDIT,
                    section: Some("Buy conditions"),
                    picklist: None,
                    kinds: &[],
                },
                SchemaField {
                    name: "SoundAlert",
                    default: FieldValue::Bool(true),
                    ui: ui::CHECKBOX,
                    section: None,
                    picklist: None,
                    kinds: &[],
                },
                SchemaField {
                    name: "SoundKind",
                    default: FieldValue::String("ding1".into()),
                    ui: ui::COMBO,
                    section: None,
                    picklist: Some("ding1|ding2"),
                    kinds: &[],
                },
            ],
        )
    }

    #[test]
    fn schema_defaults_and_sections_parse_upstream() {
        let blob = schema_blob();
        let schema = StrategySchema::parse_compressed(&blob).expect("schema");
        assert_eq!(schema.kinds.len(), 1);
        assert_eq!(schema.fields[0].default_value, None);
        assert_eq!(
            schema.fields[1].default_value,
            Some(FieldValue::Double(1000.0))
        );
        assert_eq!(
            schema.fields[1].layout,
            StrategyFieldLayout::Comment("Buy conditions".into())
        );
        assert_eq!(schema.fields[2].default_value, Some(FieldValue::Bool(true)));
        assert_eq!(
            schema.fields[3].default_value,
            Some(FieldValue::String("ding1".into()))
        );
        assert_eq!(schema.fields[3].static_picklist, ["ding1", "ding2"]);
        assert!(schema.fields[2].static_picklist.is_empty());
        let sections = schema.editor_sections_for_kind(6);
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[1].title, "Buy conditions");
        assert_eq!(sections[1].fields.len(), 3);

        let req = BaseHeader::parse(&build_schema_request(77)).unwrap();
        assert_eq!((req.cmd_id, req.uid), (CMD_SCHEMA_REQUEST, 77));
        match StratCommand::parse(&schema_payload(5, &blob)) {
            Some(StratCommand::Schema(s)) => assert_eq!(s.data, blob),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn field_kinds_limit_visibility_per_kind() {
        let field = |name, kinds| SchemaField {
            name,
            default: FieldValue::Double(1.0),
            ui: ui::EDIT,
            section: None,
            picklist: None,
            kinds,
        };
        let blob = build_schema_blob(
            &[(6, "MoonShot"), (2, "DropsDetection")],
            &[
                field("OrderSize", &[]),
                field("MShotPrice", &[6]),
                field("DropsPriceDelta", &[2]),
            ],
        );
        let schema = StrategySchema::parse_compressed(&blob).expect("schema");
        let kinds: Vec<Vec<u8>> = schema
            .fields
            .iter()
            .map(|f| f.visible_kind_ordinals.clone())
            .collect();
        assert_eq!(kinds, [vec![6, 2], vec![6], vec![2]]);
        assert!(schema.fields[2].visible_for_kind(2));
        assert!(!schema.fields[2].visible_for_kind(6));
    }

    #[test]
    fn batch_round_trips_through_upstream_and_keeps_empty_folders() {
        let schema = parse_schema(&schema_blob()).unwrap();
        let mut fields = StrategyFields::new();
        fields.insert("StrategyName", FieldValue::String("Shot".into()));
        fields.insert("OrderSize", FieldValue::Double(500.0));
        fields.insert("SoundAlert", FieldValue::Bool(false));
        let s = StrategySnapshot::new(
            9,
            2,
            1_700_000_000_000,
            true,
            StrategyKind::MOON_SHOT,
            "A/B",
            fields,
        );
        let data = encode_batch(&schema, std::slice::from_ref(&s), ["Empty"]);
        let (list, paths) = decode_batch(&data, &schema).expect("batch");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].strategy_id, 9);
        assert!(list[0].checked);
        assert_eq!(list[0].path.as_ref(), "A/B");
        assert_eq!(list[0].fields, s.fields);
        assert!(paths.iter().any(|p| p.as_ref() == "Empty"));

        let wire = snapshot(
            1,
            &Snapshot {
                server_epoch: 5,
                client_max_last_date: 7,
                full: true,
                data: data.clone(),
                folders_last_modified: 11,
            },
        );
        match StratCommand::parse(&wire) {
            Some(StratCommand::Snapshot(p)) => {
                assert!(p.full);
                assert_eq!((p.server_epoch, p.client_max_last_date), (5, 7));
                assert_eq!(p.folders_last_modified, 11);
                assert_eq!(p.data, data);
            }
            other => panic!("unexpected {other:?}"),
        }
        let inbound = build_snapshot(2, 5, 7, false, &data, 0, 0);
        let parsed = parse_snapshot(&inbound[BASE_HEADER_SIZE..]).expect("snapshot");
        assert!(!parsed.full);
        assert_eq!(parsed.data, data);
        let inbound = build_snapshot(2, 5, 7, true, &data, 13, 0);
        assert_eq!(
            parse_snapshot(&inbound[BASE_HEADER_SIZE..])
                .unwrap()
                .folders_last_modified,
            13
        );
    }

    #[test]
    fn small_commands_mirror_upstream() {
        let del = build_delete(1, 42, "Old/Folder");
        assert_eq!(
            parse_delete(&del[BASE_HEADER_SIZE..]),
            Some((42, "Old/Folder".into()))
        );
        match StratCommand::parse(&delete(1, 42, "X")) {
            Some(StratCommand::Delete(d)) => {
                assert_eq!((d.strategy_id, d.folder_path.as_str()), (42, "X"))
            }
            other => panic!("unexpected {other:?}"),
        }
        let sp = build_sell_price_update(1, 42, 2.5);
        assert_eq!(parse_sell_price(&sp[BASE_HEADER_SIZE..]), Some((42, 2.5)));

        let items = [
            StratCheckedItem {
                strategy_id: 1,
                checked: true,
            },
            StratCheckedItem {
                strategy_id: 2,
                checked: false,
            },
        ];
        let sync = build_checked_sync(3, &items, true);
        let parsed = parse_checked_sync(&sync[BASE_HEADER_SIZE..]).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!((parsed[1].strategy_id, parsed[1].checked), (2, false));
        match StratCommand::parse(&checked_echo(4, &parsed)) {
            Some(StratCommand::CheckedEcho(e)) => assert_eq!(e.items.to_vec(), items.to_vec()),
            other => panic!("unexpected {other:?}"),
        }
        match StratCommand::parse(&checked_sync(4, &parsed)) {
            Some(StratCommand::CheckedSync(s)) => assert!(s.is_delta && s.items.len() == 2),
            other => panic!("unexpected {other:?}"),
        }
        match StratCommand::parse(&runtime_state(6, true)) {
            Some(StratCommand::RuntimeState(r)) => assert!(r.strategies_running),
            other => panic!("unexpected {other:?}"),
        }
        match StratCommand::parse(&detect_signal(7, "SBER", 3, true, "buy 300.5")) {
            Some(StratCommand::DetectSignal(d)) => {
                assert!(d.is_regular_detect() && d.is_short);
                assert_eq!(
                    (d.market_name.as_str(), d.strategy_id, d.msg.as_str()),
                    ("SBER", 3, "buy 300.5")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
