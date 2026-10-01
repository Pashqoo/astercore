//! MoonBot strategy text (`#Begin_Folder` / `##Begin_Strategy` / `Key=Value`):
//! the core's persistent strategy list, pasteable into the terminal's tree.
//! Header keys follow MoonBot (`Active`, `FVersion`, `FIntID`, `LastEditDate`);
//! `LastEditDate` is Unix milliseconds because the rollback guard needs them.

use std::fmt::Write as _;

use moonproto::{FieldValue, StrategyFieldType, StrategyFields, StrategySchema, StrategySnapshot};

const BEGIN_FOLDER: &str = "#Begin_Folder ";
const END_FOLDER: &str = "#End_Folder";
const BEGIN_STRATEGY: &str = "##Begin_Strategy";
const END_STRATEGY: &str = "##End_Strategy";
const KIND_FIELD: &str = "SignalType";

/// Strategies in list order (folders opened/closed as the path changes), then
/// folders that hold no strategy.
pub fn render(
    strategies: &[StrategySnapshot],
    folders: &[String],
    schema: &StrategySchema,
) -> String {
    let mut out = String::new();
    let mut open: Vec<&str> = Vec::new();
    for s in strategies {
        move_to(&mut out, &mut open, &s.path);
        write_strategy(&mut out, s, schema);
    }
    move_to(&mut out, &mut open, "");
    for f in folders {
        let occupied = strategies
            .iter()
            .any(|s| s.path.eq_ignore_ascii_case(f) || under(&s.path, f));
        if !occupied && !folders.iter().any(|o| under(o, f)) {
            move_to(&mut out, &mut open, f);
            move_to(&mut out, &mut open, "");
        }
    }
    out
}

fn under(path: &str, folder: &str) -> bool {
    path.len() > folder.len()
        && path.as_bytes()[folder.len()] == b'/'
        && path[..folder.len()].eq_ignore_ascii_case(folder)
}

fn move_to<'a>(out: &mut String, open: &mut Vec<&'a str>, path: &'a str) {
    let want: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let common = open
        .iter()
        .zip(&want)
        .take_while(|(a, b)| a.eq_ignore_ascii_case(b))
        .count();
    while open.len() > common {
        open.pop();
        out.push_str(END_FOLDER);
        out.push('\n');
    }
    for seg in &want[common..] {
        open.push(seg);
        let _ = writeln!(out, "{BEGIN_FOLDER}{seg}");
    }
}

fn write_strategy(out: &mut String, s: &StrategySnapshot, schema: &StrategySchema) {
    out.push_str(BEGIN_STRATEGY);
    out.push('\n');
    let _ = writeln!(out, "   Active={}", if s.checked { -1 } else { 0 });
    let _ = writeln!(out, "   FVersion={}", s.strategy_ver);
    let _ = writeln!(out, "   FIntID={}", s.strategy_id);
    let _ = writeln!(out, "   LastEditDate={}", s.last_date);
    let kind = schema.kind_name_for_strategy_kind(s.kind()).unwrap_or("");
    let _ = writeln!(out, "   {KIND_FIELD}={kind}");
    for f in &schema.fields {
        if f.name == KIND_FIELD {
            continue;
        }
        if let Some(v) = s.fields.get(&f.name) {
            let _ = writeln!(out, "   {}={}", f.name, text(v));
        }
    }
    out.push_str(END_STRATEGY);
    out.push('\n');
}

/// One typed field as the text a strategy file carries — and the text the
/// page's helper puts in its form, so a setting reads the same in both.
pub(crate) fn text(v: &FieldValue) -> String {
    match v {
        FieldValue::Bool(b) => if *b { "YES" } else { "NO" }.into(),
        FieldValue::Int32(n) => n.to_string(),
        FieldValue::Int64(n) => n.to_string(),
        FieldValue::UInt32(n) => n.to_string(),
        FieldValue::UInt64(n) => n.to_string(),
        FieldValue::Byte(n) => n.to_string(),
        FieldValue::Word(n) => n.to_string(),
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Single(f) => f.to_string(),
        FieldValue::String(s) => s.replace(['\r', '\n'], " "),
    }
}

/// Strategies and every folder seen (including empty ones); malformed
/// strategies are skipped with a log line.
pub fn parse(text: &str, schema: &StrategySchema) -> (Vec<StrategySnapshot>, Vec<String>) {
    let mut strategies = Vec::new();
    let mut folders: Vec<String> = Vec::new();
    let mut stack: Vec<String> = Vec::new();
    let mut fields: Option<Vec<(String, String)>> = None;
    for line in text.lines() {
        let marker = line.trim();
        if let Some(name) = marker.strip_prefix(BEGIN_FOLDER) {
            stack.push(name.trim().to_string());
            let path = stack.join("/");
            if !folders.iter().any(|f| f.eq_ignore_ascii_case(&path)) {
                folders.push(path);
            }
        } else if marker == END_FOLDER {
            stack.pop();
        } else if marker == BEGIN_STRATEGY {
            fields = Some(Vec::new());
        } else if marker == END_STRATEGY {
            if let Some(pairs) = fields.take() {
                match strategy(&pairs, &stack.join("/"), schema) {
                    Some(s) => strategies.push(s),
                    None => log::warn!("strategies file: skipped a malformed strategy"),
                }
            }
        } else if let (Some(pairs), Some((k, v))) = (fields.as_mut(), line.split_once('=')) {
            pairs.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    (strategies, folders)
}

fn strategy(
    pairs: &[(String, String)],
    path: &str,
    schema: &StrategySchema,
) -> Option<StrategySnapshot> {
    let get = |name: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    };
    let id: u64 = get("FIntID")?.parse().ok()?;
    let kind_name = get(KIND_FIELD)?;
    let kind = schema
        .kinds
        .iter()
        .find(|k| k.name.eq_ignore_ascii_case(kind_name))?
        .kind();
    let mut fields = StrategyFields::new();
    for (k, v) in pairs.iter().filter(|(k, _)| k != KIND_FIELD) {
        if let Some(value) = schema.field(k).and_then(|f| value(f.type_id, v)) {
            fields.insert(k.as_str(), value);
        }
    }
    Some(StrategySnapshot::new(
        id,
        get("FVersion").and_then(|v| v.parse().ok()).unwrap_or(0),
        get("LastEditDate")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        get("Active").is_some_and(truthy),
        kind,
        path,
        fields,
    ))
}

fn truthy(v: &str) -> bool {
    matches!(
        v.to_ascii_lowercase().as_str(),
        "yes" | "true" | "1" | "-1" | "on"
    )
}

/// One field's text as the schema's type. A strategy file is the one place
/// this core turns typed fields into text and back, so the page's helper —
/// whose form is text too — reads its settings through the same door.
pub(crate) fn value(t: StrategyFieldType, v: &str) -> Option<FieldValue> {
    Some(match t {
        StrategyFieldType::Bool => FieldValue::Bool(truthy(v)),
        StrategyFieldType::Int32 => FieldValue::Int32(v.parse().ok()?),
        StrategyFieldType::Int64 => FieldValue::Int64(v.parse().ok()?),
        StrategyFieldType::UInt32 => FieldValue::UInt32(v.parse().ok()?),
        StrategyFieldType::UInt64 => FieldValue::UInt64(v.parse().ok()?),
        StrategyFieldType::Byte => FieldValue::Byte(v.parse().ok()?),
        StrategyFieldType::Word => FieldValue::Word(v.parse().ok()?),
        StrategyFieldType::Double => FieldValue::Double(v.parse().ok()?),
        StrategyFieldType::Single => FieldValue::Single(v.parse().ok()?),
        StrategyFieldType::String => FieldValue::String(v.to_string()),
        StrategyFieldType::Unknown(_) => return None,
    })
}
