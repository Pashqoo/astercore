//! Strategy schema and list, as far as M0 needs them.
//!
//! The schema is a **mandatory Init step**: the client asks for it between
//! `UpdateMarketsList` and `Ready` and fails init if it does not parse
//! (`moonproto::client::init::steps`, `TStratSchemaRequest`). So it cannot wait
//! for M3, when the strategy engines themselves are ported.
//!
//! What it deliberately is NOT is TInvestCore's schema. That one declares ~150
//! fields — pool filters, delta gates, turnover windows, per-signal settings —
//! and every one of them is read by a module this core does not have yet
//! (`moonshot`, `screener`, `guards`). Copying the list over would put a full
//! strategy editor in front of the trader whose every field is silently
//! ignored, which is worse than an editor that plainly has four. M3 brings the
//! fields together with the code that honours them (`PLAN.md`, M3).
//!
//! The kinds ARE all five, because they are what the terminal offers in its
//! `SignalType` combo and what M3 ports; a core that named one would be
//! claiming the others do not exist on this venue.

use moonproto::server::codec::strat::{self, ui, SchemaField, Snapshot};
use moonproto::{FieldValue, StrategyKind, StrategySchema};

pub const KIND_MOONSHOT: (u8, &str) = (StrategyKind::MOON_SHOT.ordinal(), "MoonShot");
pub const KIND_DROPS: (u8, &str) = (StrategyKind::DROPS.ordinal(), "DropsDetection");
pub const KIND_STRIKE: (u8, &str) = (StrategyKind::MOON_STRIKE.ordinal(), "MoonStrike");
pub const KIND_HOOK: (u8, &str) = (StrategyKind::MOON_HOOK.ordinal(), "MoonHook");
/// MoonBot's `Manual`: no entries of its own; the exit settings of the
/// terminal's hand trades while `use_manual_strategy` names it.
pub const KIND_MANUAL: (u8, &str) = (StrategyKind::MANUAL.ordinal(), "Manual");

const KINDS: [(u8, &str); 5] = [
    KIND_MOONSHOT,
    KIND_DROPS,
    KIND_STRIKE,
    KIND_HOOK,
    KIND_MANUAL,
];

/// The four fields every strategy has whatever its kind, and nothing else.
///
/// They are the strategy's identity (name, kind, comment) plus the one switch
/// that decides whether it may touch real money at all. Each of them is honest
/// at M0: the core keeps them and runs nothing, so none of them is a promise.
fn schema_fields() -> Vec<SchemaField> {
    let f = |name, default, ui, section| SchemaField {
        name,
        default,
        ui,
        section,
        picklist: None,
        kinds: &[],
    };
    vec![
        f(
            "StrategyName",
            FieldValue::String(String::new()),
            ui::EDIT,
            None,
        ),
        SchemaField {
            name: "SignalType",
            default: FieldValue::String(KIND_MOONSHOT.1.to_string()),
            ui: ui::COMBO,
            section: None,
            picklist: Some("MoonShot|DropsDetection|MoonStrike|MoonHook|Manual"),
            kinds: &[],
        },
        f("Comment", FieldValue::String(String::new()), ui::EDIT, None),
        // Default ON, which is the opposite of MoonBot's and of TInvestCore's
        // default — and the only honest value while this core has no order
        // path at all (M2). A strategy created now cannot trade; one created
        // now and left alone must not start trading the moment it can.
        f("EmulatorMode", FieldValue::Bool(true), ui::CHECKBOX, None),
    ]
}

/// The core's strategy list: at M0 the schema plus whatever the terminal sent.
pub struct Strategies {
    schema: StrategySchema,
    schema_blob: Vec<u8>,
    /// The last snapshot a terminal sent, kept whole and unparsed.
    ///
    /// Unparsed on purpose: nothing here acts on a strategy's fields yet, and
    /// decoding them into a shape M3 will replace would be a second model of
    /// the same data. Kept rather than dropped because it is the trader's work
    /// and the core is where M3 will read it from.
    ///
    /// What it does NOT do yet is travel: nothing sends it back, so a second
    /// terminal is not handed the first one's list and a restart of the core
    /// loses it. Serving and persisting the list needs the per-strategy
    /// rollback guard and the MoonBot text file M3 ports, and half of that —
    /// echoing a list this core cannot run — would hand a terminal strategies
    /// that do nothing.
    snapshot: Option<Snapshot>,
    running: bool,
}

impl Strategies {
    pub fn new() -> Self {
        let schema_blob = strat::build_schema_blob(&KINDS, &schema_fields());
        // The one place this can be caught is here, and a core whose schema
        // does not parse cannot reach `Ready` at all: the client fails that
        // Init step. A panic at startup beats a terminal that times out on it.
        let schema = strat::parse_schema(&schema_blob).expect("own schema parses");
        Self {
            schema,
            schema_blob,
            snapshot: None,
            running: false,
        }
    }

    pub fn schema_blob(&self) -> &[u8] {
        &self.schema_blob
    }

    /// The decoded schema, for the serializer M3 will read the list with.
    pub fn schema(&self) -> &StrategySchema {
        &self.schema
    }

    pub fn running(&self) -> bool {
        self.running
    }

    pub fn set_running(&mut self, running: bool) {
        self.running = running;
    }

    /// Keep what the terminal sent. A delta snapshot is NOT merged into the
    /// full one — merging needs the per-strategy rollback guard M3 ports, and a
    /// wrong merge would hand a later terminal a list the trader never made.
    /// Only a full snapshot replaces the kept list; a delta is counted and
    /// logged.
    pub fn store(&mut self, snap: Snapshot) {
        if !snap.full {
            log::debug!(
                "strategies: delta snapshot of {} bytes ignored until M3",
                snap.data.len()
            );
            return;
        }
        log::info!(
            "strategies: full snapshot of {} bytes kept",
            snap.data.len()
        );
        self.snapshot = Some(snap);
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
}

impl Default for Strategies {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Init step this exists for: the client parses the blob and fails init
    /// if the parse does not produce the kinds and fields it declares.
    #[test]
    fn the_schema_parses_as_the_client_parses_it() {
        let s = Strategies::new();
        // Through the same entry point the core itself uses, which is the one
        // the client's parser is a mirror of.
        let schema = strat::parse_schema(s.schema_blob()).expect("schema");
        assert_eq!(schema.kinds.len(), 5);
        assert_eq!(schema.fields.len(), 4);
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            ["StrategyName", "SignalType", "Comment", "EmulatorMode"]
        );
        // The one default that is a decision rather than a blank.
        assert_eq!(
            schema.fields[3].default_value,
            Some(FieldValue::Bool(true)),
            "a strategy must not be born able to spend real money"
        );
    }

    #[test]
    fn only_a_full_snapshot_replaces_the_kept_list() {
        let mut s = Strategies::new();
        let snap = |full, byte| Snapshot {
            server_epoch: 1,
            client_max_last_date: 0,
            full,
            data: vec![byte],
            folders_last_modified: 0,
        };
        s.store(snap(true, 7));
        s.store(snap(false, 9));
        assert_eq!(
            s.snapshot().expect("kept").data,
            vec![7],
            "a delta must not overwrite the full list it is a delta of"
        );
    }
}
