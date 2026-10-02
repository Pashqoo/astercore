//! Live core orders, the account orders left to the account, the strategies'
//! running flag and the terminal's last `TClientSettings` across core
//! restarts (MoonBot keeps its
//! `*Orders.backup` for the same reason; its emulator mode must not fall
//! back to real trading on a restart): a JSON snapshot in
//! `data/orders.json`, written through a temp file and a rename.
//!
//! Written at most once a second when it changed, and at once before an
//! order request leaves for the exchange, so every request key is on disk
//! before the broker can know it; what still slips through is adopted as
//! before (`Orders::adopt`).

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::orders::{CoreOrder, Orders};

/// Minimum interval between two unforced writes.
const SAVE_PERIOD_MS: i64 = 1_000;

/// What the previous run left.
#[derive(Default, Deserialize)]
#[serde(default)]
pub struct Saved {
    pub running: bool,
    pub orders: Vec<CoreOrder>,
    /// The raw `TClientSettings` payload; empty when none was saved.
    pub client_settings: Vec<u8>,
    /// Start of the auto-stop loss counter (Unix s; 0 = all deals).
    pub loss_since: i64,
    /// When the terminal sent `client_settings` (ms; 0 = unknown): its
    /// temporary black list counts down from then, not from a restart.
    pub settings_at: i64,
    /// A market panic stopped the strategies («Restart if» may start them).
    pub market_stopped: bool,
    /// Ids of account orders left to the account (`Orders::left_ids`): a
    /// report of theirs after the restart stays theirs.
    pub left: Vec<String>,
    /// Set by `open` when a file was there and could not be used (unreadable or damaged): the
    /// orders of the previous run are lost, and the engine says so loudly. Never serialized.
    #[serde(skip)]
    pub lost: Option<String>,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    running: bool,
    orders: &'a [serde_json::Value],
    client_settings: &'a [u8],
    loss_since: i64,
    settings_at: i64,
    market_stopped: bool,
    left: &'a [String],
}

/// What is kept beside the orders.
#[derive(Clone, Copy)]
pub struct State<'a> {
    pub running: bool,
    pub client_settings: &'a [u8],
    pub settings_at: i64,
    pub loss_since: i64,
    pub market_stopped: bool,
}

pub struct OrderStore {
    path: PathBuf,
    /// Last text written (unchanged state is not written again).
    last: String,
    saved_at: i64,
    /// The last write failed: logged once until one succeeds.
    failing: bool,
    /// `last` is on disk for sure (the write that made it was forced, with an fsync): a later
    /// forced save of the same text still owes the fsync if this is false.
    synced: bool,
}

impl OrderStore {
    /// The store at `path` and what it holds. A missing file is a clean start; an unreadable or
    /// damaged one is put aside and reported in `Saved::lost` (the engine raises the alarm).
    pub fn open(path: PathBuf) -> (Self, Saved) {
        // Kept aside for a post-mortem; the next save replaces the file.
        // Strategies stay stopped: nothing says what was running.
        let aside = |why: String| {
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let bad = path.with_extension(format!("json.bad-{secs}"));
            let kept = fs::rename(&path, &bad).map_or_else(
                |e| format!("not kept: {e}"),
                |()| format!("kept as {}", bad.display()),
            );
            let lost = format!(
                "{}: {why}, starting without orders, strategies stopped ({kept})",
                path.display()
            );
            log::error!("orders: {lost}");
            Saved {
                lost: Some(lost),
                ..Saved::default()
            }
        };
        let saved = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| aside(e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
            Err(e) => aside(e.to_string()),
        };
        let store = Self {
            path,
            last: String::new(),
            saved_at: 0,
            failing: false,
            synced: false,
        };
        (store, saved)
    }

    /// Write the snapshot if it changed; unforced writes at most once per
    /// `SAVE_PERIOD_MS`. `false` when the snapshot is not on disk: the write
    /// failed, so a forced save before an order request did not keep its key.
    pub fn save(&mut self, orders: &Orders, state: &State<'_>, force: bool, now: i64) -> bool {
        let State {
            running,
            client_settings,
            settings_at,
            loss_since,
            market_stopped,
        } = *state;
        if !force && now - self.saved_at < SAVE_PERIOD_MS {
            return true;
        }
        self.saved_at = now;
        let list = orders.persisted();
        let left = orders.left_ids();
        let text = match serde_json::to_string(&Snapshot {
            running,
            orders: &list,
            client_settings,
            loss_since,
            settings_at,
            market_stopped,
            left: &left,
        }) {
            Ok(text) => text,
            Err(e) => {
                log::warn!("orders: snapshot: {e}");
                return false;
            }
        };
        if text == self.last {
            // The same snapshot as the last write, which a once-a-second save left unsynced:
            // a forced save (the one before an order request) still has to make it durable.
            if force && !self.synced {
                match fs::File::open(&self.path).and_then(|f| f.sync_all()) {
                    Ok(()) => self.synced = true,
                    Err(e) => {
                        log::warn!("orders: {}: sync: {e}", self.path.display());
                        return false;
                    }
                }
            }
            return true;
        }
        let tmp = self.path.with_extension("json.tmp");
        let written = self
            .path
            .parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| {
                use std::io::Write;
                let mut f = fs::File::create(&tmp)?;
                f.write_all(text.as_bytes())?;
                // On disk before the rename makes it the file, so a crash leaves the old one or
                // the whole new one, never an empty name — but only for a FORCED save, the one
                // before an order request, whose key must survive a crash. The once-a-second
                // ones skip the fsync (it would stall the trading thread every second) and
                // accept that a power loss may cost them: the next forced save repeats the
                // text and syncs it (`synced`).
                if force {
                    f.sync_all()?;
                }
                Ok(())
            })
            .and_then(|()| fs::rename(&tmp, &self.path));
        match written {
            Ok(()) => {
                self.last = text;
                self.synced = force;
                self.failing = false;
                true
            }
            Err(e) => {
                if !self.failing {
                    self.failing = true;
                    log::warn!("orders: {}: {e}", self.path.display());
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// A file that cannot be read is not a clean start: the orders are reported lost and the
    /// file is put aside instead of being replaced by the next save.
    #[test]
    fn an_unreadable_file_is_reported_lost_and_put_aside() {
        let dir = std::env::temp_dir().join(format!("aster-store-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        // A directory where the file should be: `fs::read` fails with something other than
        // NotFound.
        let path = dir.join("orders.json");
        fs::create_dir_all(&path).unwrap();
        let (_, saved) = OrderStore::open(path.clone());
        assert!(saved.lost.is_some());
        assert!(saved.orders.is_empty());
        assert!(!path.exists(), "kept aside");
        let (_, clean) = OrderStore::open(path);
        assert!(clean.lost.is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    use super::*;

    /// A snapshot from before `left` existed still loads, with nothing left.
    #[test]
    fn a_snapshot_without_left_loads() {
        let old = r#"{"running":true,"orders":[],"client_settings":[],"loss_since":0,"settings_at":0,"market_stopped":false}"#;
        let saved: Saved = serde_json::from_str(old).unwrap();
        assert!(saved.running && saved.left.is_empty());
        let new = r#"{"running":false,"left":["9002","app-sale"]}"#;
        let saved: Saved = serde_json::from_str(new).unwrap();
        assert_eq!(saved.left, ["9002", "app-sale"]);
    }
}
