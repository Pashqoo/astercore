//! Minimal `log` backend: level from `ASTER_CORE_LOG` (default `info`). Each line goes to
//! stderr (journald under systemd) and to `logs/YYYY-MM-DD.log` in the working directory: one file
//! per day of the trader's clock (Moscow, `clock.rs`; lines keep UTC time), the last `KEEP_DAYS`
//! files kept. The web page reads their tail. Ported from TInvestCore.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, PoisonError};

use log::{Level, LevelFilter, Log, Metadata, Record};

use crate::clock as time;
use crate::clock::{trader_midnight as msk_midnight_fn, TRADER_OFFSET_MS};

const LOG_DIR: &str = "logs";
/// Moscow days of files kept, today included. The settings replace it at the
/// start and whenever the page changes it ([`set_keep_days`]); an atomic
/// rather than a lock, because it is read on the logging path itself.
static KEEP_DAYS: AtomicI64 = AtomicI64::new(3);
/// The widest window the journal is kept for. Lives here, where the sweeping
/// happens, and `settings` validates against it: one number, one place.
pub const MAX_KEEP_DAYS: i64 = 365;
const DAY_MS: i64 = 86_400_000;
/// Bytes read from the end of a day file for a tail. A `debug` day is tens of
/// megabytes and the page asks for a few hundred lines: the file is read from
/// this far back and not from the start.
const TAIL_BYTES: u64 = 512 * 1024;

/// The current Moscow day's file; `file` is `None` after a failed open or write, retried when the
/// day changes.
struct DayFile {
    /// Unix ms of the day's Moscow midnight.
    day: i64,
    file: Option<File>,
}

static DAY_FILE: Mutex<Option<DayFile>> = Mutex::new(None);

struct StderrLog;

impl Log for StderrLog {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let level = match record.level() {
            Level::Error => "E",
            Level::Warn => "W",
            Level::Info => "I",
            Level::Debug => "D",
            Level::Trace => "T",
        };
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        let sod = ms.rem_euclid(86_400_000);
        let line = format!(
            "{:02}:{:02}:{:02}.{:03} {level} {} {}",
            sod / 3_600_000,
            sod % 3_600_000 / 60_000,
            sod % 60_000 / 1000,
            sod % 1000,
            record.target(),
            record.args()
        );
        eprintln!("{line}");
        append(ms, &line);
    }

    fn flush(&self) {}
}

fn append(ms: i64, line: &str) {
    let mut guard = DAY_FILE.lock().unwrap_or_else(PoisonError::into_inner);
    let day = msk_midnight_fn(ms);
    if guard.as_ref().is_none_or(|f| f.day != day) {
        *guard = Some(DayFile {
            day,
            file: open_day(ms),
        });
        remove_stale(ms);
    }
    let Some(day_file) = guard.as_mut() else {
        return;
    };
    if let Some(Err(e)) = day_file.file.as_mut().map(|f| writeln!(f, "{line}")) {
        eprintln!("log file: {e}; file logging paused until the next Moscow day");
        day_file.file = None;
    }
}

/// `YYYY-MM-DD` of the Moscow day containing `ms`.
fn msk_date(ms: i64) -> String {
    time::format_rfc3339(ms + TRADER_OFFSET_MS)[..10].to_owned()
}

fn open_day(ms: i64) -> Option<File> {
    let path = format!("{LOG_DIR}/{}.log", msk_date(ms));
    fs::create_dir_all(LOG_DIR)
        .and_then(|()| OpenOptions::new().create(true).append(true).open(&path))
        .map_err(|e| eprintln!("log file {path}: {e}"))
        .ok()
}

/// A day file (`YYYY-MM-DD.log`) older than the `KEEP_DAYS` Moscow days ending with `now_ms`'s.
fn is_stale(name: &str, now_ms: i64) -> bool {
    let keep = KEEP_DAYS.load(Ordering::Relaxed).max(1);
    let oldest_kept = msk_date(now_ms - (keep - 1) * DAY_MS);
    name.strip_suffix(".log").is_some_and(|d| {
        d.len() == 10
            && time::parse_rfc3339_ms(&format!("{d}T00:00:00Z")).is_some()
            && d < oldest_kept.as_str()
    })
}

fn remove_stale(now_ms: i64) {
    let Ok(entries) = fs::read_dir(LOG_DIR) else {
        return;
    };
    for name in entries.filter_map(|e| e.ok()?.file_name().into_string().ok()) {
        if is_stale(&name, now_ms) {
            if let Err(e) = fs::remove_file(format!("{LOG_DIR}/{name}")) {
                eprintln!("log file {LOG_DIR}/{name}: {e}");
            }
        }
    }
}

/// The last `lines` of the journal, oldest first, for the page (`web.rs`):
/// today's Moscow day file, topped up from the day before when today's is
/// shorter — a page opened at 00:05 MSK would otherwise be blank.
///
/// Read from its own handle, not through the writer's mutex: the page must not
/// be able to hold up a line the trading loop is logging.
pub fn tail(now_ms: i64, lines: usize) -> String {
    let mut out = day_tail(now_ms, lines);
    if out.len() < lines {
        let mut earlier = day_tail(now_ms - DAY_MS, lines - out.len());
        earlier.append(&mut out);
        out = earlier;
    }
    out.join("\n")
}

/// The last `lines` of one Moscow day's file, oldest first; an absent or
/// unreadable file is no lines, because the page asking for the journal must
/// not be an error the operator has to read.
fn day_tail(ms: i64, lines: usize) -> Vec<String> {
    let path = format!("{LOG_DIR}/{}.log", msk_date(ms));
    let Ok(mut file) = File::open(&path) else {
        return Vec::new();
    };
    let len = file.metadata().map_or(0, |m| m.len());
    let from = len.saturating_sub(TAIL_BYTES);
    let mut buf = Vec::new();
    if file
        .seek(SeekFrom::Start(from))
        .and_then(|_| file.read_to_end(&mut buf))
        .is_err()
    {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&buf);
    let mut read: Vec<&str> = text.lines().collect();
    // Seeking into the middle of the file cuts the first line in half.
    if from > 0 && !read.is_empty() {
        read.remove(0);
    }
    read[read.len().saturating_sub(lines)..]
        .iter()
        .map(|l| (*l).to_owned())
        .collect()
}

pub fn init() {
    let level = std::env::var("ASTER_CORE_LOG")
        .ok()
        .and_then(|v| v.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::Info);
    if log::set_logger(&StderrLog).is_ok() {
        log::set_max_level(level);
    }
}

/// Apply how many Moscow days of files to keep. Takes effect at the next day
/// change, when the stale ones are swept — nothing is deleted the moment the
/// number is lowered, which also means an operator who lowers it by mistake
/// has until midnight to put it back.
pub fn set_keep_days(days: i64) {
    let days = days.clamp(1, MAX_KEEP_DAYS);
    if KEEP_DAYS.swap(days, Ordering::Relaxed) != days {
        log::info!("log files kept: {days} Moscow day(s)");
    }
}

/// Apply the level from the settings, at the start and whenever the page
/// changes it. A name that is not a level is reported and changes nothing: a
/// typo in the log level must never be what takes the core off the market.
pub fn set_level(name: &str) -> bool {
    match name.trim().parse::<LevelFilter>() {
        Ok(level) => {
            if level != log::max_level() {
                log::set_max_level(level);
                log::info!("log level {level}");
            }
            true
        }
        Err(_) => {
            log::warn!(
                "log level {name:?} is not a level; staying at {}",
                log::max_level()
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-09-19 20:59:59.999 UTC = 23:59:59.999 MSK.
    const LAST_MS_19TH_MSK: i64 = 1_789_851_599_999;

    #[test]
    fn day_turns_at_moscow_midnight() {
        assert_eq!(msk_date(LAST_MS_19TH_MSK), "2026-09-19");
        assert_eq!(msk_date(LAST_MS_19TH_MSK + 1), "2026-09-20");
        assert_ne!(
            msk_midnight_fn(LAST_MS_19TH_MSK),
            msk_midnight_fn(LAST_MS_19TH_MSK + 1)
        );
    }

    #[test]
    fn keeps_three_moscow_days() {
        // The count is process-global now: this test says which one it means.
        set_keep_days(3);
        let now = LAST_MS_19TH_MSK + 1; // 2026-09-20 00:00 MSK
        assert!(!is_stale("2026-09-20.log", now));
        assert!(!is_stale("2026-09-18.log", now));
        assert!(is_stale("2026-09-17.log", now));
        assert!(is_stale("2025-12-31.log", now));
        for foreign in [
            "2026-09-17.txt",
            "old.log",
            "2026-9-1.log",
            "2026-09-17.log.gz",
        ] {
            assert!(!is_stale(foreign, now), "{foreign}");
        }
        // A wider window keeps what the narrow one swept, and a nonsense
        // count still keeps today rather than deleting the file being written.
        set_keep_days(7);
        assert!(!is_stale("2026-09-14.log", now));
        assert!(is_stale("2026-09-13.log", now));
        set_keep_days(0);
        assert!(!is_stale("2026-09-20.log", now), "today is never stale");
        assert!(is_stale("2026-09-19.log", now));
        // And the far end is the one `settings::validate` refuses past, so a
        // value that got here another way cannot widen the window further.
        // The window counts today in, so 365 days of it reach back to
        // 2025-09-21; the day before that is the first one swept, whatever
        // number got past the clamp.
        set_keep_days(i64::MAX);
        assert!(!is_stale("2025-09-21.log", now), "a year back is kept");
        assert!(
            is_stale("2025-09-20.log", now),
            "past the year it still goes"
        );
        set_keep_days(3);
    }
}
