//! Minimal `log` backend: level from `ASTER_CORE_LOG` (default `info`), one
//! line per record on stderr (journald under systemd), UTC.
//!
//! TInvestCore's version also keeps a file per Moscow day and serves tails of
//! it to its web page. Neither half carries over as it stands: the page is M4,
//! and the day boundary here is UTC — Aster's own `timezone` is UTC and the
//! core has no local-time arithmetic anywhere (`PLAN.md`). The day files land
//! with the page that reads them.

use std::io::Write;

use log::{Level, LevelFilter, Log, Metadata, Record};

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
            "{:02}:{:02}:{:02}.{:03} {level} {} {}\n",
            sod / 3_600_000,
            sod % 3_600_000 / 60_000,
            sod % 60_000 / 1_000,
            sod % 1_000,
            record.target(),
            record.args()
        );
        // One write per line, and a failure is dropped rather than retried: the
        // logger is called from the UDP loop and from the price thread, and a
        // core must not stall or panic because its stderr went away.
        let _ = std::io::stderr().write_all(line.as_bytes());
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

/// Install the backend. Called once, at the top of `main`; a second call is
/// ignored by `log` itself.
pub fn init() {
    let level = match std::env::var("ASTER_CORE_LOG")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        // Anything else, the empty value included: `info`. An unreadable level
        // must not silence the journal of a running core.
        _ => LevelFilter::Info,
    };
    log::set_max_level(level);
    let _ = log::set_logger(&StderrLog);
}
