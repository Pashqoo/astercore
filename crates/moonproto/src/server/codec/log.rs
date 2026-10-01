//! `Command::LogMsg` server side: one server log line the terminal shows in
//! its core log (`time: f64 Delphi days + utf8`).

use super::market_data::delphi_days;

pub fn log_msg(time_ms: i64, text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + text.len());
    out.extend_from_slice(&delphi_days(time_ms).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, EventDispatcher};
    use crate::protocol::Command;

    #[test]
    fn log_line_dispatches_upstream() {
        let mut d = EventDispatcher::new();
        let events = d.dispatch(
            Command::LogMsg,
            &log_msg(1_700_000_000_000, "SBER: closed"),
            1,
        );
        assert_eq!(events.len(), 1);
        let Event::ServerLog(log) = &events[0] else {
            panic!("{events:?}");
        };
        assert_eq!(log.msg, "SBER: closed");
        assert_eq!(log.time().unix_millis(), 1_700_000_000_000);
    }
}
