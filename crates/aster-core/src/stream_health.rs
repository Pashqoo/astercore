//! Liveness of the static streams the core rests on: a stream is alive while
//! its last frame is at most [`STALE_AFTER_MS`] old. The age, not the session
//! state, is the signal: a thread blocked in `read` does not see its own
//! silence, a reconnect takes seconds, and a thread that died stops beating by
//! itself.
//!
//! Ported from TInvestCore, trimmed. What is NOT here: the last-price canary
//! (it caught a T-Invest gateway that kept a stream open while dropping its
//! trades — not observed on Aster, and Aster's tape has no "last price
//! changed at" field to judge it by), and the chat alarms (`telegram`, M4).

use crate::aster::ws::{Beat, Stamp};

/// How old the last frame may get before a stream is stale.
///
/// NOT TInvestCore's 15 s. There the gateway pinged every 5 s, so a silence
/// was a dead stream. Here the gateway pings every 5 MINUTES and Aster's tape
/// is thin — measured 01.10: 200 markets' `aggTrade` delivered 3–14 frames in
/// 8–12 s, a quiet chunk can say nothing for a while and be perfectly alive.
/// So staleness is judged on the stream that cannot be quiet: `!markPrice@arr`
/// pushes the whole catalog every 3 s (measured), and 15 s is five missed
/// pushes. A trade chunk hears the pong of the core's own ping every 30 s
/// (`ws::PING_EVERY`) however quiet its markets are, so its 120 s bound means
/// "the connection is gone and reconnecting has not worked", never "the
/// markets are quiet". Its silence takes the markets' `feed_fresh` with it,
/// which holds their orders and the strategies' entries on them.
pub const STALE_AFTER_MS: i64 = 15_000;
pub const TRADES_STALE_AFTER_MS: i64 = 120_000;
/// Period of the summary line: every watched stream, alive or not, with the
/// sessions it opened since the last one.
pub const SUMMARY_EVERY_MS: i64 = 300_000;

/// What a stream carries, and so what its silence takes away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Trades of these markets (symbols): the tape goes quiet.
    Markets(Vec<String>),
    /// The mark price and funding of the whole catalog.
    Marks,
}

impl Scope {
    fn what(&self) -> String {
        match self {
            Self::Markets(symbols) => format!("{} markets' trades", symbols.len()),
            Self::Marks => "mark prices and funding".into(),
        }
    }

    fn stale_after(&self) -> i64 {
        match self {
            Self::Markets(_) => TRADES_STALE_AFTER_MS,
            Self::Marks => STALE_AFTER_MS,
        }
    }
}

struct Watched {
    name: String,
    scope: Scope,
    beat: Beat,
    alive: bool,
    /// `beat.sessions()` at the last summary.
    sessions_seen: i64,
}

#[derive(Default)]
pub struct StreamHealth {
    streams: Vec<Watched>,
    /// The first judge only starts the summary period.
    summarized: Option<Stamp>,
}

impl StreamHealth {
    /// Register a stream; its thread touches the returned beat on every frame.
    pub fn watch(&mut self, name: impl Into<String>, scope: Scope) -> Beat {
        let beat = Beat::new();
        self.streams.push(Watched {
            name: name.into(),
            scope,
            beat: beat.clone(),
            alive: true,
            sessions_seen: 0,
        });
        beat
    }

    /// Streams whose liveness flipped since the last call at `now`, one log
    /// line each; every [`SUMMARY_EVERY_MS`] also the summary line.
    pub fn judge(&mut self, now: Stamp) -> Vec<(&Scope, bool)> {
        if let Some(line) = self.summary(now) {
            log::info!("{line}");
        }
        let mut out = Vec::new();
        for w in &mut self.streams {
            let age = now.since(w.beat.at());
            let alive = age <= w.scope.stale_after();
            if alive == w.alive {
                continue;
            }
            w.alive = alive;
            let what = w.scope.what();
            if alive {
                log::info!("stream {}: frames again, {what} live", w.name);
            } else {
                log::warn!(
                    "stream {}: no frame for {} s, {what} stale",
                    w.name,
                    age / 1000
                );
            }
            out.push((&w.scope, alive));
        }
        out
    }

    /// `streams: trades#0 ok 1 session, marks stale 40 s 3 sessions`, once
    /// per [`SUMMARY_EVERY_MS`]; sessions counted since the previous line.
    fn summary(&mut self, now: Stamp) -> Option<String> {
        let last = *self.summarized.get_or_insert(now);
        if now.since(last) < SUMMARY_EVERY_MS || self.streams.is_empty() {
            return None;
        }
        self.summarized = Some(now);
        let parts: Vec<String> = self
            .streams
            .iter_mut()
            .map(|w| {
                let total = w.beat.sessions();
                let opened = total - std::mem::replace(&mut w.sessions_seen, total);
                let state = if w.alive {
                    "ok".to_string()
                } else {
                    format!("stale {} s", now.since(w.beat.at()) / 1000)
                };
                let plural = if opened == 1 { "" } else { "s" };
                format!("{} {state} {opened} session{plural}", w.name)
            })
            .collect();
        Some(format!("streams: {}", parts.join(", ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_silent_stream_goes_stale_once_and_comes_back_once() {
        let mut h = StreamHealth::default();
        let beat = h.watch("marks", Scope::Marks);
        let t = beat.at();
        assert!(
            h.judge(t.plus(STALE_AFTER_MS)).is_empty(),
            "alive at the bound"
        );
        let slept = Stamp {
            wall: t.wall + STALE_AFTER_MS + 1,
            mono: t.mono,
        };
        assert_eq!(h.judge(slept), [(&Scope::Marks, false)]);
        assert!(h.judge(t.plus(STALE_AFTER_MS + 2)).is_empty(), "no repeat");
        beat.touch();
        assert_eq!(h.judge(beat.at()), [(&Scope::Marks, true)]);
    }

    #[test]
    fn a_quiet_trade_chunk_is_given_the_wider_bound() {
        let mut h = StreamHealth::default();
        let scope = Scope::Markets(vec!["BTCUSDT".into()]);
        let beat = h.watch("trades#0", scope.clone());
        let t = beat.at();
        assert!(
            h.judge(t.plus(STALE_AFTER_MS + 1)).is_empty(),
            "a thin tape is not a dead one"
        );
        assert_eq!(
            h.judge(t.plus(TRADES_STALE_AFTER_MS + 1)),
            [(&scope, false)]
        );
    }

    #[test]
    fn summary_every_period_with_sessions_since_the_last_one() {
        let mut h = StreamHealth::default();
        let beat = h.watch("marks", Scope::Marks);
        let t = beat.at();
        assert_eq!(h.summary(t), None, "the first call starts the period");
        assert_eq!(h.summary(t.plus(SUMMARY_EVERY_MS - 1)), None);
        let line = h.summary(t.plus(SUMMARY_EVERY_MS)).unwrap();
        assert_eq!(line, "streams: marks ok 0 sessions");
    }
}
