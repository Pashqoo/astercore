//! Loss guards of a strategy (MoonBot strategy settings): `TotalLoss` stops
//! its entries once its closed deals lost that much over the auto-start loss
//! window (Filters / Price/Position), and the Sessions tab penalizes a
//! strategy × market whose accumulated profit fell to `SessionStratMin`.
//!
//! Sessions (FAQ «Sessions»): each closed deal's profit accumulates per
//! strategy × market; reaching `SessionStratMax` is a plus session and
//! reaching `SessionStratMin` a minus one — each resets the accumulator and
//! the opposite counter, and a minus session keeps the strategy off the
//! market for `SessionPenaltyTime`. `SessionResetOnMinus` zeroes a positive
//! accumulator on a losing deal bigger than a tenth of the minimum. Order
//! size changes by session counters are not carried out. Profits are the
//! report's USDT; a late commission counts as a change of its deal, a
//! deleted row as a change to zero. Only deals of the mode the strategy
//! trades in now (emulator or real) count. The sessions are not stored: at
//! start the closed deals are replayed in close order, so a minus session's
//! hold outlives a restart.

use std::collections::HashMap;

use crate::reports::Row;

/// The Sessions tab of one strategy (`IgnoreSession` off).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SessionRule {
    /// `SessionStratMin`, USDT, below zero (0 = no minus sessions).
    pub min: f64,
    /// `SessionStratMax`, USDT, above zero (0 = no plus sessions).
    pub max: f64,
    pub penalty_ms: i64,
    pub reset_on_minus: bool,
}

#[derive(Debug, Default)]
struct Acc {
    profit: f64,
    plus: u32,
    minus: u32,
    until: i64,
}

#[derive(Debug, Default)]
pub struct Guards {
    /// Profit of each closed deal already counted, by report row.
    counted: HashMap<i64, f64>,
    acc: HashMap<(u64, String), Acc>,
}

impl Guards {
    /// Rebuild from the report: every closed deal booked at its close time
    /// under `rule` (`None`: counted, not accumulated).
    pub fn replay<'a>(
        &mut self,
        rows: impl Iterator<Item = &'a Row>,
        rule: impl Fn(&Row) -> Option<SessionRule>,
    ) {
        *self = Self::default();
        let mut closed: Vec<&Row> = rows.filter(|r| r.closed && !r.deleted).collect();
        closed.sort_by_key(|r| (r.close_date, r.rec_id));
        for r in closed {
            self.book(r, rule(r), r.close_date * 1000);
        }
    }

    /// A report row changed: the change of its profit goes to the session of
    /// its strategy × market under `rule`. Returns a log line on a session.
    pub fn book(&mut self, row: &Row, rule: Option<SessionRule>, now: i64) -> Option<String> {
        if row.strategy_id == 0 || !row.closed || !row.profit.is_finite() {
            return None;
        }
        let profit = if row.deleted { 0.0 } else { row.profit };
        let before = self.counted.insert(row.rec_id, profit).unwrap_or(0.0);
        let delta = profit - before;
        let rule = rule?;
        if delta == 0.0 {
            return None;
        }
        let a = self
            .acc
            .entry((row.strategy_id, row.coin.clone()))
            .or_default();
        let was = a.profit;
        a.profit += delta;
        let label = format!("{}: session of strategy {}", row.coin, row.strategy_id);
        if rule.min < 0.0 && a.profit <= rule.min {
            a.minus += 1;
            a.plus = 0;
            a.profit = 0.0;
            a.until = now + rule.penalty_ms;
            return Some(format!(
                "{label}: minus session #{} ({:.2} USDT ≤ SessionStratMin {}), no entries for {} s",
                a.minus,
                was + delta,
                rule.min,
                rule.penalty_ms / 1000
            ));
        }
        if rule.max > 0.0 && a.profit >= rule.max {
            a.plus += 1;
            a.minus = 0;
            a.profit = 0.0;
            return Some(format!(
                "{label}: plus session #{} ({:.2} USDT ≥ SessionStratMax {})",
                a.plus,
                was + delta,
                rule.max
            ));
        }
        if rule.reset_on_minus && rule.min < 0.0 && was > 0.0 && -delta > rule.min.abs() / 10.0 {
            a.profit = 0.0;
            return Some(format!(
                "{label}: SessionResetOnMinus: a loss of {:.2} USDT zeroes the session profit {was:.2}",
                -delta
            ));
        }
        None
    }

    /// Strategy × market pairs held by a minus session, with the end of it.
    pub fn held(&self, now: i64) -> HashMap<(u64, String), i64> {
        self.acc
            .iter()
            .filter(|(_, a)| a.until > now)
            .map(|(k, a)| (k.clone(), a.until))
            .collect()
    }
}

/// What the strategy pass reads from the report.
#[derive(Debug, Default)]
pub struct View {
    /// Realized profit per strategy over the `TotalLoss` window.
    pub totals: HashMap<u64, f64>,
    /// Minus sessions: strategy × market held until (ms).
    pub sessions: HashMap<(u64, String), i64>,
    /// `PenaltyTime`: when a third loss in a row closed, per strategy ×
    /// market (ms).
    pub streaks: HashMap<(u64, String), i64>,
    /// `PenaltyTime`: the last manual deal per market × emulator mode (ms).
    pub manual: HashMap<(String, bool), i64>,
}

/// `PenaltyTime` marks from the report: per strategy × market the close of
/// every third loss in a row (a win breaks the run, a zero profit leaves
/// it; only deals that `count`), and per market × mode the last manual deal.
#[allow(clippy::type_complexity)]
pub fn penalty_marks<'a>(
    rows: impl Iterator<Item = &'a Row>,
    counts: impl Fn(&Row) -> bool,
) -> (HashMap<(u64, String), i64>, HashMap<(String, bool), i64>) {
    let mut closed: Vec<&Row> = Vec::new();
    let mut manual: HashMap<(String, bool), i64> = HashMap::new();
    for r in rows.filter(|r| !r.deleted) {
        // A hand trade: no strategy, or one routed to a Manual strategy (the
        // row keeps its kind name after the strategy is gone).
        if r.strategy_id == 0 || r.signal_type == crate::strategies::KIND_MANUAL.1 {
            let at = manual.entry((r.coin.clone(), r.emulator)).or_default();
            *at = (*at).max(r.buy_date * 1000);
        } else if r.closed && counts(r) {
            closed.push(r);
        }
    }
    closed.sort_by_key(|r| (r.close_date, r.rec_id));
    let mut runs: HashMap<(u64, String), (u32, i64)> = HashMap::new();
    for r in closed {
        let run = runs.entry((r.strategy_id, r.coin.clone())).or_default();
        if r.profit > 0.0 {
            run.0 = 0;
        } else if r.profit < 0.0 {
            run.0 += 1;
            if run.0 >= 3 {
                *run = (0, r.close_date * 1000);
            }
        }
    }
    let streaks = runs
        .into_iter()
        .filter(|(_, (_, at))| *at > 0)
        .map(|(k, (_, at))| (k, at))
        .collect();
    (streaks, manual)
}

/// Realized profit per strategy of the deals closed since `since_s` (USDT)
/// that `counts` (the strategy's current mode).
pub fn totals<'a>(
    rows: impl Iterator<Item = &'a Row>,
    since_s: i64,
    counts: impl Fn(&Row) -> bool,
) -> HashMap<u64, f64> {
    let mut out = HashMap::new();
    for r in rows.filter(|r| {
        r.strategy_id != 0 && r.closed && !r.deleted && r.close_date >= since_s && counts(r)
    }) {
        *out.entry(r.strategy_id).or_default() += r.profit;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(rec_id: i64, strategy_id: u64, coin: &str, profit: f64) -> Row {
        Row {
            rec_id,
            strategy_id,
            coin: coin.into(),
            profit,
            closed: true,
            close_date: 100 + rec_id,
            ..Row::default()
        }
    }

    const RULE: SessionRule = SessionRule {
        min: -100.0,
        max: 200.0,
        penalty_ms: 60_000,
        reset_on_minus: false,
    };

    #[test]
    fn minus_session_holds_its_strategy_on_its_market_only() {
        let mut g = Guards::default();
        assert_eq!(
            g.book(&row(1, 7, "BTCUSDT", -60.0), Some(RULE), 1_000),
            None
        );
        let log = g
            .book(&row(2, 7, "BTCUSDT", -50.0), Some(RULE), 2_000)
            .unwrap();
        assert!(
            log.contains("minus session #1") && log.contains("-110.00"),
            "{log}"
        );
        let held = g.held(2_000);
        assert_eq!(held.get(&(7, "BTCUSDT".into())), Some(&62_000));
        assert_eq!(held.len(), 1);
        assert!(g.held(62_000).is_empty());
        // The accumulator started over: another -60 is not a session.
        assert_eq!(
            g.book(&row(3, 7, "BTCUSDT", -60.0), Some(RULE), 3_000),
            None
        );
        // Other strategies, other markets, manual deals keep their own count.
        assert_eq!(
            g.book(&row(4, 8, "BTCUSDT", -90.0), Some(RULE), 3_000),
            None
        );
        assert_eq!(g.book(&row(5, 7, "GAZP", -90.0), Some(RULE), 3_000), None);
        assert_eq!(
            g.book(&row(6, 0, "BTCUSDT", -900.0), Some(RULE), 3_000),
            None
        );
    }

    #[test]
    fn a_late_fee_counts_its_difference_once() {
        let mut g = Guards::default();
        g.replay([row(1, 7, "BTCUSDT", -95.0)].iter(), |_| None);
        // The fee of a deal closed before the restart: -2 more, no session.
        assert_eq!(
            g.book(&row(1, 7, "BTCUSDT", -97.0), Some(RULE), 1_000),
            None
        );
        // A new deal: -97 is not counted again, -10 → -12 in total.
        assert_eq!(
            g.book(&row(2, 7, "BTCUSDT", -10.0), Some(RULE), 1_000),
            None
        );
        // Its fee brings the session to -100 exactly.
        let log = g
            .book(&row(2, 7, "BTCUSDT", -98.0), Some(RULE), 2_000)
            .unwrap();
        assert!(log.contains("minus session"), "{log}");
        // Repeated rows change nothing.
        assert_eq!(
            g.book(&row(2, 7, "BTCUSDT", -98.0), Some(RULE), 2_000),
            None
        );
    }

    #[test]
    fn plus_session_and_reset_on_minus() {
        let mut g = Guards::default();
        let log = g.book(&row(1, 7, "BTCUSDT", 250.0), Some(RULE), 0).unwrap();
        assert!(log.contains("plus session #1"), "{log}");
        let rule = SessionRule {
            reset_on_minus: true,
            ..RULE
        };
        g.book(&row(2, 7, "BTCUSDT", 50.0), Some(rule), 0);
        // A loss over 10 (a tenth of the minimum) zeroes the +50.
        let log = g.book(&row(3, 7, "BTCUSDT", -11.0), Some(rule), 0).unwrap();
        assert!(log.contains("SessionResetOnMinus"), "{log}");
        // From zero, -95 is still no minus session.
        assert_eq!(g.book(&row(4, 7, "BTCUSDT", -95.0), Some(rule), 0), None);
        // Without a rule (IgnoreSession) nothing is kept.
        assert_eq!(g.book(&row(5, 7, "BTCUSDT", -500.0), None, 0), None);
        assert!(g.held(0).is_empty());
    }

    #[test]
    fn replay_keeps_a_running_hold_and_deletion_undoes_a_deal() {
        let mut a = row(1, 7, "BTCUSDT", -60.0);
        a.close_date = 1_000;
        let mut b = row(2, 7, "BTCUSDT", -50.0);
        b.close_date = 1_010;
        let mut g = Guards::default();
        // Replayed in close order whatever the report order: the minus
        // session at 1 010 s holds SBER until 1 070 s.
        g.replay([b.clone(), a.clone()].iter(), |_| Some(RULE));
        assert_eq!(
            g.held(1_050_000).get(&(7, "BTCUSDT".into())),
            Some(&1_070_000)
        );
        assert!(g.held(1_070_000).is_empty());
        // A mode that does not count (`None`) accumulates nothing.
        g.replay([a.clone(), b.clone()].iter(), |_| None);
        assert!(g.held(1_050_000).is_empty());
        // Deleting a counted deal takes its loss back; restoring adds it again.
        let mut g = Guards::default();
        g.book(&a, Some(RULE), 0);
        a.deleted = true;
        assert_eq!(g.book(&a, Some(RULE), 0), None);
        assert_eq!(g.book(&b, Some(RULE), 0), None);
        a.deleted = false;
        assert!(g.book(&a, Some(RULE), 0).unwrap().contains("minus session"));
    }

    #[test]
    fn penalty_marks_stamp_the_third_loss_and_the_last_manual_deal() {
        let mut rows: Vec<Row> = [-5.0, -5.0, 0.0, 3.0, -1.0, -1.0, -1.0]
            .iter()
            .enumerate()
            .map(|(i, &p)| row(i as i64 + 1, 7, "BTCUSDT", p))
            .collect();
        let mut manual = row(20, 0, "GAZP", 0.0);
        manual.closed = false;
        manual.buy_date = 500;
        rows.push(manual);
        let (streaks, manual) = penalty_marks(rows.iter(), |_| true);
        // -5, -5, 0 (neutral), +3 breaks; then three losses: rows 5..7.
        assert_eq!(streaks.get(&(7, "BTCUSDT".into())), Some(&107_000));
        assert_eq!(manual.get(&("GAZP".into(), false)), Some(&500_000));
        // Deals of another mode do not count.
        let (streaks, _) = penalty_marks(rows.iter(), |_| false);
        assert!(streaks.is_empty());
    }

    #[test]
    fn totals_by_strategy_since() {
        let rows = [
            row(1, 7, "BTCUSDT", -50.0),
            row(2, 7, "GAZP", -70.0),
            row(3, 8, "BTCUSDT", 30.0),
            row(4, 0, "BTCUSDT", -500.0),
        ];
        let t = totals(rows.iter(), 102, |_| true);
        assert_eq!(t.get(&7), Some(&-70.0));
        assert_eq!(t.get(&8), Some(&30.0));
        assert_eq!(t.get(&0), None);
    }
}
