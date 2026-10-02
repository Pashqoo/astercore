//! One-off account setup: every market isolated, at the highest leverage the account's
//! brackets allow. Driven by `examples/set_isolated_max.rs`, never by the core itself.
//!
//! The plan is a pure function of what the exchange states per symbol (`positionRisk`'s row, the
//! brackets, the resting orders), so what a dry run prints is what an apply would send; only a
//! position or order that appears in between makes the exchange refuse a call.

use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Duration;

use crate::aster::json::{PositionRisk, SymbolBrackets};
use crate::aster::rest::{self, Rest};
use crate::aster::sign::Signer;

pub const ISOLATED: &str = "ISOLATED";
/// `-4046`: the margin type already is the one asked for.
const CODE_NO_NEED_TO_CHANGE: i64 = -4046;
/// `-1003`: too many requests.
const CODE_TOO_MANY_REQUESTS: i64 = -1003;
/// `-4047` (open orders) and `-4048` (open position): the exchange will not change the margin
/// type of a symbol that is in use. Not a failure of the run: the symbol is left as it is. These
/// are Binance's codes and have not been measured on Aster: any other refusal lands in `failed`.
const CODES_IN_USE: [i64; 2] = [-4047, -4048];
/// Between two calls: ~7 a second, a fraction of the 2400-weight minute the core shares.
pub const PACE: Duration = Duration::from_millis(150);

/// Why a symbol is left as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Block {
    /// A position is open: the exchange refuses a margin change, and a leverage change would move
    /// the liquidation of a live position.
    OpenPosition,
    /// An order is resting: refused by the exchange for the margin type, and the order was sized
    /// for the margin it has.
    OpenOrders,
    /// `positionRisk` sent no row: nothing is known of the symbol, so nothing is sent.
    NoRow,
    /// The brackets named no leverage: there is no maximum to set.
    NoBracket,
}

impl Block {
    pub fn text(self) -> &'static str {
        match self {
            Self::OpenPosition => "a position is open",
            Self::OpenOrders => "an order is resting",
            Self::NoRow => "no positionRisk row",
            Self::NoBracket => "no leverage bracket",
        }
    }
}

/// What one symbol needs. A symbol that needs nothing has no step and no block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub symbol: String,
    /// Set when something is to change but must not be touched now.
    pub block: Option<Block>,
    /// Margin type to be set to `ISOLATED`.
    pub margin: bool,
    /// Leverage to be set, to this figure.
    pub leverage: Option<i32>,
}

impl Plan {
    pub fn is_noop(&self) -> bool {
        self.block.is_none() && !self.margin && self.leverage.is_none()
    }
}

/// What one symbol needs, from its `positionRisk` row (`None` when the exchange sent none), its
/// brackets' maximum and whether an order rests on it.
pub fn plan(symbol: &str, row: Option<&PositionRisk>, max: Option<i32>, orders: bool) -> Plan {
    let mut out = Plan {
        symbol: symbol.to_string(),
        block: None,
        margin: false,
        leverage: None,
    };
    let Some(row) = row else {
        out.block = Some(Block::NoRow);
        return out;
    };
    let Some(max) = max.filter(|&m| m > 0) else {
        out.block = Some(Block::NoBracket);
        return out;
    };
    // A row that does not state its margin type is not assumed isolated.
    let isolated = row
        .margin_type
        .as_deref()
        .is_some_and(|t| t.eq_ignore_ascii_case(ISOLATED));
    let margin = !isolated;
    let leverage = (row.leverage != Some(max)).then_some(max);
    if !margin && leverage.is_none() {
        return out;
    }
    if row.amount != 0.0 {
        out.block = Some(Block::OpenPosition);
    } else if orders {
        out.block = Some(Block::OpenOrders);
    } else {
        out.margin = margin;
        out.leverage = leverage;
    }
    out
}

/// The plan for every symbol, in symbol order. Hedge mode gives two rows a symbol: the row that
/// holds a position (or else the first) speaks for it, which is the safe reading — an open leg
/// blocks the change. `ordered` names the symbols with a resting order.
pub fn plans(
    symbols: &[String],
    rows: &[PositionRisk],
    brackets: &[SymbolBrackets],
    ordered: &HashSet<String>,
) -> Vec<Plan> {
    let mut by_symbol: HashMap<&str, &PositionRisk> = HashMap::new();
    for r in rows {
        let slot = by_symbol.entry(r.symbol.as_str()).or_insert(r);
        if slot.amount == 0.0 && r.amount != 0.0 {
            *slot = r;
        }
    }
    let max: HashMap<&str, Option<i32>> = brackets
        .iter()
        .map(|b| (b.symbol.as_str(), b.max_leverage()))
        .collect();
    let mut out: Vec<Plan> = symbols
        .iter()
        .map(|s| {
            plan(
                s,
                by_symbol.get(s.as_str()).copied(),
                max.get(s.as_str()).copied().flatten(),
                ordered.contains(s),
            )
        })
        .collect();
    out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    out
}

#[derive(Debug, Default)]
pub struct Report {
    pub margin_set: usize,
    pub leverage_set: usize,
    /// Symbols left as they were, with the reason. A rerun when they are flat settles them.
    pub skipped: Vec<(String, String)>,
    /// Calls the exchange refused for another reason, or that did not get through.
    pub failed: Vec<(String, String)>,
    /// The run stopped on a rate limit or a ban: what is left was not sent.
    pub aborted: Option<String>,
}

/// The exchange is limiting or banning this IP: a reason to stop, not to go on at 7 calls a
/// second — the core shares the IP and its orders would stand behind the same limit.
fn is_limit(e: &rest::Error) -> bool {
    matches!(e, rest::Error::Api { status, code, .. }
        if matches!(*status, 418 | 429) || *code == CODE_TOO_MANY_REQUESTS)
}

/// Sends the plans, one call at a time, [`PACE`] apart. A refusal of one symbol is recorded and
/// the run goes on; the leverage of a symbol goes out only when its margin is isolated. A rate
/// limit or a ban ends the run. The signer's clock is the caller's to have measured.
pub fn apply(rest: &mut Rest, signer: &mut Signer, plans: &[Plan]) -> Report {
    let mut report = Report::default();
    let mut calls = 0usize;
    let total = plans.iter().filter(|p| !p.is_noop()).count();
    let mut done = 0usize;
    'symbols: for p in plans {
        if p.is_noop() {
            continue;
        }
        done += 1;
        if done.is_multiple_of(25) {
            eprintln!("  {done}/{total} symbols");
        }
        if let Some(block) = p.block {
            report.skipped.push((p.symbol.clone(), block.text().into()));
            continue;
        }
        let mut pause = || {
            if calls > 0 {
                thread::sleep(PACE);
            }
            calls += 1;
        };
        if p.margin {
            pause();
            match rest.set_margin_type(signer, &p.symbol, ISOLATED) {
                Ok(()) => report.margin_set += 1,
                // Already isolated: what was asked for.
                Err(rest::Error::Api { code, .. }) if code == CODE_NO_NEED_TO_CHANGE => {}
                Err(e) if is_limit(&e) => {
                    report.aborted = Some(format!("{}: margin: {e}", p.symbol));
                    break 'symbols;
                }
                Err(rest::Error::Api { code, msg, .. }) if CODES_IN_USE.contains(&code) => {
                    report
                        .skipped
                        .push((p.symbol.clone(), format!("margin refused {code}: {msg}")));
                    continue;
                }
                Err(e) => {
                    report
                        .failed
                        .push((p.symbol.clone(), format!("margin: {e}")));
                    continue;
                }
            }
        }
        if let Some(max) = p.leverage {
            pause();
            match rest.set_leverage(signer, &p.symbol, max) {
                Ok(done) if done.leverage == max => report.leverage_set += 1,
                Ok(done) => report.failed.push((
                    p.symbol.clone(),
                    format!(
                        "leverage: asked {max}x, the exchange holds {}x",
                        done.leverage
                    ),
                )),
                Err(e) if is_limit(&e) => {
                    report.aborted = Some(format!("{}: leverage: {e}", p.symbol));
                    break 'symbols;
                }
                Err(e) => report
                    .failed
                    .push((p.symbol.clone(), format!("leverage: {e}"))),
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str, amount: f64, margin: Option<&str>, leverage: Option<i32>) -> PositionRisk {
        serde_json::from_value(serde_json::json!({
            "symbol": symbol,
            "positionAmt": amount.to_string(),
            "entryPrice": "0",
            "unRealizedProfit": "0",
            "marginType": margin,
            "leverage": leverage.map(|l| l.to_string()),
        }))
        .unwrap()
    }

    fn brackets(symbol: &str, levs: &[i32]) -> SymbolBrackets {
        serde_json::from_value(serde_json::json!({
            "symbol": symbol,
            "brackets": levs.iter().map(|l| serde_json::json!({"initialLeverage": l})).collect::<Vec<_>>(),
        }))
        .unwrap()
    }

    #[test]
    fn a_flat_cross_symbol_gets_both_steps() {
        let r = row("BTCUSDT", 0.0, Some("CROSSED"), Some(20));
        let p = plan("BTCUSDT", Some(&r), Some(200), false);
        assert_eq!((p.block, p.margin, p.leverage), (None, true, Some(200)));
    }

    #[test]
    fn what_is_already_right_needs_nothing_even_with_a_position() {
        let r = row("BTCUSDT", 0.0, Some("isolated"), Some(200));
        assert!(plan("BTCUSDT", Some(&r), Some(200), false).is_noop());
        let held = row("BTCUSDT", 1.0, Some("ISOLATED"), Some(200));
        assert!(plan("BTCUSDT", Some(&held), Some(200), true).is_noop());
    }

    #[test]
    fn an_open_position_or_order_blocks_both_steps() {
        let r = row("BTCUSDT", -0.5, Some("CROSSED"), Some(20));
        let p = plan("BTCUSDT", Some(&r), Some(200), false);
        assert_eq!(
            (p.block, p.margin, p.leverage),
            (Some(Block::OpenPosition), false, None)
        );
        let flat = row("BTCUSDT", 0.0, Some("ISOLATED"), Some(20));
        let p = plan("BTCUSDT", Some(&flat), Some(200), true);
        assert_eq!(
            (p.block, p.margin, p.leverage),
            (Some(Block::OpenOrders), false, None)
        );
    }

    #[test]
    fn nothing_is_sent_for_what_is_not_known() {
        assert_eq!(
            plan("NEWUSDT", None, Some(5), false).block,
            Some(Block::NoRow)
        );
        let r = row("X", 0.0, Some("CROSSED"), Some(5));
        for max in [None, Some(0)] {
            assert_eq!(
                plan("X", Some(&r), max, false).block,
                Some(Block::NoBracket)
            );
        }
        // A row that does not state its margin type is not assumed isolated.
        let r = row("X", 0.0, None, Some(5));
        assert!(plan("X", Some(&r), Some(5), false).margin);
    }

    #[test]
    fn plans_take_the_highest_bracket_and_the_leg_that_holds_a_position() {
        let symbols = vec!["B".to_string(), "A".to_string(), "C".to_string()];
        let rows = [
            row("A", 0.0, Some("CROSSED"), Some(5)),
            row("A", 2.0, Some("CROSSED"), Some(5)),
            row("B", 0.0, Some("ISOLATED"), Some(75)),
            row("C", 0.0, Some("CROSSED"), Some(5)),
        ];
        let b = [
            brackets("A", &[20, 100, 50]),
            brackets("B", &[75]),
            brackets("C", &[10]),
        ];
        let ordered: HashSet<String> = ["C".to_string()].into();
        let got = plans(&symbols, &rows, &b, &ordered);
        assert_eq!(got[0].symbol, "A", "symbol order");
        assert_eq!(got[0].block, Some(Block::OpenPosition));
        assert!(got[1].is_noop());
        assert_eq!(got[2].block, Some(Block::OpenOrders));
    }

    #[test]
    fn a_rate_limit_or_a_ban_is_told_from_an_ordinary_refusal() {
        let api = |status, code| rest::Error::Api {
            status,
            code,
            msg: String::new(),
        };
        assert!(is_limit(&api(429, 0)));
        assert!(is_limit(&api(418, 0)));
        assert!(is_limit(&api(400, -1003)));
        assert!(!is_limit(&api(400, -4047)));
        assert!(!is_limit(&rest::Error::Transport("x".into())));
    }
}
