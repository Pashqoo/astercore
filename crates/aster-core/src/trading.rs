//! Aster order reports normalized to one shape, [`OrderUpdate`], whatever they
//! came from: the reply to a `POST`/`DELETE`/`GET /fapi/v3/order`, a row of
//! `openOrders`, or an `ORDER_TRADE_UPDATE` of the user-data stream. The order
//! model (`orders.rs`, ported from TInvestCore) reads only this shape, so it
//! never sees which of them a report was.
//!
//! Quantities are in lots — whole `stepSize` steps of the market — the unit the
//! model counts in (`Market::lot`).

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crate::aster::json::{OrderEvent, OrderReply};
use crate::aster::rest::{self, OrderRef, Rest};
use crate::aster::sign::Signer;
use crate::feed::FeedEvent;
use crate::model::on_grid;
use crate::orders::{Action, Leg, Op};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl ExecStatus {
    /// An Aster order status (`X` of the stream, `status` of a reply). An
    /// order the exchange expired (an IOC/GTX limit that did not rest, a
    /// market order's unfilled rest) ended without more fills, which is what
    /// a cancel means to the model. `NEW_INSURANCE` and `NEW_ADL` are the
    /// exchange's liquidation orders, never one of the core's; `None`.
    pub fn from_aster(status: &str) -> Option<Self> {
        Some(match status {
            "NEW" => Self::New,
            "PARTIALLY_FILLED" => Self::PartiallyFilled,
            "FILLED" => Self::Filled,
            "CANCELED" | "EXPIRED" => Self::Cancelled,
            "REJECTED" => Self::Rejected,
            _ => return None,
        })
    }

    pub fn is_final(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    /// `orderId`, as a string.
    pub exchange_id: String,
    /// `clientOrderId`: the key the core generated; may be another client's.
    pub request_id: String,
    /// The symbol.
    pub uid: String,
    pub status: ExecStatus,
    pub sell: bool,
    pub is_market: bool,
    pub lots_requested: i64,
    pub lots_executed: i64,
    /// Order price and average fill price (0 when unknown).
    pub price: f64,
    pub avg_price: f64,
    /// The reply to the core's own call, not a stream report: a stream
    /// rejection may precede the reply that settles the call.
    pub unary: bool,
    pub time_ms: i64,
    pub message: String,
}

/// The exchange refused the call for good: the request was read and turned
/// down, so it did not and will not take effect. A 5xx, a timeout or a
/// transport error leaves its fate unknown.
pub(crate) fn definitive(e: &rest::Error) -> bool {
    matches!(e, rest::Error::Api { status, .. } if (400..500).contains(status) && *status != 408)
}

/// The order worker's name in `FeedEvent::Lost`.
pub const WORKER: &str = "the order worker";

/// Pause between order calls: at most 10 a second, a fifth of the 300 per
/// 10 s that the `ORDERS` limit of `exchangeInfo` allows, so one burst of
/// cancels cannot spend the account's budget.
const ORDER_PACE: Duration = Duration::from_millis(100);

/// The market grid a call is written on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grid {
    /// `stepSize`: one lot.
    pub step: f64,
    /// `tickSize`.
    pub tick: f64,
}

pub enum TradeCommand {
    /// Exchange work of the order model on market `uid` (the symbol).
    Exchange {
        action: Action,
        uid: String,
        grid: Grid,
    },
    /// Read the account's live orders → `TradingEvent::OpenOrders`.
    OpenOrders,
}

pub enum TradingEvent {
    Order(OrderUpdate),
    Failed {
        action: Action,
        definitive: bool,
        msg: String,
    },
    /// The account's live orders: on start and after every reopening of the
    /// user-data stream, whose events in between may be lost.
    OpenOrders(Vec<OrderUpdate>),
    /// Round trip of the last call of an action, ms; a call that never
    /// answered reads as the call timeout.
    Ping(i64),
}

/// The order worker: a thread named `aster-orders` that makes the model's
/// exchange calls one at a time and reports each outcome as `FeedEvent::Trading`.
/// Its signer is a clone of the account's (one nonce sequence for the wallet).
/// Its clock is measured against the gateway every
/// [`CLOCK_EVERY`](crate::account::CLOCK_EVERY) and before the next call after
/// a failure, as the account reader's is: a nonce outside the gateway's
/// ±60 s is the failure a sleeping Mac makes. `grids` reads the open orders of
/// any market. A panic is sent as [`FeedEvent::Lost`]: exits that never go
/// out are worse than a core that leaves.
pub fn start(
    mut rest: Rest,
    mut signer: Signer,
    grids: HashMap<String, Grid>,
    ev: Sender<FeedEvent>,
) -> Sender<TradeCommand> {
    let (tx, rx) = mpsc::channel::<TradeCommand>();
    thread::Builder::new()
        .name("aster-orders".into())
        .spawn(move || {
            let lost = ev.clone();
            let run = panic::catch_unwind(AssertUnwindSafe(move || {
                let send = |e: TradingEvent| ev.send(FeedEvent::Trading(e)).is_ok();
                let mut clock_due = Instant::now();
                while let Ok(cmd) = rx.recv() {
                    if Instant::now() >= clock_due {
                        match rest.sync_clock() {
                            Ok(delta) => {
                                log::debug!("orders: clock delta {delta} ms");
                                clock_due = Instant::now() + crate::account::CLOCK_EVERY;
                            }
                            Err(e) => log::warn!("orders: clock: {e}"),
                        }
                    }
                    let (action, uid, grid) = match cmd {
                        TradeCommand::Exchange { action, uid, grid } => (action, uid, grid),
                        TradeCommand::OpenOrders => {
                            match rest.open_orders(&mut signer) {
                                Ok(rows) => {
                                    let list = rows
                                        .iter()
                                        .filter_map(|r| {
                                            let step = grids.get(&r.symbol)?.step;
                                            OrderUpdate::from_reply(r, step, true)
                                        })
                                        .collect();
                                    if !send(TradingEvent::OpenOrders(list)) {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    log::warn!("orders: open orders: {e}");
                                    clock_due = Instant::now();
                                }
                            }
                            continue;
                        }
                    };
                    let (order, leg, op) = describe(&action);
                    log::debug!("{action:?}");
                    let mut done = execute(&mut rest, &mut signer, &action, &uid, grid);
                    if done.code == Some(CODE_NONCE_EXPIRED) && done.reports.is_empty() {
                        log::warn!("orders: request outside the time window, clock measured again");
                        match rest.sync_clock() {
                            Ok(_) => {
                                clock_due = Instant::now() + crate::account::CLOCK_EVERY;
                                done = execute(&mut rest, &mut signer, &action, &uid, grid);
                            }
                            Err(e) => log::warn!("orders: clock: {e}"),
                        }
                    }
                    let rtt = rest.last_rtt().unwrap_or(rest::CALL_TIMEOUT).as_millis() as i64;
                    send(TradingEvent::Ping(rtt));
                    for u in done.reports {
                        log::debug!("{op:?} order {order:#x} {leg:?}: {u:?}");
                        send(TradingEvent::Order(u));
                    }
                    if let Some((definitive, msg)) = done.failed {
                        if done.code == Some(CODE_REDUCE_ONLY) {
                            log::error!(
                                "{op:?} order {order:#x} {leg:?}: {msg} — the exit's reduce-only \
                                 was refused: the core and the account disagree about the position"
                            );
                        } else {
                            log::warn!("{op:?} order {order:#x} {leg:?}: {msg}");
                        }
                        clock_due = Instant::now();
                        let failed = TradingEvent::Failed {
                            action,
                            definitive,
                            msg,
                        };
                        if !send(failed) {
                            return;
                        }
                    }
                    thread::sleep(ORDER_PACE);
                }
            }));
            if run.is_err() {
                let _ = lost.send(FeedEvent::Lost(WORKER));
            }
        })
        .expect("spawn");
    tx
}

fn describe(a: &Action) -> (u64, Leg, Op) {
    match a {
        Action::Post { order, leg, .. } => (*order, *leg, Op::Post),
        Action::Cancel { order, leg, .. } => (*order, *leg, Op::Cancel),
        Action::Replace { order, leg, .. } => (*order, *leg, Op::Replace),
        Action::Query { order, leg, .. } | Action::QueryRequest { order, leg, .. } => {
            (*order, *leg, Op::Query)
        }
    }
}

/// What one action's calls brought back: the reports, in order, and the
/// failure that ended it, if one did — `(definitive, message)`. Both can be
/// there: a `Replace` whose cancel went through and whose post did not.
#[derive(Debug, Default)]
struct Done {
    reports: Vec<OrderUpdate>,
    failed: Option<(bool, String)>,
    /// The exchange's code of that failure, when it gave one.
    code: Option<i64>,
}

impl Done {
    fn fail(mut self, e: &rest::Error) -> Self {
        self.failed = Some((definitive(e), e.to_string()));
        if let rest::Error::Api { code, .. } = e {
            self.code = Some(*code);
        }
        self
    }
}

/// `-4225 Nonce Expired`: the request's nonce was outside the gateway's
/// window (docs, v3 «Nonce Mechanism» and its example answer). It was not
/// carried out, so it is made again once the clock is measured anew. That
/// helps a clock that fell behind; a sequence pushed ahead of the gateway
/// stays ahead (`Signer::next_nonce` never steps back), and the retry then
/// fails the same way and is reported.
const CODE_NONCE_EXPIRED: i64 = -4225;
/// `-2022 REDUCE_ONLY_REJECT`: an exit refused for its reduce-only flag — the
/// core's model and the account disagree about the position, which is a
/// defect to look into, not a market condition (`PLAN.md`, error codes).
const CODE_REDUCE_ONLY: i64 = -2022;

/// One action's exchange calls.
///
/// A `Replace` is a cancel and a post under the new key: Aster has no
/// replace that takes a new key. The cancel's report goes first, so fills of
/// the old order are counted. When the cancel's answer shows fills the model
/// had not counted (`filled`), the new order is not posted: its lots were
/// sized without them, and posting would buy past the budget. The `Replace`
/// then fails, and the model falls back to the old order, whose final report
/// it has (`Orders::failed`).
fn execute(rest: &mut Rest, signer: &mut Signer, a: &Action, uid: &str, grid: Grid) -> Done {
    let mut done = Done::default();
    let report = |done: &mut Done, r: &OrderReply, key: Option<&str>| -> bool {
        match OrderUpdate::from_reply(r, grid.step, true) {
            Some(mut u) => {
                if let (true, Some(key)) = (u.request_id.is_empty(), key) {
                    u.request_id = key.to_owned();
                }
                done.reports.push(u);
                true
            }
            None => {
                done.failed = Some((false, format!("unreadable order status {:?}", r.status)));
                false
            }
        }
    };
    match a {
        Action::Post {
            key,
            leg,
            lots,
            price,
            sell,
            ..
        } => {
            let params = post_params(uid, *leg, key, *lots, *price, *sell, grid);
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match rest.new_order(signer, &params) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Replace {
            leg,
            exchange_id,
            key,
            lots,
            price,
            filled,
            ..
        } => {
            let old = match rest.cancel_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(old) => old,
                Err(e) => return done.fail(&e),
            };
            if !report(&mut done, &old, None) {
                return done;
            }
            let old_filled = done.reports[0].lots_executed;
            if old_filled > *filled {
                done.failed = Some((
                    true,
                    format!(
                        "{} lot(s) filled while it was being replaced; the new order is not placed",
                        old_filled - filled
                    ),
                ));
                return done;
            }
            let sell = old.side == "SELL";
            let params = post_params(uid, *leg, key, *lots, Some(*price), sell, grid);
            let params: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match rest.new_order(signer, &params) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Cancel { exchange_id, .. } => {
            match rest.cancel_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(r) => {
                    report(&mut done, &r, None);
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::QueryRequest { key, .. } => {
            match rest.query_order(signer, uid, OrderRef::Key(key)) {
                Ok(r) => {
                    report(&mut done, &r, Some(key));
                }
                Err(e) => return done.fail(&e),
            }
        }
        Action::Query { exchange_id, .. } => {
            match rest.query_order(signer, uid, OrderRef::Id(exchange_id)) {
                Ok(r) => {
                    report(&mut done, &r, None);
                }
                Err(e) => return done.fail(&e),
            }
        }
    }
    done
}

/// The parameters of a new order: a GTC limit at `price`, or MARKET without
/// one. Every exit is `reduceOnly` (`PLAN.md`, «Ордера»): one-way mode, so an
/// exit cannot turn the position over, whatever fills before it.
fn post_params(
    uid: &str,
    leg: Leg,
    key: &str,
    lots: i64,
    price: Option<f64>,
    sell: bool,
    grid: Grid,
) -> Vec<(&'static str, String)> {
    let mut p = vec![
        ("symbol", uid.to_owned()),
        ("side", if sell { "SELL" } else { "BUY" }.to_owned()),
        (
            "type",
            if price.is_some() { "LIMIT" } else { "MARKET" }.to_owned(),
        ),
        ("quantity", on_grid(lots as f64 * grid.step, grid.step)),
    ];
    if let Some(price) = price {
        p.push(("timeInForce", "GTC".to_owned()));
        p.push(("price", on_grid(price, grid.tick)));
    }
    if leg == Leg::Sell {
        p.push(("reduceOnly", "true".to_owned()));
    }
    p.push(("newClientOrderId", key.to_owned()));
    // The final state of a MARKET order, not only its acceptance: the model
    // learns the fill from the reply, not seconds later from the stream.
    p.push(("newOrderRespType", "RESULT".to_owned()));
    p
}

impl OrderUpdate {
    /// From an `ORDER_TRADE_UPDATE` of the user-data stream; quantities in
    /// lots of `step`. `None` for a status the model does not model (the
    /// exchange's own liquidation orders), without a grid to count lots on,
    /// and for a quantity or price that does not read: a fill read as zero
    /// would be a fill never counted, and the next reply or reconciliation
    /// carries the order's true state.
    pub fn from_event(o: &OrderEvent, step: f64) -> Option<Self> {
        if step.is_nan() || step <= 0.0 {
            return None;
        }
        let num = |s: &str| s.parse::<f64>().ok().filter(|v| v.is_finite());
        let lots = |q: f64| (q / step).round() as i64;
        Some(Self {
            exchange_id: o.id.to_string(),
            request_id: o.client_id.clone(),
            uid: o.symbol.clone(),
            status: ExecStatus::from_aster(&o.status)?,
            sell: o.side == "SELL",
            is_market: o.kind == "MARKET",
            lots_requested: lots(num(&o.qty)?),
            lots_executed: lots(num(&o.filled)?),
            price: num(&o.price)?,
            avg_price: if o.avg_price.is_empty() {
                0.0
            } else {
                num(&o.avg_price)?
            },
            unary: false,
            time_ms: o.time_ms,
            message: String::new(),
        })
    }

    /// From an order reply; quantities in lots of `step`. `None` for a status
    /// the model does not model and without a grid: an order read as live
    /// that is not would be kept and acted on.
    pub fn from_reply(r: &OrderReply, step: f64, unary: bool) -> Option<Self> {
        if step.is_nan() || step <= 0.0 {
            return None;
        }
        let lots = |q: f64| (q / step).round() as i64;
        Some(Self {
            exchange_id: r.order_id.to_string(),
            request_id: r.client_order_id.clone(),
            uid: r.symbol.clone(),
            status: ExecStatus::from_aster(&r.status)?,
            sell: r.side == "SELL",
            is_market: r.kind == "MARKET",
            lots_requested: lots(r.orig_qty),
            lots_executed: lots(r.executed_qty),
            price: r.price,
            avg_price: r.avg_price,
            unary,
            time_ms: r.update_ms,
            message: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRID: Grid = Grid {
        step: 0.001,
        tick: 0.1,
    };

    fn params(p: &[(&'static str, String)]) -> String {
        p.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    #[test]
    fn an_entry_limit_and_a_market_exit_are_written_on_the_grid() {
        let entry = post_params(
            "BTCUSDT",
            Leg::Buy,
            "k1",
            16,
            Some(84_249.349_9),
            false,
            GRID,
        );
        assert_eq!(
            params(&entry),
            "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=0.016&timeInForce=GTC&\
             price=84249.3&newClientOrderId=k1&newOrderRespType=RESULT"
        );
        let exit = post_params("BTCUSDT", Leg::Sell, "k2", 7, None, true, GRID);
        assert_eq!(
            params(&exit),
            "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.007&reduceOnly=true&\
             newClientOrderId=k2&newOrderRespType=RESULT"
        );
        // A short's exit buys back, and is reduce-only all the same.
        let cover = post_params("BTCUSDT", Leg::Sell, "k3", 1, Some(80_000.0), false, GRID);
        assert!(
            params(&cover).contains("side=BUY&type=LIMIT")
                && params(&cover).contains("reduceOnly=true")
        );
    }

    #[test]
    fn a_reply_becomes_a_report_in_lots() {
        // The docs' reply shape, with the order's numbers.
        let r: OrderReply = serde_json::from_str(
            r#"{"clientOrderId":"k1","cumQty":"0","cumQuote":"0","executedQty":"0.007",
                "orderId":22542179,"avgPrice":"84250.1","origQty":"0.016","price":"84249.3",
                "reduceOnly":false,"side":"BUY","positionSide":"BOTH","status":"PARTIALLY_FILLED",
                "stopPrice":"0","closePosition":false,"symbol":"BTCUSDT","timeInForce":"GTC",
                "type":"LIMIT","origType":"LIMIT","updateTime":1566818724722,
                "workingType":"CONTRACT_PRICE","priceProtect":false}"#,
        )
        .unwrap();
        let u = OrderUpdate::from_reply(&r, GRID.step, true).unwrap();
        assert_eq!(
            (
                u.exchange_id.as_str(),
                u.request_id.as_str(),
                u.uid.as_str()
            ),
            ("22542179", "k1", "BTCUSDT")
        );
        assert_eq!(
            (
                u.status,
                u.lots_requested,
                u.lots_executed,
                u.sell,
                u.is_market
            ),
            (ExecStatus::PartiallyFilled, 16, 7, false, false)
        );
        assert_eq!((u.price, u.avg_price), (84_249.3, 84_250.1));
    }

    #[test]
    fn a_stream_report_becomes_a_report_and_an_unreadable_one_does_not() {
        let mut o = OrderEvent {
            symbol: "BTCUSDT".into(),
            client_id: "k1".into(),
            id: 8886774,
            side: "SELL".into(),
            kind: "MARKET".into(),
            execution: "TRADE".into(),
            status: "FILLED".into(),
            qty: "0.016".into(),
            price: "0".into(),
            filled: "0.016".into(),
            last_price: "84250.1".into(),
            avg_price: "84250.1".into(),
            time_ms: 1568879465651,
        };
        let u = OrderUpdate::from_event(&o, GRID.step).unwrap();
        assert_eq!(
            (
                u.status,
                u.lots_requested,
                u.lots_executed,
                u.sell,
                u.is_market,
                u.unary
            ),
            (ExecStatus::Filled, 16, 16, true, true, false)
        );
        assert_eq!((u.exchange_id.as_str(), u.avg_price), ("8886774", 84_250.1));
        o.filled = "abc".into();
        assert!(OrderUpdate::from_event(&o, GRID.step).is_none());
        o.filled = "0.016".into();
        o.status = "NEW_ADL".into();
        assert!(OrderUpdate::from_event(&o, GRID.step).is_none());
    }

    #[test]
    fn an_expired_order_is_a_cancel_to_the_model() {
        assert_eq!(
            ExecStatus::from_aster("EXPIRED"),
            Some(ExecStatus::Cancelled)
        );
        assert_eq!(ExecStatus::from_aster("NEW_ADL"), None);
    }

    #[test]
    fn a_refusal_is_definitive_and_a_timeout_is_not() {
        let api = |status| rest::Error::Api {
            status,
            code: -2019,
            msg: String::new(),
        };
        assert!(definitive(&api(400)));
        assert!(definitive(&api(429)));
        assert!(!definitive(&api(408)));
        assert!(!definitive(&api(503)));
        assert!(!definitive(&rest::Error::Transport("timeout".into())));
    }
}
