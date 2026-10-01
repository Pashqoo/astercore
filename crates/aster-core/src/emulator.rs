//! MoonBot's trade emulator: the exchange of emulated core orders. Their
//! actions are answered here instead of by the exchange, with the reports
//! `trading` would send. A marketable order fills when placed, at the best
//! quote; a resting one fills in full once the market reaches its price — an
//! exchange trade at it or through it, or the opposite best quote — at its
//! own limit. Like MoonBot's, it sees no ping, no queue and no book depth.
//! Nothing is kept here: every emulated order lives in `Orders` and its
//! store, so a restart resumes them as it does real ones. Ported from
//! TInvestCore.

use crate::model::Market;
use crate::orders::{Action, EmuOrder, Orders, CODE_ORDER_NOT_FOUND};
use crate::trading::{ExecStatus, OrderUpdate, TradingEvent};

/// Emulator exchange ids are numeric like Aster's `orderId` (the core moves
/// and cancels a leg by a numeric id only) and far above them.
const ID_BASE: i64 = 7_000_000_000_000_000_000;

pub struct Emulator {
    next_id: i64,
}

impl Emulator {
    /// Ids grow from the start time, so a restart never repeats one.
    pub fn starting_at(now_ms: i64) -> Self {
        Self {
            next_id: ID_BASE + now_ms.max(0) * 1000,
        }
    }

    /// The exchange's answer to `action` on an emulated order of `orders`.
    pub fn answer(
        &mut self,
        action: Action,
        orders: &Orders,
        market: Option<&Market>,
        now_ms: i64,
    ) -> TradingEvent {
        let known = match &action {
            Action::Post {
                key,
                uid,
                lots,
                price,
                sell,
                ..
            } => return self.place(key, uid, *lots, *price, *sell, market, now_ms),
            Action::Replace {
                order,
                leg,
                exchange_id,
                key,
                uid,
                lots,
                price,
                ..
            } => match orders.emu_order(*order, *leg, exchange_id) {
                // Same direction as the order it replaces.
                Some(old) => {
                    return self.place(key, uid, *lots, Some(*price), old.sell, market, now_ms)
                }
                None => None,
            },
            Action::Cancel {
                order,
                leg,
                exchange_id,
            } => orders.emu_order(*order, *leg, exchange_id).map(|e| {
                let status = e.status.filter(|s| s.is_final());
                report(&e, status.unwrap_or(ExecStatus::Cancelled), now_ms)
            }),
            Action::Query {
                order,
                leg,
                exchange_id,
            } => orders
                .emu_order(*order, *leg, exchange_id)
                .map(|e| report(&e, e.status.unwrap_or(ExecStatus::New), now_ms)),
            Action::QueryRequest { order, leg, key } => {
                match orders.emu_order(*order, *leg, key) {
                    // Never answered (a restart cut it off): placed now.
                    Some(e) if e.exchange_id.is_empty() => {
                        let price = (e.price > 0.0).then_some(e.price);
                        let lots = e.lots - e.filled;
                        return self.place(&e.key, &e.uid, lots, price, e.sell, market, now_ms);
                    }
                    Some(e) => Some(report(&e, e.status.unwrap_or(ExecStatus::New), now_ms)),
                    None => None,
                }
            }
        };
        match known {
            Some(u) => TradingEvent::Order(u),
            None => TradingEvent::Failed {
                action,
                definitive: true,
                msg: format!("{CODE_ORDER_NOT_FOUND}: the emulator knows no such order"),
            },
        }
    }

    /// A new emulated exchange order: filled at once at the best quote when
    /// marketable (a market order always is), resting otherwise.
    #[allow(clippy::too_many_arguments)]
    fn place(
        &mut self,
        key: &str,
        uid: &str,
        lots: i64,
        limit: Option<f64>,
        sell: bool,
        market: Option<&Market>,
        now_ms: i64,
    ) -> TradingEvent {
        let quote = market.map_or(0.0, |m| best(m, sell));
        let fill = match limit {
            None => (quote > 0.0).then_some(quote),
            Some(p) => (quote > 0.0 && crosses(sell, quote, p)).then_some(quote),
        };
        let (status, message) = match (limit, fill) {
            (None, None) => (ExecStatus::Rejected, "emulator: no market price".to_owned()),
            (_, Some(_)) => (ExecStatus::Filled, String::new()),
            (Some(_), None) => (ExecStatus::New, String::new()),
        };
        self.next_id += 1;
        TradingEvent::Order(OrderUpdate {
            exchange_id: self.next_id.to_string(),
            request_id: key.to_owned(),
            uid: uid.to_owned(),
            status,
            sell,
            is_market: limit.is_none(),
            lots_requested: lots,
            lots_executed: if fill.is_some() { lots } else { 0 },
            price: limit.or(fill).unwrap_or(0.0),
            avg_price: fill.unwrap_or(0.0),
            unary: true,
            time_ms: now_ms,
            message,
        })
    }
}

/// Fills of the emulated orders resting on `m` after an exchange trade at
/// `trade` (`None`: the book moved): each in full at its own limit.
pub fn fills(
    resting: &[EmuOrder],
    m: &Market,
    trade: Option<f64>,
    now_ms: i64,
) -> Vec<OrderUpdate> {
    resting
        .iter()
        .filter(|e| e.price > 0.0)
        .filter(|e| {
            // The opposite best quote: a buy fills when the ask comes down to it.
            let quote = if e.sell { m.bid_px() } else { m.ask_px() };
            trade.is_some_and(|t| t > 0.0 && crosses(e.sell, t, e.price))
                || (quote > 0.0 && crosses(e.sell, quote, e.price))
        })
        .map(|e| {
            // Never partly filled before: the emulator fills only in full.
            let mut u = report(e, ExecStatus::Filled, now_ms);
            u.lots_executed = e.lots;
            u.avg_price = e.price;
            u.unary = false;
            u
        })
        .collect()
}

/// `price` is at or through `limit` for the side: a sell gets at least it,
/// a buy pays at most it.
fn crosses(sell: bool, price: f64, limit: f64) -> bool {
    if sell {
        price >= limit
    } else {
        price <= limit
    }
}

/// The price a new order of the side meets: the opposite best quote, else
/// the last trade.
fn best(m: &Market, sell: bool) -> f64 {
    let side = if sell { m.bid_px() } else { m.ask_px() };
    if side > 0.0 {
        side
    } else {
        m.last()
    }
}

/// The current state of `e` as a unary reply with `status`.
fn report(e: &EmuOrder, status: ExecStatus, now_ms: i64) -> OrderUpdate {
    OrderUpdate {
        exchange_id: e.exchange_id.clone(),
        request_id: e.key.clone(),
        uid: e.uid.clone(),
        status,
        sell: e.sell,
        is_market: false,
        lots_requested: e.lots,
        lots_executed: e.filled,
        price: e.price,
        avg_price: e.mean,
        unary: true,
        time_ms: now_ms,
        message: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use moonproto::server::codec::trade::{status, StartOrder};

    use super::*;
    use crate::model::{fixtures, Catalog};
    use crate::orders::Leg;

    type Model = Catalog;

    fn model() -> Model {
        let mut model = fixtures::sber_catalog();
        let m = model.at_mut(1).unwrap();
        (m.last_price, m.bid, m.ask) = (Some(300.0), Some(299.9), Some(300.1));
        model
    }

    /// One emulated 1-lot long entry at `price` (0 = market).
    fn start(orders: &mut Orders, model: &Model, price: f64) -> (u64, Vec<Action>) {
        let s = StartOrder {
            market: "SBER".into(),
            is_short: false,
            use_market_stop: false,
            strategy_id: 0,
            size: 3100.0,
            price,
            planned_sell: 0.0,
            stops: None,
        };
        let fx = orders.start(0, &s, model.at(1).unwrap(), 1);
        orders.set_emulator(fx.changed[0]);
        (fx.changed[0], fx.actions)
    }

    /// Answer `actions` and apply the answers, as the engine does.
    fn run(emu: &mut Emulator, orders: &mut Orders, model: &Model, actions: Vec<Action>) {
        for a in actions {
            match emu.answer(a, orders, model.at(1), 2) {
                TradingEvent::Order(u) => {
                    orders.apply(&u, 2);
                }
                TradingEvent::Failed {
                    action,
                    definitive,
                    msg,
                } => {
                    orders.failed(&action, definitive, &msg, 2);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn marketable_and_market_orders_fill_at_the_quote() {
        let (mut model, mut orders, mut emu) = (model(), Orders::new(), Emulator::starting_at(0));
        for price in [301.0, 0.0] {
            let (id, actions) = start(&mut orders, &model, price);
            run(&mut emu, &mut orders, &model, actions);
            let o = orders.get(id).unwrap();
            assert_eq!(o.status, status::BUY_DONE, "price {price}");
            assert_eq!(o.record().buy.mean_price, 300.1);
        }
        // A market order with no price at all is refused.
        let (id, actions) = start(&mut orders, &model, 0.0);
        let m = model.at_mut(1).unwrap();
        (m.last_price, m.bid, m.ask) = (None, None, None);
        run(&mut emu, &mut orders, &model, actions);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_FAIL);
    }

    #[test]
    fn a_resting_order_fills_at_its_limit_when_the_market_reaches_it() {
        let (mut model, mut orders, mut emu) = (model(), Orders::new(), Emulator::starting_at(0));
        let (id, actions) = start(&mut orders, &model, 299.0);
        run(&mut emu, &mut orders, &model, actions);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_SET);
        let resting = orders.emu_resting("u-sber");
        assert_eq!(resting.len(), 1);
        let m = model.at(1).unwrap();
        assert!(fills(&resting, m, Some(299.01), 3).is_empty());
        assert!(fills(&resting, m, None, 3).is_empty());
        // Traded through: filled in full at its own limit, not the print.
        let done = fills(&resting, m, Some(298.5), 3);
        assert_eq!(done.len(), 1);
        assert_eq!((done[0].lots_executed, done[0].avg_price), (1, 299.0));
        orders.apply(&done[0], 3);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
        assert!(orders.emu_resting("u-sber").is_empty());
        // The ask coming down to a limit fills it too.
        let (id, actions) = start(&mut orders, &model, 299.0);
        run(&mut emu, &mut orders, &model, actions);
        model.at_mut(1).unwrap().ask = Some(299.0);
        let done = fills(&orders.emu_resting("u-sber"), model.at(1).unwrap(), None, 4);
        orders.apply(&done[0], 4);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_DONE);
    }

    #[test]
    fn cancel_move_and_a_request_cut_off_by_a_restart() {
        let (model, mut orders, mut emu) = (model(), Orders::new(), Emulator::starting_at(0));
        let (id, actions) = start(&mut orders, &model, 299.0);
        run(&mut emu, &mut orders, &model, actions);
        // A move replaces the order: the new one rests, the old never fills.
        let fx = orders.target(id, Leg::Buy, 298.0, None);
        assert!(matches!(fx.actions[..], [Action::Replace { .. }]));
        run(&mut emu, &mut orders, &model, fx.actions);
        let resting = orders.emu_resting("u-sber");
        assert_eq!((resting.len(), resting[0].price), (1, 298.0));
        let fx = orders.cancel(id, Leg::Buy);
        run(&mut emu, &mut orders, &model, fx.actions);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_CANCEL);
        // A Post never answered is placed when the restart asks for it by key.
        let (id, actions) = start(&mut orders, &model, 299.0);
        let Some(Action::Post { key, .. }) = actions.first().cloned() else {
            panic!("{actions:?}");
        };
        let ask = Action::QueryRequest {
            order: id,
            leg: Leg::Buy,
            key,
        };
        run(&mut emu, &mut orders, &model, vec![ask]);
        assert_eq!(orders.get(id).unwrap().status, status::BUY_SET);
        assert_eq!(orders.emu_resting("u-sber").len(), 1);
        // An order the emulator does not know is answered as Aster's -2013.
        let lost = Action::Query {
            order: id,
            leg: Leg::Buy,
            exchange_id: "1".into(),
        };
        match emu.answer(lost, &orders, model.at(1), 5) {
            TradingEvent::Failed { msg, .. } => assert!(msg.contains(CODE_ORDER_NOT_FOUND)),
            _ => panic!("answered an unknown order"),
        }
    }
}
