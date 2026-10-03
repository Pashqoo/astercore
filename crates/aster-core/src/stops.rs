//! Stops of an open strategy position, MoonBot's `Stops` section as its FAQ
//! describes it: stop 1 (`StopLoss`), which the second stop moves to its own
//! level; the third stop, a line of its own beside it; the trailing stop
//! behind its take profit; the BV/SV stop. Pure per-order decisions:
//! `moonshot::manage_exit` feeds one pass of prices in, turns a fired stop
//! into a marketable exit and chases it.
//!
//! Levels are % from the entry, signed as MoonBot's (negative = against the
//! position), and priced like every exit (`exit_price`: a short divides).
//! Ported from TInvestCore. `DontSellBelowLiq`, `StopAboveLiq` and
//! `PanicSellDelisted` are kept with the strategy and still read by nothing:
//! on Aster they could work (a liquidation price, a delivery date), and that
//! is recorded as a debt in `PLAN.md`, not done here.

use crate::bvsv;
use crate::model::Market;
use crate::orders::{reason, PANIC_SPREAD};

/// A stop break on the book must hold this long (a 200 ms spike once sold a
/// position at its low) unless it is deeper than `DEEP_MULT` stop distances.
const CONFIRM_MS: i64 = 2_000;
const DEEP_MULT: f64 = 3.0;

/// The second or the third stop: `after` s past the fill, once the price
/// has reached `price` %, the stop takes `level` %.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Switch {
    pub after: f64,
    pub price: f64,
    pub level: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trailing {
    /// `TrailingPercent`: the line trails the best price by this % (MoonBot
    /// keeps it negative; its sign is ignored).
    pub pct: f64,
    /// `TrailingSpread` %: how far its exit crosses the book (0 = `PANIC_SPREAD`).
    pub spread: f64,
    /// `TrailingEMA`: EMA over this many points of the price it trails, one point per
    /// [`EMA_TICK_MS`] (0 or 1 = the price itself).
    pub ema: u32,
    /// `UseTakeProfit` with `TakeProfit` %: the line appears only once the
    /// price has reached it; without it, once `StopLossDelay` has passed.
    pub take_profit: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BvSv {
    pub kind: bvsv::Kind,
    pub n: i64,
    /// Fires below this ratio of the position's side: bought to sold for a
    /// long, sold to bought for a short.
    pub ratio: f64,
    /// `BV_SV_Reverse`: the inverse ratio — out when the price goes the
    /// position's way.
    pub reverse: bool,
    /// `BV_SV_TakeProfit` %: live only once the price has reached it.
    pub from: f64,
}

impl BvSv {
    /// The ratio `(bought, sold)` gives the position's side; `None` without volume.
    pub fn ratio_of(&self, short: bool, (bought, sold): (f64, f64)) -> Option<f64> {
        let (num, den) = if short != self.reverse {
            (sold, bought)
        } else {
            (bought, sold)
        };
        match (num > 0.0, den > 0.0) {
            (false, false) => None,
            (_, false) => Some(f64::INFINITY),
            _ => Some(num / den),
        }
    }
}

/// The `Stops` fields of a strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// `StopLoss` %; `None` with `UseStopLoss` off or a zero level.
    pub level: Option<f64>,
    /// `FastStopLoss`: stops 1–3 also fire on a trade through the line, and
    /// without the book confirmation; the trailing stop does not (FAQ).
    pub fast: bool,
    /// `UseMarketOrder`: a fired stop's exit sits at the edge of the
    /// `PERCENT_PRICE` band, which crosses the book as a market order does,
    /// instead of its spread through the book (the spread without a known
    /// band). Only the emulator and an exit with an allowed drop read it: a
    /// live stop with no floor goes out as a MARKET order (`moonshot`).
    pub market: bool,
    /// `StopLossEMA`: stops 1–3 watch an EMA over this many points of the book price, one
    /// point per [`EMA_TICK_MS`] (0 = the price itself), so a single spike fires nothing.
    pub ema: u32,
    /// `StopLossDelay`: no stop and no trailing fires this many seconds after the fill.
    pub delay: f64,
    /// `StopLossSpread` %: how far a fired stop's exit crosses the book (0 = `PANIC_SPREAD`).
    pub spread: f64,
    /// `StopSpreadAdd1mDelta`: the spread grows by this share of the
    /// market's 1-minute move (FAQ: 0.1 × 25 % = +2.5 %).
    pub add_1m: f64,
    /// `AllowedDrop` %: a fired stop's exit goes no further from the entry.
    pub allowed_drop: f64,
    /// `StopLossFixed`: stops 1–3 stay priced off the first entry seen, not
    /// the mean of later fills.
    pub fixed: bool,
    /// `UseSecondStop`: moves stop 1 to its level. Both extra stops need `UseStopLoss`.
    pub second: Option<Switch>,
    /// `UseStopLoss3`: a line of its own; once it fires, the exit goes no
    /// further than `allowed_drop3` until the price passes the main line.
    pub third: Option<Switch>,
    pub allowed_drop3: f64,
    pub trailing: Option<Trailing>,
    pub bvsv: Option<BvSv>,
}

impl Config {
    pub fn read(
        num: &dyn Fn(&str) -> f64,
        flag: &dyn Fn(&str) -> bool,
        text: &dyn Fn(&str) -> String,
    ) -> Self {
        let on = flag("UseStopLoss");
        let switch = |use_: &str, after: &str, price: &str, level: &str| {
            (on && flag(use_)).then(|| Switch {
                after: num(after),
                price: num(price),
                level: num(level),
            })
        };
        let ema = |v: f64| v.max(0.0) as u32;
        Self {
            level: Some(num("StopLoss")).filter(|&l| on && l != 0.0),
            fast: flag("FastStopLoss"),
            market: flag("UseMarketOrder"),
            ema: ema(text("StopLossEMA").trim().parse().unwrap_or(0.0)),
            delay: num("StopLossDelay"),
            spread: num("StopLossSpread"),
            add_1m: num("StopSpreadAdd1mDelta"),
            allowed_drop: num("AllowedDrop"),
            fixed: flag("StopLossFixed"),
            second: switch(
                "UseSecondStop",
                "TimeToSwitch2Stop",
                "PriceToSwitch2Stop",
                "SecondStopLoss",
            ),
            third: switch(
                "UseStopLoss3",
                "TimeToSwitchStop3",
                "PriceToSwitchStop3",
                "StopLoss3",
            ),
            allowed_drop3: num("AllowedDrop3"),
            trailing: (flag("UseTrailing") && num("TrailingPercent") != 0.0).then(|| Trailing {
                pct: num("TrailingPercent"),
                spread: num("TrailingSpread"),
                ema: ema(num("TrailingEMA")),
                take_profit: flag("UseTakeProfit").then(|| num("TakeProfit")),
            }),
            bvsv: flag("UseBV_SV_Stop").then(|| BvSv {
                kind: bvsv::Kind::parse(&text("BV_SV_Kind")),
                n: num("BV_SV_TradesN") as i64,
                ratio: num("BV_SV_Ratio"),
                reverse: flag("BV_SV_Reverse"),
                from: num("BV_SV_TakeProfit"),
            }),
        }
    }

    /// Spread of stops 1–3's exit, a fraction.
    pub fn stop_spread(&self) -> f64 {
        pct_or_panic(self.spread)
    }
}

/// Which stop fired: it decides the exit's spread, how far it may go and the
/// sell reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fired {
    Stop,
    Stop3,
    Trailing,
    BvSv,
}

impl Fired {
    pub fn reason(self) -> u8 {
        match self {
            Self::Stop | Self::Stop3 => reason::STOP_LOSS,
            Self::Trailing => reason::TRAILING,
            Self::BvSv => reason::BV_SV_STOP,
        }
    }

    /// The stop an exit placed for `code` belongs to (a restored order); the
    /// third stop comes back as stop 1.
    pub fn of_reason(code: u8) -> Option<Self> {
        match code {
            reason::STOP_LOSS => Some(Self::Stop),
            reason::TRAILING => Some(Self::Trailing),
            reason::BV_SV_STOP => Some(Self::BvSv),
            _ => None,
        }
    }
}

/// One pass of a position's prices.
pub struct Pass {
    pub short: bool,
    /// Mean entry.
    pub entry: f64,
    /// Since the fill, ms.
    pub held_ms: i64,
    /// The book side the exit would hit (bid for a long, ask for a short;
    /// the last trade without a book).
    pub px: f64,
    /// Lowest and highest exchange trade since the last pass.
    pub swing: Option<(f64, f64)>,
    /// `(bought, sold)` USDT of the BV/SV window, once covered.
    pub volumes: Option<(f64, f64)>,
    pub now: i64,
}

/// A stop that fired this pass, with what the log line tells.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trigger {
    pub fired: Fired,
    /// The price that crossed and the line it crossed (the peak for the
    /// trailing stop's line; the ratio for BV/SV).
    pub px: f64,
    pub line: f64,
    pub peak: f64,
    pub ratio: f64,
    /// `FastStopLoss` with no delay and stop 1 past the entry: sold at once
    /// off the entry (FAQ «Immediate StopLoss»).
    pub immediate: bool,
}

/// The line the order image shows.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Line {
    pub price: f64,
    /// Spread of its exit, %.
    pub spread: f64,
    pub what: &'static str,
    /// Its level %, from `from` (the stop's entry, or the trailing peak).
    pub pct: f64,
    pub from: f64,
}

impl Line {
    const NONE: Self = Self {
        price: 0.0,
        spread: 0.0,
        what: "",
        pct: 0.0,
        from: 0.0,
    };
}

/// One point of an EMA per this long: MoonBot averages the bid of its REST ticker, which comes
/// about every 2.15 s (`_doc/STRATEGY_FORMULAS/sell-common.md`, the core developer's answer of
/// 23.09), not the pass — however often the pass runs, `StopLossEMA = 3` is three ticker points.
pub const EMA_TICK_MS: i64 = 2_150;

/// The average and the time (ms since the fill) of its last point.
#[derive(Debug, Default, Clone, Copy)]
struct Ema {
    value: f64,
    at: i64,
}

impl Ema {
    /// The average after the price `px` seen `at_ms` into the position: it takes a new point
    /// once per [`EMA_TICK_MS`] and is the same in between. `n <= 1` is the price itself.
    fn next(&mut self, px: f64, n: u32, at_ms: i64) -> f64 {
        if n <= 1 || self.value <= 0.0 {
            self.value = px;
            self.at = at_ms;
        } else if at_ms.saturating_sub(self.at) >= EMA_TICK_MS {
            self.value += (px - self.value) * 2.0 / (f64::from(n) + 1.0);
            self.at = at_ms;
        }
        self.value
    }
}

/// Per-order stop state.
#[derive(Debug, Default)]
pub struct State {
    /// First entry seen: `StopLossFixed` prices stops 1–3 off it.
    base: f64,
    second: bool,
    third: bool,
    ema: Ema,
    trail_ema: Ema,
    /// Best trailed price since the trailing stop went live; 0 = not yet.
    peak: f64,
    /// `BV_SV_TakeProfit` has been reached.
    bvsv_live: bool,
    armed: i64,
    trail_armed: i64,
    /// The line the order image kept, restored mid-chase from reason 7
    /// (stops 1, 2 and 3 share it): it tells which of them fired; 0 = none
    /// left to resolve.
    restored_line: f64,
    /// The kind of line last announced in the log.
    announced: &'static str,
    pub fired: Option<Fired>,
    /// When the fired stop's exit last moved.
    pub moved: i64,
}

impl State {
    /// A state for an exit restored mid-chase; `line` is the stop line its
    /// order image kept.
    pub fn restored(fired: Fired, now: i64, line: f64) -> Self {
        Self {
            fired: Some(fired),
            moved: now,
            restored_line: if fired == Fired::Stop { line } else { 0.0 },
            ..Self::default()
        }
    }

    /// A restored stop-1 chase whose kept line is the third stop's or the
    /// second's: that stop fired, and its drop and main line hold again.
    fn resume(&mut self, cfg: &Config, short: bool, base: f64) {
        let line = std::mem::take(&mut self.restored_line);
        if line <= 0.0 {
            return;
        }
        let is = |sw: Option<Switch>| {
            sw.is_some_and(|s| (exit_price(short, base, s.level) - line).abs() <= line * 1e-9)
        };
        if is(cfg.third) {
            self.third = true;
            self.fired = Some(Fired::Stop3);
        } else if is(cfg.second) {
            self.second = true;
        }
    }

    /// The entry stops 1–3 are priced off.
    pub fn base(&mut self, cfg: &Config, entry: f64) -> f64 {
        if self.base <= 0.0 {
            self.base = entry;
        }
        if cfg.fixed {
            self.base
        } else {
            entry
        }
    }

    /// The main line's level: stop 1, or the second stop once switched.
    fn main_level(&self, cfg: &Config) -> Option<f64> {
        match cfg.second {
            Some(s) if self.second => Some(s.level),
            _ => cfg.level,
        }
    }

    fn third_level(&self, cfg: &Config) -> Option<f64> {
        cfg.third.filter(|_| self.third).map(|s| s.level)
    }

    /// The tightest live line — the one the price meets first — for the
    /// order image.
    pub fn shown(&mut self, cfg: &Config, short: bool, entry: f64) -> Line {
        let base = self.base(cfg, entry);
        self.resume(cfg, short, base);
        let mut best = Line::NONE;
        let mut offer = |line: Line| {
            let tighter = if short {
                line.price < best.price
            } else {
                line.price > best.price
            };
            if best.price <= 0.0 || tighter {
                best = line;
            }
        };
        let stop = |what, pct| Line {
            price: exit_price(short, base, pct),
            spread: cfg.stop_spread() * 100.0,
            what,
            pct,
            from: base,
        };
        if let Some(l) = self.main_level(cfg) {
            offer(stop(if self.second { "StopLoss2" } else { "StopLoss" }, l));
        }
        if let Some(l) = self.third_level(cfg) {
            offer(stop("StopLoss3", l));
        }
        if let (Some(t), true) = (cfg.trailing, self.peak > 0.0) {
            offer(Line {
                price: exit_price(short, self.peak, -t.pct.abs()),
                spread: pct_or_panic(t.spread) * 100.0,
                what: "Trailing",
                pct: -t.pct.abs(),
                from: self.peak,
            });
        }
        best
    }

    /// Whether `what` is a kind of line not yet announced; the trailing
    /// line moves every pass, the log tells only when it appears.
    pub fn announce(&mut self, what: &'static str) -> bool {
        std::mem::replace(&mut self.announced, what) != what
    }

    /// One pass on fresh prices: switches the second and third stops, moves
    /// the trailing line, and returns the stop that fires, if any.
    pub fn judge(&mut self, cfg: &Config, p: &Pass) -> Option<Trigger> {
        let short = p.short;
        // `past`: against the position through `line`; `reached`: its way.
        let past = |px: f64, line: f64| if short { px >= line } else { px <= line };
        let reached = |px: f64, line: f64| if short { px <= line } else { px >= line };
        let live = p.held_ms >= secs_ms(cfg.delay);
        let base = self.base(cfg, p.entry);
        for (on, sw) in [(&mut self.second, cfg.second), (&mut self.third, cfg.third)] {
            if let Some(sw) = sw {
                if !*on
                    && p.held_ms >= secs_ms(sw.after)
                    && reached(p.px, exit_price(short, base, sw.price))
                {
                    *on = true;
                }
            }
        }
        let stop_px = self.ema.next(p.px, cfg.ema, p.held_ms);
        let trail_px = self
            .trail_ema
            .next(p.px, cfg.trailing.map_or(0, |t| t.ema), p.held_ms);
        let main = self.main_level(cfg).map(|l| exit_price(short, base, l));
        let third = self.third_level(cfg).map(|l| exit_price(short, base, l));
        let line = match (main, third) {
            (Some(a), Some(b)) => Some(if short { a.min(b) } else { a.max(b) }),
            (a, b) => a.or(b),
        };
        let none = Trigger {
            fired: Fired::Stop,
            px: 0.0,
            line: 0.0,
            peak: 0.0,
            ratio: 0.0,
            immediate: false,
        };
        if let Some(line) = line {
            // A fast stop also meets the trades between passes; its probe is
            // the worst of them.
            let probe = match p.swing.filter(|_| cfg.fast) {
                Some((_, hi)) if short => stop_px.max(hi),
                Some((lo, _)) => stop_px.min(lo),
                None => stop_px,
            };
            let immediate = cfg.fast && cfg.delay <= 0.0 && cfg.level.is_some_and(|l| l > 0.0);
            let fire = if cfg.fast {
                past(probe, line)
            } else {
                let (fire, armed) = confirm(stop_px, line, base, short, self.armed, p.now);
                self.armed = armed;
                fire
            };
            if live && (fire || immediate) {
                let main_hit = immediate || main.is_some_and(|m| past(probe, m));
                return Some(Trigger {
                    fired: if main_hit { Fired::Stop } else { Fired::Stop3 },
                    px: probe,
                    line,
                    immediate,
                    ..none
                });
            }
        }
        if let (Some(t), true) = (cfg.trailing, live) {
            if self.peak <= 0.0 {
                if t.take_profit
                    .is_none_or(|tp| reached(trail_px, exit_price(short, p.entry, tp)))
                {
                    self.peak = trail_px;
                }
            } else if reached(trail_px, self.peak) {
                self.peak = trail_px;
            }
            if self.peak > 0.0 {
                let line = exit_price(short, self.peak, -t.pct.abs());
                let (fire, armed) =
                    confirm(trail_px, line, self.peak, short, self.trail_armed, p.now);
                self.trail_armed = armed;
                if fire {
                    return Some(Trigger {
                        fired: Fired::Trailing,
                        px: trail_px,
                        line,
                        peak: self.peak,
                        ..none
                    });
                }
            }
        }
        if let Some(b) = cfg.bvsv {
            if !self.bvsv_live && reached(p.px, exit_price(short, p.entry, b.from)) {
                self.bvsv_live = true;
            }
            let ratio = p.volumes.and_then(|v| b.ratio_of(short, v));
            if let Some(r) = ratio.filter(|&r| live && self.bvsv_live && r < b.ratio) {
                return Some(Trigger {
                    fired: Fired::BvSv,
                    px: p.px,
                    ratio: r,
                    ..none
                });
            }
        }
        None
    }

    /// Where a fired stop's exit goes this pass: its spread through the book
    /// (the band edge with `UseMarketOrder`), never further from the entry
    /// than its allowed drop; `None` without a price. `px` is the book side
    /// the exit hits, `delta_1m` the market's 1-minute move %. The third
    /// value says the price is the exit's own and must stay a limit — an
    /// immediate stop priced off the entry, or the allowed drop holding the
    /// exit back from the book — rather than a crossing a MARKET order can
    /// stand in for (`moonshot::manage_exit`).
    #[allow(clippy::too_many_arguments)]
    pub fn exit(
        &mut self,
        cfg: &Config,
        t: Option<&Trigger>,
        m: &Market,
        short: bool,
        entry: f64,
        px: f64,
        delta_1m: f64,
    ) -> Option<(f64, f64, bool)> {
        let fired = self.fired?;
        let base = self.base(cfg, entry);
        let spread = match (fired, cfg.trailing) {
            (Fired::Trailing, Some(tr)) => pct_or_panic(tr.spread),
            _ => cfg.stop_spread() + cfg.add_1m * delta_1m.abs() / 100.0,
        };
        let edge = m
            .band()
            .map_or(0.0, |(down, up)| if short { up } else { down });
        let immediate = t.is_some_and(|t| t.immediate);
        let price = if immediate {
            m.snap(
                exit_price(short, base, 0.0) * if short { 1.0 + spread } else { 1.0 - spread },
                short,
            )
        } else if cfg.market && edge > 0.0 {
            edge
        } else {
            m.marketable(!short, spread)?
        };
        // The third stop's own drop holds until the price passes the main line.
        let main = self.main_level(cfg).map(|l| exit_price(short, base, l));
        let past_main = main.is_some_and(|l| if short { px >= l } else { px <= l });
        let drop = if fired == Fired::Stop3 && !past_main {
            cfg.allowed_drop3
        } else {
            cfg.allowed_drop
        };
        // Snapped towards the position: the exit never goes past the drop.
        // A drop of the whole entry or more is no floor at all: a short's
        // divides by `1 + drop/100`, which turns infinite, then negative.
        let floor = if drop > -100.0 {
            m.snap(exit_price(short, base, drop), !short)
        } else if short {
            f64::INFINITY
        } else {
            0.0
        };
        let floored = if short { floor < price } else { floor > price };
        let price = if short {
            price.min(floor)
        } else {
            price.max(floor)
        };
        Some((m.within_limits(price, short), spread, immediate || floored))
    }
}

/// Price `pct` % in profit from the entry: `entry × (1 + pct/100)` for a long,
/// `entry / (1 + pct/100)` for a short (MoonBot: 2394.48 / 1.01 = 2370.77).
pub fn exit_price(short: bool, entry: f64, pct: f64) -> f64 {
    if short {
        entry / (1.0 + pct / 100.0)
    } else {
        entry * (1.0 + pct / 100.0)
    }
}

/// A book-price break of `line` with confirmation (TMB `stopTrigger`):
/// `(fire, armed_at)`. `from` is what the line is measured from (the entry,
/// the trailing peak): a break past `DEEP_MULT` distances fires at once.
fn confirm(px: f64, line: f64, from: f64, short: bool, armed_at: i64, now: i64) -> (bool, i64) {
    let crossed = if short { px >= line } else { px <= line };
    if !crossed {
        return (false, 0);
    }
    let dist = (from - line).abs();
    if dist > 0.0 && (px - line).abs() >= (DEEP_MULT - 1.0) * dist {
        return (true, now);
    }
    if armed_at == 0 {
        return (false, now);
    }
    (now - armed_at >= CONFIRM_MS, armed_at)
}

/// A spread % as a fraction; 0 = `PANIC_SPREAD`.
fn pct_or_panic(pct: f64) -> f64 {
    if pct > 0.0 {
        pct / 100.0
    } else {
        PANIC_SPREAD
    }
}

fn secs_ms(s: f64) -> i64 {
    // As `moonshot::secs_ms`: a typed setting, NaN is 0, and ten years is the ceiling, so a
    // sum with a clock cannot wrap.
    if s.is_nan() {
        return 0;
    }
    (s.min(315_360_000.0) * 1000.0) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_break_confirms_after_its_delay_or_at_once_when_deep() {
        assert_eq!(confirm(287.0, 287.85, 296.75, false, 0, 100), (false, 100));
        assert_eq!(
            confirm(287.0, 287.85, 296.75, false, 100, 2100),
            (true, 100)
        );
        assert_eq!(confirm(260.0, 287.85, 296.75, false, 0, 100), (true, 100));
        assert_eq!(confirm(290.0, 287.85, 296.75, false, 100, 200), (false, 0));
    }

    /// `StopLossEMA` counts ticker points, not passes: however often the pass looks, the average
    /// takes one point per `EMA_TICK_MS`, and `n <= 1` is the price itself.
    #[test]
    fn the_ema_takes_one_point_per_ticker_interval_whatever_the_pass_rate() {
        let mut slow = Ema::default();
        let mut fast = Ema::default();
        // The first sight seeds both at 100.
        assert_eq!(slow.next(100.0, 3, 0), 100.0);
        assert_eq!(fast.next(100.0, 3, 0), 100.0);
        // A spike to 90: seen once a second by one, every 100 ms by the other, for 2 s.
        for t in (1..=20).map(|i| i * 100) {
            fast.next(90.0, 3, t);
        }
        slow.next(90.0, 3, 1_000);
        slow.next(90.0, 3, 2_000);
        assert_eq!(fast.value, 100.0, "no point yet before {EMA_TICK_MS} ms");
        assert_eq!(slow.value, 100.0);
        // The first point after the interval moves both the same: 100 + (90 − 100)·2/4.
        let (a, b) = (fast.next(90.0, 3, 2_200), slow.next(90.0, 3, 2_200));
        assert_eq!((a, b), (95.0, 95.0));
        // The price itself without averaging.
        assert_eq!(Ema::default().next(42.0, 0, 7), 42.0);
        assert_eq!(Ema::default().next(42.0, 1, 7), 42.0);
    }
}
