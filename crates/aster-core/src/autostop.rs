//! Global auto-stop on loss (MoonBot «Settings → Auto start»: «Stop if the
//! total loss is greater than» over the last N trades, or over the last H
//! hours with at least M trades; «Also Panic Sell all orders»). The rules come
//! with the terminal's `TClientSettings` (`as_cfg`); losses are the report's
//! realized USDT of closed deals since the counter last started over.
//!
//! The same tab's global panic on a market move («General Panic Sell when the
//! BTC rate changes», and the one on the whole market's average) and its
//! «Restart if» band read MoonBot's own two numbers again: the hourly delta of
//! `BTCUSDT` and the mean hourly delta of the traded markets. Ported from
//! TInvestCore, which had no BTC and folded both into one MOEX index.

use moonproto::{AutoStartConfig, AutoStartConfig2};

/// Auto-stop rules; both windows may be on at once.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Rules {
    /// `(loss limit, USDT; trades)`: the last N closed deals.
    pub by_trades: Option<(f64, usize)>,
    /// `(loss limit, USDT; window, s; minimum trades)`.
    pub by_hours: Option<(f64, i64, usize)>,
    /// Panic sell every position of the core as well.
    pub sell_all: bool,
    /// Emulator deals count too (MoonBot «ignore emulator» off).
    pub with_emulator: bool,
    /// Panic sell all and stop when BTC's hourly delta falls to −this %.
    pub panic_drop: Option<f64>,
    /// … or rises to +this %.
    pub panic_rise: Option<f64>,
    /// … or when the markets' mean hourly delta falls to −this %.
    pub panic_market_drop: Option<f64>,
    /// «Restart if»: after such a stop, start again once BTC's delta is
    /// inside `(low, high)` and the market's above `market_min` (all %).
    pub restart: Option<(f64, f64, f64)>,
    /// «Auto stop if the error level ≥ N»: API errors in the last minute.
    pub errors: Option<Circuit>,
    /// «Auto stop on ping»: the median round trip of the answered API calls
    /// (order requests, `GetPositions`), ms.
    pub ping: Option<Circuit>,
}

/// One circuit breaker of the auto-start tab.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Circuit {
    /// Trips at or above it (errors a minute, or ms).
    pub level: i64,
    /// Also panic sell every position.
    pub sell_all: bool,
    /// Start again this long after the stop, once below the level (ms).
    pub restart_ms: Option<i64>,
}

/// What the hourly deltas ask for.
#[derive(Debug, Clone, PartialEq)]
pub enum MarketCall {
    /// Stop and panic sell all, with the reason.
    Panic(String),
    /// Start the strategies again.
    Restart(String),
}

/// One closed deal: close time (Unix s) and realized profit (USDT).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Closed {
    pub close_s: i64,
    pub profit: f64,
}

impl Rules {
    pub fn from_config(c: &AutoStartConfig, c2: &AutoStartConfig2) -> Self {
        let limit = |v: f64| (v.is_finite() && v != 0.0).then_some(v.abs());
        let band = [c2.btc_higher_than, c2.btc_lower_than, c2.market_higher_than];
        Self {
            by_trades: c
                .auto_stop_if_loss
                .then(|| limit(c.auto_stop_loss))
                .flatten()
                .filter(|_| c.stop_trades > 0)
                .map(|l| (l, c.stop_trades as usize)),
            by_hours: c
                .auto_stop_if_loss_hours
                .then(|| limit(c.auto_stop_hours_val))
                .flatten()
                .filter(|_| c.stop_hours > 0)
                .map(|l| {
                    (
                        l,
                        i64::from(c.stop_hours) * 3600,
                        c.stop_hours_trades.max(0) as usize,
                    )
                }),
            sell_all: c.sell_if_loss,
            with_emulator: !c.ignore_emulator,
            panic_drop: c.panic_btc.then(|| limit(c.panic_btc_delta)).flatten(),
            panic_rise: c.panic_btc.then(|| limit(c.panic_btc_delta_up)).flatten(),
            panic_market_drop: c
                .panic_market
                .then(|| limit(c.panic_market_delta))
                .flatten(),
            restart: (c2.restart_on_market && band.iter().all(|v| v.is_finite()))
                .then_some((band[0], band[1], band[2])),
            errors: (c.auto_stop_on_errors && c.errors_level > 0).then_some(Circuit {
                level: i64::from(c.errors_level),
                sell_all: c.sell_all_on_errors,
                restart_ms: c
                    .restart_after_err
                    .then_some(i64::from(c.restart_err_time.max(0)) * 1000),
            }),
            ping: (c.auto_stop_on_ping && c.ping_level > 0).then_some(Circuit {
                level: i64::from(c.ping_level),
                sell_all: c.sell_all_on_ping,
                restart_ms: c
                    .restart_after_ping
                    .then_some(i64::from(c.restart_ping_time.max(0)) * 1000),
            }),
        }
    }

    pub fn watches_market(&self) -> bool {
        self.panic_drop.is_some() || self.panic_rise.is_some() || self.panic_market_drop.is_some()
    }

    pub fn active(&self) -> bool {
        self.by_trades.is_some() || self.by_hours.is_some()
    }
}

/// Why the rules stop trading, if they do: deals closed before `since_s`
/// (the counter's start) do not count.
pub fn tripped(rules: &Rules, deals: &[Closed], since_s: i64, now_s: i64) -> Option<String> {
    let mut recent: Vec<&Closed> = deals.iter().filter(|d| d.close_s >= since_s).collect();
    recent.sort_by_key(|d| std::cmp::Reverse(d.close_s));
    if let Some((limit, n)) = rules.by_trades {
        let last = &recent[..n.min(recent.len())];
        let sum: f64 = last.iter().map(|d| d.profit).sum();
        if !last.is_empty() && sum <= -limit {
            return Some(format!(
                "loss {:.2} USDT over the last {} trade(s) reached the limit {limit}",
                -sum,
                last.len()
            ));
        }
    }
    if let Some((limit, window_s, min_trades)) = rules.by_hours {
        let inside: Vec<&&Closed> = recent
            .iter()
            .filter(|d| d.close_s >= now_s - window_s)
            .collect();
        let sum: f64 = inside.iter().map(|d| d.profit).sum();
        if !inside.is_empty() && inside.len() >= min_trades && sum <= -limit {
            return Some(format!(
                "loss {:.2} USDT over the last {} h ({} trade(s)) reached the limit {limit}",
                -sum,
                window_s / 3600,
                inside.len()
            ));
        }
    }
    None
}

/// The hourly deltas (%) the market rules read: `BTCUSDT`'s and the mean of
/// the traded markets'. `None` while a side cannot be measured yet.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Deltas {
    pub btc: Option<f64>,
    pub market: Option<f64>,
}

/// The move is past a panic threshold (why), whether the strategies run
/// or not.
pub fn market_past(rules: &Rules, d: Deltas) -> Option<String> {
    if let Some(btc) = d.btc.filter(|v| v.is_finite()) {
        if let Some(drop) = rules.panic_drop.filter(|&x| btc <= -x) {
            return Some(format!("BTC hourly delta {btc:.2}% fell past -{drop}%"));
        }
        if let Some(rise) = rules.panic_rise.filter(|&x| btc >= x) {
            return Some(format!("BTC hourly delta {btc:.2}% rose past +{rise}%"));
        }
    }
    let market = d.market.filter(|v| v.is_finite())?;
    rules
        .panic_market_drop
        .filter(|&x| market <= -x)
        .map(|drop| format!("market hourly delta {market:.2}% fell past -{drop}%"))
}

/// The hourly deltas against the market rules: a panic once per crossing
/// (`latched`: this move already panicked), whether the strategies run or
/// not; a restart after a panic of this kind stopped them, once BTC is back
/// inside the band and the market above its floor. A restart needs both
/// numbers: an unmeasured side is not «inside».
pub fn market_call(
    rules: &Rules,
    d: Deltas,
    running: bool,
    stopped_by_market: bool,
    latched: bool,
) -> Option<MarketCall> {
    if let Some(why) = market_past(rules, d) {
        return (!latched).then_some(MarketCall::Panic(why));
    }
    if running {
        return None;
    }
    let (low, high, market_min) = rules.restart?;
    let btc = d.btc.filter(|v| v.is_finite())?;
    let market = d.market.filter(|v| v.is_finite())?;
    (stopped_by_market && btc > low && btc < high && market > market_min).then(|| {
        MarketCall::Restart(format!(
            "BTC hourly delta {btc:.2}% is back inside ({low}%, {high}%), market {market:.2}%"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(close_s: i64, profit: f64) -> Closed {
        Closed { close_s, profit }
    }

    #[test]
    fn circuits_map_levels_and_restarts() {
        let (mut c, c2) = (AutoStartConfig::default(), AutoStartConfig2::default());
        c.auto_stop_on_errors = true;
        c.errors_level = 3;
        c.sell_all_on_errors = true;
        c.restart_after_err = true;
        c.restart_err_time = 120;
        c.auto_stop_on_ping = true;
        c.ping_level = 0;
        let r = Rules::from_config(&c, &c2);
        assert_eq!(
            r.errors,
            Some(Circuit {
                level: 3,
                sell_all: true,
                restart_ms: Some(120_000)
            })
        );
        // A zero ping level is off.
        assert_eq!(r.ping, None);
    }

    fn both(btc: f64, market: f64) -> Deltas {
        Deltas {
            btc: Some(btc),
            market: Some(market),
        }
    }

    #[test]
    fn market_rules_panic_and_restart_in_the_band() {
        let (mut c, mut c2) = (AutoStartConfig::default(), AutoStartConfig2::default());
        assert!(!Rules::from_config(&c, &c2).watches_market());
        c.panic_btc = true;
        c.panic_btc_delta = 3.0;
        c.panic_btc_delta_up = 5.0;
        c.panic_market = true;
        c.panic_market_delta = -2.0;
        c2.restart_on_market = true;
        (c2.btc_higher_than, c2.btc_lower_than, c2.market_higher_than) = (-1.0, 2.0, -0.5);
        let r = Rules::from_config(&c, &c2);
        assert_eq!(
            (r.panic_drop, r.panic_rise, r.panic_market_drop),
            (Some(3.0), Some(5.0), Some(2.0))
        );
        assert_eq!(market_call(&r, both(-2.9, -1.9), true, false, false), None);
        // BTC and the market each trip on their own threshold.
        assert!(matches!(
            market_call(&r, both(-3.0, 0.0), true, false, false),
            Some(MarketCall::Panic(w)) if w.contains("BTC")
        ));
        assert!(matches!(
            market_call(&r, both(0.0, -2.0), true, false, false),
            Some(MarketCall::Panic(w)) if w.contains("market")
        ));
        assert!(matches!(
            market_call(&r, both(5.1, 0.0), true, false, false),
            Some(MarketCall::Panic(_))
        ));
        // Once per crossing; with the strategies stopped too (positions sell).
        assert_eq!(market_call(&r, both(-3.5, 0.0), true, false, true), None);
        assert!(matches!(
            market_call(&r, both(-3.5, 0.0), false, false, false),
            Some(MarketCall::Panic(_))
        ));
        // Stopped by the market: restart only inside the band.
        assert_eq!(market_call(&r, both(-1.5, 0.0), false, true, false), None);
        assert_eq!(market_call(&r, both(0.3, -0.7), false, true, false), None);
        assert!(matches!(
            market_call(&r, both(0.3, 0.0), false, true, false),
            Some(MarketCall::Restart(_))
        ));
        // An unmeasured side is not inside the band.
        let half = Deltas {
            btc: Some(0.3),
            market: None,
        };
        assert_eq!(market_call(&r, half, false, true, false), None);
        // Stopped by hand: never restarted by the market.
        assert_eq!(market_call(&r, both(0.3, 0.0), false, false, false), None);
        assert_eq!(
            market_call(&r, both(f64::NAN, f64::NAN), true, false, false),
            None
        );
    }

    #[test]
    fn config_maps_to_rules() {
        let c2 = AutoStartConfig2::default();
        let mut c = AutoStartConfig::default();
        assert!(!Rules::from_config(&c, &c2).active());
        c.auto_stop_if_loss = true;
        c.auto_stop_loss = -500.0;
        c.stop_trades = 3;
        c.auto_stop_if_loss_hours = true;
        c.auto_stop_hours_val = 1000.0;
        c.stop_hours = 2;
        c.stop_hours_trades = 4;
        c.sell_if_loss = true;
        c.ignore_emulator = true;
        let r = Rules::from_config(&c, &c2);
        assert_eq!(r.by_trades, Some((500.0, 3)));
        assert_eq!(r.by_hours, Some((1000.0, 7200, 4)));
        assert!(r.sell_all && !r.with_emulator);
        // A zero limit or window is off, not «stop at any loss».
        c.auto_stop_loss = 0.0;
        c.stop_hours = 0;
        assert!(!Rules::from_config(&c, &c2).active());
    }

    #[test]
    fn last_trades_window_counts_the_newest_by_close_time() {
        let r = Rules {
            by_trades: Some((500.0, 2)),
            ..Rules::default()
        };
        // Newest two: -300 and -250 = -550.
        let deals = [d(100, 1000.0), d(300, -250.0), d(200, -300.0)];
        assert!(tripped(&r, &deals, 0, 400).unwrap().contains("550.00"));
        // A win among the newest two keeps trading.
        let deals = [d(100, -1000.0), d(300, 100.0), d(200, -300.0)];
        assert_eq!(tripped(&r, &deals, 0, 400), None);
        // One big loss is enough while fewer than N trades exist.
        assert!(tripped(&r, &[d(10, -600.0)], 0, 20).is_some());
        // Deals before the counter's start do not count.
        assert_eq!(tripped(&r, &[d(10, -600.0)], 11, 20), None);
        assert_eq!(tripped(&r, &[], 0, 20), None);
    }

    #[test]
    fn hours_window_needs_its_minimum_trades() {
        let r = Rules {
            by_hours: Some((500.0, 3600, 2)),
            ..Rules::default()
        };
        let now = 10_000;
        // One trade inside the hour: below the minimum.
        assert_eq!(tripped(&r, &[d(now - 10, -900.0)], 0, now), None);
        let deals = [
            d(now - 10, -300.0),
            d(now - 20, -300.0),
            d(now - 4000, -5000.0),
        ];
        let why = tripped(&r, &deals, 0, now).unwrap();
        assert!(why.contains("600.00") && why.contains("2 trade"), "{why}");
        // Exactly at the limit stops; just above does not.
        let deals = [d(now - 10, -250.0), d(now - 20, -250.0)];
        assert!(tripped(&r, &deals, 0, now).is_some());
        let deals = [d(now - 10, -249.99), d(now - 20, -250.0)];
        assert_eq!(tripped(&r, &deals, 0, now), None);
    }
}
