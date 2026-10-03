//! The deal's chart as a PNG, drawn by the core itself.
//!
//! MoonBot sends a screenshot of its own window; ours is a foreign terminal
//! on another machine, so the picture is drawn here instead — a short window
//! around the deal, the entry and the exit marked, the stop and the take as
//! lines, the profit in the caption.
//!
//! Its looks are TMB's (`internal/adapters/chart/render.go` there), because
//! the trader reads both chats with one pair of eyes: the terminal's dark
//! panel, the tape as crosses coloured by the side of the aggressor, a
//! ladder of fixed Y scales so two deals are comparable at a glance, the
//! price chips on the right-hand axis, and the volume as a translucent layer
//! along the bottom of the plot rather than a pane of its own. The whole
//! frame is drawn at 2x and sent at 2x — that, and a real font, is where the
//! crispness of TMB's pictures comes from.
//!
//! Everything under the drawing is still by hand: a canvas of RGB bytes with
//! alpha blending and antialiasing, and a PNG written chunk by chunk over
//! `flate2` (the deflate and the CRC32 both). The one thing a picture cannot
//! fake is its text, so the glyphs come from Go Mono through `ab_glyph` —
//! the face TMB draws with.

use std::io::Write;
use std::sync::OnceLock;

use ab_glyph::{point, Font, FontRef, PxScale, ScaleFont};
use flate2::write::ZlibEncoder;
use flate2::{Compression, Crc};
use moonproto::server::codec::market_data::{Candle, HistoryTrade};

use crate::clock::trader_offset_ms;
use crate::orders::Move;

/// Delphi day 0 in Unix days (`moonproto::time`, which keeps it private).
const DELPHI_EPOCH_DAYS: f64 = 25_569.0;
const DAY_MS: f64 = 86_400_000.0;

/// Device pixels per logical one. Everything below is written in TMB's own
/// logical units and multiplied by this, so the two files read the same.
const SCALE_PX: usize = 2;
const SCALE: f64 = SCALE_PX as f64;

pub const WIDTH: usize = 1000 * SCALE_PX;
pub const HEIGHT: usize = 600 * SCALE_PX;

/// Room for the price chips on the right, the clock underneath, and the
/// title band on top.
const PAD_LEFT: f64 = 10.0 * SCALE;
const PAD_RIGHT: f64 = 64.0 * SCALE;
const PAD_TOP: f64 = 34.0 * SCALE;
const PAD_BOTTOM: f64 = 22.0 * SCALE;

/// How much of the plot's height the volume layer is allowed to reach.
const VOLUME_RATIO: f64 = 0.26;
/// The horizontal grid is always ten intervals, so the step is a tenth of
/// the scale the ladder picked.
const GRID_ROWS: i64 = 10;
/// How far down the plot the caption and the numbers reach.
const HEADER: f64 = 52.0 * SCALE;
/// Half the arm of a tick's cross, and the radius of an order's dot.
const MARK: f64 = 3.0 * SCALE;
const DOT: f64 = 5.0 * SCALE;
/// A candle body narrower than this has nothing to show; wider than this is
/// a wall, not a candle — an hour in which the market traded four times must
/// be four candles and the silence between them.
const MIN_BODY: f64 = 1.0 * SCALE;
const MAX_BODY: f64 = 13.0 * SCALE;

/// The ladder of Y scales, in percent of the centre price (TMB's `SCALES`,
/// which mirrors its own frontend). Fixed steps make the charts of two
/// different deals comparable on sight; a range picked to fit the data makes
/// every picture look the same however much the price actually moved.
const SCALE_STEPS: [f64; 9] = [2.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 75.0, 100.0];
/// Slack, so the outermost print is not cut in half by the frame.
const SCALE_SLACK: f64 = 1.02;

/// About this many clock labels along the bottom, whatever the window.
const TIME_LABELS: i64 = 6;
/// The steps a time axis is allowed to use, in minutes: the numbers a trader
/// reads without arithmetic.
const TIME_STEPS: [i64; 14] = [
    1, 2, 5, 10, 15, 30, 60, 120, 180, 360, 720, 1440, 2880, 10_080,
];
/// However long the window, this many labels and no more — a position held
/// for a month must not write its axis over itself.
const TIME_LABELS_MAX: usize = 40;

const FONT_TITLE: f64 = 16.0 * SCALE;
const FONT_TEXT: f64 = 12.0 * SCALE;
const FONT_SMALL: f64 = 11.0 * SCALE;
const FONT_CHIP: f64 = 10.0 * SCALE;

// ----- the palette -----------------------------------------------------------
// TMB's own (`internal/adapters/chart/theme.go`), which is the terminal's
// dark theme with the alpha kept as an alpha instead of being mixed into the
// colour: a grid line over a volume bar has to stay a grid line.

type Rgb = [u8; 3];

const PANEL: Rgb = [0x1c, 0x1c, 0x1e];
const BUY: Rgb = [0x30, 0xd1, 0x58];
const SELL: Rgb = [0xff, 0x45, 0x3a];
const FLAT: Rgb = [0x8e, 0x8e, 0x93];
/// The grid and the axis are the same grey at two strengths.
const RULE: Rgb = [84, 84, 88];
/// Every letter on the picture is this, at the strength its job deserves.
const INK: Rgb = [235, 235, 245];
/// The entry is neutral; the exit carries the side's own colour, which is
/// what tells a long from a short at a glance.
const ENTRY: Rgb = [255, 255, 255];
const EXIT_LONG: Rgb = [0x0a, 0x84, 0xff];
const EXIT_SHORT: Rgb = [0xbf, 0x5a, 0xf2];

const GRID_A: f64 = 0.22;
const AXIS_A: f64 = 0.55;
const TEXT_A: f64 = 0.45;
const TITLE_A: f64 = 0.86;
const CAPTION_A: f64 = 0.90;
const VOLUME_A: f64 = 0.18;
const LEVEL_A: f64 = 0.35;
/// The minutes the position was open: a hint of a band, not a billboard.
const BAND_A: f64 = 0.06;

/// What the deal ended as, in USDT. It is one value or none: a market
/// chart with no deal behind it has neither a position nor a day's result,
/// and three loose fields would let it carry half of one.
pub struct Outcome {
    /// Realized, net of commission — the sign colours the caption.
    pub profit: f64,
    /// What the position cost at the entry.
    pub spent: f64,
    /// The profit of every deal closed today — MoonBot's session line.
    pub session: f64,
}

/// What the picture is about; the prices are in the market's own units.
pub struct Deal<'a> {
    pub market: &'a str,
    /// Minutes in one bar: the title says it, and the axis places the bars by
    /// it, so a minute nobody traded stays an empty minute.
    pub minutes: i64,
    pub short: bool,
    pub entry: f64,
    pub entry_ms: i64,
    pub exit: f64,
    pub exit_ms: i64,
    pub stop: Option<f64>,
    pub take: Option<f64>,
    /// Where each leg rested before it ended, oldest first (`CoreOrder::moves`)
    /// — the line the order drew, not the one level it happened to end at.
    /// Empty is legitimate: a market chart has no order behind it, and a deal
    /// from before the core recorded this has nothing to show.
    pub entry_moves: &'a [Move],
    pub exit_moves: &'a [Move],
    /// The line under the title: profit, fee, why it closed.
    pub caption: &'a str,
    /// The numbers of the third line; `None` on a market chart.
    pub outcome: Option<Outcome>,
}

/// What the price is drawn from. MoonBot draws every trade, and so do we
/// where the gateway has them: a deal that lived four seconds is a handful of
/// ticks and no candle at all. Bars stay for the chat's own `/chart`, for the
/// terminal's card, and for a deal whose hour of trades the gateway will no
/// longer serve.
pub enum Series<'a> {
    Bars(&'a [Candle]),
    Ticks(&'a [HistoryTrade]),
}

/// The same, owned and cleaned: a price that is not a number would be drawn
/// at the top edge as if it were one (`f64::min` and `f64::max` ignore a NaN,
/// and the cast to a pixel saturates), so it is dropped instead.
enum Track {
    Bars(Vec<Candle>),
    Ticks(Vec<HistoryTrade>),
}

impl Track {
    fn of(series: &Series) -> Self {
        // Sorted here, once, so nothing downstream has to care: a caller
        // handing the prints over shuffled would otherwise report a move
        // backwards. A clock that is not a number goes out with the prices —
        // `total_cmp` sorts a NaN above every real time, which would make it
        // the track's last point.
        match series {
            Series::Bars(bars) => {
                let mut bars: Vec<Candle> = bars
                    .iter()
                    .filter(|c| finite(c) && c.time.is_finite())
                    .cloned()
                    .collect();
                bars.sort_by(|a, b| a.time.total_cmp(&b.time));
                Self::Bars(bars)
            }
            Series::Ticks(ticks) => {
                let mut ticks: Vec<HistoryTrade> = ticks
                    .iter()
                    .filter(|t| t.time.is_finite() && t.price.is_finite() && t.qty.is_finite())
                    .cloned()
                    .collect();
                ticks.sort_by(|a, b| a.time.total_cmp(&b.time));
                Self::Ticks(ticks)
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Bars(bars) => bars.is_empty(),
            Self::Ticks(ticks) => ticks.is_empty(),
        }
    }

    /// The lowest and the highest price the track touches.
    fn range(&self) -> (f64, f64) {
        match self {
            Self::Bars(bars) => bars.iter().fold((f64::MAX, f64::MIN), |(lo, hi), c| {
                (lo.min(f64::from(c.low)), hi.max(f64::from(c.high)))
            }),
            Self::Ticks(ticks) => ticks.iter().fold((f64::MAX, f64::MIN), |(lo, hi), t| {
                (lo.min(f64::from(t.price)), hi.max(f64::from(t.price)))
            }),
        }
    }

    /// The window the axis covers. A bar owns the period that follows it; a
    /// trade is a moment, so the track ends at the last one — with a minute
    /// of air after it, or a single trade would be a plot of zero width.
    fn span(&self, period: i64) -> (i64, i64) {
        match self {
            Self::Bars(bars) => {
                let (t0, t1) = bars.iter().fold((i64::MAX, i64::MIN), |(a, b), c| {
                    let t = unix_ms(c.time);
                    (a.min(t), b.max(t))
                });
                (t0, t1.saturating_add(period))
            }
            Self::Ticks(ticks) => {
                let (t0, t1) = ticks.iter().fold((i64::MAX, i64::MIN), |(a, b), t| {
                    let t = unix_ms(t.time);
                    (a.min(t), b.max(t))
                });
                (t0, if t1 > t0 { t1 } else { t1 + 60_000 })
            }
        }
    }

    /// First price, last price, how many points, and how much traded. Which
    /// point is first is decided by its clock, never by where it sits in the
    /// list: the axis is already drawn from the clock, and a caller handing
    /// these over out of order would otherwise report a move backwards.
    fn ends(&self) -> Option<(f64, f64, usize, f64)> {
        match self {
            Self::Bars(bars) => {
                let first = bars.iter().min_by(|a, b| a.time.total_cmp(&b.time))?;
                let last = bars.iter().max_by(|a, b| a.time.total_cmp(&b.time))?;
                Some((
                    f64::from(first.open),
                    f64::from(last.close),
                    bars.len(),
                    bars.iter()
                        .map(|c| f64::from(c.volume))
                        .filter(|v| v.is_finite())
                        .sum(),
                ))
            }
            Self::Ticks(ticks) => {
                let first = ticks.iter().min_by(|a, b| a.time.total_cmp(&b.time))?;
                let last = ticks.iter().max_by(|a, b| a.time.total_cmp(&b.time))?;
                Some((
                    f64::from(first.price),
                    f64::from(last.price),
                    ticks.len(),
                    ticks.iter().map(|t| f64::from(t.qty).abs()).sum(),
                ))
            }
        }
    }

    /// What the title calls the picture: a tick chart has no timeframe to
    /// name, it is every trade there was.
    fn kind(&self, minutes: i64) -> String {
        match self {
            Self::Ticks(_) => "TICKS".to_owned(),
            Self::Bars(_) => format!("{minutes}M"),
        }
    }
}

/// Whether `series` holds anything a chart could draw (the check `deal_png` opens with), so a
/// caller that draws elsewhere can still answer «nothing to say» at once.
pub fn drawable(series: &Series) -> bool {
    !Track::of(series).is_empty()
}

/// The chart of `deal` over `series`, or `None` when there is nothing to
/// draw (nothing at all, or every price identical — a picture of one flat
/// line tells the trader less than the text already did).
pub fn deal_png(series: Series, deal: &Deal) -> Option<Vec<u8>> {
    let levels = [
        Some(deal.entry).filter(|p| *p > 0.0),
        Some(deal.exit).filter(|p| *p > 0.0),
        deal.stop.filter(|p| *p > 0.0),
        deal.take.filter(|p| *p > 0.0),
    ];
    let track = Track::of(&series);
    if track.is_empty() {
        return None;
    }
    // Everything is placed by its own clock, never by its number in the list:
    // an illiquid instrument publishes a bar only for the minutes it traded,
    // and trades arrive when they arrive. Laying either out evenly draws a
    // chart that lies about time.
    let period = period_ms(deal.minutes);
    let tape = track.span(period);
    let (t0, t1) = axis_span(tape, deal);
    // The two lines are clipped to the window BEFORE the frame is built
    // around them: an entry that rested an hour lower than anything in the
    // picture would otherwise pull the price scale down a rung for a stretch
    // nobody sees.
    let entry_line = visible_moves(deal.entry_moves, t0, deal.entry_ms);
    let exit_line = visible_moves(deal.exit_moves, exit_from(deal, t0), deal.exit_ms);
    let (mut lo, mut hi) = track.range();
    let drawn_prices = entry_line
        .iter()
        .chain(exit_line.iter())
        .map(|m| m.price)
        .chain(levels.into_iter().flatten());
    for level in drawn_prices {
        lo = lo.min(level);
        hi = hi.max(level);
    }
    if !(lo.is_finite() && hi.is_finite()) || hi <= lo {
        return None;
    }
    let (low, high, scale_pct) = y_scale(lo, hi);
    let plot = Plot {
        x0: PAD_LEFT,
        x1: WIDTH as f64 - PAD_RIGHT,
        y0: PAD_TOP,
        y1: HEIGHT as f64 - PAD_BOTTOM,
        low,
        high,
        t0,
        t1,
    };
    let vol_top = plot.y1 - (plot.y1 - plot.y0) * VOLUME_RATIO;
    let step = (high - low) / GRID_ROWS as f64;

    let mut canvas = Canvas::new(WIDTH, HEIGHT);
    canvas.fill(PANEL);

    // The minutes the position was open, as a band behind everything else.
    // MoonBot shades the order's zone; TMB shades nothing at all — a faint
    // band says the same thing without painting over the tape.
    if deal.entry > 0.0 && deal.exit > 0.0 {
        let won = deal.outcome.as_ref().map_or(
            if deal.short {
                deal.exit <= deal.entry
            } else {
                deal.exit >= deal.entry
            },
            |o| o.profit >= 0.0,
        );
        let (xa, xb) = (plot.x_of(deal.entry_ms), plot.x_of(deal.exit_ms));
        canvas.rect(
            xa.min(xb),
            plot.y0,
            // A deal that lived one minute is still a band, not a hairline.
            xa.max(xb).max(xa.min(xb) + 2.0 * SCALE),
            plot.y1,
            if won { BUY } else { SELL },
            BAND_A,
        );
    }

    grid(&mut canvas, &plot, step);
    time_axis(&mut canvas, &plot);

    // The volume first, then the price over it: the layer is the background
    // of the story, not a mark on top of it.
    let vmax = match &track {
        Track::Bars(bars) => bar_volume(&mut canvas, &plot, bars, period, vol_top),
        Track::Ticks(ticks) => tick_volume(&mut canvas, &plot, ticks, vol_top),
    };
    if vmax > 0.0 {
        for (y, v) in [(vol_top, vmax), ((vol_top + plot.y1) / 2.0, vmax / 2.0)] {
            canvas.text(
                (plot.x0 + 2.0 * SCALE, y),
                (0.0, 0.5),
                &short_num(v),
                FONT_SMALL,
                (INK, TEXT_A),
            );
        }
    }
    match &track {
        Track::Bars(bars) => candles(&mut canvas, &plot, bars, period),
        Track::Ticks(ticks) => crosses(&mut canvas, &plot, ticks),
    }

    // The levels the deal was planned around, then where it actually went.
    // Each one says its price: "STOP" alone leaves the trader counting grid
    // lines to find out at what.
    if let Some(stop) = deal.stop.filter(|p| *p > 0.0) {
        level(&mut canvas, &plot, stop, "STOP", SELL, step);
    }
    if let Some(take) = deal.take.filter(|p| *p > 0.0) {
        level(&mut canvas, &plot, take, "TAKE", BUY, step);
    }
    // The entry is drawn from the left edge of the window to the moment it
    // was taken, the exit from the entry to its own moment — the way TMB
    // draws an order: a line you can follow, not an arrowhead to find. Where
    // the order moved while it waited, the line moves with it: the level it
    // ended at is one fact about it, and the way it got there is the other.
    let exit_colour = if deal.short { EXIT_SHORT } else { EXIT_LONG };
    if deal.entry > 0.0 {
        let (x, y) = (plot.x_of(deal.entry_ms), plot.y_of(deal.entry));
        order_line(
            &mut canvas,
            &plot,
            &entry_line,
            t0,
            (deal.entry_ms, deal.entry),
            ENTRY,
        );
        canvas.disc((x, y), DOT, ENTRY, 1.0);
        mark_label(
            &mut canvas,
            &plot,
            (x, y),
            &format!("ENTRY {}", price_label(deal.entry, step)),
            (ENTRY, CAPTION_A),
        );
    }
    if deal.exit > 0.0 {
        let (x, y) = (plot.x_of(deal.exit_ms), plot.y_of(deal.exit));
        order_line(
            &mut canvas,
            &plot,
            &exit_line,
            exit_from(deal, t0),
            (deal.exit_ms, deal.exit),
            exit_colour,
        );
        canvas.disc((x, y), DOT, exit_colour, 1.0);
        mark_label(
            &mut canvas,
            &plot,
            (x, y),
            &format!("EXIT {}", price_label(deal.exit, step)),
            (exit_colour, CAPTION_A),
        );
    }
    // The chips go on last: they sit on the price axis, over its labels, and
    // the one number the trader looks for must not be the one underneath.
    if deal.entry > 0.0 {
        chip(
            &mut canvas,
            plot.y_of(deal.entry),
            &price_label(deal.entry, step),
            ENTRY,
        );
    }
    if deal.exit > 0.0 {
        chip(
            &mut canvas,
            plot.y_of(deal.exit),
            &price_label(deal.exit, step),
            exit_colour,
        );
    }

    // Where the tape stops short of the deal, the empty stretch is the
    // market not printing, not the picture losing its data — so it is
    // marked rather than left as a mystery.
    // Against the deal's own moments, never against the window: the window
    // carries air at both ends, and comparing with that marked every tape as
    // stopping short of itself.
    let marks = drawn_marks(deal);
    if marks.iter().any(|m| *m < tape.0) {
        tape_edge(&mut canvas, &plot, tape.0, "TAPE STARTS");
    }
    if marks.iter().any(|m| *m > tape.1) {
        tape_edge(&mut canvas, &plot, tape.1, "TAPE ENDS");
    }

    // The day the deal happened, not the day the axis begins: the axis
    // carries air at both ends, and a deal just after trader's midnight came
    // out stamped with yesterday.
    let stamp = marks.first().copied().unwrap_or(tape.0);
    title(&mut canvas, &plot, &track, deal, scale_pct, stamp);
    Some(png(WIDTH, HEIGHT, &canvas.px))
}

/// The window the axis covers: the tape's own span, widened to hold the
/// deal's own moments, with air at both ends.
///
/// The widening is the whole point. An emulated fill happens on the book,
/// and an instrument that has not printed since is common enough that a deal
/// regularly falls outside the tape drawn around it — the entry and the exit
/// then clamped to the frame's edge, where the price chips cover them, and
/// the picture said the deal happened at a moment it did not. An axis that
/// holds the deal puts both marks where they belong and leaves the stretch
/// the market stayed quiet for visibly empty, which is the truth of it.
///
/// The air keeps a mark that sits on the end of the window from being drawn
/// half outside the frame.
fn axis_span(tape: (i64, i64), deal: &Deal) -> (i64, i64) {
    let (mut t0, mut t1) = tape;
    for moment in axis_marks(tape, deal) {
        t0 = t0.min(moment);
        t1 = t1.max(moment);
    }
    let air = (t1.saturating_sub(t0) / 25).max(1_000);
    (t0.saturating_sub(air), t1.saturating_add(air))
}

/// How far the axis will stretch from the tape to reach a mark. Not a test
/// of whether the clock is believable — a position held over the weekend has
/// a perfectly real entry days before the hour of prints the gateway keeps —
/// but of what a frame can show: an axis that spans three days leaves that
/// hour a single column, and a picture of one column tells the trader
/// nothing about either.
///
/// Past it the mark pins to the frame's edge, where `Plot::x_of` has always
/// put a moment outside the window, and the tape's own edge is marked so the
/// pinning is not mistaken for the moment itself.
const MARK_REACH_MS: i64 = 86_400_000;

/// The deal's marks as the picture draws them: a mark needs a price as well
/// as a clock to be worth anything to the axis. A report row can carry a
/// synthetic entry — a timestamp and no price, where a foreign exit was
/// adopted — and neither stretching the window nor marking the tape short
/// serves a mark nobody will see. (A price with no clock is still drawn: it
/// pins to the edge, which is all an unfilled clock can honestly say.)
fn drawn_marks(deal: &Deal) -> Vec<i64> {
    [(deal.entry, deal.entry_ms), (deal.exit, deal.exit_ms)]
        .into_iter()
        .filter(|(price, ms)| *price > 0.0 && *ms > 0)
        .map(|(_, ms)| ms)
        .collect()
}

/// Of those, the ones the axis may stretch to hold — see `MARK_REACH_MS`.
fn axis_marks(tape: (i64, i64), deal: &Deal) -> Vec<i64> {
    let reach = tape.0.saturating_sub(MARK_REACH_MS)..=tape.1.saturating_add(MARK_REACH_MS);
    drawn_marks(deal)
        .into_iter()
        .filter(|ms| reach.contains(ms))
        .collect()
}

/// The Y range: the centre of what has to fit, and the first rung of the
/// ladder tall enough to hold it. An asymmetric deal does not waste a rung.
///
/// Everything fits, and that comes before the ladder does: past the top rung
/// there is no rung that holds the window, and the ladder steps aside rather
/// than cut the ends off the picture. It is there to make two ordinary deals
/// comparable, not to hide half of an extraordinary one — the frame then
/// fits the data the way it did before the ladder existed, and the title
/// says the width it really used. Which matters because everything else here
/// leaves out what falls outside the frame: a level with its line off the
/// picture is worse than no level, so a clipped range would have silently
/// dropped a stop.
///
/// The same exit covers a centre at or below zero — not a price anything
/// real trades at, and a percentage of it means nothing, so the title says
/// no scale at all.
fn y_scale(lo: f64, hi: f64) -> (f64, f64, f64) {
    let centre = (lo + hi) / 2.0;
    let need = if centre > 0.0 {
        (hi - lo) / centre * 100.0 * SCALE_SLACK
    } else {
        f64::INFINITY
    };
    if need.is_nan() || need > SCALE_STEPS[SCALE_STEPS.len() - 1] {
        let margin = (hi - lo) * 0.08;
        let (low, high) = (lo - margin, hi + margin);
        let pct = if centre > 0.0 {
            (high - low) / centre * 100.0
        } else {
            0.0
        };
        return (low, high, pct);
    }
    let pct = pick_scale(need);
    let half = centre * pct / 200.0;
    (centre - half, centre + half, pct)
}

/// The first rung that holds `need` percent. Past the top rung there is
/// none, and `y_scale` above takes that window off the ladder before it ever
/// gets here — the fallback is what keeps this function total, not a way to
/// clip a frame.
fn pick_scale(need: f64) -> f64 {
    SCALE_STEPS
        .into_iter()
        .find(|s| *s >= need)
        .unwrap_or(SCALE_STEPS[SCALE_STEPS.len() - 1])
}

/// Ten intervals of price with the centre line as the axis, and the price of
/// every line written down the right-hand edge.
fn grid(canvas: &mut Canvas, plot: &Plot, step: f64) {
    let centre = (plot.low + plot.high) / 2.0;
    for k in -(GRID_ROWS / 2)..=(GRID_ROWS / 2) {
        let price = centre + k as f64 * step;
        let y = plot.y_of(price);
        let alpha = if k == 0 { AXIS_A } else { GRID_A };
        canvas.line((plot.x0, y), (plot.x1, y), SCALE, RULE, alpha);
        canvas.text(
            (WIDTH as f64 - PAD_RIGHT + 4.0 * SCALE, y),
            (0.0, 0.5),
            &price_label(price, step),
            FONT_SMALL,
            (INK, TEXT_A),
        );
    }
}

/// The clock down the picture: whole minutes on the trader's clock, so the grid lines
/// fall where a trader expects them and a hole in the data is visible as the
/// distance between two bars.
///
/// Round times only. The corners used to carry the window's own two ends,
/// which read as the first and the last print and stopped being either once
/// the axis grew air at both ends — a label nobody can trust is worse than
/// one more grid line.
fn time_axis(canvas: &mut Canvas, plot: &Plot) {
    let baseline = plot.y1 + 6.0 * SCALE;
    let step = time_step(plot.t1 - plot.t0);
    let offset = trader_offset_ms();
    let mut t = (plot.t0 + offset).div_euclid(step) * step + step - offset;
    for _ in 0..TIME_LABELS_MAX {
        if t >= plot.t1 {
            break;
        }
        let x = plot.x_of(t);
        canvas.line((x, plot.y0), (x, plot.y1), SCALE, RULE, GRID_A);
        let label = clock_ms(t);
        let half = measure(&label, FONT_SMALL) / 2.0;
        // A label that would hang off the plot leans on the edge instead.
        // Dropping it used to be free, back when the corners carried a clock
        // of their own; now a narrow window could come out with no clock on
        // it at all.
        let (px, ax) = if x - half < plot.x0 {
            (plot.x0, 0.0)
        } else if x + half > plot.x1 {
            (plot.x1, 1.0)
        } else {
            (x, 0.5)
        };
        canvas.text((px, baseline), (ax, 1.0), &label, FONT_SMALL, (INK, TEXT_A));
        t += step;
    }
}

/// The tape's volume along the bottom of the plot: every print bucketed into
/// the column it lands in and stacked by side, sold below bought. Two prints
/// of one moment share an x, and drawing them over each other would show
/// only the larger — so the column is a sum, not the last writer.
///
/// Returns the tallest column, which is what labels the layer.
fn tick_volume(canvas: &mut Canvas, plot: &Plot, ticks: &[HistoryTrade], top: f64) -> f64 {
    let bar = 2.0 * SCALE;
    let columns = (((plot.x1 - plot.x0) / bar).ceil().max(1.0) as usize) + 1;
    // [bought, sold], both positive: the side is the sign of the quantity.
    let mut buckets = vec![[0.0_f64; 2]; columns];
    for t in ticks {
        let k = (((plot.x_of(unix_ms(t.time)) - plot.x0) / bar)
            .round()
            .max(0.0) as usize)
            .min(columns - 1);
        let qty = f64::from(t.qty);
        if qty >= 0.0 {
            buckets[k][0] += qty;
        } else {
            buckets[k][1] -= qty;
        }
    }
    let vmax = buckets.iter().fold(0.0_f64, |m, b| m.max(b[0] + b[1]));
    if vmax <= 0.0 {
        return 0.0;
    }
    let height = plot.y1 - top;
    for (k, b) in buckets.iter().enumerate() {
        let x = plot.x0 + k as f64 * bar;
        let mut y = plot.y1;
        for (volume, colour) in [(b[1], SELL), (b[0], BUY)] {
            if volume <= 0.0 {
                continue;
            }
            let h = volume / vmax * height;
            canvas.rect(
                x - SCALE / 2.0,
                y - h,
                x + bar - SCALE / 2.0,
                y,
                colour,
                VOLUME_A,
            );
            y -= h;
        }
    }
    vmax
}

/// The same layer for bars: one column per candle, in the candle's colour.
fn bar_volume(canvas: &mut Canvas, plot: &Plot, bars: &[Candle], period: i64, top: f64) -> f64 {
    let vmax = bars
        .iter()
        .map(|c| f64::from(c.volume))
        .filter(|v| v.is_finite())
        .fold(0.0_f64, f64::max);
    if vmax <= 0.0 {
        return 0.0;
    }
    let half = body_half(plot, period);
    let height = plot.y1 - top;
    for c in bars {
        let volume = f64::from(c.volume);
        if !volume.is_finite() || volume <= 0.0 {
            continue;
        }
        let cx = plot.x_of(unix_ms(c.time) + period / 2);
        let h = volume / vmax * height;
        let colour = if c.close >= c.open { BUY } else { SELL };
        canvas.rect(cx - half, plot.y1 - h, cx + half, plot.y1, colour, VOLUME_A);
    }
    vmax
}

/// Every trade there was, as a cross in the colour of the side that took it.
/// A line joining the prints would be a hairball on a busy tape; the crosses
/// show the same path and, where they thicken, the density with it.
///
/// A print outside the frame is left out rather than smeared onto its edge —
/// a guard, not a policy: `y_scale` builds the frame around every price this
/// module was handed, so nothing real reaches it.
///
/// A busy hour is tens of thousands of prints, and the picture is drawn on the
/// worker `engine_ops` keeps for it — still, the cross is rasterised once and
/// stamped, not worked out again for each print. Every print is still drawn, repeats included: a hundred trades at
/// one price stack into the solid column that says so, and skipping the
/// repeats would have drawn that column as a single cross.
fn crosses(canvas: &mut Canvas, plot: &Plot, ticks: &[HistoryTrade]) {
    let cross = Stamp::cross(MARK, 1.2 * SCALE);
    for t in ticks {
        let price = f64::from(t.price);
        if price < plot.low || price > plot.high {
            continue;
        }
        let (x, y) = (plot.x_of(unix_ms(t.time)), plot.y_of(price));
        let qty = f64::from(t.qty);
        let colour = if qty > 0.0 {
            BUY
        } else if qty < 0.0 {
            SELL
        } else {
            FLAT
        };
        canvas.stamp((x, y), &cross, colour, 1.0);
    }
}

/// Candles on their own minutes. A candle with no part of it inside the
/// frame is left out, the same as a print outside it.
fn candles(canvas: &mut Canvas, plot: &Plot, bars: &[Candle], period: i64) {
    let half = body_half(plot, period);
    for c in bars {
        let (low, high) = (f64::from(c.low), f64::from(c.high));
        if high < plot.low || low > plot.high {
            continue;
        }
        let cx = plot.x_of(unix_ms(c.time) + period / 2);
        let colour = if c.close >= c.open { BUY } else { SELL };
        canvas.line(
            (cx, plot.y_of(high)),
            (cx, plot.y_of(low)),
            SCALE,
            colour,
            1.0,
        );
        let top = plot.y_of(f64::from(c.open.max(c.close)));
        let bottom = plot.y_of(f64::from(c.open.min(c.close)));
        canvas.rect(
            cx - half,
            top,
            cx + half,
            bottom.max(top + SCALE),
            colour,
            1.0,
        );
    }
}

/// How wide half a candle body is: its share of the window, with a floor and
/// a ceiling.
fn body_half(plot: &Plot, period: i64) -> f64 {
    let slot = if plot.t1 > plot.t0 {
        (plot.x1 - plot.x0) * period as f64 / (plot.t1 - plot.t0) as f64
    } else {
        plot.x1 - plot.x0
    };
    (slot * 0.62).clamp(MIN_BODY, MAX_BODY) / 2.0
}

/// A planned level across the window, with its name and its price above the
/// line. Outside the frame it is not drawn at all — a label alone, with its
/// line off the picture, is what the old chart did and it read as a lie.
/// Where the exit's line may start: the entry's own moment.
///
/// An exit does not exist before the entry fills. Drawn from the left edge —
/// which is what a deal living less than a pixel of time used to fall back to
/// — the line said the sell had been resting there all along, while the buy
/// was still unfilled underneath it. A line of no length is the truth about a
/// deal that lived a second; the dot, the name and the chip carry the level.
fn exit_from(deal: &Deal, t0: i64) -> i64 {
    if deal.entry > 0.0 && deal.entry_ms > 0 {
        deal.entry_ms.max(t0)
    } else {
        t0
    }
}

/// The part of an order's line the window holds, oldest first: where it was
/// resting when the window opened, then every move inside it.
///
/// A move older than the window is folded into that first rest rather than
/// drawn at its own place — `x_of` clamps it to the left edge, and a stack of
/// risers there would say the order moved at the edge of the picture, which is
/// the one moment it certainly did not.
fn visible_moves(moves: &[Move], from: i64, until: i64) -> Vec<Move> {
    let mut out: Vec<Move> = Vec::new();
    for m in moves {
        // A clock the order never filled in would be clamped to the right
        // edge by `x_of` and drag the whole line across the picture.
        if m.at <= 0 || !m.price.is_finite() || m.price <= 0.0 {
            continue;
        }
        if m.at > until {
            break;
        }
        if m.at <= from {
            out.clear();
            out.push(Move {
                at: from,
                price: m.price,
            });
        } else {
            out.push(*m);
        }
    }
    out
}

/// The line an order drew while it waited: a run at every price it rested at,
/// a riser where it moved, and the last run into the moment it ended.
///
/// With nothing recorded — a deal older than the recording, a hand trade the
/// core saw once — it is the flat line this always drew, from `from` to the
/// end. That is still the truth about the level, if not about the way there.
fn order_line(
    canvas: &mut Canvas,
    plot: &Plot,
    moves: &[Move],
    from: i64,
    end: (i64, f64),
    colour: Rgb,
) {
    let (end_ms, end_price) = end;
    let width = 1.6 * SCALE;
    let x_end = plot.x_of(end_ms);
    // The rests first, so the drawing is one pass over a shape that is
    // already right; `from` carries the flat case and the first run alike.
    let mut rests: Vec<(f64, f64)> = moves.iter().map(|m| (plot.x_of(m.at), m.price)).collect();
    if rests.is_empty() {
        rests.push((plot.x_of(from), end_price));
    }
    for (i, &(x, price)) in rests.iter().enumerate() {
        let y = plot.y_of(price);
        let next = rests.get(i + 1).copied();
        // The run ends where the next rest begins, or at the end of the line.
        // `max` guards the one case the clock cannot: two rests inside the
        // same millisecond, which would otherwise draw a run backwards.
        let x1 = next.map_or(x_end, |(nx, _)| nx).max(x);
        canvas.line((x, y), (x1, y), width, colour, 1.0);
        if let Some((_, next_price)) = next {
            canvas.line((x1, y), (x1, plot.y_of(next_price)), width, colour, 1.0);
        }
    }
    // A fill can be better than the price the order rested at. The riser says
    // so; sliding the whole line onto the fill would have drawn a wait that
    // never happened at that price.
    let last = rests.last().map_or(end_price, |&(_, price)| price);
    let (y_last, y_end) = (plot.y_of(last), plot.y_of(end_price));
    if (y_last - y_end).abs() >= 0.5 {
        canvas.line((x_end, y_last), (x_end, y_end), width, colour, 1.0);
    }
}

fn level(canvas: &mut Canvas, plot: &Plot, price: f64, name: &str, colour: Rgb, step: f64) {
    if price < plot.low || price > plot.high {
        return;
    }
    // The guard above is the same one the tape and the candles carry: the
    // frame is built around this level, so it is there to be drawn.
    let y = plot.y_of(price);
    canvas.line((plot.x0, y), (plot.x1, y), 1.5 * SCALE, colour, LEVEL_A);
    line_label(
        canvas,
        plot,
        y,
        &format!("{name} {}", price_label(price, step)),
        (colour, 0.75),
    );
}

/// The end of the tape, where the deal reaches past it: a dashed upright and
/// the clock of the last print the market gave us.
fn tape_edge(canvas: &mut Canvas, plot: &Plot, at: i64, name: &str) {
    let x = plot.x_of(at);
    let dash = 6.0 * SCALE;
    let mut y = plot.y0;
    while y < plot.y1 {
        canvas.line((x, y), (x, (y + dash).min(plot.y1)), SCALE, RULE, AXIS_A);
        y += dash * 2.0;
    }
    // The same flip the marks carry: against the right-hand edge a label
    // anchored to the left of itself runs under the price chips, which is
    // the defect this whole change set out to clear.
    let text = format!("{name} {}", clock_ms(at));
    let gap = 5.0 * SCALE;
    let (px, ax) = if x + gap + measure(&text, FONT_CHIP) <= plot.x1 {
        (x + gap, 0.0)
    } else {
        (x - gap, 1.0)
    };
    canvas.text(
        (px, plot.y0 + HEADER),
        (ax, 1.0),
        &text,
        FONT_CHIP,
        (INK, TEXT_A),
    );
}

/// The name and the price of a mark, beside its own dot rather than at the
/// edge of the plot: that is where the eye already is, and it keeps the two
/// marks clear of the planned levels, whose labels live on the left and used
/// to be written straight through an exit that closed near the take.
///
/// Against the right-hand edge it flips to the other side of the dot, or the
/// name would run under the price chips.
fn mark_label(canvas: &mut Canvas, plot: &Plot, at: (f64, f64), text: &str, ink: (Rgb, f64)) {
    let gap = DOT + 6.0 * SCALE;
    let (x, y) = at;
    let (px, ax) = if x + gap + measure(text, FONT_CHIP) <= plot.x1 {
        (x + gap, 0.0)
    } else {
        (x - gap, 1.0)
    };
    // Clear of the line as well as of the dot: centred on it, the line ran
    // straight through the letters, and a white name on a white line is no
    // name at all. Under the line where above it is the corner the caption
    // and the numbers already occupy.
    let (py, ay) = if y < plot.y0 + HEADER {
        (y + 4.0 * SCALE, 1.0)
    } else {
        (y - 4.0 * SCALE, 0.0)
    };
    canvas.text((px, py), (ax, ay), text, FONT_CHIP, ink);
}

/// The name and the price of a line, written above it — or under it where
/// above is the corner the caption and the numbers already occupy. A stop
/// just below the top of the frame used to write its price through them.
fn line_label(canvas: &mut Canvas, plot: &Plot, y: f64, text: &str, ink: (Rgb, f64)) {
    let (at, anchor) = if y < plot.y0 + HEADER {
        ((plot.x0 + 6.0 * SCALE, y + 4.0 * SCALE), (0.0, 1.0))
    } else {
        ((plot.x0 + 6.0 * SCALE, y - 4.0 * SCALE), (0.0, 0.0))
    };
    canvas.text(at, anchor, text, FONT_CHIP, ink);
}

/// A price on the axis, on a plate of its own colour — TMB's `priceChip`.
fn chip(canvas: &mut Canvas, y: f64, text: &str, colour: Rgb) {
    let x = WIDTH as f64 - PAD_RIGHT;
    canvas.rect(
        x,
        y - 8.0 * SCALE,
        WIDTH as f64,
        y + 8.0 * SCALE,
        colour,
        1.0,
    );
    canvas.text(
        (x + 3.0 * SCALE, y),
        (0.0, 0.5),
        text,
        FONT_CHIP,
        (chip_ink(colour), 1.0),
    );
}

/// Black on a bright plate, white on a dark one: the entry's plate is white,
/// and its price was being written white on white.
fn chip_ink(c: Rgb) -> Rgb {
    let luma = 0.299 * f64::from(c[0]) + 0.587 * f64::from(c[1]) + 0.114 * f64::from(c[2]);
    if luma > 150.0 {
        [0, 0, 0]
    } else {
        [255, 255, 255]
    }
}

/// The band on top: what the picture is of, and what it is worth. The day
/// goes in the right-hand corner — a picture in a chat outlives the message
/// it came with, so it says its own date.
fn title(canvas: &mut Canvas, plot: &Plot, track: &Track, deal: &Deal, scale_pct: f64, stamp: i64) {
    let middle = PAD_TOP / 2.0;
    let mut x = PAD_LEFT;
    canvas.text(
        (x, middle),
        (0.0, 0.5),
        deal.market,
        FONT_TITLE,
        (INK, TITLE_A),
    );
    x += measure(deal.market, FONT_TITLE) + 12.0 * SCALE;

    // A picture with no deal behind it (the chat's own `/chart`) has no side
    // to name.
    let side = match (deal.entry > 0.0 || deal.exit > 0.0, deal.short) {
        (false, _) => "",
        (true, true) => "SHORT ",
        (true, false) => "LONG ",
    };
    let what = format!("{side}{}", track.kind(deal.minutes));
    canvas.text((x, middle), (0.0, 0.5), &what, FONT_TEXT, (INK, TEXT_A));
    x += measure(&what, FONT_TEXT) + 12.0 * SCALE;
    if scale_pct > 0.0 {
        canvas.text(
            (x, middle),
            (0.0, 0.5),
            &format!("Scale: {scale_pct:.0}%"),
            FONT_SMALL,
            (INK, TEXT_A),
        );
    }
    canvas.text(
        (plot.x1, middle),
        (1.0, 0.5),
        &day_text(stamp),
        FONT_SMALL,
        (INK, TEXT_A),
    );

    // And the numbers, in the top-left of the plot the way TMB stacks its
    // screener rows. The caption carries the sign of the result, so the one
    // thing read at a glance is coloured by it.
    let (colour, alpha) = match &deal.outcome {
        Some(o) if o.profit >= 0.0 => (BUY, CAPTION_A),
        Some(_) => (SELL, CAPTION_A),
        None => (INK, TEXT_A),
    };
    let x = PAD_LEFT + 6.0 * SCALE;
    canvas.text(
        (x, plot.y0 + 14.0 * SCALE),
        (0.0, 0.5),
        deal.caption,
        FONT_TEXT,
        (colour, alpha),
    );
    canvas.text(
        (x, plot.y0 + 32.0 * SCALE),
        (0.0, 0.5),
        &stats_line(track, deal),
        FONT_SMALL,
        (INK, TEXT_A),
    );
}

/// MoonBot's block of numbers, in ours: what the window did, what the
/// position cost, where the day stands. A market chart has neither a position
/// nor a day, so it says what it does have — how many bars, and how much of
/// the instrument went through them.
fn stats_line(track: &Track, deal: &Deal) -> String {
    let Some((open, close, points, volume)) = track.ends() else {
        return String::new();
    };
    let change = if open > 0.0 {
        (close - open) / open * 100.0
    } else {
        0.0
    };
    let mut text = format!("WINDOW {}%", percent(change));
    match &deal.outcome {
        Some(o) => {
            if o.spent > 0.0 {
                text.push_str(&format!("   POS {} USDT", money(o.spent)));
            }
            text.push_str(&format!("   DAY {} USDT", signed(o.session)));
        }
        None => {
            let what = match track {
                Track::Bars(_) => "BARS",
                Track::Ticks(_) => "TICKS",
            };
            text.push_str(&format!("   {what} {points}   VOL {}", short_num(volume)));
        }
    }
    text
}

/// The longest bar the axis will draw, in minutes: a week. It is the clamp
/// that keeps `period_ms` finite for any `Deal` anyone builds — the chat's
/// `/chart` never reaches it, because it takes only the timeframes the feeds
/// serve (`chart_history::MINUTES`, up to a day).
pub const MAX_MINUTES: i64 = 10_080;

/// The bar period in milliseconds. A timeframe of zero would be divided by.
fn period_ms(minutes: i64) -> i64 {
    minutes.clamp(1, MAX_MINUTES) * 60_000
}

/// The coarsest step that still leaves about `TIME_LABELS` labels on the
/// axis — and never zero, which would be a loop that does not end.
fn time_step(span_ms: i64) -> i64 {
    let want = span_ms.max(0) / TIME_LABELS;
    let minutes = TIME_STEPS
        .into_iter()
        .find(|m| m * 60_000 >= want)
        .unwrap_or(TIME_STEPS[TIME_STEPS.len() - 1]);
    minutes * 60_000
}

/// A USDT amount: no decimals past a hundred, where they are noise.
fn money(v: f64) -> String {
    if v.abs() >= 100.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.2}")
    }
}

/// A USDT amount with its sign kept — on a result, the sign is the point.
fn signed(v: f64) -> String {
    with_sign(money(v))
}

/// A percent, which is not a USDT amount: `money`'s "no decimals past a
/// hundred" is a rule about USDT, and a window that doubled would lose its
/// tenth to it.
fn percent(v: f64) -> String {
    with_sign(format!("{v:.2}"))
}

/// The sign in front of an already formatted number. A value that only
/// *rounds* to zero is flat, not a loss: "-0.00" reads as a losing day at the
/// one glance a picture in a chat gets.
fn with_sign(body: String) -> String {
    let bare = body.trim_start_matches('-');
    if bare.chars().all(|c| c == '0' || c == '.') {
        return bare.to_owned();
    }
    if body.starts_with('-') {
        body
    } else {
        format!("+{body}")
    }
}

/// A volume in as few characters as a label beside a plot can spare.
fn short_num(v: f64) -> String {
    let abs = v.abs();
    if abs >= 1e9 {
        format!("{:.1}B", v / 1e9)
    } else if abs >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else if abs >= 1e3 {
        format!("{:.1}K", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

/// A price with as many decimals as it needs and no more: 250.1, 0.0345,
/// 0.000034. Two rules meet here — the catalog carries ticks of 0.00001, and
/// an axis that labels such a price "0" is worse than no axis at all; and a
/// narrow window around a 250-rouble share needs the decimals that tell its
/// ticks apart, or five labels read the same.
fn price_label(price: f64, step: f64) -> String {
    let abs = price.abs();
    let by_price = if abs >= 100.0 {
        1
    } else if abs >= 1.0 {
        2
    } else if abs >= 0.01 || abs == 0.0 {
        4
    } else {
        // Four digits that mean something, however small the price is.
        (3 - abs.log10().floor() as i64).clamp(4, 12) as usize
    };
    let by_step = if step.is_finite() && step > 0.0 {
        (1 - step.log10().floor() as i64).clamp(0, 12) as usize
    } else {
        0
    };
    let decimals = by_price.max(by_step).min(12);
    let text = format!("{price:.decimals$}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    if text.is_empty() || text == "-" {
        "0".into()
    } else {
        text.to_owned()
    }
}

/// `HH:MM` on the trader's clock, from Unix milliseconds.
fn clock_ms(unix_ms: i64) -> String {
    let minutes = (unix_ms + trader_offset_ms())
        .div_euclid(60_000)
        .rem_euclid(1_440);
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

/// `YYYY-MM-DD HH:MM` on the trader's clock. The date comes from the one place in the
/// tree that owns the civil calendar, rather than a second copy of it here.
fn day_text(unix_ms: i64) -> String {
    let stamp = crate::clock::format_rfc3339(unix_ms + trader_offset_ms());
    match stamp.get(..10) {
        Some(date) => format!("{date} {}", clock_ms(unix_ms)),
        None => clock_ms(unix_ms),
    }
}

/// Every price of a candle is a number.
fn finite(c: &Candle) -> bool {
    [c.open, c.high, c.low, c.close]
        .iter()
        .all(|v| v.is_finite())
}

/// Fewer bars without losing either end: groups of `k` become one candle,
/// the way a chart of a longer timeframe would draw them. Returns the group
/// size, because the title has to say the timeframe it actually shows.
///
/// Cutting the list instead (keeping the tail) throws away the entry side of
/// a long deal and draws its mark in the wrong place.
///
/// The groups are counted, not measured: an aggregated bar is anchored at the
/// first bar of its group that carries finite prices — where its data starts
/// — but the caller's `minutes * k` is its true *width* only when the group
/// had no hole in it, and a position held across a session break has
/// hour-wide holes between one-minute bars. The picture pays for that in the
/// width of a rectangle, never in where the rectangle stands. Drawing it
/// truly would take a squeeze that buckets by the clock instead of by count,
/// which is a different function from this one.
pub fn squeeze(bars: &[Candle], max: usize) -> (Vec<Candle>, usize) {
    let k = bars.len().div_ceil(max.max(1)).max(1);
    if k == 1 {
        return (bars.to_vec(), 1);
    }
    let out = bars
        .chunks(k)
        .filter_map(|group| {
            // A group of nothing but NaN would fold into the f32 sentinels,
            // which are finite and would flatten every real bar beside them.
            let good: Vec<&Candle> = group.iter().filter(|c| finite(c)).collect();
            let (first, last) = (good.first()?, good.last()?);
            Some(Candle {
                open: first.open,
                close: last.close,
                high: good.iter().map(|c| c.high).fold(f32::MIN, f32::max),
                low: good.iter().map(|c| c.low).fold(f32::MAX, f32::min),
                volume: good.iter().map(|c| c.volume).sum(),
                time: first.time,
            })
        })
        .collect();
    (out, k)
}

/// A candle's Delphi day as Unix milliseconds.
pub fn unix_ms(delphi_days: f64) -> i64 {
    ((delphi_days - DELPHI_EPOCH_DAYS) * DAY_MS).round() as i64
}

/// The plot's geometry, in device pixels and in the market's own prices.
struct Plot {
    x0: f64,
    x1: f64,
    y0: f64,
    y1: f64,
    low: f64,
    high: f64,
    /// The window the axis covers, in Unix milliseconds: what the tape spans
    /// widened to hold the deal's own marks, with air at either end — see
    /// `axis_span`. Neither end is a moment anything happened at.
    t0: i64,
    t1: i64,
}

impl Plot {
    fn y_of(&self, price: f64) -> f64 {
        let t = ((price - self.low) / (self.high - self.low)).clamp(0.0, 1.0);
        self.y1 - (self.y1 - self.y0) * t
    }

    /// Where a moment sits on the time axis. The deal's own marks are held
    /// inside the window by `axis_span`, so what still clamps here is a
    /// moment that has no business in the picture: a clock the report never
    /// filled in, or one so far out that `drawn_marks` would not let it move
    /// the axis. Clamped to the edge rather than drawn outside the plot.
    fn x_of(&self, ms: i64) -> f64 {
        if ms <= 0 || self.t1 <= self.t0 {
            return self.x1;
        }
        let t = ((ms - self.t0) as f64 / (self.t1 - self.t0) as f64).clamp(0.0, 1.0);
        self.x0 + (self.x1 - self.x0) * t
    }
}

// ----- the canvas ------------------------------------------------------------

struct Canvas {
    w: usize,
    h: usize,
    px: Vec<u8>,
}

impl Canvas {
    fn new(w: usize, h: usize) -> Self {
        Self {
            w,
            h,
            px: vec![0; w * h * 3],
        }
    }

    fn fill(&mut self, c: Rgb) {
        for p in self.px.as_chunks_mut::<3>().0 {
            p.copy_from_slice(&c);
        }
    }

    /// One pixel, `a` of the way from what is there to `c`. Everything else
    /// on this canvas is coverage turned into an alpha and handed to this.
    fn blend(&mut self, x: i64, y: i64, c: Rgb, a: f64) {
        if a.is_nan() || a <= 0.0 || x < 0 || y < 0 {
            return;
        }
        let (x, y) = (x as usize, y as usize);
        if x >= self.w || y >= self.h {
            return;
        }
        let a = a.min(1.0);
        let i = (y * self.w + x) * 3;
        for (k, &ink) in c.iter().enumerate() {
            let under = f64::from(self.px[i + k]);
            self.px[i + k] = (under + (f64::from(ink) - under) * a).round() as u8;
        }
    }

    /// An axis-aligned rectangle whose edges fall where they fall: a pixel
    /// the rectangle only half covers is only half painted.
    fn rect(&mut self, x0: f64, y0: f64, x1: f64, y1: f64, c: Rgb, a: f64) {
        if ![x0, y0, x1, y1].iter().all(|v| v.is_finite()) {
            return;
        }
        let (x0, x1) = (x0.min(x1), x0.max(x1));
        let (y0, y1) = (y0.min(y1), y0.max(y1));
        for py in span(y0, y1, self.h) {
            let cy = cover(py, y0, y1);
            if cy <= 0.0 {
                continue;
            }
            for px in span(x0, x1, self.w) {
                let cx = cover(px, x0, x1);
                self.blend(px, py, c, a * cx * cy);
            }
        }
    }

    /// A straight line of a given width, antialiased: a pixel is painted by
    /// how far its centre is from the segment. Bresenham's staircase is what
    /// a chart drawn by hand looks like, and it is exactly what TMB's does
    /// not.
    fn line(&mut self, from: (f64, f64), to: (f64, f64), width: f64, c: Rgb, a: f64) {
        let ((x0, y0), (x1, y1)) = (from, to);
        if ![x0, y0, x1, y1].iter().all(|v| v.is_finite()) {
            return;
        }
        let r = width.max(0.5) / 2.0;
        for py in span(y0.min(y1) - r - 1.0, y0.max(y1) + r + 1.0, self.h) {
            for px in span(x0.min(x1) - r - 1.0, x0.max(x1) + r + 1.0, self.w) {
                let d = seg_distance((px as f64 + 0.5, py as f64 + 0.5), from, to);
                self.blend(px, py, c, a * (r + 0.5 - d).clamp(0.0, 1.0));
            }
        }
    }

    /// A shape rasterised once and put down wherever it is needed, by its
    /// own centre.
    fn stamp(&mut self, centre: (f64, f64), stamp: &Stamp, c: Rgb, a: f64) {
        if !(centre.0.is_finite() && centre.1.is_finite()) {
            return;
        }
        let x0 = (centre.0 - stamp.centre).round() as i64;
        let y0 = (centre.1 - stamp.centre).round() as i64;
        for row in 0..stamp.size {
            for col in 0..stamp.size {
                let cover = stamp.cover[row * stamp.size + col];
                if cover > 0.0 {
                    self.blend(x0 + col as i64, y0 + row as i64, c, a * cover);
                }
            }
        }
    }

    /// A filled circle, antialiased the same way.
    fn disc(&mut self, centre: (f64, f64), r: f64, c: Rgb, a: f64) {
        let (cx0, cy0) = centre;
        if !(cx0.is_finite() && cy0.is_finite() && r.is_finite()) {
            return;
        }
        for py in span(cy0 - r - 1.0, cy0 + r + 1.0, self.h) {
            for px in span(cx0 - r - 1.0, cx0 + r + 1.0, self.w) {
                let d = ((px as f64 + 0.5 - cx0).powi(2) + (py as f64 + 0.5 - cy0).powi(2)).sqrt();
                self.blend(px, py, c, a * (r + 0.5 - d).clamp(0.0, 1.0));
            }
        }
    }

    /// A string at `at`, anchored inside its own box by `anchor`: 0 is the
    /// left edge and the baseline, 1 the right edge and the top of a digit,
    /// so `(0.5, 0.5)` centres a label on a point. Anything the face has no
    /// glyph for is a gap, never a panic: this runs inside the trading
    /// process.
    fn text(&mut self, at: (f64, f64), anchor: (f64, f64), s: &str, size: f64, ink: (Rgb, f64)) {
        let Some(face) = font() else {
            return;
        };
        let (c, a) = ink;
        let scale = PxScale::from(size as f32);
        let advance = f64::from(face.as_scaled(scale).h_advance(face.glyph_id('0')));
        let mut pen = at.0 - anchor.0 * advance * s.chars().count() as f64;
        let baseline = at.1 + anchor.1 * cap_height(size);
        for ch in s.chars() {
            let glyph = face
                .glyph_id(ch)
                .with_scale_and_position(scale, point(pen as f32, baseline as f32));
            if let Some(outline) = face.outline_glyph(glyph) {
                let bounds = outline.px_bounds();
                let (ox, oy) = (f64::from(bounds.min.x), f64::from(bounds.min.y));
                outline.draw(|gx, gy, coverage| {
                    self.blend(
                        (ox + f64::from(gx)) as i64,
                        (oy + f64::from(gy)) as i64,
                        c,
                        a * f64::from(coverage),
                    );
                });
            }
            pen += advance;
        }
    }
}

/// How far a point lies from a segment — the one piece of arithmetic every
/// antialiased stroke on this canvas is made of.
fn seg_distance(p: (f64, f64), from: (f64, f64), to: (f64, f64)) -> f64 {
    let ((x0, y0), (x1, y1)) = (from, to);
    let (dx, dy) = (x1 - x0, y1 - y0);
    let len2 = dx * dx + dy * dy;
    let t = if len2 > 0.0 {
        (((p.0 - x0) * dx + (p.1 - y0) * dy) / len2).clamp(0.0, 1.0)
    } else {
        0.0
    };
    ((p.0 - (x0 + dx * t)).powi(2) + (p.1 - (y0 + dy * t)).powi(2)).sqrt()
}

/// A little square of coverage, rasterised once and stamped many times. The
/// tape's cross is the same shape every print, and working its antialiasing
/// out again for each of tens of thousands of them was most of what the
/// picture cost the trading loop.
struct Stamp {
    size: usize,
    /// Where the shape's own centre sits inside the square.
    centre: f64,
    cover: Vec<f64>,
}

impl Stamp {
    /// The cross a print is drawn as: two diagonals of `a` half-length.
    fn cross(a: f64, width: f64) -> Self {
        let r = width.max(0.5) / 2.0;
        let size = ((a + r + 1.0) * 2.0).ceil() as usize;
        let centre = size as f64 / 2.0;
        let mut cover = vec![0.0; size * size];
        for row in 0..size {
            for col in 0..size {
                let p = (col as f64 + 0.5 - centre, row as f64 + 0.5 - centre);
                let arm = |from, to| (r + 0.5 - seg_distance(p, from, to)).clamp(0.0, 1.0);
                let (one, other) = (arm((-a, -a), (a, a)), arm((-a, a), (a, -a)));
                // What two strokes laid over each other come to, rather than
                // the nearer of the two: where the arms meet, the second
                // stroke darkens what the first had already put down, and
                // the nearer distance alone would quietly thin the middle of
                // every cross on the picture.
                cover[row * size + col] = 1.0 - (1.0 - one) * (1.0 - other);
            }
        }
        Self {
            size,
            centre,
            cover,
        }
    }
}

/// The pixel rows or columns a shape can touch, clipped to the canvas — the
/// clip is what keeps a coordinate that went wild from turning into a loop
/// over a billion pixels.
fn span(lo: f64, hi: f64, limit: usize) -> std::ops::Range<i64> {
    let start = (lo.floor()).clamp(0.0, limit as f64) as i64;
    let end = (hi.ceil()).clamp(0.0, limit as f64) as i64;
    start..end.max(start)
}

/// How much of pixel `p` the interval `lo..hi` covers, 0 to 1.
fn cover(p: i64, lo: f64, hi: f64) -> f64 {
    let p = p as f64;
    ((p + 1.0).min(hi) - p.max(lo)).clamp(0.0, 1.0)
}

// ----- the font --------------------------------------------------------------

/// Go Mono — the face TMB's charts are drawn with, vendored beside its
/// licence (`assets/Go-Mono-LICENSE`) so the picture does not depend on what
/// the machine that runs the core happens to have installed.
const FONT_BYTES: &[u8] = include_bytes!("../assets/Go-Mono.ttf");

/// Parsed once. A font that would not parse is not a reason to take the
/// trading process down — the chart simply comes out without its text, and
/// the test below is what keeps that from ever shipping.
fn font() -> Option<&'static FontRef<'static>> {
    static FACE: OnceLock<Option<FontRef<'static>>> = OnceLock::new();
    FACE.get_or_init(|| FontRef::try_from_slice(FONT_BYTES).ok())
        .as_ref()
}

/// How wide a string comes out — the face is monospaced, so it is the one
/// advance times the count.
fn measure(s: &str, size: f64) -> f64 {
    let Some(face) = font() else {
        return 0.0;
    };
    let scaled = face.as_scaled(PxScale::from(size as f32));
    f64::from(scaled.h_advance(face.glyph_id('0'))) * s.chars().count() as f64
}

/// The height of a digit at that size: what "the top of the text" means when
/// a label is anchored by its box rather than by its baseline.
fn cap_height(size: f64) -> f64 {
    let Some(face) = font() else {
        return size * 0.7;
    };
    let glyph = face.glyph_id('0').with_scale(PxScale::from(size as f32));
    face.outline_glyph(glyph)
        .map_or(size * 0.7, |o| f64::from(o.px_bounds().height()))
}

// ----- the file itself -------------------------------------------------------

/// A PNG: signature, IHDR, one IDAT of the zlib-deflated scanlines, IEND.
fn png(w: usize, h: usize, rgb: &[u8]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(h * (w * 3 + 1));
    for y in 0..h {
        // Filter 0 (none): the picture is flat colour, and deflate takes care
        // of the repetition.
        raw.push(0);
        raw.extend_from_slice(&rgb[y * w * 3..(y + 1) * w * 3]);
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    let data = encoder
        .write_all(&raw)
        .and_then(|()| encoder.finish())
        .unwrap_or_default();

    let mut out = Vec::with_capacity(data.len() + 128);
    out.extend_from_slice(&[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]);
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&(w as u32).to_be_bytes());
    ihdr.extend_from_slice(&(h as u32).to_be_bytes());
    // 8 bits per channel, colour type 2 (truecolour), no interlace.
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
    chunk(&mut out, b"IHDR", &ihdr);
    chunk(&mut out, b"IDAT", &data);
    chunk(&mut out, b"IEND", &[]);
    out
}

fn chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc = Crc::new();
    crc.update(kind);
    crc.update(data);
    out.extend_from_slice(&crc.sum().to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bars(n: usize) -> Vec<Candle> {
        (0..n)
            .map(|i| {
                let base = 100.0 + (i as f32) * 0.25;
                Candle {
                    open: base,
                    high: base + 0.4,
                    low: base - 0.3,
                    close: base + 0.1,
                    volume: 1_000.0 + i as f32,
                    time: DELPHI_EPOCH_DAYS + (1_790_496_000_000.0 + i as f64 * 60_000.0) / DAY_MS,
                }
            })
            .collect()
    }

    fn deal<'a>(candles: &[Candle]) -> Deal<'a> {
        Deal {
            market: "SBER",
            minutes: 1,
            short: false,
            entry: 100.5,
            entry_ms: unix_ms(candles[0].time),
            exit: 102.0,
            exit_ms: unix_ms(candles[candles.len() - 1].time),
            stop: Some(99.0),
            take: Some(103.0),
            entry_moves: &[],
            exit_moves: &[],
            caption: "+150.00 USDT (+1.00%)",
            outcome: Some(Outcome {
                profit: 150.0,
                spent: 15_000.0,
                session: 1_240.0,
            }),
        }
    }

    #[test]
    fn a_deal_becomes_a_png_a_decoder_would_accept() {
        let candles = bars(30);
        let png = deal_png(Series::Bars(&candles), &deal(&candles)).expect("a chart");
        assert_eq!(&png[..8], &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]);
        // IHDR first, with our own size, and IEND last.
        assert_eq!(&png[12..16], b"IHDR");
        assert_eq!(
            u32::from_be_bytes(png[16..20].try_into().unwrap()),
            WIDTH as u32
        );
        assert_eq!(
            u32::from_be_bytes(png[20..24].try_into().unwrap()),
            HEIGHT as u32
        );
        assert_eq!(&png[png.len() - 8..png.len() - 4], b"IEND");
        // Every chunk's CRC is its own: walk them the way a decoder does.
        let mut at = 8;
        let mut kinds = Vec::new();
        while at + 12 <= png.len() {
            let len = u32::from_be_bytes(png[at..at + 4].try_into().unwrap()) as usize;
            let kind = &png[at + 4..at + 8];
            let body = &png[at + 8..at + 8 + len];
            let mut crc = Crc::new();
            crc.update(kind);
            crc.update(body);
            let want = u32::from_be_bytes(png[at + 8 + len..at + 12 + len].try_into().unwrap());
            assert_eq!(crc.sum(), want, "chunk {:?}", String::from_utf8_lossy(kind));
            kinds.push(String::from_utf8_lossy(kind).to_string());
            at += 12 + len;
        }
        assert_eq!(at, png.len(), "no bytes past the last chunk");
        assert_eq!(kinds, ["IHDR", "IDAT", "IEND"]);
    }

    /// Nothing to draw is `None`, not a blank picture the chat has to squint
    /// at — the text already said what happened.
    #[test]
    fn nothing_to_draw_is_no_picture() {
        assert!(deal_png(Series::Bars(&[]), &deal(&bars(2))).is_none());
        let flat: Vec<Candle> = bars(3)
            .into_iter()
            .map(|c| Candle {
                open: 100.0,
                high: 100.0,
                low: 100.0,
                close: 100.0,
                ..c
            })
            .collect();
        let mut d = deal(&flat);
        (d.entry, d.exit, d.stop, d.take) = (100.0, 100.0, None, None);
        assert!(
            deal_png(Series::Bars(&flat), &d).is_none(),
            "one flat line is not a chart"
        );
    }

    #[test]
    fn a_price_maps_onto_the_plot_and_stays_inside_it() {
        let plot = Plot {
            x0: 10.0,
            x1: 100.0,
            y0: 20.0,
            y1: 120.0,
            low: 100.0,
            high: 200.0,
            t0: 0,
            t1: 60_000,
        };
        assert_eq!(plot.y_of(200.0), 20.0, "the top of the range is the top");
        assert_eq!(plot.y_of(100.0), 120.0);
        assert_eq!(plot.y_of(150.0), 70.0);
        // Anything outside is clamped, never drawn off the canvas.
        assert_eq!(plot.y_of(1_000.0), 20.0);
        assert_eq!(plot.y_of(-5.0), 120.0);
    }

    /// The ladder, which is what makes two deals comparable: the range is a
    /// fixed percentage of the centre rather than a snug fit around the data
    /// — until the data is wider than the ladder goes, and fitting it is the
    /// only way to keep all of it on the picture.
    #[test]
    fn the_y_scale_climbs_a_ladder() {
        assert_eq!(pick_scale(0.1), 2.0);
        assert_eq!(pick_scale(2.0), 2.0);
        assert_eq!(pick_scale(2.01), 5.0);
        assert_eq!(pick_scale(60.0), 75.0);
        // Past the top rung it is the top rung — `y_scale` never asks, so
        // this is only what keeps the function total.
        assert_eq!(pick_scale(400.0), 100.0);
        // A 1% window around 100 lands on the 2% rung, centred on the data.
        let (low, high, pct) = y_scale(99.5, 100.5);
        assert_eq!(pct, 2.0);
        assert!((low - 99.0).abs() < 1e-9, "{low}");
        assert!((high - 101.0).abs() < 1e-9, "{high}");
        // Everything that had to fit still fits, with the slack to spare.
        assert!(low < 99.5 && high > 100.5);
        // Wider than the top rung: the ladder steps aside rather than cut
        // the ends off the picture, and the title says the width it used.
        // Everything downstream leaves out what falls outside the frame, so
        // a clipped frame is a stop that quietly vanishes.
        let (low, high, pct) = y_scale(100.0, 400.0);
        assert!(low < 100.0 && high > 400.0, "{low}..{high}");
        assert!(pct > 100.0, "{pct}");
        // A centre that is not a price has no percentage: the old margin,
        // and a title with no scale in it.
        let (low, high, pct) = y_scale(-2.0, 1.0);
        assert_eq!(pct, 0.0);
        assert!(low < -2.0 && high > 1.0);
    }

    /// A deal whose minute the feed has not published yet sits on the edge of
    /// the window instead of outside the picture (anonymous ISS lags ~15 min).
    #[test]
    fn a_moment_past_the_window_is_clamped_to_it() {
        let candles = bars(10);
        let plot = Plot {
            x0: 0.0,
            x1: 100.0,
            y0: 0.0,
            y1: 100.0,
            low: 1.0,
            high: 2.0,
            t0: unix_ms(candles[0].time),
            t1: unix_ms(candles[9].time) + 60_000,
        };
        assert_eq!(plot.x_of(plot.t1 + 600_000), plot.x1);
        assert_eq!(plot.x_of(plot.t0 - 600_000), plot.x0);
        assert_eq!(plot.x_of(0), plot.x1, "no time, no mark");
    }

    /// The axis is time, not the number of the bar: a market that traded in
    /// the first minute and then again an hour later draws two candles an
    /// hour apart, not two candles side by side.
    #[test]
    fn a_hole_in_the_bars_is_a_hole_on_the_axis() {
        let plot = Plot {
            x0: 0.0,
            x1: 120.0,
            y0: 0.0,
            y1: 100.0,
            low: 1.0,
            high: 2.0,
            t0: 0,
            t1: 60 * 60_000,
        };
        // A third of the way through the hour is a third of the way across.
        assert_eq!(plot.x_of(20 * 60_000), 40.0);
        assert_eq!(plot.x_of(30 * 60_000), 60.0);
    }

    /// The axis picks a step a trader reads, and never a step of zero — one
    /// would be a loop that does not end.
    #[test]
    fn the_time_step_is_a_round_number_of_minutes() {
        assert_eq!(time_step(0), 60_000, "an empty window still steps");
        assert_eq!(time_step(-1), 60_000);
        // Six labels over 36 minutes: every 10 minutes, the next step up.
        assert_eq!(time_step(36 * 60_000), 10 * 60_000);
        assert_eq!(time_step(6 * 60_000), 60_000);
        assert_eq!(time_step(24 * 3_600_000), 360 * 60_000);
        // Longer than the longest step is still the longest step.
        assert_eq!(time_step(i64::MAX / 2), 10_080 * 60_000);
    }

    /// The numbers under the caption: the window's own move, and the deal's
    /// money when there is a deal behind the picture.
    #[test]
    fn the_stats_line_says_the_window_and_the_money() {
        let candles = bars(10);
        let line = stats_line(&Track::Bars(candles.clone()), &deal(&candles));
        assert!(line.starts_with("WINDOW +"), "{line}");
        assert!(line.contains("POS 15000 USDT"), "{line}");
        assert!(line.contains("DAY +1240 USDT"), "{line}");
        // A market chart has no position and no day, and says so by saying
        // what it does have instead.
        let mut market = deal(&candles);
        market.outcome = None;
        let line = stats_line(&Track::Bars(candles.clone()), &market);
        assert!(line.contains("BARS 10"), "{line}");
        assert!(!line.contains("POS"), "{line}");
    }

    fn tape(n: usize) -> Vec<HistoryTrade> {
        (0..n)
            .map(|i| HistoryTrade {
                time: DELPHI_EPOCH_DAYS as f32 as f64
                    + (1_790_496_000_000.0 + i as f64 * 5_000.0) / DAY_MS,
                price: 100.0 + (i as f32) * 0.05,
                // The sign is the side: bought stacks over sold in the
                // volume layer, and colours the cross.
                qty: if i % 3 == 0 { -40.0 } else { 25.0 },
            })
            .collect()
    }

    /// A deal that lived four seconds has no candle of its own; the tape has
    /// every print of it, and that is what the picture is drawn from.
    #[test]
    fn a_tape_draws_a_chart_of_its_own() {
        let ticks = tape(60);
        let mut d = deal(&bars(4));
        (d.entry, d.exit) = (100.5, 102.0);
        (d.entry_ms, d.exit_ms) = (unix_ms(ticks[5].time), unix_ms(ticks[50].time));
        (d.stop, d.take) = (Some(99.0), Some(103.0));
        let png = deal_png(Series::Ticks(&ticks), &d).expect("a chart");
        assert_eq!(&png[..8], &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]);
        // No tape at all is no picture, the same as no bars.
        assert!(deal_png(Series::Ticks(&[]), &d).is_none());
        // A tape of one print is a plot of zero width unless the span is
        // opened up; it must draw rather than divide by nothing.
        let one = tape(1);
        let mut flat = deal(&bars(4));
        (flat.entry, flat.exit, flat.stop, flat.take) = (0.0, 0.0, Some(90.0), None);
        assert!(deal_png(Series::Ticks(&one), &flat).is_some());
    }

    /// A bar owns the period that follows it and a print does not, which is
    /// what keeps the last bar inside the plot and stops the tick picture
    /// dating itself a minute late.
    #[test]
    fn a_bar_owns_the_minute_after_it_and_a_print_does_not() {
        let ticks = tape(5);
        let bars = bars(5);
        assert_eq!(
            Track::Bars(bars.clone()).span(60_000).1,
            unix_ms(bars[4].time) + 60_000
        );
        assert_eq!(
            Track::Ticks(ticks.clone()).span(60_000).1,
            unix_ms(ticks[4].time)
        );
    }

    /// The axis is the deal's, not the tape's. An emulated fill lands on the
    /// book, and an instrument that has not printed since leaves every tick
    /// of the window older than the deal — which used to clamp both marks to
    /// the frame's edge, under the price chips, and put the deal at a moment
    /// it never happened at.
    #[test]
    fn the_axis_holds_the_deal_even_when_the_tape_stops_short() {
        let candles = bars(10);
        let mut d = deal(&candles);
        let tape = (0, 600_000);
        // The deal closes two minutes past the last print there is.
        (d.entry_ms, d.exit_ms) = (720_000, 720_000);
        let (t0, t1) = axis_span(tape, &d);
        assert!(t1 > d.exit_ms, "the exit is inside the window, not on it");
        assert!(t0 < tape.0, "and the tape's own start keeps its air");
        // Strictly inside, which is what puts the mark on the plot instead
        // of under the chip on its edge.
        let plot = Plot {
            x0: 0.0,
            x1: 100.0,
            y0: 0.0,
            y1: 100.0,
            low: 1.0,
            high: 2.0,
            t0,
            t1,
        };
        assert!(plot.x_of(d.exit_ms) < plot.x1, "{}", plot.x_of(d.exit_ms));
        assert!(plot.x_of(tape.0) > plot.x0);
        // A deal inside the tape leaves the window to the tape, plus air.
        (d.entry_ms, d.exit_ms) = (100_000, 300_000);
        let (t0, t1) = axis_span(tape, &d);
        assert!(t0 < 0 && t1 > 600_000);
        // A moment the report never filled in does not drag the axis to 1970.
        (d.entry_ms, d.exit_ms) = (0, 0);
        assert_eq!(axis_span(tape, &d), (-24_000, 624_000));
        // A synthetic entry — a clock with no price behind it — is never
        // drawn, so it must not move the axis either, or the tape pays its
        // width for a mark nobody sees.
        (d.entry, d.entry_ms) = (0.0, 720_000);
        (d.exit, d.exit_ms) = (102.0, 300_000);
        assert_eq!(drawn_marks(&d), vec![300_000]);
        assert_eq!(axis_span(tape, &d), (-24_000, 624_000));
        // A clock a week out is real enough — a position held over the
        // weekend has one — but an axis that reaches it leaves the tape a
        // single column, so the axis stays where it is and the mark pins to
        // the frame's edge.
        (d.entry, d.entry_ms) = (100.5, 7 * 86_400_000);
        assert_eq!(axis_marks(tape, &d), vec![300_000]);
        assert_eq!(axis_span(tape, &d), (-24_000, 624_000));
        // And it still counts as a mark past the tape, which is what draws
        // the dashed edge that says so: without it the pinned mark would
        // read as a moment at the tape's own end.
        assert!(drawn_marks(&d).iter().any(|m| *m > tape.1));
    }

    /// Which print is first is the clock's business, not the list's.
    #[test]
    fn the_ends_come_from_the_clock_not_from_the_order() {
        let ticks = tape(6);
        let mut shuffled = ticks.clone();
        shuffled.reverse();
        let ends = |t: Vec<HistoryTrade>| {
            let (open, close, n, _) = Track::of(&Series::Ticks(&t)).ends().expect("ends");
            (open, close, n)
        };
        assert_eq!(ends(ticks.clone()), ends(shuffled.clone()));
        // And the track itself is put in order at the door, so nothing
        // downstream has to sort it a second time.
        let Track::Ticks(sorted) = Track::of(&Series::Ticks(&shuffled)) else {
            panic!("a tape");
        };
        assert!(sorted.windows(2).all(|w| w[0].time <= w[1].time));
    }

    /// The tape's own line of numbers counts prints, not bars.
    #[test]
    fn the_stats_line_counts_prints_on_a_tape() {
        let ticks = tape(10);
        let mut market = deal(&bars(4));
        market.outcome = None;
        let line = stats_line(&Track::Ticks(ticks), &market);
        assert!(line.contains("TICKS 10"), "{line}");
        // 4 sells of 40 and 6 buys of 25: the layer's scale is the total, and
        // the side is the sign, so the volume is the sum of the sizes.
        assert!(line.contains("VOL 310"), "{line}");
    }

    /// Fewer bars, both ends kept: the entry side of a long deal must not be
    /// the part that disappears.
    #[test]
    fn a_long_window_is_aggregated_not_cut() {
        let long = bars(300);
        let (out, group) = squeeze(&long, 240);
        assert_eq!(group, 2);
        assert_eq!(out.len(), 150);
        assert_eq!(
            out[0].open, long[0].open,
            "the first bar is still the first"
        );
        assert_eq!(
            out[out.len() - 1].close,
            long[299].close,
            "and the last is still the last"
        );
        assert_eq!(out[0].high, long[0].high.max(long[1].high));
        assert_eq!(out[0].low, long[0].low.min(long[1].low));
        assert_eq!(out[0].volume, long[0].volume + long[1].volume);
        // Nothing to squeeze is left alone.
        let (same, group) = squeeze(&long, 1_000);
        assert_eq!((same.len(), group), (300, 1));
    }

    #[test]
    fn a_label_says_the_price_and_the_clock_says_moscow() {
        assert_eq!(price_label(250.14, 1.0), "250.1");
        assert_eq!(price_label(2.5, 0.1), "2.5");
        assert_eq!(price_label(0.034_51, 0.001), "0.0345");
        // A tick of 0.00001 is in the catalog: such a price is not "0".
        assert_eq!(price_label(0.000_034_5, 0.00001), "0.0000345");
        assert_eq!(price_label(0.000_001_234_5, 0.0000001), "0.000001234");
        assert_eq!(price_label(0.0, 1.0), "0");
        // A narrow window around a big price: the labels must differ.
        assert_eq!(price_label(276.72, 0.01), "276.72");
        assert_ne!(price_label(276.72, 0.01), price_label(276.73, 0.01));
        // 2026-09-27 07:00 UTC is 10:00 in Moscow.
        assert_eq!(clock_ms(1_790_492_400_000), "10:00");
        assert_eq!(
            unix_ms(DELPHI_EPOCH_DAYS + 1_790_492_400_000.0 / DAY_MS),
            1_790_492_400_000
        );
        assert_eq!(day_text(1_790_492_400_000), "2026-09-27 10:00");
        // The volume label is short enough to sit beside the plot.
        assert_eq!(short_num(5_030_000.0), "5.0M");
        assert_eq!(short_num(940.0), "940");
    }

    /// A day that sums to float noise is flat, not a loss — and a percent is
    /// not a USDT amount, so it keeps its decimals however big it gets.
    #[test]
    fn a_result_that_only_rounds_to_zero_is_flat() {
        assert_eq!(signed(-0.000_001), "0.00");
        assert_eq!(signed(0.0), "0.00");
        assert_eq!(signed(1_240.0), "+1240");
        assert_eq!(signed(-540.4), "-540");
        assert_eq!(percent(153.7), "+153.70");
        assert_eq!(percent(-1.2), "-1.20");
        assert_eq!(percent(-0.000_4), "0.00");
    }

    /// The face is vendored, it parses, and it is monospaced — a picture
    /// whose text silently disappeared is the one failure the byte-counting
    /// tests above would not notice.
    #[test]
    fn the_vendored_face_draws() {
        let face = font().expect("Go Mono parses");
        assert!(face.glyph_id('0').0 != 0, "the face has digits");
        let one = measure("0", FONT_SMALL);
        assert!(one > 0.0, "a glyph has a width");
        assert!(
            (measure("00000", FONT_SMALL) - one * 5.0).abs() < 1e-9,
            "monospaced: five glyphs are five advances"
        );
        assert!(
            cap_height(FONT_SMALL) > 0.0 && cap_height(FONT_SMALL) < FONT_SMALL,
            "a digit is shorter than its em"
        );
    }

    /// Text lands on the canvas where it was asked to, and only there: an
    /// anchor that drifted would put the price chips over the plot.
    #[test]
    fn a_label_is_painted_inside_its_own_box() {
        let mut canvas = Canvas::new(200, 60);
        canvas.fill(PANEL);
        canvas.text((100.0, 30.0), (0.5, 0.5), "42", 24.0, (INK, 1.0));
        let width = measure("42", 24.0);
        let painted = |x: usize, y: usize| {
            let i = (y * canvas.w + x) * 3;
            canvas.px[i..i + 3] != PANEL
        };
        let mut any = false;
        for y in 0..canvas.h {
            for x in 0..canvas.w {
                if painted(x, y) {
                    any = true;
                    assert!(
                        (x as f64) >= 100.0 - width / 2.0 - 2.0
                            && (x as f64) <= 100.0 + width / 2.0 + 2.0,
                        "a glyph at x={x} is outside the centred box"
                    );
                    assert!(y > 10 && y < 50, "a glyph at y={y} is outside the line");
                }
            }
        }
        assert!(any, "the label was drawn at all");
    }
}
