//! The price snapshots M0 answers `UpdateMarketsList` from, refreshed off the
//! UDP loop.
//!
//! M1 replaces this with the WebSocket streams (`depth20@100ms`,
//! `markPrice@1s`). Until then the prices come from two REST calls — and they
//! cannot be made from the loop that receives UDP: `ureq` is synchronous and
//! those calls take 0.3–0.6 s measured 01.10, which is 0.3–0.6 s of a trading
//! core not reading its socket. So this is threads that send snapshots, the
//! same shape `feed.rs` will have in M1: the loop only ever applies what has
//! already arrived.
//!
//! One thread per call, and a REST client per thread. The two calls have
//! periods thirty times apart and the same 30 s dead-gateway timeout
//! (`rest::CALL_TIMEOUT`), so sharing a thread would let one slow funding
//! answer hold the book still for half a minute — the whole point of this
//! module is that nothing waits behind anything else.
//!
//! What a thread does NOT do is hide an outage. Two things make a failure
//! visible rather than silent: a refusal that repeats past [`stale_after`]
//! raises a warning and **clears the prices** (an empty snapshot, which
//! `Catalog::apply_book` reads as "nothing is quoted"), and a throttle or a ban
//! backs off instead of hammering. A core that cannot reach the exchange must
//! stop presenting its last prices as current — that is the difference between
//! a terminal showing no price and a terminal showing a price that is minutes
//! old.

use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::aster::json::{BookTicker, PremiumIndex};
use crate::aster::rest::{self, Rest};

/// Top of book for the whole catalog, every this often.
///
/// Matched to the client's own cadence: `RefreshConfig::update_markets_every`
/// is 2 s by default, so the terminal asks for prices every 2 seconds and
/// anything slower here would answer the same numbers twice. The call costs
/// weight 2 (measured 01.10), i.e. 60 of the 2400/min ceiling at this period —
/// the cheapest thing the core does.
pub const BOOK_PERIOD: Duration = Duration::from_secs(2);
/// Funding and mark price for the whole catalog, every this often.
///
/// Funding is charged on a shared schedule — measured 01.10, two distinct
/// `nextFundingTime` values across the entire catalog — so it moves in hours,
/// not seconds. The mark price does move, and a minute of staleness on it is
/// visible in the terminal's risk column; this is the price of reading it over
/// REST at all, and M1's `markPrice@1s` stream is what makes it live. It is not
/// read at the book's period because the answer is 167 KB against 88 KB and
/// carries 766 rows for the 596 markets that want them.
pub const PREMIUM_PERIOD: Duration = Duration::from_secs(60);
/// How long a feed backs off after the exchange says "too many requests".
///
/// Aster answers `-1003`/`-1015` or HTTP 429, and 418 for an IP the gateway has
/// already banned. Retrying a ban at the ordinary period is how a throttle
/// becomes a longer ban, so the next call waits a minute whatever the period
/// is. The header-driven budget (`x-mbx-used-weight-1m`, the three published
/// windows) belongs to `api_meter.rs` in M4; this is the floor under it.
const THROTTLED_BACKOFF: Duration = Duration::from_secs(60);

/// How old the newest good answer may get before the feed gives up on it and
/// clears the prices.
///
/// Five missed periods, and never less than half a minute: one refused call is
/// a blip that costs a period of staleness, while five in a row is an outage
/// the trader has to know about. On the book that is 30 s, on the funding
/// answer 5 minutes.
fn stale_after(period: Duration) -> Duration {
    (period * 5).max(Duration::from_secs(30))
}

/// One answer from the exchange, as the UDP loop applies it.
///
/// An empty `Vec` is not a no-op: it is the statement that nothing is quoted,
/// which is what a feed sends when its outage has outlived [`stale_after`].
pub enum Snapshot {
    Book(Vec<BookTicker>),
    Premium(Vec<PremiumIndex>),
}

/// The running refreshers: what they send, and whether they are still there.
///
/// The threads are held rather than detached for one reason: the channel they
/// share closes only when the LAST sender drops, so a receiver alone cannot
/// tell one dead feed from two live ones. A panicked book feed would otherwise
/// leave the bid and the ask frozen for as long as the core runs, with the
/// funding feed holding the channel open and the journal saying nothing.
pub struct Feeds {
    rx: Receiver<Snapshot>,
    threads: Vec<(&'static str, JoinHandle<()>)>,
}

impl Feeds {
    /// The next snapshot, if one has arrived.
    pub fn try_recv(&self) -> Result<Snapshot, TryRecvError> {
        self.rx.try_recv()
    }

    /// The first feed that has stopped, by name.
    ///
    /// A feed ends only by panicking or by finding the receiver gone, and the
    /// second cannot happen while this struct is alive — so a name here means a
    /// panic, and the prices it refreshed will never move again.
    pub fn dead(&self) -> Option<&'static str> {
        self.threads
            .iter()
            .find(|(_, t)| t.is_finished())
            .map(|(name, _)| *name)
    }
}

/// Start both refreshers. The threads end when [`Feeds`] is dropped, which is
/// how the core's own exit stops them — there is no stop flag to forget to
/// set.
///
/// The first call of each is one period away, not immediate: the startup
/// catalog read already took both answers (`main.rs`), and repeating them at
/// once would spend the weight to learn what the core just learnt.
pub fn start() -> Feeds {
    let (tx, rx) = mpsc::channel();
    let threads = vec![
        spawn_feed(
            "bookTicker",
            BOOK_PERIOD,
            tx.clone(),
            |rest| rest.book_ticker_all(),
            Snapshot::Book,
        ),
        spawn_feed(
            "premiumIndex",
            PREMIUM_PERIOD,
            tx,
            |rest| rest.premium_index_all(),
            Snapshot::Premium,
        ),
    ];
    Feeds { rx, threads }
}

fn spawn_feed<T, F, W>(
    name: &'static str,
    period: Duration,
    tx: Sender<Snapshot>,
    fetch: F,
    wrap: W,
) -> (&'static str, JoinHandle<()>)
where
    T: Send + 'static,
    F: Fn(&mut Rest) -> Result<Vec<T>, rest::Error> + Send + 'static,
    W: Fn(Vec<T>) -> Snapshot + Send + 'static,
{
    let thread = thread::Builder::new()
        .name(format!("prices:{name}"))
        .spawn(move || {
            // A client per thread: `ureq` pools a connection per agent, and the
            // two feeds would otherwise contend for one.
            let mut rest = Rest::new();
            run(name, period, &mut rest, &tx, &fetch, &wrap);
        })
        .expect("spawn price thread");
    (name, thread)
}

fn run<T>(
    name: &'static str,
    period: Duration,
    rest: &mut Rest,
    tx: &Sender<Snapshot>,
    fetch: &impl Fn(&mut Rest) -> Result<Vec<T>, rest::Error>,
    wrap: &impl Fn(Vec<T>) -> Snapshot,
) {
    let stale = stale_after(period);
    let mut due = Instant::now() + period;
    // The startup read is the first good answer, so the staleness clock starts
    // from here and not from the first call of this thread.
    let mut last_ok = Instant::now();
    let mut given_up = false;
    loop {
        let now = Instant::now();
        if now < due {
            thread::sleep(due - now);
        }
        // From the call's START, so a call slower than its own period cannot
        // make the next one due before it returns.
        due = Instant::now() + period;
        match fetch(rest) {
            Ok(rows) => {
                if rows.is_empty() {
                    log::warn!("prices: {name} answered no rows at all");
                }
                last_ok = Instant::now();
                if given_up {
                    log::warn!("prices: {name} answers again");
                    given_up = false;
                }
                // A send failure means the core is gone; so is the reason to
                // keep calling the exchange.
                if tx.send(wrap(rows)).is_err() {
                    return;
                }
            }
            Err(e) => {
                let age = last_ok.elapsed();
                if throttled(&e) {
                    // Said at warn even on the first one: this is the core
                    // being told it asks too often, and the next call waits.
                    log::warn!("prices: {name} throttled ({e}), backing off {THROTTLED_BACKOFF:?}");
                    due = Instant::now() + THROTTLED_BACKOFF;
                } else {
                    // One refusal costs a period of staleness. At debug because
                    // a blip at this cadence would otherwise fill the journal;
                    // the bound below is what turns a run of them into a line
                    // the trader sees.
                    log::debug!("prices: {name}: {e}");
                }
                // Checked for EVERY refusal, the throttled one included: a ban
                // freezes the prices exactly as a dead gateway does, and it is
                // the longer outage of the two. Deciding this inside the branch
                // above is what would let a ban keep stale prices alive
                // indefinitely while the journal only ever said "backing off".
                if age >= stale && !given_up {
                    given_up = true;
                    log::error!(
                        "prices: {name} has failed for {age:?} ({e}) — \
                         the prices are cleared, the terminal shows none"
                    );
                    if tx.send(wrap(Vec::new())).is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Whether the exchange refused this call for asking too often.
///
/// HTTP 429 is the warning, 418 is an IP the gateway has already banned, and
/// `-1003`/`-1015` are the same refusals spelled in Aster's own error codes
/// (`PLAN.md`, the error table). Everything else is an ordinary failure and
/// costs one period.
fn throttled(e: &rest::Error) -> bool {
    matches!(
        e,
        rest::Error::Api { status, code, .. }
            if *status == 429 || *status == 418 || *code == -1003 || *code == -1015
    )
}
