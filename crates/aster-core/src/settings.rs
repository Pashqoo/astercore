//! Editable core settings: `data/config.json`, the single source of truth for
//! everything the operator changes at runtime: the journal (its level and how
//! many days of it are kept), what the strategies do when the core starts, the
//! Telegram bot and the web page. Ported from TInvestCore.
//!
//! The account is not here: on Aster it is the API wallet's key file
//! (`ASTER_API_KEY_FILE`, `AGENTS.md`, `## Secrets`), which the core never
//! writes. A missing file is created once, with the journal level of
//! `ASTER_CORE_LOG`, and from then on the file decides. The file holds the bot
//! token and the web password, so it is born mode 600 and a looser mode is
//! tightened on load.

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const DEFAULT_PATH: &str = "data/config.json";
/// Default web endpoint: loopback, beside the core's own UDP port (3101).
pub const DEFAULT_WEB_BIND: &str = "127.0.0.1:3102";
/// MoonBot's daily summary hour: 23:50 of the Moscow day.
const DEFAULT_DAILY_AT: &str = "23:50";
/// Moscow days of journal files kept, today included.
const DEFAULT_LOG_KEEP_DAYS: i64 = 3;
/// A year: past that the files are an archive, not a journal. The number
/// belongs to the module that sweeps them.
use crate::stderr_log::MAX_KEEP_DAYS as MAX_LOG_KEEP_DAYS;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// `error` | `warn` | `info` | `debug` | `trace`.
    pub log_level: String,
    /// Moscow days of `logs/` kept, today included.
    pub log_keep_days: i64,
    /// What the strategies do when the core starts.
    pub start_strategies: StartMode,
    pub telegram: Telegram,
    pub web: Web,
}

/// Whether a starting core arms the strategies.
///
/// The default is what the core has always done: put back the flag the last
/// stop left behind. The other two are for an operator who wants a restart —
/// a deploy, a crash, a reboot — to mean something definite; `Off` above all,
/// which is the one a trader reaches for after an unplanned restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartMode {
    /// The flag saved in `data/orders.json`.
    #[default]
    Remember,
    /// Always armed.
    On,
    /// Always stopped, whatever was running before.
    Off,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Telegram {
    /// Bot token from @BotFather; empty = the reports are off.
    pub token: String,
    /// The approved chat, set by the PIN pairing; 0 = not paired.
    pub chat_id: i64,
    /// `host:port` of a SOCKS5 proxy, empty = direct: a host that cannot
    /// reach `api.telegram.org` itself (TInvestCore's server went through its
    /// own xray), one binary, one setting.
    pub proxy: String,
    /// `HH:MM` of the Moscow day for the summary (`events.daily`).
    pub daily_at: String,
    pub events: Events,
    pub shots: Shots,
}

/// What the core reports. MoonBot's demo defaults: deals yes, detects no.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Events {
    pub deals: bool,
    pub detects: bool,
    /// Exchange refusals, deduplicated (37 refusals in 26 minutes are one line).
    pub refusals: bool,
    /// Auto-stop, market panic, the circuit breakers and dead streams.
    pub alarms: bool,
    /// The core started or stopped.
    pub lifecycle: bool,
    /// The summary, at the Moscow hour [`Telegram::daily_at`] names.
    pub daily: bool,
}

/// Picture thresholds, MoonBot's model: a deal's chart is sent when **any**
/// armed threshold is passed (0 = that one is off).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Shots {
    pub may_send: bool,
    /// Profit of the deal, USDT.
    pub profit_abs: f64,
    /// Profit of the deal, per cent.
    pub profit_pct: f64,
    /// Profit of the trading day so far, USDT — MoonBot's
    /// `profit_session`, so the terminal's own threshold maps onto it.
    pub profit_session: f64,
    /// Send the losing deals too.
    pub send_negative: bool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Web {
    /// `host:port`. Anything but loopback needs a password: the page stops
    /// and starts the trading and holds the bot token.
    pub bind: String,
    pub password: String,
}

/// «set» / «not set» for a secret in a `Debug`: a settings struct ends up in `{:?}` of a log line
/// or a panic message sooner or later, and the bot token and the page password must not.
fn secret(value: &str) -> &'static str {
    if value.is_empty() {
        "not set"
    } else {
        "set"
    }
}

impl std::fmt::Debug for Telegram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telegram")
            .field("token", &secret(&self.token))
            .field("chat_id", &self.chat_id)
            // A proxy url may carry `user:password@`.
            .field("proxy", &secret(&self.proxy))
            .field("daily_at", &self.daily_at)
            .field("events", &self.events)
            .field("shots", &self.shots)
            .finish()
    }
}

impl std::fmt::Debug for Web {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Web")
            .field("bind", &self.bind)
            .field("password", &secret(&self.password))
            .finish()
    }
}

impl std::fmt::Debug for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Edit")
            .field("log_level", &self.log_level)
            .field("log_keep_days", &self.log_keep_days)
            .field("start_strategies", &self.start_strategies)
            .field(
                "telegram_token",
                &self.telegram_token.as_deref().map(secret),
            )
            .field("telegram_chat_id", &self.telegram_chat_id)
            .field(
                "telegram_proxy",
                &self.telegram_proxy.as_deref().map(secret),
            )
            .field("telegram_daily_at", &self.telegram_daily_at)
            .field("telegram_events", &self.telegram_events)
            .field("telegram_shots", &self.telegram_shots)
            .field("web_bind", &self.web_bind)
            .field("web_password", &self.web_password.as_deref().map(secret))
            .finish()
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            log_keep_days: DEFAULT_LOG_KEEP_DAYS,
            start_strategies: StartMode::default(),
            telegram: Telegram::default(),
            web: Web::default(),
        }
    }
}

impl Default for Telegram {
    fn default() -> Self {
        Self {
            token: String::new(),
            chat_id: 0,
            proxy: String::new(),
            daily_at: DEFAULT_DAILY_AT.into(),
            events: Events::default(),
            shots: Shots::default(),
        }
    }
}

impl Default for Events {
    fn default() -> Self {
        Self {
            deals: true,
            detects: false,
            refusals: true,
            alarms: true,
            lifecycle: true,
            daily: true,
        }
    }
}

impl Default for Shots {
    fn default() -> Self {
        Self {
            may_send: true,
            profit_abs: 0.0,
            profit_pct: 0.0,
            profit_session: 0.0,
            send_negative: false,
        }
    }
}

impl Default for Web {
    fn default() -> Self {
        Self {
            bind: DEFAULT_WEB_BIND.into(),
            password: String::new(),
        }
    }
}

/// One edit from the page: the fields it actually changed, `None` for the rest.
///
/// A patch and not a whole [`Settings`] because the page never holds one: the
/// bot token and the web password do not leave the core (`control::SettingsView`
/// carries «set / not set»), so a page that sent back what it was shown would
/// blank them both. The owner of the settings — the trading loop — merges the
/// patch into its own copy and saves that, which is also why a long-running
/// form cannot revert a field somebody else changed meanwhile.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Edit {
    pub log_level: Option<String>,
    pub log_keep_days: Option<i64>,
    pub start_strategies: Option<StartMode>,
    /// Empty string = forget the token (the reports go off), as the page's
    /// «clear» button asks; `None` = leave it alone, which is every save the
    /// operator makes without retyping it.
    pub telegram_token: Option<String>,
    /// 0 = forget the approved chat, so the PIN pairing can be redone.
    pub telegram_chat_id: Option<i64>,
    pub telegram_proxy: Option<String>,
    /// `HH:MM` of the Moscow day for the summary.
    pub telegram_daily_at: Option<String>,
    pub telegram_events: Option<Events>,
    pub telegram_shots: Option<Shots>,
    pub web_bind: Option<String>,
    pub web_password: Option<String>,
}

/// Fields whose new value only takes effect at the next start.
///
/// `start_strategies` is one of them by definition: `engine::with_orders` is
/// its only reader and it runs once, at boot. Leaving it out would have the
/// page answer a plain «saved» to an operator who just asked a running core to
/// come up stopped next time.
pub const RESTART_FIELDS: [&str; 2] = ["web.bind", "start_strategies"];

/// The `.env` values a first start copies into the file. Read from the
/// environment by [`from_env`]; passed in so the migration is testable.
#[derive(Clone, Debug, Default)]
pub struct Migration {
    pub log_level: Option<String>,
}

/// `ASTER_CORE_LOG`; the exchange key is never migrated — it is a file of
/// its own (`ASTER_API_KEY_FILE`).
pub fn from_env() -> Migration {
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    Migration {
        log_level: var("ASTER_CORE_LOG"),
    }
}

#[derive(Debug)]
pub struct Loaded {
    pub settings: Settings,
    /// The file did not exist and was written from the environment.
    pub created: bool,
    /// Variables still set in `.env` whose value the file overrides — two
    /// truths for one field. Only the names: the values belong in neither the
    /// journal nor a report.
    pub ignored_env: Vec<&'static str>,
}

/// Read `path` if it is there, and create nothing when it is not.
///
/// Split out of [`load_or_create`] for the operator tools that link this crate
/// by path (`core_ctl`): they need the account the core actually trades on,
/// and a probe that writes a fresh config into whatever directory it happened
/// to be run from would answer about a core that does not exist.
///
/// Read-only about the *contents*, not about the mode: a successful read still
/// runs `tighten`, so a file left group- or world-readable is chmod'ed to 600
/// here as it is on the core's own start. That is deliberate — the file holds
/// the bot token and the web password, and a reader that noticed the exposure
/// and walked past it would be the worse of the two behaviours.
pub fn load(path: impl AsRef<Path>) -> Result<Option<Settings>, String> {
    let path = path.as_ref();
    match fs::read(path) {
        Ok(bytes) => {
            // First of all: the file holds the bot token and the web password
            // whether or not what is in it parses or passes, and every path
            // below can return early.
            tighten(path);
            let settings: Settings =
                serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))?;
            settings.validate()?;
            Ok(Some(settings))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Read `path`, or create it from `env` when it does not exist.
///
/// A file that exists and cannot be read or parsed is an error, not an empty
/// start: its fields decide which account trades and whether the web page asks
/// for a password, and guessing either is worse than not starting.
pub fn load_or_create(path: impl AsRef<Path>, env: &Migration) -> Result<Loaded, String> {
    let path = path.as_ref();
    match load(path)? {
        Some(settings) => {
            let mut ignored_env = Vec::new();
            if differs(env.log_level.as_deref(), &settings.log_level) {
                ignored_env.push("ASTER_CORE_LOG");
            }
            Ok(Loaded {
                settings,
                created: false,
                ignored_env,
            })
        }
        None => {
            let mut settings = Settings::default();
            if let Some(level) = env.log_level.clone() {
                settings.log_level = level;
            }
            settings.validate()?;
            settings
                .write_new(path)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            Ok(Loaded {
                settings,
                created: true,
                ignored_env: Vec::new(),
            })
        }
    }
}

/// `HH:MM` as minutes since midnight, or `None` when it is not a time of day.
/// Written out rather than pulled in: it is four lines and the alternative is
/// a date-time dependency for one field.
pub fn parse_hhmm(text: &str) -> Option<i64> {
    let (h, m) = text.trim().split_once(':')?;
    let (h, m) = (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?);
    ((0..24).contains(&h) && (0..60).contains(&m)).then_some(h * 60 + m)
}

/// An environment value that is set and says something else than the file.
fn differs(env: Option<&str>, file: &str) -> bool {
    env.is_some_and(|v| v != file)
}

impl Settings {
    /// Refuse the combinations that would hand the page over: a web endpoint
    /// reachable from the network without a password. A bad `bind` is an
    /// error too — an unparsable one would otherwise pass this check and only
    /// fail when the listener opens.
    pub fn validate(&self) -> Result<(), String> {
        // Both are read long after the start — the journal rolls at Moscow
        // midnight, the summary goes out in the evening — so a file edited by
        // hand is caught here and not hours later by a line nobody is reading.
        if parse_hhmm(&self.telegram.daily_at).is_none() {
            return Err(format!(
                "telegram.daily_at {:?} is not HH:MM",
                self.telegram.daily_at
            ));
        }
        if !(1..=MAX_LOG_KEEP_DAYS).contains(&self.log_keep_days) {
            return Err(format!(
                "log_keep_days {} is not 1..={MAX_LOG_KEEP_DAYS}",
                self.log_keep_days
            ));
        }
        let addr = self.web_addr()?;
        if !addr.ip().is_loopback() && self.web.password.is_empty() {
            return Err(format!(
                "web.bind {addr} is not loopback and web.password is empty: \
                 the page stops the trading and holds the bot token"
            ));
        }
        Ok(())
    }

    /// Merge one [`Edit`] and say which of the changed fields only take effect
    /// at the next start.
    ///
    /// All or nothing: the merge happens on a copy that is validated before it
    /// replaces anything, so a page that asks for a network endpoint with no
    /// password changes neither the endpoint nor the seven fields it sent
    /// along with it. The same reason [`Self::save`] validates.
    pub fn apply(&mut self, edit: &Edit) -> Result<Vec<&'static str>, String> {
        let mut next = self.clone();
        if let Some(v) = &edit.log_level {
            let level = v.trim();
            // Refused here rather than warned about later: the page would
            // otherwise save a level the next start cannot read, and the core
            // would run at whatever it was before with a file that says
            // something else.
            if level.parse::<log::LevelFilter>().is_err() {
                return Err(format!(
                    "log_level {level:?} is not off|error|warn|info|debug|trace"
                ));
            }
            next.log_level = level.to_owned();
        }
        if let Some(v) = &edit.telegram_token {
            next.telegram.token = v.trim().to_owned();
        }
        if let Some(v) = edit.telegram_chat_id {
            next.telegram.chat_id = v;
        }
        if let Some(v) = &edit.telegram_proxy {
            crate::telegram::check_proxy(v)?;
            next.telegram.proxy = v.trim().to_owned();
        }
        if let Some(v) = &edit.telegram_daily_at {
            let at = v.trim();
            if parse_hhmm(at).is_none() {
                return Err(format!("telegram.daily_at {at:?} is not HH:MM"));
            }
            next.telegram.daily_at = at.to_owned();
        }
        if let Some(v) = edit.log_keep_days {
            if !(1..=MAX_LOG_KEEP_DAYS).contains(&v) {
                return Err(format!("log_keep_days {v} is not 1..={MAX_LOG_KEEP_DAYS}"));
            }
            next.log_keep_days = v;
        }
        if let Some(v) = edit.start_strategies {
            next.start_strategies = v;
        }
        if let Some(v) = &edit.telegram_events {
            next.telegram.events = v.clone();
        }
        if let Some(v) = &edit.telegram_shots {
            next.telegram.shots = v.clone();
        }
        if let Some(v) = &edit.web_bind {
            next.web.bind = v.trim().to_owned();
        }
        // Not trimmed: a password is what was typed.
        if let Some(v) = &edit.web_password {
            next.web.password = v.clone();
        }
        next.validate()?;
        let restart = RESTART_FIELDS
            .iter()
            .copied()
            .zip([
                next.web.bind != self.web.bind,
                next.start_strategies != self.start_strategies,
            ])
            .filter_map(|(name, changed)| changed.then_some(name))
            .collect();
        *self = next;
        Ok(restart)
    }

    pub fn web_addr(&self) -> Result<SocketAddr, String> {
        self.web
            .bind
            .parse()
            .map_err(|_| format!("web.bind {:?} is not host:port", self.web.bind))
    }

    /// Milliseconds into the Moscow day at which the summary goes out.
    /// Validated on the way in, so a file that reached here has one.
    pub fn daily_at_ms(&self) -> i64 {
        parse_hhmm(&self.telegram.daily_at).unwrap_or(23 * 60 + 50) * 60_000
    }

    /// A bot token and an approved chat: the reports have somewhere to go.
    pub fn telegram_ready(&self) -> bool {
        !self.telegram.token.is_empty() && self.telegram.chat_id != 0
    }

    /// Replace the file, through a temp file and a rename, mode 600.
    ///
    /// Validated first: a config that `load_or_create` would refuse must never
    /// reach the disk, or the page would take the core off the market at its
    /// next start with a setting that looked accepted.
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let path = path.as_ref();
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("json.tmp");
        // A `.tmp` left by a crash may carry wider rights than 600 (`.mode` applies at creation
        // only), and the rename would hand them to `config.json`: it is removed first, so the
        // new one is created, and with the right mode.
        let _ = fs::remove_file(&tmp);
        write_private(&tmp, &text)?;
        fs::rename(&tmp, path)
    }

    fn write_new(&self, path: &Path) -> io::Result<()> {
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir)?;
        }
        write_private(path, &text)
    }
}

/// Write `contents` to a file no one else can read: it holds the bot token and
/// the web password. The twin of `key_store::write_private`, kept apart from it
/// because this one replaces an existing file and that one must never.
#[cfg(unix)]
fn write_private(path: &Path, contents: &str) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())?;
    // The token and the password: on disk before the rename that makes this the config.
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, contents: &str) -> io::Result<()> {
    use std::io::Write;
    let mut file = fs::File::create(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()
}

/// Tighten a file an older core, an editor or a copy left readable. Reported,
/// never fatal: a readable config is worse than the previous start, refusing
/// to trade over it is worse still.
#[cfg(unix)]
fn tighten(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mode = match fs::metadata(path) {
        Ok(meta) => meta.permissions().mode() & 0o777,
        Err(e) => {
            log::warn!("config: cannot read the mode of {}: {e}", path.display());
            return;
        }
    };
    if mode & 0o077 == 0 {
        return;
    }
    match fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
        Ok(()) => log::warn!(
            "config: {} was mode {mode:o}, tightened to 600",
            path.display()
        ),
        Err(e) => log::error!(
            "config: {} is mode {mode:o} and cannot be tightened: {e}",
            path.display()
        ),
    }
}

#[cfg(not(unix))]
fn tighten(_path: &Path) {}

#[cfg(test)]
mod tests {
    /// The bot token, the page password and a proxy url with credentials never reach a `{:?}`.
    #[test]
    fn debug_output_hides_the_secrets() {
        let mut s = Settings::default();
        s.telegram.token = "123456:SECRET-TOKEN".into();
        s.telegram.proxy = "socks5://user:hunter2@host:1080".into();
        s.web.password = "page-password".into();
        let edit = Edit {
            telegram_token: Some("123456:SECRET-TOKEN".into()),
            web_password: Some("page-password".into()),
            ..Edit::default()
        };
        for text in [format!("{s:?}"), format!("{edit:?}")] {
            for secret in ["SECRET-TOKEN", "hunter2", "page-password"] {
                assert!(!text.contains(secret), "{secret} in {text}");
            }
        }
        assert!(format!("{s:?}").contains("token: \"set\""));
    }

    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("aster-cfg-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn env(level: &str) -> Migration {
        Migration {
            log_level: Some(level.into()),
        }
    }

    #[test]
    fn a_first_start_writes_the_env_values_and_the_next_one_reads_them() {
        let dir = scratch("create");
        let path = dir.join("config.json");
        let first = load_or_create(&path, &env("debug")).unwrap();
        assert!(first.created && first.ignored_env.is_empty());
        assert_eq!(first.settings.log_level, "debug");
        // Defaults nobody had to write down.
        assert_eq!(first.settings.web.bind, DEFAULT_WEB_BIND);
        assert!(first.settings.telegram.events.deals);
        assert!(!first.settings.telegram.events.detects);
        assert!(first.settings.telegram.shots.may_send);

        let again = load_or_create(&path, &env("debug")).unwrap();
        assert!(!again.created);
        assert_eq!(again.settings, first.settings);
        fs::remove_dir_all(dir).unwrap();
    }

    /// The file is the truth; a `.env` that says something else is named, so
    /// the operator hears about the second truth instead of guessing which won.
    #[test]
    fn env_values_that_the_file_overrides_are_named() {
        let dir = scratch("ignored");
        let path = dir.join("config.json");
        load_or_create(&path, &env("info")).unwrap();
        let loaded = load_or_create(&path, &env("debug")).unwrap();
        assert_eq!(loaded.settings.log_level, "info");
        assert_eq!(loaded.ignored_env, ["ASTER_CORE_LOG"]);
        // The same value is not a second truth.
        let loaded = load_or_create(&path, &env("info")).unwrap();
        assert!(loaded.ignored_env.is_empty());
        fs::remove_dir_all(dir).unwrap();
    }

    /// Missing fields take their defaults, so a file written by an older core
    /// still loads — and the fields it never heard of are not zeroes.
    #[test]
    fn an_older_file_loads_with_the_defaults() {
        let dir = scratch("partial");
        let path = dir.join("config.json");
        fs::write(&path, r#"{"log_keep_days":5}"#).unwrap();
        let loaded = load_or_create(&path, &Migration::default()).unwrap();
        assert_eq!(loaded.settings.log_keep_days, 5);
        assert_eq!(loaded.settings.log_level, "info");
        assert_eq!(loaded.settings.web.bind, DEFAULT_WEB_BIND);
        assert!(loaded.settings.telegram.shots.may_send);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_broken_file_is_an_error_not_an_empty_start() {
        let dir = scratch("broken");
        let path = dir.join("config.json");
        fs::write(&path, "{ not json").unwrap();
        let err = load_or_create(&path, &Migration::default()).unwrap_err();
        assert!(err.contains("config.json"), "{err}");
        // It still holds the token and the password, so its mode is fixed even
        // though nothing in it could be read.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_web_page_on_the_network_needs_a_password() {
        let open = Settings {
            web: Web {
                bind: "0.0.0.0:3102".into(),
                password: String::new(),
            },
            ..Settings::default()
        };
        assert!(open.validate().is_err());
        let guarded = Settings {
            web: Web {
                bind: "0.0.0.0:3102".into(),
                password: "s3cret".into(),
            },
            ..Settings::default()
        };
        assert!(guarded.validate().is_ok());
        // Loopback without a password is the default case.
        assert!(Settings::default().validate().is_ok());
        let bad = Settings {
            web: Web {
                bind: "3110".into(),
                password: String::new(),
            },
            ..Settings::default()
        };
        assert!(bad.validate().is_err(), "an unparsable bind must not pass");
    }

    /// The page sends what it changed; everything it was never shown — the bot
    /// token above all — has to survive the save.
    #[test]
    fn an_edit_leaves_the_fields_it_does_not_name_alone() {
        let mut settings = Settings {
            telegram: Telegram {
                token: "123:secret".into(),
                chat_id: -1_001,
                ..Telegram::default()
            },
            ..Settings::default()
        };
        let restart = settings
            .apply(&Edit {
                web_bind: Some(" 127.0.0.1:3112 ".into()),
                log_level: Some("debug".into()),
                telegram_events: Some(Events {
                    detects: true,
                    ..Events::default()
                }),
                ..Edit::default()
            })
            .unwrap();
        assert_eq!(settings.web.bind, "127.0.0.1:3112", "trimmed");
        assert_eq!(settings.log_level, "debug");
        assert!(settings.telegram.events.detects);
        assert_eq!(settings.telegram.token, "123:secret", "never blanked");
        assert_eq!(settings.telegram.chat_id, -1_001);
        assert_eq!(restart, ["web.bind"], "the log level applies at once");

        // An empty token is how the page turns the reports off, and that is a
        // different thing from not naming the field.
        settings
            .apply(&Edit {
                telegram_token: Some(String::new()),
                ..Edit::default()
            })
            .unwrap();
        assert!(settings.telegram.token.is_empty());
    }

    /// A refused edit changes nothing at all: half of a settings form is worse
    /// than none of it.
    #[test]
    fn a_refused_edit_applies_none_of_itself() {
        let mut settings = Settings::default();
        let before = settings.clone();
        for bad in [
            Edit {
                // The page on the network with no password — `validate`.
                web_bind: Some("0.0.0.0:3102".into()),
                log_level: Some("debug".into()),
                ..Edit::default()
            },
            Edit {
                log_level: Some("chatty".into()),
                log_keep_days: Some(4),
                ..Edit::default()
            },
        ] {
            assert!(settings.apply(&bad).is_err(), "{bad:?}");
            assert_eq!(settings, before, "{bad:?}");
        }
        // The same endpoint with a password is accepted, and it needs a restart.
        let restart = settings
            .apply(&Edit {
                web_bind: Some("0.0.0.0:3102".into()),
                web_password: Some(" pw with space ".into()),
                ..Edit::default()
            })
            .unwrap();
        assert_eq!(restart, ["web.bind"]);
        assert_eq!(settings.web.password, " pw with space ", "not trimmed");
    }

    /// The three settings this file gained for what the core already did.
    #[test]
    fn the_new_fields_take_what_the_core_can_act_on_and_refuse_the_rest() {
        assert_eq!(parse_hhmm("23:50"), Some(23 * 60 + 50));
        assert_eq!(parse_hhmm("00:00"), Some(0));
        assert_eq!(parse_hhmm(" 9:05 "), Some(9 * 60 + 5));
        for bad in ["24:00", "23:60", "2350", "23:5x", "-1:00", "", ":"] {
            assert_eq!(parse_hhmm(bad), None, "{bad:?}");
        }
        assert_eq!(Settings::default().daily_at_ms(), (23 * 60 + 50) * 60_000);

        let mut settings = Settings::default();
        assert_eq!(settings.log_keep_days, DEFAULT_LOG_KEEP_DAYS);
        assert_eq!(settings.start_strategies, StartMode::Remember);
        settings
            .apply(&Edit {
                telegram_daily_at: Some("09:30".into()),
                log_keep_days: Some(30),
                start_strategies: Some(StartMode::Off),
                ..Edit::default()
            })
            .unwrap();
        assert_eq!(settings.daily_at_ms(), (9 * 60 + 30) * 60_000);
        assert_eq!(settings.log_keep_days, 30);
        assert_eq!(settings.start_strategies, StartMode::Off);
        // The level and the hour apply at once; what the next start does with
        // the strategies is by definition next start's business, and the page
        // has to say so.
        assert_eq!(
            settings
                .apply(&Edit {
                    start_strategies: Some(StartMode::On),
                    telegram_daily_at: Some("10:00".into()),
                    ..Edit::default()
                })
                .unwrap(),
            ["start_strategies"]
        );

        // None of these may reach the disk: they are read hours later, and a
        // bad one would only show up as a line nobody is watching.
        let before = settings.clone();
        for bad in [
            Edit {
                telegram_daily_at: Some("25:00".into()),
                ..Edit::default()
            },
            Edit {
                log_keep_days: Some(0),
                ..Edit::default()
            },
            Edit {
                log_keep_days: Some(MAX_LOG_KEEP_DAYS + 1),
                ..Edit::default()
            },
        ] {
            assert!(settings.apply(&bad).is_err(), "{bad:?}");
            assert_eq!(settings, before, "{bad:?}");
        }
        // And a file edited by hand is caught at the start, not at 23:50.
        let hand = Settings {
            telegram: Telegram {
                daily_at: "half past nine".into(),
                ..Telegram::default()
            },
            ..Settings::default()
        };
        assert!(hand.validate().is_err());
        let hand = Settings {
            log_keep_days: -1,
            ..Settings::default()
        };
        assert!(hand.validate().is_err());
    }

    /// A file from the core before these fields existed still loads, and the
    /// defaults it gets are the behaviour it had.
    #[test]
    fn a_file_without_the_new_fields_keeps_the_old_behaviour() {
        let dir = scratch("older");
        let path = dir.join("config.json");
        fs::write(&path, r#"{"log_level":"debug","telegram":{"chat_id":-1}}"#).unwrap();
        let loaded = load_or_create(&path, &Migration::default())
            .unwrap()
            .settings;
        assert_eq!(loaded.log_keep_days, 3, "the three days it always kept");
        assert_eq!(loaded.start_strategies, StartMode::Remember);
        assert_eq!(loaded.telegram.daily_at, "23:50", "MoonBot's hour");
        assert_eq!(loaded.telegram.chat_id, -1, "and the rest still loads");
        fs::remove_dir_all(dir).unwrap();
    }

    /// A `config.json.tmp` a crash left with wide rights is not the file the next save renames
    /// into place: `.mode` applies at creation only.
    #[cfg(unix)]
    #[test]
    fn a_stale_wide_tmp_does_not_widen_the_saved_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = scratch("stale-tmp");
        let path = dir.join("config.json");
        let loaded = load_or_create(&path, &Migration::default()).unwrap();
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, "stale").unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o666)).unwrap();
        loaded.settings.save(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert!(!tmp.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn the_file_is_private_when_created_and_when_saved() {
        use std::os::unix::fs::PermissionsExt;

        let dir = scratch("mode");
        let path = dir.join("config.json");
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let loaded = load_or_create(&path, &Migration::default()).unwrap();
        assert_eq!(mode(&path), 0o600);
        // A file left open by an editor is tightened on load, and a save
        // never widens it again.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        load_or_create(&path, &Migration::default()).unwrap();
        assert_eq!(mode(&path), 0o600);
        loaded.settings.save(&path).unwrap();
        assert_eq!(mode(&path), 0o600);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_save_round_trips_every_field() {
        let dir = scratch("save");
        let path = dir.join("config.json");
        let settings = Settings {
            log_level: "warn".into(),
            telegram: Telegram {
                token: "123:abc".into(),
                chat_id: -1_001,
                proxy: "127.0.0.1:1080".into(),
                shots: Shots {
                    profit_abs: 250.0,
                    ..Shots::default()
                },
                ..Telegram::default()
            },
            web: Web {
                password: "pw".into(),
                ..Web::default()
            },
            ..Settings::default()
        };
        assert!(settings.telegram_ready());
        settings.save(&path).unwrap();
        let loaded = load_or_create(&path, &Migration::default()).unwrap();
        assert_eq!(loaded.settings, settings);
        assert!(!path.with_extension("json.tmp").exists());
        // What the next start would refuse never reaches the disk: the page
        // would otherwise take the core off the market with a saved setting.
        let open = Settings {
            web: Web {
                bind: "0.0.0.0:3102".into(),
                password: String::new(),
            },
            ..settings.clone()
        };
        assert!(open.save(&path).is_err());
        let still = load_or_create(&path, &Migration::default()).unwrap();
        assert_eq!(still.settings, settings, "the good file is untouched");
        fs::remove_dir_all(dir).unwrap();
    }
}
