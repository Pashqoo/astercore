//! The core's update from its sources, asked for by the terminal (`TUpdateVersionCommand`).
//!
//! The core never builds anything: it runs as its own user with no toolchain and no git, and it
//! holds the exchange wallet's key. It writes a request into its data directory; a root service
//! started by a systemd path unit (`tools/aster-core-update.path`, `tools/update.sh`) fetches the
//! commit, builds and tests it and stages the binary beside the running one; the core swaps it in
//! and restarts once it holds no position. Each file has one writer, and every line carries the
//! request's id, so a file left over from an earlier update is never read as this one's:
//!
//! - `data/update-request` — the core: `<id> <release 0|1> <target|-> <build> <commit>`. The
//!   script claims it by renaming it to `update-running`, which it removes when it is done.
//! - `update-state` — the script, in its own root-owned directory (`ASTER_UPDATE_STATE_DIR`):
//!   `<id> accepted` · `building <sha> <build>` · `staged <sha> <build>` · `failed <why>` ·
//!   `rolled-back <why>` · `done <sha> <build>`. Not in `data/`: root never writes into a
//!   directory the core's user can plant a link in.
//! - `data/update-ack` — the core: `<id> swapped <sha> <from build>` · `cancelled <why>` ·
//!   `running <sha> <build>` (the new core, once it serves) · `reported` (a rollback was told).
//!
//! The binary is swapped by rename inside the core's own directory, which the core owns already:
//! the update adds no right the process did not have.

use std::path::{Path, PathBuf};

/// The build number: the commit count of `main` at the built commit, set by `tools/update.sh`
/// (and by hand per the README). The terminal reads it as the version (`7.63` for 763), so it
/// must grow with every commit. 1 for a build that did not set it.
pub const BUILD: i32 = parse_build(option_env!("ASTER_CORE_BUILD"));
/// The commit this binary was built from; `dev` when the build did not say.
pub const COMMIT: &str = match option_env!("ASTER_CORE_COMMIT") {
    Some(c) => c,
    None => "dev",
};

/// How long the updater has to answer a request at all: a path unit starts it within a second,
/// and its first act is `accepted`.
const ANSWER_WAIT_MS: i64 = 30_000;
/// How long a fetch, a build and a test may take before the core stops waiting for them: past
/// the sum of the script's own bounds (a clone or a fetch 5 min each, the build 25, the test 10),
/// so the core never gives up on a build that is still running.
const BUILD_WAIT_MS: i64 = 50 * 60_000;
/// How long a staged build waits for the core to hold no position (the trader's decision, 05.10).
const FLAT_WAIT_MS: i64 = 10 * 60_000;
/// The updater's files are read this often while an update is pending, and never otherwise.
const CHECK_EVERY_MS: i64 = 1_000;
/// The token the terminal reads as «the core refused the update» (`BGF-SUB4`, MoonBot's).
pub const REFUSED: &str = "BGF-SUB4";

const REQUEST: &str = "update-request";
const STATE: &str = "update-state";
const ACK: &str = "update-ack";

const fn parse_build(s: Option<&str>) -> i32 {
    let Some(s) = s else {
        return 1;
    };
    let b = s.as_bytes();
    if b.is_empty() || b.len() > 9 {
        return 1;
    }
    let mut n = 0i32;
    let mut i = 0;
    while i < b.len() {
        if !b[i].is_ascii_digit() {
            return 1;
        }
        n = n * 10 + (b[i] - b'0') as i32;
        i += 1;
    }
    n
}

/// A build name the terminal may ask for: a commit (7–40 hex) or a tag. The script checks that
/// it is on `main`; here only that it is one word the request line and a shell can carry.
pub fn valid_target(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !name.starts_with(['-', '.', '/'])
        && !name.contains("..")
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-' | b'/'))
}

/// One line of `update-state` or `update-ack`: the request's id, the word, the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Line {
    id: i64,
    kind: String,
    rest: String,
}

impl Line {
    fn parse(text: &str) -> Option<Self> {
        let line = text.lines().next()?.trim();
        let mut it = line.splitn(3, ' ');
        let id = it.next()?.parse().ok()?;
        let kind = it.next()?.to_string();
        let rest = it.next().unwrap_or("").trim().to_string();
        Some(Self { id, kind, rest })
    }

    /// `<sha> <build>` of a `building`/`staged`/`swapped`/`running` line.
    fn sha_build(&self) -> Option<(&str, i32)> {
        let mut it = self.rest.split(' ');
        let sha = it.next().filter(|s| !s.is_empty())?;
        let build = it.next()?.parse().ok()?;
        Some((sha, build))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Written, not yet answered.
    Asked,
    /// The updater took it: fetching, building, testing.
    Building,
    /// A tested binary waits beside the running one, since this moment.
    Staged { since: i64 },
    /// The new binary is in place and the core is on its way out: nothing more to read, and
    /// no other update may start (it would clear the ack the updater is waiting for).
    Swapped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    id: i64,
    target: String,
    asked_at: i64,
    phase: Phase,
    /// When the updater's files were read last.
    checked_at: i64,
}

/// What the engine does after a check of a pending update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Nothing to tell.
    Wait,
    /// A stage was reached: journal and terminals.
    Note(String),
    /// The update is over without a new build; the text says why (journal, chat, terminals).
    Over(String),
    /// The new binary is in place: restart the core.
    Restart(String),
}

/// [`Updater::judge`]'s answer, before the files are acted on.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    Wait,
    Note(String),
    Fail(String),
    Cancel(String),
    Swap(String),
}

/// The update's side of the core: where its files are and the one update in flight.
#[derive(Debug)]
pub struct Updater {
    dir: PathBuf,
    state_dir: PathBuf,
    exe: PathBuf,
    pending: Option<Pending>,
}

impl Updater {
    /// The core's files in `dir` (the data directory), the updater's `update-state` in
    /// `state_dir`; the running binary at `exe`, the staged one beside it as `<exe>.next`, the
    /// replaced one kept as `<exe>.prev`.
    pub fn new(dir: PathBuf, state_dir: PathBuf, exe: PathBuf) -> Self {
        Self {
            dir,
            state_dir,
            exe,
            pending: None,
        }
    }

    pub fn pending(&self) -> bool {
        self.pending.is_some()
    }

    fn file(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn read_state(&self) -> Option<Line> {
        std::fs::read_to_string(self.state_dir.join(STATE))
            .ok()
            .and_then(|t| Line::parse(&t))
    }

    fn sibling(&self, suffix: &str) -> PathBuf {
        let mut name = self.exe.file_name().unwrap_or_default().to_os_string();
        name.push(suffix);
        self.exe.with_file_name(name)
    }

    /// Ask the updater for a build: the release (`main`) or a named commit or tag. Refused
    /// while another update is in flight; the caller checks positions and the name.
    pub fn ask(&mut self, target: &str, release: bool, now: i64) -> Result<String, String> {
        if self.pending.is_some() {
            return Err("an update is already in progress".into());
        }
        // The last update's ack goes, so nothing of it can be read as this one's answer (its
        // state line carries another id). A file that is not there is the usual case.
        let _ = std::fs::remove_file(self.file(ACK));
        let shown = if release {
            "the release (main)"
        } else {
            target
        };
        let line = format!(
            "{now} {} {} {BUILD} {COMMIT}\n",
            u8::from(release),
            if release { "-" } else { target }
        );
        write_atomic(&self.file(REQUEST), &line)
            .map_err(|e| format!("the request was not written: {e}"))?;
        self.pending = Some(Pending {
            id: now,
            target: shown.to_string(),
            asked_at: now,
            phase: Phase::Asked,
            checked_at: now,
        });
        Ok(format!(
            "update to {shown} accepted: building on the server, the core restarts when it \
             holds no position"
        ))
    }

    /// Advance the update in flight; `flat` is «the core holds no position». Reads the
    /// updater's files at most once a second, and only while an update is pending.
    pub fn check(&mut self, flat: bool, now: i64) -> Step {
        let Some(p) = &self.pending else {
            return Step::Wait;
        };
        if p.phase == Phase::Swapped || now - p.checked_at < CHECK_EVERY_MS {
            return Step::Wait;
        }
        let id = p.id;
        let state = self.read_state().filter(|l| l.id == id);
        match self.judge(state, flat, now) {
            Verdict::Wait => Step::Wait,
            Verdict::Note(text) => Step::Note(text),
            // A request nobody took must not be built later, unasked; a staged binary nobody
            // will swap in must not wait beside the running one.
            Verdict::Fail(why) => {
                self.pending = None;
                let _ = std::fs::remove_file(self.file(REQUEST));
                let _ = std::fs::remove_file(self.sibling(".next"));
                Step::Over(why)
            }
            Verdict::Cancel(why) => {
                self.pending = None;
                let _ = std::fs::remove_file(self.sibling(".next"));
                let _ = write_atomic(&self.file(ACK), &format!("{id} cancelled {why}\n"));
                Step::Over(why)
            }
            Verdict::Swap(text) => match self.swap(id) {
                Ok(()) => {
                    if let Some(p) = self.pending.as_mut() {
                        p.phase = Phase::Swapped;
                    }
                    Step::Restart(text)
                }
                Err(e) => {
                    self.pending = None;
                    let _ = std::fs::remove_file(self.sibling(".next"));
                    let why = format!("update not installed: {e}");
                    // The updater waits for an answer either way.
                    let _ = write_atomic(&self.file(ACK), &format!("{id} cancelled {why}\n"));
                    Step::Over(why)
                }
            },
        }
    }

    /// The decision, apart from the files: what the updater said, against the clock.
    fn judge(&mut self, state: Option<Line>, flat: bool, now: i64) -> Verdict {
        let Some(p) = self.pending.as_mut() else {
            return Verdict::Wait;
        };
        p.checked_at = now;
        if let Some(l) = &state {
            match (l.kind.as_str(), p.phase) {
                ("failed", _) => return Verdict::Fail(format!("update failed: {}", l.rest)),
                ("building", Phase::Asked) => {
                    p.phase = Phase::Building;
                    return Verdict::Note(format!("update: building {}", l.rest));
                }
                ("accepted", Phase::Asked) => {
                    p.phase = Phase::Building;
                }
                ("staged", Phase::Asked | Phase::Building) => {
                    p.phase = Phase::Staged { since: now };
                    let what = l.rest.clone();
                    if !flat {
                        return Verdict::Note(format!(
                            "update: build {what} is ready, waiting up to {} min for the core \
                             to hold no position",
                            FLAT_WAIT_MS / 60_000
                        ));
                    }
                }
                _ => {}
            }
        }
        match p.phase {
            Phase::Asked if now - p.asked_at > ANSWER_WAIT_MS => Verdict::Fail(format!(
                "update failed: the updater did not answer in {} s — is \
                 aster-core-update.path installed on this host?",
                ANSWER_WAIT_MS / 1000
            )),
            Phase::Building if now - p.asked_at > BUILD_WAIT_MS => Verdict::Fail(format!(
                "update failed: no build of {} in {} min",
                p.target,
                BUILD_WAIT_MS / 60_000
            )),
            Phase::Staged { .. } if flat => {
                let what = state.map(|l| l.rest).unwrap_or_default();
                Verdict::Swap(format!("update: build {what} installed, restarting"))
            }
            Phase::Staged { since } if now - since > FLAT_WAIT_MS => Verdict::Cancel(format!(
                "update cancelled: the core held a position for {} min after the build was \
                 ready",
                FLAT_WAIT_MS / 60_000
            )),
            _ => Verdict::Wait,
        }
    }

    /// The staged binary in place of the running one, which is kept as `.prev`; the ack tells
    /// the updater to watch for the new core.
    fn swap(&self, id: i64) -> Result<(), String> {
        let sha = self
            .read_state()
            .filter(|l| l.id == id)
            .and_then(|l| l.sha_build().map(|(s, _)| s.to_string()))
            .ok_or("the staged build has no commit")?;
        let (next, prev) = (self.sibling(".next"), self.sibling(".prev"));
        if !next.is_file() {
            return Err(format!("{} is not there", next.display()));
        }
        std::fs::rename(&self.exe, &prev).map_err(|e| format!("{}: {e}", self.exe.display()))?;
        if let Err(e) = std::fs::rename(&next, &self.exe) {
            // The running binary goes back where it was: a restart must find one.
            let _ = std::fs::rename(&prev, &self.exe);
            return Err(format!("{}: {e}", next.display()));
        }
        if let Err(e) = write_atomic(&self.file(ACK), &format!("{id} swapped {sha} {BUILD}\n")) {
            // Without the ack nobody watches the new core come up: the old binary goes back,
            // and a later restart cannot start an unwatched build.
            let _ = std::fs::rename(&self.exe, &next);
            let _ = std::fs::rename(&prev, &self.exe);
            return Err(format!("the ack was not written: {e}"));
        }
        Ok(())
    }

    /// At start: what the last update came to, said once. A core started by its own update
    /// tells the updater it is up (`running`); a core the updater put back says the update was
    /// rolled back.
    pub fn on_start(&self) -> Option<String> {
        let read = |name| {
            std::fs::read_to_string(self.file(name))
                .ok()
                .and_then(|t| Line::parse(&t))
        };
        let ack = read(ACK)?;
        let state = self.read_state().filter(|s| s.id == ack.id);
        if let Some(s) = state.as_ref().filter(|s| s.kind == "rolled-back") {
            if ack.kind == "reported" {
                return None;
            }
            let _ = write_atomic(&self.file(ACK), &format!("{} reported\n", ack.id));
            return Some(format!("update rolled back: {}", s.rest));
        }
        if ack.kind != "swapped" {
            return None;
        }
        let mut it = ack.rest.split(' ');
        let (sha, from) = (it.next().unwrap_or(""), it.next().unwrap_or("?"));
        if sha != COMMIT {
            // Said, so a rollback that follows has its reason in the journal.
            return Some(format!(
                "update: the swapped build is {sha}, this binary is {COMMIT} — the updater will \
                 put the previous one back"
            ));
        }
        let _ = write_atomic(
            &self.file(ACK),
            &format!("{} running {COMMIT} {BUILD}\n", ack.id),
        );
        Some(format!(
            "updated: build {from} → {BUILD} ({})",
            &COMMIT[..COMMIT.len().min(10)]
        ))
    }
}

/// Write by rename, so the other side never reads half a line.
fn write_atomic(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aster-update-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("data")).unwrap();
        std::fs::create_dir_all(dir.join("state")).unwrap();
        dir
    }

    fn updater(dir: &Path) -> Updater {
        std::fs::write(dir.join("aster-core"), b"old").unwrap();
        Updater::new(dir.join("data"), dir.join("state"), dir.join("aster-core"))
    }

    fn state(dir: &Path, line: &str) {
        std::fs::write(dir.join("state").join(STATE), line).unwrap();
    }

    #[test]
    fn build_number_parses_only_digits() {
        assert_eq!(parse_build(None), 1);
        assert_eq!(parse_build(Some("763")), 763);
        assert_eq!(parse_build(Some("")), 1);
        assert_eq!(parse_build(Some("12a")), 1);
        assert_eq!(parse_build(Some("1234567890")), 1);
    }

    #[test]
    fn targets_are_one_safe_word() {
        for ok in ["eeadeb0", "v1.2", "release/2026-10", "MoonBot-F8"] {
            assert!(valid_target(ok), "{ok}");
        }
        for bad in [
            "",
            "-x",
            "a b",
            "a;rm",
            "..",
            "a..b",
            ".hidden",
            "/abs",
            &"x".repeat(65),
        ] {
            assert!(!valid_target(bad), "{bad}");
        }
    }

    #[test]
    fn request_carries_id_target_and_running_build() {
        let dir = temp("request");
        let mut u = updater(&dir);
        u.ask("eeadeb0", false, 1_000).unwrap();
        let req = std::fs::read_to_string(dir.join("data").join(REQUEST)).unwrap();
        assert_eq!(req, format!("1000 0 eeadeb0 {BUILD} {COMMIT}\n"));
        assert!(u.ask("", true, 2_000).is_err(), "one update at a time");
    }

    #[test]
    fn staged_build_is_swapped_in_when_flat() {
        let dir = temp("swap");
        let mut u = updater(&dir);
        u.ask("", true, 0).unwrap();
        state(&dir, "0 building abc123 120\n");
        assert_eq!(
            u.check(true, 1_000),
            Step::Note("update: building abc123 120".into())
        );
        std::fs::write(dir.join("aster-core.next"), b"new").unwrap();
        state(&dir, "0 staged abc123 120\n");
        // A position: the build waits.
        assert!(matches!(u.check(false, 2_000), Step::Note(_)));
        assert_eq!(u.check(false, 3_000), Step::Wait);
        assert!(matches!(u.check(true, 4_000), Step::Restart(_)));
        assert_eq!(std::fs::read(dir.join("aster-core")).unwrap(), b"new");
        assert_eq!(std::fs::read(dir.join("aster-core.prev")).unwrap(), b"old");
        let ack = std::fs::read_to_string(dir.join("data").join(ACK)).unwrap();
        assert_eq!(ack, format!("0 swapped abc123 {BUILD}\n"));
        // On its way out the core takes no other update: it would clear that ack.
        assert!(u.pending());
        assert_eq!(u.check(true, 9_000), Step::Wait);
        assert!(u.ask("", true, 9_000).is_err());
        assert!(dir.join("data").join(ACK).exists());
    }

    #[test]
    fn a_position_past_the_wait_cancels_and_drops_the_staged_binary() {
        let dir = temp("cancel");
        let mut u = updater(&dir);
        u.ask("", true, 0).unwrap();
        std::fs::write(dir.join("aster-core.next"), b"new").unwrap();
        state(&dir, "0 staged abc123 120\n");
        assert!(matches!(u.check(false, 1_000), Step::Note(_)));
        let over = u.check(false, 1_000 + FLAT_WAIT_MS + 1);
        assert!(matches!(&over, Step::Over(w) if w.starts_with("update cancelled")));
        assert!(!dir.join("aster-core.next").exists());
        assert_eq!(std::fs::read(dir.join("aster-core")).unwrap(), b"old");
        assert!(!u.pending());
    }

    #[test]
    fn failures_silence_and_stale_files_end_the_update() {
        let dir = temp("fail");
        let mut u = updater(&dir);
        // A state line of an earlier request is not this one's answer.
        u.ask("", true, 5_000).unwrap();
        state(&dir, "1 failed old news\n");
        assert_eq!(u.check(true, 6_000), Step::Wait);
        state(&dir, "5000 failed deadbeef is not on main\n");
        assert_eq!(
            u.check(true, 7_000),
            Step::Over("update failed: deadbeef is not on main".into())
        );
        assert!(!u.pending());
        // No updater at all.
        u.ask("", true, 10_000).unwrap();
        assert_eq!(u.check(true, 11_000), Step::Wait);
        assert!(matches!(
            u.check(true, 10_000 + ANSWER_WAIT_MS + 1),
            Step::Over(_)
        ));
        assert!(!u.pending());
        assert!(
            !dir.join("data").join(REQUEST).exists(),
            "a request nobody took is not built later"
        );
    }

    #[test]
    fn start_reports_the_update_once() {
        let dir = temp("start");
        let u = updater(&dir);
        let data = dir.join("data");
        assert_eq!(u.on_start(), None);
        std::fs::write(data.join(ACK), format!("7 swapped {COMMIT} 119\n")).unwrap();
        let said = u.on_start().unwrap();
        assert!(said.starts_with("updated: build 119 →"), "{said}");
        assert_eq!(
            std::fs::read_to_string(data.join(ACK)).unwrap(),
            format!("7 running {COMMIT} {BUILD}\n")
        );
        assert_eq!(u.on_start(), None, "said once");
        // Put back by the updater: the old core says so, once.
        std::fs::write(data.join(ACK), "8 swapped 0000000 120\n").unwrap();
        state(&dir, "8 rolled-back build 121 did not come up in 120 s\n");
        assert_eq!(
            u.on_start(),
            Some("update rolled back: build 121 did not come up in 120 s".into())
        );
        assert_eq!(u.on_start(), None);
        // Swapped to a build that is not this binary: said, not silent.
        std::fs::write(data.join(ACK), "9 swapped 1111111 120\n").unwrap();
        assert!(u
            .on_start()
            .unwrap()
            .contains("will put the previous one back"));
    }
}
