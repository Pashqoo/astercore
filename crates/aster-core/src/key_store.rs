//! Persisted server key: one file holding the MoonBot-format base64 export.
//!
//! The export already carries keys, port and transport mode, so it is the
//! single source of truth; the same string is what the user pastes into the
//! terminal.
//!
//! Ported from TInvestCore unchanged but for the file name: the container is
//! MoonProto's, not the venue's, and a redesign on the way over would be a
//! second key format to debug against a terminal that reads only one.

use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::Path;

use moonproto::key_import::parse_key_info;
use moonproto::server::key_export::ServerKey;
use moonproto::TransportMode;

pub const DEFAULT_PATH: &str = "aster-core.key";

/// Read an existing key, and never mint one. A client of the core — a probe,
/// a deploy check — must use this: minting a key in the core's directory
/// during a rotation pins an address the core then keeps.
pub fn load(path: impl AsRef<Path>) -> io::Result<ServerKey> {
    let path = path.as_ref();
    let b64 = fs::read_to_string(path)?;
    let key = parse(&b64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, path.display().to_string()))?;
    make_private(path);
    Ok(key)
}

/// `address` is the endpoint a new key advertises — where the terminal dials.
/// `None` exports a zero address, and the terminal reads that as `127.0.0.1`:
/// right for a core on the same machine, useless for one on a server. The key
/// material of an existing file is never rewritten (only a mode left open by
/// an older core is tightened), so changing the address means removing the key
/// and importing the new one into the terminal.
pub fn load_or_create(
    path: impl AsRef<Path>,
    address: Option<IpAddr>,
    port: u16,
) -> io::Result<ServerKey> {
    let path = path.as_ref();
    match load(path) {
        Ok(key) => Ok(key),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let key = ServerKey::generate(address, port, TransportMode::V2);
            write_private(path, &key.export())?;
            Ok(key)
        }
        Err(e) => Err(e),
    }
}

/// The export *is* the private key, so the file is born unreadable to anyone
/// else; `create_new` keeps a key that appeared meanwhile from being clobbered.
#[cfg(unix)]
fn write_private(path: &Path, contents: &str) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())
}

#[cfg(not(unix))]
fn write_private(path: &Path, contents: &str) -> io::Result<()> {
    fs::write(path, contents)
}

/// Tighten a key file an older core left readable to everyone. A failure here
/// is reported, not fatal: a readable key is worse than the previous start,
/// but refusing to trade over it is worse still.
#[cfg(unix)]
fn make_private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mode = match fs::metadata(path) {
        Ok(meta) => meta.permissions().mode() & 0o777,
        Err(e) => {
            log::warn!("key: cannot read the mode of {}: {e}", path.display());
            return;
        }
    };
    if mode & 0o077 == 0 {
        return;
    }
    match fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
        Ok(()) => log::warn!(
            "key: {} was mode {mode:o}, tightened to 600",
            path.display()
        ),
        Err(e) => log::error!(
            "key: {} is mode {mode:o} and cannot be tightened: {e}",
            path.display()
        ),
    }
}

#[cfg(not(unix))]
fn make_private(_path: &Path) {}

fn parse(b64: &str) -> Option<ServerKey> {
    let info = parse_key_info(b64)?;
    let net = info.network?;
    Some(ServerKey {
        master_key: info.keys.master_key,
        mac_key: info.keys.mac_key,
        rnd: info.rnd,
        address: net.address,
        port: net.port,
        transport_mode: net.transport_mode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("aster-core-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_then_load_returns_same_key() {
        let dir = scratch("same");
        let path = dir.join("k.key");
        let created = load_or_create(&path, None, 3101).unwrap();
        let loaded = load_or_create(&path, None, 9).unwrap();
        assert_eq!(loaded.master_key, created.master_key);
        assert_eq!(loaded.mac_key, created.mac_key);
        assert_eq!(loaded.port, 3101);
        assert_eq!(loaded.rnd, created.rnd);
        fs::remove_dir_all(dir).unwrap();
    }

    /// The address the terminal dials survives the export/import round trip —
    /// a zero address there is what sent the terminal to 127.0.0.1.
    #[test]
    fn created_key_advertises_the_given_address() {
        let dir = scratch("addr");
        let path = dir.join("k.key");
        let addr: IpAddr = "203.0.113.29".parse().unwrap();
        let created = load_or_create(&path, Some(addr), 3101).unwrap();
        let loaded = load_or_create(&path, None, 3101).unwrap();
        assert_eq!(created.address, Some(addr));
        assert_eq!(loaded.address, Some(addr));
        assert_eq!(loaded.port, 3101);
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn created_key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = scratch("mode");
        let path = dir.join("k.key");
        load_or_create(&path, None, 3101).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the key file must not be readable by others");
        fs::remove_dir_all(dir).unwrap();
    }

    /// A client reading the core's directory mid-rotation must not mint a key
    /// there: the core would then keep whatever address that key carries.
    #[test]
    fn load_never_creates_a_key() {
        let dir = scratch("load");
        let path = dir.join("k.key");
        let err = load(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!path.exists(), "load must leave the directory untouched");
        fs::remove_dir_all(dir).unwrap();
    }

    /// A key written by an older core is 0644; loading it must not leave it so.
    #[cfg(unix)]
    #[test]
    fn loading_tightens_a_world_readable_key() {
        use std::os::unix::fs::PermissionsExt;

        let dir = scratch("tighten");
        let path = dir.join("k.key");
        let created = load_or_create(&path, None, 3101).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

        let loaded = load_or_create(&path, None, 3101).unwrap();
        assert_eq!(loaded.rnd, created.rnd, "the key itself must survive");
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        fs::remove_dir_all(dir).unwrap();
    }
}
