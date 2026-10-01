//! MoonBot exported-key writer: mirror of [`crate::key_import`].
//!
//! Produces the V1 container (`F$xC2` password head) with endpoint and
//! transport-mode metadata, so a terminal imports it exactly like a key
//! exported from MoonBot. The checksum and buffer cipher are byte-exact copies
//! of the private helpers in `key_import.rs`, which cannot be reused without
//! editing the upstream file.

use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use zeroize::Zeroize;

use crate::{MoonKey, TransportMode};

const NEW_PWD_HEAD: &str = "F$xC2";
const PWD_TAIL: &str = "aR#d";
const FMT_VER_CUR: u8 = 1;
const KEY_CONTAINER_SIZE: usize = 72;
const PLAIN_SIZE: usize = 8 + 1 + KEY_CONTAINER_SIZE + 2 + 1 + 4 + 16 + 1;
const ID_IPV4: u8 = 0;
const ID_IPV6: u8 = 1;
const RND_LEN: usize = 8;
const UNIX_EPOCH_DELPHI_DAYS: f64 = 25_569.0;

/// Key material and endpoint of one server key.
#[derive(Clone)]
pub struct ServerKey {
    pub master_key: MoonKey,
    pub mac_key: MoonKey,
    /// Key label shown by the terminal (`TMoonProtoKeyContainer.rnd`, <=16 ASCII).
    pub rnd: String,
    /// Endpoint advertised in the export; `None` address keeps the client's own.
    pub address: Option<IpAddr>,
    pub port: u16,
    pub transport_mode: TransportMode,
}

impl std::fmt::Debug for ServerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerKey")
            .field("rnd", &self.rnd)
            .field("address", &self.address)
            .field("port", &self.port)
            .field("transport_mode", &self.transport_mode.name())
            .finish_non_exhaustive()
    }
}

impl ServerKey {
    /// Fresh random keys with a random 8-char label.
    pub fn generate(address: Option<IpAddr>, port: u16, transport_mode: TransportMode) -> Self {
        use rand::Rng;
        const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        let mut rng = rand::thread_rng();
        let rnd = (0..RND_LEN)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect();
        Self {
            master_key: rng.gen(),
            mac_key: rng.gen(),
            rnd,
            address,
            port,
            transport_mode,
        }
    }

    /// Base64 export string accepted by [`crate::import_key`].
    pub fn export(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.export_at(now)
    }

    fn export_at(&self, unix_secs: i64) -> String {
        let mut plain = vec![0u8; PLAIN_SIZE];
        plain[0..8].copy_from_slice(&unix_secs.to_le_bytes());
        plain[8] = FMT_VER_CUR;
        self.write_container(&mut plain[9..9 + KEY_CONTAINER_SIZE], unix_secs);

        let mut off = 9 + KEY_CONTAINER_SIZE;
        plain[off..off + 2].copy_from_slice(&self.port.to_le_bytes());
        off += 2;
        match self.address {
            Some(IpAddr::V6(v6)) => {
                plain[off] = ID_IPV6;
                plain[off + 5..off + 21].copy_from_slice(&v6.octets());
            }
            other => {
                plain[off] = ID_IPV4;
                let v4 = match other {
                    Some(IpAddr::V4(v4)) => u32::from(v4),
                    _ => 0,
                };
                plain[off + 1..off + 5].copy_from_slice(&v4.to_le_bytes());
            }
        }
        off += 1 + 4 + 16;
        plain[off] = self.transport_mode.to_byte();

        let checksum = calculate_checksum_w(&plain);
        let mut password = password_bytes(NEW_PWD_HEAD, unix_secs);
        encode_buffer(&mut plain, &password);
        password.zeroize();

        let mut raw = Vec::with_capacity(16 + PLAIN_SIZE);
        raw.extend_from_slice(&unix_secs.to_le_bytes());
        raw.extend_from_slice(&checksum.to_le_bytes());
        raw.extend_from_slice(&plain);
        plain.zeroize();
        base64::engine::general_purpose::STANDARD.encode(raw)
    }

    fn write_container(&self, c: &mut [u8], unix_secs: i64) {
        let rnd = self.rnd.as_bytes();
        let rnd_len = rnd.len().min(16);
        c[0] = rnd_len as u8;
        c[1..1 + rnd_len].copy_from_slice(&rnd[..rnd_len]);
        c[17] = 1; // filled
        let date = unix_secs as f64 / 86_400.0 + UNIX_EPOCH_DELPHI_DAYS;
        c[18..26].copy_from_slice(&date.to_le_bytes());
        c[30] = 1; // container version
        c[32..48].copy_from_slice(&self.master_key);
        c[48..64].copy_from_slice(&self.mac_key);
    }
}

fn password_bytes(password_head: &str, ts: i64) -> Vec<u8> {
    format!("{password_head}{ts}{PWD_TAIL}")
        .bytes()
        .take(25)
        .collect()
}

/// MoonBot `CalculateCheckSumW` x64 algorithm (copy of `key_import.rs`).
fn calculate_checksum_w(buf: &[u8]) -> i64 {
    let mut rax = 0u64;
    for &byte in buf {
        let bl = byte ^ 0b1010_1010;
        let bh = byte ^ 0b0011_1001;

        let (al, cf) = add8(rax as u8, bl);
        rax = set_al(rax, al);
        let (next_rax, cf) = rcl64_1(rax, cf);
        rax = next_rax;
        let (al, _) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);
        let (next_rax, cf) = rol64_8(rax);
        rax = next_rax;
        let ah = ((rax >> 8) & 0xFF) as u8;
        let (ah, cf) = adc8(ah, bh, cf);
        rax = set_ah(rax, ah);
        let (al, _) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);
        let (next_rax, cf) = rol64_8(rax);
        rax = next_rax;
        let (al, _) = adc8(rax as u8, 0, cf);
        rax = set_al(rax, al);
    }
    rax as i64
}

fn add8(a: u8, b: u8) -> (u8, bool) {
    let sum = a as u16 + b as u16;
    (sum as u8, sum > 0xFF)
}

fn adc8(a: u8, b: u8, carry: bool) -> (u8, bool) {
    let sum = a as u16 + b as u16 + u16::from(carry);
    (sum as u8, sum > 0xFF)
}

fn rcl64_1(value: u64, carry: bool) -> (u64, bool) {
    ((value << 1) | u64::from(carry), (value & (1 << 63)) != 0)
}

fn rol64_8(value: u64) -> (u64, bool) {
    let rotated = value.rotate_left(8);
    (rotated, (rotated & 1) != 0)
}

fn set_al(value: u64, al: u8) -> u64 {
    (value & !0xFF) | u64::from(al)
}

fn set_ah(value: u64, ah: u8) -> u64 {
    (value & !(0xFF << 8)) | (u64::from(ah) << 8)
}

/// Inverse of `key_import::decode_buffer` (sfunc.pas EncodeBuffer).
fn encode_buffer(buf: &mut [u8], code: &[u8]) {
    let at = |i: usize| code.get(i).copied().unwrap_or(0);
    for (counter, byte) in buf.iter_mut().enumerate() {
        let al = (counter & 0xFF) as u8;
        let ah = ((counter >> 8) & 0xFF) as u8;
        let nibble = counter & 0xF;
        let c2_mod = (at(2) & 7) as u32;
        let cn1 = at(nibble + 1);
        let cl_val = at(nibble + 2).wrapping_add(nibble as u8).wrapping_add(ah);
        let cl_val2 = at(nibble).wrapping_add(cn1).wrapping_add(1);

        let mut b = *byte;
        b = b.wrapping_add(al);
        b = b.wrapping_add(ah);
        b ^= cl_val2;
        b = b.wrapping_add(nibble as u8);
        b = b.rotate_left((cn1 & 7) as u32);
        b ^= cl_val;
        b = b.wrapping_add(at(3));
        b ^= at(0);
        b = b.wrapping_add(at(1) ^ ah);
        b = b.rotate_right(c2_mod);
        b = b.wrapping_add(al);
        b = b.rotate_left(c2_mod);
        b ^= al;
        b = b.wrapping_add(al);
        b = b.wrapping_add(ah);
        *byte = b;
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::key_import::{parse_key_info, ImportedKeyFormat};

    #[test]
    fn export_round_trips_through_import() {
        let key = ServerKey {
            master_key: [0x11; 16],
            mac_key: [0x22; 16],
            rnd: "TINVEST1".into(),
            address: Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port: 3100,
            transport_mode: TransportMode::V2,
        };
        // 2026-01-01T00:00:00Z
        let info = parse_key_info(&key.export_at(1_767_225_600)).expect("import");
        assert_eq!(info.format, ImportedKeyFormat::V1);
        assert_eq!(info.keys.master_key, key.master_key);
        assert_eq!(info.keys.mac_key, key.mac_key);
        assert_eq!(info.rnd, "TINVEST1");
        assert_eq!(info.display_name, "TINVEST1  01.01.2026 00:00");
        let net = info.network.expect("network");
        assert_eq!(net.address, key.address);
        assert_eq!(net.port, 3100);
        assert_eq!(net.transport_mode, TransportMode::V2);
    }

    #[test]
    fn generated_key_imports_without_address() {
        let key = ServerKey::generate(None, 4000, TransportMode::V0);
        let info = parse_key_info(&key.export()).expect("import");
        assert_eq!(info.keys.master_key, key.master_key);
        assert_eq!(info.rnd.len(), RND_LEN);
        let net = info.network.expect("network");
        assert_eq!(net.address, None);
        assert_eq!(net.port, 4000);
    }
}
