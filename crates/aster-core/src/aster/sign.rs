//! Aster v3 request signing: EIP-712 over the request's own query string.
//!
//! The scheme, read from `asterdex/api-docs` (`aster-finance-futures-api-v3.md`
//! and its testnet twin, 23.09.2026), not from memory:
//!
//! - the business parameters, then `nonce`, `user` and `signer`, are
//!   form-encoded in that order into one string;
//! - that string is the `msg` of an EIP-712 `Message(string msg)` under the
//!   domain `AsterSignTransaction` / `"1"` / chain id / the zero contract —
//!   chain id **1666 on mainnet, 714 on testnet**, the one field the two differ
//!   in;
//! - the 65-byte `r‖s‖v` signature goes last, as `&signature=0x…`, and the
//!   gateway checks it against the string it was sent.
//!
//! The nonce is the time in microseconds, accepted within ±60 s of the
//! gateway's clock and remembered per API wallet (the last 100): a repeat is a
//! duplicate, an older one than all of them is expired. So the nonce here is
//! the clock but never less than one past the last one issued.
//!
//! This is the one place a secret becomes something that leaves the process
//! (`AGENTS.md`, `## Secrets`). Nothing here logs, prints or formats the key or
//! a signature; [`Credentials`] has a `Debug` that shows the addresses only.

use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};
use zeroize::Zeroize;

/// Which Aster the account lives on. The signed form is the same on both; the
/// chain id inside the signature is not, so a key used against the wrong one
/// is refused as a bad signature rather than as an unknown account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn chain_id(self) -> u64 {
        match self {
            Self::Mainnet => 1666,
            Self::Testnet => 714,
        }
    }

    /// Where this network's signed REST calls go.
    pub fn rest_base(self) -> &'static str {
        match self {
            Self::Mainnet => super::rest::BASE,
            Self::Testnet => "https://fapi.asterdex-testnet.com",
        }
    }

    /// The host of this network's user-data stream: the one each network's
    /// docs name in their "User Data Streams" section. The testnet docs name
    /// `fstream5.` for the market streams and `fstream.` here; measured 01.10,
    /// the two resolve to the same two addresses.
    pub fn ws_host(self) -> &'static str {
        match self {
            Self::Mainnet => super::ws::HOST,
            Self::Testnet => "fstream.asterdex-testnet.com",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }

    /// `mainnet` or `testnet`, any case; anything else is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mainnet" => Some(Self::Mainnet),
            "testnet" => Some(Self::Testnet),
            _ => None,
        }
    }
}

/// Why a key file did not yield credentials. No variant carries any of the
/// file's text: an error message is exactly where a key would leak to the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyFileError {
    /// No 64-hex-digit token, i.e. no EVM private key.
    NoPrivateKey,
    /// More than one 64-hex-digit token; which one signs is not ours to guess.
    SeveralPrivateKeys,
    /// The 64 hex digits are not a valid secp256k1 scalar (zero, or ≥ n).
    InvalidPrivateKey,
    /// More than one address besides the API wallet's own, so the main
    /// account cannot be told apart.
    SeveralUsers,
}

impl fmt::Display for KeyFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoPrivateKey => "no EVM private key (64 hex digits) in the file",
            Self::SeveralPrivateKeys => {
                "more than one 64-hex-digit token; keep only the API wallet's key"
            }
            Self::InvalidPrivateKey => "the 64 hex digits are not a valid secp256k1 private key",
            Self::SeveralUsers => {
                "more than one address besides the API wallet's own; keep only the main account's"
            }
        })
    }
}

impl std::error::Error for KeyFileError {}

/// The API wallet's key and the two addresses a v3 request names.
///
/// `signer` is DERIVED from the key, never read from the file: the docs say
/// the key is the signer's, so a written-down signer address can only agree
/// with it or be wrong. `user` is the main account — whatever address in the
/// file is not the signer's.
pub struct Credentials {
    key: SigningKey,
    signer: String,
    user: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("signer", &self.signer)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl Credentials {
    /// Read the key file at `path`. The I/O error is the only text that can
    /// come back, and it carries neither the path nor the contents — the
    /// caller names the path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, LoadError> {
        let mut text = std::fs::read_to_string(path).map_err(LoadError::Io)?;
        let parsed = Self::parse(&text).map_err(LoadError::Format);
        text.zeroize();
        parsed
    }

    /// Credentials from the text of a key file, in whatever layout it was
    /// written: labels (`key=`, `"privateKey":`, `user:`) are ignored, and
    /// the tokens are told apart by shape — 64 hex digits are the key, 40 are
    /// an address, with or without `0x` either way.
    pub fn parse(text: &str) -> Result<Self, KeyFileError> {
        let mut keys = Vec::new();
        let mut addresses = Vec::new();
        for token in text.split(|c: char| !c.is_ascii_alphanumeric()) {
            let hex = token
                .strip_prefix("0x")
                .or_else(|| token.strip_prefix("0X"))
                .unwrap_or(token);
            if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            match hex.len() {
                64 => keys.push(hex),
                40 => addresses.push(hex),
                _ => {}
            }
        }
        let hex = match keys.as_slice() {
            [] => return Err(KeyFileError::NoPrivateKey),
            [one] => *one,
            _ => return Err(KeyFileError::SeveralPrivateKeys),
        };
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = (hex_val(hex.as_bytes()[2 * i]) << 4) | hex_val(hex.as_bytes()[2 * i + 1]);
        }
        let key = SigningKey::from_slice(&bytes);
        bytes.fill(0);
        let key = key.map_err(|_| KeyFileError::InvalidPrivateKey)?;

        let signer = address_of(&key);
        let mut users: Vec<String> = Vec::new();
        for a in addresses.iter().map(|a| checksum(a)) {
            if !a.eq_ignore_ascii_case(&signer) && !users.iter().any(|u| u == &a) {
                users.push(a);
            }
        }
        let user = match users.len() {
            0 => None,
            1 => users.pop(),
            _ => return Err(KeyFileError::SeveralUsers),
        };
        Ok(Self { key, signer, user })
    }

    /// The API wallet's address, EIP-55 checksummed.
    pub fn signer(&self) -> &str {
        &self.signer
    }

    /// The main account's address, if the file names one.
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }
}

#[derive(Debug)]
pub enum LoadError {
    Io(std::io::Error),
    Format(KeyFileError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Format(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Signs requests for one API wallet on one network and owns its nonce.
///
/// One per wallet, not one per thread: the gateway tracks nonces per API
/// wallet, so two signers of the same wallet could issue the same microsecond
/// twice and the second request would be refused as a duplicate. A clone is
/// that same signer for another thread — it shares the key (not a copy of
/// it) and the one nonce sequence, so the account reader and the order worker
/// never issue the same nonce.
#[derive(Clone)]
pub struct Signer {
    creds: Arc<Credentials>,
    network: Network,
    domain: [u8; 32],
    last_nonce: Arc<AtomicU64>,
}

impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signer")
            .field("creds", &self.creds)
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

impl Signer {
    pub fn new(creds: Credentials, network: Network) -> Self {
        Self {
            domain: domain_separator(network.chain_id()),
            creds: Arc::new(creds),
            network,
            last_nonce: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub fn credentials(&self) -> &Credentials {
        &self.creds
    }

    /// The next nonce: `now_us`, the gateway's time in microseconds, unless an
    /// earlier call already took that microsecond or a later one.
    pub fn next_nonce(&mut self, now_us: u64) -> u64 {
        let next = |last: u64| now_us.max(last + 1);
        let last = self
            .last_nonce
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                Some(next(last))
            })
            .expect("the update always yields");
        next(last)
    }

    /// The complete signed query string for `params`: the parameters, then
    /// `nonce`, `user` (when known) and `signer`, then `signature` — the form
    /// that goes after `?` on a GET and into the body of a POST or DELETE.
    pub fn sign_query(&mut self, params: &[(&str, &str)], now_us: u64) -> String {
        let nonce = self.next_nonce(now_us).to_string();
        let mut msg = String::new();
        let mut push = |k: &str, v: &str| {
            if !msg.is_empty() {
                msg.push('&');
            }
            encode_into(&mut msg, k);
            msg.push('=');
            encode_into(&mut msg, v);
        };
        for (k, v) in params {
            push(k, v);
        }
        push("nonce", &nonce);
        if let Some(user) = &self.creds.user {
            push("user", user);
        }
        push("signer", &self.creds.signer);

        let sig = sign_digest(&self.creds.key, &message_digest(&self.domain, &msg));
        msg.push_str("&signature=0x");
        for b in sig {
            msg.push(HEX[(b >> 4) as usize] as char);
            msg.push(HEX[(b & 15) as usize] as char);
        }
        msg
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => c - b'A' + 10,
    }
}

fn keccak(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Keccak256::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// `hashStruct(EIP712Domain)` for Aster's domain on `chain_id`.
fn domain_separator(chain_id: u64) -> [u8; 32] {
    let type_hash = keccak(&[
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ]);
    let mut chain = [0u8; 32];
    chain[24..].copy_from_slice(&chain_id.to_be_bytes());
    keccak(&[
        &type_hash,
        &keccak(&[b"AsterSignTransaction"]),
        &keccak(&[b"1"]),
        &chain,
        // The zero `verifyingContract`, left-padded to a word.
        &[0u8; 32],
    ])
}

/// The EIP-712 digest of `Message { msg }` under `domain`.
fn message_digest(domain: &[u8; 32], msg: &str) -> [u8; 32] {
    let struct_hash = keccak(&[
        &keccak(&[b"Message(string msg)"]),
        &keccak(&[msg.as_bytes()]),
    ]);
    keccak(&[b"\x19\x01", domain, &struct_hash])
}

/// `r‖s‖v` with `v` = 27 + recovery id, the form `eth_account` produces.
fn sign_digest(key: &SigningKey, digest: &[u8; 32]) -> [u8; 65] {
    let (sig, recid) = key
        .sign_prehash_recoverable(digest)
        // A 32-byte prehash under a valid key cannot fail; the arm exists
        // because the signature is generic over lengths that can.
        .expect("a 32-byte prehash always signs");
    let mut out = [0u8; 65];
    out[..64].copy_from_slice(&sig.to_bytes());
    out[64] = 27 + recid.to_byte();
    out
}

/// The EIP-55 address of `key`'s public key.
fn address_of(key: &SigningKey) -> String {
    let point = key.verifying_key().to_encoded_point(false);
    let hash = keccak(&[&point.as_bytes()[1..]]);
    let mut hex = String::with_capacity(40);
    for b in &hash[12..] {
        hex.push(HEX[(b >> 4) as usize] as char);
        hex.push(HEX[(b & 15) as usize] as char);
    }
    checksum(&hex)
}

/// EIP-55 mixed-case form of a 40-hex-digit address, `0x` included.
fn checksum(hex: &str) -> String {
    let lower = hex.to_ascii_lowercase();
    let hash = keccak(&[lower.as_bytes()]);
    let mut out = String::from("0x");
    for (i, c) in lower.chars().enumerate() {
        let nibble = (hash[i / 2] >> if i % 2 == 0 { 4 } else { 0 }) & 15;
        out.push(if c.is_ascii_alphabetic() && nibble >= 8 {
            c.to_ascii_uppercase()
        } else {
            c
        });
    }
    out
}

/// Form-encode `s` the way the docs' `urllib.parse.urlencode` does: letters,
/// digits and `_.-~` as they are, space as `+`, every other byte `%XX`. The
/// site's unsigned open-interest query (`rest.rs`) encodes its symbol with it too.
pub(super) fn encode_into(out: &mut String, s: &str) {
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX.to_ascii_uppercase()[(b >> 4) as usize] as char);
                out.push(HEX.to_ascii_uppercase()[(b & 15) as usize] as char);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The demo wallet published in the docs' own example — public, holds
    /// nothing. Every expected value below was produced by `eth_account`
    /// 0.13.7 (`encode_typed_data` + `Account.sign_message`) from this key.
    const DEMO_KEY: &str = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1";
    const DEMO_SIGNER: &str = "0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0";
    const DEMO_USER: &str = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e";

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn the_signer_is_derived_from_the_key_as_the_docs_pair_them() {
        let c = Credentials::parse(DEMO_KEY).unwrap();
        assert_eq!(c.signer(), DEMO_SIGNER);
        assert_eq!(c.user(), None);
    }

    #[test]
    fn digest_and_signature_match_eth_account_byte_for_byte() {
        let c = Credentials::parse(DEMO_KEY).unwrap();
        let cases = [
            (
                1666,
                "symbol=ASTERUSDT&type=LIMIT&side=BUY&timeInForce=GTC&quantity=20&price=0.5\
                 &nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e\
                 &signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0",
                "214122039b686e20a15baab91d368d354cf8c88067cb8b0e96e9018d8b08a625",
                "0a56c5923ebf3524475c5f631940ec4c0e41dbd300ad28198d1915d8f3ca49ce\
                 26fd1aeb8b0c3079133595da0de38322ae7e860e0c76d032c4a491d27c1430b01c",
            ),
            (
                714,
                "nonce=1748310859508867&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0",
                "82a021bbc2deda6682bad987800125e5688e4d9425474d4ef401fa9d4aaa41dd",
                "2579c8dbfb5b908736407164cbb6bc270af2fcde07b5253c8908656c0720537f\
                 0dca80577799f22fd3fc9696787154a76c1bcacf215cb12ce9413396185ba1661b",
            ),
            (
                1666,
                "",
                "e0f1aad7fc9bb5b14c501bb69b110286c94406576860971231b381b9eb11dbef",
                "b7fe1961a997fb3fab846eadbfb0cbf0297029df88d0b5f5f1f738f10f8bbd33\
                 7a5baeda44397cb37248146bec810048988a5597bb841de18d298dbc1b8994201b",
            ),
        ];
        for (chain, msg, digest, sig) in cases {
            let d = message_digest(&domain_separator(chain), msg);
            assert_eq!(hex(&d), digest, "digest, chain {chain}");
            assert_eq!(
                hex(&sign_digest(&c.key, &d)),
                sig,
                "signature, chain {chain}"
            );
        }
    }

    #[test]
    fn the_signed_query_is_the_signed_message_plus_its_signature() {
        let text = format!("user = {DEMO_USER}\nkey: {DEMO_KEY}\nsigner {DEMO_SIGNER}\n");
        let mut s = Signer::new(Credentials::parse(&text).unwrap(), Network::Mainnet);
        let q = s.sign_query(
            &[
                ("symbol", "ASTERUSDT"),
                ("type", "LIMIT"),
                ("side", "BUY"),
                ("timeInForce", "GTC"),
                ("quantity", "20"),
                ("price", "0.5"),
            ],
            1748310859508867,
        );
        // The first vector above, with its signature appended.
        assert_eq!(
            q,
            "symbol=ASTERUSDT&type=LIMIT&side=BUY&timeInForce=GTC&quantity=20&price=0.5\
             &nonce=1748310859508867&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e\
             &signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0\
             &signature=0x0a56c5923ebf3524475c5f631940ec4c0e41dbd300ad28198d1915d8f3ca49ce\
             26fd1aeb8b0c3079133595da0de38322ae7e860e0c76d032c4a491d27c1430b01c"
        );
    }

    #[test]
    fn a_nonce_is_never_reused_even_within_one_microsecond_or_a_clock_step_back() {
        let mut s = Signer::new(Credentials::parse(DEMO_KEY).unwrap(), Network::Testnet);
        assert_eq!(s.next_nonce(1_000), 1_000);
        assert_eq!(s.next_nonce(1_000), 1_001);
        assert_eq!(s.next_nonce(900), 1_002, "the clock stepped back");
        assert_eq!(s.next_nonce(5_000), 5_000);
    }

    #[test]
    fn the_key_file_layout_does_not_matter_but_its_tokens_do() {
        let json = format!(
            r#"{{"privateKey":"{}","user":"{DEMO_USER}"}}"#,
            &DEMO_KEY[2..]
        );
        let c = Credentials::parse(&json).unwrap();
        assert_eq!((c.signer(), c.user()), (DEMO_SIGNER, Some(DEMO_USER)));
        // A lower-cased user is checksummed, and the signer's own address in the
        // file is recognised as the signer's whatever its case.
        let text = format!(
            "{}\n{}\n{DEMO_KEY}",
            DEMO_USER.to_ascii_lowercase(),
            DEMO_SIGNER.to_ascii_lowercase()
        );
        assert_eq!(Credentials::parse(&text).unwrap().user(), Some(DEMO_USER));

        assert_eq!(
            Credentials::parse("user=0x1").unwrap_err(),
            KeyFileError::NoPrivateKey
        );
        assert_eq!(
            Credentials::parse(&format!("{DEMO_KEY} {DEMO_KEY}")).unwrap_err(),
            KeyFileError::SeveralPrivateKeys
        );
        assert_eq!(
            Credentials::parse(&"0".repeat(64)).unwrap_err(),
            KeyFileError::InvalidPrivateKey
        );
        let two = format!("{DEMO_KEY} {DEMO_USER} 0x{}", "1".repeat(40));
        assert_eq!(
            Credentials::parse(&two).unwrap_err(),
            KeyFileError::SeveralUsers
        );
    }

    #[test]
    fn nothing_secret_reaches_debug_or_an_error() {
        let c = Credentials::parse(DEMO_KEY).unwrap();
        let mut shown = format!("{c:?}");
        shown += &format!(" {:?}", Signer::new(c, Network::Mainnet));
        assert!(!shown.contains(&DEMO_KEY[2..]), "{shown}");
        assert!(shown.contains(DEMO_SIGNER));
    }

    #[test]
    fn values_are_form_encoded_like_urlencode() {
        let mut out = String::new();
        encode_into(&mut out, "a b,[\"x\"]~_.-");
        assert_eq!(out, "a+b%2C%5B%22x%22%5D~_.-");
    }

    #[test]
    fn the_network_names_its_chain() {
        assert_eq!(
            Network::parse(" Testnet ").map(Network::chain_id),
            Some(714)
        );
        assert_eq!(Network::parse("mainnet").map(Network::chain_id), Some(1666));
        assert_eq!(Network::parse("devnet"), None);
    }
}
