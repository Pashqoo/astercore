//! Server side of the transport layer: pack S->C datagrams, unpack C->S ones.
//!
//! Mirrors `crate::transport` (which only packs client packets and unpacks
//! server packets). The STUN/DNS helpers duplicate private functions of
//! `transport::extended` because upstream files are not edited.

use crate::transport::{outer_light_crypt, ClientMsgHeader, MacContext, TRANSPORT_VER};

pub(crate) const SERVER_HDR_SIZE: usize = 7;
pub(crate) const CLIENT_HDR_SIZE: usize = crate::transport::CLIENT_HDR_SIZE;

const STUN_MAGIC: [u8; 4] = [0x21, 0x12, 0xA4, 0x42];
const DNS_WARMUP_RESPONSE: [u8; 17] = [
    0x4D, 0x50, 0x81, 0x80, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x01,
];

/// Result of unpacking one client datagram.
pub(crate) enum Inbound {
    Packet {
        cmd: u8,
        client_id: u64,
        payload: Vec<u8>,
    },
    /// V2 DNS warm-up probe; answer with [`dns_warmup_response`].
    DnsWarmup,
}

/// Pack one server command into a wire-ready datagram for `mask_ver` (0..=2).
pub(crate) fn pack_server_packet(
    mac_ctx: &MacContext,
    cmd: u8,
    payload: &[u8],
    mask_ver: u8,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SERVER_HDR_SIZE + payload.len());
    buf.push(rand::random::<u8>());
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.push(TRANSPORT_VER);
    buf.push(cmd);
    buf.extend_from_slice(payload);
    let mac = mac_ctx.mac(&buf);
    buf[1..5].copy_from_slice(&mac.to_le_bytes());
    outer_light_crypt(&mut buf, mac_ctx.obf_key());
    if mask_ver == 1 {
        wrap_in_stun(&mut buf);
    }
    buf
}

/// Unpack one client datagram: mode unwrap, de-obfuscation, MAC and version check.
pub(crate) fn unpack_client_packet(
    mac_ctx: &MacContext,
    raw: &[u8],
    mask_ver: u8,
) -> Option<Inbound> {
    let mut buf = match mask_ver {
        1 => unwrap_from_stun(raw)?,
        2 if is_dns_warmup(raw) => return Some(Inbound::DnsWarmup),
        _ => raw.to_vec(),
    };
    if buf.len() < CLIENT_HDR_SIZE {
        return None;
    }
    outer_light_crypt(&mut buf, mac_ctx.obf_key());
    let hdr = ClientMsgHeader::from_bytes(&buf)?;
    let saved = [buf[1], buf[2], buf[3], buf[4]];
    buf[1..5].copy_from_slice(&0u32.to_le_bytes());
    let computed = mac_ctx.mac(&buf);
    buf[1..5].copy_from_slice(&saved);
    if computed != hdr.checksum || hdr.ver != TRANSPORT_VER {
        return None;
    }
    buf.drain(..CLIENT_HDR_SIZE);
    Some(Inbound::Packet {
        cmd: hdr.cmd,
        client_id: hdr.client_id,
        payload: buf,
    })
}

pub(crate) fn dns_warmup_response() -> &'static [u8] {
    &DNS_WARMUP_RESPONSE
}

/// Delphi `WrapInSTUN(IsServer=True)`: Binding Response framing.
fn wrap_in_stun(data: &mut Vec<u8>) {
    let len = data.len();
    if len == 0 {
        return;
    }
    let mut w;
    if len <= 11 {
        w = vec![0u8; 20];
        w[8] = len as u8;
        w[9..9 + len].copy_from_slice(data);
    } else {
        let attr_len = 4 + (len - 12);
        w = vec![0u8; 20 + attr_len];
        w[2] = (attr_len >> 8) as u8;
        w[3] = (attr_len & 0xFF) as u8;
        w[8..20].copy_from_slice(&data[..12]);
        w[21] = 0x13;
        let payload_len = len - 12;
        w[22] = (payload_len >> 8) as u8;
        w[23] = (payload_len & 0xFF) as u8;
        w[24..].copy_from_slice(&data[12..]);
    }
    w[0..2].copy_from_slice(&[0x01, 0x01]);
    w[4..8].copy_from_slice(&STUN_MAGIC);
    *data = w;
}

fn unwrap_from_stun(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 20 || raw[4..8] != STUN_MAGIC || raw[1] != 0x01 || raw[0] > 0x01 {
        return None;
    }
    let len = ((raw[2] as usize) << 8) | raw[3] as usize;
    if len == 0 {
        let payload_len = raw[8] as usize;
        if !(1..=11).contains(&payload_len) {
            return None;
        }
        return Some(raw[9..9 + payload_len].to_vec());
    }
    if raw.len() < 24 || raw[20] != 0x00 || raw[21] != 0x13 {
        return None;
    }
    let attr_len = ((raw[22] as usize) << 8) | raw[23] as usize;
    if raw.len() < 24 + attr_len {
        return None;
    }
    let mut out = Vec::with_capacity(12 + attr_len);
    out.extend_from_slice(&raw[8..20]);
    out.extend_from_slice(&raw[24..24 + attr_len]);
    Some(out)
}

fn is_dns_warmup(d: &[u8]) -> bool {
    d.len() >= 17
        && d[0] == 0x4D
        && d[1] == 0x50
        && ((d[2] == 0x01 && d[3] == 0x00) || (d[2] == 0x81 && d[3] == 0x80))
        && d[4] == 0x00
        && d[5] == 0x01
        && d[12] == 0x00
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{pack_client_packet, transport_unpack, ClientTransportModeState};

    fn client_packet(
        mac_ctx: &MacContext,
        cmd: u8,
        payload: &[u8],
        mode: u8,
    ) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut buf = Vec::new();
        let mut state = ClientTransportModeState::new();
        let extra = pack_client_packet(
            &mut buf,
            mac_ctx,
            cmd,
            0xC11E_0001,
            payload,
            mode,
            &mut state,
        );
        (buf, extra)
    }

    #[test]
    fn server_packets_unpack_with_upstream_client_code_in_all_modes() {
        let key = [3u8; 16];
        let mac_ctx = MacContext::new(&key);
        for mode in 0..=2u8 {
            for payload in [&b"x"[..], &[7u8; 40][..], &[9u8; 1200][..]] {
                let packet = pack_server_packet(&mac_ctx, 33, payload, mode);
                let (hdr, decoded) = transport_unpack(&key, &packet, mode).expect("client unpack");
                assert_eq!(hdr.cmd, 33);
                assert_eq!(decoded, payload);
            }
        }
    }

    #[test]
    fn client_packets_from_upstream_code_unpack_in_all_modes() {
        let key = [4u8; 16];
        let mac_ctx = MacContext::new(&key);
        for mode in 0..=2u8 {
            for payload in [&b"z"[..], &[1u8; 300][..]] {
                let (packet, extra) = client_packet(&mac_ctx, 17, payload, mode);
                if let Some(extra) = extra {
                    assert!(matches!(
                        unpack_client_packet(&mac_ctx, &extra, mode),
                        Some(Inbound::DnsWarmup)
                    ));
                }
                match unpack_client_packet(&mac_ctx, &packet, mode) {
                    Some(Inbound::Packet {
                        cmd,
                        client_id,
                        payload: p,
                    }) => {
                        assert_eq!(cmd, 17);
                        assert_eq!(client_id, 0xC11E_0001);
                        assert_eq!(p, payload);
                    }
                    _ => panic!("mode {mode}: expected packet"),
                }
            }
        }
    }

    #[test]
    fn wrong_mac_key_is_rejected() {
        let mac_ctx = MacContext::new(&[5u8; 16]);
        let (packet, _) = client_packet(&mac_ctx, 2, b"hello", 0);
        assert!(unpack_client_packet(&MacContext::new(&[6u8; 16]), &packet, 0).is_none());
    }
}
