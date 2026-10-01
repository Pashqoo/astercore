//! Public market feed, server side: `TradesStream` / `TradesResendResponse`,
//! `OrderBook`, live candle pushes and the candle / history API bodies.
//!
//! Mirrors `commands::trades_stream`, `commands::order_book`,
//! `commands::candles` and `commands::market_history` (client parsers).

use std::io::Write;

use flate2::write::{DeflateEncoder, ZlibEncoder};
use flate2::Compression;

pub use crate::commands::candles::DeepHistoryKind;
use crate::compression;

use super::BaseHeader;

/// Unix milliseconds -> Delphi `TDateTime` days (UTC).
pub fn delphi_days(unix_ms: i64) -> f64 {
    unix_ms as f64 / 86_400_000.0 + crate::time::UNIX_EPOCH_AS_DELPHI_DAYS
}

// ----- TradesStream ----------------------------------------------------------

/// One trade row: offset from the packet `base_time`, price, signed quantity
/// (negative = sell).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradeRow {
    pub time_delta_ms: i16,
    pub price: f32,
    pub qty: f32,
}

const TRADES_SECTION_MAX_ROWS: usize = 255;

/// `MPC_TradesStream` payload: `base_time:f64 + packet_num:u16 + sections + flags:u8`.
/// Every section is written as futures trades (type 0) of one market; sections
/// longer than 255 rows are split. Compression is left to the transport.
pub fn trades_packet(base_time: f64, packet_num: u16, sections: &[(u16, &[TradeRow])]) -> Vec<u8> {
    let rows: usize = sections.iter().map(|(_, r)| r.len()).sum();
    let mut out = Vec::with_capacity(11 + sections.len() * 3 + rows * 10);
    out.extend_from_slice(&base_time.to_le_bytes());
    out.extend_from_slice(&packet_num.to_le_bytes());
    for (m_index, rows) in sections {
        for chunk in rows.chunks(TRADES_SECTION_MAX_ROWS) {
            out.extend_from_slice(&(m_index & 0x3FFF).to_le_bytes());
            out.push(chunk.len() as u8);
            for r in chunk {
                out.extend_from_slice(&r.time_delta_ms.to_le_bytes());
                out.extend_from_slice(&r.price.to_le_bytes());
                out.extend_from_slice(&r.qty.to_le_bytes());
            }
        }
    }
    out.push(0); // flags: not compressed, no taker
    out
}

/// `MPC_TradesResendResponse`: `count:u8 + [len:u16 + packet]*`. At most 255
/// packets; each must fit `u16`.
pub fn trades_resend_response(packets: &[&[u8]]) -> Vec<u8> {
    let packets: Vec<&[u8]> = packets
        .iter()
        .copied()
        .filter(|p| p.len() <= u16::MAX as usize)
        .take(255)
        .collect();
    let mut out = Vec::with_capacity(1 + packets.iter().map(|p| p.len() + 2).sum::<usize>());
    out.push(packets.len() as u8);
    for p in packets {
        out.extend_from_slice(&(p.len() as u16).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// `emk_TradesResend` params: `count:u8 + packet_num:u16[count]`.
pub fn parse_trades_resend_params(params: &[u8]) -> Option<Vec<u16>> {
    let count = *params.first()? as usize;
    let body = params.get(1..1 + count * 2)?;
    Some(
        body.as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .collect(),
    )
}

// ----- OrderBook -------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Level {
    pub price: f32,
    pub qty: f32,
}

/// Futures-kind book (`book_kind = 0`); the terminal reads this kind for venues
/// it does not know.
pub const BOOK_KIND_FUTURES: u8 = 0;

/// `MPC_OrderBook` payload (always SynLZ):
/// `market_index:u16 + seq:u16 + flags:u8 + bid_count:u16 + bids + asks`, levels `f32 f32`.
pub fn order_book_packet(
    m_index: u16,
    seq: u16,
    is_full: bool,
    book_kind: u8,
    bids: &[Level],
    asks: &[Level],
) -> Vec<u8> {
    let mut plain = Vec::with_capacity(7 + (bids.len() + asks.len()) * 8);
    plain.extend_from_slice(&m_index.to_le_bytes());
    plain.extend_from_slice(&seq.to_le_bytes());
    plain.push(u8::from(is_full) | ((book_kind & 1) << 1));
    plain.extend_from_slice(&(bids.len() as u16).to_le_bytes());
    for l in bids.iter().chain(asks) {
        plain.extend_from_slice(&l.price.to_le_bytes());
        plain.extend_from_slice(&l.qty.to_le_bytes());
    }
    compression::synlz_compress(&plain)
}

/// `emk_RequestOrderBookFull` params: `market_index:u16 + book_kind:u8`.
pub fn parse_order_book_full_params(params: &[u8]) -> Option<(u16, u8)> {
    Some((
        u16::from_le_bytes([*params.first()?, *params.get(1)?]),
        *params.get(2)?,
    ))
}

// ----- Candles ---------------------------------------------------------------

/// OHLCV bar; `time` is bar open in Delphi days (UTC).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candle {
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub volume: f32,
    pub time: f64,
}

impl Candle {
    /// `TDeepPrice` order: open, close, high, low, volume, time.
    fn write_deep_price(&self, out: &mut Vec<u8>) {
        for v in [self.open, self.close, self.high, self.low, self.volume] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&self.time.to_le_bytes());
    }
}

/// One-byte `DeepHistoryKind` param (`GetCoinCardCandles`, `SubscribeCandles`).
pub fn parse_kind_param(params: &[u8]) -> Option<DeepHistoryKind> {
    DeepHistoryKind::from_byte(*params.first()?)
}

/// `emk_GetCoinCardCandles` body: `count:i32 + TDeepPrice[count]`.
pub fn coin_card_candles(candles: &[Candle]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + candles.len() * 28);
    out.extend_from_slice(&(candles.len() as i32).to_le_bytes());
    for c in candles {
        c.write_deep_price(&mut out);
    }
    out
}

const CMD_CANDLE_UPDATE: u8 = 3;
const CMD_CANDLE_TF_STATE: u8 = 4;

/// Live `TCandleUpdateCommand` (`Command::API`, cmd 3).
pub fn candle_update(uid: u64, m_index: u16, kind: DeepHistoryKind, c: &Candle) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 31);
    BaseHeader::write(&mut out, CMD_CANDLE_UPDATE, uid);
    out.extend_from_slice(&m_index.to_le_bytes());
    out.push(kind.to_byte());
    c.write_deep_price(&mut out);
    out
}

/// `TCandleTFStateCommand` (`Command::API`, cmd 4): the timeframe the core
/// streams for a market; `None` disables live candles.
pub fn candle_tf_state(
    uid: u64,
    m_index: u16,
    kind: Option<DeepHistoryKind>,
    revision: i32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(super::BASE_HEADER_SIZE + 7);
    BaseHeader::write(&mut out, CMD_CANDLE_TF_STATE, uid);
    out.extend_from_slice(&m_index.to_le_bytes());
    out.push(kind.map_or(-1i8, |k| k.to_byte() as i8) as u8);
    out.extend_from_slice(&revision.to_le_bytes());
    out
}

/// Chunk payloads shared by `RequestCandlesData` and `RequestMarketHistory`:
/// `[index:u16][total:u16] + slice`. One chunk per `EngineResponse`.
const CHUNK_BYTES: usize = 60 * 1024;

fn chunked(blob: &[u8]) -> Vec<Vec<u8>> {
    let total = blob.len().div_ceil(CHUNK_BYTES).max(1);
    (0..total)
        .map(|i| {
            let slice = &blob[i * CHUNK_BYTES..blob.len().min((i + 1) * CHUNK_BYTES)];
            let mut out = Vec::with_capacity(4 + slice.len());
            out.extend_from_slice(&(i as u16).to_le_bytes());
            out.extend_from_slice(&(total as u16).to_le_bytes());
            out.extend_from_slice(slice);
            out
        })
        .collect()
}

/// `emk_RequestCandlesData`: retained 5m candles per market, chunked zlib of
/// `legacy_count:i32=0, ver:u8=2, count:i32, server_tz_shift_minutes:f64=0,
/// [name:utf16, count:i32, TDeepPricePack(high,low,volume:f32,time:f64)*, walls 64 B]*`.
/// Times are UTC (shift 0). An empty list still yields one valid chunk.
pub fn candles_snapshot(markets: &[(&str, &[Candle])]) -> Vec<Vec<u8>> {
    let mut plain = Vec::with_capacity(
        17 + markets
            .iter()
            .map(|(n, c)| 70 + n.len() * 2 + c.len() * 20)
            .sum::<usize>(),
    );
    plain.extend_from_slice(&0i32.to_le_bytes());
    plain.push(2);
    plain.extend_from_slice(&(markets.len() as i32).to_le_bytes());
    plain.extend_from_slice(&0f64.to_le_bytes());
    for (name, candles) in markets {
        let utf16: Vec<u16> = name.encode_utf16().collect();
        plain.extend_from_slice(&(utf16.len() as u16).to_le_bytes());
        for ch in utf16 {
            plain.extend_from_slice(&ch.to_le_bytes());
        }
        plain.extend_from_slice(&(candles.len() as i32).to_le_bytes());
        for c in *candles {
            for v in [c.high, c.low, c.volume] {
                plain.extend_from_slice(&v.to_le_bytes());
            }
            plain.extend_from_slice(&c.time.to_le_bytes());
        }
        plain.extend_from_slice(&[0u8; 64]); // buy + sell walls
    }
    let mut enc = ZlibEncoder::new(
        Vec::with_capacity(plain.len() / 2 + 16),
        Compression::default(),
    );
    enc.write_all(&plain).expect("Vec write");
    chunked(&enc.finish().expect("Vec finish"))
}

// ----- Market history --------------------------------------------------------

/// Trade for the chart archive: absolute time in Delphi days, signed quantity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistoryTrade {
    pub time: f64,
    pub price: f32,
    pub qty: f32,
}

/// `emk_RequestMarketHistory`: chunked raw-deflate archive v1 with the trades
/// section filled and mini-candles / last-prices / liquidations empty.
pub fn market_history(trades: &[HistoryTrade]) -> Vec<Vec<u8>> {
    let mut plain = Vec::with_capacity(17 + trades.len() * 16);
    plain.push(1);
    plain.extend_from_slice(&(trades.len() as i32).to_le_bytes());
    for t in trades {
        plain.extend_from_slice(&t.time.to_le_bytes());
        plain.extend_from_slice(&t.price.to_le_bytes());
        plain.extend_from_slice(&t.qty.to_le_bytes());
    }
    plain.extend_from_slice(&[0u8; 12]); // mini candles, last prices, liquidations
    let mut enc = DeflateEncoder::new(
        Vec::with_capacity(plain.len() / 2 + 16),
        Compression::default(),
    );
    enc.write_all(&plain).expect("Vec write");
    chunked(&enc.finish().expect("Vec finish"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::candles::{
        parse_candle_timeframe_state_command, parse_candle_update_command,
        parse_coin_card_candles_response, parse_request_candles_data_response, CandlesAggregator,
    };
    use crate::commands::chunked_response::{ChunkedResponseAggregator, ChunkedResponseResult};
    use crate::commands::engine_request;
    use crate::commands::market_history::parse_market_history_archive;
    use crate::commands::order_book::parse_order_book_packet;
    use crate::commands::trades_stream::{parse_trades_packet, TradeSection};
    use crate::server::codec::engine::EngineRequest;
    use crate::state::iter_trades_resend_response;

    fn rows(n: usize) -> Vec<TradeRow> {
        (0..n)
            .map(|i| TradeRow {
                time_delta_ms: i as i16,
                price: 100.0 + i as f32,
                qty: if i % 2 == 0 { 1.0 } else { -2.0 },
            })
            .collect()
    }

    #[test]
    fn trades_packet_parses_upstream_and_splits_long_sections() {
        let long = rows(300);
        let short = rows(2);
        let raw = trades_packet(45_000.5, 77, &[(3, &long), (5, &short)]);
        let pkt = parse_trades_packet(&raw).expect("parse");
        assert_eq!((pkt.base_time, pkt.packet_num), (45_000.5, 77));
        assert_eq!(pkt.sections.len(), 3);
        let lens: Vec<(u16, usize)> = pkt
            .sections
            .iter()
            .map(|s| match s {
                TradeSection::Trades(t) => (t[0].market_index, t.len()),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(lens, [(3, 255), (3, 45), (5, 2)]);
        let TradeSection::Trades(t) = &pkt.sections[2] else {
            unreachable!()
        };
        assert!(!t[1].is_spot);
        assert_eq!((t[1].price, t[1].qty, t[1].time_delta_ms), (101.0, -2.0, 1));
    }

    #[test]
    fn resend_round_trips_through_upstream_iterator_and_request_parser() {
        let a = trades_packet(1.0, 1, &[]);
        let b = trades_packet(2.0, 2, &[(0, &rows(1))]);
        let resp = trades_resend_response(&[&a, &b]);
        let inner: Vec<&[u8]> = iter_trades_resend_response(&resp).collect();
        assert_eq!(inner, [a.as_slice(), b.as_slice()]);

        let reqs = engine_request::trades_resend_batches(&[5, 6, 9]);
        let req = EngineRequest::parse(&reqs[0]).expect("req");
        assert_eq!(parse_trades_resend_params(&req.params), Some(vec![5, 6, 9]));
    }

    #[test]
    fn order_book_parses_upstream() {
        let bids = [
            Level {
                price: 10.0,
                qty: 1.0,
            },
            Level {
                price: 9.0,
                qty: 2.0,
            },
        ];
        let asks = [Level {
            price: 11.0,
            qty: 3.0,
        }];
        let raw = order_book_packet(4, 9, true, BOOK_KIND_FUTURES, &bids, &asks);
        let pkt = parse_order_book_packet(&raw).expect("parse");
        assert_eq!(
            (pkt.market_index, pkt.seq, pkt.is_full, pkt.book_kind),
            (4, 9, true, 0)
        );
        assert_eq!(pkt.buys.len(), 2);
        assert_eq!((pkt.sells[0].rate, pkt.sells[0].quantity), (11.0, 3.0));

        let req = EngineRequest::parse(&engine_request::request_order_book_full(7, 1)).unwrap();
        assert_eq!(parse_order_book_full_params(&req.params), Some((7, 1)));
    }

    fn candle(i: usize) -> Candle {
        Candle {
            open: 1.0 + i as f32,
            high: 2.0 + i as f32,
            low: 0.5,
            close: 1.5,
            volume: 10.0,
            time: 45_000.0 + i as f64 / 288.0,
        }
    }

    #[test]
    fn coin_card_and_live_candles_parse_upstream() {
        let cs = [candle(0), candle(1)];
        let parsed = parse_coin_card_candles_response(&coin_card_candles(&cs)).expect("parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            (parsed[1].open(), parsed[1].high(), parsed[1].volume()),
            (2.0, 3.0, 10.0)
        );
        assert_eq!(parsed[1].time, cs[1].time);

        let upd = parse_candle_update_command(&candle_update(9, 3, DeepHistoryKind::Min5, &cs[0]))
            .expect("update");
        assert_eq!(
            (upd.uid, upd.market_index, upd.kind),
            (9, 3, DeepHistoryKind::Min5)
        );
        assert_eq!(upd.candle.close(), 1.5);

        let st = parse_candle_timeframe_state_command(&candle_tf_state(1, 3, None, 2)).unwrap();
        assert_eq!((st.market_index, st.timeframe, st.revision), (3, -1, 2));
        let st = parse_candle_timeframe_state_command(&candle_tf_state(
            1,
            3,
            Some(DeepHistoryKind::Day1),
            3,
        ))
        .unwrap();
        assert_eq!(st.timeframe, 5);

        let req = EngineRequest::parse(&crate::commands::candles::get_coin_card_candles(
            "SBER",
            DeepHistoryKind::Hour4,
        ))
        .unwrap();
        assert_eq!(parse_kind_param(&req.params), Some(DeepHistoryKind::Hour4));
    }

    #[test]
    fn candles_snapshot_chunks_and_parses_upstream() {
        let mut x = 0x9E37_79B9u32;
        let many: Vec<Candle> = (0..20_000)
            .map(|i| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                Candle {
                    volume: x as f32,
                    ..candle(i)
                }
            })
            .collect();
        let chunks = candles_snapshot(&[("SBER", &many), ("GAZP", &[])]);
        assert!(
            chunks.len() > 1,
            "expected several chunks, got {}",
            chunks.len()
        );
        let mut agg = CandlesAggregator::new();
        let mut merged = None;
        for c in &chunks {
            merged = agg.on_chunk(c);
        }
        let markets =
            parse_request_candles_data_response(&merged.expect("complete")).expect("parse");
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].market_name, "SBER");
        assert_eq!(markets[0].candles_5m.len(), 20_000);
        assert_eq!(markets[0].candles_5m[1].high(), 3.0);
        assert!(markets[1].candles_5m.is_empty());

        let empty = candles_snapshot(&[]);
        assert_eq!(empty.len(), 1);
        let merged = CandlesAggregator::new()
            .on_chunk(&empty[0])
            .expect("single chunk");
        assert!(parse_request_candles_data_response(&merged)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn market_history_parses_upstream() {
        let trades = [HistoryTrade {
            time: 45_000.25,
            price: 12.5,
            qty: -3.0,
        }];
        let chunks = market_history(&trades);
        let mut agg = ChunkedResponseAggregator::new("test", 1 << 20);
        let ChunkedResponseResult::Complete(blob) = agg.on_chunk(&chunks[0]) else {
            panic!("single chunk expected")
        };
        let archive = parse_market_history_archive(&blob).expect("archive");
        assert_eq!(archive.futures_trades.len(), 1);
        assert_eq!(
            (
                archive.futures_trades[0].price,
                archive.futures_trades[0].qty
            ),
            (12.5, -3.0)
        );
        assert!(archive.mini_candles.is_empty() && archive.liquidations.is_empty());
    }
}
