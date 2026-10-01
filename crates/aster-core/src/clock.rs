//! The core's two clocks in text and days. Every timestamp of the venue and
//! the wire is unix milliseconds, UTC. The trader's is Moscow (UTC+3): MoonBot
//! reads its work windows, the daily summary, the day files of the journal and
//! the times printed on a deal's picture on the trader's local clock, as
//! TInvestCore did. RFC 3339 conversions ported from TInvestCore
//! (`tinvest/time.rs`), the trader's day from its `tinvest/moex.rs`.

/// The trader's clock against UTC.
pub const TRADER_OFFSET_MS: i64 = 3 * 3_600_000;

/// Start of the trader's calendar day containing `unix_ms`.
pub fn trader_midnight(unix_ms: i64) -> i64 {
    const DAY_MS: i64 = 86_400_000;
    (unix_ms + TRADER_OFFSET_MS).div_euclid(DAY_MS) * DAY_MS - TRADER_OFFSET_MS
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.frac]Z` (fraction of any length, truncated to ms).
pub fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mm, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let mut pos = 19;
    let mut millis = 0i64;
    if b.get(pos) == Some(&b'.') {
        pos += 1;
        let start = pos;
        while b.get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        let frac = &s[start..pos];
        let head = &frac[..frac.len().min(3)];
        millis = head.parse::<i64>().ok()? * 10i64.pow(3 - head.len() as u32);
    }
    if &s[pos..] != "Z" {
        return None;
    }
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    Some(((days_from_civil(y, m, d) * 86_400 + hh * 3600 + mm * 60 + ss) * 1000) + millis)
}

/// `YYYY-MM-DDTHH:MM:SSZ` for a Unix-millisecond instant (sub-second part dropped).
pub fn format_rfc3339(unix_ms: i64) -> String {
    let secs = unix_ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

// Howard Hinnant's algorithms (proleptic Gregorian, days since 1970-01-01).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_trader_day_starts_at_moscow_midnight() {
        // 2024-05-01 00:30 MSK is 2024-04-30 21:30 UTC.
        let t = parse_rfc3339_ms("2024-04-30T21:30:00Z").unwrap();
        assert_eq!(format_rfc3339(trader_midnight(t)), "2024-04-30T21:00:00Z");
        assert_eq!(trader_midnight(trader_midnight(t)), trader_midnight(t));
    }

    #[test]
    fn round_trips_and_truncates_fraction() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_ms("2024-05-01T10:15:00.123456Z"),
            Some(1_714_558_500_123)
        );
        assert_eq!(
            parse_rfc3339_ms("2024-05-01T10:15:00.5Z"),
            Some(1_714_558_500_500)
        );
        assert_eq!(format_rfc3339(1_714_558_500_123), "2024-05-01T10:15:00Z");
        assert_eq!(parse_rfc3339_ms("2024-05-01 10:15:00Z"), None);
        assert_eq!(parse_rfc3339_ms("2024-05-01T10:15:00+03:00"), None);
        for ms in [-86_400_000i64, 951_782_400_000, 4_102_444_800_000] {
            assert_eq!(parse_rfc3339_ms(&format_rfc3339(ms)), Some(ms));
        }
    }
}
