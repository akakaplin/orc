//! Civil date arithmetic, in place of a direct `chrono` dependency.
//!
//! `chrono` is present transitively (both `parquet` and `arrow-array` require
//! it), so this module saves no compile time. What it avoids is a *direct*
//! dependency on a crate whose presence is an implementation detail of something
//! else — if `parquet` ever drops it, nothing here breaks.
//!
//! All timestamps in the engine are `i64` epoch **microseconds**, UTC.
//!
//! The conversions are Howard Hinnant's `civil_from_days` / `days_from_civil`,
//! which are exact for the proleptic Gregorian calendar over the whole `i64`
//! range: <https://howardhinnant.github.io/date_algorithms.html>

use crate::error::{Error, Result};

const US_PER_SEC: i64 = 1_000_000;
const US_PER_HOUR: i64 = 3_600 * US_PER_SEC;
const US_PER_DAY: i64 = 24 * US_PER_HOUR;

/// Days since 1970-01-01 to civil `(year, month, day)`.
///
/// Correct for negative inputs (dates before 1970).
pub fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01 so that leap day lands at the end of the
    // 400-year era, which is what makes the rest of this branch-free.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11], March-based
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (y + i64::from(m <= 2), m as u32, d as u32)
}

/// Civil `(year, month, day)` to days since 1970-01-01. Exact inverse of
/// [`days_to_ymd`].
pub fn ymd_to_days(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400); // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 }; // March-based
    let doy = (153 * i64::from(mp) + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The Hive partition directory name for a timestamp: `hour=YYYY-MM-DDTHH`.
///
/// Width is fixed and zero-padded, because this string becomes a path component
/// that readers glob and sort. Years beyond four digits are not padded further —
/// the timestamp accept window rejects those long before they reach here.
///
/// Uses floor division deliberately: Rust's `/` truncates toward zero, which
/// would put every pre-1970 timestamp in the wrong day.
pub fn hour_partition(ts_us: i64) -> String {
    let days = ts_us.div_euclid(US_PER_DAY);
    let hour = ts_us.rem_euclid(US_PER_DAY) / US_PER_HOUR;
    let (y, m, d) = days_to_ymd(days);
    format!("hour={y:04}-{m:02}-{d:02}T{hour:02}")
}

/// Parse an RFC 3339 timestamp into epoch microseconds.
///
/// Accepts `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` only — **UTC with a literal `Z`**.
/// Numeric offsets like `+02:00` are rejected rather than silently mishandled,
/// since this parses one config field (`limits.ts_min`) where being explicit
/// costs nothing and a misread offset would silently shift the accept window.
pub fn parse_rfc3339_utc(s: &str) -> Result<i64> {
    let bad = |why: &str| Error::Config(format!("invalid timestamp {s:?}: {why}"));

    let rest = s
        .strip_suffix('Z')
        .ok_or_else(|| bad("must end with 'Z'; only UTC is accepted"))?;
    let (date, rest) = rest
        .split_once('T')
        .ok_or_else(|| bad("expected YYYY-MM-DDTHH:MM:SSZ"))?;

    let mut dparts = date.split('-');
    let y: i64 = next_num(&mut dparts, "year", s)?;
    let mo: i64 = next_num(&mut dparts, "month", s)?;
    let d: i64 = next_num(&mut dparts, "day", s)?;
    if dparts.next().is_some() {
        return Err(bad("too many '-' separated fields in the date"));
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return Err(bad("month or day out of range"));
    }

    let (time, frac_us) = match rest.split_once('.') {
        None => (rest, 0),
        Some((t, frac)) => {
            if frac.is_empty() || frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad("fractional seconds must be 1-6 digits"));
            }
            // Right-pad to microseconds: ".5" is 500000us, not 5us.
            let scaled: i64 = frac.parse::<i64>().map_err(|_| bad("bad fraction"))?;
            (t, scaled * 10i64.pow(6 - frac.len() as u32))
        }
    };

    let mut tparts = time.split(':');
    let h: i64 = next_num(&mut tparts, "hour", s)?;
    let mi: i64 = next_num(&mut tparts, "minute", s)?;
    let sec: i64 = next_num(&mut tparts, "second", s)?;
    if tparts.next().is_some() {
        return Err(bad("too many ':' separated fields in the time"));
    }
    // 60 would be a leap second; we have no leap-second table, so reject it
    // rather than silently accept a value we cannot represent faithfully.
    if h > 23 || mi > 59 || sec > 59 {
        return Err(bad("hour, minute or second out of range"));
    }

    let days = ymd_to_days(y, mo as u32, d as u32);
    Ok(days * US_PER_DAY + h * US_PER_HOUR + mi * 60 * US_PER_SEC + sec * US_PER_SEC + frac_us)
}

fn next_num<'a>(parts: &mut impl Iterator<Item = &'a str>, what: &str, whole: &str) -> Result<i64> {
    let raw = parts
        .next()
        .ok_or_else(|| Error::Config(format!("invalid timestamp {whole:?}: missing {what}")))?;
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Config(format!(
            "invalid timestamp {whole:?}: {what} {raw:?} is not a number"
        )));
    }
    raw.parse()
        .map_err(|_| Error::Config(format!("invalid timestamp {whole:?}: {what} out of range")))
}

/// Format epoch microseconds as `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
///
/// Exists for error messages: a rejected timestamp reported as `1970-01-21`
/// tells you instantly that a client sent milliseconds, where the raw integer
/// tells you nothing.
pub fn format_rfc3339_utc(ts_us: i64) -> String {
    let days = ts_us.div_euclid(US_PER_DAY);
    let rem = ts_us.rem_euclid(US_PER_DAY);
    let (y, mo, d) = days_to_ymd(days);
    let h = rem / US_PER_HOUR;
    let mi = (rem % US_PER_HOUR) / (60 * US_PER_SEC);
    let s = (rem % (60 * US_PER_SEC)) / US_PER_SEC;
    let us = rem % US_PER_SEC;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{us:06}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Values below come from Python's `datetime`, not from this implementation.
    // That independence is the point: a self-consistent bug is exactly what
    // these catch. (It earned its keep -- three of these constants were wrong
    // when hand-computed, and the algorithm was right.)
    #[test]
    fn known_dates() {
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        assert_eq!(days_to_ymd(-1), (1969, 12, 31));
        assert_eq!(days_to_ymd(19_782), (2024, 2, 29)); // leap day
        assert_eq!(days_to_ymd(11_016), (2000, 2, 29)); // 400-year leap
        assert_eq!(days_to_ymd(-25_508), (1900, 3, 1)); // century non-leap
        assert_eq!(days_to_ymd(20_673), (2026, 8, 8));
    }

    #[test]
    fn ymd_round_trips_over_four_centuries() {
        // ~1878 to ~2242, spanning every leap-rule case including 1900 and 2100.
        for d in (-33_600..100_000).step_by(7) {
            let (y, m, day) = days_to_ymd(d);
            assert_eq!(ymd_to_days(y, m, day), d, "round trip failed at day {d}");
        }
    }

    #[test]
    fn hour_partition_is_fixed_width() {
        assert_eq!(hour_partition(0), "hour=1970-01-01T00");
        // 2026-08-08T13:00:00Z
        const T13: i64 = 1_786_194_000_000_000;
        assert_eq!(hour_partition(T13), "hour=2026-08-08T13");
        // Last microsecond of an hour must not spill into the next one.
        assert_eq!(hour_partition(T13 + US_PER_HOUR - 1), "hour=2026-08-08T13");
        assert_eq!(hour_partition(T13 + US_PER_HOUR), "hour=2026-08-08T14");
    }

    #[test]
    fn hour_partition_handles_pre_epoch() {
        // Truncating division would wrongly give 1970-01-01T00 for all of these.
        assert_eq!(hour_partition(-1), "hour=1969-12-31T23");
        assert_eq!(hour_partition(-US_PER_DAY), "hour=1969-12-31T00");
        assert_eq!(hour_partition(-US_PER_DAY - 1), "hour=1969-12-30T23");
    }

    #[test]
    fn parses_and_formats() {
        assert_eq!(parse_rfc3339_utc("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            parse_rfc3339_utc("2000-01-01T00:00:00Z").unwrap(),
            946_684_800 * US_PER_SEC
        );
        // Fractional seconds are left-aligned: ".5" is half a second.
        assert_eq!(
            parse_rfc3339_utc("1970-01-01T00:00:00.5Z").unwrap(),
            500_000
        );
        assert_eq!(parse_rfc3339_utc("1970-01-01T00:00:00.000001Z").unwrap(), 1);

        for ts in [0, 1, 946_684_800_000_000, 1_786_310_400_123_456, -1_000_000] {
            let s = format_rfc3339_utc(ts);
            assert_eq!(parse_rfc3339_utc(&s).unwrap(), ts, "round trip of {s}");
        }
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "2026-08-08T00:00:00",          // no Z
            "2026-08-08T00:00:00+02:00",    // offset, not UTC
            "2026-08-08",                   // no time
            "2026-13-01T00:00:00Z",         // month out of range
            "2026-08-32T00:00:00Z",         // day out of range
            "2026-08-08T24:00:00Z",         // hour out of range
            "2026-08-08T00:60:00Z",         // minute out of range
            "2026-08-08T00:00:60Z",         // leap second, unrepresentable
            "2026-08-08T00:00:00.Z",        // empty fraction
            "2026-08-08T00:00:00.1234567Z", // over-precise
            "2026-aa-08T00:00:00Z",         // non-numeric
            "",
        ] {
            assert!(
                parse_rfc3339_utc(bad).is_err(),
                "should have rejected {bad:?}"
            );
        }
    }
}
