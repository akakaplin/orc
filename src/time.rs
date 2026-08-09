//! Civil date arithmetic, in place of a direct `chrono` dependency.
//!
//! `chrono` is in the tree transitively — `parquet` requires it at any feature
//! set — so this saves no compile time. It avoids a *direct* dependency on a
//! crate whose presence is someone else's implementation detail.
//!
//! Timestamps are `u64` epoch **microseconds**, UTC, always at or after
//! 1970-01-01. That is what lets [`hour_partition`] and [`format_rfc3339_utc`]
//! use plain `/` and `%`: truncating division is only wrong for negative inputs.
//!
//! The calendar conversions keep their `i64` signature — they are the general
//! algorithm, exact over the whole proleptic Gregorian range, and `days_to_ymd`
//! is its own inverse's test. Howard Hinnant's `civil_from_days` /
//! `days_from_civil`: <https://howardhinnant.github.io/date_algorithms.html>

use crate::error::{Error, Result};

const US_PER_SEC: u64 = 1_000_000;
/// Also the width of one `hour=` partition, which is why the flush shares it.
pub(crate) const US_PER_HOUR: u64 = 3_600 * US_PER_SEC;
const US_PER_DAY: u64 = 24 * US_PER_HOUR;

/// Years [`parse_rfc3339_utc`] will accept.
///
/// 1970 because the result is unsigned epoch microseconds. The ceiling is well
/// inside what a `u64` of microseconds reaches (~584 000 years), so the
/// arithmetic at the end of `parse` cannot overflow — which makes a typo like
/// `"999999999-01-01T00:00:00Z"` an error rather than a debug panic and a
/// wrapped accept window in release.
const MIN_YEAR: i64 = 1970;
const MAX_YEAR: i64 = 250_000;

/// Days in a month, Gregorian.
fn days_in_month(year: i64, month: u32) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Proleptic Gregorian leap year.
fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

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
/// Fixed width and zero-padded, because this becomes a path component that
/// readers glob and sort. Years past four digits are not padded further — the
/// accept window rejects those long before they reach here.
pub fn hour_partition(ts_us: u64) -> String {
    let days = ts_us / US_PER_DAY;
    let hour = ts_us % US_PER_DAY / US_PER_HOUR;
    let (y, m, d) = days_to_ymd(days as i64);
    format!("hour={y:04}-{m:02}-{d:02}T{hour:02}")
}

/// Parse an RFC 3339 timestamp into epoch microseconds.
///
/// `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` only — **UTC with a literal `Z`**. Offsets
/// like `+02:00` are rejected rather than mishandled: this parses one config
/// field, where being explicit costs nothing and a misread offset would shift
/// the accept window.
pub fn parse_rfc3339_utc(s: &str) -> Result<u64> {
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
    // Bounded before `ymd_to_days`, a polynomial with no opinion about whether
    // its inputs are a real date or whether they overflow.
    if !(MIN_YEAR..=MAX_YEAR).contains(&y) {
        return Err(bad(&format!(
            "year out of range; must be {MIN_YEAR}..={MAX_YEAR} (timestamps are unsigned \
             epoch microseconds, so there is nothing before 1970 to represent)"
        )));
    }
    if !(1..=12).contains(&mo) {
        return Err(bad("month out of range"));
    }
    // Against the month's real length, not a flat 1..=31: `ymd_to_days` rolls
    // 2026-02-31 forward to 2026-03-03, moving the accept window off the day the
    // operator wrote.
    if d < 1 || d > days_in_month(y, mo as u32) {
        return Err(bad(&format!(
            "day {d} is out of range for month {mo} of {y}"
        )));
    }

    let (time, frac_us) = match rest.split_once('.') {
        None => (rest, 0),
        Some((t, frac)) => {
            if frac.is_empty() || frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad("fractional seconds must be 1-6 digits"));
            }
            // Right-pad to microseconds: ".5" is 500000us, not 5us.
            let scaled: u64 = frac.parse::<u64>().map_err(|_| bad("bad fraction"))?;
            (t, scaled * 10u64.pow(6 - frac.len() as u32))
        }
    };

    let mut tparts = time.split(':');
    let h: i64 = next_num(&mut tparts, "hour", s)?;
    let mi: i64 = next_num(&mut tparts, "minute", s)?;
    let sec: i64 = next_num(&mut tparts, "second", s)?;
    if tparts.next().is_some() {
        return Err(bad("too many ':' separated fields in the time"));
    }
    // 60 would be a leap second, and there is no leap-second table here.
    if h > 23 || mi > 59 || sec > 59 {
        return Err(bad("hour, minute or second out of range"));
    }

    // `y >= 1970` makes this non-negative and `y <= MAX_YEAR` keeps the product
    // far inside `u64`, so the cast and the arithmetic are exact.
    let days = ymd_to_days(y, mo as u32, d as u32) as u64;
    let (h, mi, sec) = (h as u64, mi as u64, sec as u64);
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
/// For error messages: a rejected timestamp shown as `1970-01-21` says at once
/// that a client sent milliseconds, where the raw integer says nothing.
pub fn format_rfc3339_utc(ts_us: u64) -> String {
    let days = ts_us / US_PER_DAY;
    let rem = ts_us % US_PER_DAY;
    let (y, mo, d) = days_to_ymd(days as i64);
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
        const T13: u64 = 1_786_194_000_000_000;
        assert_eq!(hour_partition(T13), "hour=2026-08-08T13");
        // Last microsecond of an hour must not spill into the next one.
        assert_eq!(hour_partition(T13 + US_PER_HOUR - 1), "hour=2026-08-08T13");
        assert_eq!(hour_partition(T13 + US_PER_HOUR), "hour=2026-08-08T14");
        // The epoch itself is the floor, and `/` is exact there -- this is the
        // boundary that made floor division necessary when `ts` could be
        // negative, and the type is what removes the question now.
        assert_eq!(hour_partition(US_PER_DAY - 1), "hour=1970-01-01T23");
        assert_eq!(hour_partition(US_PER_DAY), "hour=1970-01-02T00");
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

        for ts in [0, 1, 946_684_800_000_000, 1_786_310_400_123_456] {
            let s = format_rfc3339_utc(ts);
            assert_eq!(parse_rfc3339_utc(&s).unwrap(), ts, "round trip of {s}");
        }
    }

    /// Timestamps are unsigned, so 1970 is the floor and a pre-epoch `ts_min` is
    /// refused rather than silently wrapping to an enormous positive instant.
    #[test]
    fn pre_epoch_timestamps_are_out_of_range() {
        for bad in ["1969-12-31T23:59:59Z", "1900-01-01T00:00:00Z"] {
            let err = parse_rfc3339_utc(bad).unwrap_err().to_string();
            assert!(err.contains("year"), "{bad}: {err}");
        }
        assert!(parse_rfc3339_utc("1970-01-01T00:00:00Z").is_ok());
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

    /// A flat `1..=31` day check let `ymd_to_days` roll an impossible date
    /// forward instead of refusing it, so `ts_min` silently started on a day the
    /// operator never wrote.
    #[test]
    fn impossible_calendar_dates_are_refused_not_rolled_forward() {
        for bad in [
            "2026-02-30T00:00:00Z", // would have become 2026-03-02
            "2026-02-29T00:00:00Z", // 2026 is not a leap year
            "2026-04-31T00:00:00Z", // April has 30
            "2026-06-31T00:00:00Z",
            "2026-09-31T00:00:00Z",
            "2026-11-31T00:00:00Z",
            "2026-01-00T00:00:00Z", // day zero
        ] {
            assert!(
                parse_rfc3339_utc(bad).is_err(),
                "should have rejected {bad:?}, got {:?}",
                parse_rfc3339_utc(bad).map(format_rfc3339_utc)
            );
        }

        // The leap days that *are* real must still parse.
        for good in [
            "2024-02-29T00:00:00Z", // divisible by 4
            "2000-02-29T00:00:00Z", // divisible by 400
            "2026-01-31T00:00:00Z",
            "2026-04-30T00:00:00Z",
        ] {
            assert!(parse_rfc3339_utc(good).is_ok(), "should have parsed {good}");
        }
        // 1900 is not a leap year: divisible by 100, not by 400.
        assert!(parse_rfc3339_utc("1900-02-29T00:00:00Z").is_err());
    }

    /// The year was bounded only by `i64::FromStr`, so a typo overflowed the
    /// microsecond multiply -- a panic in a debug build, a wrapped accept window
    /// in release. Both from one hand-edited config field.
    #[test]
    fn an_out_of_range_year_is_an_error_rather_than_an_overflow() {
        for bad in [
            "999999999-01-01T00:00:00Z",
            "300000-01-01T00:00:00Z",
            "9223372036854775807-01-01T00:00:00Z",
        ] {
            let err = parse_rfc3339_utc(bad).unwrap_err().to_string();
            assert!(err.contains("year"), "{bad}: {err}");
        }

        // And the extremes of the accepted range still compute, which is what
        // makes the bound a real guarantee rather than a smaller cliff.
        for edge in [MIN_YEAR, MAX_YEAR] {
            let s = format!("{edge}-01-01T00:00:00Z");
            let us = parse_rfc3339_utc(&s).unwrap_or_else(|e| panic!("{s}: {e}"));
            assert_eq!(parse_rfc3339_utc(&format_rfc3339_utc(us)).unwrap(), us);
        }
        // And one year past the ceiling is refused, so the bound is the bound.
        assert!(parse_rfc3339_utc(&format!("{}-01-01T00:00:00Z", MAX_YEAR + 1)).is_err());
    }
}
