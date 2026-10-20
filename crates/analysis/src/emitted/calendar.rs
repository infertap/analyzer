// SPDX-License-Identifier: AGPL-3.0-only

//! The proleptic Gregorian calendar over day indexes, and the bucket indexes a stamp falls in.

/// Civil date from a day count. Hinnant's algorithm, exact for the whole `i64` range.
///
/// **Written out rather than pulled in**, matching the nine lines of calendar arithmetic the read
/// layer already owns for the same reason: a wrong date computation is loudly testable, and this
/// one is tested against the engine's own month boundary over a century of days.
#[must_use]
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    // `z - era * 146_097` is the day-of-era and is non-negative by construction: `era` floors
    // toward negative infinity, so the remainder lands in [0, 146096]. The sign cannot be lost
    // because there is none.
    #[allow(clippy::cast_sign_loss)]
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    #[allow(clippy::cast_possible_wrap)]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    #[allow(clippy::cast_possible_truncation)]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (y + i64::from(m <= 2), m, d)
}

/// Day count from a civil date. The inverse of [`civil_from_days`].
#[must_use]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    #[allow(clippy::cast_sign_loss)]
    let yoe = (y - era * 400) as u64;
    let mp = u64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    #[allow(clippy::cast_possible_wrap)]
    let out = era * 146_097 + doe as i64 - 719_468;
    out
}

/// The day a store event is attributed to: days since the epoch, UTC.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // day indices are small integers
pub fn day_index(at_ms: f64) -> i64 {
    (at_ms / crate::DAY_MS).floor() as i64
}

/// The first of a day's month, days since the epoch.
#[must_use]
pub fn month_start(day: i64) -> i64 {
    let (y, m, _) = civil_from_days(day);
    days_from_civil(y, m, 1)
}

/// The hour a store event is attributed to: unix SECONDS, floored, UTC.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn hour_index(at_ms: f64) -> i64 {
    (at_ms / 3_600_000.0).floor() as i64 * 3600
}

/// Unix SECONDS of the UTC minute containing `at_ms` — [`Grain::Minute`](super::Grain::Minute)'s bucket, in the
/// representation a `DateTime` column holds, same convention as [`hour_index`].
#[must_use]
#[allow(clippy::cast_possible_truncation)] // minute indices sit far inside i64
pub fn minute_index(at_ms: f64) -> i64 {
    (at_ms / 60_000.0).floor() as i64 * 60
}
