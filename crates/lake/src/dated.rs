// SPDX-License-Identifier: AGPL-3.0-only

//! The dated key layout's arithmetic (`conformance/delivery/layout.json`): a shipper lands
//! sealed segments under `<prefix><yyyy-mm-dd>/<hh>/...`, and this module is the one place that
//! convention is parsed and rendered. Two transports read it — the ingest LIST bounds itself by
//! a ledger watermark's hour floor, and the segment-reading event source enumerates a window's
//! hour prefixes — and two implementations of one layout is how a floor and a listing come to
//! disagree about which objects exist.
//!
//! It sits in the lake crate because the lake is the bucket's layout: the analyzer's own root
//! (`_analyzer/`) beside the shipper's dated tree, and every transport that reads either
//! already stands on it. The partner closure (`segments` plus `analysis`) does not carry it.
//!
//! Civil-date arithmetic written out rather than bought, the allowlist's arithmetic rule:
//! a wrong date computation is loudly testable. Howard Hinnant's
//! algorithms, days relative to 1970-01-01.

use std::fmt::Write as _;

/// Margin behind any bucket listing's lower bound, in hours. The margin is a CONTRACT with the
/// shipper — its upload deadline plus skew allowance — and this value is a placeholder until
/// that derivation lands. Objects arriving later
/// than this are declared loss via the seq hole, recoverable only by a deliberate deep replay
/// (floor reset), never silently. Both listings hold the same margin: ingest re-lists it behind
/// the watermark, and the event source widens a window's hour range by it on each side, because
/// a segment's key hour is the SHIP hour while its records carry the producer clock.
pub const LIST_MARGIN_HOURS: i64 = 24;

/// The ONE shipped suffix (contract 2.1, since 1.9): producers ship Parquet and nothing else.
/// Both transports — the ingest LIST and the segment-reading event source — apply the one
/// candidate rule derived from this constant, [`is_shipped_key`], so the two cannot drift;
/// `shipped_keys_are_the_suffix_outside_the_analyzer_root` below pins the rule.
pub const SHIPPED_SUFFIX: &str = ".parquet";

/// **The analyzer-owned root under the fleet prefix**:
/// everything the analyzer writes into the customer's bucket — the lineages, the
/// annotation objects, the preflight probe — lives under this one segment. A DNS hostname
/// cannot begin with an underscore and a date cannot either, so nothing the tap ships can
/// spell it: the tap's data and the analyzer's derivations are told apart by the first path
/// segment, forever, and [`is_shipped_key`] refuses anything beneath it so the analyzer can
/// never ingest its own output as a segment.
pub const ANALYZER_ROOT: &str = "_analyzer/";
/// The preflight probe's home under the root.
pub const PREFLIGHT_PREFIX: &str = "_analyzer/preflight/";

/// Whether a key is under the analyzer-owned root, at the prefix or anywhere below it (the
/// fleet prefix is the caller's and is not re-parsed here: a segment named `_analyzer`
/// anywhere is the analyzer's, since nothing the tap ships can produce one).
#[must_use]
pub fn is_analyzer_owned(key: &str) -> bool {
    key.starts_with(ANALYZER_ROOT) || key.contains("/_analyzer/")
}

/// Whether a listed key is a shipped-form candidate — the per-key predicate both transports
/// apply to a listing they already hold: the shipped suffix, and NOT under the analyzer-owned
/// root.
#[must_use]
pub fn is_shipped_key(key: &str) -> bool {
    key.ends_with(SHIPPED_SUFFIX) && !is_analyzer_owned(key)
}

const HOUR_MS: f64 = 3_600_000.0;

/// The listing floor: the watermark key's `<yyyy-mm-dd>/<hh>` segment pair, minus the
/// margin, re-rendered under the same prefix. `None` when the watermark is empty or
/// carries no dated segment (early life, or an operator archive drop as the newest
/// commit) — the caller then has no floor.
#[must_use]
pub fn hour_floor(watermark: &str, margin_hours: i64) -> Option<String> {
    let segments: Vec<&str> = watermark.split('/').collect();
    for i in 0..segments.len().saturating_sub(1) {
        let (Some(date), Some(hour)) = (parse_date(segments[i]), parse_hour(segments[i + 1]))
        else {
            continue;
        };
        let (y, m, d) = date;
        let total = days_from_civil(y, m, d) * 24 + hour - margin_hours;
        let mut floor = segments[..i].join("/");
        if !floor.is_empty() {
            floor.push('/');
        }
        floor.push_str(&render_hour(total));
        return Some(floor);
    }
    None
}

/// The `<yyyy-mm-dd>/<hh>` prefixes whose hours intersect `[lo_ms, hi_ms]`, in ascending order —
/// what a windowed LIST enumerates instead of sweeping a whole prefix. Empty when the range is.
#[must_use]
pub fn hour_prefixes(lo_ms: f64, hi_ms: f64) -> Vec<String> {
    let (Some(lo), Some(hi)) = (hour_of_ms(lo_ms), hour_of_ms(hi_ms)) else {
        return Vec::new();
    };
    (lo..=hi).map(render_hour).collect()
}

/// The key a shipper writes one segment to: `<prefix><date>/<hh>/<node>/<name>`, the hour
/// taken from `at_ms`. A rig plants segments under the layout the analyzer discovers by
/// calling this rather than spelling the layout a second time; `None` when the stamp is not
/// finite.
#[must_use]
pub fn segment_key(prefix: &str, at_ms: f64, node: &str, name: &str) -> Option<String> {
    let hour = render_hour(hour_of_ms(at_ms)?);
    Some(format!("{prefix}{hour}/{node}/{name}"))
}

/// The hour since the epoch named by a key's first dated `<yyyy-mm-dd>/<hh>` segment pair —
/// the SHIP hour, which is what an age-based rule over shipped objects reads. `None` for a key
/// with no dated segment (an operator archive drop), whose age is unknowable from its name.
#[must_use]
pub fn key_hour(key: &str) -> Option<i64> {
    let segments: Vec<&str> = key.split('/').collect();
    for i in 0..segments.len().saturating_sub(1) {
        if let (Some((y, m, d)), Some(hour)) =
            (parse_date(segments[i]), parse_hour(segments[i + 1]))
        {
            return Some(days_from_civil(y, m, d) * 24 + hour);
        }
    }
    None
}

/// The calendar day named by a key's dated segment, as days since the epoch — what a retention
/// gate needs before a byte is fetched. `None` on the same terms as [`key_hour`].
#[must_use]
pub fn key_day(key: &str) -> Option<i64> {
    key.split('/').find_map(|seg| {
        let (y, m, d) = parse_date(seg)?;
        Some(days_from_civil(y, m, d))
    })
}

/// An epoch-hour index from a millisecond stamp. `None` where the stamp is not finite — a
/// non-value must refuse to name an hour rather than name hour zero.
fn hour_of_ms(ms: f64) -> Option<i64> {
    if !ms.is_finite() {
        return None;
    }
    let hours = (ms / HOUR_MS).floor();
    // In range by the guard above and the f64 domain; the cast is the conversion itself.
    #[allow(clippy::cast_possible_truncation)]
    Some(hours as i64)
}

fn render_hour(total: i64) -> String {
    let (y, m, d) = civil_from_days(total.div_euclid(24));
    let h = total.rem_euclid(24);
    let mut out = String::new();
    let _ = write!(out, "{y:04}-{m:02}-{d:02}/{h:02}");
    out
}

fn parse_date(seg: &str) -> Option<(i64, i64, i64)> {
    let b = seg.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let num = |s: &str| s.parse::<i64>().ok();
    let (y, m, d) = (num(&seg[0..4])?, num(&seg[5..7])?, num(&seg[8..10])?);
    ((1..=12).contains(&m) && (1..=31).contains(&d)).then_some((y, m, d))
}

fn parse_hour(seg: &str) -> Option<i64> {
    if seg.len() != 2 {
        return None;
    }
    let h = seg.parse::<i64>().ok()?;
    (0..=23).contains(&h).then_some(h)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The `yyyy-mm-dd` spelling of an epoch-day index — the tap's own, rendered here so the
/// derived tier and the listing floor spell a day one way.
#[must_use]
pub fn date_of_day(day: i64) -> String {
    let (y, m, d) = civil_from_days(day);
    format!("{y:04}-{m:02}-{d:02}")
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_floor_subtracts_the_margin_inside_a_day() {
        let floor = hour_floor("fleet/2026-08-06/23/node-1/seg-a-9.jsonl", 5);
        assert_eq!(floor.as_deref(), Some("fleet/2026-08-06/18"));
    }

    #[test]
    fn hour_floor_crosses_days_months_and_years() {
        let floor = hour_floor("tap/2026-01-01/00/n/seg-a-1.jsonl", 1);
        assert_eq!(floor.as_deref(), Some("tap/2025-12-31/23"));
        let floor = hour_floor("tap/2026-03-01/00/n/seg-a-1.jsonl", 24);
        // 2026 is not a leap year: the day before March 1 is February 28.
        assert_eq!(floor.as_deref(), Some("tap/2026-02-28/00"));
    }

    #[test]
    fn hour_floor_refuses_undated_keys() {
        assert_eq!(hour_floor("", 24), None);
        assert_eq!(hour_floor("tap/archive/seg-a-1.jsonl", 24), None);
        // a 2-digit segment that is not an hour, after a segment that is not a date
        assert_eq!(hour_floor("tap/25/99/x.jsonl", 24), None);
    }

    #[test]
    #[allow(clippy::cast_precision_loss)] // epoch-hour indices are far below 2^53
    fn hour_prefixes_cover_the_range_inclusively() {
        // 2026-08-06 23:30 → 2026-08-07 01:10 spans three hours, across a day boundary.
        let lo = ((days_from_civil(2026, 8, 6) * 24) as f64 + 23.5) * HOUR_MS;
        let hi = ((days_from_civil(2026, 8, 7) * 24) as f64 + 1.2) * HOUR_MS;
        assert_eq!(
            hour_prefixes(lo, hi),
            vec!["2026-08-06/23", "2026-08-07/00", "2026-08-07/01"]
        );
        assert!(
            hour_prefixes(hi, lo).is_empty(),
            "an inverted range is empty"
        );
        assert!(
            hour_prefixes(f64::NAN, hi).is_empty(),
            "a non-value names no hour"
        );
    }

    #[test]
    fn key_hour_reads_the_first_dated_pair_and_refuses_undated_keys() {
        assert_eq!(
            key_hour("fleet/2026-08-06/23/node-1/seg-a-9.jsonl"),
            Some(days_from_civil(2026, 8, 6) * 24 + 23)
        );
        assert_eq!(key_hour("tap/archive/seg-a-1.jsonl"), None);
    }

    #[test]
    fn key_day_reads_the_dated_segment() {
        assert_eq!(
            key_day("fleet/2026-08-06/23/node-1/seg-a-9.jsonl"),
            Some(days_from_civil(2026, 8, 6))
        );
        assert_eq!(key_day("no/dates/here.jsonl"), None);
    }

    /// The one candidate rule, pinned: the shipped suffix outside the analyzer-owned root, so
    /// a caller cannot restate it locally.
    #[test]
    fn shipped_keys_are_the_suffix_outside_the_analyzer_root() {
        for (key, candidate) in [
            ("tap/2026-08-14/03/n1/seg-a-0.parquet", true),
            // undated operator archive drops are candidates on both transports
            ("tap/archive/seg-a-0.parquet", true),
            ("tap/2026-08-14/03/n1/seg-a-0.jsonl", false),
            ("tap/2026-08-14/03/n1/seg-a-0.parquet.tmp", false),
            ("tap/notes.txt", false),
        ] {
            assert_eq!(is_shipped_key(key), candidate, "{key}");
        }
    }

    #[test]
    fn civil_round_trips_across_the_gregorian_trap() {
        // 2100 is NOT a leap year — the divide-by-100 rule the naive version misses.
        for days in [0, 1, 59, 11_016, 47_540, 47_541, 47_892] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(
            days_from_civil(2100, 3, 1) - days_from_civil(2100, 2, 28),
            1
        );
    }
}

#[cfg(test)]
mod derived_layout_tests {
    use super::*;

    /// **The analyzer never ingests its own output.** An object it writes may end in the
    /// shipped suffix, and only the root keeps it out of the candidate set.
    #[test]
    fn analyzer_keys_are_never_shipped_candidates() {
        let key = "tap/_analyzer/namespaces/n/lineages/l/state/t/2026-09-03/07.parquet";
        assert!(key.ends_with(SHIPPED_SUFFIX));
        assert!(is_analyzer_owned(key));
        assert!(
            !is_shipped_key(key),
            "an analyzer object read as a segment is a feedback loop"
        );
        assert!(is_shipped_key("tap/2026-09-03/07/n1/seg-a-0.parquet"));
        // a hostname or an archive drop that merely CONTAINS the word is the tap's
        assert!(is_shipped_key(
            "tap/analyzer-box/2026-09-03/07/seg-a-0.parquet"
        ));
        assert!(is_shipped_key("tap/my_analyzer/seg-a-0.parquet"));
    }

    /// The root cannot be spelled by anything the tap ships: not a date, not a hostname.
    #[test]
    fn the_root_is_neither_a_date_nor_a_hostname() {
        let first = ANALYZER_ROOT.trim_end_matches('/');
        assert!(first.starts_with('_'));
        assert!(parse_date(first).is_none());
        // RFC 952/1123: a hostname label begins with a letter or digit
        assert!(!first.chars().next().unwrap().is_ascii_alphanumeric());
        assert!(PREFLIGHT_PREFIX.starts_with(ANALYZER_ROOT));
    }
}
