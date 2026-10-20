// SPDX-License-Identifier: AGPL-3.0-only

//! **The rollup fold, and the calendar arithmetic it rests on.**
//!
//! The fold computes these rows in process, so no read-back and no `FINAL` stands between a
//! write and its rollup. What that costs is **date arithmetic that must agree with the
//! engine's**, so it gets the differential.
//!
//! `toStartOfMonth` is the one worth checking. Day and hour flooring are division; a month
//! boundary is a civil-calendar computation, and a leap-year rule off by one day produces rows
//! that key to the wrong month — which the TTL then ages into the wrong monthly bucket, silently,
//! years later.

use analyzer_analysis::classify::Class;
use analyzer_analysis::emitted::{
    day_index, hour_index, month_start, rollup, ClassifiedFact, Grain, Meaning,
};
use analyzer_analysis::intervals::Holder;
use analyzer_analysis::{Discriminator, Params, DAY_MS};

/// Every bucket boundary this fold computes, against the engine's own functions.
///
/// **A century of days and a year of hours**, because the failure this closes is a rule that holds
/// for most dates and not all: century leap years, month lengths, and the day a division-based
/// shortcut would drift. Sampling would find a uniform error and miss exactly the one worth having
/// this test for.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_fold_and_the_engine_agree_on_every_bucket_boundary() {
    let (engine, _db) = harness::fresh_engine();

    // Days 0..36525 covers 1970-01-01 through 2070, including 2000 (a leap year the divisible-by-100
    // rule alone would get wrong) and every February in between.
    let days: Vec<i64> = (0..36_525).collect();
    let wanted: Vec<String> = days
        .iter()
        .map(|d| format!("toInt64(toStartOfMonth(toDate({d})))"))
        .collect();

    // One query per chunk rather than per day: 36K round trips would make this a benchmark.
    let mut engine_months: Vec<i64> = Vec::with_capacity(days.len());
    for chunk in wanted.chunks(2_000) {
        let out = engine
            .run(&format!("SELECT {}", chunk.join(", ")))
            .expect("month query");
        engine_months.extend(
            out.trim()
                .split('\t')
                .map(|v| v.parse::<i64>().expect("month as day count")),
        );
    }
    assert_eq!(engine_months.len(), days.len());

    let mut mismatches = Vec::new();
    for (d, expected) in days.iter().zip(engine_months.iter()) {
        let ours = month_start(*d);
        if ours != *expected {
            mismatches.push(format!("day {d}: fold {ours}, engine {expected}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "month boundary disagrees with the engine on {} day(s): {}",
        mismatches.len(),
        mismatches
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ")
    );

    // The day and hour floors, over a year of hours — the grains the rollup keys on directly.
    let hours: Vec<i64> = (0..8_760).map(|h| h * 3600).collect();
    let day_exprs: Vec<String> = hours
        .iter()
        // **Two conversions have to be explicit or this test lies.** `toDate` on a bare integer
        // reads it as a DAY NUMBER rather than a timestamp. And the engine's default timezone is
        // the HOST's — measured as `America/Detroit` on the machine that first ran this — so a
        // conversion without `'UTC'` compares the fold's UTC arithmetic against local time and
        // reports the difference as a bug in the fold. The production SQL names 'UTC' at every
        // such call for the same reason.
        .map(|s| format!("toInt64(toDate(toDateTime({s}), 'UTC'))"))
        .collect();
    let hour_exprs: Vec<String> = hours
        .iter()
        .map(|s| format!("toUnixTimestamp(toStartOfHour(toDateTime({s}), 'UTC'))"))
        .collect();

    for (exprs, label) in [(day_exprs, "day"), (hour_exprs, "hour")] {
        let mut got: Vec<i64> = Vec::with_capacity(hours.len());
        for chunk in exprs.chunks(2_000) {
            let out = engine
                .run(&format!("SELECT {}", chunk.join(", ")))
                .expect("bucket query");
            got.extend(
                out.trim()
                    .split('\t')
                    .map(|v| v.parse::<i64>().expect("bucket")),
            );
        }
        for (secs, expected) in hours.iter().zip(got.iter()) {
            #[allow(clippy::cast_precision_loss)]
            let at_ms = *secs as f64 * 1000.0;
            let ours = if label == "day" {
                day_index(at_ms)
            } else {
                hour_index(at_ms)
            };
            assert_eq!(ours, *expected, "{label} bucket for unix second {secs}");
        }
    }
}

fn fact(instance: &str, at_ms: f64, class: Class, n_tokens: i64, coverage: f64) -> ClassifiedFact {
    ClassifiedFact {
        holder: Holder {
            instance: instance.to_string(),
            dp_rank: 0,
            group_idx: 0,
        },
        block: "p".to_string(),
        key_epoch: None,
        at_ms,
        n_tokens,
        class,
        gap_ms: None,
        coverage,
        workload_class: "agentic".to_string(),
        model: None,
        meaning: Meaning::current(&Discriminator::of(&Params::default())),
    }
}

/// The grouping itself: facts that share a key collapse, and facts that differ on any dimension
/// do not.
#[test]
fn facts_collapse_on_the_key_and_only_on_the_key() {
    let day = 20_301.0 * DAY_MS;
    let facts = vec![
        fact("i1", day + 1.0, Class::Etr, 10, 1.0),
        // same holder, class, day: collapses with the first
        fact("i1", day + 2.0, Class::Etr, 5, 1.0),
        // a different CLASS is a different row, not a different count
        fact("i1", day + 3.0, Class::Cold, 7, 1.0),
        // a different HOLDER is a physically different cache
        fact("i2", day + 4.0, Class::Etr, 3, 1.0),
    ];

    let rows = rollup(&facts, Grain::Daily);
    assert_eq!(rows.len(), 3, "three distinct keys: {rows:#?}");

    let etr_i1 = rows
        .iter()
        .find(|r| r.class == Class::Etr && r.holder.instance == "i1")
        .expect("the collapsed row");
    assert_eq!(etr_i1.events, 2, "two facts collapsed into one row");
    assert_eq!(etr_i1.tokens, 15, "and their masses added");
}

/// The fold's identity dedup carries `key_epoch` (identity-bearing). Two same-instant facts
/// differing only in epoch are two facts, so they must be two events here — and the same pair
/// under ONE epoch is one identity emitted twice, so it must count one.
#[test]
fn same_instant_facts_differing_only_in_epoch_are_two_facts_and_same_epoch_is_one() {
    let day = 20_301.0 * DAY_MS;
    let mut a = fact("i1", day + 1.0, Class::Etr, 10, 1.0);
    a.key_epoch = Some(1);
    let mut b = a.clone();
    b.key_epoch = Some(2);

    let rows = rollup(&[a.clone(), b], Grain::Daily);
    assert_eq!(rows.len(), 1, "epoch is identity, not a rollup dimension");
    assert_eq!(
        rows[0].events, 2,
        "two epochs are two facts — a dedup blind to key_epoch makes the trend undercount \
         the drill beneath it"
    );

    let twin = a.clone();
    let rows = rollup(&[a, twin], Grain::Daily);
    assert_eq!(rows[0].events, 1, "one identity emitted twice is one fact");
}

/// **A day's facts land in one daily bucket and many hourly ones**, and the two grains must key on
/// the same instants. A grain that disagreed with itself would make the hourly trend and the daily
/// trend describe different days.
#[test]
fn the_two_grains_partition_the_same_facts() {
    let day = 20_301.0 * DAY_MS;
    let facts: Vec<ClassifiedFact> = (0..5)
        .map(|h| {
            #[allow(clippy::cast_precision_loss)]
            let at = day + f64::from(h) * 3_600_000.0 + 1.0;
            fact("i1", at, Class::Cold, 4, 1.0)
        })
        .collect();

    let daily = rollup(&facts, Grain::Daily);
    assert_eq!(daily.len(), 1, "one day");
    assert_eq!(daily[0].events, 5);
    assert_eq!(daily[0].bucket, 20_301, "days since the epoch");
    assert!(
        daily[0].month.is_some(),
        "the daily grain ages into months and carries one"
    );

    let hourly = rollup(&facts, Grain::Hourly);
    assert_eq!(hourly.len(), 5, "five hours");
    assert_eq!(hourly.iter().map(|r| r.events).sum::<u64>(), 5);
    assert!(
        hourly.iter().all(|r| r.month.is_none()),
        "the hourly grain has no monthly ageing and carries no month"
    );
}

/// Coverage does not ride the row.
#[test]
fn coverage_does_not_ride_the_row() {
    let day = 20_301.0 * DAY_MS;
    // Two facts on one key at different coverages fold to ONE row of events and tokens with
    // no coverage figure on it: the day's fraction lives in
    // `coverage_daily` and the reads weight by it — so a cell never changes because the
    // day's coverage moved, which is what lets a tick write only the cells it touched.
    let facts = vec![
        fact("i1", day + 1.0, Class::Etr, 100, 1.0),
        fact("i1", day + 2.0, Class::Etr, 100, 0.5),
    ];
    let rows = rollup(&facts, Grain::Daily);
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].events, rows[0].tokens), (2, 200), "{rows:#?}");
}
