// SPDX-License-Identifier: AGPL-3.0-only

//! The contract types' tests.

use super::{CoverageFact, EmittedDay, Meaning, COVERAGE_DAILY};
use crate::classify::{Class, Classification, ClassifiedStore, ObservedResidency};
use crate::completeness::{Coverage, DailyCoverage};
use crate::events::IntervalKey;
use crate::intervals::Holder;
use crate::{Discriminator, Params, DAY_MS};

#[test]
fn a_coverage_fact_is_a_row_of_its_table_keyed_by_day_index_and_instance() {
    let fact = CoverageFact {
        day: 20_301,
        source: "i1".to_string(),
        covered_ms: 1.0,
        window_ms: 2.0,
        meaning: Meaning::current(&Discriminator::of(&Params::default())),
    };
    let row = fact.row();
    COVERAGE_DAILY.accepts(&row).expect("the declared shape");
    assert_eq!(
        row.key,
        vec![
            crate::module::Value::Date(20_301),
            crate::module::Value::Str("i1".to_string())
        ],
        "days since the epoch, never a rendered date"
    );
    assert!(!row.gone);
}

fn classification() -> Classification {
    Classification {
        stores: vec![ClassifiedStore {
            incarnation: "run-a".into(),
            workload_class: Some("agentic".to_string()),
            instance: "i1".to_string(),
            dp_rank: 0,
            group_idx: 0,
            block: "p".to_string(),
            key_epoch: None,
            at_ms: 5.0,
            n_tokens: 8,
            class: Class::Etr,
            gap_ms: Some(90.0),
        }],
        eviction_waste_tokens: 8,
        post_clear_tokens: 0,
        bucket_counts: std::collections::BTreeMap::new(),
        coverage: Coverage {
            fraction: 0.5,
            per_source_covered_ms: Vec::new(),
            uncertain: Vec::new(),
            per_day: vec![DailyCoverage {
                day_ms: 3.0 * DAY_MS,
                source: "i1".to_string(),
                covered_ms: 10.0,
                window_ms: DAY_MS,
            }],
        },
        residencies: vec![ObservedResidency {
            workload_class: Some("agentic".to_string()),
            key: IntervalKey {
                instance: "i1".to_string(),
                dp_rank: 0,
                group_idx: 0,
                identity: "b".to_string(),
                key_epoch: None,
                tier: None,
                backend_id: None,
            },
            open_ms: 5.0,
            close_ms: 20.0,
            duration_ms: 15.0,
            opener_class: Class::Cold,
        }],
    }
}

#[test]
fn every_row_carries_the_meaning_pair_and_there_is_no_way_to_omit_it() {
    // The defect this closes: a row without the pair collides with the previous generation's
    // row on the same key and OVERWRITES it. `Meaning::current` is the only constructor, so
    // the omission is unreachable rather than merely discouraged.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day =
        EmittedDay::from_classification(&classification(), &m, &std::collections::BTreeMap::new());
    assert_eq!(day.facts[0].meaning, m);
    assert_eq!(day.coverage[0].meaning, m);
    assert_eq!(m.semantics_version, crate::SEMANTICS_VERSION);
}

#[test]
fn a_declared_dimension_gets_a_bucket_and_an_observed_one_gets_a_null() {
    // Two treatments, and the difference is not cosmetic. A declared dimension nothing claimed
    // is a bucket a reader groups by; an observed one nobody saw is absent, and defaulting it
    // would answer a question the stream never did.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day =
        EmittedDay::from_classification(&classification(), &m, &std::collections::BTreeMap::new());
    assert_eq!(
        day.facts[0].workload_class, "agentic",
        "the class the producer declared on the segment header, carried by the row"
    );
    assert_eq!(
        day.facts[0].model, None,
        "observed, and unobserved is not a value"
    );
}

#[test]
fn a_partial_day_is_refusable_rather_than_silently_replacing_a_whole_one() {
    // `coverage_daily` replaces on (fleet, day, instance, meaning). A half-day row lands on
    // the same key as the full day it belongs to and shrinks both the covered figure and its
    // denominator — silently, since the row still looks well-formed.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let whole = super::CoverageFact {
        day: 20_301,
        source: "i1".to_string(),
        covered_ms: DAY_MS - 5000.0,
        window_ms: DAY_MS,
        meaning: m.clone(),
    };
    assert!(whole.is_whole_day());
    let partial = super::CoverageFact {
        window_ms: 3_600_000.0,
        ..whole
    };
    assert!(!partial.is_whole_day(), "an hour is not a day");
}

#[test]
fn every_band_is_in_all_and_spells_itself_once() {
    // Same agreement `every_class_is_in_all_and_spells_itself_once` pins for `Class`:
    // `as_str`'s match is exhaustive, `ALL` is a list that would silently stay short.
    let names: std::collections::BTreeSet<&str> =
        super::Band::ALL.iter().map(super::Band::as_str).collect();
    assert_eq!(
        names.len(),
        super::Band::ALL.len(),
        "two bands share a spelling: {names:?}"
    );
    assert_eq!(
        names,
        ["<10s", "10s-1m", "1-10m", "10-60m", "none"]
            .into_iter()
            .collect()
    );
}

#[test]
fn band_edges_are_half_open_and_total() {
    use super::Band;
    let min = 60_000.0;
    // Each edge belongs to the band ABOVE it — half-open [lo, hi), the workspace's one
    // window convention.
    for (gap, want) in [
        (Some(-5.0), Band::Under10Sec), // clock skew lands short, never invents a gap
        (Some(0.0), Band::Under10Sec),
        (Some(9_999.0), Band::Under10Sec),
        (Some(10_000.0), Band::Sec10ToMin1),
        (Some(min - 1.0), Band::Sec10ToMin1),
        (Some(min), Band::Min1To10),
        (Some(10.0 * min - 1.0), Band::Min1To10),
        (Some(10.0 * min), Band::Min10To60),
        (Some(60.0 * min - 1.0), Band::Min10To60),
        (None, Band::Unobserved),
        (Some(f64::NAN), Band::Unobserved),
    ] {
        assert_eq!(Band::of(gap), want, "gap {gap:?}");
    }
}

#[test]
fn banding_splits_the_rollup_without_moving_any_sum() {
    // The band is a partition of gap_ms, so collapsing the band rollup over its band must
    // reproduce the class rollup's daily rows exactly — same keys, same four measures. If
    // this fails, the thrash lens and the meter disagree about one quantity.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let mut c = classification();
    let proto = c.stores[0].clone();
    for (i, gap) in [
        None,
        Some(30_000.0),
        Some(30_001.0),
        Some(700_000.0),
        Some(2.0 * crate::DAY_MS),
    ]
    .into_iter()
    .enumerate()
    {
        let mut s = proto.clone();
        s.block = format!("b{i}");
        s.gap_ms = gap;
        c.stores.push(s);
    }
    let day = EmittedDay::from_classification(&c, &m, &std::collections::BTreeMap::new());

    let banded = super::band_rollup(&day.facts, super::Grain::Daily);
    assert!(
        banded.len() > 1,
        "the fixture must actually spread across bands"
    );
    let mut collapsed: std::collections::BTreeMap<_, (u64, u64)> =
        std::collections::BTreeMap::new();
    for r in &banded {
        let slot = collapsed
            .entry((
                r.bucket,
                r.class.as_str(),
                r.holder.instance.clone(),
                r.workload_class.clone(),
            ))
            .or_insert((0, 0));
        slot.0 += r.events;
        slot.1 += r.tokens;
    }
    let daily = super::rollup(&day.facts, super::Grain::Daily);
    assert_eq!(collapsed.len(), daily.len());
    for r in &daily {
        let got = &collapsed[&(
            r.bucket,
            r.class.as_str(),
            r.holder.instance.clone(),
            r.workload_class.clone(),
        )];
        assert_eq!((got.0, got.1), (r.events, r.tokens));
    }
}

/// A classification that spreads across two instances, two ranks, two cache groups, two
/// workloads, four classes, six minutes in three hours and five bands — so a collapse that
/// summed the wrong dimension away, or kept one it should not, cannot pass by accident.
fn spread_classification() -> Classification {
    let mut c = classification();
    let proto = c.stores[0].clone();
    c.stores.clear();
    let gaps = [
        None,
        Some(3_000.0),
        Some(30_000.0),
        Some(300_000.0),
        Some(3_000_000.0),
    ];
    let classes = [Class::Etr, Class::Cold, Class::PostClear, Class::ReAdmit];
    let mut n = 0u32;
    for instance in ["i1", "i2"] {
        for dp_rank in 0..2u32 {
            for group_idx in 0..2u32 {
                for k in 0..6u32 {
                    let mut s = proto.clone();
                    s.instance = instance.to_string();
                    s.dp_rank = dp_rank;
                    s.group_idx = group_idx;
                    s.block = format!("b{n}");
                    // Six distinct minutes across three hours, the rank one second later
                    // so same-minute facts on two ranks land in one fleet cell.
                    s.at_ms = 3.0 * DAY_MS
                        + f64::from(k) * 1_800_000.0
                        + f64::from(dp_rank) * 1_000.0
                        + f64::from(group_idx) * 10.0;
                    s.n_tokens = i64::from(n % 7) + 1;
                    s.class = classes[(n % 4) as usize];
                    s.gap_ms = gaps[(n % 5) as usize];
                    s.workload_class = if n.is_multiple_of(3) {
                        None
                    } else {
                        Some(
                            if n.is_multiple_of(2) {
                                "agentic"
                            } else {
                                "batch"
                            }
                            .to_string(),
                        )
                    };
                    c.stores.push(s);
                    n += 1;
                }
            }
        }
    }
    c
}

type Sums = (u64, u64);

fn add(slot: &mut Sums, events: u64, tokens: u64) {
    slot.0 += events;
    slot.1 += tokens;
}

#[test]
fn the_rank_minute_cells_summed_over_ranks_are_the_fleet_minute_cells() {
    // The fold-side twin of `correctness::the_rank_cells_summed_over_ranks_are_the_fleet_
    // minute_cells`: one fold, two collapses, and the finer one sums to the coarser one
    // key for key — for the class cells and for the banded cells alike.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day = EmittedDay::from_classification(
        &spread_classification(),
        &m,
        &std::collections::BTreeMap::new(),
    );
    let cells = super::day_cells(&day.facts);
    let ranks: std::collections::BTreeSet<(String, u32)> = cells
        .rank_minute
        .iter()
        .map(|r| (r.instance.clone(), r.dp_rank))
        .collect();
    assert_eq!(ranks.len(), 4, "the fixture spreads across four ranks");
    assert!(
        cells.fleet_minute.iter().any(|r| r.events > 1),
        "at least one fleet cell must SUM two ranks, or the collapse is untested"
    );

    let mut over_ranks: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.rank_minute {
        let key = (
            r.bucket,
            r.class.as_str(),
            r.workload_class.clone(),
            r.model.clone(),
        );
        add(over_ranks.entry(key).or_default(), r.events, r.tokens);
    }
    let fleet: std::collections::BTreeMap<_, Sums> = cells
        .fleet_minute
        .iter()
        .map(|r| {
            (
                (
                    r.bucket,
                    r.class.as_str(),
                    r.workload_class.clone(),
                    r.model.clone(),
                ),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(
        over_ranks, fleet,
        "the rank cells summed over ranks are the fleet cells"
    );

    let mut over_ranks: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.band_rank_minute {
        let key = (
            r.bucket,
            r.class.as_str(),
            r.band.as_str(),
            r.workload_class.clone(),
            r.model.clone(),
        );
        add(over_ranks.entry(key).or_default(), r.events, r.tokens);
    }
    let fleet: std::collections::BTreeMap<_, Sums> = cells
        .band_fleet_minute
        .iter()
        .map(|r| {
            (
                (
                    r.bucket,
                    r.class.as_str(),
                    r.band.as_str(),
                    r.workload_class.clone(),
                    r.model.clone(),
                ),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(
        over_ranks, fleet,
        "the banded rank cells summed over ranks are the banded fleet cells"
    );
}

#[test]
#[allow(clippy::too_many_lines)] // one pin, four grains: the inventory IS the test
fn the_band_cells_partition_the_class_cells_at_every_grain() {
    // `banding_splits_the_rollup_without_moving_any_sum` at daily grain, extended to all
    // four class/band pairs the eight tables form: summing the band away reproduces the
    // class cells exactly, at day, hour, fleet-minute and rank-minute grain.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day = EmittedDay::from_classification(
        &spread_classification(),
        &m,
        &std::collections::BTreeMap::new(),
    );
    let cells = super::day_cells(&day.facts);
    let bands: std::collections::BTreeSet<&str> =
        cells.band_daily.iter().map(|r| r.band.as_str()).collect();
    assert_eq!(bands.len(), 5, "the fixture lands in five bands: {bands:?}");

    let holder_key = |h: &Holder| (h.instance.clone(), h.dp_rank, h.group_idx);
    let mut daily: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.band_daily {
        let key = (
            r.bucket,
            r.class.as_str(),
            r.workload_class.clone(),
            holder_key(&r.holder),
        );
        add(daily.entry(key).or_default(), r.events, r.tokens);
    }
    let class_daily: std::collections::BTreeMap<_, Sums> = cells
        .class_daily
        .iter()
        .map(|r| {
            (
                (
                    r.bucket,
                    r.class.as_str(),
                    r.workload_class.clone(),
                    holder_key(&r.holder),
                ),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(daily, class_daily, "day grain");

    let mut hourly: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.band_hourly {
        let key = (
            r.bucket,
            r.class.as_str(),
            r.workload_class.clone(),
            holder_key(&r.holder),
        );
        add(hourly.entry(key).or_default(), r.events, r.tokens);
    }
    let class_hourly: std::collections::BTreeMap<_, Sums> = cells
        .class_hourly
        .iter()
        .map(|r| {
            (
                (
                    r.bucket,
                    r.class.as_str(),
                    r.workload_class.clone(),
                    holder_key(&r.holder),
                ),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(hourly, class_hourly, "hour grain");

    let mut fleet: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.band_fleet_minute {
        let key = (r.bucket, r.class.as_str(), r.workload_class.clone());
        add(fleet.entry(key).or_default(), r.events, r.tokens);
    }
    let fleet_minute: std::collections::BTreeMap<_, Sums> = cells
        .fleet_minute
        .iter()
        .map(|r| {
            (
                (r.bucket, r.class.as_str(), r.workload_class.clone()),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(fleet, fleet_minute, "fleet minute grain");

    let mut rank: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.band_rank_minute {
        let key = (
            r.bucket,
            r.instance.clone(),
            r.dp_rank,
            r.class.as_str(),
            r.workload_class.clone(),
        );
        add(rank.entry(key).or_default(), r.events, r.tokens);
    }
    let rank_minute: std::collections::BTreeMap<_, Sums> = cells
        .rank_minute
        .iter()
        .map(|r| {
            (
                (
                    r.bucket,
                    r.instance.clone(),
                    r.dp_rank,
                    r.class.as_str(),
                    r.workload_class.clone(),
                ),
                (r.events, r.tokens),
            )
        })
        .collect();
    assert_eq!(rank, rank_minute, "rank minute grain");
}

#[test]
fn the_minute_cells_summed_over_the_day_are_the_daily_cells() {
    // One fold at three grains: the fleet minute cells summed over the day's minutes at the
    // (day, class, workload, model) cut equal the daily holder cells summed over holder and
    // tier at the same cut. The engine-side twin is
    // `correctness::the_minute_spine_sums_to_the_daily_rollup_and_never_doubles`.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day = EmittedDay::from_classification(
        &spread_classification(),
        &m,
        &std::collections::BTreeMap::new(),
    );
    let cells = super::day_cells(&day.facts);
    let minutes: std::collections::BTreeSet<i64> =
        cells.fleet_minute.iter().map(|r| r.bucket).collect();
    assert!(
        minutes.len() >= 6,
        "the fixture spreads across minutes: {minutes:?}"
    );

    let mut from_minutes: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.fleet_minute {
        let key = (
            r.bucket.div_euclid(86_400),
            r.class.as_str(),
            r.workload_class.clone(),
            r.model.clone(),
        );
        add(from_minutes.entry(key).or_default(), r.events, r.tokens);
    }
    let mut from_days: std::collections::BTreeMap<_, Sums> = std::collections::BTreeMap::new();
    for r in &cells.class_daily {
        let key = (
            r.bucket,
            r.class.as_str(),
            r.workload_class.clone(),
            r.model.clone(),
        );
        add(from_days.entry(key).or_default(), r.events, r.tokens);
    }
    assert_eq!(from_minutes, from_days);
}

#[test]
fn a_model_rides_every_minute_collapse_as_workload_does() {
    // The engine-backed `day_cells` suite runs a model-free fixture (the relay is the
    // roster's, pinned by `roster_guards`), so the model KEY PATH is pinned here: two
    // instances, one with a relayed model and one without, must land in separate fleet
    // and rank cells that carry the model, never in one cell with the model dropped.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let c = spread_classification();
    let mut models = std::collections::BTreeMap::new();
    models.insert(
        ("i1".to_string(), "run-a".to_string()),
        "llama-3".to_string(),
    );
    let day = EmittedDay::from_classification(&c, &m, &models);
    let cells = super::day_cells(&day.facts);
    let fleet_models: std::collections::BTreeSet<Option<String>> =
        cells.fleet_minute.iter().map(|r| r.model.clone()).collect();
    assert_eq!(
        fleet_models,
        [None, Some("llama-3".to_string())].into_iter().collect(),
        "both model values survive the fleet collapse"
    );
    for r in &cells.rank_minute {
        assert_eq!(
            r.model.as_deref(),
            (r.instance == "i1").then_some("llama-3"),
            "a rank cell carries its instance's relayed model: {r:?}"
        );
    }
    for r in &cells.band_rank_minute {
        assert_eq!(
            r.model.as_deref(),
            (r.instance == "i1").then_some("llama-3")
        );
    }
}

/// The spread fixture's facts, and the same facts a day later under fresh block names —
/// two days, so clearing and retiring have a day to leave alone.
fn two_days() -> (Vec<super::ClassifiedFact>, Vec<super::ClassifiedFact>) {
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let day = EmittedDay::from_classification(
        &spread_classification(),
        &m,
        &std::collections::BTreeMap::new(),
    );
    let later: Vec<super::ClassifiedFact> = day
        .facts
        .iter()
        .map(|f| {
            let mut g = f.clone();
            g.at_ms += DAY_MS;
            g.block = format!("{}-next", f.block);
            g
        })
        .collect();
    (day.facts, later)
}

#[test]
fn subtracting_the_facts_added_returns_the_state_to_where_it_was_and_zeroed_cells_leave() {
    // The keyed state's law: subtract is the exact inverse of add over the same facts,
    // at every one of the eight shapes, and a cell whose events reach zero leaves rather
    // than lingering as a zero row.
    let (facts, _) = two_days();
    let (a, b) = facts.split_at(facts.len() / 2);
    let mut state = super::Cells::default();
    state.add_facts(a);
    let only_a = state.clone();
    state.add_facts(b);
    assert_eq!(
        state.rows(),
        super::day_cells(&facts),
        "adding in two halves is the whole-day fold"
    );
    state.subtract_facts(b);
    assert_eq!(
        state, only_a,
        "subtracting the second half restores the first"
    );
    assert_eq!(state.underflows(), 0);
    state.subtract_facts(a);
    assert!(
        state.is_empty(),
        "every cell leaves when its events reach zero; {} stayed",
        state.len()
    );
    assert_eq!(state.underflows(), 0);
}

#[test]
fn merging_lane_partials_is_order_independent_and_equals_one_fold() {
    // A whole fold runs a lane per stripe and absorbs them: three partials merged in
    // either order are the one fold over everything.
    let (facts, _) = two_days();
    let third = facts.len() / 3;
    let parts = [
        &facts[..third],
        &facts[third..2 * third],
        &facts[2 * third..],
    ];
    let partial = |f: &[super::ClassifiedFact]| {
        let mut c = super::Cells::default();
        c.add_facts(f);
        c
    };
    let mut forward = partial(parts[0]);
    forward.merge(partial(parts[1]));
    forward.merge(partial(parts[2]));
    let mut backward = partial(parts[2]);
    backward.merge(partial(parts[1]));
    backward.merge(partial(parts[0]));
    assert_eq!(forward, backward);
    assert_eq!(forward.rows(), super::day_cells(&facts));
}

#[test]
fn a_day_clears_and_retires_by_its_bucket_at_every_grain() {
    // Every key carries its bucket — a day index at day grain, unix seconds below it —
    // and the day is recovered from either spelling, so a clear or a retire takes every
    // shape's cells for that day and no other's.
    let (first, second) = two_days();
    let d = super::day_index(first[0].at_ms);
    let mut state = super::Cells::default();
    state.add_facts(&first);
    state.add_facts(&second);
    assert_eq!(state.days(), [d, d + 1].into_iter().collect());
    state.clear_day(d);
    assert_eq!(state.days(), [d + 1].into_iter().collect());
    assert_eq!(
        state.rows(),
        super::day_cells(&second),
        "the other day is untouched"
    );
    let mut state = super::Cells::default();
    state.add_facts(&first);
    state.add_facts(&second);
    state.retire_before(d + 1);
    assert_eq!(state.rows(), super::day_cells(&second));
    state.retire_before(d + 2);
    assert!(state.is_empty());
}

#[test]
fn a_subtraction_of_facts_never_added_is_counted_not_hidden() {
    // The one thing a state derived from the facts can never do is take out more than
    // it holds. Saturating arithmetic keeps the numbers sane; the COUNT is what a pin
    // or a journal line reads, so the drift is named rather than absorbed.
    let (first, second) = two_days();
    let mut state = super::Cells::default();
    state.add_facts(&first);
    state.subtract_facts(&second);
    assert!(state.underflows() > 0, "absent cells are underflows");
    let mut state = super::Cells::default();
    state.add_facts(&first);
    state.subtract_facts(&first);
    assert_eq!(state.underflows(), 0);
    state.subtract_facts(&first);
    assert!(
        state.underflows() > 0,
        "a second subtraction has nothing to take"
    );
}

#[test]
fn a_drain_renders_every_touched_cell_as_its_value_or_its_tombstone_and_then_nothing() {
    // What a tick writes: the cells that moved since the last drain — present ones
    // whole, departed ones as tombstones carrying their tag — and a second drain is
    // empty. Subtracting everything a state holds leaves every key as a tombstone.
    let (facts, _) = two_days();
    let mut state = super::Cells::default();
    state.add_facts(&facts);
    let first = state.drain_changes();
    assert_eq!(
        first.len(),
        state.len(),
        "every cell is a change on first sight"
    );
    assert_eq!(first.gone(), 0);
    assert!(state.drain_changes().is_empty(), "drained means forgotten");
    let (a, b) = facts.split_at(facts.len() / 2);
    state.subtract_facts(b);
    let changes = state.drain_changes();
    assert!(
        !changes.is_empty() && changes.gone() > 0,
        "a departed key is a tombstone"
    );
    for c in &changes.class_daily {
        assert_eq!(
            c.gone,
            c.row.events == 0,
            "a tombstone carries zero measures and nothing else does: {c:?}"
        );
    }
    state.subtract_facts(a);
    let changes = state.drain_changes();
    assert_eq!(
        changes.gone(),
        changes.len(),
        "everything left, everything a tombstone"
    );
    assert!(state.is_empty());
    for c in &changes.band_daily {
        assert!(
            super::Band::ALL.contains(&c.row.band),
            "a tombstone keeps its band"
        );
    }
}

#[test]
fn clearing_a_day_touches_that_days_cells_and_no_others_and_rows_of_day_selects_it() {
    let (first, second) = two_days();
    let d = super::day_index(first[0].at_ms);
    let mut state = super::Cells::default();
    state.add_facts(&first);
    state.add_facts(&second);
    assert_eq!(state.rows_of_day(d), super::day_cells(&first));
    assert_eq!(state.rows_of_day(d + 1), super::day_cells(&second));
    state.discard_changes();
    let day_rows = state.rows_of_day(d);
    let before = day_rows.class_daily.len()
        + day_rows.class_hourly.len()
        + day_rows.fleet_minute.len()
        + day_rows.rank_minute.len()
        + day_rows.band_daily.len()
        + day_rows.band_hourly.len()
        + day_rows.band_fleet_minute.len()
        + day_rows.band_rank_minute.len();
    state.clear_day(d);
    let changes = state.drain_changes();
    assert_eq!(
        changes.len(),
        before,
        "every cell of the cleared day is a change"
    );
    assert_eq!(changes.gone(), before, "and every one a tombstone");
    assert!(
        changes.class_daily.iter().all(|c| c.row.bucket == d),
        "no other day's cell moved"
    );
    assert_eq!(state.rows(), super::day_cells(&second));
}

#[test]
fn band_rollup_dedups_identity_exactly_as_the_class_rollup_does() {
    // An opener and a same-instant re-admit on one block are ONE fact — the store's
    // replace-key law, which both folds must state identically.
    let m = Meaning::current(&Discriminator::of(&Params::default()));
    let mut c = classification();
    let mut twin = c.stores[0].clone();
    twin.class = Class::ReAdmit;
    c.stores.push(twin);
    let day = EmittedDay::from_classification(&c, &m, &std::collections::BTreeMap::new());
    let banded = super::band_rollup(&day.facts, super::Grain::Daily);
    assert_eq!(banded.iter().map(|r| r.events).sum::<u64>(), 1);
}
