// SPDX-License-Identifier: AGPL-3.0-only

//! The duplication cells' fold tests.

use crate::events::{Event, Kind, ScopeClosers};

fn event(kind: Kind, instance: &str, block: &str, at_ms: f64, n_tokens: i64) -> Event {
    Event {
        kind,
        instance: instance.to_string(),
        dp_rank: Some(0),
        group_idx: Some(0),
        block_id: Some(block.to_string()),
        content_id: None,
        key_epoch: None,
        tier: None,
        backend_id: None,
        at_ms,
        locality: None,
        n_tokens,
        parent_id: None,
        parent_unknown: false,
        delivery_id: None,
        incarnation: "run-1".to_string(),
        seq: None,
        reused: false,
        workload_class: Some("chat".to_string()),
    }
}

/// **A block whose holders hold still adds its rates**: for identities folded once and then
/// left alone, the sum of their [`super::Rates`] over a later window states exactly the
/// cells each identity's own fold over that window states, and their whale days the same,
/// across partial minutes, a midnight, two classes, a sole-instance cut and the pairs.
#[test]
fn identities_that_hold_still_add_their_rates_exactly() {
    use crate::module::DerivationModule;
    const DAY: f64 = 86_400_000.0;
    let half_skew = 500.0;
    let on = |instance: &str, rank: u32, block: &str, at: f64, n: i64, class: &str| {
        let mut e = event(Kind::Store, instance, block, at, n);
        e.dp_rank = Some(rank);
        e.workload_class = Some(class.to_string());
        e
    };
    let first = crate::Window {
        start_ms: DAY - 600_000.0,
        end_ms: DAY - 300_000.0,
    };
    let later = (DAY - 287_655.0, DAY + 200_777.0);
    let identities = [
        // Two ranks of one instance: internal duplication, a sole-instance cut.
        (
            "a",
            vec![
                on("i1", 0, "a", DAY - 590_000.0, 16, "chat"),
                on("i1", 1, "a", DAY - 580_000.0, 16, "chat"),
            ],
        ),
        // Two instances, two classes.
        (
            "b",
            vec![
                on("i1", 0, "b", DAY - 570_000.0, 32, "chat"),
                on("i2", 0, "b", DAY - 560_000.0, 32, "agentic"),
            ],
        ),
        // One holder.
        ("c", vec![on("i3", 0, "c", DAY - 550_000.0, 8, "chat")]),
    ];
    let none = ScopeClosers::default();
    let ctx = super::DupCtx {
        closers: &none,
        half_skew,
    };
    let (mut want, mut got) = (super::DupCells::default(), super::DupCells::default());
    let mut total = super::Rates::default();
    for (block, stores) in identities {
        let closers = ScopeClosers::of(&stores);
        let held = super::Duplication
            .fold(
                &block.to_string(),
                &stores,
                super::IdentityState::default(),
                first,
                &super::DupCtx {
                    closers: &closers,
                    half_skew,
                },
            )
            .state;
        let mut quiet = held.clone();
        quiet.whale.days.clear();
        let folded = super::Duplication.fold(
            &block.to_string(),
            &[],
            quiet.clone(),
            crate::Window {
                start_ms: later.0,
                end_ms: later.1,
            },
            &ctx,
        );
        want.merge(folded.cells);
        let rates = super::Rates::of(&held, half_skew, later.0).expect("in effect");
        total.add(&rates, 1.0);
        let mut accrued = quiet.whale.clone();
        accrued.accrue(&rates, held.n_tokens, later);
        assert_eq!(
            accrued.days, folded.state.whale.days,
            "{block}'s whale days"
        );
    }
    got.add_rates(&total, later);
    assert!(!want.is_empty() && want.pairs.len() == 2 && !want.instance_minute.is_empty());
    assert_eq!(got, want, "the rates' cells are the folds' cells");
    // Taking an identity out leaves the others' sum.
    let a = super::Rates::of(
        &super::Duplication
            .fold(
                &"c".to_string(),
                &[on("i3", 0, "c", DAY - 550_000.0, 8, "chat")],
                super::IdentityState::default(),
                first,
                &ctx,
            )
            .state,
        half_skew,
        later.0,
    )
    .expect("in effect");
    total.add(&a, -1.0);
    total.add(&a, 1.0);
    let mut again = super::DupCells::default();
    again.add_rates(&total, later);
    assert_eq!(again, want, "a rate taken out and put back leaves the sum");
}

#[test]
fn an_open_not_yet_in_effect_has_no_rates() {
    use crate::module::DerivationModule;
    let stores = vec![event(Kind::Store, "i1", "p", 59_800.0, 8)];
    let closers = ScopeClosers::of(&stores);
    let state = super::Duplication
        .fold(
            &"p".to_string(),
            &stores,
            super::IdentityState::default(),
            crate::Window {
                start_ms: 0.0,
                end_ms: 60_000.0,
            },
            &super::DupCtx {
                closers: &closers,
                half_skew: 500.0,
            },
        )
        .state;
    assert!(super::Rates::of(&state, 500.0, 60_000.0).is_none());
    assert!(super::Rates::of(&state, 500.0, 60_300.0).is_some());
}

/// **The module's fold is the tick's body**: over one identity's events it yields exactly
/// the cells the direct fold yields over the intervals those events derive, carries the
/// open seed with the mass, drops the state once the residency closes, and asks a repair
/// only for a mass RISE over a mass already emitted.
#[test]
fn the_duplication_module_folds_as_the_tick_did_and_carries_only_open_seeds() {
    use crate::module::DerivationModule;
    let (t0, t1) = (60_000.0_f64, 300_000.0_f64);
    let stores = vec![
        event(Kind::Store, "i1", "p", t0, 8),
        event(Kind::Store, "i2", "p", t0 + 30_000.0, 8),
    ];
    let closers = ScopeClosers::of(&stores);
    let ctx = super::DupCtx {
        closers: &closers,
        half_skew: 0.0,
    };
    let window = crate::Window {
        start_ms: 0.0,
        end_ms: t1,
    };
    let c = super::Duplication.fold(
        &"p".to_string(),
        &stores,
        super::IdentityState::default(),
        window,
        &ctx,
    );
    // The direct fold over the same intervals.
    let keys = crate::events::group_by_interval_key(&stores);
    let intervals = crate::intervals::residency_intervals_for_keys_seeded(
        &keys,
        &closers,
        &std::collections::BTreeMap::new(),
    );
    let mut direct = super::DupCells::default();
    direct.fold_identity(&intervals, 8, 0.0, (0.0, t1));
    assert!(!direct.is_empty(), "the fixture folds cells");
    assert_eq!(c.cells, direct, "the module's cells are the direct fold's");
    assert_eq!(
        c.state.seeds.len(),
        2,
        "two holders open at the window's end"
    );
    assert_eq!(c.state.n_tokens, 8);
    assert!(c.repair_days.is_empty(), "a first sighting is not a repair");

    // The next window: a mass RISE and both residencies closing.
    let later = vec![
        event(Kind::Store, "i1", "p", t1 + 1_000.0, 16),
        event(Kind::Evict, "i1", "p", t1 + 60_000.0, 0),
        event(Kind::Evict, "i2", "p", t1 + 60_000.0, 0),
    ];
    let closers = ScopeClosers::of(&later);
    let ctx = super::DupCtx {
        closers: &closers,
        half_skew: 0.0,
    };
    let next = super::Duplication.fold(
        &"p".to_string(),
        &later,
        c.state,
        crate::Window {
            start_ms: t1,
            end_ms: t1 + 120_000.0,
        },
        &ctx,
    );
    assert_eq!(
        next.repair_days,
        vec![0],
        "the rise re-prices the window's day"
    );
    assert!(
        next.state.seeds.is_empty(),
        "every residency closed: no seed is carried"
    );
    assert!(
        next.state.whale.days.contains_key(&0),
        "the day's whale totals stay with the identity until the day retires"
    );
    assert!(
        !next.cells.is_empty(),
        "the closing window still contributes"
    );
}

#[test]
fn every_spelling_the_fold_writes_maps_back_to_itself() {
    for holders in [1u32, 2, 3, 4, 5, 8, 9, 16, 17, 40] {
        let spelled = super::copy_band(holders);
        assert_eq!(super::static_band(spelled), Some(spelled));
    }
    for (h, i, r) in [
        (1u32, 1u32, 1u32),
        (2, 2, 1),
        (2, 1, 2),
        (2, 1, 1),
        (3, 3, 3),
    ] {
        let spelled = super::locality(h, i, r);
        assert_eq!(super::static_locality(spelled), Some(spelled));
    }
    assert_eq!(super::static_band("k2"), None);
    assert_eq!(super::static_locality("local"), None);
}

#[test]
fn a_restored_row_is_a_cell_and_not_a_change() {
    let mut cells = super::DupCells::default();
    cells
        .place(
            "replication_minute",
            &["chat", "2", "cross", "120000"],
            &[1.5, 3.0],
        )
        .expect("minute");
    cells
        .place(
            "replication_instance_hourly",
            &["i0", "5-8", "within", "3600"],
            &[7.0, 42.0],
        )
        .expect("instance hour");
    cells
        .place(
            "replication_pairs_day",
            &["chat", "3", "i0", "0", "1", "i1", "2", "0"],
            &[9.0],
        )
        .expect("pair");
    assert_eq!(cells.len(), 3);
    assert!(cells.changes().is_empty(), "a restore touches nothing");
    let minute: Vec<_> = cells.minute.iter().collect();
    assert_eq!(
        minute,
        vec![(
            &super::BandKey {
                bucket: 120_000,
                class: "chat".to_string(),
                band: "2",
                locality: "cross",
            },
            &super::BandSum {
                token_ms: 1.5,
                copies_token_ms: 3.0
            }
        )]
    );
    let pair: Vec<_> = cells.pairs.iter().collect();
    assert_eq!(pair[0].0.a.instance, "i0");
    assert_eq!(pair[0].0.b.dp_rank, 2);
    assert!(cells
        .place(
            "replication_minute",
            &["chat", "k2", "cross", "0"],
            &[1.0, 1.0]
        )
        .is_err());
    assert!(cells.place("residual_terms", &[], &[]).is_err());
}

use super::{day_windows, DupCells};
use crate::events::IntervalKey;
use crate::intervals::Interval;

fn interval(instance: &str, dp_rank: u32, open: f64, close: Option<f64>) -> Interval {
    Interval {
        key: IntervalKey {
            instance: instance.to_string(),
            dp_rank,
            group_idx: 0,
            identity: "blk".to_string(),
            key_epoch: None,
            tier: None,
            backend_id: None,
        },
        open_ms: Some(open),
        close_ms: close,
        close_reason: None,
        readmits: 0,
        workload_class: Some("agentic".to_string()),
    }
}

#[test]
fn two_holders_of_one_identity_fold_to_duplicated_token_time_with_no_engine() {
    // The acceptance's engine-free fold: one identity held by two ranks for a minute
    // folds to a band-2 minute cell of 60 s × tokens and one pair cell, from intervals
    // alone.
    let mut cells = DupCells::default();
    let ivs = [
        interval("i1", 0, 60_000.0, Some(120_000.0)),
        interval("i1", 1, 60_000.0, Some(120_000.0)),
    ];
    cells.fold_identity(&ivs, 10, 0.0, (60_000.0, 120_000.0));
    let fleet: Vec<_> = cells
        .minute
        .iter()
        .filter(|(k, _)| k.class.is_empty())
        .collect();
    assert_eq!(fleet.len(), 1, "one fleet minute cell: {fleet:?}");
    let (key, sum) = fleet[0];
    assert_eq!(key.bucket, 60_000);
    assert_eq!(
        key.band,
        super::copy_band(2),
        "two copies is the two-copies band"
    );
    assert!((sum.token_ms - 600_000.0).abs() < 1e-6, "{sum:?}");
    assert!((sum.copies_token_ms - 1_200_000.0).abs() < 1e-6, "{sum:?}");
    assert_eq!(cells.pairs.len(), 1, "one pair of holders");
    assert_eq!(
        cells.hourly.len(),
        2,
        "class and fleet scope, one hour each"
    );
    assert_eq!(cells.daily.len(), 2);
    assert_eq!(
        cells.instance_hourly.len(),
        1,
        "the duplication was internal to i1"
    );
}

#[test]
fn a_tick_writes_what_moved_and_a_second_drain_is_empty() {
    let mut cells = DupCells::default();
    // One residency across two minutes, emitted in two windows — as a tick boundary
    // falls inside it.
    let ivs = [interval("i1", 0, 0.0, Some(120_000.0))];
    cells.fold_identity(&ivs, 4, 0.0, (0.0, 60_000.0));
    let first = cells.drain_changes();
    assert_eq!(first.len(), cells.len());
    assert_eq!(first.gone(), 0);
    assert!(cells.drain_changes().is_empty());
    cells.fold_identity(&ivs, 4, 0.0, (60_000.0, 120_000.0));
    let second = cells.drain_changes();
    assert!(
        second.len() < cells.len(),
        "only the cells the second window touched"
    );
    assert!(
        second.hourly.len() == 2,
        "the hour rollups moved: {:?}",
        second.hourly
    );
}

#[test]
fn clearing_and_retiring_go_by_the_day_in_every_shapes_own_unit() {
    let mut cells = DupCells::default();
    let day_ms = 86_400_000.0;
    cells.fold_identity(
        &[interval("i1", 0, 0.0, Some(60_000.0))],
        4,
        0.0,
        (0.0, 60_000.0),
    );
    cells.fold_identity(
        &[interval("i1", 0, day_ms, Some(day_ms + 60_000.0))],
        4,
        0.0,
        (day_ms, day_ms + 60_000.0),
    );
    assert_eq!(cells.days(), [0, 1].into_iter().collect());
    cells.discard_changes();
    cells.clear_day(0);
    let changes = cells.drain_changes();
    assert_eq!(
        changes.gone(),
        changes.len(),
        "every cleared cell is a tombstone"
    );
    assert_eq!(cells.days(), [1].into_iter().collect());
    cells.retire_before(2);
    assert!(cells.is_empty());
}

#[test]
fn merging_partials_is_the_one_fold() {
    let ivs = [
        interval("i1", 0, 0.0, Some(120_000.0)),
        interval("i2", 0, 0.0, Some(120_000.0)),
    ];
    let mut whole = DupCells::default();
    whole.fold_identity(&ivs, 8, 0.0, (0.0, 120_000.0));
    let mut a = DupCells::default();
    a.fold_identity(&ivs, 8, 0.0, (0.0, 60_000.0));
    let mut b = DupCells::default();
    b.fold_identity(&ivs, 8, 0.0, (60_000.0, 120_000.0));
    b.merge(a);
    assert_eq!(b, whole);
}

#[test]
fn day_windows_split_at_midnight_and_nowhere_else() {
    let d = 86_400_000.0;
    assert_eq!(
        day_windows((d - 10.0, d + 10.0)),
        vec![(0, (d - 10.0, d)), (1, (d, d + 10.0))]
    );
    assert_eq!(day_windows((5.0, 6.0)), vec![(0, (5.0, 6.0))]);
    assert!(day_windows((6.0, 5.0)).is_empty());
}
