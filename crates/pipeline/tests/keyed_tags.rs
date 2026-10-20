// SPDX-License-Identifier: AGPL-3.0-only

//! Declared tag intervals through the keyed driver (spec §2.8): the tag cells a run ends with
//! are those of every store repeat compute classified, attributed against the declarations as
//! they finally stand, whether the declarations were committed before the stores classified,
//! hours after, or shortened by a late statement.

mod keyed_log;

use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::tags::{TagCells, TagIndex, TagStatement};
use analyzer_analysis::Params;
use analyzer_pipeline::keyed::{Driver, Seeds, SegmentSource, SpineLayout, Store, Watermark};
use analyzer_pipeline::ledger::DeclaredTag;
use keyed_log::{segments, stream, Chunks, Log};

fn open(log: &Log) -> (Driver<Seeds>, Store) {
    let (driver, store) = keyed_log::open_driver(&log.lake, &Params::default()).expect("open");
    (
        driver.with_layout(SpineLayout {
            partitions: 8,
            retention_days: 7,
        }),
        store,
    )
}

fn tag(
    id: &str,
    identity: &str,
    label: &str,
    from: f64,
    until: Option<f64>,
    at: f64,
) -> DeclaredTag {
    DeclaredTag {
        declaration_id: id.into(),
        identity: identity.into(),
        tag: label.into(),
        from_ms: from,
        until_ms: until,
        at_ms: at,
    }
}

/// When the declarations are committed.
#[derive(Clone, Copy, PartialEq)]
enum When {
    /// Before any store classifies.
    First,
    /// After the watermark has passed their intervals.
    Late,
    /// First, then shortened by a statement that arrives late.
    Shortened,
}

/// Run the driver over six hours of thirty blocks with three declarations, and return its tag
/// cells and the facts it stated.
fn run(when: When) -> (TagCells, Vec<ClassifiedFact>, Vec<DeclaredTag>) {
    let params = Params::default();
    let all = stream(6.0, 30);
    let parts = segments(&all);
    let t0 = all[0].at_ms;
    let hour = 3_600_000.0;
    let mut declared = vec![
        tag(
            "d1",
            "b3",
            "rule-1.applied",
            t0 + hour,
            Some(t0 + 3.0 * hour),
            t0,
        ),
        tag(
            "d2",
            "b4",
            "rule-1.kept",
            t0 + hour,
            Some(t0 + 3.0 * hour),
            t0,
        ),
        tag("d3", "b7", "rule-2.applied", t0 + 0.5 * hour, None, t0),
    ];
    let log = Log::new("tags");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = open(&log);
    if when != When::Late {
        log.commit_tags("declarer", 0, declared.clone());
    }
    let mut stated = Vec::new();
    let end = all[all.len() - 1].at_ms + params.delta_skew_ms + 1.0;
    let watermarks = parts
        .iter()
        .map(|p| p.last().map_or(0.0, |e| e.at_ms) - 150_000.0)
        .chain([end]);
    for (seq, watermark) in watermarks.enumerate() {
        if let Some(part) = parts.get(seq) {
            log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        }
        if seq == 50 {
            match when {
                When::Late => log.commit_tags("declarer", 0, declared.clone()),
                When::Shortened => {
                    let shorter = tag(
                        "d1",
                        "b3",
                        "rule-1.applied",
                        t0 + hour,
                        Some(t0 + 1.5 * hour),
                        t0 + 4.0 * hour,
                    );
                    log.commit_tags("declarer", 1, vec![shorter.clone()]);
                    declared.push(shorter);
                }
                When::First => {}
            }
        }
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(&log.lake, &source, &params, Watermark::At(watermark))
            .expect("tick");
        stated.extend(run.cells.facts);
        driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    (driver.tag_cells().clone(), stated, declared)
}

/// The reference: every stated store, attributed against the declarations as they finally
/// stand.
fn expected(stated: &[ClassifiedFact], declared: &[DeclaredTag]) -> TagCells {
    let mut index = TagIndex::default();
    for d in declared {
        index.state(&TagStatement {
            declaration_id: d.declaration_id.clone(),
            identity: d.identity.clone(),
            tag: d.tag.clone(),
            from_ms: d.from_ms,
            until_ms: d.until_ms,
            at_ms: d.at_ms,
        });
    }
    let mut cells = TagCells::default();
    cells.apply(stated, &index, Params::default().delta_skew_ms, 1);
    cells
}

#[test]
fn tag_cells_are_the_stated_stores_attributed_against_the_final_declarations() {
    for when in [When::First, When::Late, When::Shortened] {
        let (got, stated, declared) = run(when);
        let want = expected(&stated, &declared);
        assert!(
            want.cells().len() > 20,
            "too few tag cells to prove anything"
        );
        assert_eq!(
            got.cells(),
            want.cells(),
            "declared {}",
            match when {
                When::First => "first",
                When::Late => "late",
                When::Shortened => "then shortened",
            }
        );
    }
}
