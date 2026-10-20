// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed operators driven by the input log (`keyed::Driver`): ticking as the log grows
//! classifies and places what one tick over the settled log does, and what waits between ticks
//! is a few batch references, never the records past the watermark.
//!
//! Engine-free: the input log is committed directly, and a map of batch to records stands in
//! for the segments' objects.

mod keyed_log;

use analyzer_analysis::class_band::ClassBandCells;
use analyzer_analysis::dup_cells::DupCells;
use analyzer_analysis::module::Mergeable;
use analyzer_analysis::Params;
use analyzer_pipeline::keyed::{
    Driver, Emission, PlacementCompute, RamState, RepeatCompute, Seeds, SegmentSource, Watermark,
};
use keyed_log::{segments, stream, Chunks, Log};

fn driver() -> Driver<Seeds> {
    let skew = Params::default().delta_skew_ms;
    Driver::new(
        RepeatCompute::new(4, Emission::Watermarked { delay_ms: skew }),
        PlacementCompute::new(
            4,
            Emission::Watermarked {
                delay_ms: skew / 2.0,
            },
            RamState::default,
        ),
    )
}

#[test]
fn ticking_as_the_log_grows_classifies_what_the_settled_log_does() {
    let params = Params::default();
    let events = stream(12.0, 12);
    let log = Log::new("driver-grow");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let mut growing = driver();
    let mut total = ClassBandCells::default();
    let mut placed = DupCells::default();
    for (seq, segment) in segments(&events).into_iter().enumerate() {
        let newest = segment.last().map_or(0.0, |e| e.at_ms);
        log.commit(&chunks, i64::try_from(seq).expect("small"), segment);
        // The watermark trails the newest segment, as a segment's seal makes it.
        let run = growing
            .tick(
                &log.lake,
                &source,
                &params,
                Watermark::At(newest - 150_000.0),
            )
            .expect("tick");
        assert_eq!(run.late.records, 0);
        assert!(
            run.pending <= 2,
            "{} batches pending: records held, not references",
            run.pending
        );
        total.merge(run.cells);
        placed.merge(run.placement.cells);
    }
    let end = events.last().map_or(0.0, |e| e.at_ms) + params.delta_skew_ms + 1.0;
    let last = growing
        .tick(&log.lake, &source, &params, Watermark::At(end))
        .expect("the last tick");
    total.merge(last.cells);
    placed.merge(last.placement.cells);

    let mut settled = driver();
    let settled_run = settled
        .tick(&log.lake, &source, &params, Watermark::At(end))
        .expect("the settled run");
    assert!(
        total.buckets.values().sum::<u64>() > 0,
        "nothing was classified"
    );
    assert_eq!(total.buckets, settled_run.cells.buckets, "class counts");
    assert_eq!(
        total.cells, settled_run.cells.cells,
        "the class and band cells"
    );
    assert!(!placed.is_empty(), "nothing was placed");
    assert_eq!(placed, settled_run.placement.cells, "the placement cells");
    assert!(
        !settled.covered().cells().minutes().is_empty(),
        "no coverage was stated"
    );
    assert_eq!(
        growing.covered().cells().minutes(),
        settled.covered().cells().minutes(),
        "coverage by minute"
    );
    assert_eq!(
        growing.covered().cells().days_held(),
        settled.covered().cells().days_held(),
        "coverage by day"
    );
}
