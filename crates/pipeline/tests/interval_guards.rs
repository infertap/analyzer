// SPDX-License-Identifier: AGPL-3.0-only

//! Residency-substrate guards: the window edge and the empty case.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::events::EventSource;
use analyzer_analysis::intervals::residency_intervals_from_events;
use analyzer_analysis::Window;
use analyzer_pipeline::parquet_source::{FsStore, ParquetSource};
use serde_json::Value;

/// **The window is half-open `[start, end)`, and both edges are asserted.**
///
/// A fencepost here is the quietest possible defect: an inclusive end silently admits one extra
/// event into every window, and since a re-derivation restates whole days that extra event lands
/// in the *neighbouring* day's evidence too. Nothing errors; a boundary store simply gets counted
/// twice across two days.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_window_includes_its_start_instant_and_excludes_its_end() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 21_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"at_start",
                           "n_tokens":8,"at_ms":base}),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"inside",
                           "n_tokens":8,"at_ms":base+50.0}),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"at_end",
                           "n_tokens":8,"at_ms":base+100.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "interval-edge");

    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 100.0,
    };
    let blocks: Vec<String> =
        residency_intervals_from_events(&source.cache_events("fx", &w).expect("cache IR"))
            .iter()
            .map(|i| i.key.identity.clone())
            .collect();

    assert!(
        blocks.contains(&"at_start".to_string()),
        "the start instant is INSIDE the window: {blocks:?}"
    );
    assert!(
        blocks.contains(&"inside".to_string()),
        "an interior event is inside: {blocks:?}"
    );
    assert!(
        !blocks.contains(&"at_end".to_string()),
        "the end instant is OUTSIDE — half-open, or every boundary event is counted by two \
         adjacent windows: {blocks:?}"
    );
}

/// An empty window yields no intervals rather than one spurious row.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_window_with_no_facts_yields_no_intervals() {
    let source = ParquetSource::new(
        FsStore::new(harness::fresh_dir("interval-empty")),
        harness::fresh_dir("interval-empty-cache"),
        "tap/",
        48,
    );
    let w = Window {
        start_ms: 0.0,
        end_ms: 1.0,
    };
    assert!(
        residency_intervals_from_events(&source.cache_events("fx", &w).expect("cache IR"))
            .is_empty(),
        "nothing observed must produce nothing, never a zero-valued interval"
    );
}

/// One event constructor for the restart trio below.
fn c1_event(
    kind: analyzer_analysis::events::Kind,
    at_ms: f64,
    seq: u64,
) -> analyzer_analysis::events::Event {
    use analyzer_analysis::events::{Event, Kind};
    let names_a_block = kind == Kind::Store;
    let mut event = Event::new(kind, "i1", at_ms);
    event.dp_rank = Some(0);
    event.group_idx = names_a_block.then_some(0);
    event.block_id = names_a_block.then(|| "b1".into());
    event.content_id = names_a_block.then(|| "c1".into());
    event.n_tokens = if names_a_block { 8 } else { 0 };
    event.incarnation = "run-a".into();
    event.seq = Some(seq);
    event
}

/// **Without a reset record, a restart reads as one residency — and that is the honest
/// reading of the evidence, which is exactly why the record must exist.**
///
/// When an engine rank restarts, its cache is cold and every block is gone — and the wire
/// says nothing: `AllBlocksCleared` is emitted only from `reset_prefix_cache()`, never at
/// startup, and `IntervalKey` carries no incarnation. `incarnation` here is the TAP's run,
/// unchanged across an ENGINE restart, so no existing field can split this. Absent a
/// `holder_reset`, the fold must not invent a boundary: one interval carrying a re-admit is
/// what the stream said. The cost of that honesty — residency overstated across the outage,
/// co-residency manufactured into the duplication figure — is carried by the producer's
/// obligation to EMIT the record (spec 2.3), not by a reader guessing.
#[test]
fn a_restart_with_no_reset_record_reads_as_one_residency() {
    use analyzer_analysis::events::Kind;
    let events = vec![
        c1_event(Kind::Store, 1_000.0, 100),
        c1_event(Kind::Store, 2_000.0, 1),
    ];
    let ivs = residency_intervals_from_events(&events);
    assert_eq!(ivs.len(), 1, "no evidence, no invented boundary: {ivs:?}");
    assert_eq!(ivs[0].readmits, 1);
}

/// **A `holder_reset` splits the residency and names the close.**
///
/// The reset carries no block identity; the reader expands it over every open interval under
/// its scope — the `clear` doctrine one level up — and the close reason is `Reset`, not
/// `Clear`: the engine announced nothing, and the engine-id space moved with the process.
#[test]
fn a_holder_reset_splits_residency_and_names_the_close() {
    use analyzer_analysis::events::Kind;
    use analyzer_analysis::intervals::CloseReason;
    let events = vec![
        c1_event(Kind::Store, 1_000.0, 100),
        c1_event(Kind::HolderReset, 1_500.0, 1),
        c1_event(Kind::Store, 2_000.0, 1),
    ];
    let ivs = residency_intervals_from_events(&events);
    assert_eq!(
        ivs.len(),
        2,
        "pre-restart and post-restart stores are two residencies: {ivs:?}"
    );
    assert_eq!(ivs[0].close_ms, Some(1_500.0));
    assert_eq!(ivs[0].close_reason, Some(CloseReason::Reset));
    assert_eq!(
        ivs[0].readmits, 0,
        "the re-store opened, it did not re-admit"
    );
    assert_eq!(ivs[1].open_ms, Some(2_000.0));
    assert_eq!(ivs[1].close_ms, None, "right-open at window end");
}

/// **A store at the reset's own boundary instant belongs to the fresh cache.** The
/// closes-before-stores tie-break is what makes stamping the boundary from the regressed
/// message's earliest event safe: same instant, the reset closes first, the store opens the
/// next residency.
#[test]
fn a_store_at_the_reset_boundary_opens_the_fresh_residency() {
    use analyzer_analysis::events::Kind;
    use analyzer_analysis::intervals::CloseReason;
    let events = vec![
        c1_event(Kind::Store, 1_000.0, 100),
        c1_event(Kind::HolderReset, 2_000.0, 1),
        c1_event(Kind::Store, 2_000.0, 1), // the regressed message's own first store
    ];
    let ivs = residency_intervals_from_events(&events);
    assert_eq!(ivs.len(), 2, "{ivs:?}");
    assert_eq!(ivs[0].close_reason, Some(CloseReason::Reset));
    assert_eq!(ivs[0].close_ms, Some(2_000.0));
    assert_eq!(
        ivs[1].open_ms,
        Some(2_000.0),
        "the boundary-instant store survives the reset and opens the next residency"
    );
}
