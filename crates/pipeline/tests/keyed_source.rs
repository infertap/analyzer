// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed pipeline's input, pinned at the mechanism (`keyed::SegmentSource`): each run's
//! segments in its own sequence whatever the commit order; a gap holds a run until the segment
//! arrives, a loss is declared, or the grace releases it; a released gap's segment arrives late
//! and says so; a position resumes without delivering anything twice; and the watermark is the
//! slowest live run's contiguous edge.
//!
//! Engine-free: the source reads the input log alone, and these cases commit its records
//! directly, as `producer_watermark.rs` does.

use analyzer_pipeline::keyed::{
    Delivery, MissingSegment, Order, Position, SegmentReader, SegmentSource, Source,
};
use analyzer_pipeline::ledger::{BatchRecord, DeclaredDrop, SegmentArrival, SHARD};
use analyzer_pipeline::{lineage::Lineage, Lake};

struct Log {
    lake: Lake,
    dir: std::path::PathBuf,
}

impl Drop for Log {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn log(name: &str) -> Log {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "keyed-source-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    Log {
        lake: Lake::new(
            std::sync::Arc::new(analyzer_objectstore::DirStore::new(&dir)),
            String::new(),
            Lineage::new("test", "writer").expect("lineage"),
        ),
        dir,
    }
}

/// A segment of `run` at `seq`, whose records end at `at_ms`, ingested at `ingested_ms`.
fn segment(run: &str, seq: i64, at_ms: f64) -> SegmentArrival {
    SegmentArrival {
        incarnation: run.into(),
        segment_seq: seq,
        instance: Some("i1".into()),
        first_record_at_ms: at_ms - 1000.0,
        last_record_at_ms: at_ms,
        heartbeat_secs: 60,
        max_segment_secs: 300,
        hb_count: 1,
        hb_first_at_ms: None,
        hb_last_at_ms: None,
        stop_at_ms: None,
        msgs_seen: 1,
        dropped: 0,
        oversized: 0,
        dropped_at_hb_first: 0,
        oversized_at_hb_first: 0,
        publisher_restarts: 0,
        agent_version: Some("t".into()),
        topic_model: None,
        topic_pod: None,
        declared_model: None,
        declared_pod: None,
        engine_version: None,
        attributed: true,
        canary: None,
    }
}

fn batch(id: &str, ingested_ms: u64) -> BatchRecord {
    BatchRecord {
        batch_id: id.into(),
        carrier: "segment".into(),
        source: "fixture".into(),
        object_key: String::new(),
        etag: String::new(),
        input_position: 0,
        ingested_at_ms: ingested_ms,
        record_count: 1,
        unknown_kinds: 0,
        first_at_ms: None,
        last_at_ms: None,
        days: Vec::new(),
        segments: Vec::new(),
        drops: Vec::new(),
        lifecycle: Vec::new(),
        stored: Vec::new(),
        annotations: Vec::new(),
        tags: Vec::new(),
    }
}

fn carrying(id: &str, ingested_ms: u64, arrival: SegmentArrival) -> BatchRecord {
    let mut b = batch(id, ingested_ms);
    b.segments.push(arrival);
    b
}

fn dropping(id: &str, run: &str, first: i64, last: i64) -> BatchRecord {
    let mut b = batch(id, 1);
    b.drops.push(DeclaredDrop {
        incarnation: run.into(),
        first_seq: first,
        last_seq: last,
        at_ms: 0.0,
    });
    b
}

/// Commit `batches` as one cycle, at the next position.
fn commit(log: &Log, batches: Vec<BatchRecord>) {
    let ledger = log.lake.ledger(SHARD).expect("input log");
    ledger
        .commit_cycle(ledger.next_position().expect("position"), batches)
        .expect("commit");
}

/// Reads nothing: these cases are about order and position, not records.
struct NoRecords;

impl SegmentReader for NoRecords {
    fn read(
        &self,
        _: &Delivery,
    ) -> Result<Vec<analyzer_analysis::events::Event>, analyzer_pipeline::Error> {
        Ok(Vec::new())
    }
}

/// Deliver from `from`, returning the new position and each delivery's batch and order.
fn deliver(source: &SegmentSource<'_>, from: &Position) -> (Position, Vec<(String, Order)>) {
    let mut seen = Vec::new();
    let at = source
        .deliver(from, &mut |d: Delivery| {
            seen.push((d.batch_id, d.order));
            Ok(())
        })
        .expect("deliver");
    (at, seen)
}

fn ids(seen: &[(String, Order)]) -> Vec<&str> {
    seen.iter().map(|(id, _)| id.as_str()).collect()
}

#[test]
fn each_run_arrives_in_its_own_sequence_whatever_the_commit_order() {
    let log = log("order");
    // One cycle, committed in reverse: the run anchors at its lowest sequence.
    commit(
        &log,
        vec![
            carrying("r1-2", 1, segment("r1", 2, 3000.0)),
            carrying("r1-0", 1, segment("r1", 0, 1000.0)),
            carrying("r1-1", 1, segment("r1", 1, 2000.0)),
        ],
    );
    let source = SegmentSource::new(&log.lake, &NoRecords);
    let (at, seen) = deliver(&source, &Position::default());
    assert_eq!(ids(&seen), ["r1-0", "r1-1", "r1-2"]);
    assert!(seen.iter().all(|(_, o)| *o == Order::InOrder));
    assert_eq!(source.watermark(&at), Some(3000.0));
}

#[test]
fn a_gap_holds_its_run_until_the_segment_arrives_and_both_held_segments_follow() {
    let log = log("gap");
    commit(&log, vec![carrying("r1-0", 1, segment("r1", 0, 1000.0))]);
    let source = SegmentSource::new(&log.lake, &NoRecords);
    let (at, seen) = deliver(&source, &Position::default());
    assert_eq!(ids(&seen), ["r1-0"]);
    // Two segments behind the gap, in one cycle: one input position for both.
    commit(
        &log,
        vec![
            carrying("r1-2", 1, segment("r1", 2, 3000.0)),
            carrying("r1-3", 1, segment("r1", 3, 4000.0)),
        ],
    );
    let (at, seen) = deliver(&source, &at);
    assert!(seen.is_empty(), "a gap delivered past: {seen:?}");
    assert_eq!(at.held(), 2, "both segments of one cycle are held");
    assert_eq!(
        source.watermark(&at),
        Some(1000.0),
        "the proof holds at the gap"
    );
    commit(&log, vec![carrying("r1-1", 1, segment("r1", 1, 2000.0))]);
    let (at, seen) = deliver(&source, &at);
    assert_eq!(ids(&seen), ["r1-1", "r1-2", "r1-3"]);
    assert_eq!(at.held(), 0);
    assert_eq!(source.watermark(&at), Some(4000.0));
}

#[test]
fn a_declared_loss_fills_a_gap_even_when_it_is_committed_later() {
    let log = log("drop");
    commit(
        &log,
        vec![
            carrying("r1-0", 1, segment("r1", 0, 1000.0)),
            carrying("r1-3", 1, segment("r1", 3, 4000.0)),
        ],
    );
    let source = SegmentSource::new(&log.lake, &NoRecords);
    let (at, seen) = deliver(&source, &Position::default());
    assert_eq!(ids(&seen), ["r1-0"]);
    commit(&log, vec![dropping("d", "r1", 1, 2)]);
    // Order is promised within a run; across runs and batches without a claim, delivery
    // follows commit order, so the held segment (committed first) precedes the drop.
    let (at, seen) = deliver(&source, &at);
    assert_eq!(
        seen,
        [
            ("r1-3".to_string(), Order::InOrder),
            ("d".to_string(), Order::Unsequenced)
        ]
    );
    assert_eq!(source.watermark(&at), Some(4000.0));
}

#[test]
fn waiting_never_releases_and_the_grace_releases_to_the_lowest_held_segment() {
    let log = log("grace");
    commit(
        &log,
        vec![carrying("r1-0", 1_000, segment("r1", 0, 1000.0))],
    );
    commit(
        &log,
        vec![
            carrying("r1-2", 2_000, segment("r1", 2, 3000.0)),
            carrying("r1-3", 2_000, segment("r1", 3, 4000.0)),
        ],
    );
    let waiting = SegmentSource::new(&log.lake, &NoRecords);
    let (_, seen) = deliver(&waiting, &Position::default());
    assert_eq!(ids(&seen), ["r1-0"], "a waiting source releases nothing");

    let grace = MissingSegment::ReleaseAfter { grace_ms: 60_000.0 };
    let early = SegmentSource::new(&log.lake, &NoRecords).with_missing(grace, 30_000.0);
    let (_, seen) = deliver(&early, &Position::default());
    assert_eq!(ids(&seen), ["r1-0"], "released inside the grace");

    let late = SegmentSource::new(&log.lake, &NoRecords).with_missing(grace, 70_000.0);
    let (at, seen) = deliver(&late, &Position::default());
    assert_eq!(
        ids(&seen),
        ["r1-0", "r1-2", "r1-3"],
        "2 skipped or out of order"
    );
    assert!(seen.iter().all(|(_, o)| *o == Order::InOrder));

    // The missing segment, when it comes, is late.
    commit(
        &log,
        vec![carrying("r1-1", 80_000, segment("r1", 1, 2000.0))],
    );
    let (_, seen) = deliver(&late, &at);
    assert_eq!(seen, [("r1-1".to_string(), Order::Late)]);
}

#[test]
fn a_position_resumes_and_delivers_nothing_twice() {
    let log = log("resume");
    commit(&log, vec![carrying("r1-0", 1, segment("r1", 0, 1000.0))]);
    let source = SegmentSource::new(&log.lake, &NoRecords);
    let (first, _) = deliver(&source, &Position::default());
    let (again, seen) = deliver(&source, &first);
    assert!(seen.is_empty(), "redelivered: {seen:?}");
    assert_eq!(again, first);
    // A checkpoint's round trip resumes where the source stood.
    let stored: Position =
        serde_json::from_str(&serde_json::to_string(&first).expect("encode")).expect("decode");
    commit(&log, vec![carrying("r1-1", 1, segment("r1", 1, 2000.0))]);
    let (_, seen) = deliver(&source, &stored);
    assert_eq!(ids(&seen), ["r1-1"]);
}

#[test]
fn a_stopped_run_stops_constraining_and_batches_without_a_claim_prove_nothing() {
    let log = log("stop");
    let mut stopped = segment("r2", 0, 9000.0);
    stopped.stop_at_ms = Some(9000.0);
    commit(
        &log,
        vec![
            carrying("r1-0", 1, segment("r1", 0, 5000.0)),
            carrying("r2-0", 1, stopped),
        ],
    );
    let source = SegmentSource::new(&log.lake, &NoRecords);
    let (at, _) = deliver(&source, &Position::default());
    assert_eq!(
        source.watermark(&at),
        Some(5000.0),
        "the live run bounds the proof"
    );

    let orphans = self::log("orphans");
    commit(&orphans, vec![batch("o", 1)]);
    let source = SegmentSource::new(&orphans.lake, &NoRecords);
    let (at, seen) = deliver(&source, &Position::default());
    assert_eq!(seen, [("o".to_string(), Order::Unsequenced)]);
    assert_eq!(source.watermark(&at), None);
}

#[test]
fn the_watermark_is_the_slowest_runs_contiguous_edge() {
    let cases: Vec<(Vec<BatchRecord>, Option<f64>)> = vec![
        // A gap at 1 holds the run at segment 0's last record.
        (
            vec![
                carrying("a0", 1, segment("a", 0, 1000.0)),
                carrying("a2", 1, segment("a", 2, 3000.0)),
            ],
            Some(1000.0),
        ),
        // A declared loss closes the gap: the run is live through segment 2.
        (
            vec![
                carrying("a0", 1, segment("a", 0, 1000.0)),
                carrying("a2", 1, segment("a", 2, 3000.0)),
                dropping("d", "a", 1, 1),
            ],
            Some(3000.0),
        ),
        // Two live runs: the fleet holds at the slower.
        (
            vec![
                carrying("a0", 1, segment("a", 0, 1000.0)),
                carrying("b0", 1, segment("b", 0, 7000.0)),
                carrying("b1", 1, segment("b", 1, 8000.0)),
            ],
            Some(1000.0),
        ),
    ];
    for (i, (batches, want)) in cases.into_iter().enumerate() {
        let log = log(&format!("agree-{i}"));
        commit(&log, batches);
        let source = SegmentSource::new(&log.lake, &NoRecords);
        let (at, _) = deliver(&source, &Position::default());
        assert_eq!(source.watermark(&at), want, "case {i}");
    }
}
