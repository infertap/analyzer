// SPDX-License-Identifier: AGPL-3.0-only

//! Classification guards: hand-built cases where one specific wrong reading would be invisible
//! in aggregate.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: each case is a
//! flattering-direction guard from `docs/error-direction.md` — a reading that looks reasonable
//! and moves the headline number the way you would like it to move.

use analyzer_analysis::degradation::{Cause, Register};
use analyzer_analysis::events::EventSource as _;
use analyzer_analysis::intervals::Holder;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::degradation_io::ledger;
use analyzer_pipeline::read::classify;
use serde_json::json;

/// Delivery loss reaches the register through sequence accounting (spec 4.2).
///
/// A segment the analyzer never receives — reclaimed at the producer, lost in transit — leaves a
/// hole in its run's `segment_seq` chain, and that hole is what prices the loss: the missing
/// segment could have carried the close that would have decided an absence-decided store. The
/// guard: with one seq missing, the affected window must read `Register::Indeterminate` for an
/// instance the run covered, even though every record that DID arrive is intact.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_missing_segment_seq_forces_indeterminate_on_the_affected_window() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            json!({"kind": "segment_open", "incarnation": "run-a", "segment_seq": 0,
                   "at_ms": 1_000.0}),
            // seq 1 never arrived
            json!({"kind": "segment_open", "incarnation": "run-a", "segment_seq": 2,
                   "at_ms": 3_000.0}),
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 16,
                   "at_ms": 2_000.0, "incarnation": "run-a"}),
        ],
        off,
        "classify-seq-hole",
    );
    let w = Window {
        start_ms: off,
        end_ms: off + 1_000_000.0,
    };
    let l = ledger(
        &source,
        "fx",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "fx", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("fx")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1, "one loss, one entry");
    assert_eq!(
        l.entries[0].cause,
        Cause::SequenceGap {
            first_missing: 1,
            last_missing: 1
        }
    );
    assert_eq!(
        l.register_for(
            &[Holder {
                instance: "i1".to_string(),
                dp_rank: 0,
                group_idx: 0
            }],
            &w
        ),
        Some(Register::Indeterminate),
        "the seq hole is what carries delivery loss to the register"
    );
}

/// A declared workload class reaches the classified row it describes.
///
/// The producer states it once on the segment header; the decoder stamps it onto every record
/// behind that header; the IR carries it; the fact emits it. This asserts the whole chain rather
/// than any link, because every link is invisible on its own — a header nobody reads, a column
/// nobody fills and a fact reading `undeclared` all look identical from one step away.
///
/// The second segment declares a different class, which is what makes this a test rather than a
/// constant: one class everywhere would pass against a reader that ignored the header entirely.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_declared_workload_class_reaches_the_fact() {
    let (engine, _db) = harness::fresh_engine();
    let at = harness::rebase_offset() + 3_000_000.0;

    for (fleet, class, inst) in [
        ("agentic-fleet", "agentic", "i1"),
        ("batch-fleet", "batch", "i2"),
    ] {
        // Sequential cases share the database and reset between them (the harness idiom):
        // one fleet per database is the shipped shape.
        harness::reset(&engine);
        let seg = format!(
            "{{\"kind\":\"segment_open\",\"at_ms\":{at},\"contract_version\":\"1.2\",\
              \"incarnation\":\"run-a\",\"segment_seq\":0,\"gpu\":\"h100\",\
              \"workload_class\":\"{class}\"}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"{inst}\",\"block_id\":\"p\",\
              \"n_tokens\":16,\"at_ms\":{}}}\n\
             {{\"kind\":\"heartbeat\",\"incarnation\":\"run-a\",\"at_ms\":{},\
              \"msgs_seen\":1,\"dropped\":0,\"oversized\":0,\"endpoints\":[]}}",
            at + 10.0,
            at + 60_000.0
        );
        // The heartbeat a minute on is the run's clock: a sequenced log is classified through
        // its run's last heartbeat, less the skew band, and never past it.
        harness::ingest_bytes(&engine, fleet, "seg.jsonl", seg.as_bytes())
            .expect("ingest the segment");
        let source = harness::source_over_bytes(&format!("classify-{class}"), seg.as_bytes());

        // The column the decoder stamped, from the header onto a record that never carried
        // it — observable on the shaped row, which is what the IR reads. Provenance is
        // operator vocabulary this contract does not name, so it rides the shaped row's
        // `extra` — carried, never validated, and never a store column.
        let decoded =
            analyzer_ingest::decode_segment("seg.jsonl", fleet, seg.as_bytes()).expect("decode");
        let store_row: serde_json::Value = decoded
            .shaped
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("shaped row"))
            .find(|r| r["kind"] == "store")
            .expect("the store row");
        assert_eq!(
            store_row["workload_class"], class,
            "{fleet}: the shaped row carries the header's declaration"
        );
        assert_eq!(
            store_row["extra"]["gpu"], "h100",
            "{fleet}: provenance reached the record"
        );

        // And through the fold, which is the only step a consumer sees.
        let w = harness::cache_window(&engine, fleet);
        let out = classify(&source, fleet, &w, &Params::default()).expect("classify");
        assert_eq!(out.stores.len(), 1, "{fleet}: one store");
        assert_eq!(
            out.stores[0].workload_class.as_deref(),
            Some(class),
            "{fleet}: the fact carries what the producer declared"
        );

        // And in the store, on the class rollup a tick publishes: the workload split reads it.
        harness::executor(&engine, fleet, &Params::default())
            .and_then(|executor| executor.advance(&source))
            .expect("tick");
        let stored = engine
            .run(&format!(
                "SELECT DISTINCT workload_class FROM class_rollup_hourly FINAL \
                 WHERE fleet_id = '{fleet}' AND gone = 0"
            ))
            .expect("read the column");
        let everything = engine
            .run(
                "SELECT fleet_id, instance_id, workload_class, events FROM class_rollup_hourly \
                 FINAL ORDER BY ALL",
            )
            .expect("dump the rollup");
        assert_eq!(
            stored.trim(),
            class,
            "{fleet}: the rollup row carries the header's declaration; the rollup holds:\n\
             {everything}"
        );
    }
}

/// A producer that declares nothing leaves the class absent, and the fact says `undeclared`.
///
/// Absence is a bucket, never a null: a reader must be able to group by "nobody said" rather than
/// have those rows vanish from a split.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_undeclared_producer_reads_as_a_bucket() {
    let (engine, _db) = harness::fresh_engine();
    let at = harness::rebase_offset() + 3_000_000.0;
    let seg = format!(
        "{{\"kind\":\"segment_open\",\"at_ms\":{at},\"contract_version\":\"1.2\",\
          \"incarnation\":\"run-a\",\"segment_seq\":0}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p\",\
          \"n_tokens\":16,\"at_ms\":{}}}",
        at + 10.0
    );
    harness::ingest_bytes(&engine, "quiet", "seg.jsonl", seg.as_bytes()).expect("ingest");
    let source = harness::source_over_bytes("classify-quiet", seg.as_bytes());

    let w = harness::cache_window(&engine, "quiet");
    let out = classify(&source, "quiet", &w, &Params::default()).expect("classify");
    assert_eq!(out.stores[0].workload_class, None, "nothing was declared");

    let day = analyzer_analysis::emitted::EmittedDay::from_classification(
        &out,
        &analyzer_analysis::emitted::Meaning::current(&analyzer_analysis::Discriminator::of(
            &Params::default(),
        )),
        &std::collections::BTreeMap::new(),
    );
    assert_eq!(
        day.facts[0].workload_class,
        analyzer_analysis::emitted::UNDECLARED,
        "absent on the wire becomes a bucket on the row, never a null"
    );
}
