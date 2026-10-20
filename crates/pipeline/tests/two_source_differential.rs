// SPDX-License-Identifier: AGPL-3.0-only

//! **Two sources, one corpus — the differential that keeps the derivation's reading honest.**
//!
//! The two legs are two INDEPENDENT IMPLEMENTATIONS over two ENCODINGS. `BucketSource` is the
//! test tree's naive twin: whole-prefix listing, whole-file row decode, no cache, no pruning.
//! `ParquetSource` is the production source: dated discovery, the fetch-once content cache, the
//! projected and row-group-pruned columnar decode. They share exactly one thing — the shaped-row
//! vocabulary `analyzer_segments` defines — and disagree about everything operational, which is
//! what gives the comparison teeth: a pruning bound that clips a live row, a projection that
//! drops a live column, a cache that serves stale bytes, or a discovery that misses an hour all
//! surface as the two IRs diverging. The mixed-form corpus adds the encoding axis: the same
//! records as JSONL and as Parquet twins must read identically, so neither decoder can drift
//! from the record model without the other one calling it.
//!
//! **Why equality at the IR is the whole proof.** Every derivation above this boundary is a pure
//! fold over the Ordered Event IR — no engine, no I/O, no ambient state. So identical IR implies
//! identical output for every fold, by construction rather than by enumeration. The
//! classification below is run anyway, off both IRs, because "the fold executes with no store in
//! the path" is a separate claim from "the inputs match" and both are worth having.
//!
//! **The corpus is chosen for the derivation that can differ, not for size.** Every IR field is
//! a function of the bytes, so equality alone is satisfiable by two readers that both drop
//! `segment_seq` on the floor — and sequence accounting (spec 4.2) is the machinery that prices
//! delivery loss. So the corpus omits one incarnation's MIDDLE `segment_seq`: both sources must
//! fold the SAME `SequenceGap` from their own IR, and the ledger must scope the resulting
//! `Indeterminate` to exactly the instance that run covered. The hole is here so the comparison
//! is evidence.

use harness::bucket_source::BucketSource;

use analyzer_analysis::degradation::{sequence_holes_from_events, Cause, Register, Scope};
use analyzer_analysis::events::{Event, EventSource};
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::degradation_io::ledger;
use analyzer_pipeline::parquet_source::{FsStore, ParquetSource};

/// Write one segment into the corpus and ingest the SAME bytes into the store.
///
/// The input log carries the lifecycle the section 5.1 scope expansion reads — the ledger
/// assertion below exercises it — so both halves must describe one stream.
fn seed(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    dir: &std::path::Path,
    fleet: &str,
    name: &str,
    body: &str,
) {
    std::fs::write(dir.join(name), body).expect("write segment");
    harness::ingest_bytes(engine, fleet, name, body.as_bytes()).expect("ingest segment");
}

/// The corpus's segment bodies: cache facts, lifecycle facts, and one incarnation with a hole
/// in its `segment_seq` chain — shared by the all-JSONL seeding and the mixed-form corpus, so
/// every leg of the differential reads the same records.
fn segment_bodies(base: f64) -> Vec<(&'static str, String)> {
    // A tap run declares itself, then reports. `segment_open` carries the incarnation every row in
    // its own batch is stamped with, so each segment needs its own header.
    let inc = "run-a";
    vec![
        (
            "0001.jsonl",
            format!(
            "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"{inc}\",\"segment_seq\":1,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\"n_tokens\":16,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p2\",\"parent_id\":\"p1\",\"n_tokens\":32,\"at_ms\":{}}}\n\
             {{\"kind\":\"evict\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\"at_ms\":{}}}",
            base,
            base + 10.0,
            base + 20.0,
            base + 30.0
        ),
        ),
        // A second holder, with the scope dimensions present — they are nullable, and a source
        // that read absent and zero as the same thing would pass a single-worker corpus.
        (
            "0002.jsonl",
            format!(
            "{{\"kind\":\"segment_open\",\"instance_id\":\"i2\",\"incarnation\":\"{inc}\",\"segment_seq\":2,\"at_ms\":{}}}\n\
             {{\"kind\":\"agent_start\",\"instance_id\":\"i2\",\"incarnation\":\"{inc}\",\"heartbeat_secs\":30,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i2\",\"dp_rank\":2,\"group_idx\":1,\"block_id\":\"q1\",\"n_tokens\":8,\"tier\":\"gpu\",\"locality\":\"local\",\"at_ms\":{}}}\n\
             {{\"kind\":\"clear\",\"instance_id\":\"i2\",\"dp_rank\":2,\"group_idx\":1,\"at_ms\":{}}}",
            base + 40.0,
            base + 41.0,
            base + 50.0,
            base + 60.0
        ),
        ),
        // **The anchor, first half.** A second incarnation, covering i3 alone. Its chain
        // observes seq 0 and seq 2 (the file below); seq 1 never arrives. The hole is the section 4.2
        // sequence accounting every source must derive identically from its own bytes.
        (
            "0003.jsonl",
            format!(
            "{{\"kind\":\"segment_open\",\"instance_id\":\"i3\",\"incarnation\":\"run-b\",\"segment_seq\":0,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i3\",\"block_id\":\"r1\",\"n_tokens\":4,\"at_ms\":{}}}",
            base + 70.0,
            base + 80.0
        ),
        ),
        // **A segment whose FILE order is not the IR's order**, without which the differential
        // cannot see the sort at all. Found by mutation: deleting the bucket source's `sort`
        // call left every assertion green, because a corpus written in sorted order agrees with
        // itself.
        //
        // Two keys are wrong here on purpose. `s2` is stamped LATER than the pair below it and
        // written FIRST, so `at_ms` has to reorder. And the `evict` of `s1` shares an instant
        // with its own `store` while being written after it, so `close_first_rank` has to put
        // the close first — the rule that keeps a re-admission at the same timestamp from
        // reading as a still-open interval.
        (
            "0004.jsonl",
            format!(
            "{{\"kind\":\"segment_open\",\"instance_id\":\"i4\",\"incarnation\":\"{inc}\",\"segment_seq\":3,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i4\",\"block_id\":\"s2\",\"n_tokens\":2,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i4\",\"block_id\":\"s1\",\"n_tokens\":2,\"at_ms\":{}}}\n\
             {{\"kind\":\"evict\",\"instance_id\":\"i4\",\"block_id\":\"s1\",\"at_ms\":{}}}",
            base + 90.0,
            base + 120.0,
            base + 100.0,
            base + 100.0
        ),
        ),
        // **The anchor, second half.** run-b's seq 2 — the far side of the hole. Two observed
        // opens with seq 1 between them absent is what makes the gap DETECTED loss
        // rather than the retention edge's lower-bound territory.
        (
            "0005.jsonl",
            format!(
            "{{\"kind\":\"segment_open\",\"instance_id\":\"i3\",\"incarnation\":\"run-b\",\"segment_seq\":2,\"at_ms\":{}}}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i3\",\"block_id\":\"r2\",\"n_tokens\":4,\"at_ms\":{}}}",
            base + 130.0,
            base + 140.0
        ),
        ),
    ]
}

/// The corpus, seeded whole: every segment into the bucket dir AND the store as identical
/// JSONL bytes.
fn corpus(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    dir: &std::path::Path,
    fleet: &str,
    base: f64,
) {
    for (name, body) in segment_bodies(base) {
        seed(engine, dir, fleet, name, &body);
    }
}

/// The production leg: a `ParquetSource` over a dated tree holding the given `(name, bytes)`
/// segments, with a fresh cache and a horizon comfortably past the rebase offset.
fn parquet_source_over(
    label: &str,
    base: f64,
    entries: &[(String, Vec<u8>)],
) -> ParquetSource<FsStore> {
    let root = harness::fresh_dir(&format!("{label}-dated"));
    let hour = harness::dated_hour_of(base);
    for (name, bytes) in entries {
        harness::plant_dated(&root, &hour, name, bytes);
    }
    ParquetSource::new(
        FsStore::new(&root),
        harness::fresh_dir(&format!("{label}-cache")),
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms()
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
#[allow(clippy::too_many_lines)] // one narrative: two legs over three event families
fn the_two_implementations_yield_the_same_ordered_event_ir() {
    let (engine, _db) = harness::fresh_engine();
    let dir = harness::fresh_dir("two-source");
    let fleet = "fx";
    let base = harness::rebase_offset() + 1_000_000.0;
    corpus(&engine, &dir, fleet, base);

    let bucket = BucketSource::new(&dir);
    // The production leg: the same JSONL bytes under a dated tree, read by the shipped source.
    let entries: Vec<(String, Vec<u8>)> = segment_bodies(base)
        .into_iter()
        .map(|(name, body)| (name.to_string(), body.into_bytes()))
        .collect();
    let parquet = parquet_source_over("two-source", base, &entries);

    // Wide enough to hold the whole corpus: the window is not what is under test here.
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 1_000.0,
    };

    // ---- cache facts -------------------------------------------------------------------------
    let from_bucket = bucket.cache_events(fleet, &w).expect("bucket cache events");
    let from_parquet = parquet
        .cache_events(fleet, &w)
        .expect("parquet cache events");

    assert!(
        !from_bucket.is_empty(),
        "the corpus must actually decode, or this test proves nothing"
    );
    assert_eq!(
        from_bucket.len(),
        from_parquet.len(),
        "event counts differ\nbucket: {from_bucket:#?}\nparquet: {from_parquet:#?}"
    );
    for (i, (a, b)) in from_bucket.iter().zip(from_parquet.iter()).enumerate() {
        assert_eq!(
            a, b,
            "cache event {i} differs\nbucket: {a:#?}\nparquet: {b:#?}"
        );
    }

    // ---- lifecycle facts ---------------------------------------------------------------------
    let (lo, hi) = (base - 1.0, base + 1_000.0);
    let life_bucket = bucket
        .lifecycle_events(fleet, lo, hi)
        .expect("bucket lifecycle");
    let life_parquet = parquet
        .lifecycle_events(fleet, lo, hi)
        .expect("parquet lifecycle");
    assert_eq!(
        life_bucket.len(),
        life_parquet.len(),
        "lifecycle counts differ\nbucket: {life_bucket:#?}\nparquet: {life_parquet:#?}"
    );
    for (i, (a, b)) in life_bucket.iter().zip(life_parquet.iter()).enumerate() {
        assert_eq!(
            a, b,
            "lifecycle event {i} differs\nbucket: {a:#?}\nparquet: {b:#?}"
        );
    }

    // **The anchor asserted directly.** Equality above is satisfiable by two readers that both
    // drop `segment_seq` on the floor; this is the line that makes the corpus load-bearing. The
    // section 4.2 fold over each source's OWN lifecycle IR must find the same hole — run-b's missing
    // middle seq — because that hole is how delivery loss is priced.
    let holes = sequence_holes_from_events(&life_bucket);
    assert_eq!(
        holes,
        sequence_holes_from_events(&life_parquet),
        "the two implementations must fold the SAME SequenceGap from their own bytes"
    );
    assert_eq!(holes.len(), 1, "exactly the planted hole: {holes:#?}");
    assert_eq!(
        (
            holes[0].incarnation.as_str(),
            holes[0].missing_from,
            holes[0].missing_to
        ),
        ("run-b", 1, 1)
    );

    // And the ledger prices it: run-scoped to run-b, covering exactly i3, forcing
    // `Indeterminate` — the register the loss arrives under. The ledger assembles off the
    // production source; its section 5.1 expansion still asks the store who the run covered.
    let l = ledger(
        &parquet,
        fleet,
        &w,
        &analyzer_pipeline::read::uncertainty_model(&parquet, fleet, &w, &Params::default())
            .expect("model"),
        &parquet
            .segment_opens_at_retention_horizon(fleet)
            .expect("opens"),
    )
    .expect("ledger");
    let gaps: Vec<_> = l
        .entries
        .iter()
        .filter(|e| matches!(e.cause, Cause::SequenceGap { .. }))
        .collect();
    assert_eq!(gaps.len(), 1, "one hole, one entry: {gaps:#?}");
    assert_eq!(gaps[0].register(), Register::Indeterminate);
    let Scope::Run {
        incarnation,
        instances,
    } = &gaps[0].scope
    else {
        panic!("a sequence hole is a run-scoped loss: {:?}", gaps[0].scope);
    };
    assert_eq!(incarnation, "run-b");
    assert_eq!(
        instances,
        &vec!["i3".to_string()],
        "exactly the instance the holed run covered degrades, and no other"
    );

    // ---- segment opens at the retention horizon ----------------------------------------------
    let opens_bucket = bucket
        .segment_opens_at_retention_horizon(fleet)
        .expect("bucket opens");
    let opens_parquet = parquet
        .segment_opens_at_retention_horizon(fleet)
        .expect("parquet opens");
    assert_eq!(
        opens_bucket.len(),
        5,
        "one open per segment: {opens_bucket:#?}"
    );
    for (i, (a, b)) in opens_bucket.iter().zip(opens_parquet.iter()).enumerate() {
        assert_eq!(
            a, b,
            "segment open {i} differs\nbucket: {a:#?}\nparquet: {b:#?}"
        );
    }
}

/// The mixed-form corpus: the same records, some segments JSONL and some their
/// Parquet twins, and the readings agree — `ParquetSource` over the MIXED bytes against
/// `BucketSource` over the all-JSONL rendering, identical on every field except `delivery_id`,
/// which is the content hash and therefore differs wherever the FORM differs, by construction
/// rather than by defect.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_mixed_form_corpus_reads_identically_across_the_encodings() {
    let dir = harness::fresh_dir("mixed-form");
    let fleet = "fx";
    let base = harness::rebase_offset() + 1_000_000.0;

    // The bucket dir stays all-JSONL; the dated tree takes the MIXED forms, so the two legs
    // share every record while the encodings differ on half the segments.
    let mut mixed: Vec<(String, Vec<u8>)> = Vec::new();
    for (i, (name, body)) in segment_bodies(base).into_iter().enumerate() {
        std::fs::write(dir.join(name), &body).expect("write jsonl corpus");
        let (key, bytes) = if i % 2 == 0 {
            (
                name.replace(".jsonl", ".parquet"),
                harness::parquet::twin_of_jsonl(&body, 2),
            )
        } else {
            (name.to_string(), body.into_bytes())
        };
        mixed.push((key, bytes));
    }
    let with_extension = |ext: &str| {
        mixed.iter().any(|(k, _)| {
            std::path::Path::new(k)
                .extension()
                .is_some_and(|e| e == ext)
        })
    };
    assert!(
        with_extension("parquet") && with_extension("jsonl"),
        "a mixed corpus must actually mix"
    );

    let bucket = BucketSource::new(&dir);
    let parquet = parquet_source_over("mixed-form", base, &mixed);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 1_000.0,
    };

    // The two implementations over the two encodings: same records, different bytes for the
    // converted half — identical everywhere but the content hash.
    let strip = |mut events: Vec<Event>| -> Vec<Event> {
        for e in &mut events {
            e.delivery_id = None;
        }
        events
    };
    let from_bucket = bucket.cache_events(fleet, &w).expect("bucket cache");
    let from_parquet = parquet.cache_events(fleet, &w).expect("parquet cache");
    assert!(!from_parquet.is_empty(), "an empty corpus proves nothing");
    assert_eq!(
        strip(from_bucket.to_vec()),
        strip(from_parquet.to_vec()),
        "one record model, two encodings: the readings must not differ beyond the hash"
    );

    // Lifecycle and the anchor: both fold the same SequenceGap out of their own bytes.
    let (lo, hi) = (base - 1.0, base + 1_000.0);
    let life_bucket = bucket
        .lifecycle_events(fleet, lo, hi)
        .expect("bucket lifecycle");
    let life_parquet = parquet
        .lifecycle_events(fleet, lo, hi)
        .expect("parquet lifecycle");
    assert_eq!(
        life_bucket, life_parquet,
        "lifecycle IR differs by encoding"
    );
    let holes = sequence_holes_from_events(&life_parquet);
    assert_eq!(holes, sequence_holes_from_events(&life_bucket));
    assert_eq!(holes.len(), 1, "exactly the planted hole: {holes:#?}");
    assert_eq!(
        (
            holes[0].incarnation.as_str(),
            holes[0].missing_from,
            holes[0].missing_to
        ),
        ("run-b", 1, 1)
    );

    let opens_bucket = bucket
        .segment_opens_at_retention_horizon(fleet)
        .expect("bucket opens");
    let opens_parquet = parquet
        .segment_opens_at_retention_horizon(fleet)
        .expect("parquet opens");
    assert_eq!(opens_bucket.len(), 5, "one open per segment");
    assert_eq!(
        opens_bucket, opens_parquet,
        "segment opens differ by encoding"
    );
}

/// The derivation runs off segments with no store anywhere in the fold.
///
/// **A separate claim from the one above.** Equal inputs prove equal outputs only if something
/// actually consumes them; this executes the real classification over both file IRs and asserts
/// they agree. `classify_from_events` takes an uncertainty model and a parameter set and nothing
/// else — no `Engine` is reachable from it, which is the property the bright line exists to hold.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_classification_folds_the_same_off_either_implementation() {
    let (engine, _db) = harness::fresh_engine();
    let dir = harness::fresh_dir("two-source-fold");
    let fleet = "fx";
    let base = harness::rebase_offset() + 1_000_000.0;
    corpus(&engine, &dir, fleet, base);

    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 1_000.0,
    };
    let params = Params::default();

    let entries: Vec<(String, Vec<u8>)> = segment_bodies(base)
        .into_iter()
        .map(|(name, body)| (name.to_string(), body.into_bytes()))
        .collect();
    let parquet = parquet_source_over("two-source-fold", base, &entries);

    // The uncertainty model is bookkeeping about what was OBSERVED, not a cache derivation.
    // Both folds take the same one — built off the production source — so what varies between
    // them is the cache-event reading alone.
    let model = analyzer_pipeline::read::uncertainty_model(&parquet, fleet, &w, &params)
        .expect("uncertainty model");

    let bucket_ir = BucketSource::new(&dir)
        .cache_events(fleet, &w)
        .expect("bucket cache events");
    let parquet_ir = parquet
        .cache_events(fleet, &w)
        .expect("parquet cache events");

    let from_bucket =
        analyzer_analysis::classify::classify_from_events(&bucket_ir, &model, &params);
    let from_parquet =
        analyzer_analysis::classify::classify_from_events(&parquet_ir, &model, &params);

    assert_eq!(
        from_bucket.bucket_counts, from_parquet.bucket_counts,
        "the same corpus classified differently by implementation"
    );
    assert!(
        !from_bucket.bucket_counts.is_empty(),
        "the fold must actually classify something"
    );
}

#[test]
fn a_verified_carrier_with_no_selected_events_is_distinct_from_missing_history() {
    use analyzer_pipeline::source::{FillExclusion, InputSelection};
    let base = harness::rebase_offset();
    let records = [
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"a","at_ms":1000.0}),
        serde_json::json!({"kind":"evict","instance_id":"i1","block_id":"a","at_ms":4000.0}),
    ];
    let (root, _) = harness::plant_tree(&records, base, "carrier-evidence");
    let files = harness::planted_files(&root);
    assert_eq!(files.len(), 1);
    let flat = harness::fresh_dir("carrier-evidence-flat");
    std::fs::write(flat.join("carrier.jsonl"), &files[0].1).unwrap();
    let expected = analyzer_segments::decode_segment(&files[0].0, "f", &files[0].1)
        .unwrap()
        .batch_id;
    let production = harness::source_over_tree("carrier-evidence-source", root, base);
    let reference = BucketSource::new(flat);
    for source in [
        &production as &dyn analyzer_pipeline::source::FillSource,
        &reference as &dyn analyzer_pipeline::source::FillSource,
    ] {
        let mut count = 0;
        let completed = source
            .stream_cache_events_into(
                "f",
                InputSelection::Window(Window::new(base + 2000.0, base + 3000.0).unwrap()),
                &FillExclusion::default(),
                &mut |events, _| {
                    count += events.len();
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].0, expected,
            "empty selection still attests the carrier"
        );
    }
}
