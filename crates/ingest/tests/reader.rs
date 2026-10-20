// SPDX-License-Identifier: AGPL-3.0-only

//! Reader tests over an input log of their own, through [`analyzer_ingest::ingest_bytes`] —
//! the transport-free entry the conformance corpora and `selftest` share. The transport
//! itself (listing, GET, object identity) is proven in `tests/bucket_live.rs` against a real
//! bucket; everything here is about what the reader does with bytes once it has them:
//! shaping, the tail rule, the unknown-kind rule, content-hash idempotency, and recovery
//! from a cycle whose commit never landed. No engine: ingest writes the log alone.

mod common;

use std::path::PathBuf;

use analyzer_ingest::ingest_bytes;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// One store record for `block`, stamped at `t` — the minimal distinct-content batch.
fn store_line(block: &str, t: f64) -> String {
    format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"{block}\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n"
    )
}

#[test]
fn replay_is_recovery_byte_identical_derived_state() {
    // Replay-is-recovery(design): destroy the database, re-feed the same bytes,
    // identical state — byte-exact, since nothing is stamped at read time any more.
    //
    // Under the shipped backend a database is not a directory a test may delete — the
    // server owns its data directory exclusively. Dropping the guard drops the
    // database, which is the same annihilation expressed in the terms the server
    // understands, and the recovery it proves is the one a customer would actually
    // perform: replay from the bucket.
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":7,\"at_ms\":{t}}}\n\
         {{\"kind\":\"heartbeat\",\"at_ms\":{t},\"msgs_seen\":3,\"dropped\":0}}\n"
    );

    // Everything ingest writes about a batch: its record in the input log, less the one
    // legitimately differing field, each side's own commit clock.
    let dump = |lake: &analyzer_lake::Lake| -> String {
        let records: Vec<String> = common::committed(lake)
            .into_iter()
            .map(|mut b| {
                b.ingested_at_ms = 0;
                format!("{b:?}")
            })
            .collect();
        format!(
            "{}\n---\n{}",
            records.join("\n"),
            common::stored_mass_of(lake)
        )
    };

    let before = {
        let lake = common::fresh_lake("replay-a");
        ingest_bytes(&lake, "f1", "a.jsonl", content.as_bytes()).expect("first ingest");
        dump(&lake)
    }; // the log is gone, not merely emptied
    let lake = common::fresh_lake("replay-b");
    ingest_bytes(&lake, "f1", "a.jsonl", content.as_bytes()).expect("recovery ingest");
    let after = dump(&lake);

    assert_eq!(
        before, after,
        "recovery must reproduce derived state byte-for-byte"
    );
    assert!(
        before.contains("a.jsonl") && before.contains("\tstore\t"),
        "the dump actually carries the data: {before}"
    );
}

#[test]
fn the_truncated_tail_is_discarded_and_the_batch_commits() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_"
    );
    let report = ingest_bytes(&lake, "f1", "cut.jsonl", content.as_bytes()).expect("ingest");
    assert_eq!(report.records_inserted, 1);
    let committed = common::committed(&lake);
    assert_eq!(
        committed
            .iter()
            .map(|b| (b.record_count, b.unknown_kinds))
            .collect::<Vec<_>>(),
        vec![(1, 0)],
        "the torn final line is not a record, and the batch still commits"
    );
}

#[test]
fn unknown_kinds_land_whole_counted_and_never_fatal() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"future_kind\",\"at_ms\":{t},\"payload\":{{\"x\":1}},\
         \"note\":\"n\"}}\n"
    );
    let report = ingest_bytes(&lake, "f1", "future.jsonl", content.as_bytes()).expect("ingest");
    assert_eq!(report.unknown_kinds, 1);
    assert_eq!(report.records_inserted, 1);
    // "Lands whole" is the DECODE's claim now — nothing event-grain persists an unknown
    // kind, so the shaped row is where wholeness is observable: the kind string kept, the
    // agent-scoped sentinel applied, every unrecognized field riding `extra`.
    let decoded =
        analyzer_ingest::decode_segment("future.jsonl", "f1", content.as_bytes()).expect("decode");
    let row: serde_json::Value =
        serde_json::from_str(decoded.shaped.lines().next().expect("a shaped row"))
            .expect("shaped row parses");
    assert_eq!(row["kind"], "future_kind");
    assert_eq!(row["instance_id"], "");
    assert_eq!(row["extra"]["note"], "n");
    assert_eq!(row["extra"]["payload"], "{\"x\":1}");
    let unknown: Vec<u64> = common::committed(&lake)
        .iter()
        .map(|b| b.unknown_kinds)
        .collect();
    assert_eq!(unknown, vec![1]);
}

#[test]
fn a_cycle_whose_commit_never_landed_is_fetched_again_and_recorded_once() {
    // The crash window between a cycle's records and its head: the log's head refuses the
    // commit, the cycle fails, and nothing is committed; the next cycle fetches the object
    // again and records it once, its stored mass once. No pending state, nothing to purge:
    // the first cycle's records are orphans the log never reads.
    let faulty = std::sync::Mutex::new(None);
    let lake = common::fresh_lake_over("uncommitted", |root| {
        let store = analyzer_objectstore::testing::FaultyStore::over(std::sync::Arc::new(
            analyzer_objectstore::DirStore::new(root),
        ));
        *faulty.lock().expect("store") = Some(store.clone());
        std::sync::Arc::new(store)
    });
    let faulty = faulty
        .into_inner()
        .expect("store")
        .expect("the fault store");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":5,\"at_ms\":{t}}}\n"
    );
    faulty.refuse(Some(std::sync::Arc::new(
        |key: &str, _: Option<&[u8]>, _: &[u8]| {
            key.ends_with("ledger/shards/0/head")
                .then(|| "the commit never landed".to_string())
        },
    )));
    ingest_bytes(&lake, "f1", "a.jsonl", content.as_bytes()).expect_err("the commit refused");
    assert!(common::committed(&lake).is_empty(), "nothing committed");
    faulty.refuse(None);
    #[allow(clippy::cast_possible_truncation)] // day indices are small integers
    let day = (t / 86_400_000.0).floor() as i64;
    let report = ingest_bytes(&lake, "f1", "a.jsonl", content.as_bytes()).expect("the retry");
    assert_eq!(report.files_ingested, 1);
    assert_eq!(
        (
            common::stored_mass_of(&lake),
            common::committed(&lake).len()
        ),
        (format!("i1\tstore\t{day}\t1\t5\n"), 1),
        "one record, five tokens, one committed batch — never doubled"
    );
}

#[test]
fn identical_content_under_two_names_ingests_once() {
    // Content-hash idempotency across calls: the second feed of the same bytes under a
    // different name reads the first's committed ledger row and skips. One batch, one
    // committed row, records never doubled.
    let lake = common::fresh_lake("reader");
    let content = store_line("p1", now_ms());

    let first = ingest_bytes(&lake, "f1", "a.jsonl", content.as_bytes()).expect("first");
    assert_eq!((first.files_ingested, first.duplicates_skipped), (1, 0));
    let second = ingest_bytes(&lake, "f1", "b.jsonl", content.as_bytes()).expect("second");
    assert_eq!((second.files_ingested, second.duplicates_skipped), (0, 1));
    assert_eq!(
        second.files_seen,
        second.files_ingested + second.duplicates_skipped,
        "the fail-closed accounting invariant: every file seen is ingested or a duplicate"
    );
    let committed = common::committed(&lake).len();
    let stored: u64 = common::stored_mass_of(&lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(
        format!("{stored}\t{committed}"),
        "1\t1",
        "one record in the denominator, one committed ledger row — never doubled"
    );
}

#[test]
fn mid_file_corruption_fails_closed() {
    let lake = common::fresh_lake("reader");
    let content = "garbage line\n{\"kind\":\"store\",\"instance_id\":\"i1\",\
                   \"block_id\":\"p1\",\"at_ms\":1.0}\n";
    let err = ingest_bytes(&lake, "f1", "bad.jsonl", content.as_bytes()).expect_err("must refuse");
    assert!(
        matches!(
            &err,
            analyzer_ingest::Error::Segment(analyzer_segments::Error::Corrupt(m))
                if m.contains("not the final line")
        ),
        "unexpected error: {err}"
    );
    let committed = common::committed(&lake).len();
    let stored: u64 = common::stored_mass_of(&lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(
        format!("{stored}\t{committed}"),
        "0\t0",
        "nothing lands from a refused batch: no denominator, no committed row"
    );
}

/// Nothing from a refused batch commits, **including records that preceded the fault**.
///
/// `mid_file_corruption_fails_closed` puts the bad line first, so it pins the refusal but says
/// nothing about partial commits. This one puts two good records ahead of the fault, which is
/// what the property actually needs to mean.
///
/// It is load-bearing beyond corruption. A recovered orphan carries its
/// `segment_recovered` marker as a TRAILER — appending is O(1), prepending is not — so a
/// reader that committed incrementally would have already stored those records as attributed
/// by the time it learned they were not. Whole-batch-or-nothing is what makes a trailer safe
/// to put at the end.
#[test]
fn a_fault_late_in_a_batch_commits_none_of_the_records_before_it() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p2\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n\
         garbage in the middle\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p3\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n"
    );
    ingest_bytes(&lake, "f1", "late.jsonl", content.as_bytes()).expect_err("must refuse");
    let committed = common::committed(&lake).len();
    let stored: u64 = common::stored_mass_of(&lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(
        format!("{stored}\t{committed}"),
        "0\t0",
        "the two valid records BEFORE the fault must not have committed: no denominator, \
         no committed row"
    );
}

/// The delivery family is modelled, not counted as drift.
///
/// `segment_open` is the first record of every segment file, so before it was modelled
/// `unknown_kinds` sat at roughly the number of files ingested — and that counter renders in the
/// report as a trust line. A fail-honest counter pinned above zero for a benign reason cannot
/// report the reason it exists for: real contract drift was indistinguishable from the floor.
#[test]
fn delivery_records_are_modelled_rather_than_counted_as_drift() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"segment_open\",\"incarnation\":\"1753142891447-4417\",\
         \"segment_seq\":0,\"at_ms\":{t},\"gpu\":\"h100\"}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n"
    );
    let report = ingest_bytes(
        &lake,
        "f1",
        "seg-1753142891447-4417-0.jsonl",
        content.as_bytes(),
    )
    .expect("ingest");
    assert_eq!(
        report.unknown_kinds, 0,
        "a segment_open is contractual (conformance/delivery/layout.json), not drift"
    );
    assert_eq!(report.records_inserted, 2);

    // Typed fields, not `extra` strings, on the shaped rows the IR decodes: continuity is
    // scoped to (instance, incarnation), and a seq that arrives as a string cannot be
    // compared for gaps.
    let decoded =
        analyzer_ingest::decode_segment("seg-1753142891447-4417-0.jsonl", "f1", content.as_bytes())
            .expect("decode");
    let rows: Vec<serde_json::Value> = decoded
        .shaped
        .lines()
        .map(|l| serde_json::from_str(l).expect("shaped row parses"))
        .collect();
    let open = &rows[0];
    assert_eq!(open["kind"], "segment_open");
    assert_eq!(open["incarnation"], "1753142891447-4417");
    assert_eq!(open["segment_seq"], 0);
    assert_eq!(open["extra"]["gpu"], "h100");

    // The cache record carries the BATCH's incarnation, taken from the header — that is the
    // whole point. A block record's key space, configuration and continuity sequence all belong
    // to the tap run that wrote it, and without this the only row that knows which run that was
    // is the header itself. `segment_seq` stays absent: only the header has a position.
    let store = &rows[1];
    assert_eq!(store["kind"], "store");
    assert_eq!(store["incarnation"], "1753142891447-4417");
    assert!(store.get("segment_seq").is_none());
}

/// `segments_dropped` is a DECLARED data loss, and the reader could not read it.
///
/// Past its disk cap the tap drops the oldest sealed segments and records that deliberately
/// twice — in-band with the count and seq range, and structurally as a gap among the segments
/// that did arrive. A consumer that reads neither is not honoring the contract it pins, and
/// would report coverage over a window it knows it is missing.
#[test]
fn a_declared_segment_drop_is_readable_rather_than_flattened_into_drift() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"segment_open\",\"incarnation\":\"1753142891447-4417\",\
         \"segment_seq\":9,\"at_ms\":{t}}}\n\
         {{\"kind\":\"segments_dropped\",\"incarnation\":\"1753142891447-4417\",\
         \"count\":3,\"first\":\"seg-1753142891447-4417-4.jsonl\",\
         \"last\":\"seg-1753142891447-4417-6.jsonl\",\"first_seq\":4,\
         \"last_seq\":6,\"at_ms\":{t}}}\n"
    );
    let report = ingest_bytes(
        &lake,
        "f1",
        "seg-1753142891447-4417-9.jsonl",
        content.as_bytes(),
    )
    .expect("ingest");
    assert_eq!(
        report.unknown_kinds, 0,
        "declared loss is not unknown drift"
    );

    let decoded =
        analyzer_ingest::decode_segment("seg-1753142891447-4417-9.jsonl", "f1", content.as_bytes())
            .expect("decode");
    let dropped: serde_json::Value = decoded
        .shaped
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("shaped row parses"))
        .find(|r| r["kind"] == "segments_dropped")
        .expect("the declaration is a shaped row");
    assert_eq!(dropped["incarnation"], "1753142891447-4417");
    assert_eq!(
        (dropped["first_seq"].as_u64(), dropped["last_seq"].as_u64()),
        (Some(4), Some(6)),
        "the tap said which segments it dropped; the seq range must arrive as INTEGERS, because \
         reconciling a declared loss against a structural gap is a comparison over numbers and \
         the filenames it can be parsed from are the transport's business, not this reader's"
    );
    assert_eq!(dropped["extra"]["count"], "3");
    assert_eq!(dropped["extra"]["first"], "seg-1753142891447-4417-4.jsonl");
}

/// A genuinely unmodelled kind still counts. The point of modelling the delivery family was to
/// let this counter mean something again, so it has to still fire.
#[test]
fn an_actually_unknown_kind_still_counts_after_the_delivery_family_landed() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"segment_open\",\"incarnation\":\"i\",\"segment_seq\":0,\
         \"at_ms\":{t}}}\n\
         {{\"kind\":\"some_future_kind\",\"at_ms\":{t}}}\n"
    );
    let report = ingest_bytes(&lake, "f1", "seg-i-0.jsonl", content.as_bytes()).expect("ingest");
    assert_eq!(
        report.unknown_kinds, 1,
        "exactly the future kind — the segment_open must not be in this number, and the \
         future kind must not be missing from it"
    );
}

/// The reader's sealed-segment prefix matches the vendored contract.
#[test]
fn the_sealed_prefix_matches_the_vendored_contract() {
    let layout = repo_root().join("contract/inference-events/conformance/delivery/layout.json");
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&layout).expect("read layout.json"))
            .expect("layout.json parses");
    assert_eq!(
        analyzer_segments::SEALED_PREFIX,
        v["sealed_prefix"].as_str().expect("sealed_prefix"),
        "the reader's sealed-segment prefix has drifted from the tap's contract"
    );
}

/// A batch's incarnation is VERIFIED, not inferred from the first header it happens to find.
///
/// Stamping the batch identity onto every row rests on one assumption: a batch is one segment
/// file, so it has one writer. That is true by construction today — batch identity is a hash of
/// the content — but it is an assumption about the TRANSPORT, and `layout.json` is explicit
/// that correctness must not bind to one. A shipper that concatenated two segments would put two
/// headers in one batch, and first-one-wins would file the second half's rows under a run whose
/// key, configuration and continuity sequence are all different: a silent misattribution, in the
/// exact dimension that decides whether two records can be compared at all.
#[test]
fn two_headers_in_one_batch_refuse_rather_than_pick_one() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let content = format!(
        "{{\"kind\":\"segment_open\",\"incarnation\":\"a\",\"segment_seq\":0,\
         \"at_ms\":{t}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n\
         {{\"kind\":\"segment_open\",\"incarnation\":\"b\",\"segment_seq\":0,\
         \"at_ms\":{t}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p2\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n"
    );
    let err = ingest_bytes(&lake, "f1", "seg-a-0.jsonl", content.as_bytes())
        .expect_err("two writers in one batch");
    assert!(
        matches!(
            &err,
            analyzer_ingest::Error::Segment(analyzer_segments::Error::Malformed(m))
                if m.contains("segment_open records in one batch")
        ),
        "the refusal must name the cause; got {err}"
    );
}

/// A batch CLAIMING to be a sealed segment must carry its identity; one making no such claim
/// need not.
///
/// The contract puts a segment's identity in its first record, so a `seg-` name without one is
/// corruption rather than data. But this reader ingests any batch, and two legitimate inputs
/// have no delivery framing at all: an archive or hand-assembled corpus, and the
/// `orphan-<ms>.jsonl` producers before contract 1.11 shipped when a crash took the header
/// (admitted under spec 4.1's tolerance; a 1.11 producer discards and declares instead). Those
/// get the empty incarnation — a real state that reads as "not comparable" wherever key space
/// is resolved, not a default standing in for one.
#[test]
fn only_a_batch_claiming_to_be_a_segment_must_carry_a_header() {
    let lake = common::fresh_lake("reader");
    let t = now_ms();
    let store = format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\
         \"n_tokens\":4,\"at_ms\":{t}}}\n"
    );

    let err = ingest_bytes(&lake, "f1", "seg-a-0.jsonl", store.as_bytes())
        .expect_err("a seg- name needs a header");
    assert!(
        matches!(
            &err,
            analyzer_ingest::Error::Segment(analyzer_segments::Error::Malformed(m))
                if m.contains("no segment_open")
        ),
        "the refusal must name the cause; got {err}"
    );

    // An orphan, and a plain archive name: both ingest, both unattributed.
    for (label, name) in [
        ("orphan", "orphan-1700000000000.jsonl"),
        ("archive", "records.jsonl"),
    ] {
        // A fleet's log of its own: a lineage's input log is one fleet's.
        let lake = common::fresh_lake(label);
        let report = ingest_bytes(&lake, label, name, store.as_bytes())
            .unwrap_or_else(|e| panic!("{label} must ingest: {e}"));
        assert_eq!(report.records_inserted, 1, "{label}");
        let decoded = analyzer_ingest::decode_segment(name, label, store.as_bytes())
            .unwrap_or_else(|e| panic!("{label} must decode: {e}"));
        let row: serde_json::Value =
            serde_json::from_str(decoded.shaped.lines().next().expect("a shaped row"))
                .expect("shaped row parses");
        assert_eq!(
            row["incarnation"], "",
            "{label} belongs to no sequence, so its incarnation is empty rather than guessed"
        );
    }
}
