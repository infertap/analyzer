// SPDX-License-Identifier: AGPL-3.0-only

//! **One real Parquet segment, from a real bucket, through the transport we own.**
//!
//! Deliberately not a mock: the object is fetched by `analyzer-objectstore`, decoded by the
//! Parquet reader in `crates/segments`, and committed to the input log. The engine is not on the
//! path.
//!
//! The second cycle is the other half of the claim: `list_after` reaches S3's own `start-after`,
//! so a steady-state cycle enumerates nothing rather than listing everything and discarding it.
//!
//! `#[ignore]` and env-gated, like every test that needs a credential and a network:
//!
//! ```text
//! INFERTAP_TEST_S3_URL=s3://bucket/ INFERTAP_TEST_S3_PREFIX=prefix/ \
//!   cargo test -p analyzer-ingest --test ingest_live -- --ignored
//! ```
mod common;

use analyzer_objectstore::S3Store;

#[test]
#[ignore = "live: needs INFERTAP_TEST_S3_URL and a real segment under its prefix"]
fn a_real_segment_ingests_through_the_object_store() {
    // Env-gated means SKIP without the env, never panic: a credential-less `--ignored`
    // sweep must be able to run the hermetic integration set without this test failing
    // it for the crime of having no live bucket.
    let Ok(url) = std::env::var("INFERTAP_TEST_S3_URL") else {
        eprintln!("skipped: INFERTAP_TEST_S3_URL not set — this test needs a live bucket");
        return;
    };
    // The prefix names REAL captured data (the E2 evidence shape); a bucket without it
    // (the battery's fresh bucket) is a skip by this test's own rule above, which the expect that
    // stood here broke the first time any environment carried the URL without the data.
    let Ok(prefix) = std::env::var("INFERTAP_TEST_S3_PREFIX") else {
        eprintln!(
            "skipped: INFERTAP_TEST_S3_PREFIX not set — this test needs a real staged segment"
        );
        return;
    };
    let store = S3Store::open(&url, None).expect("a store");

    let lake = common::fresh_lake("ingest-live");

    let r = analyzer_ingest::bucket::ingest_bucket(&lake, &store, "live-fleet", &prefix, false)
        .expect("ingest cycle");
    println!(
        "  files_seen={} ingested={} records={} bytes={}",
        r.files_seen, r.files_ingested, r.records_inserted, r.bytes_seen
    );
    assert_eq!(r.files_ingested, 1, "the one staged segment must land");
    assert!(
        r.records_inserted > 0,
        "a segment with no records is not a segment"
    );

    // Second cycle: the watermark seek must skip it entirely.
    let r2 = analyzer_ingest::bucket::ingest_bucket(&lake, &store, "live-fleet", &prefix, false)
        .expect("second cycle");
    assert_eq!(
        (r2.files_seen, r2.files_ingested),
        (0, 0),
        "a steady-state cycle touches no object at all"
    );
}
