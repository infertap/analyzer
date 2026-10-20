// SPDX-License-Identifier: AGPL-3.0-only

//! Live round-trip against a real S3-API bucket.
//!
//! `#[ignore]` and env-gated, on the same rule the engine-backed suites follow: a test that needs
//! a credential and a network is not part of `cargo test --workspace`. Run it with a bucket:
//!
//! ```text
//! INFERTAP_TEST_S3_URL=s3://bucket/prefix cargo test -p analyzer-objectstore -- --ignored
//! ```
//!
//! Credentials come from the provider chain, never from arguments — which is the property under
//! test as much as the round trip is.
use analyzer_objectstore::{ObjectStore as _, S3Store};

#[test]
#[ignore = "live: a real AWS role. INFERTAP_TEST_S3_URL=s3://bucket/prefix INFERTAP_TEST_S3_PROVIDER_CHAIN=1"]
fn put_then_list_then_get_round_trips_through_the_provider_chain() {
    // Opt-in by name, because this test's identity is the PROVIDER CHAIN: opened with no
    // static credential, it must resolve a real instance role, which only a real AWS box
    // can offer. On a CI runner the chain walks to the instance-metadata address and a
    // different cloud's metadata service answers — the battery measured Azure replying
    // 411 to an IMDS token request. The static-credential round trip is bucket_live's to
    // prove there; this stays the hand-run verification it was built as (clean-room,
    // ). And env-gated means SKIP, never panic — the guard below once said
    // "required" and panicked, the same skip-law break the battery found twice already.
    if std::env::var("INFERTAP_TEST_S3_PROVIDER_CHAIN").is_err() {
        eprintln!("skipped: INFERTAP_TEST_S3_PROVIDER_CHAIN not set — the provider chain needs a real instance role");
        return;
    }
    let Ok(url) = std::env::var("INFERTAP_TEST_S3_URL") else {
        eprintln!("skipped: INFERTAP_TEST_S3_URL not set — this test needs a live bucket");
        return;
    };
    // `None` on purpose: the provider chain is the thing under test as much as the round trip.
    let store = S3Store::open(&url, None).expect("a store");

    let key = format!("objectstore-selftest/probe-{}.txt", std::process::id());
    let body = format!("infertap-objectstore-{}", std::process::id());

    store.put(&key, body.as_bytes()).expect("put");

    let listed = store.list("objectstore-selftest/").expect("list");
    let found = listed.iter().find(|m| m.key == key).unwrap_or_else(|| {
        panic!("wrote {key} but the listing did not report it: {listed:?}");
    });
    assert_eq!(
        found.size,
        body.len() as u64,
        "the listing's size must be the object's own"
    );

    let got = store.get(&key).expect("get");
    assert_eq!(
        String::from_utf8_lossy(&got),
        body,
        "a transport that mutates objects cannot carry telemetry"
    );
}
