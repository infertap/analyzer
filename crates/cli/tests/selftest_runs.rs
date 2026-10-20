// SPDX-License-Identifier: AGPL-3.0-only

//! **The deployment-conformance check, actually executed.**
//!
//! `selftest` is the shipped proof that a customer's install derives the contract's bytes. Until
//! nothing ran it. The only test on the module compared the embedded corpus LIST
//! against the repository — which passes whether or not the check works — and the check did not
//! work: it truncated `coresidency_spans`, a table on the schema's DEAD list that has never
//! existed, so the very first statement failed and every invocation returned an environment
//! error. It surfaced by running the binary, not by running the suite.
//!
//! **The typo was the small half.** A shipped check nothing exercises is the same shape as a
//! golden that outlives its fixture: it reports success forever because it is never asked. This
//! file is the fix for the shape.
//!
//! It is deliberately the WHOLE thing — session, migration, ingest, pass, derive, byte-compare
//! against the goldens embedded in the binary — because every one of those is what a customer
//! runs, and a test that stubbed any of them would be proving something else.

use analyzer_serving_clickhouse::engine::testing::engine_bin;
use std::path::PathBuf;

fn scratch(label: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("analyzer-selftest-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// Every embedded case derives its shipped bytes, through the SHIPPED BINARY.
///
/// **It runs the executable rather than calling the function**, because that is what a customer
/// runs — argument parsing, the exit code, and the message on failure are all part of the check
/// and a direct call would prove none of them. The CLI has no library target either, so this is
/// also the only way in.
///
/// The exit code carries the distinction `selftest` draws in its own signature: an environment
/// failure ("this box cannot run the check") and a contract failure ("this box does not conform")
/// are different phone calls, so this test reports the output rather than collapsing both to red.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine cli"]
fn selftest_runs_against_the_pinned_engine() {
    let work = scratch("run");
    let out = scratch("out");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_infertap-analyzer"))
        .arg("selftest")
        .args(["--scratch", &work.to_string_lossy()])
        .args(["--out", &out.to_string_lossy()])
        .args(["--clickhouse", &engine_bin().to_string_lossy()])
        .output()
        .expect("spawn the analyzer binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "selftest failed
--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    // A pass over zero cases is not a proof of anything, and the success message is where the
    // count is stated, so the count is asserted rather than assumed. PARSED, not pinned: this
    // line once said "20 conformance cases" verbatim, the corpus grew to 21, and the literal
    // sat stale for as long as nothing ran this suite — the exact count is the
    // `embedded_corpus_matches_the_repository` ratchet's job, and a second pin here can only
    // rot. This assertion guards the one thing its comment claims: not zero.
    let cases: u64 = stdout
        .split(" conformance cases")
        .next()
        .and_then(|before| before.rsplit(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("selftest reported no parsable case count:\n{stdout}"));
    assert!(
        cases > 0,
        "selftest passed over zero cases, which proves nothing:\n{stdout}"
    );

    std::fs::remove_dir_all(&work).ok();
    std::fs::remove_dir_all(&out).ok();
}
