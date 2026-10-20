// SPDX-License-Identifier: AGPL-3.0-only

//! The standing differential audit against a real database.
//!
//! Each test authors a raw stream whose correct grading is known from the tap's own
//! semantics, runs the re-derivation, and asserts the classes exactly. The first four are
//! the ruling's named no-false-alarm flows — evict-before-store across a batch boundary,
//! restart mid-flight, re-store after eviction, offload tier flows — because a permanent
//! alarm earns its permanence by never crying wolf; the rest are the two defect gradings
//! and the surfacing through self-health.

use analyzer_analysis::differential;
use serde_json::{json, Value};

// A RELATIVE base, hours into the rebase day — the harness adds `rebase_offset()`, so an
// absolute epoch here would land the stream decades in the future, where dated discovery
// rightly finds nothing: a segment's key hour is its ship hour, and nothing ships from
// the future.
const T0: f64 = 3_600_000.0;
/// Two tap incarnations, in the layout convention (`<run_start_unix_ms>-<pid>`).
const RUN1: &str = "1785153600000-100";
const RUN2: &str = "1785153900000-200";

fn store(instance: &str, block: &str, at: f64, content: Option<&str>, inc: &str) -> Value {
    let mut r = json!({
        "kind": "store", "instance_id": instance, "block_id": block,
        "n_tokens": 16, "tier": "GPU", "at_ms": at, "incarnation": inc,
    });
    if let Some(c) = content {
        r["content_id"] = c.into();
    }
    r
}

fn evict(instance: &str, block: &str, at: f64, content: Option<&str>, inc: &str) -> Value {
    let mut r = json!({
        "kind": "evict", "instance_id": instance, "block_id": block,
        "tier": "GPU", "at_ms": at, "incarnation": inc,
    });
    if let Some(c) = content {
        r["content_id"] = c.into();
    }
    r
}

fn audit_over(records: &[Value], label: &str) -> differential::AuditOutcome {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let source = harness::seed_source(&engine, label, records, offset, label);
    let window = harness::cache_window(&engine, label);
    analyzer_pipeline::read::audit(&source, label, &window).expect("audit")
}

fn totals(o: &differential::AuditOutcome) -> (u64, u64, u64, u64, u64, u64) {
    (
        o.audited(),
        o.instances.iter().map(|i| i.agreed).sum(),
        o.mismatched(),
        o.underresolved(),
        o.indeterminate(),
        o.unjoined(),
    )
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_re_store_after_eviction_agrees_twice() {
    // The tap's delete-on-evict path in miniature: store, evict (resolved and released),
    // re-store (a fresh promise), evict again. Both evicts grade against their own store.
    let o = audit_over(
        &[
            store("i0", "b1", T0, Some("c1"), RUN1),
            evict("i0", "b1", T0 + 100.0, Some("c1"), RUN1),
            store("i0", "b1", T0 + 200.0, Some("c1"), RUN1),
            evict("i0", "b1", T0 + 300.0, Some("c1"), RUN1),
        ],
        "re-store",
    );
    assert_eq!(totals(&o), (2, 2, 0, 0, 0, 0));
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_offload_flow_grades_both_tiers_against_the_latest_store() {
    // The same block resident on GPU and CPU: content identity is tier-invariant, so both
    // evicts agree with the most recent store whichever tier wrote it.
    let mut gpu_evict = evict("i0", "b1", T0 + 200.0, Some("c1"), RUN1);
    gpu_evict["tier"] = "GPU".into();
    let mut cpu_store = store("i0", "b1", T0 + 100.0, Some("c1"), RUN1);
    cpu_store["tier"] = "CPU".into();
    let mut cpu_evict = evict("i0", "b1", T0 + 300.0, Some("c1"), RUN1);
    cpu_evict["tier"] = "CPU".into();
    let o = audit_over(
        &[
            store("i0", "b1", T0, Some("c1"), RUN1),
            cpu_store,
            gpu_evict,
            cpu_evict,
        ],
        "offload",
    );
    assert_eq!(totals(&o), (2, 2, 0, 0, 0, 0));
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_evict_before_any_store_is_unjoined_never_an_alarm() {
    // Evict-before-store across a batch boundary: the evict arrived first (a warm cache the
    // tap never saw stored), the store only after. Strictly-prior joining leaves the early
    // evict ungraded rather than graded against a future the tap could not have seen —
    // and the same rule covers the same-instant case, where one batch stamps store and
    // evict with a single timestamp that cannot establish order.
    let o = audit_over(
        &[
            evict("i0", "b1", T0, None, RUN1),
            store("i0", "b1", T0 + 100.0, Some("c1"), RUN1),
            evict("i0", "b2", T0 + 200.0, None, RUN1),
            store("i0", "b2", T0 + 200.0, Some("c2"), RUN1),
        ],
        "evict-first",
    );
    assert_eq!(totals(&o), (2, 0, 0, 0, 0, 2));
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_tap_restart_mid_flight_is_indeterminate_never_underresolved() {
    // Store written by one tap run, evict by the next: the fresh bridge legitimately
    // resolves nothing it did not see stored, so the absent content_id is expected, not a
    // lost promise. Unattributed segments (empty incarnation) get the same grade — an
    // unknown provenance cannot support a same-run claim in either direction.
    let o = audit_over(
        &[
            store("i0", "b1", T0, Some("c1"), RUN1),
            evict("i0", "b1", T0 + 100.0, None, RUN2),
            store("i0", "b2", T0 + 200.0, Some("c2"), ""),
            evict("i0", "b2", T0 + 300.0, None, ""),
        ],
        "restart",
    );
    assert_eq!(totals(&o), (2, 0, 0, 0, 2, 0));
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn matching_absence_is_agreement_and_scope_is_the_full_holder() {
    // A store that could not chain makes an evict that cannot resolve: both absent, agreed.
    // And the join is scoped to (instance, dp_rank, group_idx): the same engine hash on
    // another rank is another cache, so rank 1's evict finds no rank-1 store and stays
    // unjoined rather than borrowing rank 0's resolution.
    let mut rank1_evict = evict("i0", "b1", T0 + 200.0, None, RUN1);
    rank1_evict["dp_rank"] = 1.into();
    let o = audit_over(
        &[
            store("i0", "b1", T0, None, RUN1),
            evict("i0", "b1", T0 + 100.0, None, RUN1),
            rank1_evict,
        ],
        "scope",
    );
    assert_eq!(totals(&o), (2, 1, 0, 0, 0, 1));
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_contradicted_resolution_alarms_naming_the_instance() {
    // The defect class the audit exists to catch: the evict carries an identity the raw
    // stream contradicts. Graded per instance, so the alarm names the offender and only
    // the offender.
    let o = audit_over(
        &[
            store("i0", "b1", T0, Some("c1"), RUN1),
            evict("i0", "b1", T0 + 100.0, Some("WRONG"), RUN1),
            store("i1", "b1", T0, Some("c1"), RUN1),
            evict("i1", "b1", T0 + 100.0, Some("c1"), RUN1),
        ],
        "mismatch",
    );
    assert_eq!(totals(&o), (2, 1, 1, 0, 0, 0));
    let bad = &o.instances[0];
    assert_eq!((bad.instance.as_str(), bad.mismatched), ("i0", 1));
    assert_eq!(o.naming(|i| i.mismatched), "i0 (1)");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_lost_promise_within_one_run_is_underresolved() {
    // Same tap run stored it with an identity; the evict carries none. The tap held the
    // promise and lost it (capacity refusal, an over-released clear, a bridge defect) —
    // declared-missing, so it warns rather than alarms, but never passes silently.
    let o = audit_over(
        &[
            store("i0", "b1", T0, Some("c1"), RUN1),
            evict("i0", "b1", T0 + 100.0, None, RUN1),
        ],
        "underresolved",
    );
    assert_eq!(totals(&o), (1, 0, 0, 1, 0, 0));
}
