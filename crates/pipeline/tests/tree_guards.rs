// SPDX-License-Identifier: AGPL-3.0-only

//! Block-tree guards: cross-space edges, dangling parents, unknown ancestry.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::events::EventSource;
use analyzer_analysis::tree::BlockTree;
use serde_json::Value;

/// The cross-space chain, built explicitly, because it is the case the fixtures may not force.
///
/// A three-block chain where every block carries BOTH a content id and an engine id, and every
/// `parent_id` names the engine id. The nodes are therefore content-keyed while the edges arrive
/// engine-keyed — so a build that skips the bridge produces three roots with depth zero instead of
/// a chain of depth two. The bridge is what makes the difference, and nothing errors without
/// it: every edge simply reads as dangling.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_content_keyed_chain_resolves_its_engine_keyed_parents() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 3_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"e1","content_id":"c1",
                           "n_tokens":16,"at_ms":base}),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"e2","content_id":"c2",
                           "parent_id":"e1","n_tokens":16,"at_ms":base+10.0}),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"e3","content_id":"c3",
                           "parent_id":"e2","n_tokens":16,"at_ms":base+20.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "tree-chain");
    let w = harness::cache_window(&engine, "fx");

    // The property under test: the chain must be two links deep, expressed in content space.
    let ir = source.cache_events("fx", &w).expect("event IR");
    let tree = BlockTree::from_events(&ir);
    assert!(
        tree.nodes.contains_key("c3"),
        "nodes are content-keyed: {:?}",
        tree.nodes.keys()
    );
    assert_eq!(
        tree.proven_depth("c3"),
        2,
        "the engine-keyed parent edges must resolve into content space, or the chain reads as \
         three roots and R1 silently stops crediting them"
    );
    assert_eq!(
        tree.dangling_edges(),
        0,
        "no edge should dangle in a fully observed chain"
    );
}

/// **Unknown ancestry is not a root** — `docs/error-direction.md` B4, and until the harness gained
/// a `parent_unknown` column this rule had no corpus coverage at all.
///
/// `parent_id` absent asserts a root, whose ancestry claim is EMPTY and therefore vacuously
/// proven — R1 credits it. `parent_unknown: true` asserts the tap could not determine the parent,
/// which is an unproven claim, not an empty one. Collapsing the two manufactures contribution out
/// of ignorance, and it is invisible: the node count is identical either way and only the R1 mass
/// moves, upward.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_undeterminable_parent_is_never_read_as_a_root() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 4_500_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"rooted",
                           "n_tokens":16,"at_ms":base}),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"undeterminable",
                           "parent_unknown":true,"n_tokens":16,"at_ms":base+10.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "tree-unknown");
    let w = harness::cache_window(&engine, "fx");

    let ir = source.cache_events("fx", &w).expect("event IR");
    let tree = BlockTree::from_events(&ir);

    assert!(
        tree.is_root("rooted"),
        "an absent parent_id asserts a root: {:?}",
        tree.nodes.get("rooted")
    );
    assert!(
        !tree.is_root("undeterminable"),
        "an undeterminable parent is an UNPROVEN claim, not an empty one — reading it as a root \
         credits it under R1 and manufactures duplication out of ignorance: {:?}",
        tree.nodes.get("undeterminable")
    );
    assert_eq!(
        tree.proven_depth("undeterminable"),
        0,
        "unknown ancestry proves no links"
    );
}

/// A dangling parent: named, never stored in the window.
///
/// The edge is RETAINED as unprovable — never dropped, and never promoted to a root. Promoting it
/// is the failure the differential caught during the rewrite: a root's ancestry claim is vacuously
/// proven, so R1 credits it, and every dangling chain would start contributing to duplication.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_named_but_unobserved_parent_stays_dangling() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 4_000_000.0;

    let records: Vec<Value> = vec![serde_json::json!({
        "kind":"store","instance_id":"i1","block_id":"child","parent_id":"never_stored",
        "n_tokens":16,"at_ms":base
    })];
    let source = harness::seed_source(&engine, "fx", &records, offset, "tree-dangling");
    let w = harness::cache_window(&engine, "fx");
    let tree = BlockTree::from_events(&source.cache_events("fx", &w).expect("ir"));

    assert!(
        tree.has_dangling_parent("child"),
        "the claim is retained against a block nothing observed"
    );
    assert!(
        !tree.is_root("child"),
        "a named-but-unobserved parent is an UNPROVEN claim, not an empty one — reading it as a \
         root credits it under R1 and inflates duplication"
    );
    assert_eq!(
        tree.proven_depth("child"),
        0,
        "unproven ancestry proves no links"
    );
    assert_eq!(
        tree.dangling_edges(),
        1,
        "and it is counted, so the figure can state its own bound"
    );
}
