// SPDX-License-Identifier: AGPL-3.0-only

//! The dated destination layout, asserted against the vendored contract — the assertion
//! whose absence let two conformant components diverge silently (the tap shipped
//! node-first keys, discovery listed date-first, both suites green, a real install
//! ingesting nothing;, spec 4.4). `storage::dated`'s header has always CITED
//! `conformance/delivery/layout.json` as the layout's source; this test is what makes the
//! citation enforced rather than claimed, on the reader's side of the seam. The tap's
//! `tests/delivery_layout.rs` holds the producer's half against the same bytes.

use analyzer_lake::dated;
use std::path::PathBuf;

fn layout() -> serde_json::Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../contract/inference-events/conformance/delivery/layout.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("read vendored layout.json"))
        .expect("layout.json parses")
}

/// Both sides pin the same stated layout literal: if `layout.json` moves, this fails
/// first, and the discovery arithmetic moves with it in one commit — the same both-sides
/// pin the tap's suite holds.
#[test]
fn the_contract_states_the_layout_this_discovery_walks() {
    let l = layout();
    assert_eq!(
        l["dest_key_layout"].as_str().expect("dest_key_layout"),
        "<prefix>/<utc yyyy-mm-dd>/<utc hh>/<node>/<sealed segment file>"
    );
}

/// The arithmetic walks a layout-shaped key exactly as stated: the floor lands on the
/// `<yyyy-mm-dd>/<hh>` segment pair under the prefix, the margin steps whole hours
/// through it, and the windowed enumeration renders the same two segments.
#[test]
fn the_discovery_arithmetic_matches_the_stated_layout() {
    let key = "fleet/2026-08-21/14/node-a/seg-1785000000000-42-7.parquet";
    assert_eq!(
        dated::hour_floor(key, 0).as_deref(),
        Some("fleet/2026-08-21/14"),
        "the floor is the layout's date and hour segments under its prefix"
    );
    assert_eq!(
        dated::hour_floor(key, 24).as_deref(),
        Some("fleet/2026-08-20/14"),
        "the margin steps whole hours through the same two segments"
    );
    // The node segment is BELOW the hour, so per-node keys share hour directories and one
    // bounded listing covers every node — the property node-first destroys.
    assert!(key.starts_with("fleet/2026-08-21/14/node-a/"));

    // The file segment is the shipped form the candidate rule admits.
    let l = layout();
    let shipped: Vec<&str> = l["shipped_suffixes"]
        .as_array()
        .expect("shipped_suffixes")
        .iter()
        .map(|v| v.as_str().expect("suffix is a string"))
        .collect();
    assert!(
        shipped.contains(&dated::SHIPPED_SUFFIX),
        "discovery's suffix is one the contract ships"
    );
    assert!(dated::is_shipped_key(key));
}

/// The contract's stream component, and every documented prefix ending in it: the tap ships
/// the KV-cache stream to `<root>/kv/`, the analyzer's prefix is that destination, and the two
/// stay one thing by each side asserting its own half against the vendored layout (the tap:
/// its example destination; here: the quickstart's examples, the installer's usage, the chart's
/// values). A prefix documented without the component would point an install one level up.
#[test]
fn the_documented_prefix_ends_in_the_contract_s_stream_component() {
    let l = layout();
    let stream = l["stream"].as_str().expect("the layout names its stream");
    assert_eq!(stream, "kv");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let quickstart = std::fs::read_to_string(root.join("docs/quickstart.md")).expect("quickstart");
    let examples: Vec<&str> = quickstart
        .lines()
        .filter(|l| l.contains("--bucket-prefix ") && !l.contains('`'))
        .collect();
    assert!(
        !examples.is_empty(),
        "the quickstart documents the prefix by example"
    );
    for line in &examples {
        let value = line
            .split("--bucket-prefix ")
            .nth(1)
            .expect("a value")
            .trim();
        assert!(
            value.ends_with(&format!("{stream}/")),
            "a documented prefix ends in the stream component with its own slash: {line}"
        );
    }
    let usage = std::fs::read_to_string(root.join("packaging/vm/install.sh")).expect("installer");
    assert!(
        usage.contains(&format!("stream's component {stream}/")),
        "the installer's usage names the component"
    );
    let values =
        std::fs::read_to_string(root.join("charts/infertap-analyzer/values.yaml")).expect("chart");
    assert!(
        values.contains(&format!("stream's component `{stream}/`")),
        "the chart's values name the component"
    );
}
