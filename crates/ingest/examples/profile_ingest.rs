// SPDX-License-Identifier: AGPL-3.0-only

//! Where the per-record cost of ingest goes, on both of its paths: the projected columnar walk
//! a Parquet segment takes, and the full decode a JSONL segment takes.
//!
//! Run against a REAL sealed segment, not a fixture:
//!
//! ```text
//! cargo run --release -p analyzer-ingest --example profile_ingest -- <seg.parquet>
//! ```
//!
//! Measures only what needs no engine and no bucket. The end-to-end rate on a rig is a
//! PIPELINE rate and cannot say how much of it is our CPU; this says.
//!
//! The fast path never decodes a segment to shaped NDJSON: it takes one projected columnar walk
//! for the arrivals fold and the day/span facts. So the phases below are two COLUMNS, not one
//! list: the fast path's cost, and the shaped reference path's cost, over the same bytes.
use std::time::{Duration, Instant};

use analyzer_ingest::FAST_WALK_PROJECTION;

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// A profiler prints megabytes and per-record ratios of drill-scale numbers; the
// precision the lints guard is far past anything these figures state.
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::too_many_lines)] // one linear read of "what does a segment cost"
fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: profile_ingest <seg.parquet>");
    let bytes = std::fs::read(&path).expect("read segment");
    let mb = bytes.len() as f64 / 1_048_576.0;

    // Phase 0 — the content hash. Unavoidable on either path: it is the idempotency key,
    // and the fast path pays it inside the projected decode.
    let t = Instant::now();
    let id = analyzer_segments::content_batch_id(&bytes);
    let t_hash = t.elapsed();

    // ---- the FAST path: one projected columnar walk ----
    let t = Instant::now();
    let projected = analyzer_segments::columnar::decode_segment_projected(
        &path,
        "profile",
        bytes.clone(),
        analyzer_segments::columnar::Projection {
            columns: FAST_WALK_PROJECTION,
            ..analyzer_segments::columnar::Projection::default()
        },
    )
    .expect("projected decode");
    let t_fast_decode = t.elapsed();

    // The one fold the fast path runs over those rows: three kinds, at_ms required — the
    // same rules `cache_facts_of` applies, which is what the equivalence pin holds it to.
    let t = Instant::now();
    let mut days = std::collections::BTreeSet::new();
    let mut span: Option<(f64, f64)> = None;
    let mut cache_rows = 0u64;
    for row in &projected.rows {
        let Some(obj) = row.as_object() else { continue };
        let Some(kind) = obj.get("kind").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !matches!(kind, "store" | "evict" | "clear") {
            continue;
        }
        let Some(at_ms) = obj.get("at_ms").and_then(serde_json::Value::as_f64) else {
            continue;
        };
        cache_rows += 1;
        span = Some(span.map_or((at_ms, at_ms), |(lo, hi)| (lo.min(at_ms), hi.max(at_ms))));
        #[allow(clippy::cast_possible_truncation)]
        days.insert((at_ms / 86_400_000.0).floor() as i64);
    }
    let t_fast_fold = t.elapsed();

    // ---- the SHAPED reference path: full decode to NDJSON, then two walks over it ----
    let t = Instant::now();
    let decoded = analyzer_segments::decode_segment(&path, "profile", &bytes).expect("decode");
    let t_ref_decode = t.elapsed();
    let shaped_mb = decoded.shaped.len() as f64 / 1_048_576.0;

    // Walk one: the lifecycle/arrivals fold re-parses every line.
    let t = Instant::now();
    let mut seen = 0u64;
    for line in decoded.shaped.lines().filter(|l| !l.is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).expect("reparse");
        if row.get("kind").is_some() {
            seen += 1;
        }
    }
    let t_ref_walk1 = t.elapsed();

    // Walk two: cache_facts_of re-parses, clones a field subset, reserialises.
    let t = Instant::now();
    let mut deltas = String::new();
    for line in decoded.shaped.lines().filter(|l| !l.is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).expect("reparse");
        let Some(obj) = row.as_object() else { continue };
        let Some(kind) = obj.get("kind").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if !matches!(kind, "store" | "evict" | "clear") {
            continue;
        }
        let mut out = serde_json::Map::new();
        for (k, v) in obj {
            out.insert(k.clone(), v.clone());
        }
        deltas.push_str(&serde_json::Value::Object(out).to_string());
        deltas.push('\n');
    }
    let t_ref_walk2 = t.elapsed();

    let n = decoded.records as f64;
    let per = |d: Duration| d.as_secs_f64() * 1e6 / n;

    println!("segment: {path}");
    println!("  {mb:.2} MB parquet, {n} records, batch_id {}", &id[..12]);
    println!(
        "  projected rows {} (unknown kinds {}), cache rows {cache_rows}, days {}",
        projected.rows.len(),
        projected.unknown_kinds,
        days.len()
    );
    println!(
        "  shaped {shaped_mb:.2} MB ({:.0} B/rec), delta text {:.2} MB, lines walked {seen}\n",
        decoded.shaped.len() as f64 / n,
        deltas.len() as f64 / 1_048_576.0
    );

    let row = |name: &str, d: Duration| {
        println!("{:<38} {:>9.1} {:>12.3}", name, ms(d), per(d));
    };
    println!("{:<38} {:>9} {:>12}", "phase", "ms", "us/record");
    println!("-- fast path (D3) ------------------------------------------------");
    row("  projected columnar walk", t_fast_decode);
    row("  the one fold (arrivals + day/span)", t_fast_fold);
    let fast = t_fast_decode + t_fast_fold;
    row("  TOTAL analyzer-side", fast);
    println!("-- shaped reference path -----------------------------------------");
    row("  content_batch_id (SHA-256)", t_hash);
    row("  full decode + shape->text", t_ref_decode);
    row("  walk 1: lifecycle re-parse", t_ref_walk1);
    row("  walk 2: cache facts re-parse+emit", t_ref_walk2);
    let reference = t_hash + t_ref_decode + t_ref_walk1 + t_ref_walk2;
    row("  TOTAL analyzer-side", reference);

    println!();
    println!(
        "analyzer-side speedup, fast vs shaped: {:.2}x",
        reference.as_secs_f64() / fast.as_secs_f64()
    );
    println!(
        "  fast path   {:>10.0} rec/s single core",
        n / fast.as_secs_f64()
    );
    println!(
        "  shaped path {:>10.0} rec/s single core",
        n / reference.as_secs_f64()
    );
    // The number that decides whether the arrivals derivation also moves to SQL: if the
    // fold dominates the fast path, a second drift pair might be worth buying. If the
    // columnar READ dominates, moving the fold buys nothing and costs a ratchet.
    println!(
        "\nfast path split: columnar read {:.1}%, the fold {:.1}%",
        100.0 * t_fast_decode.as_secs_f64() / fast.as_secs_f64(),
        100.0 * t_fast_fold.as_secs_f64() / fast.as_secs_f64()
    );
}
