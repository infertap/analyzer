// SPDX-License-Identifier: AGPL-3.0-only

//! **The derived-facts document: every figure the product prints, for one window.**
//!
//! **Production code, because three callers need the same document and two of them ship.** The
//! repository's snapshot suites pin it against committed goldens; `selftest` carries goldens
//! inside the binary and re-derives them on the customer's host, proving the DEPLOYMENT conforms
//! rather than the source. A second definition would drift, and the drift would read as a
//! semantics change in whichever caller noticed second.
//!
//! **The artifact is the figures themselves**, not a sentence written about them: pinning a
//! verdict would pin the figures at one remove. Nothing in here knows what a verdict or a dollar
//! is.

use crate::read;
use crate::Error;
use analyzer_analysis::events::EventSource;
use analyzer_analysis::{reuse, Params, Window};
use serde_json::{json, Value};

/// Every derived figure for one case, in a stable shape.
///
/// Long on purpose: the enumeration IS the artifact. A figure with no line here is a figure the
/// snapshot does not cover, so splitting this for length would hide exactly what it exists to
/// make visible.
///
/// Floats ride as their bit patterns where they are identities (timestamps, masses) and as plain
/// numbers where a reader wants to read them. Sorted everywhere a set is meant, because neither
/// path promises an order and asserting one would pin a property no caller has.
/// # Errors
/// Any source read failing. **Returned rather than panicked**, because `selftest` carries this
/// into a customer's deployment: a transient failure there must read as a failed check
/// with a cause, not as a crash in a support ticket.
#[allow(clippy::too_many_lines)] // one document, assembled once, in the order it is read
pub fn derive(source: &dyn EventSource, fleet: &str, w: &Window) -> Result<Value, Error> {
    // One derivation, one memo: the ~23 adapter calls below re-ask five distinct load shapes,
    // and each shape should run discovery and decode once. The memo lives exactly as long as
    // this call (`memo::MemoSource`), so nothing here can serve a stale answer to a later one.
    let memo = crate::memo::MemoSource::new(source);
    let source: &dyn EventSource = &memo;
    let params = Params::default();

    let mut ivs: Vec<Value> = read::residency_intervals(source, fleet, w)?
        .iter()
        .map(|i| {
            json!({
                "holder": [i.key.instance, i.key.dp_rank, i.key.group_idx],
                "block": i.key.identity, "key_epoch": i.key.key_epoch, "tier": i.key.tier,
                "open_ms": i.open_ms, "close_ms": i.close_ms,
                "close_reason": format!("{:?}", i.close_reason),
                "readmits": i.readmits,
            })
        })
        .collect();
    ivs.sort_by_key(std::string::ToString::to_string);

    let c = read::classify(source, fleet, w, &params)?;
    let mut stores: Vec<Value> = c
        .stores
        .iter()
        .map(|s| {
            json!({
                "holder": [s.instance, s.dp_rank, s.group_idx],
                "block": s.block, "key_epoch": s.key_epoch, "at_ms": s.at_ms,
                "n_tokens": s.n_tokens, "class": s.class.as_str(), "gap_ms": s.gap_ms,
            })
        })
        .collect();
    stores.sort_by_key(std::string::ToString::to_string);

    let mut residencies: Vec<Value> = c
        .residencies
        .iter()
        .map(|r| {
            json!({"instance": r.key.instance, "close_ms": r.close_ms,
                   "duration_ms": r.duration_ms, "opener_class": r.opener_class.as_str()})
        })
        .collect();
    residencies.sort_by_key(std::string::ToString::to_string);

    let model = read::uncertainty_model(source, fleet, w, &params)?;
    let cov = model.coverage(None);

    let tree = read::block_tree(source, fleet, w)?;
    let mut edges: Vec<Value> = tree
        .observed_edges()
        .iter()
        .map(|(c, p)| json!([c, p]))
        .collect();
    edges.sort_by_key(std::string::ToString::to_string);

    let model = crate::read::uncertainty_model(source, fleet, w, &params)?;
    let opens = source.segment_opens_at_retention_horizon(fleet)?;
    let ledger = crate::degradation_io::ledger(source, fleet, w, &model, &opens)?;
    let mut entries: Vec<Value> = ledger
        .in_window(w)
        .iter()
        .map(|d| {
            json!({"scope": format!("{:?}", d.scope), "cause": format!("{:?}", d.cause),
                   "bracket": [d.producer_bracket.start_ms, d.producer_bracket.end_ms],
                   "engine_extent": d.engine_extent, "incidents": d.incidents})
        })
        .collect();
    entries.sort_by_key(std::string::ToString::to_string);

    let (unqualified, recompute, first_insertion) = read::reuse_figures(source, fleet, w)?;
    let fig = |f: &reuse::Figure| {
        json!({"countable": f.countable, "records": f.records,
                                         "tokens": f.tokens})
    };

    Ok(json!({
        // **The document states the meaning it was derived under, and this is not decoration.**
        // Without it a golden cannot tell "the data changed" from "the parameters changed" — it
        // reports a diff either way and reports NOTHING when a parameter moves without shifting
        // any figure this corpus happens to reach (a `delta_skew_ms` of 1000 -> 5000 moves no
        // tap-corpus figure, so only the stated parameters show it).
        //
        // It is the same law the meaning pair holds on every keyed-history row, applied to the
        // artifact that pins the derivation rather than to the rows it derives.
        "meaning": {
            "semantics_version": analyzer_analysis::SEMANTICS_VERSION,
            "discriminator": analyzer_analysis::Discriminator::of(&params).as_str(),
            "staleness_grace_ms": analyzer_analysis::staleness::STALENESS_GRACE_MS,
            "delta_skew_ms": params.delta_skew_ms,
            "examined_history_ms": params.examined_history_ms,
        },
        "intervals": ivs,
        "classification": {
            "stores": stores,
            "eviction_waste_tokens": c.eviction_waste_tokens,
            "post_clear_tokens": c.post_clear_tokens,
            "bucket_counts": c.bucket_counts,
            "residencies": residencies,
        },
        "coverage": {
            "fraction": cov.fraction,
            "per_source_covered_ms": cov.per_source_covered_ms,
            "per_day": cov.per_day.iter().map(|d| json!({
                "day_ms": d.day_ms, "source": d.source,
                "covered_ms": d.covered_ms, "window_ms": d.window_ms})).collect::<Vec<_>>(),
        },
        "tree": {
            "nodes": tree.nodes.keys().collect::<Vec<_>>(),
            "observed_edges": edges,
            "dangling_edges": tree.dangling_edges(),
            "parent_conflicts": tree.parent_conflicts,
        },
        "degradation_ledger": entries,
        "liveness": {
            "run_verdicts": read::run_verdicts(source, fleet, w)
                ?.iter()
                .map(|(r, v)| json!([r, format!("{v:?}")])).collect::<Vec<_>>(),
            "fleet_health": format!("{:?}",
                read::fleet_health(source, fleet, w)?),
        },
        "narrowing": format!("{:?}",
            read::gap_span(source, fleet, w)?),
        "evict_audit": format!("{:?}", read::audit(source, fleet, w)?),
        "reuse": {
            "declaration": format!("{:?}", read::declaration(source, fleet, w)?),
            "unqualified": fig(&unqualified),
            "recompute": fig(&recompute),
            "first_insertion": fig(&first_insertion),
        },
    }))
}
