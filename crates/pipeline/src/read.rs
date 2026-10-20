// SPDX-License-Identifier: AGPL-3.0-only

//! The read adapters: load the Ordered Event IR for a window, hand it to a fold, return figures.
//!
//! **Every function here is a few lines, and that is the point.** The semantics all live on the
//! other side of the boundary — in `analysis`, over typed events, with no I/O in sight. What
//! remains is the question each derivation asks of its [`EventSource`]: which slice, at which
//! scope. The production source is `parquet_source::ParquetSource` — sealed segments through the
//! content cache — and a test hands in whatever second implementation it is proving against.
//!
//! **The scope choice is the only judgment these make**, and it is per-input rather than one
//! global clip. Cache facts arrive window-bounded. Lifecycle facts arrive clipped by the wall
//! margin their consumer needs — `EVIDENCE_CLIP_MARGIN_MS` where the output is BANKED (coverage
//! is keyed history and its evidence must sit inside the retention the daemon guarantees), and
//! `LIFECYCLE_CLIP_MARGIN_MS` where the output is snapshot-scoped. Getting that wrong here is
//! invisible in aggregate and moves a banked figure, which is why the two constants are named
//! rather than shared.

use crate::Error;
use analyzer_analysis::classify::Classification;
use analyzer_analysis::completeness::{
    UncertaintyModel, EVIDENCE_CLIP_MARGIN_MS, LIFECYCLE_CLIP_MARGIN_MS,
};
use analyzer_analysis::differential::AuditOutcome;
use analyzer_analysis::events::EventSource;
use analyzer_analysis::intervals::Interval;
use analyzer_analysis::narrowing::GapSpan;
use analyzer_analysis::reuse::{Declaration, Figure};
use analyzer_analysis::staleness::{FleetHealth, RunStaleness};
use analyzer_analysis::tree::BlockTree;
use analyzer_analysis::{Params, Window};
use std::collections::BTreeMap;

/// The lifecycle slice at the BANKED clip — evidence that reaches emitted rows.
pub(crate) fn lifecycle_for_banked(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<Vec<analyzer_analysis::events::LifecycleEvent>, Error> {
    source
        .lifecycle_events(
            fleet_id,
            w.start_ms - EVIDENCE_CLIP_MARGIN_MS,
            w.end_ms + EVIDENCE_CLIP_MARGIN_MS,
        )
        .map_err(Error::from)
}

/// The lifecycle slice at the SNAPSHOT clip — verdicts and ledger entries nothing banks.
///
/// # Errors
/// Source failures or malformed input.
pub(crate) fn lifecycle_for_snapshot(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<Vec<analyzer_analysis::events::LifecycleEvent>, Error> {
    source
        .lifecycle_events(
            fleet_id,
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .map_err(Error::from)
}

/// Residency intervals over a window (join-semantics).
///
/// # Errors
/// Source failures or malformed input.
pub fn residency_intervals(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<Vec<Interval>, Error> {
    Ok(
        analyzer_analysis::intervals::residency_intervals_from_events(
            &source.cache_events(fleet_id, w)?,
        ),
    )
}

/// The windowed block tree.
///
/// A free function rather than `BlockTree::build`: the construction is
/// `BlockTree::from_events`, and a constructor that needed a source would put the source back
/// inside the type.
///
/// # Errors
/// Source failures or malformed input.
pub fn block_tree(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<BlockTree, Error> {
    Ok(BlockTree::from_events(&source.cache_events(fleet_id, w)?))
}

/// The per-window uncertainty model: drop windows, boundaries, evidence, truncated deliveries.
///
/// **Two slices at two scopes**, which is the whole of what this adapter decides.
///
/// # Errors
/// Source failures or malformed input.
pub fn uncertainty_model(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
    params: &Params,
) -> Result<UncertaintyModel, Error> {
    let cache = source.cache_events(fleet_id, w)?;
    let lifecycle = lifecycle_for_banked(source, fleet_id, w)?;
    analyzer_analysis::completeness::uncertainty_model_from_events(&cache, &lifecycle, w, params)
        .map_err(Error::from)
}

/// Classify every store fact in the window (spec order).
///
/// # Errors
/// Source failures or malformed input.
pub fn classify(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
    params: &Params,
) -> Result<Classification, Error> {
    let ir = source.cache_events(fleet_id, w)?;
    let model = uncertainty_model(source, fleet_id, w, params)?;
    Ok(analyzer_analysis::classify::classify_from_events(
        &ir, &model, params,
    ))
}

/// The narrowed span for the first sound `seq` discontinuity, or a refusal.
///
/// # Errors
/// Source failures or malformed input.
pub fn gap_span(source: &dyn EventSource, fleet_id: &str, w: &Window) -> Result<GapSpan, Error> {
    Ok(analyzer_analysis::narrowing::gap_span_from_events(
        &source.cache_events(fleet_id, w)?,
    ))
}

/// The standing evict-resolution audit.
///
/// **The partition law is enforced here rather than in the fold**, because it is a statement about
/// whether the audit may REPORT: an audit that cannot account for every evict refuses rather than
/// reporting around the gap, and refusing is an I/O-shaped outcome.
///
/// # Errors
/// Source failures, or classes that do not partition the audited count.
pub fn audit(source: &dyn EventSource, fleet_id: &str, w: &Window) -> Result<AuditOutcome, Error> {
    let outcome =
        analyzer_analysis::differential::audit_from_events(&source.cache_events(fleet_id, w)?);
    verify_audit_partition(&outcome)?;
    Ok(outcome)
}

/// The partition law, shared by both audit drivers (the flat window read above and the
/// grouped-stream deep audit): an audit that cannot account for every evict refuses
/// rather than reporting around the gap.
pub(crate) fn verify_audit_partition(outcome: &AuditOutcome) -> Result<(), Error> {
    for row in &outcome.instances {
        let accounted =
            row.agreed + row.mismatched + row.underresolved + row.indeterminate + row.unjoined;
        if accounted != row.audited {
            return Err(Error::Derivation(format!(
                "differential audit: classes do not partition for {}: {accounted} accounted \
                 of {} audited — the audit is defective and refuses to report",
                row.instance, row.audited
            )));
        }
    }
    Ok(())
}

/// What a `store` means on this fleet's streams (spec 2.3).
///
/// **Retention-scoped, deliberately**: a declaration is a property of a producer RUN, not of a
/// window, and a run whose lifecycle records all predate the window still declares what its
/// stores mean. The seam's declaration slice is that scope, at declaration cost — the
/// production source projects the declaration columns alone rather than decoding the whole
/// horizon's lifecycle set for one run-constant value.
///
/// # Errors
/// Source failures or malformed input.
pub fn declaration(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &analyzer_analysis::Window,
) -> Result<Declaration, Error> {
    Ok(analyzer_analysis::reuse::declaration_from_events(
        &source.lifecycle_declarations(fleet_id, w)?,
    ))
}

/// The three spec 5.9 figures: unqualified, recompute, first insertion.
///
/// # Errors
/// Source failures or malformed input.
pub fn reuse_figures(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<(Figure, Figure, Figure), Error> {
    let ir = source.cache_events(fleet_id, w)?;
    let lifecycle = lifecycle_for_snapshot(source, fleet_id, w)?;
    Ok(analyzer_analysis::reuse::figures_from_events(
        &ir, &lifecycle,
    ))
}

/// Per-run staleness verdicts.
///
/// # Errors
/// Source failures or malformed input.
pub fn run_verdicts(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<BTreeMap<String, RunStaleness>, Error> {
    Ok(analyzer_analysis::staleness::run_verdicts_from_events(
        &lifecycle_for_snapshot(source, fleet_id, w)?,
    ))
}

/// Distinct producer agent versions observed on `agent_start` records (snapshot
/// provenance), deduplicated and sorted.
///
/// **Retention-scoped like every lifecycle-identity fetch** (inventory Q11): which agent
/// versions a fleet has run is a fact about the fleet, not about a window of it, so it reads
/// the seam's declaration slice. The `agent_version` spelling is a wire dependency on the
/// tap's exact field name — pinned by the corpus-driven test against the tap's lifecycle
/// fixture, so a tap-side rename breaks a test instead of silently emptying a provenance
/// field.
///
/// # Errors
/// Source failures or malformed input.
pub fn agent_versions(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &analyzer_analysis::Window,
) -> Result<Vec<String>, Error> {
    let versions: std::collections::BTreeSet<String> = source
        .lifecycle_declarations(fleet_id, w)?
        .into_iter()
        .filter(|e| e.kind == "agent_start")
        .filter_map(|e| e.agent_version)
        .collect();
    Ok(versions.into_iter().collect())
}

/// Fleet health over a window, graded against the observation horizon.
///
/// # Errors
/// Source failures or malformed input.
pub fn fleet_health(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<FleetHealth, Error> {
    Ok(analyzer_analysis::staleness::fleet_health_from_events(
        &lifecycle_for_snapshot(source, fleet_id, w)?,
    ))
}
