// SPDX-License-Identifier: AGPL-3.0-only

//! Figures operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::error::ApiError;
use crate::extract::Params;
use crate::requests::stamp_and_window;
use crate::requests::BandsQuery;
use crate::requests::FiguresQuery;
use crate::resources::common::reach_json;
use crate::wire;
use crate::wire::Figure;
use crate::ApiState;
use analyzer_serving_clickhouse::Dim;
use analyzer_serving_clickhouse::SegmentBasis;
use axum::extract::State;
use axum::Json;
use serde_json::json;

pub(crate) async fn figures(
    State(state): State<ApiState>,
    Params(q): Params<FiguresQuery>,
) -> Result<Json<wire::Envelope<wire::FiguresData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let (requested, dims, scope) = (q.grain(), q.dims(), q.scope());
    // The minute tier serves the fleet × class × workload × model cut ONLY (its table
    // carries no other dimensions by design — one dimension set on every minute table,
    // one dimension set on every minute table): an explicit ask outside that cut is refused by
    // name, and the AUTO chooser never picks it there. **A holder-shaped SCOPE rules it out the
    // same way a holder-shaped dim does**: an instance-scoped auto read on a short window that
    // resolved to minute would name `instance_id` against a table with no such column and reach
    // the caller as a 503. Two minute tables serve: the fleet cells for the fleet cut, the rank
    // cells for an instance or holder scope and the instance cut. A holder or tier cut has
    // neither — the rank cells roll cache groups and tiers up — and is refused by name.
    let minute = q.minute();
    let (grain, segments) = state
        .read(move |dao| {
            let grain = dao.resolve_grain(&window, requested, minute)?;
            Ok((
                grain,
                dao.waste_by_segmented(&window, grain, &dims, &scope)?,
            ))
        })
        .await?;

    // **Segmented always** — a one-element array in the ordinary case. There is no unsegmented
    // form and no total spanning segments, which is never-sum expressed as an absent field.
    let segments = segments
        .iter()
        .map(|s| wire::Segment {
            discriminator: s.discriminator.clone(),
            semantics_version: s.semantics_version.clone(),
            window: wire::WindowJson::of(&s.window),
            figures: s.figures.as_ref().map(|rows| {
                rows.iter()
                    .map(|r| wire::Bucket {
                        bucket: r.bucket.clone(),
                        dims: r
                            .dims
                            .iter()
                            .map(|(d, v)| wire::DimValue {
                                dim: dim_name(*d),
                                value: v.clone(),
                            })
                            .collect(),
                        events: r.events,
                        tokens: Figure::from(&r.tokens),
                    })
                    .collect()
            }),
            // `figures: null` plus a stated reason, never an empty array — the two are different
            // claims and only one of them is "looked and found nothing".
            refused: s.basis.refusal().map(|cause| wire::Refusal {
                cause: cause.into(),
                message: refusal_message(s.basis).to_string(),
                // The generation this span belonged to, which is the fact a reader needs to
                // render it as hatched rather than as zero.
                bound: Some(json!({
                    "discriminator": s.discriminator,
                    "semantics_version": s.semantics_version,
                })),
                requested: None,
            }),
        })
        .collect();

    Ok(envelope(
        &stamp,
        wire::FiguresData {
            register: "recompute",
            never_summed_with: "capacity",
            grain: grain.as_str(),
            segments,
        },
    ))
}

pub(crate) async fn bands(
    State(state): State<ApiState>,
    Params(q): Params<BandsQuery>,
) -> Result<Json<wire::Envelope<wire::BandsData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (requested, dims, scope) = (q.figures().grain(), q.figures().dims(), q.figures().scope());
    // One window grammar, one anchor: the stops this panel offers are ordinary
    // `<N>d` / `<N>h` specs, resolved exactly as every other read resolves them, so this
    // chart and the copies chart cannot disagree about which three hours exist.
    let (stamp, window) = stamp_and_window(&state, &q.figures().context()).await?;
    let custom_window = q.custom_window();
    let has_holder = dims.contains(&analyzer_serving_clickhouse::Dim::Holder);
    let holder_limit = q.limit();
    // Two minute tables: the rank cells serve an instance or holder scope and the instance cut,
    // the fleet cells the rest; a holder or tier cut has neither and
    // refuses by name. Explicit `minute` refuses here with its own cause; left on `auto` the
    // resolver is told which tier can serve and picks from its reach.
    let minute = q.figures().minute();
    let (grain, lens) = state
        .read(move |dao| {
            // Minute is servable here within the resolver's reach gate, and the dao refuses
            // holder-shaped dims by name.
            let grain = dao.resolve_grain(&window, requested, minute)?;
            let read = dao.bands_by(&window, grain, &dims, &scope)?;
            Ok((grain, read.lens(has_holder.then_some(holder_limit))))
        })
        .await?;
    Ok(envelope(
        &stamp,
        wire::BandsData {
            register: "recompute",
            never_summed_with: "capacity",
            grain: grain.as_str(),
            bucket_ms: grain.bucket_ms(),
            start_ms: window.start_ms,
            end_ms: window.end_ms,
            derived: (custom_window && requested.is_none()).then(|| {
                format!(
                    "range × retention: {} over {:.0} min",
                    grain.as_str(),
                    (window.end_ms - window.start_ms) / 60_000.0
                )
            }),
            population: lens.holders.map(|h| h.population),
            holders_returned: lens.holders.map(|h| h.returned),
            reach: reach_json(lens.evidence, state.declared_ledger_days()),
            rows: lens.rows.iter().map(band_row).collect(),
            gap_unobserved: Figure::from(&lens.gap_unobserved),
            gap_unobserved_events: lens.gap_unobserved_events,
        },
    ))
}

fn band_row(r: &analyzer_serving_clickhouse::BandRow) -> wire::BandRowJson {
    wire::BandRowJson {
        bucket: r.bucket.clone(),
        band: r.band.clone(),
        dims: r
            .dims
            .iter()
            .map(|(d, v)| wire::DimValue {
                dim: dim_name(*d),
                // The holder triple leaves the DAO tab-joined; the wire spelling is the
                // slash-joined `instance/dp_rank/group_idx`.
                value: if *d == analyzer_serving_clickhouse::Dim::Holder {
                    v.replace('\t', "/")
                } else {
                    v.clone()
                },
            })
            .collect(),
        events: r.events,
        tokens: Figure::from(&r.tokens),
    }
}

pub(crate) fn dim_name(d: Dim) -> &'static str {
    match d {
        Dim::Class => "class",
        Dim::Holder => "holder",
        Dim::Instance => "instance",
        Dim::Workload => "workload",
        Dim::Model => "model",
    }
}

pub(crate) fn refusal_message(basis: SegmentBasis) -> &'static str {
    match basis {
        SegmentBasis::SupersededSemantics => {
            "this span was computed under a prior join-semantics generation, which this binary \
             has no semantics to explain; read it as unexamined, never as zero"
        }
        SegmentBasis::Unlinked => {
            "the marker chain does not link this span to a named generation, so which parameters \
             produced its rows is unknown; inferring one would file its rows under another \
             generation's name"
        }
        SegmentBasis::Current | SegmentBasis::PriorParameters => "",
    }
}
