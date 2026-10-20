// SPDX-License-Identifier: AGPL-3.0-only

//! Markers operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::envelope::window_json;
use crate::error::ApiError;
use crate::error::Cause;
use crate::extract::Body;
use crate::extract::Params;
use crate::extract::Subject;
use crate::requests::stamp_and_window;
use crate::requests::NoQuery;
use crate::requests::WindowQuery;
use crate::wire;
use crate::ApiState;
use axum::extract::State;
use axum::Json;
use serde_json::json;

pub(crate) async fn markers(
    State(state): State<ApiState>,
    Params(q): Params<WindowQuery>,
) -> Result<Json<wire::Envelope<wire::MarkersData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q).await?;
    let rows = state.read(move |dao| dao.markers(&window)).await?;
    Ok(envelope(
        &stamp,
        wire::MarkersData {
            markers: rows.iter().map(marker_json).collect(),
        },
    ))
}

pub(crate) async fn marker_detail(
    State(state): State<ApiState>,
    Subject(id): Subject<String>,
    Params(q): Params<WindowQuery>,
) -> Result<Json<wire::Envelope<wire::MarkerData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q).await?;
    let rows = state.read(move |dao| dao.markers(&window)).await?;
    let marker = rows
        .iter()
        .find(|m| format!("{}-{}", m.kind, m.at_ms) == id)
        .ok_or_else(|| {
            // A marker outside the requested window is not a missing marker, so the refusal says
            // which window it looked in rather than only that it found nothing.
            ApiError::new(
                Cause::UnknownMarker,
                format!("no marker {id:?} in this window"),
            )
            .with_bound(json!({ "window": window_json(&window) }))
        })?;
    Ok(envelope(
        &stamp,
        wire::MarkerData {
            marker: marker_json(marker),
        },
    ))
}

pub(crate) fn marker_json(m: &analyzer_serving_clickhouse::Marker) -> wire::MarkerJson {
    wire::MarkerJson {
        id: format!("{}-{}", m.kind, m.at_ms),
        at_ms: m.at_ms,
        kind: m.kind.clone(),
        provenance: m.provenance.clone(),
        label: m.label.clone(),
        from_value: m.from_value.clone(),
        to_value: m.to_value.clone(),
        source: m.source.clone(),
        breaks: m.breaks(),
        // A marker naming no prior generation is an ORIGIN: the first generation's start, not a
        // transition. Carried explicitly so the console renders it without inferring from a null.
        origin: m.from_value.is_none()
            && matches!(m.kind.as_str(), "param_change" | "semantics_change"),
    }
}

pub(crate) async fn annotations_create(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
    Body(body): Body<crate::requests::AnnotationBody>,
) -> Result<Json<wire::Envelope<wire::AnnotationData>>, ApiError> {
    let ann = body.validate(state.now_ms())?.into_annotation();
    let stamp = crate::envelope::Stamp::read(&state, None, state.declared_ledger_days()).await?;

    // The durable object is the declaration: the next ingest cycle records it in the input
    // log and the core states its marker. A process with no bucket cannot declare, since a
    // declaration nothing can replay would not survive a rebuild.
    let Some(sink) = state.annotation_sink() else {
        return Err(ApiError::new(
            Cause::BucketUnavailable,
            "no bucket is attached to this process, so a declaration could not be made \
             durable"
                .to_string(),
        ));
    };
    let key = analyzer_serving_clickhouse::annotations::object_key(ann.at_ms, &ann.label);
    let bytes = analyzer_serving_clickhouse::annotations::encode(&ann);
    tokio::task::spawn_blocking(move || sink(&key, bytes.as_bytes()))
        .await
        .map_err(|e| ApiError::engine(format!("annotation write task failed: {e}")))?
        .map_err(|e| ApiError::new(Cause::BucketUnavailable, e.to_string()))?;
    let (durable, note) = (
        true,
        "declared: the durable object is written; the marker appears when the next ingest \
         cycle records it, and replay re-derives it on any rebuild",
    );
    Ok(envelope(
        &stamp,
        wire::AnnotationData {
            recorded: true,
            durable,
            note,
        },
    ))
}
