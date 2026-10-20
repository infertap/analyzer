// SPDX-License-Identifier: AGPL-3.0-only

//! The row readers: one JSON row of a segment to the IR's event, with the lifecycle ordering the
//! seam promises. Shared with the harness's naive source so the two cannot drift.

use super::LIFECYCLE_KINDS;
use crate::Error;
use analyzer_analysis::events::{Endpoint, Event, Kind, LifecycleEvent};
use serde_json::Value;

/// The lifecycle ordering the seam promises: `at_ms`, then instance, then kind. Shared with
/// the test twin, like the row readers below.
pub fn sort_lifecycle(events: &mut [LifecycleEvent]) {
    events.sort_by(|a, b| {
        a.at_ms
            .partial_cmp(&b.at_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.instance.cmp(&b.instance))
            .then_with(|| a.kind.cmp(&b.kind))
    });
}

/// One shaped row as a cache-fact [`Event`]. `Ok(None)` for a row whose kind is not a cache
/// kind. Fails closed — the SEGMENT's decode refuses — on a missing `at_ms` or a present
/// scope/sequence cell that does not read as its declared unsigned integer: `None` fields
/// remain ONLY genuinely absent cells, never a value this reader could not read.
///
/// # Errors
/// The refusals above, each naming the field and the value that caused it.
pub fn cache_event_of(row: &Value, delivery: Option<String>) -> Result<Option<Event>, Error> {
    let Some(kind) = str_of(row, "kind").as_deref().and_then(Kind::parse) else {
        return Ok(None);
    };
    // A `holder_reset` is ordered at its engine-clock boundary, not at its producer-clock
    // `at_ms` — the two ride one record and are never compared (spec 2.6), and the fold
    // consumes exactly one time axis. Fail closed on a reset without its boundary.
    let at_ms = if kind == Kind::HolderReset {
        f64_of(row, "boundary_ms").ok_or(Error::Derivation(
            "holder_reset record has no boundary_ms".to_string(),
        ))?
    } else {
        f64_of(row, "at_ms").ok_or(Error::Derivation("cache record has no at_ms".to_string()))?
    };
    Ok(Some({
        let mut event = Event::new(kind, str_of(row, "instance_id").unwrap_or_default(), at_ms);
        event.dp_rank = u32_of(row, "dp_rank")?;
        event.group_idx = u32_of(row, "group_idx")?;
        event.block_id = opt_str(row, "block_id");
        event.content_id = opt_str(row, "content_id");
        event.key_epoch = u32_of(row, "key_epoch")?;
        event.tier = opt_str(row, "tier");
        event.backend_id = opt_str(row, "backend_id");
        event.locality = opt_str(row, "locality");
        // Absent coalesces to zero and false: a segment that omits the field means
        // the unqualified state, never an error, and every source must read it so.
        event.n_tokens = i64_of(row, "n_tokens").unwrap_or(0);
        event.parent_id = opt_str(row, "parent_id");
        event.parent_unknown = truthy(row, "parent_unknown");
        event.delivery_id = delivery;
        event.incarnation = str_of(row, "incarnation").unwrap_or_default();
        event.seq = u64_of(row, "seq")?;
        event.reused = truthy(row, "reused");
        event.workload_class = opt_str(row, "workload_class");
        event
    }))
}

/// One shaped row as a [`LifecycleEvent`]. `Ok(None)` for a kind outside [`LIFECYCLE_KINDS`].
/// The same fail-closed rule as [`cache_event_of`] on every typed unsigned cell.
///
/// # Errors
/// A missing `at_ms`, or a present cell that does not read as its declared type.
pub fn lifecycle_event_of(row: &Value) -> Result<Option<LifecycleEvent>, Error> {
    let kind = str_of(row, "kind").unwrap_or_default();
    if !LIFECYCLE_KINDS.contains(&kind.as_str()) {
        return Ok(None);
    }
    let at_ms = f64_of(row, "at_ms").ok_or(Error::Derivation(
        "lifecycle record has no at_ms".to_string(),
    ))?;

    // `endpoints` arrives in one of two shapes, and this reader speaks BOTH deliberately.
    // The DELIVERY form is a list of the closed element — what the tap writes, what the
    // spec's twin pins, and what the columnar decoder materializes. The analyzer's own
    // shaping stores the same list as PARALLEL ARRAYS under dotted keys. This reader spoke
    // only the second until E2 ran a real capture through the first: every test fed it
    // shaped rows, production fed it the wire, and the roster lost every endpoint — which
    // starved the model join on every measurement surface downstream.
    let endpoints: Vec<Endpoint> =
        if let Some(list) = row.get("endpoints").and_then(Value::as_array) {
            list.iter()
                .map(|e| Endpoint {
                    source: str_of(e, "source").unwrap_or_default(),
                    dropped: e.get("dropped").and_then(as_i64).unwrap_or(0),
                    last_msg_at_ms: e.get("last_msg_at_ms").and_then(as_f64),
                    topic: e
                        .get("topic")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                })
                .collect()
        } else {
            let arr = |k: &str| {
                row.get(k)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            };
            let (sources, dropped, last, topics) = (
                arr("endpoints.source"),
                arr("endpoints.dropped"),
                arr("endpoints.last_msg_at_ms"),
                arr("endpoints.topic"),
            );
            sources
                .iter()
                .enumerate()
                .map(|(i, s)| Endpoint {
                    source: s.as_str().unwrap_or_default().to_string(),
                    // A short parallel array is a producer defect, not a shape this reader
                    // models; absent counters read as zero.
                    dropped: dropped.get(i).and_then(as_i64).unwrap_or(0),
                    last_msg_at_ms: last.get(i).and_then(as_f64),
                    topic: topics
                        .get(i)
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string),
                })
                .collect()
        };

    Ok(Some(LifecycleEvent {
        kind,
        at_ms,
        instance: str_of(row, "instance_id").unwrap_or_default(),
        incarnation: str_of(row, "incarnation").unwrap_or_default(),
        dropped: i64_of(row, "dropped"),
        oversized: i64_of(row, "oversized"),
        heartbeat_secs: u64_of(row, "heartbeat_secs")?,
        max_segment_secs: u64_of(row, "max_segment_secs")?,
        canary: opt_str(row, "canary"),
        reuse_reporting: opt_str(row, "reuse_reporting"),
        endpoints,
        segment_seq: u64_of(row, "segment_seq")?,
        first_seq: u64_of(row, "first_seq")?,
        last_seq: u64_of(row, "last_seq")?,
        refused: u64_of(row, "refused")?,
        window_start_ms: f64_of(row, "window_start_ms"),
        window_end_ms: f64_of(row, "window_end_ms"),
        dp_rank: u32_of(row, "dp_rank")?,
        group_idx: u32_of(row, "group_idx")?,
        attributed: bool_of(row, "attributed"),
        dropped_bytes: i64_of(row, "dropped_bytes"),
        agent_version: extra_or_top(row, "agent_version")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        // The `--provenance` declarations, from the segment header's `extra` map (spec 2.2:
        // an untyped key rides `extra` and a reader treats it as it would the field inline).
        declared_model: extra_or_top(row, "model")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        declared_pod: extra_or_top(row, "pod_identity")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    }))
}

// A number may arrive quoted: the shaping stringifies id fields, and some producers render
// integers as strings. A reader that accepted only bare numbers would read them as ABSENT,
// which for a counter is silently zero.
fn as_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse().ok())
}

fn str_of(row: &Value, k: &str) -> Option<String> {
    row.get(k).and_then(Value::as_str).map(str::to_string)
}

/// An absent OR empty string reads as absent. The shaping writes `''` as the agent-scoped
/// sentinel, so the two spellings are one state and every source must read them so.
fn opt_str(row: &Value, k: &str) -> Option<String> {
    str_of(row, k).filter(|s| !s.is_empty())
}

/// The delivery-family fields the shaping routes through `extra` (the row path) but the
/// shipped schema types as columns (the columnar path): the value may sit top-level or inside
/// `extra`, string-rendered, and both spellings are one field.
fn extra_or_top<'a>(row: &'a Value, k: &str) -> Option<&'a Value> {
    row.get(k).or_else(|| row.get("extra")?.get(k))
}

fn i64_of(row: &Value, k: &str) -> Option<i64> {
    extra_or_top(row, k).and_then(as_i64)
}

fn f64_of(row: &Value, k: &str) -> Option<f64> {
    row.get(k).and_then(as_f64)
}

/// JSON booleans, or the strings the shaping's `extra` route renders them as. `None` is
/// absent, never false — `attributed: false` and "no attribution claim" are different states.
fn bool_of(row: &Value, k: &str) -> Option<bool> {
    match extra_or_top(row, k) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) if s == "true" => Some(true),
        Some(Value::String(s)) if s == "false" => Some(false),
        _ => None,
    }
}

/// A typed unsigned cell: `None` ONLY where the cell is genuinely absent. A present cell that
/// is negative, out of range, or unparsable FAILS the segment's decode — reading it as absent
/// silently merged holders (`dp_rank: -1` became the single-worker zero) and silently widened
/// key epochs, the exact class the differential could not see while both sources shared it.
fn u32_of(row: &Value, k: &str) -> Result<Option<u32>, Error> {
    let Some(v) = row.get(k) else {
        return Ok(None);
    };
    if v.is_null() {
        return Ok(None);
    }
    as_i64(v)
        .and_then(|n| u32::try_from(n).ok())
        .map(Some)
        .ok_or_else(|| {
            Error::Row(format!(
                "field {k} carries {v}, which does not read as a u32 — refusing"
            ))
        })
}

fn u64_of(row: &Value, k: &str) -> Result<Option<u64>, Error> {
    let Some(v) = row.get(k) else {
        return Ok(None);
    };
    if v.is_null() {
        return Ok(None);
    }
    as_i64(v)
        .and_then(|n| u64::try_from(n).ok())
        .map(Some)
        .ok_or_else(|| {
            Error::Row(format!(
                "field {k} carries {v}, which does not read as a u64 — refusing"
            ))
        })
}

/// JSON `true`, or a non-zero number, or the strings an engine renders them as.
fn truthy(row: &Value, k: &str) -> bool {
    match row.get(k) {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().is_some_and(|v| v != 0),
        Some(Value::String(s)) => s == "1" || s == "true",
        _ => false,
    }
}

#[cfg(test)]
mod endpoint_form_tests {
    use super::lifecycle_event_of;

    /// The DELIVERY form: `endpoints` as a list of the closed element, which is what the
    /// tap writes and the columnar decoder materializes. E2's first real capture proved
    /// this reader spoke only the shaped form below — the roster fanned out over nothing,
    /// and the model join starved on every surface. This test is the one that fails there.
    #[test]
    fn the_wire_form_endpoints_list_is_read() {
        let row = serde_json::json!({
            "kind": "heartbeat", "at_ms": 5.0, "instance_id": "", "incarnation": "run-1",
            "endpoints": [
                {"source": "e2", "endpoint": "tcp://127.0.0.1:5557", "dropped": 0,
                 "last_msg_at_ms": 4.0, "topic": "kv@pod-a@model-x"},
                {"source": "e2", "endpoint": "tcp://127.0.0.1:5558", "dropped": 2,
                 "topic": ""}
            ]
        });
        let ev = lifecycle_event_of(&row).expect("reads").expect("lifecycle");
        assert_eq!(ev.endpoints.len(), 2, "both elements fan out");
        assert_eq!(ev.endpoints[0].source, "e2");
        assert_eq!(ev.endpoints[0].topic.as_deref(), Some("kv@pod-a@model-x"));
        assert_eq!(ev.endpoints[1].dropped, 2);
        assert_eq!(ev.endpoints[1].topic, None, "an empty topic reads as none");
    }

    /// The SHAPED form: the analyzer's own storage rows carry the same list as parallel
    /// arrays under dotted keys. Still spoken — the derivation path feeds it.
    #[test]
    fn the_shaped_form_parallel_arrays_are_still_read() {
        let row = serde_json::json!({
            "kind": "heartbeat", "at_ms": 5.0, "instance_id": "", "incarnation": "run-1",
            "endpoints.source": ["e2"],
            "endpoints.dropped": [1],
            "endpoints.last_msg_at_ms": [4.0],
            "endpoints.topic": ["kv@pod-a@model-x"]
        });
        let ev = lifecycle_event_of(&row).expect("reads").expect("lifecycle");
        assert_eq!(ev.endpoints.len(), 1);
        assert_eq!(ev.endpoints[0].topic.as_deref(), Some("kv@pod-a@model-x"));
        assert_eq!(ev.endpoints[0].dropped, 1);
    }
}
