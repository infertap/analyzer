// SPDX-License-Identifier: AGPL-3.0-only

//! Segment-arrival facts: the ingest-grain lifecycle derivation.
//!
//! **Facts at ingest, verdicts at read, semantics at pass.** No console surface may be an
//! unbounded lookup, and staleness that is *stored* can rot — so ingest, which already holds
//! every decoded record of the segment it is committing, derives one arrivals row per
//! `(incarnation, segment_seq, instance)` and one row per declared drop. The roster and the
//! health tiles then answer from these rows at ingest freshness, and the staleness verdict is
//! computed at READ time against the knowledge horizon (spec 5.8) — never persisted.
//!
//! Everything here restates a record's own declaration or an arrival fact, so the tables are
//! meaning-independent and a cold re-derivation from the bucket reproduces them exactly —
//! the same idempotence discipline as the ingest ledger, keyed so re-emission converges.
//!
//! **The liveness vocabulary mirrors `staleness::run_verdicts_from_events` deliberately**:
//! only `agent_start` / `heartbeat` / `agent_stop` advance `last_record_at_ms`, declared
//! bounds come from `segment_open` and count only when both halves are present, and the
//! heartbeat cadence is summarized (count, first, last) for the read-time allowance. A row
//! that meant something different from the raw path would make the two paths two readers.
//!
//! **A FOLD, since the wiring: one walk, two row sources.** The shaped path
//! feeds parsed NDJSON lines; the fast path feeds the projected columnar decode's rows.
//! One `feed` body is what keeps the two sources one derivation — a second copy of this
//! walk would be a drift pair inside our own crate, the exact shape the fast path's
//! equivalence pin exists to forbid one seam over.

use serde_json::Value;

/// What one committed batch's segments contribute to the input log: their arrival facts and
/// the gaps they declare. The batch record carries them, and the served `input_segments` and
/// `input_drops` rows render from the record, so the two cannot differ.
pub(crate) struct ArrivalFacts {
    pub segments: Vec<analyzer_lake::ledger::SegmentArrival>,
    pub drops: Vec<analyzer_lake::ledger::DeclaredDrop>,
}

fn f64_of(obj: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    obj.get(key).and_then(Value::as_f64)
}

fn str_of<'a>(obj: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// A numeric field wherever the shaping put it: typed column first, then the `extra` map,
/// where a non-typed wire number rides as its JSON text. Which envelope counters are typed
/// is the shaping's roster (`LIFECYCLE_FIELDS`), not this reader's business to memorize —
/// `publisher_restarts` is exactly the one that rides `extra` today.
fn num_of(obj: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    f64_of(obj, key).or_else(|| extra_of(obj, key).and_then(|s| s.parse().ok()))
}

fn extra_of<'a>(obj: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get("extra")
        .and_then(Value::as_object)
        .and_then(|m| m.get(key))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// Keep `value` in `slot` when `at_ms` is the earliest heartbeat seen: the value a counter
/// stated at the segment's first heartbeat, whatever order the heartbeats came in.
fn earliest(slot: &mut Option<(f64, u64)>, at_ms: f64, value: u64) {
    if slot.is_none_or(|(at, _)| at_ms < at) {
        *slot = Some((at_ms, value));
    }
}

/// One instance's slice of the segment's facts. `String` fields use `""` for absent — these
/// columns are non-nullable in the key path, and the read side treats `''` as absence.
#[derive(Default, Clone)]
struct PerInstance {
    msgs_seen: u64,
    dropped: u64,
    /// `dropped` at the earliest heartbeat naming this source, with that heartbeat's instant.
    dropped_first: Option<(f64, u64)>,
    publisher_restarts: u64,
    topic_model: Option<String>,
    topic_pod: Option<String>,
}

/// One declared drop, held as facts until [`ArrivalsFold::finish`] stamps the commit's
/// arrival instant — the stamp belongs to the COMMIT, not to the walk, so a fold fed
/// before the intent row still renders the same second the ledger row carries.
struct DeclaredDrop {
    incarnation: String,
    first_seq: i64,
    last_seq: i64,
    at_ms: f64,
}

/// The arrivals derivation as a fold: construct per batch, [`feed`](Self::feed) every
/// row in stream order (order is load-bearing — `segments_dropped` attribution falls back
/// to the open seen so far, and the envelope marks are the LAST heartbeat's), then
/// [`finish`](Self::finish). The commit's arrival instant is stamped when the rows render.
pub(crate) struct ArrivalsFold {
    incarnation: String,
    segment_seq: i64,
    attributed: bool,
    heartbeat_secs: u64,
    max_segment_secs: u64,
    declared_model: Option<String>,
    declared_pod: Option<String>,
    engine_version: Option<String>,
    /// The run's key-space canary — banked so the residual's key-space
    /// guard reads it from the store instead of decoding the day's segments to
    /// harvest one string per run. Empty is "no canary seen in this segment", which
    /// a genuinely unkeyed tap produces and which the guard reads as the unkeyed
    /// space; it is NOT the same as having no arrivals row at all.
    canary: Option<String>,
    agent_version: Option<String>,
    first_at: Option<f64>,
    last_at: Option<f64>,
    hb_count: u64,
    hb_first: Option<f64>,
    hb_last: Option<f64>,
    stop_at: Option<f64>,
    /// Envelope high-water marks: the tap's counters are cumulative, so the LAST
    /// heartbeat's values are the segment's marks — tracked by heartbeat order in the
    /// stream.
    envelope: PerInstance,
    oversized: u64,
    /// `oversized` at the earliest heartbeat, with its instant.
    oversized_first: Option<(f64, u64)>,
    instances: std::collections::BTreeMap<String, PerInstance>,
    drops: Vec<DeclaredDrop>,
}

impl ArrivalsFold {
    pub(crate) fn new() -> Self {
        Self {
            incarnation: String::new(),
            segment_seq: -1,
            attributed: false,
            heartbeat_secs: 0,
            max_segment_secs: 0,
            declared_model: None,
            declared_pod: None,
            engine_version: None,
            canary: None,
            agent_version: None,
            first_at: None,
            last_at: None,
            hb_count: 0,
            hb_first: None,
            hb_last: None,
            stop_at: None,
            envelope: PerInstance::default(),
            oversized: 0,
            oversized_first: None,
            instances: std::collections::BTreeMap::new(),
            drops: Vec::new(),
        }
    }

    #[allow(clippy::too_many_lines)] // one fold over the segment: splitting it would scatter the
                                     // single reading of what the segment said across helpers that each re-parse it
    pub(crate) fn feed(&mut self, obj: &serde_json::Map<String, Value>) {
        let Some(kind) = obj.get("kind").and_then(Value::as_str) else {
            return;
        };
        match kind {
            "segment_open" => {
                self.attributed = true;
                if let Some(v) = str_of(obj, "incarnation") {
                    self.incarnation = v.to_string();
                }
                if let Some(v) = f64_of(obj, "segment_seq") {
                    #[allow(clippy::cast_possible_truncation)] // sequence numbers are small
                    {
                        self.segment_seq = v as i64;
                    }
                }
                // Both bounds or neither, exactly as the raw verdict path reads them: a
                // declaration missing half of itself states no line (spec 5.8).
                if let (Some(hb), Some(seg)) = (
                    f64_of(obj, "heartbeat_secs"),
                    f64_of(obj, "max_segment_secs"),
                ) {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    {
                        self.heartbeat_secs = hb as u64;
                        self.max_segment_secs = seg as u64;
                    }
                }
                if let Some(m) = extra_of(obj, "model") {
                    self.declared_model = Some(m.to_string());
                }
                // **`pod_identity`, which is the key the shipped chart writes** — and the
                // key `parquet_source`'s own declared-pod read uses. A read of any other key
                // fills `declared_pod` from a field nothing sets, invisible because nothing
                // reads it back. One spelling, not two — accepting both would be the
                // naming-convention pattern-match spec 2.7 rejects.
                if let Some(p) = extra_of(obj, "pod_identity") {
                    self.declared_pod = Some(p.to_string());
                }
                if let Some(v) = extra_of(obj, "vllm_version") {
                    self.engine_version = Some(v.to_string());
                }
            }
            "segments_dropped" => {
                // The record names the run whose DATA IS GONE — its own incarnation field,
                // never the enclosing segment's (the reclaiming pass can name another run).
                let named = str_of(obj, "incarnation").unwrap_or(&self.incarnation);
                let (Some(first), Some(last)) = (f64_of(obj, "first_seq"), f64_of(obj, "last_seq"))
                else {
                    return;
                };
                let at_ms = f64_of(obj, "at_ms").unwrap_or(0.0);
                #[allow(clippy::cast_possible_truncation)] // sequence numbers are small
                self.drops.push(DeclaredDrop {
                    incarnation: named.to_string(),
                    first_seq: first as i64,
                    last_seq: last as i64,
                    at_ms,
                });
            }
            "agent_start" | "heartbeat" | "agent_stop" => {
                let Some(at_ms) = f64_of(obj, "at_ms") else {
                    return;
                };
                self.first_at = Some(self.first_at.map_or(at_ms, |v| v.min(at_ms)));
                self.last_at = Some(self.last_at.map_or(at_ms, |v| v.max(at_ms)));
                if let Some(v) = str_of(obj, "agent_version") {
                    self.agent_version = Some(v.to_string());
                }
                if let Some(v) = str_of(obj, "canary") {
                    self.canary = Some(v.to_string());
                }
                if kind == "agent_stop" {
                    self.stop_at = Some(self.stop_at.map_or(at_ms, |v: f64| v.max(at_ms)));
                }
                if kind == "heartbeat" {
                    self.hb_count += 1;
                    self.hb_first = Some(self.hb_first.map_or(at_ms, |v| v.min(at_ms)));
                    self.hb_last = Some(self.hb_last.map_or(at_ms, |v| v.max(at_ms)));
                    for key in ["msgs_seen", "dropped", "oversized", "publisher_restarts"] {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        let v = num_of(obj, key).unwrap_or(0.0) as u64;
                        match key {
                            "msgs_seen" => self.envelope.msgs_seen = v,
                            "dropped" => {
                                self.envelope.dropped = v;
                                earliest(&mut self.envelope.dropped_first, at_ms, v);
                            }
                            "oversized" => {
                                self.oversized = v;
                                earliest(&mut self.oversized_first, at_ms, v);
                            }
                            _ => self.envelope.publisher_restarts = v,
                        }
                    }
                    // The endpoints fan-out: `source` is the instance mapping spec 5.1 rests on,
                    // and the shaping's parallel arrays are read positionally. Real liveness
                    // records carry no instance_id — this is where instances come from.
                    let arr = |k: &str| {
                        obj.get(k)
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default()
                    };
                    let (sources, seen, dropped, restarts, topics) = (
                        arr("endpoints.source"),
                        arr("endpoints.msgs_seen"),
                        arr("endpoints.dropped"),
                        arr("endpoints.publisher_restarts"),
                        arr("endpoints.topic"),
                    );
                    for (i, s) in sources.iter().enumerate() {
                        let Some(source) = s.as_str().filter(|s| !s.is_empty()) else {
                            continue;
                        };
                        let cell = self.instances.entry(source.to_string()).or_default();
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        {
                            cell.msgs_seen =
                                seen.get(i).and_then(Value::as_f64).unwrap_or(0.0) as u64;
                            cell.dropped =
                                dropped.get(i).and_then(Value::as_f64).unwrap_or(0.0) as u64;
                            earliest(&mut cell.dropped_first, at_ms, cell.dropped);
                            cell.publisher_restarts =
                                restarts.get(i).and_then(Value::as_f64).unwrap_or(0.0) as u64;
                        }
                        if let Some(m) = topics
                            .get(i)
                            .and_then(Value::as_str)
                            .and_then(analyzer_segments::topic::recognize)
                        {
                            cell.topic_model = Some(m.model.to_string());
                            cell.topic_pod = Some(m.pod.to_string());
                        }
                    }
                }
            }
            // Cache records name their instance directly; they contribute membership and
            // nothing else here — their facts are the deltas' business.
            _ => {
                if let Some(inst) = str_of(obj, "instance_id") {
                    self.instances.entry(inst.to_string()).or_default();
                }
            }
        }
    }

    /// The batch's arrival facts and declared gaps.
    pub(crate) fn finish(self) -> ArrivalFacts {
        let drops = self
            .drops
            .iter()
            .map(|d| analyzer_lake::ledger::DeclaredDrop {
                incarnation: d.incarnation.clone(),
                first_seq: d.first_seq,
                last_seq: d.last_seq,
                at_ms: d.at_ms,
            })
            .collect();
        let (Some(first_at), Some(last_at)) = (self.first_at, self.last_at) else {
            // A segment with no liveness records at all (cache-only orphan tail): it still
            // arrives — membership and continuity want the row — but it carries no clock.
            // Only the drops; the arrivals row would state a time it does not have.
            return ArrivalFacts {
                segments: Vec::new(),
                drops,
            };
        };
        // One row per named instance, plus the run-level row (no instance) that carries the
        // envelope and keeps a declaration-only segment gradeable. The read side prefers named
        // rows and falls back to the run-level row exactly like the roster fan-out.
        let arrival =
            |instance: Option<&str>, cell: &PerInstance| analyzer_lake::ledger::SegmentArrival {
                incarnation: self.incarnation.clone(),
                segment_seq: self.segment_seq,
                instance: instance.map(str::to_string),
                first_record_at_ms: first_at,
                last_record_at_ms: last_at,
                heartbeat_secs: self.heartbeat_secs,
                max_segment_secs: self.max_segment_secs,
                hb_count: self.hb_count,
                hb_first_at_ms: self.hb_first,
                hb_last_at_ms: self.hb_last,
                stop_at_ms: self.stop_at,
                msgs_seen: cell.msgs_seen,
                dropped: cell.dropped,
                oversized: self.oversized,
                dropped_at_hb_first: cell.dropped_first.map_or(0, |(_, v)| v),
                oversized_at_hb_first: self.oversized_first.map_or(0, |(_, v)| v),
                publisher_restarts: cell.publisher_restarts,
                agent_version: self.agent_version.clone(),
                topic_model: cell.topic_model.clone(),
                topic_pod: cell.topic_pod.clone(),
                declared_model: self.declared_model.clone(),
                declared_pod: self.declared_pod.clone(),
                engine_version: self.engine_version.clone(),
                attributed: self.attributed,
                canary: self.canary.clone(),
            };
        let mut segments = vec![arrival(None, &self.envelope)];
        for (instance, cell) in &self.instances {
            segments.push(arrival(Some(instance), cell));
        }
        ArrivalFacts { segments, drops }
    }
}
