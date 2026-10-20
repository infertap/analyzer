// SPDX-License-Identifier: AGPL-3.0-only

//! The input log as the core reads it (`docs/lake.md`): the lineage's shard,
//! from the lake every reader of the lineage shares; its maintenance; and the served input
//! tables the core publishes from it, which readers of what arrived read instead of the log.

use crate::Error;
use analyzer_analysis::module::{Column, DayOf, Kind, Row, Table, Value};
use analyzer_lake::ledger::BatchRecord;

/// The lineage's input log. v1 runs one shard.
///
/// # Errors
/// The store cannot open a sibling connection.
pub(crate) fn log(lake: &analyzer_lake::Lake) -> Result<analyzer_lake::ledger::Ledger, Error> {
    Ok(lake.ledger(analyzer_lake::ledger::SHARD)?)
}

/// What one maintenance of the input log did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Maintained {
    /// Days dropped past retention.
    pub retired: Vec<i64>,
    /// Days folded into their manifests.
    pub compacted: Vec<i64>,
}

/// Drop the log's days older than `retention_days` and compact every open day older than
/// `horizon_days`.
pub(crate) fn maintain(
    lake: &analyzer_lake::Lake,
    retention_days: u32,
    horizon_days: u32,
) -> Result<Maintained, Error> {
    #[allow(clippy::cast_precision_loss)] // epoch milliseconds are exact in f64
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::Row(format!("input log maintenance clock: {e}")))?
        .as_millis() as f64;
    let today = analyzer_lake::ledger::day_of(now_ms);
    let log = log(lake)?;
    let retired = log.retire_before(today - i64::from(retention_days))?;
    let closed = today - i64::from(horizon_days);
    let mut compacted = Vec::new();
    for (&day, index) in &log.head()?.days {
        if day < closed && !index.compacted {
            log.compact(day)?;
            compacted.push(day);
        }
    }
    Ok(Maintained { retired, compacted })
}

/// One ingested object as readers see it: where it came from, when, how much it held, and
/// its cache facts' span.
pub const INPUT_BATCHES: Table = Table {
    name: "input_batches",
    key: &[Column::new("batch_id", Kind::Str)],
    measures: &[
        Column::new("carrier", Kind::Str),
        Column::new("source", Kind::Str),
        Column::new("object_key", Kind::Str),
        Column::new("ingested_at_ms", Kind::I64),
        Column::new("record_count", Kind::I64),
        Column::new("unknown_kinds", Kind::I64),
        Column::new("first_at_ms", Kind::NullableF64),
        Column::new("last_at_ms", Kind::NullableF64),
        Column::new("input_position", Kind::I64),
    ],
    day: DayOf::Undated,
};

/// One sealed segment's arrival facts, per instance it names (and once for the run, with
/// no instance), as the segment's own header and liveness records state them.
pub const INPUT_SEGMENTS: Table = Table {
    name: "input_segments",
    key: &[
        Column::new("incarnation", Kind::Str),
        Column::new("segment_seq", Kind::I64),
        Column::new("instance", Kind::Str),
        Column::new("batch_id", Kind::Str),
    ],
    measures: &[
        Column::new("first_record_at_ms", Kind::F64),
        Column::new("last_record_at_ms", Kind::F64),
        Column::new("heartbeat_secs", Kind::I64),
        Column::new("max_segment_secs", Kind::I64),
        Column::new("hb_count", Kind::I64),
        Column::new("hb_first_at_ms", Kind::NullableF64),
        Column::new("hb_last_at_ms", Kind::NullableF64),
        Column::new("stop_at_ms", Kind::NullableF64),
        Column::new("msgs_seen", Kind::I64),
        Column::new("dropped", Kind::I64),
        Column::new("oversized", Kind::I64),
        Column::new("dropped_at_hb_first", Kind::I64),
        Column::new("oversized_at_hb_first", Kind::I64),
        Column::new("publisher_restarts", Kind::I64),
        Column::new("agent_version", Kind::Str),
        Column::new("topic_model", Kind::Str),
        Column::new("topic_pod", Kind::Str),
        Column::new("declared_model", Kind::Str),
        Column::new("declared_pod", Kind::Str),
        Column::new("engine_version", Kind::Str),
        Column::new("attributed", Kind::U32),
        Column::new("canary", Kind::NullableStr),
        Column::new("ingested_at_ms", Kind::I64),
    ],
    day: DayOf::Undated,
};

/// One gap a segment declared in its run's sequence.
pub const INPUT_DROPS: Table = Table {
    name: "input_drops",
    key: &[
        Column::new("incarnation", Kind::Str),
        Column::new("first_seq", Kind::I64),
        Column::new("last_seq", Kind::I64),
        Column::new("batch_id", Kind::Str),
    ],
    measures: &[
        Column::new("at_ms", Kind::F64),
        Column::new("ingested_at_ms", Kind::I64),
    ],
    day: DayOf::Undated,
};

/// The stored-mass denominator, per instance, cache-fact kind, UTC day and batch: readers
/// sum over batches. `own` and `comparable` are the holder basis of spec 5.7, and
/// `incarnation` the run whose key space ([`INPUT_KEY_SPACES`]) the stores were made under.
pub const FACTS_ROLLUP_DAILY: Table = Table {
    name: "facts_rollup_daily",
    key: &[
        Column::new("instance_id", Kind::Str),
        Column::new("kind", Kind::Str),
        Column::new("day", Kind::Date),
        Column::new("batch_id", Kind::Str),
    ],
    measures: &[
        Column::new("records", Kind::I64),
        Column::new("tokens", Kind::I64),
        Column::new("incarnation", Kind::Str),
        Column::new("own", Kind::I64),
        Column::new("comparable", Kind::I64),
    ],
    day: DayOf::Date("day"),
};

/// Each run's key space: the canary its segments declared (spec 3.4). A run that declared
/// none has no row, and a reader takes it as its own unkeyed space.
pub const INPUT_KEY_SPACES: Table = Table {
    name: "input_key_spaces",
    key: &[
        Column::new("incarnation", Kind::Str),
        Column::new("canary", Kind::Str),
    ],
    measures: &[Column::new("at_ms", Kind::F64)],
    day: DayOf::Undated,
};

/// The five served input tables.
pub const INPUT_TABLES: [Table; 5] = [
    INPUT_BATCHES,
    INPUT_SEGMENTS,
    INPUT_DROPS,
    FACTS_ROLLUP_DAILY,
    INPUT_KEY_SPACES,
];

/// A count as the tables' integer column.
/// A key-path column is non-nullable, so an absent text renders as the empty string there and
/// reads back as absence through the row reader's key-path convention.
fn text(v: Option<&str>) -> Value {
    Value::Str(v.unwrap_or_default().to_string())
}

fn int(v: u64) -> Value {
    Value::I64(i64::try_from(v).unwrap_or(i64::MAX))
}

/// The tables the input publication states: the five input tables and the declared
/// deploys' markers.
const PUBLISHED: [Table; 6] = [
    INPUT_BATCHES,
    INPUT_SEGMENTS,
    INPUT_DROPS,
    FACTS_ROLLUP_DAILY,
    INPUT_KEY_SPACES,
    analyzer_analysis::emitted::MARKERS,
];

/// One committed batch as the served input tables' rows, and its declaration as a marker.
#[must_use]
pub fn rows_of(b: &BatchRecord) -> Vec<Row> {
    let row = |table: &Table, key: Vec<Value>, measures: Vec<Value>| Row {
        table: table.name,
        key,
        measures,
        gone: false,
    };
    let id = || Value::Str(b.batch_id.clone());
    let mut rows = vec![row(
        &INPUT_BATCHES,
        vec![id()],
        vec![
            Value::Str(b.carrier.clone()),
            Value::Str(b.source.clone()),
            Value::Str(b.object_key.clone()),
            int(b.ingested_at_ms),
            int(b.record_count),
            int(b.unknown_kinds),
            Value::nullable_f64(b.first_at_ms),
            Value::nullable_f64(b.last_at_ms),
            int(b.input_position),
        ],
    )];
    for a in &b.segments {
        rows.push(row(
            &INPUT_SEGMENTS,
            vec![
                Value::Str(a.incarnation.clone()),
                Value::I64(a.segment_seq),
                text(a.instance.as_deref()),
                id(),
            ],
            vec![
                Value::F64(a.first_record_at_ms),
                Value::F64(a.last_record_at_ms),
                int(a.heartbeat_secs),
                int(a.max_segment_secs),
                int(a.hb_count),
                Value::nullable_f64(a.hb_first_at_ms),
                Value::nullable_f64(a.hb_last_at_ms),
                Value::nullable_f64(a.stop_at_ms),
                int(a.msgs_seen),
                int(a.dropped),
                int(a.oversized),
                int(a.dropped_at_hb_first),
                int(a.oversized_at_hb_first),
                int(a.publisher_restarts),
                text(a.agent_version.as_deref()),
                text(a.topic_model.as_deref()),
                text(a.topic_pod.as_deref()),
                text(a.declared_model.as_deref()),
                text(a.declared_pod.as_deref()),
                text(a.engine_version.as_deref()),
                Value::U32(u32::from(a.attributed)),
                Value::nullable(a.canary.as_deref()),
                int(b.ingested_at_ms),
            ],
        ));
    }
    for d in &b.drops {
        rows.push(row(
            &INPUT_DROPS,
            vec![
                Value::Str(d.incarnation.clone()),
                Value::I64(d.first_seq),
                Value::I64(d.last_seq),
                id(),
            ],
            vec![Value::F64(d.at_ms), int(b.ingested_at_ms)],
        ));
    }
    for a in &b.annotations {
        rows.push(
            analyzer_analysis::emitted::Marker {
                at_ms: a.at_ms,
                kind: "deploy",
                label: &a.label,
                provenance: "recorded",
                from_value: None,
                to_value: None,
                source: &a.source,
                scope: a.scope.as_deref(),
                declared_at_ms: a.declared_at_ms,
            }
            .row(false),
        );
    }
    rows.extend(run_rows(b));
    rows
}

/// One batch's rows that name its run: the key space its segments declared and the
/// stored-mass denominator, which carries the run so a reader can join the two.
fn run_rows(b: &BatchRecord) -> Vec<Row> {
    let row = |table: &Table, key: Vec<Value>, measures: Vec<Value>| Row {
        table: table.name,
        key,
        measures,
        gone: false,
    };
    let id = || Value::Str(b.batch_id.clone());
    let mut rows = Vec::new();
    for a in &b.segments {
        if let Some(canary) = &a.canary {
            rows.push(row(
                &INPUT_KEY_SPACES,
                vec![
                    Value::Str(a.incarnation.clone()),
                    Value::Str(canary.clone()),
                ],
                vec![Value::F64(a.last_record_at_ms)],
            ));
        }
    }
    // One batch is one segment, of one run.
    let incarnation = b
        .segments
        .first()
        .map(|a| a.incarnation.clone())
        .unwrap_or_default();
    for m in &b.stored {
        rows.push(row(
            &FACTS_ROLLUP_DAILY,
            vec![
                Value::Str(m.instance.clone()),
                Value::Str(m.kind.clone()),
                Value::Date(m.day),
                id(),
            ],
            vec![
                int(m.records),
                Value::I64(m.tokens),
                Value::Str(incarnation.clone()),
                int(m.own),
                int(m.comparable),
            ],
        ));
    }
    rows
}

/// Publish the batches committed past the position the journal records, then record the
/// newest one. Every row is stated under the newest position published, so a retry after a
/// crash between the two restates the same rows at the same or a higher publication, and
/// converges. Returns the batches published.
pub(crate) fn publish(
    journal: &crate::journal::Journal,
    publish: &dyn crate::publish::Publish,
    fleet_id: &str,
) -> Result<u64, Error> {
    let since = journal.read()?.inputs_published;
    let batches = log(journal.lake())?.committed_since(since)?;
    let Some(through) = batches.iter().map(|b| b.input_position).max() else {
        return Ok(0);
    };
    let rows: Vec<Row> = batches.iter().flat_map(rows_of).collect();
    publish.apply(
        &crate::publish::Head {
            fleet_id,
            meaning: &analyzer_analysis::emitted::Meaning::input(),
        },
        &PUBLISHED,
        through,
        &rows,
    )?;
    journal.commit(|head| head.inputs_published = head.inputs_published.max(through))?;
    Ok(batches.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::rows_of;
    use analyzer_lake::ledger::{
        BatchRecord, DeclaredAnnotation, DeclaredDrop, SegmentArrival, StoredMass,
    };

    #[test]
    fn ingest_and_the_residency_substrate_read_locality_alike() {
        for locality in [None, Some("LOCAL"), Some("REMOTE"), Some(""), Some("local")] {
            assert_eq!(
                analyzer_segments::is_own_copy(locality),
                analyzer_analysis::events::is_own_copy(locality),
                "{locality:?}"
            );
        }
    }

    #[test]
    fn every_input_row_is_a_row_of_its_declared_table() {
        let batch = BatchRecord {
            batch_id: "b".into(),
            carrier: "segment".into(),
            source: "s".into(),
            object_key: "k".into(),
            etag: "e".into(),
            input_position: 3,
            ingested_at_ms: 1_000,
            record_count: 4,
            unknown_kinds: 0,
            first_at_ms: None,
            last_at_ms: None,
            days: Vec::new(),
            segments: vec![SegmentArrival {
                incarnation: "r".into(),
                segment_seq: 0,
                instance: Some("i".into()),
                first_record_at_ms: 1.0,
                last_record_at_ms: 2.0,
                heartbeat_secs: 60,
                max_segment_secs: 300,
                hb_count: 1,
                hb_first_at_ms: Some(1.0),
                hb_last_at_ms: None,
                stop_at_ms: None,
                msgs_seen: 1,
                dropped: 0,
                oversized: 0,
                dropped_at_hb_first: 0,
                oversized_at_hb_first: 0,
                publisher_restarts: 0,
                agent_version: Some("t".into()),
                topic_model: None,
                topic_pod: None,
                declared_model: None,
                declared_pod: None,
                engine_version: None,
                attributed: true,
                canary: Some("c0".into()),
            }],
            drops: vec![DeclaredDrop {
                incarnation: "r".into(),
                first_seq: 1,
                last_seq: 2,
                at_ms: 3.0,
            }],
            lifecycle: Vec::new(),
            stored: vec![StoredMass {
                instance: "i".into(),
                kind: "store".into(),
                day: 5,
                records: 4,
                tokens: 16,
                own: 4,
                comparable: 3,
            }],
            annotations: vec![DeclaredAnnotation {
                at_ms: 7.0,
                label: "rollout".into(),
                scope: None,
                source: "cli".into(),
                declared_at_ms: Some(8.0),
            }],
            tags: Vec::new(),
        };
        let rows = rows_of(&batch);
        assert_eq!(
            rows.len(),
            6,
            "a batch, a segment, a key space, a drop, a declared marker and a denominator row"
        );
        for row in &rows {
            let table = super::PUBLISHED
                .iter()
                .find(|t| t.name == row.table)
                .expect("a declared table");
            table.accepts(row).expect("the table accepts its row");
        }
    }
}
