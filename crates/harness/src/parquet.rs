// SPDX-License-Identifier: AGPL-3.0-only

//! The test-tree Parquet writer: a JSONL segment's twin, built with the same crate whose
//! reader is under test. The analyzer ships no writer — the tap does — so the acceptance
//! corpus generates its shipped-form halves here, dev-only, flat columns only: the corpus
//! bodies carry no nested field, and a writer that grew nesting would be re-implementing the
//! tap's converter rather than fixturing it.
//!
//! `rows_per_group` is the row-group knob the pruning proof turns: the tap's memory bound
//! yields several groups per segment, and a fixture with one group per file could never show
//! a group being skipped.

use serde_json::{Map, Value};
use std::sync::Arc;

use parquet::basic::{Compression, ConvertedType, Repetition, Type as PhysicalType};
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

#[derive(Clone, Copy, PartialEq)]
enum Col {
    Str,
    I64,
    F64,
}

/// Every wire field the corpus bodies use, with its column type. A body carrying a field this
/// table does not know fails loudly — a silently dropped field would make the differential
/// compare a twin against a different segment.
const COLUMNS: &[(&str, Col)] = &[
    ("kind", Col::Str),
    ("at_ms", Col::F64),
    ("instance_id", Col::Str),
    ("incarnation", Col::Str),
    ("block_id", Col::Str),
    ("content_id", Col::Str),
    ("parent_id", Col::Str),
    ("tier", Col::Str),
    ("locality", Col::Str),
    ("workload_class", Col::Str),
    ("canary", Col::Str),
    ("reuse_reporting", Col::Str),
    ("n_tokens", Col::I64),
    ("dp_rank", Col::I64),
    ("group_idx", Col::I64),
    ("seq", Col::I64),
    ("key_epoch", Col::I64),
    ("segment_seq", Col::I64),
    ("first_seq", Col::I64),
    ("last_seq", Col::I64),
    ("refused", Col::I64),
    ("msgs_seen", Col::I64),
    ("dropped", Col::I64),
    ("oversized", Col::I64),
    ("heartbeat_secs", Col::I64),
    ("max_segment_secs", Col::I64),
    ("window_start_ms", Col::F64),
    ("window_end_ms", Col::F64),
];

/// The Parquet twin of a JSONL segment body, snappy-compressed, `rows_per_group` rows to a
/// group, footer statistics on (the writer's default — the pruning proof reads them).
#[allow(clippy::too_many_lines)] // one schema, one writer: the column inventory IS the function
pub fn twin_of_jsonl(jsonl: &str, rows_per_group: usize) -> Vec<u8> {
    let records: Vec<Map<String, Value>> = jsonl
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<Value>(l)
                .expect("twin input parses")
                .as_object()
                .expect("twin input is an object")
                .clone()
        })
        .collect();
    for record in &records {
        for key in record.keys() {
            assert!(
                COLUMNS.iter().any(|(name, _)| name == key),
                "the test writer does not know column {key}; extend COLUMNS deliberately"
            );
        }
    }
    let columns: Vec<(&str, Col)> = COLUMNS
        .iter()
        .filter(|(name, _)| records.iter().any(|r| r.contains_key(*name)))
        .copied()
        .collect();

    let fields: Vec<Arc<Type>> = columns
        .iter()
        .map(|(name, col)| {
            let builder = match col {
                Col::Str => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8),
                Col::I64 => Type::primitive_type_builder(name, PhysicalType::INT64),
                Col::F64 => Type::primitive_type_builder(name, PhysicalType::DOUBLE),
            };
            Arc::new(
                builder
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .expect("field builds"),
            )
        })
        .collect();
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("schema builds"),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );

    let mut writer = SerializedFileWriter::new(Vec::new(), schema, props).expect("writer opens");
    for chunk in records.chunks(rows_per_group.max(1)) {
        let mut group = writer.next_row_group().expect("row group opens");
        for (name, col) in &columns {
            let mut column = group
                .next_column()
                .expect("column advances")
                .expect("schema names this column");
            let defs: Vec<i16> = chunk
                .iter()
                .map(|r| i16::from(r.get(*name).is_some_and(|v| !v.is_null())))
                .collect();
            let cells = chunk
                .iter()
                .filter_map(|r| r.get(*name))
                .filter(|v| !v.is_null());
            match col {
                Col::Str => {
                    let values: Vec<ByteArray> = cells
                        .map(|v| ByteArray::from(v.as_str().expect("a string cell")))
                        .collect();
                    column
                        .typed::<ByteArrayType>()
                        .write_batch(&values, Some(&defs), None)
                        .expect("write strings");
                }
                Col::I64 => {
                    let values: Vec<i64> =
                        cells.map(|v| v.as_i64().expect("an int64 cell")).collect();
                    column
                        .typed::<Int64Type>()
                        .write_batch(&values, Some(&defs), None)
                        .expect("write int64s");
                }
                Col::F64 => {
                    let values: Vec<f64> =
                        cells.map(|v| v.as_f64().expect("a float64 cell")).collect();
                    column
                        .typed::<DoubleType>()
                        .write_batch(&values, Some(&defs), None)
                        .expect("write float64s");
                }
            }
            column.close().expect("column closes");
        }
        group.close().expect("row group closes");
    }
    let bytes = writer.into_inner().expect("footer writes");
    assert!(bytes.starts_with(b"PAR1"), "the twin must be Parquet");
    bytes
}
