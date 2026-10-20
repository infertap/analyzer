// SPDX-License-Identifier: AGPL-3.0-only

//! The columnar decode: segment bytes in,
//! IR-ready shaped rows out, with the two reads the derivation's access pattern pays for —
//! row groups pruned by their `at_ms` footer statistics against a window BEFORE any decode,
//! and columns limited to a caller's projection.
//!
//! **Both encodings stay first-class, dispatched on content exactly as [`decode_segment`]
//! dispatches**: `PAR1` routes here, a JSON object routes through the
//! row path — the defensive acceptance spec v1.9 keeps, and the corpus surface the
//! differential exercises.
//!
//! **Pruning and projection are decode-cost levers, never filters.** A surviving row group may
//! carry rows outside the window and they are returned; the caller applies its own exact
//! predicates, because no semantic rule belongs below the `EventSource` seam. A row group is
//! skipped only when its statistics PROVE every row falls outside the window; absent or
//! unreadable statistics decode.
//!
//! **The declarations still come from row zero, whatever the projection says.** A segment
//! carries its identity in its first record (`conformance/delivery/layout.json`), and the
//! header's provenance may sit in any column — so the decode probes file row zero with every
//! column and one-row limit, then applies the declarations through the same
//! `batch_declarations` and stamping the row path uses. One shaping path, two decoders.
//!
//! [`decode_segment`]: crate::decode_segment

use crate::Error;
use crate::{batch_declarations, content_batch_id, parse_lines, stamped_row};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::statistics::Statistics;
use serde_json::{Map, Value};
use std::path::Path;

/// One segment decoded under a projection: shaped rows, declarations applied.
///
/// `rows` holds what the DECODED row groups yielded — under a window that is a superset of the
/// window's rows, never a subset — so no record count is reported here: a pruned decode cannot
/// state the file's total without reading it.
#[derive(Debug)]
pub struct ProjectedSegment {
    /// The content hash — the same delivery identity whichever path decodes the bytes,
    /// because it is a function of the bytes alone.
    pub batch_id: String,
    /// Shaped rows in file order, in the wire contract's own field names, each carrying the
    /// projected columns that were non-null plus the header's stamped declarations.
    pub rows: Vec<Value>,
    /// Records whose `kind` the shaping did not recognize — landed whole, fields under
    /// `extra`, and counted here as drift rather than refused.
    pub unknown_kinds: u64,
}

/// What a projected decode reads: the columns, the kinds, and the window the rows' instants
/// must fall in. `kind` and `at_ms` ride every projection and are not listed.
#[derive(Debug, Clone, Copy, Default)]
pub struct Projection<'a> {
    /// The columns to decode, by the contract's field names.
    pub columns: &'a [&'a str],
    /// The kinds to keep; every other kind is skipped, and an unknown one counted.
    pub kinds: &'a [&'a str],
    /// The half-open instant window the kept rows must fall in; `None` keeps every instant.
    pub window: Option<(f64, f64)>,
}

/// Decode a segment under a projection, a kind set, and an optional `[start_ms, end_ms)`
/// pruning window, dispatching on content: `PAR1` to the columnar path, a JSON object to the
/// row path.
///
/// `projection` names record-model fields (`"endpoints"` covers the nested element columns;
/// `"extra"` the provenance map); empty projects everything. `kind` and `at_ms` are always
/// decoded — the shaping needs the first, the window the second.
///
/// `kinds` is the caller's kind predicate, applied BEFORE the shaping — the same selection
/// every loader states as `kind IN (...)`, and here for the same two reasons: a family's
/// projection need not carry the fields the shaping fails closed on for kinds the caller never
/// asked about (a `store` without its `block_id` column is corrupt; a lifecycle load that
/// decoded no `block_id` column is just not asking), and a column no surviving kind carries is
/// never decoded at all. Empty keeps every kind. No SEMANTIC rule rides here — kind selection
/// is scope, and the rules that read the rows stay above the seam.
///
/// The JSONL path decodes whole (there is nothing to skip) and applies the same kind
/// selection; a caller reading only its projected fields cannot tell the forms apart.
///
/// # Errors
/// Corrupt input on either path, or a decoded value outside the contract schema's type
/// vocabulary.
pub fn decode_segment_projected(
    source: &str,
    fleet_id: &str,
    bytes: Vec<u8>,
    projection: Projection<'_>,
) -> Result<ProjectedSegment, Error> {
    let mut rows: Vec<Value> = Vec::new();
    let streamed =
        decode_segment_projected_into(source, fleet_id, bytes, projection, &mut |mut batch| {
            rows.append(&mut batch);
            Ok(())
        })?;
    Ok(ProjectedSegment {
        batch_id: streamed.batch_id,
        rows,
        unknown_kinds: streamed.unknown_kinds,
    })
}

/// Rows per sink batch on the streaming decode — the decoded-memory bound the encoded
/// size cannot be. The rig found the failure the measured way: a shared-KB
/// segment's near-identical rows compress so well that one file materializes to tens of GiB
/// of owned rows, past any cgroup ceiling. Encoded
/// bytes bound nothing about decoded rows when the encoding is a dictionary over
/// repetition — the workload this product SELLS TO maximizes exactly that ratio — so
/// decoded memory is bounded by ROWS: the byte lesson, decoded direction.
pub const STREAM_BATCH_ROWS: usize = 8_192;

/// What the streaming decode returns once the rows have flowed: the identity the caller
/// folds under, and the count the contract asks it to keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedSegment {
    /// The content hash of the bytes — the delivery identity, the same whichever path
    /// decoded them.
    pub batch_id: String,
    /// Records whose `kind` the shaping did not recognize — landed whole, fields under
    /// `extra`, and counted here as drift rather than refused.
    pub unknown_kinds: u64,
}

/// [`decode_segment_projected`], streaming: `sink` receives the stamped rows in batches
/// of at most [`STREAM_BATCH_ROWS`], and nothing holds a whole segment's rows at once on
/// the Parquet path. The JSONL path parses whole — its rows are bounded by the
/// producer's 64 MiB seal, which is the bound the Parquet path cannot claim and the
/// whole reason this twin exists.
///
/// # Errors
/// As [`decode_segment_projected`], plus the sink's own failure.
pub fn decode_segment_projected_into(
    source: &str,
    fleet_id: &str,
    bytes: Vec<u8>,
    projection: Projection<'_>,
    sink: &mut dyn FnMut(Vec<Value>) -> Result<(), Error>,
) -> Result<StreamedSegment, Error> {
    let batch_id = content_batch_id(&bytes);
    if bytes.starts_with(b"PAR1") {
        // `Bytes::from(Vec)` takes the caller's allocation whole — a cache hit already paid
        // for these bytes once, and `copy_from_slice` would copy every segment again on every
        // load.
        let unknown_kinds = crate::contain_reader_panic(source, || {
            stream_projected_rows(
                source,
                fleet_id,
                &batch_id,
                bytes::Bytes::from(bytes),
                projection,
                sink,
            )
        })?;
        return Ok(StreamedSegment {
            batch_id,
            unknown_kinds,
        });
    }
    let file = Path::new(source);
    let text = String::from_utf8_lossy(&bytes);
    let records = parse_lines(&text, file)?;
    let declared = batch_declarations(&records, file)?;
    let mut unknown_kinds = 0u64;
    let rows = records
        .iter()
        .filter(|r| keeps(projection.kinds, r))
        .map(|r| stamped_row(r, fleet_id, &batch_id, &declared, &mut unknown_kinds))
        .collect::<Result<Vec<_>, _>>()?;
    if !rows.is_empty() {
        sink(rows)?;
    }
    Ok(StreamedSegment {
        batch_id,
        unknown_kinds,
    })
}

/// The caller's kind predicate: empty keeps everything, and a record with NO kind is kept so
/// the shaping refuses it loudly rather than a filter hiding the corruption.
fn keeps(kinds: &[&str], record: &Map<String, Value>) -> bool {
    kinds.is_empty()
        || record
            .get("kind")
            .and_then(Value::as_str)
            .is_none_or(|k| kinds.contains(&k))
}

/// The Parquet half over any [`ChunkReader`] — public so a test can hand in an instrumented
/// reader and OBSERVE that pruning skipped a row group and projection skipped a column, rather
/// than trusting a counter this module reports about itself.
///
/// `Clone` because the read is two passes over one input: the row-zero header probe and the
/// projected read of the surviving groups. Returns the stamped rows and the unknown-kind count.
///
/// # Errors
/// Bytes that do not open as Parquet, a row that does not decode, a value outside the contract
/// schema's type vocabulary, or a declaration violation (`batch_declarations`' refusals).
pub fn projected_rows<R: ChunkReader + Clone + 'static>(
    source: &str,
    fleet_id: &str,
    batch_id: &str,
    reader: R,
    projection: Projection<'_>,
) -> Result<(Vec<Value>, u64), Error> {
    let mut rows: Vec<Value> = Vec::new();
    let unknown_kinds = stream_projected_rows(
        source,
        fleet_id,
        batch_id,
        reader,
        projection,
        &mut |mut batch| {
            rows.append(&mut batch);
            Ok(())
        },
    )?;
    Ok((rows, unknown_kinds))
}

/// The streaming core [`projected_rows`] collects from: stamped rows flow to `sink` in
/// batches of at most [`STREAM_BATCH_ROWS`] (also the arrow reader's own batch size, so
/// the decode never materializes more), and the whole-segment `Vec` stops existing on
/// this path.
///
/// One semantic met earlier rather than changed: a further `segment_open` among the rows
/// (a merged file) refused whole-segment via `batch_declarations` before; the stream
/// refuses at the row where it is met. Rows already sunk stay sunk — acceptable because
/// the ingest path, which always decodes whole, is the gate data enters through, so a
/// merged file cannot be in the store; this refusal is defense in depth meeting an
/// impossible input, not a path stored data can take.
///
/// # Errors
/// As [`projected_rows`], plus the sink's own failure.
pub(crate) fn stream_projected_rows<R: ChunkReader + Clone + 'static>(
    source: &str,
    fleet_id: &str,
    batch_id: &str,
    reader: R,
    projection: Projection<'_>,
    sink: &mut dyn FnMut(Vec<Value>) -> Result<(), Error>,
) -> Result<u64, Error> {
    let (projection, kinds, window) = (projection.columns, projection.kinds, projection.window);
    let file = Path::new(source);
    let meta = ArrowReaderMetadata::load(&reader, ArrowReaderOptions::new()).map_err(|e| {
        Error::Corrupt(format!(
            "{}: starts with the Parquet magic but does not open as Parquet ({e}) — \
             corrupt input, refusing",
            file.display()
        ))
    })?;
    let parquet_meta = meta.metadata().clone();
    let descr = parquet_meta.file_metadata().schema_descr();

    // The prune, BEFORE any decode. A group survives unless its statistics prove every row
    // outside the half-open window; a missing column, missing statistics or an unexpected
    // statistics type all decode — pruning may only remove what the footer proves absent.
    let at_ms_leaf = descr
        .columns()
        .iter()
        .position(|c| c.path().parts() == ["at_ms"]);
    let surviving: Vec<usize> = (0..parquet_meta.num_row_groups())
        .filter(|&g| match (window, at_ms_leaf) {
            (Some((start, end)), Some(leaf)) => {
                group_may_intersect(parquet_meta.row_group(g), leaf, start, end)
            }
            _ => true,
        })
        .collect();
    if surviving.is_empty() {
        return Ok(0);
    }

    // The header probe: file row zero, every column, one row — the declarations may ride any
    // column (provenance is whatever the contract does not name), so the probe cannot take the
    // caller's projection. One row's pages per segment is the cost of stamping correctly.
    let probe = ParquetRecordBatchReaderBuilder::new_with_metadata(reader.clone(), meta.clone())
        .with_row_groups(vec![0])
        .with_limit(1)
        .build()
        .map_err(|e| {
            Error::Corrupt(format!(
                "{}: header probe does not read ({e})",
                file.display()
            ))
        })?
        .next()
        .transpose()
        .map_err(|e| Error::Corrupt(format!("{}: row 0 does not decode ({e})", file.display())))?
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| record_of_row(&batch, 0))
        .transpose()
        .map_err(|e| Error::Corrupt(format!("{}: row 0: {e}", file.display())))?;

    // The projected read. `kind` and `at_ms` ride along always: the shaping dispatches on the
    // first and the window predicate reads the second, so a projection without them would
    // decode rows nothing downstream could use. An empty projection projects everything.
    let mask = if projection.is_empty() {
        ProjectionMask::all()
    } else {
        ProjectionMask::columns(descr, projection.iter().copied().chain(["kind", "at_ms"]))
    };
    let batches = ParquetRecordBatchReaderBuilder::new_with_metadata(reader, meta)
        .with_row_groups(surviving.clone())
        .with_projection(mask)
        .with_batch_size(STREAM_BATCH_ROWS)
        .build()
        .map_err(|e| Error::Corrupt(format!("{}: rows do not read ({e})", file.display())))?;
    // Each kept record, flagged where it is file row zero — the probe already carries that row,
    // and the header scan below must not count it twice.
    let row_zero_decoded = surviving.first() == Some(&0);

    // Declarations come from the probe alone on this path: stamping is per batch, so a
    // header discovered later cannot retroactively join them — it refuses instead, below.
    let mut declaration_records: Vec<Map<String, Value>> = Vec::new();
    if let Some(h) = probe {
        declaration_records.push(h);
    }
    let declared = batch_declarations(&declaration_records, file)?;

    let mut unknown_kinds = 0u64;
    let mut decoded = 0usize;
    let mut out: Vec<Value> = Vec::with_capacity(STREAM_BATCH_ROWS);
    for batch in batches {
        let batch = batch.map_err(|e| {
            Error::Corrupt(format!(
                "{}: a row group does not decode ({e})",
                file.display()
            ))
        })?;
        let kind_column = kind_column_of(&batch);
        for row in 0..batch.num_rows() {
            let is_row_zero = row_zero_decoded && decoded == 0;
            decoded += 1;
            if kind_excluded(kind_column.as_ref(), row, kinds) {
                continue;
            }
            let Some(record) = admitted_record(&batch, row, file, decoded - 1, kinds, is_row_zero)?
            else {
                continue;
            };
            out.push(stamped_row(
                &record,
                fleet_id,
                batch_id,
                &declared,
                &mut unknown_kinds,
            )?);
            if out.len() >= STREAM_BATCH_ROWS {
                sink(std::mem::take(&mut out))?;
                out.reserve(STREAM_BATCH_ROWS);
            }
        }
    }
    if !out.is_empty() {
        sink(out)?;
    }
    Ok(unknown_kinds)
}

/// One row materialized and admitted, or refused with the reason: the streaming loop's
/// per-row half. `None` is an ordinary filter miss; the two `Err`s are the contract's
/// refusals — a kindless record (spec 2.2), and a further segment header among the rows,
/// which is a merged file meeting on the streaming path the refusal it met whole-segment
/// before (the ingest gate keeps that input out of the store either way).
fn admitted_record(
    batch: &RecordBatch,
    row: usize,
    file: &Path,
    index: usize,
    kinds: &[&str],
    is_row_zero: bool,
) -> Result<Option<Map<String, Value>>, Error> {
    let record = record_of_row(batch, row)
        .map_err(|e| Error::Corrupt(format!("{}: decoded row {index}: {e}", file.display())))?;
    if !keeps(kinds, &record) {
        return Ok(None);
    }
    match record.get("kind").and_then(Value::as_str) {
        None => Err(Error::Malformed(format!(
            "{}: decoded row {index} has no string 'kind' — every record carries one \
             (spec 2.2)",
            file.display()
        ))),
        Some("segment_open") if !is_row_zero => Err(Error::Malformed(format!(
            "{}: a further segment header among the rows — a merged file refuses on the \
             streaming path exactly as it refused whole (the ingest gate keeps this input \
             out of the store)",
            file.display()
        ))),
        Some(_) => Ok(Some(record)),
    }
}

/// The batch's `kind` column where it decodes as strings — the input to the
/// pre-materialization kind filter. `kind` always rides the projection, so a missing or
/// non-string column here means the file is off-contract; returning `None` sends every row
/// through the ordinary materialize-and-refuse path rather than a filter hiding it.
fn kind_column_of(batch: &RecordBatch) -> Option<StringArray> {
    batch.schema().index_of("kind").ok().and_then(|i| {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
    })
}

/// Kind selection BEFORE the row materializes: the caller's predicate reads one cell, and
/// building the full record for a row the kind set excludes was most of a lifecycle load's
/// row cost. A null kind cell still materializes, so the shaping refuses it loudly rather
/// than a filter hiding the corruption — the same rule `keeps` states for the row path.
fn kind_excluded(kind_column: Option<&StringArray>, row: usize, kinds: &[&str]) -> bool {
    if kinds.is_empty() {
        return false;
    }
    kind_column.is_some_and(|col| !col.is_null(row) && !kinds.contains(&col.value(row)))
}

/// Whether this group's `at_ms` statistics ADMIT a row in `[start, end)`. `true` on any doubt.
fn group_may_intersect(group: &RowGroupMetaData, leaf: usize, start: f64, end: f64) -> bool {
    let bounds = match group.column(leaf).statistics() {
        Some(Statistics::Double(s)) => s.min_opt().zip(s.max_opt()).map(|(min, max)| (*min, *max)),
        // A producer that stamped integral milliseconds; the window is f64 either way.
        #[allow(clippy::cast_precision_loss)] // millisecond stamps are far below 2^53
        Some(Statistics::Int64(s)) => s
            .min_opt()
            .zip(s.max_opt())
            .map(|(min, max)| (*min as f64, *max as f64)),
        _ => None,
    };
    bounds.is_none_or(|(min, max)| min < end && max >= start)
}

/// One decoded row as the wire record it re-encodes: a non-null cell becomes the field of its
/// column's name, a null cell is ABSENT, and each `extra` entry becomes a top-level string
/// field — the same three rules `parquet_records` applies on the row path (spec 2.1, 2.2).
fn record_of_row(batch: &RecordBatch, row: usize) -> Result<Map<String, Value>, Error> {
    let schema = batch.schema();
    let mut record = Map::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);
        if field.name() == "extra" {
            for (key, value) in extra_entries(column.as_ref(), row)? {
                record.insert(key, Value::String(value));
            }
        } else if let Some(value) = json_of_cell(column.as_ref(), row)
            .map_err(|e| Error::Corrupt(format!("column {}: {e}", field.name())))?
        {
            record.insert(field.name().clone(), value);
        }
    }
    Ok(record)
}

/// One cell as the JSON value the wire form carries, `None` for a null cell (absent). The
/// accepted types are exactly the contract schema's vocabulary — utf8, int64, float64, bool,
/// `list<utf8>`, and the `endpoints` `list<struct>` whose element structs omit their null
/// fields. Anything else fails closed rather than guessing a mapping.
fn json_of_cell(array: &dyn Array, row: usize) -> Result<Option<Value>, Error> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Utf8 => Value::String(cast::<StringArray>(array)?.value(row).to_string()),
        DataType::Int64 => Value::Number(cast::<Int64Array>(array)?.value(row).into()),
        DataType::Float64 => serde_json::Number::from_f64(cast::<Float64Array>(array)?.value(row))
            .map(Value::Number)
            .ok_or(Error::Corrupt(
                "float64 cell is not a finite number".to_string(),
            ))?,
        DataType::Boolean => Value::Bool(cast::<BooleanArray>(array)?.value(row)),
        DataType::List(_) => {
            let elements = cast::<ListArray>(array)?.value(row);
            Value::Array(
                (0..elements.len())
                    .map(|j| {
                        json_of_cell(elements.as_ref(), j)?.ok_or_else(|| {
                            Error::Corrupt("list carries a null element".to_string())
                        })
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
        DataType::Struct(_) => {
            let group = cast::<StructArray>(array)?;
            let mut object = Map::new();
            for (name, column) in group.column_names().iter().zip(group.columns()) {
                if let Some(value) = json_of_cell(column.as_ref(), row)? {
                    object.insert((*name).to_string(), value);
                }
            }
            Value::Object(object)
        }
        other => {
            return Err(Error::Malformed(format!(
                "value of type {other} is outside the contract schema's type vocabulary"
            )))
        }
    }))
}

/// The `extra` map's entries for one row, both halves strings — the only shape the contract
/// writes (spec 2.1).
fn extra_entries(array: &dyn Array, row: usize) -> Result<Vec<(String, String)>, Error> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    let DataType::Map(..) = array.data_type() else {
        return Err(Error::Corrupt(
            "extra is not a map — corrupt input, refusing".to_string(),
        ));
    };
    let entries = cast::<MapArray>(array)?.value(row);
    let text = |column: &dyn Array, j: usize| -> Option<String> {
        column
            .as_any()
            .downcast_ref::<StringArray>()
            .filter(|c| !c.is_null(j))
            .map(|c| c.value(j).to_string())
    };
    (0..entries.len())
        .map(|j| {
            text(entries.column(0).as_ref(), j)
                .zip(text(entries.column(1).as_ref(), j))
                .ok_or_else(|| {
                    Error::Malformed(
                        "extra carries a non-string entry — the map is string to string \
                     (spec 2.1), refusing"
                            .to_string(),
                    )
                })
        })
        .collect()
}

fn cast<T: 'static>(array: &dyn Array) -> Result<&T, Error> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Corrupt(format!(
            "column does not decode as its declared {}",
            array.data_type()
        ))
    })
}
