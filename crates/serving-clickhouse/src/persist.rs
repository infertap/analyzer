// SPDX-License-Identifier: AGPL-3.0-only

//! The `ClickHouse` encoding behind [`crate::serving::ClickHouse`]: the framework's one
//! writer, one read-back and one retire over [`Persisted`] — what a module declares (its
//! tables' typed columns, its cells as rows) is everything this needs to put its cells in the store and take them back out. No module is
//! named here; a contributor's module gets all three by implementing the contract
//! (`docs/contributing-a-module.md`), and the two keyed modules in the tree go through it.
//!
//! **The write** encodes rows in `RowBinary` by each column's [`Kind`] — the bytes the
//! per-table renderers it replaced produced, held by the corpora and the parity job — one
//! insert per table naming its columns: the framework's head (`fleet_id, semantics_version,
//! discriminator`) before the key, its tail (`version, gone`) after the measures. **The
//! read-back** selects the head, the key rendered by kind (a date as its day index, a datetime
//! as its unix seconds) and the measures — latest version per key, not `gone`, scoped to days
//! by the table's day column — and hands each row to [`Persisted::restore`] as text. **The
//! retire** deletes a day's rows older than a version, table by table, asynchronously: the
//! eventually consistent replacement `publish::ClickHouse` documents, which answers
//! `settled` once it has drained.

use crate::engine::{quote_ident, quote_literal, sql_i64, Engine};
use crate::rowbinary::Writer;
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::module::{table_named, DayOf, Kind, Persisted, Row, Table, Value};
use analyzer_pipeline::Error;

/// What every row of a module's tables carries beside its own columns: the fleet, the meaning
/// pair, and the version a write states under.
#[derive(Debug, Clone, Copy)]
pub struct Head<'a> {
    /// The fleet the rows belong to.
    pub fleet_id: &'a str,
    /// The meaning pair (semantics version, discriminator) the rows are stated under.
    pub meaning: &'a Meaning,
    /// The version the rows are stated under: the latest per key is the cell.
    pub version: u64,
}

pub use analyzer_pipeline::publish::Written;

/// The columns before a module's key, in every table.
const HEAD_COLUMNS: [&str; 3] = ["fleet_id", "semantics_version", "discriminator"];
/// The columns after a module's measures, in every table.
const TAIL_COLUMNS: [&str; 2] = ["version", "gone"];

/// A UTC day in milliseconds, as an integer: the one definition, narrowed here for the
/// day-column arithmetic.
#[allow(clippy::cast_possible_truncation)] // 86,400,000 is exact
const DAY_MS: i64 = analyzer_analysis::DAY_MS as i64;
const DAY_SECS: i64 = DAY_MS / 1000;

/// The store's type for a column kind, as the keyed tables in the migrations spell it.
fn store_type(kind: Kind) -> &'static str {
    match kind {
        Kind::Str => "String",
        Kind::NullableStr => "Nullable(String)",
        Kind::U32 => "UInt32",
        Kind::I64 => "Int64",
        Kind::F32 => "Float32",
        Kind::F64 => "Float64",
        Kind::NullableF64 => "Nullable(Float64)",
        Kind::NullableU32 => "Nullable(UInt32)",
        Kind::Date => "Date",
        Kind::DateTime => "DateTime('UTC')",
    }
}

/// Create each declared table where it does not exist, in the shape every keyed table
/// has: the framework's head, the table's key and measures, the framework's tail, and the
/// latest version per key served (`ReplacingMergeTree(version, gone)`, ordered by head and
/// key). A table that exists is left as it is.
///
/// # Errors
/// The engine refused a statement.
pub(crate) fn install_tables(engine: &Engine, tables: &[Table]) -> Result<(), Error> {
    for table in tables {
        // Every identifier through quote_ident; the types are this module's own literals.
        let mut columns: Vec<String> = HEAD_COLUMNS
            .iter()
            .map(|c| format!("{} LowCardinality(String)", quote_ident(c)))
            .collect();
        columns.extend(
            table
                .key
                .iter()
                .chain(table.measures)
                .map(|c| format!("{} {}", quote_ident(c.name), store_type(c.kind))),
        );
        columns.push(format!("{} UInt64 DEFAULT 0", quote_ident("version")));
        columns.push(format!("{} UInt8 DEFAULT 0", quote_ident("gone")));
        let order: Vec<String> = HEAD_COLUMNS
            .iter()
            .copied()
            .chain(table.key.iter().map(|c| c.name))
            .map(quote_ident)
            .collect();
        engine.run(&format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = {}ReplacingMergeTree(version, gone) \
             ORDER BY ({}) SETTINGS allow_nullable_key = 1",
            quote_ident(table.name),
            columns.join(", "),
            crate::engine::ENGINE_FAMILY,
            order.join(", "),
        ))?;
    }
    Ok(())
}

/// The insert's column list: head, key, measures, tail — the statement's own order, so a
/// column added to the table cannot shift what a row means.
pub(crate) fn columns(table: &Table) -> Vec<&'static str> {
    HEAD_COLUMNS
        .into_iter()
        .chain(table.key.iter().map(|c| c.name))
        .chain(table.measures.iter().map(|c| c.name))
        .chain(TAIL_COLUMNS)
        .collect()
}

/// Calendar values must fit the declared wire type; never silently change an instant.
fn date16(days: i64) -> Result<u16, Error> {
    u16::try_from(days).map_err(|_| Error::Row("Date day is outside UInt16".into()))
}

fn datetime32(secs: i64) -> Result<u32, Error> {
    u32::try_from(secs).map_err(|_| Error::Row("DateTime seconds are outside UInt32".into()))
}

/// One value onto the wire, in its column's encoding. The row was accepted by its table
/// first, so a value of the wrong kind cannot reach here; it is refused all the same.
fn encode(w: &mut Writer, v: &Value, kind: Kind, at: &str) -> Result<(), Error> {
    match (v, kind) {
        (Value::Str(s), Kind::Str) => w.string(s),
        (Value::Str(s), Kind::NullableStr) => w.nullable(Some(s.as_str()), Writer::string),
        (Value::Null, Kind::NullableStr) => w.nullable(None::<&str>, Writer::string),
        (Value::U32(x), Kind::U32) => w.u32(*x),
        (Value::U32(x), Kind::NullableU32) => w.nullable(Some(*x), Writer::u32),
        (Value::Null, Kind::NullableU32) => w.nullable(None::<u32>, Writer::u32),
        (Value::I64(x), Kind::I64) => w.i64(*x),
        #[allow(clippy::cast_possible_truncation)] // the column is single precision
        (Value::F64(x), Kind::F32) => w.f32(*x as f32),
        (Value::F64(x), Kind::F64) => w.f64(*x),
        (Value::F64(x), Kind::NullableF64) => w.nullable(Some(*x), Writer::f64),
        (Value::Null, Kind::NullableF64) => w.nullable(None::<f64>, Writer::f64),
        (Value::Date(d), Kind::Date) => w.u16(date16(*d)?),
        (Value::DateTime(s), Kind::DateTime) => w.u32(datetime32(*s)?),
        (v, kind) => return Err(Error::Row(format!("{at}: {v:?} is not a {kind:?}"))),
    }
    Ok(())
}

/// One table's rows as a `RowBinary` body under `head`, each row held to the table's spec.
fn body(head: &Head<'_>, table: &Table, rows: &[&Row]) -> Result<Vec<u8>, Error> {
    let mut w = Writer::default();
    for row in rows {
        table.accepts(row)?;
        w.string(head.fleet_id);
        w.string(head.meaning.semantics_version);
        w.string(head.meaning.discriminator.as_str());
        for (c, v) in table
            .key
            .iter()
            .chain(table.measures)
            .zip(row.key.iter().chain(&row.measures))
        {
            encode(&mut w, v, c.kind, &format!("{}.{}", table.name, c.name))?;
        }
        w.u64(head.version);
        w.u8(u8::from(row.gone));
    }
    Ok(w.bytes)
}

/// Write `rows` — each a row of one of `tables` — under `head`: one insert per table that has
/// any, in the tables' order. A row of a table not declared is refused, and the write is not
/// attempted.
///
/// # Errors
/// A row its table refuses, a row of an undeclared table, or the engine's failure.
pub(crate) fn write_tables(
    engine: &Engine,
    head: &Head<'_>,
    tables: &[Table],
    rows: &[Row],
) -> Result<Written, Error> {
    if let Some(stray) = rows.iter().find(|r| table_named(tables, r.table).is_none()) {
        return Err(Error::Row(format!(
            "{}: not one of the module's tables",
            stray.table
        )));
    }
    let mut written = Written::default();
    for table in tables {
        let of_table: Vec<&Row> = rows.iter().filter(|r| r.table == table.name).collect();
        if of_table.is_empty() {
            continue;
        }
        let body = body(head, table, &of_table)?;
        // The table through quote_ident; every column name through `Writer::columns`, which
        // quotes each; the rows a binary stream, nothing spliced.
        let into = quote_ident(table.name);
        engine.insert_stream(
            &format!(
                "INSERT INTO {into} ({}) FORMAT RowBinary",
                Writer::columns(&columns(table))
            ),
            &mut &body[..],
        )?;
        written.rows += of_table.len() as u64;
        written.bytes += body.len() as u64;
    }
    Ok(written)
}

/// The predicate placing a table's rows in `day`: a range on the day column in the column's
/// own unit, so the store's index on it serves the read and the delete.
pub(crate) fn day_range(day_of: DayOf, day: i64) -> Result<String, Error> {
    analyzer_analysis::Window::whole_days(day, day)?;
    // The column through quote_ident, every bound through sql_i64.
    Ok(match day_of {
        DayOf::Date(col) => {
            let c = quote_ident(col);
            format!(
                "{c} >= toDate({}) AND {c} < toDate({})",
                sql_i64(day),
                sql_i64(day + 1)
            )
        }
        DayOf::DateTime(col) => {
            let c = quote_ident(col);
            format!(
                "{c} >= toDateTime({}, 'UTC') AND {c} < toDateTime({}, 'UTC')",
                sql_i64(day * DAY_SECS),
                sql_i64((day + 1) * DAY_SECS)
            )
        }
        DayOf::Millis(col) => {
            let c = quote_ident(col);
            format!(
                "{c} >= {} AND {c} < {}",
                sql_i64(day * DAY_MS),
                sql_i64((day + 1) * DAY_MS)
            )
        }
        DayOf::Day(col) => format!("{} = {}", quote_ident(col), sql_i64(day)),
        DayOf::Undated => {
            return Err(Error::Derivation(
                "an undated table holds standing facts and has no day to scope".into(),
            ))
        }
    })
}

/// The rows of `days` — all when empty — as a predicate fragment, or nothing.
fn days_scope(day_of: DayOf, days: &[i64]) -> Result<String, Error> {
    if days.is_empty() {
        return Ok(String::new());
    }
    let ranges: Vec<String> = days
        .iter()
        .map(|d| day_range(day_of, *d).map(|range| format!("({range})")))
        .collect::<Result<_, _>>()?;
    Ok(format!(" AND ({})", ranges.join(" OR ")))
}

/// One key column as the read-back renders it: a date as its day index, a datetime as its
/// unix seconds, anything else as itself — the scalars [`Value::text`] spells.
fn key_expr(name: &str, kind: Kind) -> String {
    let c = quote_ident(name);
    match kind {
        Kind::Date => format!("toInt32({c})"),
        Kind::DateTime => format!("toUnixTimestamp({c})"),
        Kind::Str
        | Kind::NullableStr
        | Kind::U32
        | Kind::NullableU32
        | Kind::I64
        | Kind::F32
        | Kind::F64
        | Kind::NullableF64 => c,
    }
}

/// Retire every version older than `head.version` of the day's rows in `tables`: what a
/// whole-day write leaves behind — a previous statement, a key the fold no longer produces, a
/// row nothing produced — one bounded delete per table, queued and not waited for.
///
/// # Errors
/// Engine failures.
pub(crate) fn retire_older_tables(
    engine: &Engine,
    head: &Head<'_>,
    tables: &[Table],
    day: i64,
) -> Result<(), Error> {
    for table in tables {
        // Bound immediately above the statement: the table through quote_ident, the fleet and
        // the meaning pair through quote_literal, the version through sql_i64, the day range
        // through `day_range`'s own bindings.
        let into = quote_ident(table.name);
        let fleet = quote_literal(head.fleet_id);
        let sem = quote_literal(head.meaning.semantics_version);
        let disc = quote_literal(head.meaning.discriminator.as_str());
        let before = sql_i64(i64::try_from(head.version).unwrap_or(i64::MAX));
        let range = day_range(table.day, day)?;
        engine.run(&format!(
            "DELETE FROM {into} WHERE fleet_id = {fleet} AND semantics_version = {sem} \
               AND discriminator = {disc} AND {range} AND version < {before} \
             SETTINGS lightweight_deletes_sync = 0"
        ))?;
    }
    Ok(())
}

/// One table's served rows — latest version per key, not `gone`, under the fleet and the
/// meaning, over `days` (all when empty) — as text fields in the read-back's order: the head,
/// the key by kind, the measures. Sorted, so two sides compare line by line.
///
/// # Errors
/// Engine failures, or a row short of its columns.
pub fn served(
    engine: &Engine,
    table: &Table,
    (fleet_id, meaning): (&str, &Meaning),
    days: &[i64],
) -> Result<Vec<Vec<String>>, Error> {
    // The table through quote_ident, the fleet and the meaning pair through quote_literal,
    // the days through `day_range`'s sql_i64 bindings; the select list is the spec's own
    // column names, each through quote_ident in `key_expr`.
    let into = quote_ident(table.name);
    let fleet = quote_literal(fleet_id);
    let sem = quote_literal(meaning.semantics_version);
    let disc = quote_literal(meaning.discriminator.as_str());
    let select: Vec<String> = HEAD_COLUMNS
        .iter()
        .map(|c| quote_ident(c))
        .chain(table.key.iter().map(|c| key_expr(c.name, c.kind)))
        .chain(table.measures.iter().map(|c| quote_ident(c.name)))
        .collect();
    let scope = days_scope(table.day, days)?;
    let out = engine.run(&format!(
        "SELECT {} FROM {into} FINAL \
         WHERE fleet_id = {fleet} AND semantics_version = {sem} AND discriminator = {disc} \
           AND gone = 0{scope}",
        select.join(", ")
    ))?;
    let width = select.len();
    let mut rows: Vec<Vec<String>> = out
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let fields: Vec<String> = line.split('\t').map(str::to_string).collect();
            if fields.len() == width {
                Ok(fields)
            } else {
                Err(Error::Row(format!(
                    "{}: a served row of {} field(s) for {width} column(s): {line}",
                    table.name,
                    fields.len()
                )))
            }
        })
        .collect::<Result<_, _>>()?;
    rows.sort();
    Ok(rows)
}

/// Every served row of the module's tables over `days` back into `state`, each through
/// [`Persisted::restore`] with the head dropped. Returns the rows placed.
///
/// # Errors
/// Engine failures, or a row the state refuses.
pub fn read_back<P: Persisted>(
    engine: &Engine,
    (fleet_id, meaning): (&str, &Meaning),
    state: &mut P,
    days: &[i64],
) -> Result<u64, Error> {
    let mut placed = 0;
    for table in P::TABLES {
        let split = HEAD_COLUMNS.len() + table.key.len();
        for fields in served(engine, table, (fleet_id, meaning), days)? {
            let (key, measures) = fields.split_at(split);
            let key: Vec<&str> = key[HEAD_COLUMNS.len()..]
                .iter()
                .map(String::as_str)
                .collect();
            let measures: Vec<&str> = measures.iter().map(String::as_str).collect();
            state
                .restore(table.name, &key, &measures)
                .map_err(|e| Error::Row(e.to_string()))?;
            placed += 1;
        }
    }
    Ok(placed)
}

#[cfg(test)]
mod tests {
    use super::{date16, datetime32, day_range, key_expr};
    use analyzer_analysis::module::{DayOf, Kind};

    #[test]
    fn the_binary_calendar_columns_are_the_engines_widths() {
        assert_eq!(date16(0).unwrap(), 0);
        assert_eq!(date16(20_000).unwrap(), 20_000);
        assert_eq!(datetime32(1_700_000_000).unwrap(), 1_700_000_000);
    }

    #[test]
    fn unrepresentable_calendar_values_are_errors() {
        for day in [-1, 65_536, i64::MAX] {
            assert!(date16(day).is_err());
        }
        for seconds in [-1, 4_294_967_296, i64::MAX] {
            assert!(datetime32(seconds).is_err());
        }
        for day in [i64::MIN, i64::MAX] {
            assert!(day_range(DayOf::Millis("minute_ms"), day).is_err());
        }
    }

    #[test]
    fn a_day_scopes_by_the_columns_own_unit() {
        assert_eq!(
            day_range(DayOf::Date("bucket"), 20_703).unwrap(),
            "`bucket` >= toDate(20703) AND `bucket` < toDate(20704)"
        );
        assert_eq!(
            day_range(DayOf::DateTime("bucket"), 1).unwrap(),
            "`bucket` >= toDateTime(86400, 'UTC') AND `bucket` < toDateTime(172800, 'UTC')"
        );
        assert_eq!(
            day_range(DayOf::Millis("minute_ms"), 1).unwrap(),
            "`minute_ms` >= 86400000 AND `minute_ms` < 172800000"
        );
        assert_eq!(day_range(DayOf::Day("day"), 7).unwrap(), "`day` = 7");
    }

    #[test]
    fn the_read_back_spells_a_calendar_column_as_its_scalar() {
        assert_eq!(key_expr("month", Kind::Date), "toInt32(`month`)");
        assert_eq!(
            key_expr("bucket", Kind::DateTime),
            "toUnixTimestamp(`bucket`)"
        );
        assert_eq!(key_expr("class", Kind::Str), "`class`");
    }
}
