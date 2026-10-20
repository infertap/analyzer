// SPDX-License-Identifier: AGPL-3.0-only

//! The core's published state in the lake (`docs/lake.md`): a [`Publish`]
//! over the lineage's state logs, so the core reads its own output back from the lake and
//! never from serving, and a [`Published`] that publishes to the lake and then to serving.
//!
//! A table's rows go into the log of (table, meaning, day): `replace` writes each day's base,
//! `apply` a delta per day it touches, one commit a day naming them (`analyzer_lake::state`).
//! A read takes the base and the deltas above it, latest per key, with the deleted keys
//! dropped. After a delta, a log's runs compact size-tiered, the fold state's decision
//! (`analyzer_lake::fold::tier`). The encoding is the port's `RowBinary` values in the table's
//! declared order and a `gone` byte per row, snappy-framed.

use crate::publish::{Head, Publish, Scope, Written};
use crate::rowbinary::{Reader, Writer};
use crate::Error;
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::module::{DayOf, Kind, Row, Table, Value};
use analyzer_analysis::DAY_MS;
use analyzer_lake::state::{LogId, Replace, StateLogs, Write};
use std::collections::BTreeMap;

/// The encoding's version: the first byte of every log object.
const FORMAT: u8 = 1;

/// The published state of one lineage, as a [`Publish`].
pub struct LakePublish {
    logs: StateLogs,
}

impl LakePublish {
    /// The published state of `lake`'s lineage.
    ///
    /// # Errors
    /// None today; the signature keeps a store that must open a connection possible.
    pub fn of(lake: &analyzer_lake::Lake) -> Result<Self, Error> {
        Ok(Self { logs: lake.state() })
    }

    /// The rows of `table` under `meaning` on `day` (`None` for an undated table), latest per
    /// key, deleted keys dropped, in key order.
    ///
    /// # Errors
    /// The store failed, or an object does not decode as `table`'s rows.
    pub fn read(
        &self,
        table: &Table,
        meaning: &Meaning,
        day: Option<i64>,
    ) -> Result<Vec<Row>, Error> {
        let read = self.logs.read(&log_id(table, meaning, day))?;
        Ok(latest(table, &read)?
            .into_values()
            .filter(|r| !r.gone)
            .collect())
    }

    /// Keep one log's runs few after a delta: the fold state's size-tiered decision over its
    /// deltas' bytes. A merge of the newest deltas keeps a deleted key's row, which still hides
    /// the key below them; a fold into the base drops it.
    fn maintain(&self, table: &Table, meaning: &Meaning, day: Option<i64>) -> Result<(), Error> {
        let id = log_id(table, meaning, day);
        let named = self.logs.named(&id)?;
        let sizes: Vec<u64> = named.deltas.iter().map(|d| d.bytes).collect();
        let how = match analyzer_lake::fold::tier(&sizes, named.base.map(|b| b.bytes), |_| false) {
            analyzer_lake::fold::Tier::Keep => return Ok(()),
            analyzer_lake::fold::Tier::Merge(n) => Replace::Merge(n),
            analyzer_lake::fold::Tier::Fold => Replace::Fold,
        };
        let newest = named
            .deltas
            .last()
            .or(named.base.as_ref())
            .map_or(0, |o| o.publication);
        let rows: Vec<Row> = match how {
            Replace::Merge(n) => {
                let group = analyzer_lake::fold::PublishedLog {
                    base: None,
                    deltas: named.deltas[named.deltas.len() - n..].to_vec(),
                };
                latest(table, &self.logs.read_named(&id, &group)?)?
                    .into_values()
                    .collect()
            }
            Replace::Fold => latest(table, &self.logs.read_named(&id, &named)?)?
                .into_values()
                .filter(|r| !r.gone)
                .collect(),
        };
        self.logs
            .replace(&id, &named, (how, newest), &encode(table, &rows)?)?;
        Ok(())
    }

    /// Drop `table`'s log under `meaning` on `day`: a retired day.
    ///
    /// # Errors
    /// The store failed.
    pub fn drop_day(&self, table: &Table, meaning: &Meaning, day: i64) -> Result<(), Error> {
        Ok(self.logs.drop_log(&log_id(table, meaning, Some(day)))?)
    }
}

/// A log's rows by key, the latest winning: the base first, then the deltas in publication
/// order. A deleted key keeps its `gone` row.
fn latest(
    table: &Table,
    read: &analyzer_lake::state::LogRead,
) -> Result<BTreeMap<Vec<String>, Row>, Error> {
    let mut latest: BTreeMap<Vec<String>, Row> = BTreeMap::new();
    for (_, body) in read.base.iter().chain(&read.deltas) {
        for row in decode(table, body)? {
            latest.insert(row.key.iter().map(Value::text).collect(), row);
        }
    }
    Ok(latest)
}

fn log_id<'a>(table: &'a Table, meaning: &'a Meaning, day: Option<i64>) -> LogId<'a> {
    LogId {
        table: table.name,
        semantics_version: meaning.semantics_version,
        discriminator: &meaning.discriminator,
        day,
    }
}

/// The UTC day a row of `table` places in, from its day column; `None` for an undated table.
pub(crate) fn day_of(table: &Table, row: &Row) -> Result<Option<i64>, Error> {
    let column = match table.day {
        DayOf::Undated => return Ok(None),
        DayOf::Date(c) | DayOf::DateTime(c) | DayOf::Millis(c) | DayOf::Day(c) => c,
    };
    let at = table
        .key
        .iter()
        .position(|k| k.name == column)
        .ok_or_else(|| Error::Row(format!("{}: day column {column} is not a key", table.name)))?;
    #[allow(clippy::cast_possible_truncation)] // day indices are small integers
    let day_ms = DAY_MS as i64;
    match (table.day, &row.key[at]) {
        (DayOf::Date(_), Value::Date(d)) | (DayOf::Day(_), Value::I64(d)) => Ok(Some(*d)),
        (DayOf::DateTime(_), Value::DateTime(s)) => Ok(Some(s.div_euclid(86_400))),
        (DayOf::Millis(_), Value::I64(ms)) => Ok(Some(ms.div_euclid(day_ms))),
        #[allow(clippy::cast_possible_truncation)] // a finite instant's day index fits i64
        (DayOf::Millis(_), Value::F64(ms)) => Ok(Some((ms / DAY_MS).floor() as i64)),
        (_, v) => Err(Error::Row(format!(
            "{}: day column {column} holds {v:?}",
            table.name
        ))),
    }
}

/// `rows` of `table` as a log object.
pub(crate) fn encode(table: &Table, rows: &[Row]) -> Result<Vec<u8>, Error> {
    let mut w = Writer::default();
    w.u8(FORMAT);
    w.string(table.name);
    for row in rows {
        table.accepts(row)?;
        for (c, v) in table
            .key
            .iter()
            .chain(table.measures)
            .zip(row.key.iter().chain(&row.measures))
        {
            put(&mut w, v, c.kind);
        }
        w.u8(u8::from(row.gone));
    }
    let mut out = Vec::new();
    {
        use std::io::Write as _;
        let mut enc = snap::write::FrameEncoder::new(&mut out);
        enc.write_all(&w.bytes)
            .map_err(|e| Error::Spine(format!("state log encode: {e}")))?;
        enc.flush()
            .map_err(|e| Error::Spine(format!("state log encode: {e}")))?;
    }
    Ok(out)
}

fn put(w: &mut Writer, v: &Value, kind: Kind) {
    match (v, kind) {
        (Value::Str(s), Kind::Str) => w.string(s),
        (Value::Str(s), Kind::NullableStr) => w.nullable(Some(s.as_str()), Writer::string),
        (Value::Null, Kind::NullableStr) => w.nullable(None::<&str>, Writer::string),
        (Value::U32(x), Kind::NullableU32) => w.nullable(Some(*x), Writer::u32),
        (Value::Null, Kind::NullableU32) => w.nullable(None::<u32>, Writer::u32),
        (Value::U32(x), _) => w.u32(*x),
        (Value::I64(x) | Value::Date(x) | Value::DateTime(x), _) => w.i64(*x),
        (Value::F64(x), Kind::NullableF64) => w.nullable(Some(*x), Writer::f64),
        (Value::Null, Kind::NullableF64) => w.nullable(None::<f64>, Writer::f64),
        (Value::F64(x), _) => w.f64(*x),
        // The row was accepted by its table first; nothing else reaches here.
        (Value::Str(_) | Value::Null, _) => w.u8(1),
    }
}

/// A log object back into `table`'s rows.
pub(crate) fn decode(table: &Table, body: &[u8]) -> Result<Vec<Row>, Error> {
    let mut bytes = Vec::new();
    {
        use std::io::Read as _;
        snap::read::FrameDecoder::new(body)
            .read_to_end(&mut bytes)
            .map_err(|e| Error::Spine(format!("state log decode: {e}")))?;
    }
    let corrupt = |_| Error::Spine(format!("state log of {}: a truncated object", table.name));
    let mut r = Reader::new(&bytes);
    if r.u8().map_err(corrupt)? != FORMAT {
        return Err(Error::Spine(format!(
            "state log of {}: a format this build does not read",
            table.name
        )));
    }
    let name = r.string().map_err(corrupt)?;
    if name != table.name {
        return Err(Error::Spine(format!(
            "state log of {}: an object of {name}",
            table.name
        )));
    }
    let mut rows = Vec::new();
    while !r.at_end() {
        let mut take = |kind: Kind| -> Result<Value, Error> {
            Ok(match kind {
                Kind::Str => Value::Str(r.string().map_err(corrupt)?.to_string()),
                Kind::NullableStr => Value::nullable(r.nullable(Reader::string).map_err(corrupt)?),
                Kind::U32 => Value::U32(r.u32().map_err(corrupt)?),
                Kind::NullableU32 => Value::nullable_u32(r.nullable(Reader::u32).map_err(corrupt)?),
                Kind::I64 => Value::I64(r.i64().map_err(corrupt)?),
                // The lake keeps an `F32` at full width: its log is the record, and the
                // store's single precision is the store's.
                Kind::F32 | Kind::F64 => Value::F64(r.f64().map_err(corrupt)?),
                Kind::NullableF64 => Value::nullable_f64(r.nullable(Reader::f64).map_err(corrupt)?),
                Kind::Date => Value::Date(r.i64().map_err(corrupt)?),
                Kind::DateTime => Value::DateTime(r.i64().map_err(corrupt)?),
            })
        };
        let key = table
            .key
            .iter()
            .map(|c| take(c.kind))
            .collect::<Result<Vec<_>, _>>()?;
        let measures = table
            .measures
            .iter()
            .map(|c| take(c.kind))
            .collect::<Result<Vec<_>, _>>()?;
        let gone = r.u8().map_err(corrupt)? != 0;
        rows.push(Row {
            table: table.name,
            key,
            measures,
            gone,
        });
    }
    Ok(rows)
}

/// One table's rows by day; `None` is an undated table's one bucket.
type DayRows<'r> = BTreeMap<Option<i64>, Vec<&'r Row>>;

/// `rows` grouped by table and day, in the order of `tables`.
fn by_table_day<'r>(tables: &[Table], rows: &'r [Row]) -> Result<Vec<(Table, DayRows<'r>)>, Error> {
    let mut out: Vec<(Table, DayRows<'r>)> = tables.iter().map(|t| (*t, BTreeMap::new())).collect();
    for row in rows {
        let (table, days) = out
            .iter_mut()
            .find(|(t, _)| t.name == row.table)
            .ok_or_else(|| Error::Row(format!("a row of {} for no declared table", row.table)))?;
        days.entry(day_of(table, row)?).or_default().push(row);
    }
    Ok(out)
}

impl Publish for LakePublish {
    fn install(&self, _tables: &[Table]) -> Result<(), Error> {
        Ok(())
    }

    fn apply(
        &self,
        head: &Head<'_>,
        tables: &[Table],
        publication: u64,
        rows: &[Row],
    ) -> Result<Written, Error> {
        let mut written = Written::default();
        // Each day's deltas under one commit, then each log's runs kept few.
        let mut by_day: BTreeMap<Option<i64>, Vec<(Table, Vec<u8>)>> = BTreeMap::new();
        for (table, days) in by_table_day(tables, rows)? {
            for (day, day_rows) in days {
                let owned: Vec<Row> = day_rows.into_iter().cloned().collect();
                let body = encode(&table, &owned)?;
                written.rows += owned.len() as u64;
                written.bytes += body.len() as u64;
                by_day.entry(day).or_default().push((table, body));
            }
        }
        for (day, bodies) in &by_day {
            let writes: Vec<Write<'_>> = bodies
                .iter()
                .map(|(table, body)| Write {
                    id: log_id(table, head.meaning, *day),
                    base: false,
                    body,
                })
                .collect();
            self.logs.publish(*day, publication, &writes)?;
            for (table, _) in bodies {
                self.maintain(table, head.meaning, *day)?;
            }
        }
        Ok(written)
    }

    fn replace(&self, scope: &Scope<'_>, publication: u64, rows: &[Row]) -> Result<Written, Error> {
        let mut written = Written::default();
        let mut by_day: BTreeMap<i64, Vec<(Table, Vec<u8>)>> = BTreeMap::new();
        for (table, mut days) in by_table_day(scope.tables, rows)? {
            for day in scope.days {
                let owned: Vec<Row> = days
                    .remove(&Some(*day))
                    .unwrap_or_default()
                    .into_iter()
                    .cloned()
                    .collect();
                let body = encode(&table, &owned)?;
                written.rows += owned.len() as u64;
                written.bytes += body.len() as u64;
                by_day.entry(*day).or_default().push((table, body));
            }
            if let Some((day, _)) = days.into_iter().find(|(_, r)| !r.is_empty()) {
                return Err(Error::Row(format!(
                    "{}: a row on {day:?}, outside the restated days",
                    table.name
                )));
            }
        }
        // Each day's bases under one commit.
        for (day, bodies) in &by_day {
            let writes: Vec<Write<'_>> = bodies
                .iter()
                .map(|(table, body)| Write {
                    id: log_id(table, scope.head.meaning, Some(*day)),
                    base: true,
                    body,
                })
                .collect();
            self.logs.publish(Some(*day), publication, &writes)?;
        }
        Ok(written)
    }

    fn settled(&self) -> Result<bool, Error> {
        Ok(true)
    }
}

/// The lake first, then serving: the core's state is what it published, and serving is its
/// projection. A failure after the lake write leaves serving behind the lake, which the
/// attempt's retry restates.
pub struct Published<'s> {
    lake: LakePublish,
    serving: &'s dyn crate::publish::Serving,
}

impl<'s> Published<'s> {
    /// Publishing to `lake`'s lineage and then to `serving`.
    ///
    /// # Errors
    /// The lake's state logs cannot be opened.
    pub fn to(
        lake: &analyzer_lake::Lake,
        serving: &'s dyn crate::publish::Serving,
    ) -> Result<Self, Error> {
        Ok(Self {
            lake: LakePublish::of(lake)?,
            serving,
        })
    }

    /// The lake side, which the core reads its own output back from.
    #[must_use]
    pub fn lake(&self) -> &LakePublish {
        &self.lake
    }
}

impl Publish for Published<'_> {
    fn install(&self, tables: &[Table]) -> Result<(), Error> {
        self.serving.install(tables)
    }

    fn apply(
        &self,
        head: &Head<'_>,
        tables: &[Table],
        publication: u64,
        rows: &[Row],
    ) -> Result<Written, Error> {
        self.lake.apply(head, tables, publication, rows)?;
        self.serving.apply(head, tables, publication, rows)
    }

    fn replace(&self, scope: &Scope<'_>, publication: u64, rows: &[Row]) -> Result<Written, Error> {
        self.lake.replace(scope, publication, rows)?;
        self.serving.replace(scope, publication, rows)
    }

    fn settled(&self) -> Result<bool, Error> {
        self.serving.settled()
    }
}

/// Every row of a module's tables over `days` from the lake back into `state`, each through
/// [`analyzer_analysis::module::Persisted::restore`]. Returns the rows placed.
///
/// # Errors
/// The lake failed, or a row the state refuses.
pub fn read_back<P: analyzer_analysis::module::Persisted>(
    lake: &LakePublish,
    meaning: &Meaning,
    state: &mut P,
    days: &[i64],
) -> Result<u64, Error> {
    let mut placed = 0;
    for table in P::TABLES {
        let logs: Vec<Option<i64>> = match table.day {
            DayOf::Undated => vec![None],
            _ => days.iter().map(|d| Some(*d)).collect(),
        };
        for day in logs {
            for row in lake.read(table, meaning, day)? {
                let key: Vec<String> = row.key.iter().map(Value::text).collect();
                let measures: Vec<String> = row.measures.iter().map(Value::text).collect();
                let key: Vec<&str> = key.iter().map(String::as_str).collect();
                let measures: Vec<&str> = measures.iter().map(String::as_str).collect();
                state
                    .restore(table.name, &key, &measures)
                    .map_err(|e| Error::Row(e.to_string()))?;
                placed += 1;
            }
        }
    }
    Ok(placed)
}

#[cfg(test)]
mod tests {
    use super::LakePublish;
    use crate::publish::{Head, Publish, Scope};
    use analyzer_analysis::emitted::{Meaning, COVERAGE_DAILY, MARKERS};
    use analyzer_analysis::module::{Row, Value};

    fn lake(name: &str) -> (analyzer_lake::Lake, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "state-publish-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        (
            analyzer_lake::Lake::new(
                std::sync::Arc::new(analyzer_objectstore::DirStore::new(&dir)),
                "r/".into(),
                analyzer_lake::lineage::Lineage::new("t", "w").expect("lineage"),
            ),
            dir,
        )
    }

    fn coverage(day: i64, instance: &str, covered: f64, gone: bool) -> Row {
        Row {
            table: COVERAGE_DAILY.name,
            key: vec![Value::Date(day), Value::Str(instance.into())],
            measures: vec![Value::F64(covered), Value::F64(86_400_000.0)],
            gone,
        }
    }

    #[test]
    fn a_restated_day_and_its_changes_read_back_latest_per_key() {
        let (lake, dir) = lake("restate");
        let publish = LakePublish::of(&lake).expect("publish");
        let meaning = Meaning::input();
        let head = Head {
            fleet_id: "f",
            meaning: &meaning,
        };
        publish
            .replace(
                &Scope {
                    head,
                    tables: &[COVERAGE_DAILY],
                    days: &[5, 6],
                },
                3,
                &[coverage(5, "a", 1.0, false), coverage(5, "b", 2.0, false)],
            )
            .expect("replace");
        publish
            .apply(
                &head,
                &[COVERAGE_DAILY],
                4,
                &[coverage(5, "a", 9.0, false), coverage(5, "b", 0.0, true)],
            )
            .expect("apply");
        let rows = publish
            .read(&COVERAGE_DAILY, &meaning, Some(5))
            .expect("read");
        assert_eq!(rows, vec![coverage(5, "a", 9.0, false)]);
        assert!(publish
            .read(&COVERAGE_DAILY, &meaning, Some(6))
            .expect("empty day")
            .is_empty());
        assert!(
            publish
                .replace(
                    &Scope {
                        head,
                        tables: &[COVERAGE_DAILY],
                        days: &[5],
                    },
                    5,
                    &[coverage(7, "c", 1.0, false)],
                )
                .is_err(),
            "a row outside the restated days is refused"
        );
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn many_deltas_compact_into_one_base_that_reads_the_same() {
        let (lake, dir) = lake("compact");
        let publish = LakePublish::of(&lake).expect("publish");
        let meaning = Meaning::input();
        let head = Head {
            fleet_id: "f",
            meaning: &meaning,
        };
        for p in 1..=70u32 {
            let row = analyzer_analysis::emitted::Marker {
                at_ms: f64::from(p % 5),
                kind: "deploy",
                label: "l",
                provenance: "recorded",
                from_value: None,
                to_value: None,
                source: "cli",
                scope: None,
                declared_at_ms: Some(f64::from(p)),
            }
            .row(false);
            publish
                .apply(&head, &[MARKERS], u64::from(p), &[row])
                .expect("apply");
        }
        let first = publish.read(&MARKERS, &meaning, None).expect("read");
        assert_eq!(first.len(), 5);
        let again = publish
            .read(&MARKERS, &meaning, None)
            .expect("compacted read");
        assert_eq!(first, again, "the compacted base reads as the log did");
        assert_eq!(
            std::fs::read_dir(dir.join("r/state/markers/input-1/_/undated"))
                .expect("dir")
                .count(),
            1
        );
        std::fs::remove_dir_all(dir).ok();
    }

    /// Thirty deltas over a restated day, keys rewritten and deleted and written again: every
    /// read is what applying them in order gives, while the log's runs compact size-tiered
    /// and stay few. A merge that dropped a deleted key's row would let the base's row of it
    /// back.
    #[test]
    fn many_changes_read_back_in_order_while_the_runs_stay_few() {
        let (lake, dir) = lake("tiers");
        let publish = LakePublish::of(&lake).expect("publish");
        let meaning = Meaning::input();
        let head = Head {
            fleet_id: "f",
            meaning: &meaning,
        };
        let names = ["a", "b", "c", "d", "e"];
        let mut model: std::collections::BTreeMap<&str, f64> =
            names.iter().map(|n| (*n, 1.0)).collect();
        publish
            .replace(
                &Scope {
                    head,
                    tables: &[COVERAGE_DAILY],
                    days: &[5],
                },
                1,
                &names
                    .iter()
                    .map(|n| coverage(5, n, 1.0, false))
                    // Rows no delta touches: a base big enough that small deltas tier and
                    // merge before they fold.
                    .chain((0..200).map(|i| coverage(5, &format!("z{i:03}"), 1.0, false)))
                    .collect::<Vec<_>>(),
            )
            .expect("replace");
        for p in 2u32..32 {
            let name = names[p as usize % names.len()];
            let gone = p % 3 == 0;
            publish
                .apply(
                    &head,
                    &[COVERAGE_DAILY],
                    u64::from(p),
                    &[coverage(5, name, f64::from(p), gone)],
                )
                .expect("apply");
            if gone {
                model.remove(name);
            } else {
                model.insert(name, f64::from(p));
            }
            let read: std::collections::BTreeMap<String, f64> = publish
                .read(&COVERAGE_DAILY, &meaning, Some(5))
                .expect("read")
                .into_iter()
                .map(|r| match (&r.key[1], &r.measures[0]) {
                    (Value::Str(n), Value::F64(v)) => (n.clone(), *v),
                    other => panic!("{other:?}"),
                })
                .collect();
            let expected: std::collections::BTreeMap<String, f64> = model
                .iter()
                .map(|(n, v)| ((*n).to_string(), *v))
                .chain((0..200).map(|i| (format!("z{i:03}"), 1.0)))
                .collect();
            assert_eq!(read, expected, "after publication {p}");
            let runs = lake
                .state()
                .named(&super::log_id(&COVERAGE_DAILY, &meaning, Some(5)))
                .expect("named")
                .deltas
                .len();
            assert!(runs < 12, "{runs} deltas after publication {p}");
        }
        std::fs::remove_dir_all(dir).ok();
    }
}
