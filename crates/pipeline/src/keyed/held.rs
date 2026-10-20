// SPDX-License-Identifier: AGPL-3.0-only

//! The held cells (`docs/pipeline.md`): the whole
//! value of every cell of the days still open to change, for each cell family the operators
//! emit. A tick's partial cells add to them; the cells that moved are the tick's batch, written
//! to the store with the operators' state and published after, whole.
//!
//! They live in the store beside the operators' state, so a restore brings back cells and state
//! as of one tick, and a replay never adds a tick to cells that already hold it. Serving and the
//! state log are what is published from them, never where they are read back from.
//!
//! A family is any [`Persisted`] cell set: the store keeps its rows in the state log's own
//! encoding, one record per cell, under the day the cell places in.

use crate::fold_state::KeyedWriter;
use crate::Error;
use analyzer_analysis::coverage_sums::CoverageSums;
use analyzer_analysis::dup_cells::DupCells;
use analyzer_analysis::emitted::Cells;
use analyzer_analysis::instance_spans::InstanceSpans;
use analyzer_analysis::module::{table_named, Mergeable, Persisted, Row, Table, Value};
use analyzer_analysis::restated::Restated;
use analyzer_analysis::tags::{TagCells, TagIndex};
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};

/// Every held cell, a record per cell under its day.
pub(crate) const KEYED_CELLS: &str = "keyed_cells";
/// The last batch, a record per cell: what a start publishes again when its tick's intent was
/// never applied.
pub(crate) const KEYED_OUTBOX: &str = "keyed_outbox";

/// One family's held cells.
#[derive(Debug, Default)]
pub struct Held<P> {
    cells: P,
}

impl<P: Persisted + Mergeable> Held<P> {
    /// The cells.
    #[must_use]
    pub fn cells(&self) -> &P {
        &self.cells
    }

    /// A tick's partial cells, added.
    pub fn add(&mut self, partial: P) {
        self.cells.merge(partial);
    }

    /// The cells that moved since the last call, as rows: each its whole value, or its
    /// tombstone.
    pub fn drain(&mut self) -> Vec<Row> {
        P::render(&self.cells.drain_changes())
    }

    /// Forget the days before `day`: settled, their cells published and no longer open to
    /// change. Nothing is published for them, and the other days' changes stand.
    pub fn retire_before(&mut self, day: i64) {
        self.cells.retire_before(day);
    }

    /// The held cells of `lake`'s store, read back into a family.
    ///
    /// # Errors
    /// The working state failed, or a record does not decode or place.
    pub fn restore(lake: &analyzer_lake::Lake) -> Result<Self, Error> {
        let mut cells = P::default();
        for day in lake.working()?.days(KEYED_CELLS)? {
            for row in read_rows(lake, &cells_on(day), P::TABLES)? {
                let key: Vec<String> = row.key.iter().map(Value::text).collect();
                let measures: Vec<String> = row.measures.iter().map(Value::text).collect();
                cells
                    .restore(
                        row.table,
                        &key.iter().map(String::as_str).collect::<Vec<_>>(),
                        &measures.iter().map(String::as_str).collect::<Vec<_>>(),
                    )
                    .map_err(|e| Error::Row(format!("a held cell does not place: {e}")))?;
            }
        }
        cells.discard_changes();
        Ok(Self { cells })
    }
}

impl Held<Cells> {
    /// Correct the cells a re-derivation restated: `old`, the stores as the cells hold them,
    /// folded out, and `new` folded in. The changes ride the next batch like any tick's.
    pub fn correct(
        &mut self,
        old: &[analyzer_analysis::emitted::ClassifiedFact],
        new: &[analyzer_analysis::emitted::ClassifiedFact],
    ) {
        self.cells.subtract_facts(old);
        self.cells.add_facts(new);
    }
}

impl Held<DupCells> {
    /// Correct the placement cells one identity's re-derivation restated
    /// ([`DupCells::correct`]).
    pub fn correct(&mut self, old: &DupCells, new: &DupCells) {
        self.cells.correct(old, new);
    }
}

impl Held<CoverageSums> {
    /// State `source` again over `window` as `restated` holds it
    /// ([`CoverageSums::restate_source`]); returns the days that changed.
    pub fn restate(
        &mut self,
        source: &str,
        window: analyzer_analysis::Window,
        restated: &CoverageSums,
    ) -> std::collections::BTreeSet<i64> {
        self.cells.restate_source(source, window, restated)
    }
}

impl Held<InstanceSpans> {
    /// State `source`'s spans again over `window` as `restated` holds them
    /// ([`InstanceSpans::restate_source`]); returns the days that changed.
    pub fn restate(
        &mut self,
        source: &str,
        window: analyzer_analysis::Window,
        restated: &InstanceSpans,
    ) -> std::collections::BTreeSet<i64> {
        self.cells.restate_source(source, window, restated)
    }
}

impl Held<TagCells> {
    /// Correct the tag cells a re-derivation restated: `old` taken out and `new` put in, each
    /// attributed by `index` under `skew_ms`.
    pub fn correct(
        &mut self,
        old: &[analyzer_analysis::emitted::ClassifiedFact],
        new: &[analyzer_analysis::emitted::ClassifiedFact],
        index: &TagIndex,
        skew_ms: f64,
    ) {
        self.cells.apply(old, index, skew_ms, -1);
        self.cells.apply(new, index, skew_ms, 1);
    }

    /// Move `facts` from `before`'s attribution to `now`'s: a restated declaration.
    pub fn restate(
        &mut self,
        facts: &[analyzer_analysis::emitted::ClassifiedFact],
        (before, now): (&TagIndex, &TagIndex),
        skew_ms: f64,
    ) {
        self.cells.apply(facts, before, skew_ms, -1);
        self.cells.apply(facts, now, skew_ms, 1);
    }
}

/// The held-cell relation of `day`.
fn cells_on(day: i64) -> Relation<'static> {
    Relation {
        name: KEYED_CELLS,
        scope: Scope::Day(day),
    }
}

fn outbox() -> Relation<'static> {
    Relation {
        name: KEYED_OUTBOX,
        scope: Scope::Lineage,
    }
}

/// A cell's record: keyed by its table and key, hashed over them, its row in the state log's
/// encoding.
fn record(tables: &[Table], row: &Row, version: f64) -> Result<Record, Error> {
    let table = table_named(tables, row.table)
        .ok_or_else(|| Error::Row(format!("{}: not one of the family's tables", row.table)))?;
    let mut key = row.table.as_bytes().to_vec();
    for value in &row.key {
        key.push(0);
        key.extend_from_slice(value.text().as_bytes());
    }
    let hash = analyzer_lake::stripe::key_hash(&key);
    Ok(Record {
        hash,
        key,
        version,
        gone: row.gone,
        payload: crate::state::encode(table, std::slice::from_ref(row))?,
    })
}

fn read_rows(
    lake: &analyzer_lake::Lake,
    relation: &Relation<'_>,
    tables: &[Table],
) -> Result<Vec<Row>, Error> {
    let mut rows = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, relation, stripe, None)? {
            let name = record.key.split(|b| *b == 0).next().unwrap_or_default();
            let Some(table) = tables.iter().find(|t| t.name.as_bytes() == name) else {
                continue;
            };
            rows.extend(crate::state::decode(table, &record.payload)?);
        }
    }
    Ok(rows)
}

/// Write `rows`, cells of family `P`, under `version`, each into its day's held record.
///
/// # Errors
/// A row places in no day, or the working state failed.
pub(crate) fn write_cells<P: Persisted>(
    lake: &analyzer_lake::Lake,
    rows: &[Row],
    version: f64,
) -> Result<(), Error> {
    let mut by_day: std::collections::BTreeMap<i64, Vec<(usize, Record)>> =
        std::collections::BTreeMap::new();
    for row in rows {
        let table = table_named(P::TABLES, row.table)
            .ok_or_else(|| Error::Row(format!("{}: not one of the family's tables", row.table)))?;
        let day = crate::state::day_of(table, row)?
            .ok_or_else(|| Error::Row(format!("{}: a held cell places in no day", row.table)))?;
        let held = record(P::TABLES, row, version)?;
        by_day
            .entry(day)
            .or_default()
            .push((analyzer_lake::stripe::of(held.hash), held));
    }
    for (day, records) in by_day {
        let writer = KeyedWriter::open(lake, cells_on(day), false)?;
        writer.put(records)?;
        writer.commit()?;
    }
    Ok(())
}

/// Restate the outbox as `batch`, under `version`: every row as it is sent, a tombstone kept
/// as the row that deletes.
///
/// # Errors
/// A row is not of its family's tables, or the working state failed.
pub(crate) fn write_outbox(
    lake: &analyzer_lake::Lake,
    batch: &Batch,
    version: f64,
) -> Result<(), Error> {
    let mut boxed = Vec::new();
    for (tables, rows) in batch.families() {
        for row in rows {
            let mut sent = record(tables, row, version)?;
            sent.gone = false;
            boxed.push((analyzer_lake::stripe::of(sent.hash), sent));
        }
    }
    let writer = KeyedWriter::open(lake, outbox(), true)?;
    writer.put(boxed)?;
    writer.commit()
}

/// The outbox: the last batch, as it was sent.
///
/// # Errors
/// The working state failed, or a record does not decode.
pub(crate) fn read_outbox(lake: &analyzer_lake::Lake) -> Result<Batch, Error> {
    Ok(Batch {
        classes: read_rows(lake, &outbox(), Cells::TABLES)?,
        placement: read_rows(lake, &outbox(), DupCells::TABLES)?,
        coverage: read_rows(lake, &outbox(), CoverageSums::TABLES)?,
        roster: read_rows(lake, &outbox(), ROSTER_TABLES)?,
        blocks: read_rows(lake, &outbox(), analyzer_analysis::block_mass::TABLES)?,
        whales: read_rows(lake, &outbox(), analyzer_analysis::whale_days::TABLES)?,
        tags: read_rows(lake, &outbox(), TagCells::TABLES)?,
        declarations: read_rows(lake, &outbox(), DECLARATION_TABLES)?,
        restated: read_rows(lake, &outbox(), Restated::TABLES)?,
        spans: read_rows(lake, &outbox(), InstanceSpans::TABLES)?,
        days: Vec::new(),
    })
}

/// Drop the held records of every day before `day`.
///
/// # Errors
/// The working state failed.
pub(crate) fn drop_before(lake: &analyzer_lake::Lake, day: i64) -> Result<(), Error> {
    let working = lake.working()?;
    for held in working.days(KEYED_CELLS)?.into_iter().filter(|d| *d < day) {
        working.drop_relation(&cells_on(held))?;
    }
    Ok(())
}

/// The roster's one table.
const ROSTER_TABLES: &[Table] = &[analyzer_analysis::roster::FLEET_INVENTORY];

/// The declared tag intervals' one table.
const DECLARATION_TABLES: &[Table] = &[analyzer_analysis::tags::TAG_INTERVALS];

/// One tick's cells that moved, every family, as the rows to publish whole.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Batch {
    /// The class and band cells.
    pub classes: Vec<Row>,
    /// The placement cells.
    pub placement: Vec<Row>,
    /// The coverage cells.
    pub coverage: Vec<Row>,
    /// The roster's rows.
    pub roster: Vec<Row>,
    /// The blocks by stored mass: each touched day's ranked rows and its day row.
    pub blocks: Vec<Row>,
    /// The whale register: each stated day's cut and its totals.
    pub whales: Vec<Row>,
    /// The tag cells.
    pub tags: Vec<Row>,
    /// The declared tag intervals that changed.
    pub declarations: Vec<Row>,
    /// What the correction paths restated, per hour and family.
    pub restated: Vec<Row>,
    /// The pair basis: each instance's observed spans per day.
    pub spans: Vec<Row>,
    /// The days settled or restated since the last batch, for the settled-day hook.
    pub days: Vec<super::SettledDay>,
}

impl Batch {
    /// Whether nothing moved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.days.is_empty() && self.families().iter().all(|(_, rows)| rows.is_empty())
    }

    /// Each family's tables and rows.
    #[must_use]
    pub fn families(&self) -> [(&'static [Table], &[Row]); 10] {
        [
            (Cells::TABLES, &self.classes),
            (DupCells::TABLES, &self.placement),
            (CoverageSums::TABLES, &self.coverage),
            (ROSTER_TABLES, &self.roster),
            (analyzer_analysis::block_mass::TABLES, &self.blocks),
            (analyzer_analysis::whale_days::TABLES, &self.whales),
            (TagCells::TABLES, &self.tags),
            (DECLARATION_TABLES, &self.declarations),
            (Restated::TABLES, &self.restated),
            (InstanceSpans::TABLES, &self.spans),
        ]
    }
}
