// SPDX-License-Identifier: AGPL-3.0-only

//! The derivation module — the boundary every fact sits behind.
//!
//! The engine derives facts that are latent in one entity's ordered events over time. This
//! module makes that the type: a [`DerivationModule`] is one fact's fold — `(one key's ordered
//! events + the state carried from before) → (what this window contributes + the state the
//! next window resumes from)` — with the declarations a contributor answers without reading
//! the framework: its name, what it needs folded first, and the history its state requires
//! ([`Initialization`]). This pure interface owns the
//! arithmetic and typed contributions. `run_window` groups an already loaded input;
//! production execution, compatible persistence, restart and repair belong in pipeline.
//! Pipeline's build-time registrations install typed execution adapters alongside this
//! metadata. Those adapters select inputs, construct context and own publication;
//! the pure module does not acquire storage or scheduling dependencies.
//!
//! [`Mergeable`] composes contributions under their declared laws. [`Tracked`] owns
//! materialized changes, and [`DayPartitioned`] is an optional day lifecycle. [`Persisted`]
//! remains the typed storage adapter for the two day-partitioned cell families. A roster
//! has composition and change tracking but no artificial day operations.

use crate::{Error, Window};
use std::collections::{BTreeMap, BTreeSet};

/// Evidence required before a module can emit at a new output boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Initialization {
    /// The declared output interval supplies the complete input contract.
    OutputWindow,
    /// Context includes the semantic examined-history horizon before output.
    ExaminedHistory,
    /// A compatible prefix seed, or verified replay from the observation origin.
    ObservedPrefix,
}

/// Composition of correctly initialized contributions. Implementations state their own
/// sum/replacement and ordering laws; this method alone does not promise commutativity.
/// Composition requires neither persistence nor a day axis.
pub trait Mergeable: Default {
    /// Combine another contribution under this type's declared composition law.
    fn merge(&mut self, other: Self);
}

/// Materialized state with changed-key tracking, independent of its partitioning.
pub trait Tracked: Default {
    /// Changed values or tombstones drained by the persistence adapter.
    type Changes;
    /// Number of materialized keys.
    fn len(&self) -> usize;
    /// Whether there are no materialized keys.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Drain changed values and tombstones.
    fn drain_changes(&mut self) -> Self::Changes;
    /// Clear change tracking after restoration or a successful write.
    fn discard_changes(&mut self);
}

/// Optional day ownership of materialized state. Roster-like contributions have no day
/// axis and do not implement this contract.
pub trait DayPartitioned {
    /// Remove a day's keys, tracking their tombstones.
    fn clear_day(&mut self, day: i64);
    /// Forget older held days while preserving their stored output.
    fn retire_before(&mut self, before: i64);
    /// Days currently represented in held state.
    fn days(&self) -> BTreeSet<i64>;
}

/// A column's kind: what the framework encodes a value as on a write and renders it as on a
/// read-back. The framework's kinds, not a module's: a module's row is a list of these, the
/// framework never learns a module's own types, and a serving store maps each kind to a column
/// type of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A string.
    Str,
    /// An optional string: [`Value::Null`] or a string; the read-back renders null as
    /// [`NULL_TEXT`].
    NullableStr,
    /// An unsigned 32-bit integer.
    U32,
    /// An optional unsigned 32-bit integer: [`Value::Null`] or a `u32`.
    NullableU32,
    /// A signed 64-bit integer.
    I64,
    /// A single-precision float: a [`Value::F64`] the store keeps at single precision.
    F32,
    /// A double-precision float.
    F64,
    /// An optional double: [`Value::Null`] or a float; the read-back renders null as
    /// [`NULL_TEXT`].
    NullableF64,
    /// A day index (days since the epoch) in the value and on the read-back.
    Date,
    /// Unix seconds in the value and on the read-back.
    DateTime,
}

/// How the read-back spells a null: the framework's one token, which a store's text form
/// renders a null as and a module's [`Persisted::restore`] reads it back from.
pub const NULL_TEXT: &str = "\\N";

/// One value of a rendered row, typed by its column's [`Kind`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A string: a [`Kind::Str`], or a present [`Kind::NullableStr`].
    Str(String),
    /// The null of a [`Kind::NullableStr`] or a [`Kind::NullableF64`].
    Null,
    /// A [`Kind::U32`].
    U32(u32),
    /// An [`Kind::I64`].
    I64(i64),
    /// A [`Kind::F64`] or [`Kind::F32`], or a present [`Kind::NullableF64`].
    F64(f64),
    /// A [`Kind::Date`], as a day index.
    Date(i64),
    /// A [`Kind::DateTime`], as unix seconds.
    DateTime(i64),
}

impl Value {
    /// A nullable string as a value: the string, or the null.
    #[must_use]
    pub fn nullable(s: Option<&str>) -> Self {
        s.map_or(Self::Null, |s| Self::Str(s.to_string()))
    }

    /// A nullable `u32` as a value: the integer, or the null.
    #[must_use]
    pub fn nullable_u32(v: Option<u32>) -> Self {
        v.map_or(Self::Null, Self::U32)
    }

    /// A nullable float as a value: the float, or the null.
    #[must_use]
    pub fn nullable_f64(v: Option<f64>) -> Self {
        v.map_or(Self::Null, Self::F64)
    }

    /// Whether this value is of `kind` — what the framework's encoder refuses a row on.
    #[must_use]
    pub const fn is(&self, kind: Kind) -> bool {
        matches!(
            (self, kind),
            (Self::Str(_), Kind::Str | Kind::NullableStr)
                | (
                    Self::Null,
                    Kind::NullableStr | Kind::NullableF64 | Kind::NullableU32
                )
                | (Self::U32(_), Kind::U32 | Kind::NullableU32)
                | (Self::I64(_), Kind::I64)
                | (Self::F64(_), Kind::F32 | Kind::F64 | Kind::NullableF64)
                | (Self::Date(_), Kind::Date)
                | (Self::DateTime(_), Kind::DateTime)
        )
    }

    /// The text the read-back hands a [`Persisted::restore`] for this value, for a module's own
    /// round-trip pin with no store: a string as it is, an integer as its decimal, a date as its
    /// day index, a datetime as its unix seconds, a null as [`NULL_TEXT`], a float as a decimal
    /// that parses back to the same double.
    #[must_use]
    pub fn text(&self) -> String {
        match self {
            Self::Str(s) => s.clone(),
            Self::Null => NULL_TEXT.to_string(),
            Self::U32(v) => v.to_string(),
            Self::I64(v) | Self::Date(v) | Self::DateTime(v) => v.to_string(),
            Self::F64(v) => v.to_string(),
        }
    }
}

/// One column of a module's table: its name in the store and its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    /// The column's name in the store.
    pub name: &'static str,
    /// The column's type in the store.
    pub kind: Kind,
}

impl Column {
    /// A column, for a table spec written as a literal.
    #[must_use]
    pub const fn new(name: &'static str, kind: Kind) -> Self {
        Self { name, kind }
    }
}

/// How a table's rows place in a day — the column and its unit — so the framework can scope
/// a read-back and a retire to a day without knowing the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayOf {
    /// A `Date` column: the day itself.
    Date(&'static str),
    /// A `DateTime` column: the day's seconds.
    DateTime(&'static str),
    /// An `Int64` or `Float64` of epoch milliseconds: the day's milliseconds.
    Millis(&'static str),
    /// An `Int64` day index.
    Day(&'static str),
    /// No day: the table holds standing facts, which take keyed changes and are never
    /// restated or retired by day.
    Undated,
}

/// One table a module's cells persist in: its key columns after the framework's head
/// (`fleet_id, semantics_version, discriminator`), its measures before the framework's tail
/// (`version, gone`), and how its rows place in a day. In the store's own column order, so
/// the spec is a statement about the schema, and pinned against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Table {
    /// The table's name.
    pub name: &'static str,
    /// The key columns, in order.
    pub key: &'static [Column],
    /// The measure columns, in order.
    pub measures: &'static [Column],
    /// The column that places a row in its day.
    pub day: DayOf,
}

impl Table {
    /// Whether `row` is a row of this table: the right arity on both sides and every value of
    /// its column's kind. The framework's encoder refuses a row this refuses; a module's own
    /// pin holds its rendering to it with no engine.
    ///
    /// # Errors
    /// [`Error::Row`], naming what is wrong with the row.
    pub fn accepts(&self, row: &Row) -> Result<(), Error> {
        let refuse = |message: String| Error::Row {
            table: self.name,
            message,
        };
        if row.table != self.name {
            return Err(refuse(format!("a row of {} offered to it", row.table)));
        }
        for (what, columns, values) in [
            ("key", self.key, &row.key),
            ("measures", self.measures, &row.measures),
        ] {
            if columns.len() != values.len() {
                return Err(refuse(format!(
                    "{} {what} value(s) for {} column(s)",
                    values.len(),
                    columns.len()
                )));
            }
            for (c, v) in columns.iter().zip(values) {
                if !v.is(c.kind) {
                    return Err(refuse(format!("{}: {v:?} is not a {:?}", c.name, c.kind)));
                }
            }
        }
        Ok(())
    }
}

/// One rendered cell: a row of `table`, its key and measures in the table's column order —
/// or its tombstone, the key with its measures at their defaults and `gone` set.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The table the row belongs to, by name.
    pub table: &'static str,
    /// The key, one [`Value`] per key column.
    pub key: Vec<Value>,
    /// The measures, one [`Value`] per measure column.
    pub measures: Vec<Value>,
    /// The cell left the state: this row is its tombstone, hiding the key.
    pub gone: bool,
}

/// A module's cells in the store — what lets the framework's ONE writer, ONE read-back and
/// ONE retire serve every module: the tables the cells live in, the two renderings into
/// rows, and the one door a served row comes back in by.
///
/// The framework encodes a [`Row`] in the store's own wire form by its columns' [`Kind`]s,
/// scopes a day by [`Table::day`], and hands a served row back as the text the engine renders
/// each kind in ([`Value::text`]). A module parses only its own spellings — a class, a band —
/// and never a calendar; the two are held equal by a round-trip pin in each module, with no
/// engine: `restore` over `render_day`'s text is the state again.
pub trait Persisted: Tracked + DayPartitioned {
    /// The tables the cells persist in.
    const TABLES: &'static [Table];

    /// The changed cells as rows: each touched key as its value or its tombstone.
    fn render(changes: &Self::Changes) -> Vec<Row>;

    /// Every cell of `day`, as present rows — what a whole-day write states before the
    /// framework retires the day's older versions.
    fn render_day(&self, day: i64) -> Vec<Row>;

    /// One served row back into the state, placed rather than touched: `key` and `measures`
    /// as the read-back renders them, in the table's column order.
    ///
    /// # Errors
    /// A table this state does not hold, a field short or unparseable, or a spelling the
    /// fold never writes.
    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error>;
}

/// The table of `name` among `tables`.
#[must_use]
pub fn table_named<'t>(tables: &'t [Table], name: &str) -> Option<&'t Table> {
    tables.iter().find(|t| t.name == name)
}

/// A state, through every row of its `day` as text and back: what a module's round-trip pin
/// asserts equal to the state — [`Persisted::restore`] is the inverse of
/// [`Persisted::render_day`] under [`Value::text`], with the rows held to their specs.
///
/// # Errors
/// A row a table spec refuses, or a row the restore refuses.
pub fn round_trip_day<P: Persisted>(state: &P, day: i64) -> Result<P, Error> {
    let mut back = P::default();
    for row in state.render_day(day) {
        let table = table_named(P::TABLES, row.table).ok_or_else(|| Error::Parse {
            name: row.table.to_string(),
            message: "not one of the module's tables".to_string(),
        })?;
        table.accepts(&row)?;
        let key: Vec<String> = row.key.iter().map(Value::text).collect();
        let measures: Vec<String> = row.measures.iter().map(Value::text).collect();
        let key: Vec<&str> = key.iter().map(String::as_str).collect();
        let measures: Vec<&str> = measures.iter().map(String::as_str).collect();
        back.restore(row.table, &key, &measures)?;
    }
    back.discard_changes();
    Ok(back)
}

/// What one key's fold hands back for one window.
#[derive(Debug, Clone, PartialEq)]
pub struct Contribution<C, S> {
    /// This window's cells for the key.
    pub cells: C,
    /// The state the next window resumes from. A state equal to its default is not carried:
    /// the key leaves the framework's map (an identity with no open residency, a fact that
    /// carries nothing).
    pub state: S,
    /// Days whose emitted cells this key now knows to be under-stated: duplication's `n_tokens`
    /// rise, which would re-price minutes already emitted. The keyed driver does not restate
    /// them; the cells keep the lower mass. Empty for most folds of most facts.
    pub repair_days: Vec<i64>,
}

/// One fact: its fold, and what it declares.
pub trait DerivationModule: Send + Sync + 'static {
    /// The entity whose ordered events determine the fact.
    type Key: Ord + Clone + Send;
    /// What one key carries across windows — the prefix the next fold resumes from. `()` for a
    /// fact that re-derives each window whole. A returned state equal to its default is not
    /// carried.
    type State: Default + Clone + PartialEq + Send;
    /// The events this module reads: the cache stream, or the lifecycle stream.
    type Event;
    /// The fact's keyed cells for a window.
    type Cells: Mergeable + Send;
    /// The window's context a fold may read beyond its own key's events — the completeness
    /// model, the parameters, the scope closers. Built once per window by the framework; read
    /// only.
    type Ctx<'a>;

    /// The fact's name: its tables' family, its log lines, its registry entry.
    const NAME: &'static str;
    /// State warmup required independently of the output interval and execution cadence.
    const INITIALIZATION: Initialization;
    /// The modules whose window-level outcome this one's context reads, by [`Self::NAME`]:
    /// the framework folds them first ([`fold_order`]). Coarse by design — a whole window's
    /// outcome, never one key's — so a need is an ordering, not a join.
    const NEEDS: &'static [&'static str] = &[];

    /// One key, one window: fold its ordered events from the carried state; hand back what
    /// this window contributes to the cells, the state the next window resumes from, and any
    /// day it asks repaired.
    fn fold(
        &self,
        key: &Self::Key,
        events: &[Self::Event],
        state: Self::State,
        window: Window,
        ctx: &Self::Ctx<'_>,
    ) -> Contribution<Self::Cells, Self::State>;
}

/// What one window's run over a module produced: the cells of every key merged, the states
/// to carry (defaults dropped), the days asked repaired, and how many keys folded.
#[derive(Debug)]
pub struct Outcome<M: DerivationModule> {
    /// Every folded key's cells, merged.
    pub cells: M::Cells,
    /// The states to carry into the next window; a key whose state equals the default is absent.
    pub states: BTreeMap<M::Key, M::State>,
    /// The days any key asked repaired, as one set.
    pub repair_days: BTreeSet<i64>,
    /// How many keys folded: every key that carried state or had events in the window.
    pub keys: u64,
}

/// The framework's one driver, engine-free: every key that carries state or has events in
/// the window is folded once, from its carried state (default where none), over its events
/// (none where it only carries); the contributions merge; the states that are not their
/// default are carried. Nothing here names a fact — a module that needs a branch here does
/// not fit the boundary.
pub fn run_window<M: DerivationModule>(
    module: &M,
    mut grouped: BTreeMap<M::Key, Vec<M::Event>>,
    carried: &BTreeMap<M::Key, M::State>,
    window: Window,
    ctx: &M::Ctx<'_>,
) -> Outcome<M> {
    let keys: BTreeSet<M::Key> = carried
        .keys()
        .cloned()
        .chain(grouped.keys().cloned())
        .collect();
    let mut out: Outcome<M> = Outcome {
        cells: <M::Cells as Default>::default(),
        states: BTreeMap::new(),
        repair_days: BTreeSet::new(),
        keys: 0,
    };
    let rest = M::State::default();
    for key in keys {
        let events = grouped.remove(&key).unwrap_or_default();
        let state = carried.get(&key).cloned().unwrap_or_default();
        let c = module.fold(&key, &events, state, window, ctx);
        Mergeable::merge(&mut out.cells, c.cells);
        if c.state != rest {
            out.states.insert(key, c.state);
        }
        out.repair_days.extend(c.repair_days);
        out.keys += 1;
    }
    out
}

/// The order one window folds its modules in: each after the modules it NEEDS, otherwise
/// as listed. Refuses, by name, a need no module declares and a cycle among the needs.
///
/// # Errors
/// A need naming no listed module; a cycle.
pub fn fold_order<'n>(modules: &[(&'n str, &[&str])]) -> Result<Vec<&'n str>, Error> {
    let names: BTreeSet<&str> = modules.iter().map(|(n, _)| *n).collect();
    for (name, needs) in modules {
        if let Some(missing) = needs.iter().find(|n| !names.contains(*n)) {
            return Err(Error::Modules(format!(
                "module {name} needs {missing}, which no module declares"
            )));
        }
    }
    let mut placed: Vec<&'n str> = Vec::with_capacity(modules.len());
    let mut done: BTreeSet<&str> = BTreeSet::new();
    while placed.len() < modules.len() {
        let next = modules
            .iter()
            .find(|(n, needs)| !done.contains(n) && needs.iter().all(|d| done.contains(d)));
        let Some((name, _)) = next else {
            let stuck: Vec<&str> = modules
                .iter()
                .map(|(n, _)| *n)
                .filter(|n| !done.contains(n))
                .collect();
            return Err(Error::Modules(format!(
                "the modules' needs form a cycle among {}",
                stuck.join(", ")
            )));
        };
        done.insert(name);
        placed.push(name);
    }
    Ok(placed)
}

/// One module's declarations, as the registry lists them: what a contributor states and
/// what the framework reads without the module's types — its name, its initialization, and
/// what it needs folded first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModuleSpec {
    /// [`DerivationModule::NAME`]: the fact's tables' family, its log lines, its registry entry.
    pub name: &'static str,
    /// State/history requirement at activation, declared by the pure module.
    pub initialization: Initialization,
    /// [`DerivationModule::NEEDS`]: the modules folded first, by name (an ordering, never a join).
    pub needs: &'static [&'static str],
}

impl ModuleSpec {
    /// A module's own declarations, read off its trait constants.
    #[must_use]
    pub const fn of<M: DerivationModule>() -> Self {
        Self {
            name: M::NAME,
            initialization: M::INITIALIZATION,
            needs: M::NEEDS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        fold_order, run_window, Contribution, DayPartitioned, DerivationModule, Mergeable, Tracked,
    };
    use crate::Window;
    use std::collections::{BTreeMap, BTreeSet};

    /// The smallest fact that fits the boundary: events per key counted per day, sums.
    #[derive(Default)]
    struct Counts {
        cells: BTreeMap<(i64, String), u64>,
        touched: BTreeSet<(i64, String)>,
    }
    impl crate::module::Mergeable for Counts {
        fn merge(&mut self, other: Self) {
            for (k, v) in other.cells {
                self.touched.insert(k.clone());
                *self.cells.entry(k).or_default() += v;
            }
        }
    }

    impl crate::module::Tracked for Counts {
        type Changes = Vec<((i64, String), Option<u64>)>;
        fn len(&self) -> usize {
            self.cells.len()
        }
        fn drain_changes(&mut self) -> Self::Changes {
            let touched = std::mem::take(&mut self.touched);
            touched
                .into_iter()
                .map(|k| {
                    let now = self.cells.get(&k).copied();
                    (k, now)
                })
                .collect()
        }
        fn discard_changes(&mut self) {
            self.touched.clear();
        }
    }

    impl crate::module::DayPartitioned for Counts {
        fn clear_day(&mut self, day: i64) {
            let gone: Vec<_> = self.cells.keys().filter(|k| k.0 == day).cloned().collect();
            for k in gone {
                self.cells.remove(&k);
                self.touched.insert(k);
            }
        }
        fn retire_before(&mut self, before: i64) {
            self.cells.retain(|k, _| k.0 >= before);
            self.touched.retain(|k| k.0 >= before);
        }
        fn days(&self) -> BTreeSet<i64> {
            self.cells.keys().map(|k| k.0).collect()
        }
    }

    struct CountFact;

    impl DerivationModule for CountFact {
        type Key = String;
        type State = ();
        type Event = f64;
        type Cells = Counts;
        type Ctx<'a> = ();
        const NAME: &'static str = "counts";
        const INITIALIZATION: crate::module::Initialization =
            crate::module::Initialization::OutputWindow;
        fn fold(
            &self,
            key: &String,
            events: &[f64],
            (): (),
            window: Window,
            (): &(),
        ) -> Contribution<Counts, ()> {
            let mut out = Counts::default();
            for at in events {
                if *at >= window.start_ms && *at < window.end_ms {
                    #[allow(clippy::cast_possible_truncation)]
                    let day = (*at / crate::DAY_MS).floor() as i64;
                    out.touched.insert((day, key.clone()));
                    *out.cells.entry((day, key.clone())).or_default() += 1;
                }
            }
            Contribution {
                cells: out,
                state: (),
                repair_days: Vec::new(),
            }
        }
    }

    /// A fact that carries a prefix: the last event seen per key, and asks a repair when an
    /// event arrives behind it.
    struct LastSeen;

    impl DerivationModule for LastSeen {
        type Key = String;
        type State = Option<f64>;
        type Event = f64;
        type Cells = Counts;
        type Ctx<'a> = ();
        const NAME: &'static str = "last_seen";
        const INITIALIZATION: crate::module::Initialization =
            crate::module::Initialization::ObservedPrefix;
        fn fold(
            &self,
            key: &String,
            events: &[f64],
            state: Option<f64>,
            _window: Window,
            (): &(),
        ) -> Contribution<Counts, Option<f64>> {
            let mut repair_days = Vec::new();
            let mut last = state;
            for at in events {
                if last.is_some_and(|l| *at < l) {
                    #[allow(clippy::cast_possible_truncation)]
                    repair_days.push((*at / crate::DAY_MS).floor() as i64);
                }
                last = Some(last.map_or(*at, |l| l.max(*at)));
            }
            let mut cells = Counts::default();
            if let Some(l) = last {
                #[allow(clippy::cast_possible_truncation)]
                let day = (l / crate::DAY_MS).floor() as i64;
                cells.touched.insert((day, key.clone()));
                cells.cells.insert((day, key.clone()), 1);
            }
            Contribution {
                cells,
                state: last,
                repair_days,
            }
        }
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
    fn the_driver_folds_every_carried_or_touched_key_once_and_drops_a_default_state() {
        let w = Window {
            start_ms: 0.0,
            end_ms: 2.0 * crate::DAY_MS,
        };
        let mut grouped: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        grouped.insert("a".into(), vec![1.0, crate::DAY_MS + 5.0]);
        grouped.insert("b".into(), vec![3.0]);
        let mut carried: BTreeMap<String, Option<f64>> = BTreeMap::new();
        carried.insert("b".into(), Some(10.0)); // b's event at 3 is behind its prefix
        carried.insert("c".into(), Some(7.0)); // c carries and has no events this window
        let out = run_window(&LastSeen, grouped, &carried, w, &());
        assert_eq!(out.keys, 3, "a touched, b both, c carried");
        assert_eq!(out.states.len(), 3);
        assert_eq!(out.states["a"], Some(crate::DAY_MS + 5.0));
        assert_eq!(out.states["b"], Some(10.0), "the prefix's max stands");
        assert_eq!(out.states["c"], Some(7.0), "carried through untouched");
        assert_eq!(
            out.repair_days,
            [0].into_iter().collect(),
            "b's arrival behind its prefix"
        );
        assert_eq!(out.cells.len(), 3);
        // A stateless fact carries nothing: its default state is dropped.
        let mut grouped: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        grouped.insert("k".into(), vec![1.0]);
        let out = run_window(&CountFact, grouped, &BTreeMap::new(), w, &());
        assert!(out.states.is_empty(), "a default state is not carried");
        assert_eq!(out.cells.len(), 1);
    }

    #[test]
    fn a_fold_contributes_a_window_and_the_cells_merge_and_drain_by_touch() {
        let f = CountFact;
        let w = Window {
            start_ms: 0.0,
            end_ms: 2.0 * crate::DAY_MS,
        };
        let a = f.fold(
            &"k".to_string(),
            &[1.0, 2.0, crate::DAY_MS + 1.0],
            (),
            w,
            &(),
        );
        let b = f.fold(&"k".to_string(), &[3.0, 3.0 * crate::DAY_MS], (), w, &());
        assert!(a.repair_days.is_empty());
        let mut held = Counts::default();
        held.merge(a.cells);
        held.merge(b.cells);
        assert_eq!(held.len(), 2, "two days, one key");
        assert_eq!(held.days(), [0, 1].into_iter().collect());
        let changes = held.drain_changes();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0], ((0, "k".to_string()), Some(3)));
        assert!(held.drain_changes().is_empty(), "drained");
        held.clear_day(0);
        let changes = held.drain_changes();
        assert_eq!(
            changes,
            vec![((0, "k".to_string()), None)],
            "the day's key as a tombstone"
        );
        held.retire_before(2);
        assert!(held.is_empty());
        assert_eq!(CountFact::NAME, "counts");
    }

    #[test]
    fn the_fold_order_places_a_need_first_and_refuses_the_unknown_and_the_cycle() {
        let order = fold_order(&[
            ("class_band", &["coverage"]),
            ("coverage", &[]),
            ("duplication", &[]),
        ])
        .expect("a valid order");
        assert_eq!(order, vec!["coverage", "class_band", "duplication"]);
        assert!(CountFact::NEEDS.is_empty(), "the default needs nothing");
        let unknown = fold_order(&[("class_band", &["coverage"])]).expect_err("unknown");
        assert!(unknown.to_string().contains("needs coverage"), "{unknown}");
        let cycle = fold_order(&[("a", &["b"]), ("b", &["a"]), ("c", &[])]).expect_err("cycle");
        assert!(cycle.to_string().contains("cycle among a, b"), "{cycle}");
    }
}
