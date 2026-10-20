// SPDX-License-Identifier: AGPL-3.0-only

//! The duplication cells as keyed state — the producer's fold to finished values, engine-free.
//!
//! Six shapes, one per summed `replication_*` serving table: the fleet minute cells and their
//! hour and day rollups (`workload_class × copies_band × locality × bucket`), the per-instance cut
//! at hour and minute (internal duplication only), and the pair cells at day grain. None names an
//! identity, so a block whose holders do not change adds to them at a constant [`Rates`], and a
//! window's cells for every such block are one sum per key ([`DupCells::add_rates`]). Each
//! identity's own day totals (the whale register) ride its state ([`WhaleAccrual`]) and leave
//! the fold as day products (`crate::whale_days`).
//!
//! [`DupCells::fold_identity`] is the producer's own emission fold —
//! `residual::replication_minute_cells` and `residual::token_time_cells` over one identity's
//! clipped spans — a pure function a test can drive with no engine. The state accumulates across ticks: a tick adds what its window folded, and the
//! cells whose value moved since the last drain are what it writes, whole
//! ([`DupCells::drain_changes`]); a settled day leaves the state ([`DupCells::retire_before`]).
//!
//! **Measures are token-time sums (`f64`)** where the class/band cells' are counts: the same
//! keyed shape, the same drain, floating arithmetic. Order-independence is exact for the
//! integer-valued token-time the sweep produces (milliseconds × integer token counts) up to the
//! float's own rounding, which the corpus compares within.
//!
//! **The spill seam** is [`DupShape`], as [`crate::emitted::Shape`] is for the class cells: the
//! method set a spilling backing must offer, with the pure-RAM map behind it.

use crate::emitted::UNDECLARED;
use crate::events::{Event, IntervalKey, Kind, ScopeClosers};
use crate::intervals::{FoldSeed, Holder, Interval, IntervalSeed};
use crate::module::{Column, Contribution, DayOf, DerivationModule, Persisted, Row, Table, Value};
use crate::residual::{
    copy_band, fleet_flattened, locality, replication_minute_cells, token_time_cells, HolderSpans,
};
use crate::{Error, Window};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod persisted_tests;
#[cfg(test)]
mod tests;

/// One band cell's two measures: token-time, and Σ exact-copies × token-time (the
/// mean-copies numerator banding cannot state).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct BandSum {
    /// Token-time in this cell: Σ span milliseconds × the identity's token count.
    pub token_ms: f64,
    /// Σ exact holder count × token-time — over `token_ms`, the cell's mean copies.
    pub copies_token_ms: f64,
}

/// One identity's whale totals for a day: its token mass (a max, not a sum), its duplicated
/// token-time and its held token-time.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct WhaleSum {
    /// The identity's token mass: the max over its stores, never a sum.
    pub n_tokens: i64,
    /// Token-time while two or more holders held the identity at once (fleet scope).
    pub dup_token_ms: f64,
    /// Token-time while any holder held the identity — the denominator a share is stated over.
    pub held_token_ms: f64,
}

/// A keyed shape with change tracking — the spill seam. Every access goes through this method
/// set; the map behind it is the pure-RAM backing.
#[derive(Debug, Clone)]
pub struct DupShape<K: Ord, V> {
    cells: BTreeMap<K, V>,
    touched: BTreeSet<K>,
}

impl<K: Ord, V> Default for DupShape<K, V> {
    fn default() -> Self {
        Self {
            cells: BTreeMap::new(),
            touched: BTreeSet::new(),
        }
    }
}

impl<K: Ord, V: PartialEq> PartialEq for DupShape<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.cells == other.cells
    }
}

impl<K: Ord + Clone, V: Copy + Default> DupShape<K, V> {
    fn apply(&mut self, key: K, f: impl FnOnce(&mut V)) {
        self.touched.insert(key.clone());
        f(self.cells.entry(key).or_default());
    }

    fn merge(&mut self, other: Self, add: impl Fn(&mut V, V)) {
        for (k, v) in other.cells {
            self.apply(k, |slot| add(slot, v));
        }
        self.touched.extend(other.touched);
    }

    /// Take `old` out of every key it or `new` names and put `new` in, through `correct`; a
    /// key left at the default value leaves, and only keys whose value moved are touched.
    fn correct(&mut self, old: &Self, new: &Self, correct: impl Fn(&mut V, V, V))
    where
        V: PartialEq,
    {
        let keys: BTreeSet<K> = old.cells.keys().chain(new.cells.keys()).cloned().collect();
        for key in keys {
            let was = old.cells.get(&key).copied().unwrap_or_default();
            let now = new.cells.get(&key).copied().unwrap_or_default();
            let before = self.cells.get(&key).copied();
            let mut value = before.unwrap_or_default();
            correct(&mut value, was, now);
            let after = (value != V::default()).then_some(value);
            if after != before {
                match after {
                    Some(v) => self.cells.insert(key.clone(), v),
                    None => self.cells.remove(&key),
                };
                self.touched.insert(key);
            }
        }
    }

    fn retire_before(&mut self, before: i64, day_of: impl Fn(&K) -> i64) {
        self.cells.retain(|k, _| day_of(k) >= before);
        self.touched.retain(|k| day_of(k) >= before);
    }

    fn clear_day(&mut self, day: i64, day_of: impl Fn(&K) -> i64) {
        let gone: Vec<K> = self
            .cells
            .keys()
            .filter(|k| day_of(k) == day)
            .cloned()
            .collect();
        for k in gone {
            self.cells.remove(&k);
            self.touched.insert(k);
        }
    }

    /// The touched keys with their current value, `None` for a key that left.
    fn drain(&mut self) -> Vec<(K, Option<V>)> {
        let touched = std::mem::take(&mut self.touched);
        touched
            .into_iter()
            .map(|k| {
                let now = self.cells.get(&k).copied();
                (k, now)
            })
            .collect()
    }

    /// [`DupShape::drain`] without forgetting: what a write states, kept until the write
    /// is known to have landed.
    fn peek(&self) -> Vec<(K, Option<V>)> {
        self.touched
            .iter()
            .map(|k| (k.clone(), self.cells.get(k).copied()))
            .collect()
    }

    /// A cell as the store served it — placed, not touched: a restore is not a change.
    fn set(&mut self, key: K, value: V) {
        self.cells.insert(key, value);
    }

    /// Cells held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether no cell is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The cells, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.cells.iter()
    }
}

/// The pair basis the placement read intersects: each instance's observed heartbeat spans, one
/// row per `(day, instance, span start)` with the span's end.
pub const INSTANCE_SPANS_DAY: crate::module::Table = crate::module::Table {
    name: "replication_instance_spans_day",
    key: &[
        crate::module::Column::new("day", crate::module::Kind::I64),
        crate::module::Column::new("instance_id", crate::module::Kind::Str),
        crate::module::Column::new("start_ms", crate::module::Kind::F64),
    ],
    measures: &[crate::module::Column::new(
        "end_ms",
        crate::module::Kind::F64,
    )],
    day: crate::module::DayOf::Day("day"),
};

/// One observed span as a row of [`INSTANCE_SPANS_DAY`].
#[must_use]
pub fn instance_span_row(
    day: i64,
    instance: &str,
    start_ms: f64,
    end_ms: f64,
) -> crate::module::Row {
    use crate::module::Value;
    crate::module::Row {
        table: INSTANCE_SPANS_DAY.name,
        key: vec![
            Value::I64(day),
            Value::Str(instance.to_string()),
            Value::F64(start_ms),
        ],
        measures: vec![Value::F64(end_ms)],
        gone: false,
    }
}

/// A served `copies_band` back to the fold's own static spelling — the inverse of
/// [`copy_band`], by the function itself so there is one list.
#[must_use]
pub fn static_band(spelled: &str) -> Option<&'static str> {
    [1, 2, 3, 4, 5, 9, 17]
        .into_iter()
        .map(copy_band)
        .find(|b| *b == spelled)
}

/// A served `locality` back to the fold's own static spelling — the inverse of
/// [`locality`], by the function itself.
#[must_use]
pub fn static_locality(spelled: &str) -> Option<&'static str> {
    [(1, 1, 1), (2, 2, 1), (2, 1, 2), (2, 1, 1)]
        .into_iter()
        .map(|(h, i, r)| locality(h, i, r))
        .find(|l| *l == spelled)
}

/// A fleet band cell's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BandKey {
    /// The bucket's start, epoch milliseconds.
    pub bucket: i64,
    /// The workload class; `""` is the fleet scope.
    pub class: String,
    /// The copies band, as its spelling on the wire.
    pub band: &'static str,
    /// The locality, as its spelling on the wire.
    pub locality: &'static str,
}

/// An instance band cell's key; both tables' `bucket` columns are `DateTime`, so the bucket is
/// in unix seconds at the hour and the minute alike.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstanceKey {
    /// The bucket's start, unix seconds.
    pub bucket: i64,
    /// The instance.
    pub instance: String,
    /// The copies band, as its spelling on the wire.
    pub band: &'static str,
    /// The locality, as its spelling on the wire.
    pub locality: &'static str,
}

/// A pair cell's key: the two holders in canonical order, `a < b`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PairKey {
    /// The day index.
    pub day: i64,
    /// The workload class.
    pub class: String,
    /// The lesser holder.
    pub a: Holder,
    /// The greater holder.
    pub b: Holder,
}

/// The producer's cells across ticks — the keyed state of the six summed `replication_*` tables.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DupCells {
    /// `replication_minute` — keyed by the minute in milliseconds.
    pub minute: DupShape<BandKey, BandSum>,
    /// `replication_hourly` — the hour in milliseconds.
    pub hourly: DupShape<BandKey, BandSum>,
    /// `replication_daily` — the day in milliseconds.
    pub daily: DupShape<BandKey, BandSum>,
    /// `replication_instance_hourly` — the hour in unix seconds.
    pub instance_hourly: DupShape<InstanceKey, BandSum>,
    /// `replication_instance_minute` — the minute in unix seconds.
    pub instance_minute: DupShape<InstanceKey, BandSum>,
    /// `replication_pairs_day`.
    pub pairs: DupShape<PairKey, f64>,
}

fn add_band(slot: &mut BandSum, v: BandSum) {
    slot.token_ms += v.token_ms;
    slot.copies_token_ms += v.copies_token_ms;
}

fn add_whale(slot: &mut WhaleSum, v: WhaleSum) {
    slot.n_tokens = slot.n_tokens.max(v.n_tokens);
    slot.dup_token_ms += v.dup_token_ms;
    slot.held_token_ms += v.held_token_ms;
}

/// The whale register's measure from one identity's minute cells at fleet scope: held at any
/// copy count, duplicated at two or more.
fn whale_of(cell: &crate::residual::MinuteCell, n_tokens: i64) -> WhaleSum {
    WhaleSum {
        n_tokens,
        dup_token_ms: if cell.holders >= 2 {
            cell.token_ms
        } else {
            0.0
        },
        held_token_ms: cell.token_ms,
    }
}

/// The day of a millisecond bucket.
fn day_of_ms(ms: i64) -> i64 {
    ms.div_euclid(86_400_000)
}

/// The day of a unix-seconds bucket.
fn day_of_secs(secs: i64) -> i64 {
    secs.div_euclid(86_400)
}

/// The half-open day sub-windows covering `window`, each paired with its epoch-day.
#[must_use]
pub fn day_windows(window: (f64, f64)) -> Vec<(i64, (f64, f64))> {
    const DAY: f64 = 86_400_000.0;
    let mut out = Vec::new();
    let mut lo = window.0;
    while lo < window.1 {
        let day = (lo / DAY).floor();
        let hi = ((day + 1.0) * DAY).min(window.1);
        #[allow(clippy::cast_possible_truncation)] // day indices are small integers
        out.push((day as i64, (lo, hi)));
        lo = hi;
    }
    out
}

/// One minute in milliseconds: the minute shape's bucket.
const MINUTE_MS: f64 = 60_000.0;

/// One identity's intervals as the sweep reads them: each edge moved inward by `half_skew`,
/// clipped to `window`, and merged per `(class, holder)`. An interval whose open was not
/// observed (an evict that closed nothing) states the close and no holding: counted from the
/// window it is folded in, it would count as much as the tick that read it happened to span.
#[must_use]
pub fn holder_spans(
    intervals: &[Interval],
    half_skew: f64,
    window: (f64, f64),
) -> Vec<HolderSpans> {
    let mut per_key: BTreeMap<(String, Holder), Vec<(f64, f64)>> = BTreeMap::new();
    for iv in intervals {
        let Some(open) = iv.open_ms.map(|o| o + half_skew) else {
            continue;
        };
        let close = iv.close_ms.map_or(f64::INFINITY, |c| c - half_skew);
        // Clip to the emission window HERE: the minute sweep needs finite spans, and
        // everything outside the window is either already emitted or not yet emittable.
        let open = open.max(window.0);
        let close = close.min(window.1);
        if open < close {
            let class = iv
                .workload_class
                .clone()
                .unwrap_or_else(|| UNDECLARED.to_string());
            per_key
                .entry((class, iv.holder()))
                .or_default()
                .push((open, close));
        }
    }
    per_key
        .into_iter()
        .map(|((class, holder), spans)| HolderSpans::merged(class, holder, &spans))
        .collect()
}

/// One identity's whale totals over `window`, per UTC day: the fleet sweep of its spans,
/// token-time held at any copy count and duplicated at two or more.
#[must_use]
pub fn whale_days(
    n_tokens: i64,
    spans: &[HolderSpans],
    window: (f64, f64),
) -> BTreeMap<i64, WhaleSum> {
    let mut days = BTreeMap::new();
    if n_tokens <= 0 || window.1 <= window.0 || spans.is_empty() {
        return days;
    }
    for cell in replication_minute_cells(n_tokens, &fleet_flattened(spans), window) {
        add_whale(
            days.entry(day_of_ms(cell.minute_ms)).or_default(),
            whale_of(&cell, n_tokens),
        );
    }
    days
}

/// One identity's whale totals per UTC day, accrued through `through_ms`: what each fold of the
/// identity added, and what its [`Rates`] add over a stretch its holders held still through.
/// The day product reads every identity's day from here once the day settles.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WhaleAccrual {
    /// Through where the days are accrued; `None` before the identity's first fold.
    pub through_ms: Option<f64>,
    /// The totals per UTC day.
    pub days: BTreeMap<i64, WhaleSum>,
}

impl WhaleAccrual {
    /// Add a fold's totals.
    pub fn add(&mut self, days: &BTreeMap<i64, WhaleSum>) {
        for (day, sum) in days {
            add_whale(self.days.entry(*day).or_default(), *sum);
        }
    }

    /// Add what `rates` state over `[from_ms, to_ms)`, split at day boundaries, for an identity
    /// of `n_tokens` tokens, and move `through_ms` to `to_ms`.
    pub fn accrue(&mut self, rates: &Rates, n_tokens: i64, (from_ms, to_ms): (f64, f64)) {
        for (day, (lo, hi)) in day_windows((from_ms, to_ms)) {
            let ms = hi - lo;
            add_whale(
                self.days.entry(day).or_default(),
                WhaleSum {
                    n_tokens,
                    dup_token_ms: rates.whale.dup_token_ms * ms,
                    held_token_ms: rates.whale.held_token_ms * ms,
                },
            );
        }
        self.through_ms = Some(to_ms);
    }

    /// Correct the days for a re-derivation: `old` taken out and `new` put in; the token mass
    /// keeps the larger.
    pub fn correct(&mut self, old: &BTreeMap<i64, WhaleSum>, new: &BTreeMap<i64, WhaleSum>) {
        let days: BTreeSet<i64> = old.keys().chain(new.keys()).copied().collect();
        for day in days {
            let (o, n) = (
                old.get(&day).copied().unwrap_or_default(),
                new.get(&day).copied().unwrap_or_default(),
            );
            let slot = self.days.entry(day).or_default();
            slot.dup_token_ms += n.dup_token_ms - o.dup_token_ms;
            slot.held_token_ms += n.held_token_ms - o.held_token_ms;
            slot.n_tokens = slot.n_tokens.max(n.n_tokens);
            if slot.dup_token_ms == 0.0 && slot.held_token_ms == 0.0 {
                self.days.remove(&day);
            }
        }
    }

    /// Drop the days before `day`. Returns whether any left.
    pub fn retire_before(&mut self, day: i64) -> bool {
        let before = self.days.len();
        self.days.retain(|d, _| *d >= day);
        self.days.len() != before
    }
}

/// What one identity adds to the summed cells per millisecond while its holders hold still: a
/// window adds its length times these to the cells of each minute and day it covers
/// ([`DupCells::add_rates`]), and the whale register's day totals likewise
/// ([`WhaleAccrual::accrue`]). Rates add, so the identities of a lane that hold still are one
/// sum per key. Every rate is a token count times a holder count, an integer, so a sum of
/// rates times a window is the sum of the identities' own cells exactly.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rates {
    /// The minute shape's keys without the bucket: (class, band, locality), `""` the fleet.
    pub band: BTreeMap<(String, &'static str, &'static str), BandSum>,
    /// The instance cuts' keys without the bucket: (instance, band, locality).
    pub instance: BTreeMap<(String, &'static str, &'static str), BandSum>,
    /// The pair cells' keys without the day: (class, a, b).
    pub pairs: BTreeMap<(String, Holder, Holder), f64>,
    /// The whale register's held and duplicated rates (`n_tokens` unused).
    pub whale: WhaleSum,
}

impl BandSum {
    fn times(self, ms: f64) -> Self {
        Self {
            token_ms: self.token_ms * ms,
            copies_token_ms: self.copies_token_ms * ms,
        }
    }
}

impl Rates {
    /// The rates of an identity holding `state`: every open seed's interval, its open moved by
    /// `half_skew`, in effect by `from_ms`. `None` where an open is not yet in effect, so the
    /// identity's holders still change inside a window from `from_ms`.
    #[must_use]
    pub fn of(state: &IdentityState, half_skew: f64, from_ms: f64) -> Option<Self> {
        let in_effect = state.seeds.values().all(|seed| {
            seed.open
                .as_ref()
                .and_then(|open| open.open_since_ms)
                .is_none_or(|since| since + half_skew <= from_ms)
        });
        if !in_effect {
            return None;
        }
        let intervals = crate::intervals::residency_intervals_for_keys_seeded(
            &[],
            &ScopeClosers::default(),
            &state.seeds,
        );
        // One whole minute at or after `from_ms`: every open is in effect through it, so its
        // cells are the rates times a minute.
        let minute = (from_ms / MINUTE_MS).ceil() * MINUTE_MS;
        let window = (minute, minute + MINUTE_MS);
        let spans = holder_spans(&intervals, half_skew, window);
        let mut cells = DupCells::default();
        cells.fold_spans(&spans, state.n_tokens, window);
        let per_ms = |v: BandSum| BandSum {
            token_ms: v.token_ms / MINUTE_MS,
            copies_token_ms: v.copies_token_ms / MINUTE_MS,
        };
        let whale = whale_days(state.n_tokens, &spans, window)
            .into_values()
            .fold(WhaleSum::default(), |mut acc, w| {
                add_whale(&mut acc, w);
                acc
            });
        Some(Self {
            band: cells
                .minute
                .iter()
                .map(|(k, v)| ((k.class.clone(), k.band, k.locality), per_ms(*v)))
                .collect(),
            instance: cells
                .instance_minute
                .iter()
                .map(|(k, v)| ((k.instance.clone(), k.band, k.locality), per_ms(*v)))
                .collect(),
            pairs: cells
                .pairs
                .iter()
                .map(|(k, v)| ((k.class.clone(), k.a.clone(), k.b.clone()), v / MINUTE_MS))
                .collect(),
            whale: WhaleSum {
                n_tokens: 0,
                dup_token_ms: whale.dup_token_ms / MINUTE_MS,
                held_token_ms: whale.held_token_ms / MINUTE_MS,
            },
        })
    }

    /// Whether the rates state nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.band.is_empty() && self.instance.is_empty() && self.pairs.is_empty()
    }

    /// Add `other` (`sign` 1) or take it out (`sign` -1); a key whose rate returns to zero
    /// leaves.
    pub fn add(&mut self, other: &Self, sign: f64) {
        fn band<K: Ord + Clone>(
            into: &mut BTreeMap<K, BandSum>,
            from: &BTreeMap<K, BandSum>,
            sign: f64,
        ) {
            for (k, v) in from {
                let slot = into.entry(k.clone()).or_default();
                slot.token_ms += sign * v.token_ms;
                slot.copies_token_ms += sign * v.copies_token_ms;
                if slot.token_ms == 0.0 && slot.copies_token_ms == 0.0 {
                    into.remove(k);
                }
            }
        }
        band(&mut self.band, &other.band, sign);
        band(&mut self.instance, &other.instance, sign);
        for (k, v) in &other.pairs {
            let slot = self.pairs.entry(k.clone()).or_default();
            *slot += sign * v;
            if *slot == 0.0 {
                self.pairs.remove(k);
            }
        }
        self.whale.dup_token_ms += sign * other.whale.dup_token_ms;
        self.whale.held_token_ms += sign * other.whale.held_token_ms;
    }
}

impl DupCells {
    /// One identity's intervals, clipped to the emission `window` and shrunk by
    /// `half_skew`, folded into every shape: the per-class and fleet-scope minute cells
    /// (and their hour and day rollups), the per-node cut where the sweep named a sole
    /// instance, the identity's day row, and the pair cells at day grain. The producer's
    /// own emission, as a pure function.
    pub fn fold_identity(
        &mut self,
        intervals: &[Interval],
        n_tokens: i64,
        half_skew: f64,
        window: (f64, f64),
    ) {
        if n_tokens <= 0 || window.1 <= window.0 {
            return;
        }
        let spans = holder_spans(intervals, half_skew, window);
        self.fold_spans(&spans, n_tokens, window);
    }

    /// Each summed cell `rates` states over `window`: per minute it covers, the rate times the
    /// milliseconds of the minute inside the window, into the minute shape and its hour and day
    /// rollups and the instance cuts; per day, into the pair cells. For an identity whose holders
    /// hold still over the window, the same cells [`DupCells::fold_identity`] states.
    pub fn add_rates(&mut self, rates: &Rates, window: (f64, f64)) {
        self.add_rates_joining(rates, Vec::new(), window);
    }

    /// [`DupCells::add_rates`] for `base` over `window` and for each of `joins` from its own
    /// instant on: an identity whose holders stop changing inside the window states its cells
    /// from there at its rates. One pass over the minutes with a running sum: a joining
    /// identity's own keys take its part of the minute it joins in, and the running sum takes
    /// its rates from the next minute, so the cost is minutes times keys plus each joining
    /// identity's own keys, never identities times minutes.
    pub fn add_rates_joining(
        &mut self,
        base: &Rates,
        mut joins: Vec<(f64, Rates)>,
        window: (f64, f64),
    ) {
        if window.1 <= window.0 {
            return;
        }
        joins.retain(|(at, rates)| *at < window.1 && !rates.is_empty());
        joins.sort_by(|a, b| a.0.total_cmp(&b.0));
        self.add_pairs(base, window);
        for (at, rates) in &joins {
            self.add_pairs(rates, (at.max(window.0), window.1));
        }
        let mut running = base.clone();
        let mut next = 0;
        let mut cursor = window.0;
        while cursor < window.1 {
            #[allow(clippy::cast_possible_truncation)] // in-window instants fit i64 ms
            let bucket = (cursor / MINUTE_MS).floor() as i64;
            #[allow(clippy::cast_precision_loss)] // minute indices are small integers
            let end = (((bucket + 1) as f64) * MINUTE_MS).min(window.1);
            let minute_ms = bucket * 60_000;
            self.add_minute(&running, minute_ms, end - cursor);
            let first = next;
            while next < joins.len() && joins[next].0 < end {
                let (at, rates) = &joins[next];
                self.add_minute(rates, minute_ms, end - at.max(cursor));
                next += 1;
            }
            for (_, rates) in &joins[first..next] {
                running.add(rates, 1.0);
            }
            cursor = end;
        }
    }

    /// `rates` times `ms` into the minute `minute_ms`, its hour and day rollups and the
    /// instance cuts.
    fn add_minute(&mut self, rates: &Rates, minute_ms: i64, ms: f64) {
        for ((class, band, loc), r) in &rates.band {
            self.add_band_cell(minute_ms, class.clone(), band, loc, r.times(ms));
        }
        let (hour_secs, minute_secs) = (
            minute_ms.div_euclid(3_600_000) * 3_600,
            minute_ms.div_euclid(1_000),
        );
        for ((instance, band, loc), r) in &rates.instance {
            let sum = r.times(ms);
            self.instance_hourly.apply(
                InstanceKey {
                    bucket: hour_secs,
                    instance: instance.clone(),
                    band,
                    locality: loc,
                },
                |s| {
                    add_band(s, sum);
                },
            );
            self.instance_minute.apply(
                InstanceKey {
                    bucket: minute_secs,
                    instance: instance.clone(),
                    band,
                    locality: loc,
                },
                |s| {
                    add_band(s, sum);
                },
            );
        }
    }

    /// `rates`' pair cells over `window`, per day.
    fn add_pairs(&mut self, rates: &Rates, window: (f64, f64)) {
        for (day, (lo, hi)) in day_windows(window) {
            for ((class, a, b), r) in &rates.pairs {
                self.pairs.apply(
                    PairKey {
                        day,
                        class: class.clone(),
                        a: a.clone(),
                        b: b.clone(),
                    },
                    |s| {
                        *s += r * (hi - lo);
                    },
                );
            }
        }
    }

    /// The fold's core, over one identity's already-merged holder spans clipped to
    /// `window`: what [`DupCells::fold_identity`] reaches after building the spans from
    /// intervals.
    pub fn fold_spans(&mut self, spans: &[HolderSpans], n_tokens: i64, window: (f64, f64)) {
        if n_tokens <= 0 || window.1 <= window.0 || spans.is_empty() {
            return;
        }
        for cell in replication_minute_cells(n_tokens, spans, window) {
            let sum = BandSum {
                token_ms: cell.token_ms,
                copies_token_ms: f64::from(cell.holders) * cell.token_ms,
            };
            self.add_band_cell(
                cell.minute_ms,
                cell.class.clone(),
                copy_band(cell.holders),
                locality(cell.holders, cell.instances, cell.ranks),
                sum,
            );
        }
        for cell in replication_minute_cells(n_tokens, &fleet_flattened(spans), window) {
            let band = copy_band(cell.holders);
            let loc = locality(cell.holders, cell.instances, cell.ranks);
            let sum = BandSum {
                token_ms: cell.token_ms,
                copies_token_ms: f64::from(cell.holders) * cell.token_ms,
            };
            self.add_band_cell(cell.minute_ms, String::new(), band, loc, sum);
            // The per-node cut of the same fleet-scope sweep: present only where the sweep
            // named a sole instance, which is exactly where the duplication was internal.
            if let Some(instance) = &cell.sole_instance {
                let hour_secs = cell.minute_ms.div_euclid(3_600_000) * 3_600;
                let minute_secs = cell.minute_ms.div_euclid(1_000);
                self.instance_hourly.apply(
                    InstanceKey {
                        bucket: hour_secs,
                        instance: instance.clone(),
                        band,
                        locality: loc,
                    },
                    |s| {
                        add_band(s, sum);
                    },
                );
                self.instance_minute.apply(
                    InstanceKey {
                        bucket: minute_secs,
                        instance: instance.clone(),
                        band,
                        locality: loc,
                    },
                    |s| {
                        add_band(s, sum);
                    },
                );
            }
        }
        // Pairs at the ruled DAY grain: the emission window is split at day boundaries so a
        // midnight-straddling tick attributes each side to its own day, and the kernel is
        // the residual's own `token_time_cells` — the two emissions cannot disagree about what
        // a pair cell means.
        for (day, sub) in day_windows(window) {
            for cell in token_time_cells(n_tokens, spans, sub) {
                self.pairs.apply(
                    PairKey {
                        day,
                        class: cell.class,
                        a: cell.a,
                        b: cell.b,
                    },
                    |s| *s += cell.token_ms,
                );
            }
        }
    }

    /// One minute cell into the minute shape and, re-bucketed, into the hour and day
    /// rollups — one computation written at three widths, so they cannot disagree.
    fn add_band_cell(
        &mut self,
        minute_ms: i64,
        class: String,
        band: &'static str,
        loc: &'static str,
        sum: BandSum,
    ) {
        let hour_ms = minute_ms.div_euclid(3_600_000) * 3_600_000;
        let day_ms = minute_ms.div_euclid(86_400_000) * 86_400_000;
        self.minute.apply(
            BandKey {
                bucket: minute_ms,
                class: class.clone(),
                band,
                locality: loc,
            },
            |s| add_band(s, sum),
        );
        self.hourly.apply(
            BandKey {
                bucket: hour_ms,
                class: class.clone(),
                band,
                locality: loc,
            },
            |s| add_band(s, sum),
        );
        self.daily.apply(
            BandKey {
                bucket: day_ms,
                class,
                band,
                locality: loc,
            },
            |s| add_band(s, sum),
        );
    }

    /// Another state's cells into this one — a lane's partial into the tick's, a tick's
    /// into the owner's. Order-independent up to float rounding.
    pub fn merge(&mut self, other: Self) {
        self.minute.merge(other.minute, add_band);
        self.hourly.merge(other.hourly, add_band);
        self.daily.merge(other.daily, add_band);
        self.instance_hourly.merge(other.instance_hourly, add_band);
        self.instance_minute.merge(other.instance_minute, add_band);
        self.pairs.merge(other.pairs, |s, v| *s += v);
    }

    /// Correct the cells for one identity's re-derivation: `old`, its cells as they were
    /// folded, taken out and `new` put in. Token-time sums move by the difference.
    pub fn correct(&mut self, old: &Self, new: &Self) {
        let band = |s: &mut BandSum, o: BandSum, n: BandSum| {
            s.token_ms += n.token_ms - o.token_ms;
            s.copies_token_ms += n.copies_token_ms - o.copies_token_ms;
        };
        self.minute.correct(&old.minute, &new.minute, band);
        self.hourly.correct(&old.hourly, &new.hourly, band);
        self.daily.correct(&old.daily, &new.daily, band);
        self.instance_hourly
            .correct(&old.instance_hourly, &new.instance_hourly, band);
        self.instance_minute
            .correct(&old.instance_minute, &new.instance_minute, band);
        self.pairs
            .correct(&old.pairs, &new.pairs, |s, o, n| *s += n - o);
    }

    /// One served row back into the state, placed rather than touched — [`Persisted::restore`]
    /// with the measures already numbers, the door the lake checkpoint comes in by too: `key`
    /// in the table's own column order (the band tables `class, band, locality, bucket`; the
    /// instance cuts `instance, band, locality, bucket`, the bucket its unix seconds; the
    /// pairs `class, day, a_instance, a_dp_rank,
    /// a_group_idx, b_instance, b_dp_rank, b_group_idx`) and `measures` the table's own, in
    /// order.
    ///
    /// # Errors
    /// A table this state does not hold, a field short or unparseable, or a band or locality
    /// spelling the fold never writes.
    pub fn place(&mut self, table: &str, key: &[&str], measures: &[f64]) -> Result<(), Error> {
        // Every refusal is the row's own parse failure, named by its table: the message a
        // caller prints is unchanged from the string it replaced.
        let refuse = |message: String| Error::Parse {
            name: table.to_string(),
            message,
        };
        let field = |i: usize| -> Result<&str, Error> {
            key.get(i)
                .copied()
                .ok_or_else(|| refuse(format!("key field {i} missing in {key:?}")))
        };
        let num = |i: usize| -> Result<i64, Error> {
            field(i)?
                .parse::<i64>()
                .map_err(|e| refuse(format!("key field {i} {:?}: {e}", key[i])))
        };
        let measure = |i: usize| -> Result<f64, Error> {
            measures
                .get(i)
                .copied()
                .ok_or_else(|| refuse(format!("measure {i} missing in {measures:?}")))
        };
        let band = |i: usize| -> Result<&'static str, Error> {
            static_band(field(i)?).ok_or_else(|| refuse(format!("band {:?}", key[i])))
        };
        let loc = |i: usize| -> Result<&'static str, Error> {
            static_locality(field(i)?).ok_or_else(|| refuse(format!("locality {:?}", key[i])))
        };
        let band_sum = || -> Result<BandSum, Error> {
            Ok(BandSum {
                token_ms: measure(0)?,
                copies_token_ms: measure(1)?,
            })
        };
        // The band tables share one column order: class or instance, band, locality, bucket.
        let band_key = || -> Result<BandKey, Error> {
            Ok(BandKey {
                bucket: num(3)?,
                class: field(0)?.to_string(),
                band: band(1)?,
                locality: loc(2)?,
            })
        };
        let instance_key = || -> Result<InstanceKey, Error> {
            Ok(InstanceKey {
                bucket: num(3)?,
                instance: field(0)?.to_string(),
                band: band(1)?,
                locality: loc(2)?,
            })
        };
        match table {
            "replication_minute" => self.minute.set(band_key()?, band_sum()?),
            "replication_hourly" => self.hourly.set(band_key()?, band_sum()?),
            "replication_daily" => self.daily.set(band_key()?, band_sum()?),
            "replication_instance_hourly" => self.instance_hourly.set(instance_key()?, band_sum()?),
            "replication_instance_minute" => self.instance_minute.set(instance_key()?, band_sum()?),
            "replication_pairs_day" => {
                let holder = |at: usize| -> Result<Holder, Error> {
                    let rank = |i: usize| -> Result<u32, Error> {
                        u32::try_from(num(i)?).map_err(|e| refuse(format!("rank {i}: {e}")))
                    };
                    Ok(Holder {
                        instance: field(at)?.to_string(),
                        dp_rank: rank(at + 1)?,
                        group_idx: rank(at + 2)?,
                    })
                };
                let k = PairKey {
                    day: num(1)?,
                    class: field(0)?.to_string(),
                    a: holder(2)?,
                    b: holder(5)?,
                };
                self.pairs.set(k, measure(0)?);
            }
            other => {
                return Err(Error::Parse {
                    name: other.to_string(),
                    message: "not a duplication table".to_string(),
                })
            }
        }
        Ok(())
    }

    /// Drop every cell of `day`, before the day is stated whole.
    pub fn clear_day(&mut self, day: i64) {
        self.minute.clear_day(day, |k| day_of_ms(k.bucket));
        self.hourly.clear_day(day, |k| day_of_ms(k.bucket));
        self.daily.clear_day(day, |k| day_of_ms(k.bucket));
        self.instance_hourly
            .clear_day(day, |k| day_of_secs(k.bucket));
        self.instance_minute
            .clear_day(day, |k| day_of_secs(k.bucket));
        self.pairs.clear_day(day, |k| k.day);
    }

    /// Settled days leave the state — the settle horizon, past which no correction reaches.
    pub fn retire_before(&mut self, before: i64) {
        self.minute.retire_before(before, |k| day_of_ms(k.bucket));
        self.hourly.retire_before(before, |k| day_of_ms(k.bucket));
        self.daily.retire_before(before, |k| day_of_ms(k.bucket));
        self.instance_hourly
            .retire_before(before, |k| day_of_secs(k.bucket));
        self.instance_minute
            .retire_before(before, |k| day_of_secs(k.bucket));
        self.pairs.retire_before(before, |k| k.day);
    }

    /// Every cell that moved since the last drain, as its current value or `None` where the
    /// key left — what a tick writes, whole, and only that.
    #[must_use]
    pub fn drain_changes(&mut self) -> DupChanges {
        DupChanges {
            minute: self.minute.drain(),
            hourly: self.hourly.drain(),
            daily: self.daily.drain(),
            instance_hourly: self.instance_hourly.drain(),
            instance_minute: self.instance_minute.drain(),
            pairs: self.pairs.drain(),
        }
    }

    /// [`DupCells::drain_changes`] without forgetting — what a write states, kept until the
    /// write is known to have landed, so a failed write's next attempt states the same cells.
    #[must_use]
    pub fn changes(&self) -> DupChanges {
        DupChanges {
            minute: self.minute.peek(),
            hourly: self.hourly.peek(),
            daily: self.daily.peek(),
            instance_hourly: self.instance_hourly.peek(),
            instance_minute: self.instance_minute.peek(),
            pairs: self.pairs.peek(),
        }
    }

    /// Forget the touches without rendering them.
    pub fn discard_changes(&mut self) {
        let _dropped = self.drain_changes();
    }

    /// Cells across the six shapes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.minute.len()
            + self.hourly.len()
            + self.daily.len()
            + self.instance_hourly.len()
            + self.instance_minute.len()
            + self.pairs.len()
    }

    /// Whether every one of the six shapes is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The days the state holds cells for, from the daily shape.
    #[must_use]
    pub fn days(&self) -> BTreeSet<i64> {
        self.daily
            .iter()
            .map(|(k, _)| day_of_ms(k.bucket))
            .collect()
    }
}

/// One identity's carried state: its open seeds per interval key and its known token mass —
/// what the next window's fold resumes from, and what the lake checkpoint holds per identity.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IdentityState {
    /// The open residency per interval key at the window's end; a key whose last interval
    /// closed is absent.
    pub seeds: BTreeMap<IntervalKey, IntervalSeed>,
    /// The identity's known token mass — the max over every store seen; a rise past it asks a
    /// repair.
    pub n_tokens: i64,
    /// The identity's whale totals per day, accrued through its last fold.
    pub whale: WhaleAccrual,
}

/// The window's context the duplication fold reads beyond its own identity's events: the
/// window's scope closers (every own-copy `clear`/`holder_reset`, held once, viewed per
/// key) and the half-skew every interval edge shrinks by.
pub struct DupCtx<'a> {
    /// The window's own-copy `clear`/`holder_reset` events, held once and viewed per key.
    pub closers: &'a ScopeClosers,
    /// `delta_skew_ms / 2`, milliseconds: every interval's open moves later and its close
    /// earlier by this much before the sweep — the clip [`DupCells::fold_identity`] applies.
    pub half_skew: f64,
}

/// Duplication as a derivation module: one identity's
/// cache events, from its carried seeds, to this window's cells of the seven duplication
/// tables and the seeds the next window resumes from. Identity-keyed and prefix-stateful:
/// the residency fold needs the identity's ordered events in one place, from where they
/// left off; the cells it contributes are sums.
pub struct Duplication;

/// One identity's seeds after the fold: the last interval per key, kept only while open — a
/// key whose last interval closed leaves the seeds, and an identity with no open key leaves
/// the state (its mass is learnt again at its next store).
fn next_seeds(identity: &str, intervals: &[Interval], n_tokens: i64) -> IdentityState {
    let mut last_per_key: BTreeMap<IntervalKey, &Interval> = BTreeMap::new();
    for iv in intervals {
        let key = IntervalKey {
            instance: iv.key.instance.clone(),
            dp_rank: iv.key.dp_rank,
            group_idx: iv.key.group_idx,
            identity: identity.to_string(),
            key_epoch: iv.key.key_epoch,
            tier: iv.key.tier.clone(),
            backend_id: iv.key.backend_id.clone(),
        };
        // Intervals per key are in fold order; a later interval supersedes.
        last_per_key.insert(key, iv);
    }
    let seeds: BTreeMap<IntervalKey, IntervalSeed> = last_per_key
        .into_iter()
        .filter(|(_, iv)| iv.close_ms.is_none())
        .map(|(key, iv)| {
            (
                key,
                IntervalSeed {
                    open: Some(FoldSeed {
                        open_since_ms: iv.open_ms,
                        readmits: iv.readmits,
                        workload_class: iv.workload_class.clone(),
                    }),
                    last_close: None,
                },
            )
        })
        .collect();
    if seeds.is_empty() {
        IdentityState::default()
    } else {
        IdentityState {
            seeds,
            n_tokens,
            whale: WhaleAccrual::default(),
        }
    }
}

impl DerivationModule for Duplication {
    type Key = String;
    type State = IdentityState;
    type Event = Event;
    type Cells = DupCells;
    type Ctx<'a> = DupCtx<'a>;
    const NAME: &'static str = "duplication";
    const INITIALIZATION: crate::module::Initialization =
        crate::module::Initialization::ObservedPrefix;

    /// The producer's per-identity body, as it ran inside the tick: the identity's events
    /// grouped by interval key under the own-copy and identity-bearing admission; its token
    /// mass the max over ALL stores carrying it, remote included (the terms rule); a mass
    /// RISE over what was already emitted asks the window's first day repaired (a first
    /// sighting, mass 0 → n, is not a rise); the intervals folded from the carried seeds; the
    /// cells this window contributes; the seeds the next window resumes from.
    fn fold(
        &self,
        identity: &String,
        events: &[Event],
        held: IdentityState,
        window: Window,
        ctx: &DupCtx<'_>,
    ) -> Contribution<DupCells, IdentityState> {
        let keys = crate::events::group_by_interval_key(events);
        let n_new = events
            .iter()
            .filter(|ev| ev.kind == Kind::Store && ev.block_id.is_some())
            .map(|ev| ev.n_tokens)
            .max()
            .unwrap_or(i64::MIN);
        let mut repair_days = Vec::new();
        if held.n_tokens > 0 && n_new > held.n_tokens {
            #[allow(clippy::cast_possible_truncation)] // ms instants are integral
            repair_days.push(day_of_ms(window.start_ms as i64));
        }
        let n_tokens = held.n_tokens.max(n_new).max(0);
        let intervals =
            crate::intervals::residency_intervals_for_keys_seeded(&keys, ctx.closers, &held.seeds);
        let window = (window.start_ms, window.end_ms);
        let spans = holder_spans(&intervals, ctx.half_skew, window);
        let mut cells = DupCells::default();
        cells.fold_spans(&spans, n_tokens, window);
        // The whale days ride the state: what this window held, on what the identity carried.
        let mut whale = held.whale;
        whale.add(&whale_days(n_tokens, &spans, window));
        whale.through_ms = Some(window.1);
        let mut state = next_seeds(identity, &intervals, n_tokens);
        if !state.seeds.is_empty() || !whale.days.is_empty() {
            state.n_tokens = n_tokens;
            state.whale = whale;
        }
        Contribution {
            cells,
            state,
            repair_days,
        }
    }
}

/// The module boundary: the producer's cells are one
/// fact's [`crate::module::Mergeable`], by the methods they already have.
impl crate::module::Mergeable for DupCells {
    fn merge(&mut self, other: Self) {
        DupCells::merge(self, other);
    }
}

impl crate::module::Tracked for DupCells {
    type Changes = DupChanges;
    fn len(&self) -> usize {
        DupCells::len(self)
    }
    fn drain_changes(&mut self) -> DupChanges {
        DupCells::drain_changes(self)
    }
    fn discard_changes(&mut self) {
        DupCells::discard_changes(self);
    }
}

impl crate::module::DayPartitioned for DupCells {
    fn clear_day(&mut self, day: i64) {
        DupCells::clear_day(self, day);
    }
    fn retire_before(&mut self, before: i64) {
        DupCells::retire_before(self, before);
    }
    fn days(&self) -> BTreeSet<i64> {
        DupCells::days(self)
    }
}

const fn col(name: &'static str, kind: crate::module::Kind) -> Column {
    Column::new(name, kind)
}

const BAND_MEASURES: &[Column] = &[
    col("token_ms", crate::module::Kind::F64),
    col("copies_token_ms", crate::module::Kind::F64),
];

fn band_rendered(table: &'static str, k: &BandKey, v: Option<BandSum>) -> Row {
    let s = v.unwrap_or_default();
    Row {
        table,
        key: vec![
            Value::Str(k.class.clone()),
            Value::Str(k.band.to_string()),
            Value::Str(k.locality.to_string()),
            Value::I64(k.bucket),
        ],
        measures: vec![Value::F64(s.token_ms), Value::F64(s.copies_token_ms)],
        gone: v.is_none(),
    }
}

fn instance_rendered(table: &'static str, k: &InstanceKey, v: Option<BandSum>) -> Row {
    let s = v.unwrap_or_default();
    Row {
        table,
        key: vec![
            Value::Str(k.instance.clone()),
            Value::Str(k.band.to_string()),
            Value::Str(k.locality.to_string()),
            Value::DateTime(k.bucket),
        ],
        measures: vec![Value::F64(s.token_ms), Value::F64(s.copies_token_ms)],
        gone: v.is_none(),
    }
}

fn pair_rendered(k: &PairKey, v: Option<f64>) -> Row {
    Row {
        table: "replication_pairs_day",
        key: vec![
            Value::Str(k.class.clone()),
            Value::I64(k.day),
            Value::Str(k.a.instance.clone()),
            Value::U32(k.a.dp_rank),
            Value::U32(k.a.group_idx),
            Value::Str(k.b.instance.clone()),
            Value::U32(k.b.dp_rank),
            Value::U32(k.b.group_idx),
        ],
        measures: vec![Value::F64(v.unwrap_or(0.0))],
        gone: v.is_none(),
    }
}

/// The module boundary's store half: the six tables in the store's own column order
/// (`crates/serving-clickhouse/migrations/0001_schema.sql`, pinned equal by the static audit), the changed
/// and the whole-day cells rendered by the four row shapes above, and a served row back in by
/// [`DupCells::place`].
impl Persisted for DupCells {
    const TABLES: &'static [Table] = &[
        Table {
            name: "replication_minute",
            key: &[
                col("workload_class", crate::module::Kind::Str),
                col("copies_band", crate::module::Kind::Str),
                col("locality", crate::module::Kind::Str),
                col("minute_ms", crate::module::Kind::I64),
            ],
            measures: BAND_MEASURES,
            day: DayOf::Millis("minute_ms"),
        },
        Table {
            name: "replication_hourly",
            key: &[
                col("workload_class", crate::module::Kind::Str),
                col("copies_band", crate::module::Kind::Str),
                col("locality", crate::module::Kind::Str),
                col("bucket_ms", crate::module::Kind::I64),
            ],
            measures: BAND_MEASURES,
            day: DayOf::Millis("bucket_ms"),
        },
        Table {
            name: "replication_daily",
            key: &[
                col("workload_class", crate::module::Kind::Str),
                col("copies_band", crate::module::Kind::Str),
                col("locality", crate::module::Kind::Str),
                col("bucket_ms", crate::module::Kind::I64),
            ],
            measures: BAND_MEASURES,
            day: DayOf::Millis("bucket_ms"),
        },
        Table {
            name: "replication_instance_hourly",
            key: &[
                col("instance_id", crate::module::Kind::Str),
                col("copies_band", crate::module::Kind::Str),
                col("locality", crate::module::Kind::Str),
                col("bucket", crate::module::Kind::DateTime),
            ],
            measures: BAND_MEASURES,
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "replication_instance_minute",
            key: &[
                col("instance_id", crate::module::Kind::Str),
                col("copies_band", crate::module::Kind::Str),
                col("locality", crate::module::Kind::Str),
                col("bucket", crate::module::Kind::DateTime),
            ],
            measures: BAND_MEASURES,
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "replication_pairs_day",
            key: &[
                col("workload_class", crate::module::Kind::Str),
                col("day", crate::module::Kind::I64),
                col("a_instance", crate::module::Kind::Str),
                col("a_dp_rank", crate::module::Kind::U32),
                col("a_group_idx", crate::module::Kind::U32),
                col("b_instance", crate::module::Kind::Str),
                col("b_dp_rank", crate::module::Kind::U32),
                col("b_group_idx", crate::module::Kind::U32),
            ],
            measures: &[col("token_ms", crate::module::Kind::F64)],
            day: DayOf::Day("day"),
        },
    ];

    fn render(changes: &DupChanges) -> Vec<Row> {
        let mut out = Vec::with_capacity(changes.len());
        out.extend(
            changes
                .minute
                .iter()
                .map(|(k, v)| band_rendered("replication_minute", k, *v)),
        );
        out.extend(
            changes
                .hourly
                .iter()
                .map(|(k, v)| band_rendered("replication_hourly", k, *v)),
        );
        out.extend(
            changes
                .daily
                .iter()
                .map(|(k, v)| band_rendered("replication_daily", k, *v)),
        );
        out.extend(
            changes
                .instance_hourly
                .iter()
                .map(|(k, v)| instance_rendered("replication_instance_hourly", k, *v)),
        );
        out.extend(
            changes
                .instance_minute
                .iter()
                .map(|(k, v)| instance_rendered("replication_instance_minute", k, *v)),
        );
        out.extend(changes.pairs.iter().map(|(k, v)| pair_rendered(k, *v)));
        out
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        let mut out = Vec::new();
        for (table, shape) in [
            ("replication_minute", &self.minute),
            ("replication_hourly", &self.hourly),
            ("replication_daily", &self.daily),
        ] {
            out.extend(
                shape
                    .iter()
                    .filter(|(k, _)| day_of_ms(k.bucket) == day)
                    .map(|(k, v)| band_rendered(table, k, Some(*v))),
            );
        }
        for (table, shape) in [
            ("replication_instance_hourly", &self.instance_hourly),
            ("replication_instance_minute", &self.instance_minute),
        ] {
            out.extend(
                shape
                    .iter()
                    .filter(|(k, _)| day_of_secs(k.bucket) == day)
                    .map(|(k, v)| instance_rendered(table, k, Some(*v))),
            );
        }
        out.extend(
            self.pairs
                .iter()
                .filter(|(k, _)| k.day == day)
                .map(|(k, v)| pair_rendered(k, Some(*v))),
        );
        out
    }

    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error> {
        let numbers: Vec<f64> = measures
            .iter()
            .enumerate()
            .map(|(i, v)| {
                v.parse::<f64>().map_err(|e| Error::Parse {
                    name: table.to_string(),
                    message: format!("measure {i} {v:?}: {e}"),
                })
            })
            .collect::<Result<_, _>>()?;
        self.place(table, key, &numbers)
    }
}

/// What a tick writes: the changed cells of the six tables, each as its current value or
/// `None` (a tombstone).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DupChanges {
    /// `replication_minute`'s changed cells.
    pub minute: Vec<(BandKey, Option<BandSum>)>,
    /// `replication_hourly`'s changed cells.
    pub hourly: Vec<(BandKey, Option<BandSum>)>,
    /// `replication_daily`'s changed cells.
    pub daily: Vec<(BandKey, Option<BandSum>)>,
    /// `replication_instance_hourly`'s changed cells.
    pub instance_hourly: Vec<(InstanceKey, Option<BandSum>)>,
    /// `replication_instance_minute`'s changed cells.
    pub instance_minute: Vec<(InstanceKey, Option<BandSum>)>,
    /// `replication_pairs_day`'s changed cells — the pair's token-time, `None` a tombstone.
    pub pairs: Vec<(PairKey, Option<f64>)>,
}

impl DupChanges {
    /// Changed cells across the six tables, tombstones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.minute.len()
            + self.hourly.len()
            + self.daily.len()
            + self.instance_hourly.len()
            + self.instance_minute.len()
            + self.pairs.len()
    }

    /// Whether nothing moved since the last drain.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The tombstones among the changes.
    #[must_use]
    pub fn gone(&self) -> usize {
        self.minute.iter().filter(|(_, v)| v.is_none()).count()
            + self.hourly.iter().filter(|(_, v)| v.is_none()).count()
            + self.daily.iter().filter(|(_, v)| v.is_none()).count()
            + self
                .instance_hourly
                .iter()
                .filter(|(_, v)| v.is_none())
                .count()
            + self
                .instance_minute
                .iter()
                .filter(|(_, v)| v.is_none())
                .count()
            + self.pairs.iter().filter(|(_, v)| v.is_none()).count()
    }
}
