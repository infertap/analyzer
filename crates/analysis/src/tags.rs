// SPDX-License-Identifier: AGPL-3.0-only

//! Declared tag intervals (spec §2.8): which identity carried which opaque label, and when, and
//! the cells a receipt groups by it.
//!
//! **Declarations.** A declarer states a `tag_interval` under a `declaration_id`, and may state
//! it again: the statement with the greatest `at_ms` is the declaration's interval, stream order
//! breaking a tie, never a merge of statements. An absent `until_ms` means the interval is
//! open. The label is opaque: it is grouped byte for byte and never interpreted.
//!
//! **Attribution crosses clock domains, so it is conservative.** The interval is on the
//! declarer's producer clock and a store's instant on the engine clock. A store is attributed
//! to an interval only when its instant, widened by the skew allowance on both sides, lies
//! wholly inside the interval; a store whose widened instant crosses either end is attributed
//! to nothing and counted as ambiguous, so the edge's loss is visible and an ambiguous store
//! never lands in either slice of a comparison.
//!
//! **The cells.** Per `(tag, identity, minute)`: the stores attributed, their tokens (the
//! prefill), the tokens of the recompute register among them (`ETR` and `post_clear`: the
//! refetch; `RE` is not a refetch), the tokens of indeterminate stores, kept apart so taint is
//! visible, and the ambiguous stores. Bounded by tagged identities times minutes, never by
//! events.

use crate::classify::Class;
use crate::emitted::ClassifiedFact;
use crate::module::{Column, DayOf, Kind, Row, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

/// One statement of a declaration.
#[derive(Debug, Clone, PartialEq)]
pub struct TagStatement {
    /// The declaration it states.
    pub declaration_id: String,
    /// The identity the label is on.
    pub identity: String,
    /// The opaque label.
    pub tag: String,
    /// When the identity began carrying the label, producer clock.
    pub from_ms: f64,
    /// When it stopped, producer clock; `None` while open.
    pub until_ms: Option<f64>,
    /// When the declarer wrote the statement, producer clock.
    pub at_ms: f64,
}

/// A declaration as it stands: its latest statement.
#[derive(Debug, Clone, PartialEq)]
pub struct TagInterval {
    /// The identity.
    pub identity: String,
    /// The label.
    pub tag: String,
    /// The interval's start.
    pub from_ms: f64,
    /// The interval's end; `None` while open.
    pub until_ms: Option<f64>,
    /// The latest statement's `at_ms`: what a later statement must reach to replace it.
    pub stated_at_ms: f64,
}

/// Every declaration's interval, by declaration and by identity.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TagIndex {
    declarations: BTreeMap<String, TagInterval>,
    by_identity: BTreeMap<String, BTreeSet<String>>,
    changed: BTreeSet<String>,
}

/// How one store stands against one interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attribution {
    /// Its widened instant lies wholly inside the interval.
    Inside,
    /// Its widened instant crosses an end: attributed to nothing, counted.
    Ambiguous,
    /// It does not meet the interval.
    Outside,
}

/// A store at `at_ms` against `[from_ms, until_ms)` under the skew allowance `skew_ms`.
#[must_use]
pub fn attribute(at_ms: f64, from_ms: f64, until_ms: Option<f64>, skew_ms: f64) -> Attribution {
    let until = until_ms.unwrap_or(f64::INFINITY);
    let (lo, hi) = (at_ms - skew_ms, at_ms + skew_ms);
    if lo >= from_ms && hi < until {
        Attribution::Inside
    } else if hi >= from_ms && lo < until {
        Attribution::Ambiguous
    } else {
        Attribution::Outside
    }
}

impl TagIndex {
    /// Take a statement: it replaces its declaration's interval when it was written at or
    /// after the one standing, stream order breaking a tie.
    pub fn state(&mut self, s: &TagStatement) {
        if self
            .declarations
            .get(&s.declaration_id)
            .is_some_and(|d| d.stated_at_ms > s.at_ms)
        {
            return;
        }
        if let Some(prior) = self.declarations.get(&s.declaration_id) {
            if let Some(ids) = self.by_identity.get_mut(&prior.identity) {
                ids.remove(&s.declaration_id);
            }
        }
        self.declarations.insert(
            s.declaration_id.clone(),
            TagInterval {
                identity: s.identity.clone(),
                tag: s.tag.clone(),
                from_ms: s.from_ms,
                until_ms: s.until_ms,
                stated_at_ms: s.at_ms,
            },
        );
        self.by_identity
            .entry(s.identity.clone())
            .or_default()
            .insert(s.declaration_id.clone());
        self.changed.insert(s.declaration_id.clone());
    }

    /// The intervals on `identity`.
    pub fn of(&self, identity: &str) -> impl Iterator<Item = &TagInterval> {
        self.by_identity
            .get(identity)
            .into_iter()
            .flatten()
            .filter_map(|d| self.declarations.get(d))
    }

    /// Every declaration and its interval.
    pub fn declarations(&self) -> impl Iterator<Item = (&String, &TagInterval)> {
        self.declarations.iter()
    }

    /// Whether any declaration is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty()
    }

    /// The declarations changed since the last call, as rows of [`TAG_INTERVALS`].
    pub fn drain(&mut self) -> Vec<Row> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .filter_map(|d| {
                let i = self.declarations.get(&d)?;
                Some(Row {
                    table: TAG_INTERVALS.name,
                    key: vec![Value::Str(d)],
                    measures: vec![
                        Value::Str(i.identity.clone()),
                        Value::Str(i.tag.clone()),
                        Value::F64(i.from_ms),
                        Value::nullable_f64(i.until_ms),
                        Value::F64(i.stated_at_ms),
                    ],
                    gone: false,
                })
            })
            .collect()
    }

    /// Declarations a store kept, with nothing changed.
    #[must_use]
    pub fn restored(declarations: impl IntoIterator<Item = (String, TagInterval)>) -> Self {
        let mut index = Self::default();
        for (d, i) in declarations {
            index
                .by_identity
                .entry(i.identity.clone())
                .or_default()
                .insert(d.clone());
            index.declarations.insert(d, i);
        }
        index
    }
}

/// The declarations as they stand: `tag_intervals`, a standing fact per declaration.
pub const TAG_INTERVALS: Table = Table {
    name: "tag_intervals",
    key: &[Column::new("declaration_id", Kind::Str)],
    measures: &[
        Column::new("identity", Kind::Str),
        Column::new("tag", Kind::Str),
        Column::new("from_ms", Kind::F64),
        Column::new("until_ms", Kind::NullableF64),
        Column::new("stated_at_ms", Kind::F64),
    ],
    day: DayOf::Undated,
};

/// The cells a receipt groups: `tag_minute`.
pub const TAG_MINUTE: Table = Table {
    name: "tag_minute",
    key: &[
        Column::new("tag", Kind::Str),
        Column::new("identity", Kind::Str),
        Column::new("bucket", Kind::DateTime),
    ],
    measures: &[
        Column::new("stores", Kind::I64),
        Column::new("prefill_tokens", Kind::I64),
        Column::new("refetch_tokens", Kind::I64),
        Column::new("indeterminate_tokens", Kind::I64),
        Column::new("ambiguous_stores", Kind::I64),
    ],
    day: DayOf::DateTime("bucket"),
};

/// One `(tag, identity, minute)` cell's sums.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TagSums {
    /// Stores attributed.
    pub stores: i64,
    /// Their tokens.
    pub prefill_tokens: i64,
    /// The tokens of the recompute register among them.
    pub refetch_tokens: i64,
    /// The tokens of indeterminate stores among them.
    pub indeterminate_tokens: i64,
    /// Stores at an edge of the interval, attributed to nothing.
    pub ambiguous_stores: i64,
}

impl TagSums {
    fn add(&mut self, o: Self, sign: i64) {
        self.stores += sign * o.stores;
        self.prefill_tokens += sign * o.prefill_tokens;
        self.refetch_tokens += sign * o.refetch_tokens;
        self.indeterminate_tokens += sign * o.indeterminate_tokens;
        self.ambiguous_stores += sign * o.ambiguous_stores;
    }

    fn is_zero(self) -> bool {
        self == Self::default()
    }
}

/// A tag cell's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TagKey {
    /// The tag's label.
    pub tag: String,
    /// The identity the tag was stated on.
    pub identity: String,
    /// The minute's start in unix seconds.
    pub minute: i64,
}

impl TagKey {
    /// The day the cell falls in.
    fn day(&self) -> i64 {
        self.minute.div_euclid(86_400)
    }
}

/// The tag cells.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TagCells {
    cells: BTreeMap<TagKey, TagSums>,
    touched: BTreeSet<TagKey>,
}

impl TagCells {
    /// Add (`sign` 1) or take out (`sign` −1) the stores of `facts` as `index` attributes them
    /// under `skew_ms`.
    pub fn apply(&mut self, facts: &[ClassifiedFact], index: &TagIndex, skew_ms: f64, sign: i64) {
        for f in facts {
            for interval in index.of(&f.block) {
                let one = match attribute(f.at_ms, interval.from_ms, interval.until_ms, skew_ms) {
                    Attribution::Outside => continue,
                    Attribution::Ambiguous => TagSums {
                        ambiguous_stores: 1,
                        ..TagSums::default()
                    },
                    Attribution::Inside => TagSums {
                        stores: 1,
                        prefill_tokens: f.n_tokens,
                        refetch_tokens: if Class::RECOMPUTE.contains(&f.class) {
                            f.n_tokens
                        } else {
                            0
                        },
                        indeterminate_tokens: if f.class == Class::Indeterminate {
                            f.n_tokens
                        } else {
                            0
                        },
                        ambiguous_stores: 0,
                    },
                };
                #[allow(clippy::cast_possible_truncation)] // minutes of finite instants fit i64
                let minute = (f.at_ms / 60_000.0).floor() as i64 * 60;
                let key = TagKey {
                    tag: interval.tag.clone(),
                    identity: f.block.clone(),
                    minute,
                };
                let cell = self.cells.entry(key.clone()).or_default();
                cell.add(one, sign);
                if cell.is_zero() {
                    self.cells.remove(&key);
                }
                self.touched.insert(key);
            }
        }
    }

    /// The cells.
    #[must_use]
    pub fn cells(&self) -> &BTreeMap<TagKey, TagSums> {
        &self.cells
    }
}

impl crate::module::Mergeable for TagCells {
    fn merge(&mut self, other: Self) {
        for (key, sums) in other.cells {
            let cell = self.cells.entry(key.clone()).or_default();
            cell.add(sums, 1);
            if cell.is_zero() {
                self.cells.remove(&key);
            }
            self.touched.insert(key);
        }
    }
}

impl crate::module::Tracked for TagCells {
    type Changes = Vec<(TagKey, Option<TagSums>)>;

    fn len(&self) -> usize {
        self.cells.len()
    }

    fn drain_changes(&mut self) -> Self::Changes {
        std::mem::take(&mut self.touched)
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

impl crate::module::DayPartitioned for TagCells {
    fn clear_day(&mut self, day: i64) {
        let gone: Vec<TagKey> = self
            .cells
            .keys()
            .filter(|k| k.day() == day)
            .cloned()
            .collect();
        for k in gone {
            self.cells.remove(&k);
            self.touched.insert(k);
        }
    }

    fn retire_before(&mut self, before: i64) {
        self.cells.retain(|k, _| k.day() >= before);
        self.touched.retain(|k| k.day() >= before);
    }

    fn days(&self) -> BTreeSet<i64> {
        self.cells.keys().map(TagKey::day).collect()
    }
}

fn row(k: &TagKey, sums: Option<TagSums>) -> Row {
    let s = sums.unwrap_or_default();
    Row {
        table: TAG_MINUTE.name,
        key: vec![
            Value::Str(k.tag.clone()),
            Value::Str(k.identity.clone()),
            Value::DateTime(k.minute),
        ],
        measures: vec![
            Value::I64(s.stores),
            Value::I64(s.prefill_tokens),
            Value::I64(s.refetch_tokens),
            Value::I64(s.indeterminate_tokens),
            Value::I64(s.ambiguous_stores),
        ],
        gone: sums.is_none(),
    }
}

impl crate::module::Persisted for TagCells {
    const TABLES: &'static [Table] = &[TAG_MINUTE];

    fn render(changes: &Self::Changes) -> Vec<Row> {
        changes.iter().map(|(k, v)| row(k, *v)).collect()
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        self.cells
            .iter()
            .filter(|(k, _)| k.day() == day)
            .map(|(k, v)| row(k, Some(*v)))
            .collect()
    }

    fn restore(
        &mut self,
        table: &str,
        key: &[&str],
        measures: &[&str],
    ) -> Result<(), crate::error::Error> {
        let bad = |what: &str| crate::error::Error::Parse {
            name: table.to_string(),
            message: what.to_string(),
        };
        if table != TAG_MINUTE.name {
            return Err(bad("not the tag table"));
        }
        let ([tag, identity, bucket], [stores, prefill, refetch, indeterminate, ambiguous]) =
            (key, measures)
        else {
            return Err(bad("a key of three columns and five measures"));
        };
        let int = |v: &str| v.parse::<i64>().map_err(|e| bad(&format!("{v:?}: {e}")));
        self.cells.insert(
            TagKey {
                tag: (*tag).to_string(),
                identity: (*identity).to_string(),
                minute: int(bucket)?,
            },
            TagSums {
                stores: int(stores)?,
                prefill_tokens: int(prefill)?,
                refetch_tokens: int(refetch)?,
                indeterminate_tokens: int(indeterminate)?,
                ambiguous_stores: int(ambiguous)?,
            },
        );
        Ok(())
    }
}

/// The minutes `intervals` cover inside `[from_ms, to_ms)`, overlapping intervals counted once:
/// one identity's exposure to a tag. An open interval runs to `to_ms`.
#[must_use]
pub fn exposure_minutes(intervals: &[(f64, Option<f64>)], from_ms: f64, to_ms: f64) -> f64 {
    let mut spans: Vec<(f64, f64)> = intervals
        .iter()
        .map(|&(lo, hi)| (lo.max(from_ms), hi.unwrap_or(to_ms).min(to_ms)))
        .filter(|(lo, hi)| lo < hi)
        .collect();
    spans.sort_by(|a, b| a.0.total_cmp(&b.0));
    let (mut total, mut end) = (0.0, f64::NEG_INFINITY);
    for (lo, hi) in spans {
        if hi > end {
            total += hi - lo.max(end);
            end = hi;
        }
    }
    total / 60_000.0
}

/// One identity under one tag over a window: its exposure and its cells summed.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Exposed {
    /// Minutes the identity carried the tag inside the window.
    pub minutes: f64,
    /// Its cells over the window.
    pub sums: TagSums,
}

/// The two-sided 90 % quantile of the standard normal.
const Z_90: f64 = 1.644_853_626_951_472_2;

/// A ratio of sums over identities, with its 90 % interval: the ratio estimator, its variance
/// linearized over identities as the sampling unit, so an identity's minutes are never taken as
/// independent draws.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ratio {
    /// Σ numerator over Σ denominator; `None` when the denominator is zero.
    pub value: Option<f64>,
    /// The interval's ends; `None` with fewer than two identities, where no spread is observed.
    pub interval_90: Option<(f64, f64)>,
}

impl Ratio {
    /// The ratio of `(numerator, denominator)` summed over identities.
    #[must_use]
    pub fn over(pairs: &[(f64, f64)]) -> Self {
        let (y, x) = pairs
            .iter()
            .fold((0.0, 0.0), |(y, x), &(yi, xi)| (y + yi, x + xi));
        if x <= 0.0 {
            return Self {
                value: None,
                interval_90: None,
            };
        }
        let r = y / x;
        #[allow(clippy::cast_precision_loss)] // identity counts stay far below 2^52
        let n = pairs.len() as f64;
        let interval_90 = (pairs.len() >= 2).then(|| {
            let residuals: f64 = pairs.iter().map(|&(yi, xi)| (yi - r * xi).powi(2)).sum();
            let se = (n / (n - 1.0) * residuals).sqrt() / x;
            (r - Z_90 * se, r + Z_90 * se)
        });
        Self {
            value: Some(r),
            interval_90,
        }
    }
}

/// A tag's figures over a window, from its identities.
#[derive(Debug, Clone, PartialEq)]
pub struct TagFigures {
    /// Identities that carried the tag inside the window.
    pub identities: usize,
    /// Their exposure, in identity-minutes.
    pub identity_minutes: f64,
    /// Their cells summed.
    pub sums: TagSums,
    /// The primary figure: refetch tokens per identity-minute of exposure. Exposure does not
    /// move with the policy, so neither does the denominator.
    pub refetch_per_identity_minute: Ratio,
    /// The secondary figure: refetch tokens per stored token. Biased upward under a policy that
    /// cuts refetch, since a treated identity stores less.
    pub refetch_per_stored_token: Ratio,
}

impl TagFigures {
    /// The figures of the identities in `exposed`.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // token counts stay far below 2^52
    pub fn of(exposed: &[Exposed]) -> Self {
        let mut sums = TagSums::default();
        for e in exposed {
            sums.add(e.sums, 1);
        }
        let refetch = |e: &Exposed| e.sums.refetch_tokens as f64;
        let per_minute: Vec<(f64, f64)> = exposed.iter().map(|e| (refetch(e), e.minutes)).collect();
        let per_token: Vec<(f64, f64)> = exposed
            .iter()
            .map(|e| (refetch(e), e.sums.prefill_tokens as f64))
            .collect();
        Self {
            identities: exposed.len(),
            identity_minutes: exposed.iter().map(|e| e.minutes).sum(),
            sums,
            refetch_per_identity_minute: Ratio::over(&per_minute),
            refetch_per_stored_token: Ratio::over(&per_token),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        attribute, exposure_minutes, Attribution, Exposed, Ratio, TagFigures, TagIndex,
        TagStatement, TagSums,
    };

    #[test]
    #[allow(clippy::float_cmp)] // whole minutes: every figure here is exact
    fn exposure_counts_overlapping_intervals_once_and_runs_an_open_one_to_the_edge() {
        let m = 60_000.0;
        // Two overlapping declarations, one open past the window's end, one before it.
        let intervals = [(0.0, Some(10.0 * m)), (5.0 * m, None), (-9.0 * m, Some(-m))];
        assert_eq!(exposure_minutes(&intervals, 0.0, 30.0 * m), 30.0);
        assert_eq!(exposure_minutes(&intervals, 20.0 * m, 30.0 * m), 10.0);
        assert_eq!(exposure_minutes(&[], 0.0, m), 0.0);
    }

    #[test]
    fn the_interval_is_spread_over_identities_and_absent_without_spread() {
        // R = 40 / 20 = 2; residuals ±10; variance 2·200 / 20² = 1, so the interval is 2 ± z.
        let r = Ratio::over(&[(10.0, 10.0), (30.0, 10.0)]);
        assert_eq!(r.value, Some(2.0));
        let (lo, hi) = r.interval_90.expect("two identities");
        assert!((lo - (2.0 - super::Z_90)).abs() < 1e-12, "{lo}");
        assert!((hi - (2.0 + super::Z_90)).abs() < 1e-12, "{hi}");
        assert_eq!(Ratio::over(&[(3.0, 1.0)]).interval_90, None, "one identity");
        assert_eq!(Ratio::over(&[(3.0, 0.0)]).value, None, "no denominator");
    }

    #[test]
    #[allow(clippy::float_cmp)] // small integers: every figure here is exact
    fn an_exposed_identity_without_stores_still_weighs_on_the_primary_figure() {
        let stored = Exposed {
            minutes: 10.0,
            sums: TagSums {
                stores: 2,
                prefill_tokens: 100,
                refetch_tokens: 40,
                ..TagSums::default()
            },
        };
        let idle = Exposed {
            minutes: 10.0,
            sums: TagSums::default(),
        };
        let f = TagFigures::of(&[stored, idle]);
        assert_eq!((f.identities, f.identity_minutes), (2, 20.0));
        assert_eq!(f.refetch_per_identity_minute.value, Some(2.0));
        assert_eq!(f.refetch_per_stored_token.value, Some(0.4));
        assert_eq!(f.sums.stores, 2);
    }

    fn statement(id: &str, at: f64, tag: &str, from: f64, until: Option<f64>) -> TagStatement {
        TagStatement {
            declaration_id: id.into(),
            identity: "cS".into(),
            tag: tag.into(),
            from_ms: from,
            until_ms: until,
            at_ms: at,
        }
    }

    #[test]
    fn an_edge_store_is_ambiguous_and_an_outside_one_is_nothing() {
        let skew = 1_000.0;
        assert_eq!(
            attribute(600_000.0, 0.0, Some(1_800_000.0), skew),
            Attribution::Inside
        );
        assert_eq!(
            attribute(1.0, 0.0, Some(1_800_000.0), skew),
            Attribution::Ambiguous
        );
        assert_eq!(
            attribute(1_799_999.0, 0.0, Some(1_800_000.0), skew),
            Attribution::Ambiguous
        );
        assert_eq!(
            attribute(5_400_000.0, 0.0, Some(1_800_000.0), skew),
            Attribution::Outside
        );
        assert_eq!(
            attribute(9e12, 0.0, None, skew),
            Attribution::Inside,
            "open runs on"
        );
    }

    #[test]
    fn the_latest_statement_is_the_interval_whatever_the_arrival_order() {
        let mut index = TagIndex::default();
        index.state(&statement("a", 20.0, "t", 0.0, Some(20.0)));
        index.state(&statement("a", 10.0, "t", 0.0, None));
        let now: Vec<_> = index.of("cS").collect();
        assert_eq!(now.len(), 1);
        assert_eq!(
            now[0].until_ms,
            Some(20.0),
            "the earlier statement arrived later and lost"
        );
        index.state(&statement("b", 5.0, "T", 0.0, None));
        let tags: Vec<&str> = index.of("cS").map(|i| i.tag.as_str()).collect();
        assert_eq!(tags, ["t", "T"], "two labels differing in case are two");
    }
}
