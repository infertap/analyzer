// SPDX-License-Identifier: AGPL-3.0-only

//! The blocks by stored mass, per day: the heaviest blocks a fleet stored, from a summary whose
//! memory does not grow with the day.
//!
//! **Weighted Space-Saving.** The summary monitors at most `capacity` blocks. A stored block
//! already monitored adds its tokens; a new one takes a free slot, or evicts the lightest
//! monitored block and inherits its count as its error. So a monitored block's count is an
//! upper bound on its true mass and `count − error` a lower bound, every error is at most the
//! day's mass over the capacity, and a block heavier than that bound is always monitored.
//!
//! **What a reader is told.** The top `cut` blocks by count, each with its lower bound and its
//! error, and whether it is **certainly** in the true top `cut`: its lower bound clears the
//! upper bound of the first block below the cut. Beyond the cut the ranking says nothing. With
//! fewer distinct blocks than the capacity, nothing is ever evicted and every figure is exact.
//!
//! Stored mass is its own register: the tokens stores wrote, never placement's held mass
//! (token-milliseconds a block stayed resident), and the two are never joined.

use crate::intervals::Holder;
use crate::module::{Column, DayOf, Kind, Row, Table, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Each ranked block's day row: `block_mass_daily`.
pub const BLOCK_MASS_DAILY: Table = Table {
    name: "block_mass_daily",
    key: &[
        Column::new("day", Kind::Date),
        Column::new("block_id", Kind::Str),
    ],
    measures: &[
        Column::new("tokens_upper", Kind::I64),
        Column::new("tokens_error", Kind::I64),
        Column::new("certain", Kind::U32),
        Column::new("holders_at_least", Kind::U32),
    ],
    day: DayOf::Date("day"),
};

/// The block-mass family's tables.
pub const TABLES: &[Table] = &[BLOCK_MASS_DAILY, BLOCK_MASS_DAY];

/// Each day's summary: its mass, the summary's capacity and the cut, what every row of the
/// day is stated under: `block_mass_day`.
pub const BLOCK_MASS_DAY: Table = Table {
    name: "block_mass_day",
    key: &[Column::new("day", Kind::Date)],
    measures: &[
        Column::new("tokens", Kind::I64),
        Column::new("capacity", Kind::U32),
        Column::new("cut", Kind::U32),
    ],
    day: DayOf::Date("day"),
};

/// One monitored block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Monitored {
    /// Tokens counted: an upper bound on the block's mass.
    pub count: i64,
    /// What the count may overstate by: inherited from the block it evicted.
    pub error: i64,
    /// The holders seen storing it since it was monitored: a lower bound on its holders.
    pub holders: BTreeSet<Holder>,
}

/// One day's weighted Space-Saving summary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpaceSaving {
    capacity: usize,
    total: i64,
    items: HashMap<String, Monitored>,
    /// The monitored blocks by count, lightest first: the next to evict.
    order: BTreeSet<(i64, String)>,
}

/// One block as ranked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ranked {
    /// The block.
    pub block: String,
    /// An upper bound on its mass.
    pub upper: i64,
    /// What the upper bound may overstate by.
    pub error: i64,
    /// Whether it is certainly in the true top `cut`.
    pub certain: bool,
    /// A lower bound on the holders that stored it.
    pub holders_at_least: u32,
}

impl SpaceSaving {
    /// An empty summary monitoring at most `capacity` blocks (at least one).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            ..Self::default()
        }
    }

    /// The summary's capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The day's stored mass.
    #[must_use]
    pub fn total(&self) -> i64 {
        self.total
    }

    /// The monitored blocks.
    pub fn monitored(&self) -> impl Iterator<Item = (&String, &Monitored)> {
        self.items.iter()
    }

    /// A summary holding `items`, as a store kept it.
    #[must_use]
    pub fn restored(
        capacity: usize,
        total: i64,
        items: impl IntoIterator<Item = (String, Monitored)>,
    ) -> Self {
        let items: HashMap<String, Monitored> = items.into_iter().collect();
        let order = items.iter().map(|(k, m)| (m.count, k.clone())).collect();
        Self {
            capacity: capacity.max(1),
            total,
            items,
            order,
        }
    }

    /// Count a store of `tokens` of `block` by `holder`.
    pub fn add(&mut self, block: &str, tokens: i64, holder: &Holder) {
        let tokens = tokens.max(0);
        self.total += tokens;
        if let Some(m) = self.items.get_mut(block) {
            self.order.remove(&(m.count, block.to_string()));
            m.count += tokens;
            m.holders.insert(holder.clone());
            self.order.insert((m.count, block.to_string()));
            return;
        }
        let mut entry = Monitored {
            count: tokens,
            error: 0,
            holders: BTreeSet::from([holder.clone()]),
        };
        if self.items.len() >= self.capacity {
            if let Some((least, evicted)) = self.order.pop_first() {
                self.items.remove(&evicted);
                entry.count = least + tokens;
                entry.error = least;
            }
        }
        self.order.insert((entry.count, block.to_string()));
        self.items.insert(block.to_string(), entry);
    }

    /// The top `cut` blocks by count, heaviest first, ties broken by block, each marked certain
    /// when its lower bound clears the upper bound of the first block below the cut.
    #[must_use]
    pub fn top(&self, cut: usize) -> Vec<Ranked> {
        let mut ranked: Vec<(&String, &Monitored)> = self.items.iter().collect();
        ranked.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
        // Below the cut: the first block not ranked, or, when every block is ranked, the least a
        // block the summary never monitored can weigh: the lightest monitored count once the
        // summary is full, zero while it is not.
        let below = ranked.get(cut).map_or_else(
            || {
                if self.items.len() >= self.capacity {
                    self.order.first().map_or(0, |(c, _)| *c)
                } else {
                    0
                }
            },
            |(_, m)| m.count,
        );
        ranked
            .into_iter()
            .take(cut)
            .map(|(block, m)| Ranked {
                block: block.clone(),
                upper: m.count,
                error: m.error,
                certain: m.count - m.error > below,
                holders_at_least: u32::try_from(m.holders.len()).unwrap_or(u32::MAX),
            })
            .collect()
    }
}

/// The blocks by stored mass, one summary per day, and what each day last published.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BlockMass {
    capacity: usize,
    cut: usize,
    days: BTreeMap<i64, SpaceSaving>,
    published: BTreeMap<i64, BTreeSet<String>>,
    touched: BTreeSet<i64>,
}

impl BlockMass {
    /// Summaries of `capacity` blocks, publishing the top `cut` of each day.
    #[must_use]
    pub fn new(capacity: usize, cut: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            cut,
            ..Self::default()
        }
    }

    /// The same days under a new `capacity` and `cut`: a day already summarized keeps its own
    /// capacity, and the next drain publishes each day under the new cut.
    #[must_use]
    pub fn reconfigured(mut self, capacity: usize, cut: usize) -> Self {
        self.capacity = capacity.max(1);
        self.cut = cut;
        self
    }

    /// Count a store of `tokens` of `block` by `holder` on `day`.
    pub fn add(&mut self, day: i64, block: &str, tokens: i64, holder: &Holder) {
        let capacity = self.capacity;
        self.days
            .entry(day)
            .or_insert_with(|| SpaceSaving::new(capacity))
            .add(block, tokens, holder);
        self.touched.insert(day);
    }

    /// Each day's summary.
    #[must_use]
    pub fn days(&self) -> &BTreeMap<i64, SpaceSaving> {
        &self.days
    }

    /// A day's summary and the blocks it last published, as a store kept them.
    pub fn restore_day(&mut self, day: i64, summary: SpaceSaving, published: BTreeSet<String>) {
        self.days.insert(day, summary);
        self.published.insert(day, published);
    }

    /// The blocks `day` last published.
    #[must_use]
    pub fn published(&self, day: i64) -> Option<&BTreeSet<String>> {
        self.published.get(&day)
    }

    /// The rows of every day touched since the last call: each day's top `cut` whole, a
    /// tombstone for a block that left it, and the day's own row. Returns the days drained.
    pub fn drain(&mut self) -> (Vec<Row>, Vec<i64>) {
        let mut rows = Vec::new();
        let days: Vec<i64> = std::mem::take(&mut self.touched).into_iter().collect();
        for day in &days {
            let day_rows = self.day_rows(*day);
            let now: BTreeSet<String> = day_rows
                .iter()
                .filter(|r| r.table == BLOCK_MASS_DAILY.name)
                .filter_map(|r| match r.key.get(1) {
                    Some(Value::Str(block)) => Some(block.clone()),
                    _ => None,
                })
                .collect();
            for gone in self
                .published
                .get(day)
                .into_iter()
                .flatten()
                .filter(|b| !now.contains(*b))
            {
                rows.push(block_row(*day, gone, None));
            }
            rows.extend(day_rows);
            self.published.insert(*day, now);
        }
        (rows, days)
    }

    /// `day`'s rows whole: its top `cut` and its own row, nothing for a day the summary does
    /// not hold. What a restatement of the day replaces the published day with.
    #[must_use]
    pub fn day_rows(&self, day: i64) -> Vec<Row> {
        let Some(summary) = self.days.get(&day) else {
            return Vec::new();
        };
        let mut rows: Vec<Row> = summary
            .top(self.cut)
            .iter()
            .map(|r| block_row(day, &r.block, Some(r)))
            .collect();
        rows.push(Row {
            table: BLOCK_MASS_DAY.name,
            key: vec![Value::Date(day)],
            measures: vec![
                Value::I64(summary.total()),
                Value::U32(u32::try_from(summary.capacity()).unwrap_or(u32::MAX)),
                Value::U32(u32::try_from(self.cut).unwrap_or(u32::MAX)),
            ],
            gone: false,
        });
        rows
    }

    /// Forget the days before `day`: their rows stand as published.
    pub fn retire_before(&mut self, day: i64) {
        self.days = self.days.split_off(&day);
        self.published = self.published.split_off(&day);
        self.touched.retain(|d| *d >= day);
    }
}

fn block_row(day: i64, block: &str, ranked: Option<&Ranked>) -> Row {
    Row {
        table: BLOCK_MASS_DAILY.name,
        key: vec![Value::Date(day), Value::Str(block.to_string())],
        measures: vec![
            Value::I64(ranked.map_or(0, |r| r.upper)),
            Value::I64(ranked.map_or(0, |r| r.error)),
            Value::U32(u32::from(ranked.is_some_and(|r| r.certain))),
            Value::U32(ranked.map_or(0, |r| r.holders_at_least)),
        ],
        gone: ranked.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockMass, SpaceSaving};
    use crate::intervals::Holder;
    use std::collections::BTreeMap;

    fn holder(i: u32) -> Holder {
        Holder {
            instance: format!("i{i}"),
            dp_rank: 0,
            group_idx: 0,
        }
    }

    /// A skewed stream: block `b{k}` stored `1000 / (k + 1)` times, sixteen tokens each, by
    /// holders round the fleet, interleaved.
    fn stream() -> Vec<(String, u32)> {
        let mut out = Vec::new();
        for round in 0..1000u32 {
            for k in 0..200u32 {
                if round < 1000 / (k + 1) {
                    out.push((format!("b{k}"), round % 7));
                }
            }
        }
        out
    }

    #[test]
    fn with_room_for_every_block_the_ranking_is_exact() {
        let mut s = SpaceSaving::new(1_000);
        let mut exact: BTreeMap<String, i64> = BTreeMap::new();
        for (block, h) in stream() {
            s.add(&block, 16, &holder(h));
            *exact.entry(block).or_default() += 16;
        }
        let top = s.top(10);
        assert!(top.iter().all(|r| r.error == 0 && r.certain));
        for r in &top {
            assert_eq!(r.upper, exact[&r.block], "{}", r.block);
        }
        assert_eq!(top[0].block, "b0");
    }

    #[test]
    fn a_small_summary_bounds_every_mass_and_keeps_the_heavy_blocks() {
        let mut s = SpaceSaving::new(40);
        let mut exact: BTreeMap<String, i64> = BTreeMap::new();
        for (block, h) in stream() {
            s.add(&block, 16, &holder(h));
            *exact.entry(block).or_default() += 16;
        }
        let bound = s.total() / 40;
        for r in s.top(10) {
            let truth = exact[&r.block];
            assert!(r.upper >= truth, "{}: {} under {}", r.block, r.upper, truth);
            assert!(
                r.upper - r.error <= truth,
                "{}: lower bound above the truth",
                r.block
            );
            assert!(
                r.error <= bound,
                "{}: error {} past {}",
                r.block,
                r.error,
                bound
            );
        }
        // Every block heavier than the bound is monitored, and a certain block is in the true
        // top ten.
        let mut truth: Vec<(&String, &i64)> = exact.iter().collect();
        truth.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        let true_top: Vec<&String> = truth.iter().take(10).map(|(b, _)| *b).collect();
        for r in s.top(10).iter().filter(|r| r.certain) {
            assert!(true_top.contains(&&r.block), "{} marked certain", r.block);
        }
        assert!(
            s.top(10).iter().any(|r| r.certain),
            "nothing certain on a skewed stream"
        );
    }

    #[test]
    fn a_day_publishes_its_top_and_a_tombstone_for_a_block_that_left_it() {
        let mut mass = BlockMass::new(100, 2);
        mass.add(5, "a", 30, &holder(0));
        mass.add(5, "b", 20, &holder(1));
        let (rows, _) = mass.drain();
        assert_eq!(rows.len(), 3, "two ranked rows and the day row");
        mass.add(5, "c", 50, &holder(2));
        let (rows, days) = mass.drain();
        assert_eq!(days, vec![5]);
        let gone: Vec<_> = rows.iter().filter(|r| r.gone).collect();
        assert_eq!(gone.len(), 1, "b left the top two");
        assert_eq!(gone[0].key[1].text(), "b");
    }
}
