// SPDX-License-Identifier: AGPL-3.0-only

//! A keyed shape with change tracking: the spill seam every cell table sits behind.

use super::rows::Grain;

/// One cell's measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Sums {
    /// Facts in the cell after the identity dedup.
    pub events: u64,
    /// `n_tokens` summed over those facts.
    pub tokens: u64,
}

impl Sums {
    /// One more fact of `n_tokens`. A negative count cannot reach here — absent coalesces to
    /// zero upstream, which undercounts waste and never inflates it.
    pub(super) fn add(&mut self, n_tokens: i64) {
        self.events += 1;
        self.tokens += u64::try_from(n_tokens).unwrap_or(0);
    }

    pub(super) fn merge(&mut self, other: Sums) {
        self.events += other.events;
        self.tokens += other.tokens;
    }
}

/// A key that carries the bucket its cell falls in.
pub trait Bucketed {
    /// The bucket: a day index at the daily grain, unix seconds at the hourly and minute grains.
    fn bucket(&self) -> i64;
}

/// One shape's cells: a keyed map from a table's key to its tag (the class, or the class
/// and the band — carried in the value so the key stays the table's own) and its sums, at
/// one grain, so the shape knows the day each of its keys falls on.
///
/// **This is the spill seam.** Every access to a shape's cells goes through this method set —
/// add, subtract, merge, retire and scan — and nothing reaches the map behind it. A spilling
/// backing replaces the map and keeps the methods; its callers do not change.
#[derive(Debug, Clone)]
pub struct Shape<K: Ord, T> {
    grain: Grain,
    pub(super) cells: std::collections::BTreeMap<K, (T, Sums)>,
    /// Subtractions that found less than they took — a cell absent, or a measure below
    /// the amount removed. Zero by construction when every subtraction removes facts this
    /// state added; anything else names a state that has drifted from the facts.
    pub(super) underflows: u64,
    /// The keys whose value moved since the last drain, each with the tag it carried — so a
    /// key that LEFT can still be rendered as its tombstone. Bookkeeping for the writer,
    /// not part of the state's value: equality ignores it.
    pub(super) touched: std::collections::BTreeMap<K, T>,
}

impl<K: Ord, T: PartialEq> PartialEq for Shape<K, T> {
    fn eq(&self, other: &Self) -> bool {
        self.grain == other.grain
            && self.cells == other.cells
            && self.underflows == other.underflows
    }
}

impl<K: Ord + Clone + Bucketed, T: Copy> Shape<K, T> {
    /// An empty shape at `grain`.
    pub(super) fn new(grain: Grain) -> Self {
        Self {
            grain,
            cells: std::collections::BTreeMap::new(),
            underflows: 0,
            touched: std::collections::BTreeMap::new(),
        }
    }

    /// The day a key's cell falls on.
    pub(super) fn day_of(&self, key: &K) -> i64 {
        match self.grain {
            Grain::Daily => key.bucket(),
            Grain::Hourly | Grain::Minute => key.bucket().div_euclid(86_400),
        }
    }

    /// The cells whose day `keep` admits, in key order.
    pub(super) fn cells_on(
        &self,
        keep: impl Fn(i64) -> bool,
    ) -> impl Iterator<Item = (&K, &(T, Sums))> {
        self.cells.iter().filter(move |(k, _)| keep(self.day_of(k)))
    }

    pub(super) fn add(&mut self, key: K, tag: T, s: Sums) {
        self.touched.insert(key.clone(), tag);
        let slot = self.cells.entry(key).or_insert((tag, Sums::default()));
        slot.1.events += s.events;
        slot.1.tokens += s.tokens;
    }

    /// Take `s` out of the cell; a cell whose events reach zero leaves the map — the
    /// read's `HAVING sum(events) > 0`, held as state rather than filtered at the read.
    pub(super) fn subtract(&mut self, key: &K, s: Sums) {
        let Some(slot) = self.cells.get_mut(key) else {
            self.underflows += 1;
            return;
        };
        self.touched.insert(key.clone(), slot.0);
        let short = slot.1.events < s.events || slot.1.tokens < s.tokens;
        if short {
            self.underflows += 1;
        }
        slot.1.events = slot.1.events.saturating_sub(s.events);
        slot.1.tokens = slot.1.tokens.saturating_sub(s.tokens);
        if slot.1.events == 0 {
            self.cells.remove(key);
        }
    }

    pub(super) fn merge(&mut self, other: Self) {
        for (k, (tag, s)) in other.cells {
            self.add(k, tag, s);
        }
        for (k, tag) in other.touched {
            self.touched.entry(k).or_insert(tag);
        }
        self.underflows += other.underflows;
    }

    /// Drop every cell on a day below `before`, and forget its touches: a retired day is the
    /// store's TTL's, never a tombstone's.
    pub(super) fn retire_before(&mut self, before: i64) {
        let grain = self.grain;
        let day_of = |k: &K| match grain {
            Grain::Daily => k.bucket(),
            Grain::Hourly | Grain::Minute => k.bucket().div_euclid(86_400),
        };
        self.cells.retain(|k, _| day_of(k) >= before);
        self.touched.retain(|k, _| day_of(k) >= before);
    }

    /// Drop every cell on `day`, each marked touched so a key the day no longer produces
    /// renders as its tombstone at the next drain.
    pub(super) fn clear_day(&mut self, day: i64) {
        let gone: Vec<(K, T)> = self
            .cells_on(|d| d == day)
            .map(|(k, (tag, _))| (k.clone(), *tag))
            .collect();
        for (k, tag) in gone {
            self.cells.remove(&k);
            self.touched.insert(k, tag);
        }
    }

    /// The touched keys, each as its current value or its tombstone, and the touches
    /// forgotten.
    pub(super) fn drain(&mut self) -> Vec<(K, T, Option<Sums>)> {
        let touched = std::mem::take(&mut self.touched);
        touched
            .into_iter()
            .map(|(k, tag)| {
                let now = self.cells.get(&k).map(|(_, s)| *s);
                (k, tag, now)
            })
            .collect()
    }

    pub(super) fn len(&self) -> usize {
        self.cells.len()
    }

    /// A cell as the store served it — placed, not touched: a restore is not a change.
    pub(super) fn set(&mut self, key: K, tag: T, s: Sums) {
        self.cells.insert(key, (tag, s));
    }
}
