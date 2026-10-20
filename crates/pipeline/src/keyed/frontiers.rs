// SPDX-License-Identifier: AGPL-3.0-only

//! One lane's frontiers, held compact: the state repeat compute carries between ticks.
//!
//! A key is a block and its epoch; its frontiers are one entry per holder that held it within
//! the examined history. The classifier reads and writes them as [`Frontier`] rows, which spell
//! the holder and the block out as strings on every row. Here a block's string is held once, the
//! holders and the workload classes once per lane, and each entry is the times, counts and
//! verdicts it carries. The conversion is exact both ways: a row put is the row got.
//!
//! The interned tables grow with the lane's holders and declared workload classes, which are the
//! fleet's configuration, not its traffic.

use super::repeat::IdentityKey;
use analyzer_analysis::classify::{Class, Frontier};
use analyzer_analysis::events::{IntervalKey, ScopeClosers};
use analyzer_analysis::intervals::CloseReason;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;
use std::sync::Arc;

/// The cache a frontier speaks for: [`Frontier`]'s holder fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Holder {
    instance: String,
    dp_rank: u32,
    group_idx: u32,
    tier: Option<String>,
    backend_id: Option<String>,
}

/// Values held once and named by their index.
#[derive(Debug)]
struct Interner<T> {
    values: Vec<T>,
    index: HashMap<T, u32>,
}

impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            index: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash> Interner<T> {
    fn id(&mut self, value: T) -> u32 {
        if let Some(id) = self.index.get(&value) {
            return *id;
        }
        let id = u32::try_from(self.values.len()).expect("fewer than 2^32 interned values");
        self.values.push(value.clone());
        self.index.insert(value, id);
        id
    }

    fn get(&self, id: u32) -> &T {
        &self.values[id as usize]
    }
}

/// One holder's frontier for one block: [`Frontier`] less the strings the lane holds once.
#[derive(Debug, Clone)]
struct Entry {
    epoch: Option<u32>,
    holder: u32,
    open: bool,
    last_close_ms: Option<f64>,
    last_close_reason: Option<CloseReason>,
    open_since_ms: Option<f64>,
    open_readmits: u64,
    open_workload_class: Option<u32>,
    open_opener_class: Option<Class>,
}

/// One lane's frontiers.
#[derive(Debug, Default)]
pub(super) struct LaneFrontiers {
    holders: Interner<Holder>,
    workloads: Interner<String>,
    /// Each block's entries, every epoch's in the order the classifier wrote them.
    blocks: HashMap<Arc<str>, Vec<Entry>>,
    /// Blocks by a close their entries made, oldest first: what leaves the examined history
    /// next. A block that closed again since is found current, or gone, when popped.
    aging: BinaryHeap<Reverse<(i64, Arc<str>)>>,
}

/// Milliseconds, whole, as the aging queue orders them.
fn ms(at: f64) -> i64 {
    #[allow(clippy::cast_possible_truncation)] // epoch milliseconds fit an i64
    let whole = at.floor() as i64;
    whole
}

impl LaneFrontiers {
    /// The frontiers held for `key`.
    pub(super) fn get(&self, key: &IdentityKey) -> Option<Vec<Frontier>> {
        let entries = self.blocks.get(key.0.as_str())?;
        let rows: Vec<Frontier> = entries
            .iter()
            .filter(|e| e.epoch == key.1)
            .map(|e| self.row(&key.0, e))
            .collect();
        (!rows.is_empty()).then_some(rows)
    }

    /// Hold `frontiers` for `key`, replacing what was held; none is the key leaving.
    pub(super) fn put(&mut self, key: &IdentityKey, frontiers: &[Frontier]) {
        self.remove(key);
        if frontiers.is_empty() {
            return;
        }
        let entries: Vec<Entry> = frontiers
            .iter()
            .map(|f| {
                debug_assert_eq!(
                    (&f.key.identity, f.key.key_epoch),
                    (&key.0, key.1),
                    "a key's own row"
                );
                self.entry(f)
            })
            .collect();
        let block = match self.blocks.get_key_value(key.0.as_str()) {
            Some((held, _)) => Arc::clone(held),
            None => Arc::from(key.0.as_str()),
        };
        for close in entries
            .iter()
            .filter(|e| !e.open)
            .filter_map(|e| e.last_close_ms)
        {
            self.aging.push(Reverse((ms(close), Arc::clone(&block))));
        }
        self.blocks.entry(block).or_default().extend(entries);
    }

    /// Forget `key`.
    pub(super) fn remove(&mut self, key: &IdentityKey) {
        let Some(entries) = self.blocks.get_mut(key.0.as_str()) else {
            return;
        };
        entries.retain(|e| e.epoch != key.1);
        if entries.is_empty() {
            self.blocks.remove(key.0.as_str());
        }
    }

    /// Forget every key of `block`, returning them.
    pub(super) fn remove_block(&mut self, block: &str) -> Vec<IdentityKey> {
        let Some(entries) = self.blocks.remove(block) else {
            return Vec::new();
        };
        epochs(&entries)
            .into_iter()
            .map(|epoch| (block.to_string(), epoch))
            .collect()
    }

    /// Every key and its frontiers, in no promised order.
    pub(super) fn scan(&self) -> impl Iterator<Item = (IdentityKey, Vec<Frontier>)> + '_ {
        self.blocks.iter().flat_map(move |(block, entries)| {
            epochs(entries).into_iter().map(move |epoch| {
                let rows = entries
                    .iter()
                    .filter(|e| e.epoch == epoch)
                    .map(|e| self.row(block, e))
                    .collect();
                ((block.to_string(), epoch), rows)
            })
        })
    }

    /// The open frontiers on a holder some closer in `closers` covers: the residencies a scope
    /// close reaches whatever their block. Reads each entry's holder, and spells out only those.
    pub(super) fn open_under(&self, closers: &ScopeClosers) -> Vec<Frontier> {
        let covered: Vec<bool> = self
            .holders
            .values
            .iter()
            .map(|h| {
                closers.covers(&IntervalKey {
                    instance: h.instance.clone(),
                    dp_rank: h.dp_rank,
                    group_idx: h.group_idx,
                    identity: String::new(),
                    key_epoch: None,
                    tier: h.tier.clone(),
                    backend_id: h.backend_id.clone(),
                })
            })
            .collect();
        if !covered.contains(&true) {
            return Vec::new();
        }
        self.blocks
            .iter()
            .flat_map(|(block, entries)| {
                entries
                    .iter()
                    .filter(|e| e.open && covered[e.holder as usize])
                    .map(|e| self.row(block, e))
            })
            .collect()
    }

    /// The blocks with an entry on `instance`, open or closed.
    pub(super) fn blocks_on(&self, instance: &str) -> Vec<String> {
        let on: Vec<bool> = self
            .holders
            .values
            .iter()
            .map(|h| h.instance == instance)
            .collect();
        if !on.contains(&true) {
            return Vec::new();
        }
        self.blocks
            .iter()
            .filter(|(_, entries)| entries.iter().any(|e| on[e.holder as usize]))
            .map(|(block, _)| block.to_string())
            .collect()
    }

    /// Drop every closed entry whose close is before `cutoff_ms`, or before `silent_cutoff_ms`
    /// for a close a silence made, returning the keys it changed. A silent close is kept longer
    /// because a late segment may yet take the silence back, and the blocks it closed are found
    /// here.
    pub(super) fn retire(&mut self, cutoff_ms: f64, silent_cutoff_ms: f64) -> Vec<IdentityKey> {
        let mut changed = Vec::new();
        let mut again = Vec::new();
        while self
            .aging
            .peek()
            .is_some_and(|Reverse((close, _))| *close < ms(cutoff_ms))
        {
            let Some(Reverse((_, block))) = self.aging.pop() else {
                break;
            };
            let Some(entries) = self.blocks.get_mut(&block) else {
                continue;
            };
            let stale = |e: &Entry| {
                let cutoff = if e.last_close_reason == Some(CloseReason::Silent) {
                    silent_cutoff_ms
                } else {
                    cutoff_ms
                };
                !e.open && e.last_close_ms.is_some_and(|c| c < cutoff)
            };
            let touched = epochs(entries)
                .into_iter()
                .filter(|epoch| entries.iter().any(|e| e.epoch == *epoch && stale(e)))
                .collect::<Vec<_>>();
            entries.retain(|e| !stale(e));
            // A silent close kept past the cutoff is queued again for when it is stale.
            for close in entries
                .iter()
                .filter(|e| !e.open && e.last_close_reason == Some(CloseReason::Silent))
                .filter_map(|e| e.last_close_ms)
                .filter(|c| *c < cutoff_ms)
            {
                again.push(Reverse((
                    ms(close + (cutoff_ms - silent_cutoff_ms)),
                    Arc::clone(&block),
                )));
            }
            if entries.is_empty() {
                self.blocks.remove(&block);
            }
            changed.extend(touched.into_iter().map(|epoch| (block.to_string(), epoch)));
        }
        self.aging.extend(again);
        changed
    }

    fn entry(&mut self, f: &Frontier) -> Entry {
        Entry {
            epoch: f.key.key_epoch,
            holder: self.holders.id(Holder {
                instance: f.key.instance.clone(),
                dp_rank: f.key.dp_rank,
                group_idx: f.key.group_idx,
                tier: f.key.tier.clone(),
                backend_id: f.key.backend_id.clone(),
            }),
            open: f.open,
            last_close_ms: f.last_close_ms,
            last_close_reason: f.last_close_reason,
            open_since_ms: f.open_since_ms,
            open_readmits: f.open_readmits,
            open_workload_class: f
                .open_workload_class
                .as_ref()
                .map(|w| self.workloads.id(w.clone())),
            open_opener_class: f.open_opener_class,
        }
    }

    fn row(&self, block: &str, e: &Entry) -> Frontier {
        let holder = self.holders.get(e.holder);
        Frontier {
            key: IntervalKey {
                instance: holder.instance.clone(),
                dp_rank: holder.dp_rank,
                group_idx: holder.group_idx,
                identity: block.to_string(),
                key_epoch: e.epoch,
                tier: holder.tier.clone(),
                backend_id: holder.backend_id.clone(),
            },
            last_close_ms: e.last_close_ms,
            last_close_reason: e.last_close_reason,
            open: e.open,
            open_since_ms: e.open_since_ms,
            open_readmits: e.open_readmits,
            open_workload_class: e.open_workload_class.map(|w| self.workloads.get(w).clone()),
            open_opener_class: e.open_opener_class,
        }
    }
}

/// The epochs among `entries`, each once, in the order they first appear.
fn epochs(entries: &[Entry]) -> Vec<Option<u32>> {
    let mut seen = Vec::new();
    for e in entries {
        if !seen.contains(&e.epoch) {
            seen.push(e.epoch);
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(instance: &str, block: &str, epoch: Option<u32>, close: Option<f64>) -> Frontier {
        Frontier {
            key: IntervalKey {
                instance: instance.into(),
                dp_rank: 0,
                group_idx: 0,
                identity: block.into(),
                key_epoch: epoch,
                tier: None,
                backend_id: Some("b".into()),
            },
            last_close_ms: close,
            last_close_reason: close.map(|_| CloseReason::Evict),
            open: close.is_none(),
            open_since_ms: close.is_none().then_some(5.0),
            open_readmits: 3,
            open_workload_class: close.is_none().then(|| "agentic".to_string()),
            open_opener_class: close.is_none().then_some(Class::Cold),
        }
    }

    #[test]
    fn an_entry_is_smaller_than_the_row_it_stands_for() {
        // The frontier is the lane's largest state, one entry per holder and block within the
        // examined history; this is the size the state is planned at.
        eprintln!(
            "entry {} B, row {} B before its strings",
            std::mem::size_of::<Entry>(),
            std::mem::size_of::<Frontier>()
        );
        assert!(std::mem::size_of::<Entry>() <= 64);
    }

    #[test]
    fn a_key_reads_back_the_rows_put_in_their_order_and_its_epochs_stay_apart() {
        let mut lane = LaneFrontiers::default();
        let a = vec![
            frontier("i2", "x", None, Some(10.0)),
            frontier("i1", "x", None, None),
        ];
        let b = vec![frontier("i1", "x", Some(7), Some(20.0))];
        lane.put(&("x".into(), None), &a);
        lane.put(&("x".into(), Some(7)), &b);
        assert_eq!(lane.get(&("x".into(), None)), Some(a.clone()));
        assert_eq!(lane.get(&("x".into(), Some(7))), Some(b));
        lane.put(&("x".into(), Some(7)), &[]);
        assert_eq!(
            lane.get(&("x".into(), Some(7))),
            None,
            "none put is the key leaving"
        );
        let scanned: Vec<_> = lane.scan().collect();
        assert_eq!(scanned, vec![(("x".to_string(), None), a)]);
    }

    #[test]
    fn retirement_drops_closes_older_than_the_cutoff_and_keeps_open_entries() {
        let mut lane = LaneFrontiers::default();
        lane.put(
            &("x".into(), None),
            &[
                frontier("i1", "x", None, Some(10.0)),
                frontier("i2", "x", None, None),
            ],
        );
        lane.put(
            &("y".into(), None),
            &[frontier("i1", "y", None, Some(50.0))],
        );
        assert_eq!(lane.retire(30.0, 30.0), vec![("x".to_string(), None)]);
        assert_eq!(
            lane.get(&("x".into(), None)),
            Some(vec![frontier("i2", "x", None, None)])
        );
        assert!(
            lane.get(&("y".into(), None)).is_some(),
            "a close inside the history stays"
        );
        assert_eq!(lane.retire(60.0, 60.0), vec![("y".to_string(), None)]);
    }

    #[test]
    fn a_silent_close_outlives_the_history_until_its_own_cutoff() {
        let mut lane = LaneFrontiers::default();
        let mut silent = frontier("i1", "x", None, Some(10.0));
        silent.last_close_reason = Some(CloseReason::Silent);
        lane.put(&("x".into(), None), &[silent.clone()]);
        lane.put(
            &("y".into(), None),
            &[frontier("i1", "y", None, Some(10.0))],
        );
        // Past the history's cutoff, before the correction horizon's: y leaves, x stays.
        assert_eq!(lane.retire(30.0, 5.0), vec![("y".to_string(), None)]);
        assert_eq!(lane.get(&("x".into(), None)), Some(vec![silent]));
        // The edge moves on: x's own cutoff passes it, and its queue entry finds it.
        assert!(lane.retire(34.0, 9.0).is_empty(), "not yet");
        assert_eq!(lane.retire(40.0, 15.0), vec![("x".to_string(), None)]);
    }

    #[test]
    fn a_close_put_again_later_is_not_retired_by_its_earlier_queue_entry() {
        let mut lane = LaneFrontiers::default();
        lane.put(
            &("x".into(), None),
            &[frontier("i1", "x", None, Some(10.0))],
        );
        lane.put(
            &("x".into(), None),
            &[frontier("i1", "x", None, Some(40.0))],
        );
        assert!(lane.retire(30.0, 30.0).is_empty());
        assert!(lane.get(&("x".into(), None)).is_some());
    }

    #[test]
    fn a_scope_close_reaches_only_the_open_entries_on_the_holders_it_covers() {
        let mut lane = LaneFrontiers::default();
        lane.put(
            &("x".into(), None),
            &[
                frontier("i1", "x", None, None),
                frontier("i2", "x", None, None),
            ],
        );
        lane.put(&("y".into(), None), &[frontier("i1", "y", None, Some(3.0))]);
        let mut clear = analyzer_analysis::events::Event::new(
            analyzer_analysis::events::Kind::Clear,
            "i1",
            20.0,
        );
        clear.dp_rank = Some(0);
        let closers = ScopeClosers::of(&[clear]);
        assert_eq!(
            lane.open_under(&closers),
            vec![frontier("i1", "x", None, None)]
        );
    }
}
