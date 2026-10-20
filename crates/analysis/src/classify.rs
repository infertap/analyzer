// SPDX-License-Identifier: AGPL-3.0-only

//! Store classification (join-semantics): the six-bucket causal taxonomy of
//! bounded-state recomputation, evaluated per store fact over the residency-interval
//! substrate and the uncertainty model.
//!
//! Precedence, in spec order: **`re_admit`** (the store hits an open interval — positive,
//! never tainted) → **RE** (an open interval for `p` elsewhere covers the `δ_skew`-widened
//! instant — positive) → **ETR / `post_clear`** (the most recent prior close for `(i, p)`
//! decides by its REASON, with no gap condition — positive) → **cold / indeterminate**
//! (absence-decided: tainted evidence makes indeterminate, per the taint rule; taint = drop-window
//! membership / fallback suspicion / truncated batch / cross-instance drop overlap of
//! the RE-simultaneity range).
//!
//! `δ_skew` discipline: RE requires the other instance's interval to cover the FULL
//! widened range `[t − δ, t + δ]` — the default allowance cannot manufacture the
//! positive; the operator-attested low-skew preset buys the sharper claim.

use crate::completeness::Coverage;
use crate::events::IntervalKey;
use crate::intervals::{CloseReason, Holder, Interval};
use crate::Params;
use std::collections::BTreeMap;

/// The six buckets. String forms are the fixture and storage vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// `re_admit`: the store lands on an interval this holder already holds open for the block.
    /// Positive evidence, never tainted.
    ReAdmit,
    /// `RE`: another holder's open interval for the block covers the whole `δ_skew`-widened
    /// instant, so the block was resident elsewhere. Positive evidence.
    Re,
    /// `ETR`: this holder's most recent observed prior close for the block was an `evict`.
    /// Its tokens sum into `eviction_waste_tokens`.
    Etr,
    /// `post_clear`: this holder's most recent observed prior close for the block was a
    /// `clear`. Its tokens sum into `post_clear_tokens`.
    PostClear,
    /// `cold`: no prior close observed within the examined history, with the absence untainted;
    /// also a store after a `holder_reset` close, decided by that evidence rather than absence.
    Cold,
    /// `indeterminate`: no prior close observed, but the deciding ranges are tainted by a drop
    /// window, so the absence is not evidence (the taint rule).
    Indeterminate,
}

impl Class {
    /// Every bucket, for callers that must enumerate the closed set rather than match on it.
    ///
    /// **The read API's `class` parameter is generated from this**, so a console gets
    /// `"ETR" | "post_clear" | ...` as a union type rather than `string` — a closed set stays
    /// closed all the way into a generated client. `every_class_is_in_all` pins the count, since
    /// adding a variant forces [`Class::as_str`]'s match but would not force this list.
    pub const ALL: [Self; 6] = [
        Self::ReAdmit,
        Self::Re,
        Self::Etr,
        Self::PostClear,
        Self::Cold,
        Self::Indeterminate,
    ];

    /// The buckets that sum into the recompute register: stores whose block had a
    /// prior observed close on the store's own key (its holder, identity and epoch). The console's headline and the exported
    /// fleet gauges both sum exactly these — stated once, here, so the glass and the
    /// alarms cannot drift on which classes count.
    pub const RECOMPUTE: [Self; 2] = [Self::Etr, Self::PostClear];

    /// The storage spelling: the fixture and table vocabulary, and what the read API serves.
    /// [`Class::parse`] is its inverse.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReAdmit => "re_admit",
            Self::Re => "RE",
            Self::Etr => "ETR",
            Self::PostClear => "post_clear",
            Self::Cold => "cold",
            Self::Indeterminate => "indeterminate",
        }
    }

    /// The inverse of [`Class::as_str`], for reading a banked class back (the frontier's
    /// opener). `None` on a token this build does not know — the caller refuses rather
    /// than guesses, the same rule every banked-string read follows.
    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|c| c.as_str() == token)
    }
}

/// One classified store event.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassifiedStore {
    /// The instance the store was observed on. With `dp_rank` and `group_idx` it names the
    /// holder, the cache the store was classified against.
    pub instance: String,
    /// The producer run, for the run-scoped model join (see `StoreEvent::incarnation`).
    pub incarnation: String,
    /// What the producer declared its traffic is for, carried from the segment header the
    /// record arrived under (spec 2.7). `undeclared` where the producer declared nothing.
    pub workload_class: Option<String>,
    /// Data-parallel rank: an independent cache on the same instance. An undeclared rank is 0.
    pub dp_rank: u32,
    /// KV cache group, hashed independently within a worker. An undeclared group is 0.
    pub group_idx: u32,
    /// The identity the store was classified under: `content_id` where the tap derived one,
    /// the engine's `block_id` otherwise ([`crate::events::Event::identity`]).
    pub block: String,
    /// The epoch scoping the block's identity, part of the key it was classified under.
    /// `None` where the producer declared none.
    pub key_epoch: Option<u32>,
    /// The store's instant, engine-domain milliseconds.
    pub at_ms: f64,
    /// Tokens in the stored block. An absent count coalesced to 0 upstream, so the waste sums
    /// this feeds undercount and never inflate.
    pub n_tokens: i64,
    /// The bucket the taxonomy assigned this store, by the module's precedence.
    pub class: Class,
    /// **Milliseconds since the most recent OBSERVED prior close for this `(holder, block,
    /// epoch)`, whatever this store's class turned out to be.** `None` when no prior close was
    /// observed within the examined history.
    ///
    /// **Recorded for every class that has one, and that is the whole point.** The class is
    /// decided by the close's REASON; this is the magnitude beside it, and the two together are
    /// what a reader draws their own line over. The deliverable is the distribution itself
    /// (`docs/classification-semantics.md` §6), so the measure is carried and the judgment is
    /// left to whoever reads it.
    ///
    /// **Right-censored at `Params::examined_history_ms`**, because that is as far back as the
    /// pass looks: a gap longer than the examined history is indistinguishable here from no
    /// prior close at all. Stated rather than discovered — it bounds what the banked
    /// distribution can ever say, and therefore how wide a re-derived window could be.
    pub gap_ms: Option<f64>,
}

/// One fully-observed residency: a block held from an observed open to an observed close.
///
/// **Both bounds observed or it does not exist**, which is the rule [`Interval`] already stated
/// at its own fields before anything banked it — a left-open interval "predates observation;
/// excluded from duration statistics", and a right-open one is "censored" at the window end. A
/// duration derived from a bound nobody saw would be a measurement of the window rather than of
/// the cache.
///
/// **Carries the class of the store that OPENED it**, because that is the only classification an
/// interval honestly has, and it makes the useful question askable: how long did blocks live
/// that were admitted cold, against those that were re-admitted.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedResidency {
    /// The interval's key. With the OPEN instant, the key the residency row replaces on: a
    /// residency is opened by one store, so its open instant is stable across
    /// re-classification while its close can move.
    pub key: IntervalKey,
    /// The instant the opening store was observed, engine-domain milliseconds. With the holder
    /// and the identity, the residency row's replace key.
    pub open_ms: f64,
    /// The instant residency ended — **the day this sample is attributed to**, because that is
    /// when the duration became knowable. Attributing to the OPEN day instead would censor every
    /// sample at the end of its own day, biasing the distribution by time of day.
    pub close_ms: f64,
    /// `close_ms - open_ms`, milliseconds. Meaningful only because both bounds were observed,
    /// which is the struct's admission rule.
    pub duration_ms: f64,
    /// The class of the store that OPENED the residency, the only class an interval honestly
    /// has (see the type docs).
    pub opener_class: Class,
    /// The OPENING store's declaration, on the same rule `opener_class` follows: a residency is
    /// described by the store that began it.
    pub workload_class: Option<String>,
}

/// The classification result with its aggregates and the coverage figure it must never
/// appear without.
#[derive(Debug, Clone, PartialEq)]
pub struct Classification {
    /// Every classified store in the window, in the classifier's `(instance, at_ms, …)` order.
    pub stores: Vec<ClassifiedStore>,
    /// `n_tokens` summed over the `ETR` stores. A lower bound: an absent count coalesces to 0,
    /// which undercounts waste and never inflates it.
    pub eviction_waste_tokens: i64,
    /// `n_tokens` summed over the `post_clear` stores, on the same lower-bound rule.
    pub post_clear_tokens: i64,
    /// Stores per bucket, keyed by [`Class::as_str`]'s spelling. A bucket no store landed in
    /// has no entry.
    pub bucket_counts: BTreeMap<&'static str, u64>,
    /// The window's observed-coverage figure, which every figure above is stated at. Describes
    /// the WINDOW, so it rides here and not on the per-group output.
    pub coverage: Coverage,
    /// Fully-observed residencies, for the banked duration distribution. Not a per-store fact,
    /// so it rides beside the stores rather than on them.
    pub residencies: Vec<ObservedResidency>,
}

struct StoreEvent {
    instance: String,
    /// The producer RUN this fact arrived under. Carried so the emit layer can join the
    /// run-scoped model relay: the roster is keyed per run, and an instance-only join would
    /// mislabel across a restart that reuses the slot.
    incarnation: String,
    workload_class: Option<String>,
    dp_rank: u32,
    group_idx: u32,
    block: String,
    key_epoch: Option<u32>,
    at_ms: f64,
    n_tokens: i64,
}

/// The identity a store event is classified against: **which cache**, which prefix, which
/// epoch. The cache is the HOLDER, not the instance: a store by data-parallel worker 1 lands in
/// a physically different cache than worker 0's, so it is not a re-admit of worker 0's block,
/// and worker 0's prior evict does not make it an ETR. The tier is absent, as it is from
/// [`Holder`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClassKey {
    /// The cache.
    pub holder: Holder,
    /// The block identity.
    pub block: String,
    /// The epoch scoping the identity, where one was declared.
    pub key_epoch: Option<u32>,
}

impl ClassKey {
    /// The key `ev` is classified under, owned: for the opener maps, which outlive the event.
    fn of(ev: &StoreEvent) -> Self {
        Self {
            holder: Holder {
                instance: ev.instance.clone(),
                dp_rank: ev.dp_rank,
                group_idx: ev.group_idx,
            },
            block: ev.block.clone(),
            key_epoch: ev.key_epoch,
        }
    }
}

/// A [`ClassKey`] borrowed from what it names: what the indexes are keyed by, so a lookup
/// allocates nothing. Ordered as the owned key is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ClassKeyRef<'a> {
    instance: &'a str,
    dp_rank: u32,
    group_idx: u32,
    block: &'a str,
    key_epoch: Option<u32>,
}

impl<'a> ClassKeyRef<'a> {
    fn of_interval(iv: &'a Interval) -> Self {
        Self {
            instance: iv.key.instance.as_str(),
            dp_rank: iv.key.dp_rank,
            group_idx: iv.key.group_idx,
            block: iv.key.identity.as_str(),
            key_epoch: iv.key.key_epoch,
        }
    }

    fn of_event(ev: &'a StoreEvent) -> Self {
        Self {
            instance: ev.instance.as_str(),
            dp_rank: ev.dp_rank,
            group_idx: ev.group_idx,
            block: ev.block.as_str(),
            key_epoch: ev.key_epoch,
        }
    }
}

// Left-open reads as open-since-forever (an observed evict proves residency until its
// close); censored reads as open-through-window-end and beyond (no close was observed,
// so a widened instant near the window edge is still covered — censoring is absence of
// a close, not evidence of one).
fn effective_bounds(iv: &Interval) -> (f64, f64) {
    (
        iv.open_ms.unwrap_or(f64::NEG_INFINITY),
        iv.close_ms.unwrap_or(f64::INFINITY),
    )
}

fn same_holder(a: &Interval, b: &Interval) -> bool {
    a.key.instance == b.key.instance
        && a.key.dp_rank == b.key.dp_rank
        && a.key.group_idx == b.key.group_idx
}

fn holds(iv: &Interval, ev: &StoreEvent) -> bool {
    iv.key.instance == ev.instance
        && iv.key.dp_rank == ev.dp_rank
        && iv.key.group_idx == ev.group_idx
}

/// One key's residencies, indexed for the three questions the taxonomy asks of a store's
/// own cache: is the store inside an open residency, does it open one, and which close
/// came last before it. Each answer is a binary search. A scan of the key's residencies
/// answered them before, per store, so a hot key — the prefix block every request
/// restates all day — made its group's classification quadratic in its stores.
#[derive(Debug, Default)]
struct KeyIndex {
    /// The observed opening instants, ascending.
    opens: Vec<f64>,
    /// The furthest close (`INFINITY` unobserved) among `opens` up to each one.
    reach: Vec<f64>,
    /// The observed closes with their reason, ascending by instant and, at one instant, in
    /// the intervals' own order — so the last of a tie decides, as `max_by` did.
    closes: Vec<(f64, Option<CloseReason>)>,
}

impl KeyIndex {
    fn of(intervals: &[&Interval]) -> Self {
        let mut opened: Vec<(f64, f64)> = intervals
            .iter()
            .filter_map(|iv| iv.open_ms.map(|o| (o, effective_bounds(iv).1)))
            .collect();
        opened.sort_by(|a, b| a.0.total_cmp(&b.0));
        let reach = prefix(opened.iter().map(|(_, close)| *close), f64::max);
        let mut closes: Vec<(f64, Option<CloseReason>)> = intervals
            .iter()
            .filter_map(|iv| iv.close_ms.map(|c| (c, iv.close_reason)))
            .collect();
        closes.sort_by(|a, b| a.0.total_cmp(&b.0));
        Self {
            opens: opened.into_iter().map(|(open, _)| open).collect(),
            reach,
            closes,
        }
    }

    /// Whether a store at `at` is strictly inside a residency whose open was observed, however
    /// long before it.
    fn over_open(&self, at: f64) -> bool {
        let before = self.opens.partition_point(|open| *open < at);
        before
            .checked_sub(1)
            .is_some_and(|last| at < self.reach[last])
    }

    /// Whether a residency opens at exactly `at` — the instant the builder stated, compared
    /// as a value: an opener and its store carry the same milliseconds.
    fn opens_at(&self, at: f64) -> bool {
        let before = self.opens.partition_point(|open| *open < at);
        self.opens
            .get(before)
            .is_some_and(|open| open.partial_cmp(&at) == Some(std::cmp::Ordering::Equal))
    }

    /// The last observed close at or before `at`, with its reason.
    fn prior_close(&self, at: f64) -> Option<(f64, Option<CloseReason>)> {
        let through = self.closes.partition_point(|(close, _)| *close <= at);
        through.checked_sub(1).map(|i| self.closes[i])
    }
}

/// An identity's residencies across every holder, indexed for the RE question: does
/// ANOTHER cache hold the block through the store's skew-widened instant, by a residency whose
/// open was observed. Up to each residency by open, one lookup gives the one reaching furthest
/// and the furthest reach of a holder other than its own: whichever is not the store's is the
/// answer.
#[derive(Debug)]
struct BlockIndex<'a> {
    /// The observed opens, ascending.
    opens: Vec<f64>,
    /// The reach of the residencies up to each open.
    reach: Vec<Reach<'a>>,
}

/// A range of residencies' reach: the furthest effective close and the residency holding it,
/// and the furthest close of a holder other than that one's, with that residency.
#[derive(Debug, Clone, Copy)]
struct Reach<'a> {
    close: f64,
    holder: &'a Interval,
    elsewhere: Option<(f64, &'a Interval)>,
}

impl<'a> Reach<'a> {
    fn of(iv: &'a Interval) -> Self {
        Self {
            close: effective_bounds(iv).1,
            holder: iv,
            elsewhere: None,
        }
    }

    /// Two ranges' reach together: an idempotent join, so overlapping ranges combine.
    fn join(a: Self, b: Self) -> Self {
        let (best, other) = if a.close >= b.close { (a, b) } else { (b, a) };
        let elsewhere = [
            best.elsewhere,
            Some((other.close, other.holder)),
            other.elsewhere,
        ]
        .into_iter()
        .flatten()
        .filter(|(_, iv)| !same_holder(iv, best.holder))
        .max_by(|x, y| x.0.total_cmp(&y.0));
        Self {
            close: best.close,
            holder: best.holder,
            elsewhere,
        }
    }
}

impl<'a> BlockIndex<'a> {
    fn of(intervals: &[&'a Interval]) -> Self {
        let mut observed: Vec<(f64, &'a Interval)> = intervals
            .iter()
            .filter_map(|iv| iv.open_ms.map(|open| (open, *iv)))
            .collect();
        observed.sort_by(|a, b| a.0.total_cmp(&b.0));
        Self {
            opens: observed.iter().map(|(open, _)| *open).collect(),
            reach: prefix(observed.iter().map(|(_, iv)| Reach::of(iv)), Reach::join),
        }
    }

    /// Whether a holder other than the store's held the block from `delta` before the
    /// store through `delta` after it, by a residency whose open was observed.
    fn held_elsewhere(&self, ev: &StoreEvent, delta: f64) -> bool {
        let opened = self.opens.partition_point(|open| *open <= ev.at_ms - delta);
        let Some(reach) = opened.checked_sub(1).map(|last| self.reach[last]) else {
            return false;
        };
        let close = if holds(reach.holder, ev) {
            reach
                .elsewhere
                .map_or(f64::NEG_INFINITY, |(close, _)| close)
        } else {
            reach.close
        };
        ev.at_ms + delta < close
    }
}

/// Each value joined with every one before it.
fn prefix<T: Copy>(values: impl IntoIterator<Item = T>, join: fn(T, T) -> T) -> Vec<T> {
    let mut out: Vec<T> = Vec::new();
    for v in values {
        out.push(out.last().map_or(v, |last| join(*last, v)));
    }
    out
}

/// The store facts one identity group's classifier reads, taken from the group's own
/// key streams.
///
/// **The filters are the flat path's, inherited by construction.** The grouping already
/// applied own-copy locality and the identity requirement (`block_id IS NOT NULL` in the
/// SQL this lineage replaces) — a remote or identity-less store never entered a key
/// stream, so extracting stores FROM the streams cannot disagree with the substrate. The
/// synthetic scope-closers the expansion appended are `clear`/`holder_reset` kinds and
/// fall to the `Store` filter.
fn store_events_of_keys(
    keys: &[(crate::events::IntervalKey, Vec<crate::events::Event>)],
) -> Vec<StoreEvent> {
    let mut out: Vec<StoreEvent> = keys
        .iter()
        .flat_map(|(_, evs)| evs.iter())
        .filter(|e| e.kind == crate::events::Kind::Store)
        .filter_map(|e| {
            let block = e.identity()?.to_string();
            let (dp_rank, group_idx) = e.holder_dims();
            Some(StoreEvent {
                instance: e.instance.clone(),
                incarnation: e.incarnation.clone(),
                workload_class: e.workload_class.clone(),
                dp_rank,
                group_idx,
                block,
                key_epoch: e.key_epoch,
                at_ms: e.at_ms,
                n_tokens: e.n_tokens,
            })
        })
        .collect();
    out.sort_by(store_order);
    out
}

/// The classifier's processing order — **`(instance, at_ms)` to match the query this
/// lineage replaces, then enough of the identity to be total.** The SQL's order was not
/// total, so the engine broke same-instant ties arbitrarily; this one cannot, because a
/// fold whose output depends on input order must be handed an order that does not vary.
/// Where the two differ — two stores on one key at one instant — the choice decides only
/// which is the opener and which the re-admit, so the multiset of classifications is
/// unchanged. Stated once and used by both the per-group sort and the window-level merge,
/// so the two paths emit one order.
fn store_order(a: &StoreEvent, b: &StoreEvent) -> std::cmp::Ordering {
    a.instance
        .cmp(&b.instance)
        .then_with(|| a.at_ms.total_cmp(&b.at_ms))
        .then_with(|| a.dp_rank.cmp(&b.dp_rank))
        .then_with(|| a.group_idx.cmp(&b.group_idx))
        .then_with(|| a.block.cmp(&b.block))
        .then_with(|| a.key_epoch.cmp(&b.key_epoch))
}

/// [`store_order`] over classified output, for the window-level merge: the same six fields,
/// so a stable sort over the concatenated groups states the order one fold of the whole
/// window would (ties are adjacent within one key stream in both shapes, and stability keeps
/// their relative order).
fn classified_order(a: &ClassifiedStore, b: &ClassifiedStore) -> std::cmp::Ordering {
    a.instance
        .cmp(&b.instance)
        .then_with(|| a.at_ms.total_cmp(&b.at_ms))
        .then_with(|| a.dp_rank.cmp(&b.dp_rank))
        .then_with(|| a.group_idx.cmp(&b.group_idx))
        .then_with(|| a.block.cmp(&b.block))
        .then_with(|| a.key_epoch.cmp(&b.key_epoch))
}

/// One identity group's classification products — repeat compute's unit of
/// output. Aggregates ride per group and SUM across groups; the coverage
/// figure deliberately does not appear here, because coverage describes the WINDOW and a
/// group must not carry a claim about ground it never saw.
#[derive(Debug)]
pub struct GroupClassification {
    /// This group's classified stores, in fold order.
    pub stores: Vec<ClassifiedStore>,
    /// This group's fully-observed residencies, in fold order.
    pub residencies: Vec<ObservedResidency>,
    /// `n_tokens` over this group's `ETR` stores; sums across groups into the window figure.
    pub eviction_waste_tokens: i64,
    /// `n_tokens` over this group's `post_clear` stores; sums across groups the same way.
    pub post_clear_tokens: i64,
    /// This group's stores per bucket, keyed by [`Class::as_str`]'s spelling; sums across
    /// groups.
    pub bucket_counts: BTreeMap<&'static str, u64>,
    /// Each interval identity's residency state at the window's end — the last-close
    /// index rows. Derived from the SAME
    /// intervals the classes came from, so the index and the classifier cannot disagree
    /// about what a close was.
    pub frontiers: Vec<Frontier>,
}

/// One interval identity's residency state at the analysis window's end.
///
/// **The classifier's context, banked**: a future slice classifies its stores against
/// this instead of re-reading `examined_history` of events. Censoring is the READER's
/// law, deliberately not this row's — the row states what was observed; whether a close
/// is too old to consult is `Params::examined_history_ms` applied at the consumer, so
/// banking cannot silently uncensor a gap.
#[derive(Debug, Clone, PartialEq)]
pub struct Frontier {
    /// The identity this row states residency for, as the interval carries it.
    pub key: IntervalKey,
    /// The most recent observed CLOSE and its reason — the next store's gap context.
    /// `None` = no close observed in this window.
    pub last_close_ms: Option<f64>,
    /// Why `last_close_ms` closed: the reason that decides the next store's class. `None`
    /// exactly when `last_close_ms` is.
    pub last_close_reason: Option<crate::intervals::CloseReason>,
    /// Still resident at window end. `open_since_ms` is `None` on a left-open residency
    /// (predates observation) — open with an unknown start, a real state.
    pub open: bool,
    /// The open residency's observed opening instant, engine-domain milliseconds. `None` when
    /// `open` is false, and on a left-open residency whose start predates observation.
    pub open_since_ms: Option<f64>,
    /// Stores the open residency had absorbed beyond its opener — resumed by the slice
    /// fold so a re-admit count never restarts at a boundary.
    pub open_readmits: u64,
    /// The opening store's declaration, carried so a residency closed in a LATER slice
    /// samples under its opener's workload — the rule `Interval::workload_class` states.
    pub open_workload_class: Option<String>,
    /// The opening store's class, for the same reason: `ObservedResidency::opener_class`
    /// is the opener's verdict, and the opener may have been classified slices ago.
    pub open_opener_class: Option<Class>,
}

/// The opener map: each interval's opening store's class, keyed by the interval key and
/// the open instant's bit pattern — built during classification, seeded across slices.
type OpenerMap = std::collections::HashMap<(ClassKey, u64), Class>;

/// The frontier fold: one row per interval identity, from the intervals themselves.
/// `openers` is the classification's own opener map, so an open residency's frontier
/// carries the class its opener was given — the one fact intervals alone cannot state.
#[must_use]
#[allow(clippy::implicit_hasher)] // one internal caller family; a hasher generic is noise here
pub fn frontiers(intervals: &[Interval], openers: &OpenerMap) -> Vec<Frontier> {
    // `BTreeMap` for deterministic emission order — the banked rows must be a function of
    // the intervals, not of hashing.
    let mut by_identity: BTreeMap<IntervalKey, Frontier> = BTreeMap::new();
    for iv in intervals {
        let slot = by_identity
            .entry(iv.key.clone())
            .or_insert_with(|| Frontier {
                key: iv.key.clone(),
                last_close_ms: None,
                last_close_reason: None,
                open: false,
                open_since_ms: None,
                open_readmits: 0,
                open_workload_class: None,
                open_opener_class: None,
            });
        if let Some(close) = iv.close_ms {
            if slot.last_close_ms.is_none_or(|held| close > held) {
                slot.last_close_ms = Some(close);
                slot.last_close_reason = iv.close_reason;
            }
        } else {
            // Right-open: still resident at window end.
            slot.open = true;
            slot.open_since_ms = iv.open_ms;
            slot.open_readmits = iv.readmits;
            slot.open_workload_class.clone_from(&iv.workload_class);
            slot.open_opener_class = iv.open_ms.and_then(|open| {
                openers
                    .get(&(
                        ClassKey {
                            holder: iv.holder(),
                            block: iv.key.identity.clone(),
                            key_epoch: iv.key.key_epoch,
                        },
                        open.to_bits(),
                    ))
                    .copied()
            });
        }
    }
    by_identity.into_values().collect()
}

/// Classify ONE identity group — the self-contained unit (join-semantics over the identity
/// key): every interval a store here can consult lives in this group, because the
/// classifier's one cross-key read (`by_block`) is keyed by exactly the identity this
/// group is. The streamed pass drives this per group from an engine-ordered read; the
/// flat entry drives it from an in-memory partition. One fold, two drivers.
#[must_use]
pub fn classify_identity_group(
    group: &crate::events::IdentityGroup,
    closers: &crate::events::ScopeClosers,
    model: &crate::completeness::UncertaintyModel,
    params: &Params,
) -> GroupClassification {
    let intervals = crate::intervals::residency_intervals_for_keys(&group.keys, closers);
    let events = store_events_of_keys(&group.keys);
    classify_group_core(&intervals, &events, model, params, &OpenerMap::new())
}

/// [`classify_identity_group`] resumed from banked frontiers — the slice classifier. Classifies THE SAME function in two sittings: the
/// seeds are the state a whole-window fold reaches at the slice boundary, so the slice's
/// stores, samples and next frontier are byte-equal to the whole window's, restricted to
/// the slice — `slice_equals_whole_*` is the pin.
///
/// **The seeds are carried whole.** What a store may see of them is the visibility law the
/// core applies per store: a close or an open within the examined history before the store.
/// So a store's class is a function of its own instant, never of where a slice began.
#[must_use]
pub fn classify_identity_group_seeded(
    group: &crate::events::IdentityGroup,
    closers: &crate::events::ScopeClosers,
    model: &crate::completeness::UncertaintyModel,
    params: &Params,
    seeds: &[Frontier],
) -> GroupClassification {
    let mut seed_map: std::collections::BTreeMap<
        crate::events::IntervalKey,
        crate::intervals::IntervalSeed,
    > = std::collections::BTreeMap::new();
    let mut seed_openers = OpenerMap::new();
    for f in seeds {
        let mut seed = crate::intervals::IntervalSeed::default();
        // A banked close with no banked reason cannot happen from our own fold (every close
        // carries one); a silently invented reason is refused by skipping the close.
        if let (Some(close), Some(reason)) = (f.last_close_ms, f.last_close_reason) {
            seed.last_close = Some((close, reason));
        }
        if f.open {
            seed.open = Some(crate::intervals::FoldSeed {
                open_since_ms: f.open_since_ms,
                readmits: f.open_readmits,
                workload_class: f.open_workload_class.clone(),
            });
            if let (Some(open), Some(class)) = (f.open_since_ms, f.open_opener_class) {
                seed_openers.insert(
                    (
                        ClassKey {
                            holder: f.key.holder(),
                            block: f.key.identity.clone(),
                            key_epoch: f.key.key_epoch,
                        },
                        open.to_bits(),
                    ),
                    class,
                );
            }
        }
        if seed != crate::intervals::IntervalSeed::default() {
            seed_map.insert(f.key.clone(), seed);
        }
    }
    let intervals =
        crate::intervals::residency_intervals_for_keys_seeded(&group.keys, closers, &seed_map);
    let events = store_events_of_keys(&group.keys);
    classify_group_core(&intervals, &events, model, params, &seed_openers)
}

/// The dormant half of a sliced window: identities with banked OPEN state, no slice
/// events of their own, and a scope closer that reaches them. A whole-window fold never has
/// this case — there the opener event is in the stream, so the key exists and the clear
/// merges in — but a slice stream carries only the slice's events, and a clear closes
/// residencies its own stream never names. The driver appends these EMPTY groups to the
/// slice's real ones; the seeded fold then closes each against its covering closers, which
/// is what emits the residency sample and corrects the outgoing frontier.
///
/// Bounded by the CLOSERS, not by the seed count: a slice with no scope closers returns
/// nothing, which is every steady-state slice.
#[must_use]
pub fn dormant_groups<'a>(
    seeds: impl IntoIterator<Item = &'a Frontier>,
    closers: &crate::events::ScopeClosers,
    grouped: &std::collections::BTreeSet<(String, Option<u32>)>,
) -> Vec<crate::events::IdentityGroup> {
    type KeyedStreams = Vec<(crate::events::IntervalKey, Vec<crate::events::Event>)>;
    let mut groups: BTreeMap<(String, Option<u32>), KeyedStreams> = BTreeMap::new();
    for f in seeds {
        if !f.open || grouped.contains(&(f.key.identity.clone(), f.key.key_epoch)) {
            continue;
        }
        let key = f.key.clone();
        if !closers.covers(&key) {
            continue;
        }
        groups
            .entry((f.key.identity.clone(), f.key.key_epoch))
            .or_default()
            .push((key, Vec::new()));
    }
    groups
        .into_iter()
        .map(
            |((identity, key_epoch), keys)| crate::events::IdentityGroup {
                identity,
                key_epoch,
                keys,
            },
        )
        .collect()
}

/// A store's own residencies, by classification key.
type OwnIndex<'a> = BTreeMap<ClassKeyRef<'a>, KeyIndex>;
/// The same block's residencies on every holder, by block and epoch.
type BlockIndexes<'a> = BTreeMap<(&'a str, Option<u32>), BlockIndex<'a>>;

/// The intervals indexed two ways: by the classification key, for a store's own residencies,
/// and by the block and its epoch, for the residencies of the same block on other holders.
fn indexes(intervals: &[Interval]) -> (OwnIndex<'_>, BlockIndexes<'_>) {
    let mut by_key: BTreeMap<ClassKeyRef<'_>, Vec<&Interval>> = BTreeMap::new();
    let mut by_block: BTreeMap<(&str, Option<u32>), Vec<&Interval>> = BTreeMap::new();
    for iv in intervals {
        by_key
            .entry(ClassKeyRef::of_interval(iv))
            .or_default()
            .push(iv);
        by_block
            .entry((iv.key.identity.as_str(), iv.key.key_epoch))
            .or_default()
            .push(iv);
    }
    (
        by_key
            .into_iter()
            .map(|(k, ivs)| (k, KeyIndex::of(&ivs)))
            .collect(),
        by_block
            .into_iter()
            .map(|(k, ivs)| (k, BlockIndex::of(&ivs)))
            .collect(),
    )
}

/// The taxonomy itself — pure, over any self-contained (intervals, events) slice: one
/// identity group's.
fn classify_group_core(
    intervals: &[Interval],
    events: &[StoreEvent],
    model: &crate::completeness::UncertaintyModel,
    params: &Params,
    seed_openers: &OpenerMap,
) -> GroupClassification {
    let (by_key, by_block) = indexes(intervals);
    let no_residency = KeyIndex::default();

    let mut stores = Vec::with_capacity(events.len());
    let mut eviction_waste = 0i64;
    let mut post_clear_tokens = 0i64;
    let mut buckets: BTreeMap<&'static str, u64> = BTreeMap::new();
    // same-instant opener bookkeeping: the builder counts every store beyond the first
    // at an interval's opening instant as a readmit (store_count − 1); the classifier
    // must agree, so the first event at an opening instant is the opener and the rest
    // are re_admits (ms-resolution duplicates are plausible under burst)
    let mut opener_seen: std::collections::HashMap<(ClassKey, u64), u64> =
        std::collections::HashMap::new();

    // The class of the store that OPENED each interval, keyed by (interval key, open instant).
    // Collected during classification because that is where the opener is already identified —
    // walking the intervals afterwards would have to re-derive it.
    //
    // It carries no WORKLOAD class: `Interval::workload_class` states that rule once, and two
    // copies of "which store owns this residency" is how two tables come to disagree.
    let mut opener_class: std::collections::HashMap<(ClassKey, u64), Class> = seed_openers.clone();

    for ev in events {
        let own = by_key
            .get(&ClassKeyRef::of_event(ev))
            .unwrap_or(&no_residency);
        let opens_here = own.opens_at(ev.at_ms);

        let same_instant_readmit = opens_here && {
            let slot = opener_seen
                .entry((ClassKey::of(ev), ev.at_ms.to_bits()))
                .or_insert(0);
            *slot += 1;
            *slot > 1
        };

        let (class, gap_ms) = class_of(
            ev,
            own,
            by_block.get(&(ev.block.as_str(), ev.key_epoch)),
            same_instant_readmit,
            model,
            params,
        );

        // lower-bound aggregates: absent n_tokens coalesces to 0 (undercounts waste,
        // never inflates it — the ≥ register the report prints)
        match class {
            Class::Etr => eviction_waste += ev.n_tokens,
            Class::PostClear => post_clear_tokens += ev.n_tokens,
            _ => {}
        }
        *buckets.entry(class.as_str()).or_insert(0) += 1;
        // the FIRST event at an interval's opening instant is its opener; later ones at the
        // same instant are re-admits and do not own the residency
        if opens_here {
            opener_class
                .entry((ClassKey::of(ev), ev.at_ms.to_bits()))
                .or_insert(class);
        }
        stores.push(ClassifiedStore {
            workload_class: ev.workload_class.clone(),
            instance: ev.instance.clone(),
            incarnation: ev.incarnation.clone(),
            dp_rank: ev.dp_rank,
            group_idx: ev.group_idx,
            block: ev.block.clone(),
            key_epoch: ev.key_epoch,
            at_ms: ev.at_ms,
            n_tokens: ev.n_tokens,
            class,
            gap_ms,
        });
    }

    GroupClassification {
        stores,
        residencies: observed_residencies(intervals, &opener_class),
        eviction_waste_tokens: eviction_waste,
        post_clear_tokens,
        bucket_counts: buckets,
        frontiers: frontiers(intervals, &opener_class),
    }
}

/// The class one store takes, and the gap to the prior close on its own key, where there was
/// one. `own` is the store's own key's residencies, `block` the same block's on other holders.
fn class_of(
    ev: &StoreEvent,
    own: &KeyIndex,
    block: Option<&BlockIndex<'_>>,
    same_instant_readmit: bool,
    model: &crate::completeness::UncertaintyModel,
    params: &Params,
) -> (Class, Option<f64>) {
    let delta = params.delta_skew_ms;
    let horizon = params.examined_history_ms;
    let mut gap_ms: Option<f64> = None;
    let class = if own.over_open(ev.at_ms) || same_instant_readmit {
        // class 0: a store over an open interval — positive, never tainted
        Class::ReAdmit
    } else if block.is_some_and(|block| block.held_elsewhere(ev, delta)) {
        // class 1 (RE): the widened instant must be fully covered — skew can
        // only weaken, never manufacture, the positive
        Class::Re
    } else {
        // classes 2/3: the most recent prior close on the store's own key decides by its REASON,
        // and by nothing else. There is no gap condition — an attribution window
        // would be an editorial judgment about the customer's cache baked into the
        // register (`docs/classification-semantics.md` §4, §12).
        // The gap is carried alongside as a measure (below), for a reader to draw
        // their own line on.
        // A close beyond the examined history is no close: `cold` claims nothing about
        // what the history before the horizon held, and a frontier carried from an earlier
        // window must say the same as a re-read of the history would.
        let prior_close = own
            .prior_close(ev.at_ms)
            .filter(|(close, _)| ev.at_ms - close < horizon);
        // **Banked whatever the class turns out to be** — see `ClassifiedStore::gap_ms`.
        // The class below is decided by the close's REASON; the gap rides beside it as a
        // measure, which is the whole shape of the change: the register states what was
        // observed and the reader draws the line.
        gap_ms = prior_close.map(|(c, _)| ev.at_ms - c);
        // **A close decides by its reason; an absence decides by whether it can be
        // trusted.** Two arms, and the shape says which is which — the presence case is
        // now a lookup rather than a comparison, which is the entire behavioural content
        // of deleting the window.
        if let Some((_, Some(reason))) = prior_close {
            match reason {
                CloseReason::Evict => Class::Etr,
                CloseReason::Clear => Class::PostClear,
                // The engine restarted and the cache began empty: this store is the
                // cold start, decided by EVIDENCE rather than absence — the reset
                // record is exactly the announcement the absence arm below must do
                // without. Not `PostClear`: nothing was cleared or announced. Not
                // `Etr`: a restart is not capacity pressure, and charging it as
                // eviction waste would indict the wrong remedy.
                CloseReason::Reset => Class::Cold,
                // The holder went unobserved: the block may have stayed or left, so neither
                // `re_admit` nor a waste class can be claimed.
                CloseReason::Silent => Class::Indeterminate,
            }
        } else {
            // absence-decided: cold unless the deciding evidence is tainted.
            // The deciding ranges (class 5, both named in the spec text):
            // the EXAMINED-HISTORY lookback on the OWN instance — a drop window
            // anywhere in it could have swallowed the close that would have
            // decided this store — and the RE-simultaneity range on any OTHER
            // instance. Plus the record's own membership taint (producer-clock
            // membership in a drop window).
            //
            // **The scope is the horizon because the absence claim now is.** It used
            // to be `w_refetch_ms`, which was correct while "cold" meant "no prior
            // close within the attribution window". With the window gone, `cold`
            // asserts no prior close within the EXAMINED HISTORY, and the taint range
            // has to cover exactly the range the claim is about or absence is being
            // read as evidence over ground nobody vouched for.
            let tainted = model.is_record_tainted(&ev.instance, ev.at_ms)
                || model.own_instance_range_taint(
                    &ev.instance,
                    ev.at_ms - params.examined_history_ms,
                    ev.at_ms,
                )
                || model.cross_instance_taint(&ev.instance, ev.at_ms - delta, ev.at_ms + delta)
                // The simultaneity range on the OWN instance counts too, because a
                // sibling data-parallel worker is another HOLDER: a drop there could
                // have swallowed the store that would have made this RE. Before the
                // scope fix this was unreachable -- same instance meant same cache, and
                // same-cache residency is decided by re_admit/ETR, never RE.
                //
                // Almost all of this range is already covered by the examined-history
                // lookback above, since the horizon (hours) dwarfs delta
                // (milliseconds). What it newly catches is a drop window opening in
                // (t, t + delta] -- narrow, and still a case where absence is not
                // evidence.
                || model.own_instance_range_taint(
                    &ev.instance,
                    ev.at_ms - delta,
                    ev.at_ms + delta,
                );
            if tainted {
                Class::Indeterminate
            } else {
                Class::Cold
            }
        }
    };
    (class, gap_ms)
}

/// Fully-observed residencies only: both bounds seen, and an opening store that was
/// classified. Every filter here drops a sample that would otherwise measure the WINDOW rather
/// than the cache — the rule `Interval`'s own fields state.
fn observed_residencies(
    intervals: &[Interval],
    opener_class: &OpenerMap,
) -> Vec<ObservedResidency> {
    intervals
        .iter()
        .filter_map(|iv| {
            let (open, close) = (iv.open_ms?, iv.close_ms?);
            let key = ClassKey {
                holder: iv.holder(),
                block: iv.key.identity.clone(),
                key_epoch: iv.key.key_epoch,
            };
            let opener = opener_class.get(&(key, open.to_bits()))?;
            Some(ObservedResidency {
                key: iv.key.clone(),
                open_ms: open,
                close_ms: close,
                duration_ms: close - open,
                opener_class: *opener,
                // One definition, read off the interval the opener began.
                workload_class: iv.workload_class.clone(),
            })
        })
        .collect()
}

/// Classify from the Ordered Event IR: the window-level entry, a driver of the grouped fold.
///
/// Groups the window on the identity axis in memory and folds each group through
/// [`classify_identity_group`], the SAME fold the streamed pass drives, then merges the
/// groups' output into one emission order (`store_order`'s fields for stores; interval-key
/// order for residencies), so a window folded whole and a window folded by group state the
/// same rows. The uncertainty model is handed in rather than built here: it describes the
/// window, so it is the one input the groups share.
#[must_use]
pub fn classify_from_events(
    ir: &[crate::events::Event],
    model: &crate::completeness::UncertaintyModel,
    params: &Params,
) -> Classification {
    let mut stores = Vec::new();
    let mut residencies: Vec<ObservedResidency> = Vec::new();
    let mut eviction_waste = 0i64;
    let mut post_clear_tokens = 0i64;
    let mut buckets: BTreeMap<&'static str, u64> = BTreeMap::new();
    let closers = crate::events::ScopeClosers::of(ir);
    for group in crate::events::group_by_identity(ir) {
        let out = classify_identity_group(&group, &closers, model, params);
        stores.extend(out.stores);
        residencies.extend(out.residencies);
        eviction_waste += out.eviction_waste_tokens;
        post_clear_tokens += out.post_clear_tokens;
        for (name, count) in out.bucket_counts {
            *buckets.entry(name).or_insert(0) += count;
        }
    }
    stores.sort_by(classified_order);
    residencies.sort_by(|a, b| a.key.cmp(&b.key));
    Classification {
        stores,
        eviction_waste_tokens: eviction_waste,
        post_clear_tokens,
        bucket_counts: buckets,
        coverage: model.coverage(None),
        residencies,
    }
}

#[cfg(test)]
mod class_set_tests {
    use super::Class;

    #[test]
    fn every_class_is_in_all_and_spells_itself_once() {
        // `as_str`'s match is exhaustive, so a new variant is a compile error there. `ALL` is a
        // list and would silently stay short -- this is what makes the two agree.
        let names: std::collections::BTreeSet<&str> =
            Class::ALL.iter().map(Class::as_str).collect();
        assert_eq!(
            names.len(),
            Class::ALL.len(),
            "two classes share a spelling: {names:?}"
        );
        assert_eq!(
            names,
            [
                "ETR",
                "RE",
                "cold",
                "indeterminate",
                "post_clear",
                "re_admit"
            ]
            .into_iter()
            .collect(),
            "the closed set changed; the read API generates its `class` parameter from it"
        );
    }
}

#[cfg(test)]
mod frontier_tests {
    use super::{frontiers, Frontier};
    use crate::events::IntervalKey;
    use crate::intervals::{CloseReason, Interval};

    fn interval(block: &str, open: Option<f64>, close: Option<f64>) -> Interval {
        Interval {
            key: IntervalKey {
                instance: "i1".into(),
                dp_rank: 0,
                group_idx: 0,
                identity: block.into(),
                key_epoch: None,
                tier: None,
                backend_id: None,
            },
            open_ms: open,
            close_ms: close,
            close_reason: close.map(|_| CloseReason::Evict),
            readmits: 0,
            workload_class: None,
        }
    }

    #[test]
    fn the_frontier_is_the_newest_close_and_the_open_tail() {
        // Three residencies of one identity: closed at 10, closed at 30, open since 40.
        // The frontier remembers the NEWEST close and the open state — the exact context
        // the next slice's classifier consults.
        let ivs = vec![
            interval("b", Some(0.0), Some(10.0)),
            interval("b", Some(20.0), Some(30.0)),
            interval("b", Some(40.0), None),
        ];
        let f = frontiers(&ivs, &std::collections::HashMap::new());
        assert_eq!(f.len(), 1, "{f:?}");
        let Frontier {
            last_close_ms,
            last_close_reason,
            open,
            open_since_ms,
            ..
        } = &f[0];
        assert_eq!(*last_close_ms, Some(30.0));
        assert_eq!(*last_close_reason, Some(CloseReason::Evict));
        assert!(*open);
        assert_eq!(*open_since_ms, Some(40.0));
    }

    #[test]
    fn a_left_open_residency_is_open_with_an_unknown_start_never_a_zero() {
        let f = frontiers(
            &[interval("b", None, None)],
            &std::collections::HashMap::new(),
        );
        assert!(f[0].open);
        assert_eq!(
            f[0].open_since_ms, None,
            "predates observation: open with an unknown start is a real state"
        );
        assert_eq!(f[0].last_close_ms, None);
    }

    #[test]
    fn identities_do_not_blend_and_the_order_is_a_function_of_the_intervals() {
        let mut tiered = interval("a", Some(0.0), Some(5.0));
        tiered.key.tier = Some("cpu".into());
        let ivs = vec![
            interval("b", Some(0.0), Some(5.0)),
            interval("a", Some(0.0), Some(9.0)),
            tiered,
        ];
        let f = frontiers(&ivs, &std::collections::HashMap::new());
        // Three identities: (a, no tier), (a, cpu) — tier is identity, as on the interval —
        // and (b, no tier). BTreeMap order, so two runs cannot differ.
        assert_eq!(f.len(), 3, "{f:?}");
        assert_eq!(
            f.iter().map(|x| x.last_close_ms).collect::<Vec<_>>(),
            vec![Some(9.0), Some(5.0), Some(5.0)]
        );
        assert!(f.iter().all(|x| !x.open));
    }
}

#[cfg(test)]
mod index_tests {
    //! The indexes against the scans they replaced, over residencies built to hit every
    //! seam: unobserved bounds, opens at one instant, closes tied at one instant with
    //! different reasons, and holders that overlap each other.

    use super::{effective_bounds, BlockIndex, KeyIndex, StoreEvent};
    use crate::events::IntervalKey;
    use crate::intervals::{CloseReason, Interval};

    fn residency(
        holder: (&str, u32),
        open: Option<f64>,
        close: Option<(f64, CloseReason)>,
    ) -> Interval {
        Interval {
            key: IntervalKey {
                instance: holder.0.into(),
                dp_rank: holder.1,
                group_idx: 0,
                identity: "b".into(),
                key_epoch: None,
                tier: None,
                backend_id: None,
            },
            open_ms: open,
            close_ms: close.map(|(c, _)| c),
            close_reason: close.map(|(_, r)| r),
            readmits: 0,
            workload_class: None,
        }
    }

    fn store(holder: (&str, u32), at_ms: f64) -> StoreEvent {
        StoreEvent {
            instance: holder.0.into(),
            incarnation: "r".into(),
            workload_class: None,
            dp_rank: holder.1,
            group_idx: 0,
            block: "b".into(),
            key_epoch: None,
            at_ms,
            n_tokens: 1,
        }
    }

    /// The pattern: two holders, a left-open and a right-open residency each, opens and
    /// closes that coincide, and a close tie with two reasons.
    fn residencies() -> Vec<Interval> {
        let (a, b) = (("i1", 0), ("i1", 1));
        vec![
            residency(a, None, Some((10.0, CloseReason::Evict))),
            residency(a, Some(10.0), Some((20.0, CloseReason::Clear))),
            residency(a, Some(10.0), Some((20.0, CloseReason::Reset))),
            residency(a, Some(25.0), Some((40.0, CloseReason::Evict))),
            residency(a, Some(50.0), None),
            residency(b, Some(5.0), Some((30.0, CloseReason::Evict))),
            residency(b, Some(30.0), Some((35.0, CloseReason::Clear))),
            residency(b, Some(45.0), None),
            residency(b, None, Some((3.0, CloseReason::Evict))),
        ]
    }

    fn instants() -> Vec<f64> {
        (0..=120).map(|t| f64::from(t) * 0.5).collect()
    }

    #[test]
    fn a_keys_index_answers_as_the_scan_did() {
        let all = residencies();
        for holder in [("i1", 0), ("i1", 1)] {
            let own: Vec<&Interval> = all
                .iter()
                .filter(|iv| iv.key.instance == holder.0 && iv.key.dp_rank == holder.1)
                .collect();
            let index = KeyIndex::of(&own);
            for at in instants() {
                let over_open = own.iter().any(|iv| {
                    let (open, close) = effective_bounds(iv);
                    iv.open_ms.is_some() && open < at && at < close
                });
                assert_eq!(index.over_open(at), over_open, "over_open at {at}");
                let opens_at = own.iter().any(|iv| iv.open_ms == Some(at));
                assert_eq!(index.opens_at(at), opens_at, "opens_at {at}");
                let prior = own
                    .iter()
                    .filter_map(|iv| iv.close_ms.map(|c| (c, iv.close_reason)))
                    .filter(|(c, _)| *c <= at)
                    .max_by(|x, y| x.0.total_cmp(&y.0));
                assert_eq!(index.prior_close(at), prior, "prior_close at {at}");
            }
        }
    }

    #[test]
    fn an_identitys_index_answers_as_the_scan_did_for_every_holder() {
        let all = residencies();
        let refs: Vec<&Interval> = all.iter().collect();
        let index = BlockIndex::of(&refs);
        for delta in [0.0, 0.25, 3.0] {
            for holder in [("i1", 0), ("i1", 1), ("i2", 0)] {
                for at in instants() {
                    let ev = store(holder, at);
                    let scanned = refs
                        .iter()
                        .filter(|iv| iv.key.instance != ev.instance || iv.key.dp_rank != ev.dp_rank)
                        .any(|iv| {
                            let (open, close) = effective_bounds(iv);
                            iv.open_ms.is_some() && open <= at - delta && at + delta < close
                        });
                    assert_eq!(
                        index.held_elsewhere(&ev, delta),
                        scanned,
                        "holder {holder:?} at {at} delta {delta}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_hot_keys_index_holds_every_residency_in_order() {
        // A thousand residencies of one key, back to back: the index answers a store in a
        // binary search where the scan compared it with every residency.
        let own: Vec<Interval> = (0..1_000)
            .map(|i| {
                let open = f64::from(i) * 10.0;
                residency(
                    ("i1", 0),
                    Some(open),
                    Some((open + 9.0, CloseReason::Evict)),
                )
            })
            .collect();
        let refs: Vec<&Interval> = own.iter().collect();
        let index = KeyIndex::of(&refs);
        assert_eq!(index.opens.len(), 1_000);
        assert!(index.over_open(9_995.0));
        assert!(!index.over_open(9_999.5));
        assert_eq!(
            index.prior_close(10_000.0),
            Some((9_999.0, Some(CloseReason::Evict)))
        );
    }
}
