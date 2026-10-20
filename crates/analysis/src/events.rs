// SPDX-License-Identifier: AGPL-3.0-only

//! The Ordered Event IR — the cache-fact slice, and the grouping the derivations fold over.
//!
//! **What this is for.** A typed, ordered event set behind one seam — [`EventSource`] — so no
//! derivation in this crate knows where the bytes came from. The production implementation
//! (`pipeline::parquet_source`) reads sealed segments from the bucket through a local content
//! cache; a test hands in whatever second implementation it is proving against. Nothing above
//! the seam changes when a source does.
//!
//! **The IR carries facts, never judgments.** No filter here encodes a semantic rule — not the
//! own-copy rule, not `block_id IS NOT NULL`, not the window's half-open edge. Those belong to
//! whichever derivation applies them, because two derivations legitimately disagree: the residency
//! substrate admits `clear` records with no block identity and excludes cache records without one,
//! and those two filters are different by design. An IR that pre-filtered would have to pick one
//! and would silently starve the other.
//!
//! **The grouping is the interface, not the flat list** ([`group_by_interval_key`]). The residency
//! builder is a fold over one key's events in time order — which is exactly what the SQL it
//! replaces does with `PARTITION BY … ORDER BY`. Handing the fold a grouped stream rather than a
//! flat slice is what keeps the memory strategy behind this boundary: an in-memory grouping, an
//! external sort, or pre-sorted storage are all implementations of the same shape. It matters
//! because sorting by key bounds memory by the largest single key's event count, which is
//! **independent of the examined-history horizon** — so widening that horizon to a week does not
//! change the fold's footprint.
//!
//! **The ordering is total and is part of the contract.** A fold whose output depends on input
//! order must be handed an order that cannot vary, or two runs over identical bytes disagree — the
//! same defect class the examined-history horizon closed one level up. Ties on `at_ms` are
//! ordinary rather than exotic: one batch stamps one timestamp on every event it carries.

use crate::Window;
use std::collections::BTreeMap;

/// The three cache-fact kinds the residency substrate is built from.
///
/// Closed, and deliberately not a `String`: a fourth kind is a semantics change that must break
/// every `match` in this crate rather than fall through a comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// Ordered before the closes deliberately — see [`Event::close_first_rank`].
    Evict,
    /// A scope-level close: the producer emptied a cache, or a declared part of one, naming no
    /// block. The grouping synthesizes it onto every key its declared scope covers.
    Clear,
    /// The producer observed the holder's engine process restart (spec 2.3
    /// `holder_reset`): every residency under its scope closes at the boundary, and the
    /// engine-id space changes with the process.
    HolderReset,
    /// The producer's heartbeats stopped past its liveness bound: every residency of the
    /// instance closes where the silence starts, its cache no longer observed. Synthesized by
    /// the analyzer from the heartbeats ([`ScopeClosers::with_silences`]); no producer emits it,
    /// so [`Kind::parse`] never reads it.
    Silence,
    /// A block admitted to a cache: the only kind that OPENS a residency. Sorted after the
    /// closes at the same instant — see [`Event::close_first_rank`].
    Store,
}

impl Kind {
    /// The kind's wire spelling — the inverse of [`Kind::parse`], for emitters and reports.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Store => "store",
            Self::Evict => "evict",
            Self::Clear => "clear",
            Self::HolderReset => "holder_reset",
            Self::Silence => "silence",
        }
    }

    /// Whether this kind CLOSES a residency.
    ///
    /// Named rather than open-coded because the interval builder's group numbering and its
    /// same-instant tie-break both key on it, and two spellings of "not a store" is how those two
    /// come to disagree.
    #[must_use]
    pub fn closes(self) -> bool {
        !matches!(self, Self::Store)
    }

    /// `pub` so the loaders can decode a kind. They live in the store-facing crate; this is the
    /// one place that knows the wire spelling.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "store" => Some(Self::Store),
            "evict" => Some(Self::Evict),
            "clear" => Some(Self::Clear),
            "holder_reset" => Some(Self::HolderReset),
            _ => None,
        }
    }
}

/// One cache fact, as the wire carried it.
///
/// **The scope dimensions are `Option`, and that is load-bearing rather than tidy.** On a cache
/// record an absent `dp_rank`/`group_idx` means the producer declared no such scope, and every
/// consumer reads it as the single-worker zero. On a `clear` the same absence means the clear is
/// GLOBAL over that dimension — it closes every holder, not holder zero. One type, two readings;
/// coalescing at load time would bake in the wrong one for whichever consumer lost.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Event {
    /// Which cache fact this is: a store, an evict, a clear, or a holder reset.
    pub kind: Kind,
    /// The stable instance label (`instance_id` on the wire) naming the engine this fact is
    /// about — the first dimension of every holder scope.
    pub instance: String,
    /// Absent means "declared no such scope". Not coalesced here — see the struct note.
    pub dp_rank: Option<u32>,
    /// The KV cache group within the worker (hashed independently). Absent reads as `dp_rank`'s
    /// absence does — see the struct note.
    pub group_idx: Option<u32>,
    /// The engine's own id. Cache records without one are outside the residency substrate;
    /// `clear` records never carry one.
    pub block_id: Option<String>,
    /// The fleet-stable id, where the tap could derive one.
    pub content_id: Option<String>,
    /// The epoch scoping the identity space (spec 3.3), stamped from the segment header. In
    /// every join key, so nothing crosses a rotation; `None` where the header declared none.
    pub key_epoch: Option<u32>,
    /// `None` is a real tier — the wire declared no medium — never a wildcard.
    pub tier: Option<String>,
    /// Optional backend identity within the publisher and tier.
    pub backend_id: Option<String>,
    /// **Engine domain**, milliseconds — the engine's own clock (a `holder_reset` carries its
    /// `boundary_ms` here). Compared with a lifecycle stamp only under `delta_skew_ms`.
    pub at_ms: f64,
    /// `LOCAL`/`REMOTE` relative to the publisher. Carried raw; the own-copy rule is the
    /// substrate's, not the IR's.
    pub locality: Option<String>,
    /// The block this one claims as its prefix parent. **Always an ENGINE-space id**, even where
    /// the child carries a content id — so an edge is resolved through the store record that
    /// named it rather than compared directly, and the two identity spaces never meet.
    pub parent_id: Option<String>,
    /// The tap could not determine the parent. **Not the same as an absent `parent_id`**, which
    /// asserts a root: an empty ancestry claim is vacuously proven and counts as contributing,
    /// while an undeterminable one proves nothing. Collapsing them manufactures contribution out
    /// of ignorance (spec 2.3).
    pub parent_unknown: bool,
    /// The block's token count as the store record carried it — the identity's token mass.
    /// Absent coalesces to zero: a close record states none.
    pub n_tokens: i64,
    /// **Which delivery this event arrived in.** Today a batch's content hash; under a columnar
    /// wire it is the sealed segment's identity. Carried because taint is scoped to the delivery
    /// that was torn, not to the instance that sent it — an instance-wide extent would over-taint
    /// every window a single bad segment touched.
    pub delivery_id: Option<String>,
    /// Which producer RUN emitted this fact. Carried because spec 5.1's scope expansion joins a
    /// run-scoped incident to the instances that run covered, and the surviving cache records are
    /// the fallback half of that mapping when a run's lifecycle segments were lost.
    pub incarnation: String,
    /// The transport sequence of the MESSAGE this event arrived in (spec 2.3), shared by every
    /// event decoded from it. **The finest resolution a narrowing can ever reach**: the stream
    /// cannot say where inside one message's bracket a loss fell.
    pub seq: Option<u64>,
    /// **Present only under a `labelled` declaration, and absent is NOT false** (spec 2.3). A
    /// producer that cannot tell a reuse announcement from a fresh insertion emits nothing here;
    /// defaulting it to "freshly inserted" would answer that question on every record of every
    /// unlabelled stream — the inference spec 5.9 forbids.
    pub reused: bool,
    /// What the producer declared its traffic is for, carried from the segment header the
    /// record arrived under (spec 2.7). `undeclared` where the producer declared nothing.
    pub workload_class: Option<String>,
}

impl Event {
    /// An event of `kind` from `instance` at `at_ms`, with every optional field absent, no
    /// tokens and no incarnation: the start a decoder or a test fills in field by field.
    /// `Event` is `#[non_exhaustive]`, so a field added later is not a breaking change for
    /// callers that build events this way.
    #[must_use]
    pub fn new(kind: Kind, instance: impl Into<String>, at_ms: f64) -> Self {
        Self {
            kind,
            instance: instance.into(),
            dp_rank: None,
            group_idx: None,
            block_id: None,
            content_id: None,
            key_epoch: None,
            tier: None,
            backend_id: None,
            at_ms,
            locality: None,
            parent_id: None,
            parent_unknown: false,
            n_tokens: 0,
            delivery_id: None,
            incarnation: String::new(),
            seq: None,
            reused: false,
            workload_class: None,
        }
    }
}

impl Event {
    /// The identity this event is joined on: `content_id` where the tap derived one, `block_id`
    /// otherwise, and `None` where the record carries no engine id at all.
    ///
    /// **The `block_id` condition is not redundant.** The SQL this replaces reads
    /// `coalesce(content_id, assumeNotNull(block_id))` *under* a `block_id IS NOT NULL` filter, so
    /// a record carrying only a content id is outside the substrate entirely. Preferring content
    /// without that condition would silently admit it.
    #[must_use]
    pub fn identity(&self) -> Option<&str> {
        let engine_id = self.block_id.as_deref()?;
        Some(self.content_id.as_deref().unwrap_or(engine_id))
    }

    /// The holder scope as the residency substrate reads it: absent dimensions are the
    /// single-worker zero. Never meaningful on a `clear` — see the struct note.
    #[must_use]
    pub fn holder_dims(&self) -> (u32, u32) {
        (self.dp_rank.unwrap_or(0), self.group_idx.unwrap_or(0))
    }

    /// The same-instant tie-break: **closes sort before stores.**
    ///
    /// A store at a clear's own instant survives it and opens the next interval — the
    /// conservative reading for post-clear detection, and the one the SQL encodes as
    /// `ORDER BY t, if(kind = 'store', 1, 0)`.
    #[must_use]
    pub fn close_first_rank(&self) -> u8 {
        u8::from(!self.kind.closes())
    }
}

/// The identity an interval is built under (join-semantics).
///
/// `tier` is in the key and deliberately absent from `Holder`: a block offloaded to CPU is
/// announced and released on each tier separately, so each tier's store must pair with that
/// tier's own evict — but a tier is a location *within* a cache, not a cache.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntervalKey {
    /// The holder's first dimension: the instance label, as [`Event::instance`].
    pub instance: String,
    /// The holder's data-parallel rank; an undeclared one is already the single-worker zero.
    pub dp_rank: u32,
    /// The holder's KV cache group; an undeclared one is already the single-worker zero.
    pub group_idx: u32,
    /// The join identity — [`Event::identity`]: the content id where the tap derived one, else
    /// the engine's block id.
    pub identity: String,
    /// The identity space's epoch (spec 3.3): two epochs of one hash are two keys.
    pub key_epoch: Option<u32>,
    /// The storage tier the residency is on. `None` is the declared-nothing tier, never a
    /// wildcard — it pairs only with events that also named none.
    pub tier: Option<String>,
    /// Optional backend identity within the publisher and tier.
    pub backend_id: Option<String>,
}

impl IntervalKey {
    /// The cache the key names: its first three dimensions, without the tier.
    #[must_use]
    pub fn holder(&self) -> crate::intervals::Holder {
        crate::intervals::Holder {
            instance: self.instance.clone(),
            dp_rank: self.dp_rank,
            group_idx: self.group_idx,
        }
    }
}

/// The total order the IR is delivered in.
///
/// `at_ms` first because every derivation is time-ordered, then enough of the identity to make the
/// order TOTAL — one batch stamps one timestamp on every event it carries, so `at_ms` alone leaves
/// ties an engine would break differently between runs.
pub(crate) fn ordering(a: &Event, b: &Event) -> std::cmp::Ordering {
    a.at_ms
        .total_cmp(&b.at_ms)
        .then_with(|| a.close_first_rank().cmp(&b.close_first_rank()))
        .then_with(|| a.instance.cmp(&b.instance))
        .then_with(|| a.dp_rank.cmp(&b.dp_rank))
        .then_with(|| a.group_idx.cmp(&b.group_idx))
        .then_with(|| a.kind.cmp(&b.kind))
        .then_with(|| a.block_id.cmp(&b.block_id))
        .then_with(|| a.content_id.cmp(&b.content_id))
        .then_with(|| a.key_epoch.cmp(&b.key_epoch))
        .then_with(|| a.tier.cmp(&b.tier))
        .then_with(|| a.backend_id.cmp(&b.backend_id))
}

/// Put an event set into the IR's total order.
pub fn sort(events: &mut [Event]) {
    events.sort_by(ordering);
}

/// Group the window's events into the per-key, time-ordered streams the residency fold consumes.
///
/// **This is where the clear-scope expansion happens, and it is the one piece of semantics in this
/// module** — because it is a property of the *grouping*, not of the fold: a `clear` is not an
/// event about one key, it is an event about every key its declared scope covers. The SQL
/// expresses it as a `UNION ALL` against `SELECT DISTINCT` over the cache keys; here it is a
/// second pass over the key set.
///
/// Scope is read **as declared**: a `NULL` dimension on the clear record means the wire named no
/// value, so the clear is global over it and matches every holder — never coalesced onto holder
/// zero, which would leave every other holder's residency open past a clear that emptied it.
/// `tier` is the same rule one dimension over: an absent tier clears every tier, a named one
/// clears only its own.
///
/// Own-copy filtering is applied here for the same reason it is in the SQL's two CTEs: the
/// substrate counts only facts about the publisher's own cache, and a remote block admitted as
/// per-holder residency would report duplication that does not exist.
#[must_use]
pub fn group_by_interval_key(events: &[Event]) -> Vec<(IntervalKey, Vec<Event>)> {
    let mut grouped: BTreeMap<IntervalKey, Vec<Event>> = BTreeMap::new();

    for ev in events.iter().filter(|e| own_copy(e)) {
        if ev.kind.closes() && ev.identity().is_none() {
            continue; // a clear names no block; the fold merges it in via [`ScopeClosers`]
        }
        let Some(identity) = ev.identity() else {
            continue;
        };
        let (dp_rank, group_idx) = ev.holder_dims();
        grouped
            .entry(IntervalKey {
                instance: ev.instance.clone(),
                dp_rank,
                group_idx,
                identity: identity.to_string(),
                key_epoch: ev.key_epoch,
                tier: ev.tier.clone(),
                backend_id: ev.backend_id.clone(),
            })
            .or_default()
            .push(ev.clone());
    }

    let mut out: Vec<(IntervalKey, Vec<Event>)> = grouped.into_iter().collect();
    for (_, evs) in &mut out {
        sort(evs);
    }
    out
}

/// The window's scope-closers — every own-copy `clear` and `holder_reset` — held ONCE
/// and viewed per key.
///
/// The expansion this replaces CLONED each closer into every covered group's stream: a
/// clear on a wide instance multiplied itself by the instance's whole key set, and a
/// streamed grouper cannot inherit that shape at all — a clear carries no identity, so
/// it can never ride an identity-ordered stream. Held once here, synthesized per key at
/// fold time, merged by the IR's one total order. The rules are the expansion's own,
/// moved whole: scope read AS DECLARED (`None` is global over its dimension, never
/// holder zero), `holder_reset` global over `group_idx` and `tier` regardless of what
/// the record carries, and a scope-level close can never mint a key — only the fold of
/// a key cache records established ever sees one.
#[derive(Debug, Default)]
pub struct ScopeClosers {
    /// Own-copy `clear`/`holder_reset` events, in [`sort`] order.
    closers: Vec<Event>,
}

impl ScopeClosers {
    /// The window's closers, drawn from its event set: every own-copy `clear` and
    /// `holder_reset`, in [`sort`] order. Every other event is left where it was.
    #[must_use]
    pub fn of(events: &[Event]) -> Self {
        let mut closers: Vec<Event> = events
            .iter()
            .filter(|e| matches!(e.kind, Kind::Clear | Kind::HolderReset) && own_copy(e))
            .cloned()
            .collect();
        sort(&mut closers);
        Self { closers }
    }

    /// The closers covering `key`, synthesized onto it — dims forced to the key's, the
    /// key's identity as the block, the view the fold reads. Sorted by the IR's total order: after synthesis the only ordering
    /// field two same-instant closers can differ in is `kind`, so the re-sort is what
    /// makes per-key order independent of the closers' declared-scope fields.
    #[must_use]
    pub fn covering(&self, key: &IntervalKey) -> Vec<Event> {
        let mut synthesized: Vec<Event> = self
            .closers
            .iter()
            .filter(|c| scope_covers(c, key))
            .map(|c| {
                let mut synthetic = c.clone();
                synthetic.dp_rank = Some(key.dp_rank);
                synthetic.group_idx = Some(key.group_idx);
                synthetic.tier.clone_from(&key.tier);
                synthetic.backend_id.clone_from(&key.backend_id);
                synthetic.key_epoch = key.key_epoch;
                synthetic.block_id = Some(key.identity.clone());
                synthetic.content_id = None;
                synthetic
            })
            .collect();
        sort(&mut synthesized);
        synthesized
    }

    /// Whether any closer's scope covers `key`: [`Self::covering`] without the synthesis.
    #[must_use]
    pub fn covers(&self, key: &IntervalKey) -> bool {
        self.closers.iter().any(|c| scope_covers(c, key))
    }

    /// These closers and a [`Kind::Silence`] for each `(instance, at)` in `silences`: where an
    /// instance's heartbeats stopped past its liveness bound.
    #[must_use]
    pub fn with_silences(mut self, silences: impl IntoIterator<Item = (String, f64)>) -> Self {
        let before = self.closers.len();
        self.closers.extend(
            silences
                .into_iter()
                .map(|(instance, at)| Event::new(Kind::Silence, instance, at)),
        );
        if self.closers.len() > before {
            sort(&mut self.closers);
        }
        self
    }

    /// Whether the window holds no scope-level close at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.closers.is_empty()
    }
}

/// One identity's slice of the window: every interval-key group whose identity (and epoch)
/// this is — the unit the grouped-stream pass folds.
///
/// **Why this is the grouping that bounds classification**: the classifier's one cross-key
/// read is `by_block`, keyed `(identity, key_epoch)` — so an identity group is
/// self-contained: every interval a store's classification can consult lives inside it.
/// Measured on real vLLM traffic, the axis is small everywhere (p50 4 events, max 136) and
/// its FOLD state is smaller still — one open interval per holder sub-key, by the re-admit
/// rule.
#[derive(Debug)]
pub struct IdentityGroup {
    /// The join identity every key in the group shares — [`IntervalKey::identity`].
    pub identity: String,
    /// The epoch every key in the group shares; a rotation splits an identity into two groups.
    pub key_epoch: Option<u32>,
    /// The group's interval-key streams, closer-expanded and time-ordered exactly as
    /// [`group_by_interval_key`] produces them — this is a PARTITION of that output, never
    /// a second grouping rule.
    pub keys: Vec<(IntervalKey, Vec<Event>)>,
}

/// Partition [`group_by_interval_key`]'s output by `(identity, key_epoch)` — the identity axis
/// repeat compute folds over.
#[must_use]
pub fn group_by_identity(events: &[Event]) -> Vec<IdentityGroup> {
    /// One identity's keyed streams, accumulating under the (identity, epoch) axis.
    type KeyedStreams = Vec<(IntervalKey, Vec<Event>)>;
    let mut groups: BTreeMap<(String, Option<u32>), KeyedStreams> = BTreeMap::new();
    for (key, evs) in group_by_interval_key(events) {
        groups
            .entry((key.identity.clone(), key.key_epoch))
            .or_default()
            .push((key, evs));
    }
    groups
        .into_iter()
        .map(|((identity, key_epoch), keys)| IdentityGroup {
            identity,
            key_epoch,
            keys,
        })
        .collect()
}

/// Whether a scope-level close covers this key. A `None` dimension on a clear is global over
/// that dimension; a named one must match exactly. A `holder_reset` is a PROCESS boundary,
/// so it is global over `group_idx` and `tier` regardless of what the record carries — a
/// process restart takes every group and every tier it hosted (spec 2.3).
fn scope_covers(closer: &Event, key: &IntervalKey) -> bool {
    if closer.instance != key.instance {
        return false;
    }
    let dp = closer.dp_rank.is_none_or(|d| d == key.dp_rank);
    match closer.kind {
        Kind::HolderReset => dp,
        // The instance went unobserved: every cache on it.
        Kind::Silence => true,
        _ => {
            dp && closer.group_idx.is_none_or(|g| g == key.group_idx)
                && (closer.tier.is_none() || closer.tier == key.tier)
        }
    }
}

/// The residency substrate counts only facts about the publisher's OWN cache.
///
/// Absent counts as own — that is the entire observed world for deployments without an offload
/// tier, and reading absence as "not mine" would empty the substrate outright. An *unrecognized*
/// value does not: "declared nothing" and "declared something this build cannot read" are
/// different, and only the first is safe to read as local. The asymmetry picks the honest
/// direction — excluding a fact under-reports duplication, admitting one over-reports it.
fn own_copy(ev: &Event) -> bool {
    is_own_copy(ev.locality.as_deref())
}

/// The own-copy rule on a declared `locality`: absent or `LOCAL`. Ingest spells it again as
/// `analyzer_segments::is_own_copy` for the holder basis it counts; a pipeline test holds
/// the two equal.
#[must_use]
pub fn is_own_copy(locality: Option<&str>) -> bool {
    matches!(locality, None | Some("LOCAL"))
}

#[cfg(test)]
mod tests {
    use super::{group_by_interval_key, Event, Kind, ScopeClosers};

    fn ev(kind: Kind, block: Option<&str>, content: Option<&str>, at_ms: f64) -> Event {
        Event {
            kind,
            instance: "i1".to_string(),
            dp_rank: None,
            group_idx: None,
            block_id: block.map(str::to_string),
            content_id: content.map(str::to_string),
            key_epoch: None,
            tier: None,
            backend_id: None,
            at_ms,
            locality: None,
            n_tokens: 0,
            parent_id: None,
            parent_unknown: false,
            delivery_id: None,
            incarnation: String::new(),
            seq: None,
            reused: false,
            workload_class: None,
        }
    }

    #[test]
    fn identity_prefers_content_and_requires_an_engine_id_beside_it() {
        // The SQL this replaces reads `coalesce(content_id, assumeNotNull(block_id))` UNDER a
        // `block_id IS NOT NULL` filter, so a record carrying only a content id is outside the
        // substrate entirely. Preferring content without that condition would silently admit it.
        assert_eq!(
            ev(Kind::Store, Some("b"), Some("c"), 0.0).identity(),
            Some("c")
        );
        assert_eq!(ev(Kind::Store, Some("b"), None, 0.0).identity(), Some("b"));
        assert_eq!(ev(Kind::Store, None, Some("c"), 0.0).identity(), None);
        assert_eq!(ev(Kind::Store, None, None, 0.0).identity(), None);
    }

    #[test]
    fn only_a_store_leaves_a_residency_open() {
        assert!(!Kind::Store.closes());
        assert!(Kind::Evict.closes());
        assert!(Kind::Clear.closes());
    }

    #[test]
    fn a_close_sorts_before_a_store_at_the_same_instant() {
        // A store at a clear's own instant survives it and opens the next interval — the
        // conservative reading for post-clear detection, and what the SQL's
        // `ORDER BY t, if(kind = 'store', 1, 0)` encodes.
        assert!(
            ev(Kind::Clear, None, None, 5.0).close_first_rank()
                < ev(Kind::Store, Some("b"), None, 5.0).close_first_rank()
        );
    }

    #[test]
    fn a_scopeless_clear_closes_every_holder_never_holder_zero() {
        // The confident-wrong direction this closes: reading an absent `dp_rank` as zero leaves
        // every other holder's residency open past a clear that emptied it — overstated
        // durations. Asserted at the FOLD's outcome: the closers are held once and
        // merged per key, so "reaches the group" stopped being an observable and "closes the
        // residency" is the claim that was always meant.
        let mut store_w1 = ev(Kind::Store, Some("p"), None, 10.0);
        store_w1.dp_rank = Some(1);
        let mut store_w2 = ev(Kind::Store, Some("p"), None, 11.0);
        store_w2.dp_rank = Some(2);
        let clear = ev(Kind::Clear, None, None, 20.0); // no dp_rank: the wire named no scope

        let intervals =
            crate::intervals::residency_intervals_from_events(&[store_w1, store_w2, clear]);
        assert_eq!(intervals.len(), 2, "two holders, two residencies");
        for iv in &intervals {
            assert_eq!(
                (iv.close_ms, iv.close_reason),
                (Some(20.0), Some(crate::intervals::CloseReason::Clear)),
                "the scopeless clear must close holder {}",
                iv.key.dp_rank
            );
        }
    }

    #[test]
    fn a_tier_scoped_clear_leaves_an_unnamed_tier_alone() {
        // `c.r IS NULL OR c.r = k.r` in the SQL: a clear naming GPU leaves a residency whose
        // tier the wire never named, exactly as the producer's own bridge releases it.
        let mut store = ev(Kind::Store, Some("p"), None, 10.0);
        store.tier = None;
        let mut clear = ev(Kind::Clear, None, None, 20.0);
        clear.tier = Some("GPU".to_string());

        let intervals = crate::intervals::residency_intervals_from_events(&[store, clear]);
        assert_eq!(intervals.len(), 1);
        assert_eq!(
            intervals[0].close_ms, None,
            "a GPU-scoped clear must not close a residency on the unnamed tier"
        );
    }

    #[test]
    fn a_remote_block_never_enters_the_substrate() {
        // Admitting a remote copy as per-holder residency reports duplication that does not
        // exist. Both layers hold the rule: the grouping (which owns own-copy) and the fold's
        // outcome.
        let mut remote = ev(Kind::Store, Some("p"), None, 10.0);
        remote.locality = Some("REMOTE".to_string());
        assert!(group_by_interval_key(std::slice::from_ref(&remote)).is_empty());
        assert!(crate::intervals::residency_intervals_from_events(&[remote]).is_empty());

        // ...and an unrecognized value is not read as local either: "declared nothing" and
        // "declared something this build cannot read" are different.
        let mut unknown = ev(Kind::Store, Some("p"), None, 10.0);
        unknown.locality = Some("SOMETHING_NEW".to_string());
        assert!(group_by_interval_key(std::slice::from_ref(&unknown)).is_empty());
        assert!(crate::intervals::residency_intervals_from_events(&[unknown]).is_empty());
    }

    #[test]
    fn a_clear_cannot_mint_a_key_only_close_one() {
        // A clear with no cache record to attach to produces nothing: a closer is only ever
        // SYNTHESIZED onto a key that cache records established.
        assert!(group_by_interval_key(&[ev(Kind::Clear, None, None, 20.0)]).is_empty());
        assert!(crate::intervals::residency_intervals_from_events(&[ev(
            Kind::Clear,
            None,
            None,
            20.0
        )])
        .is_empty());
    }

    #[test]
    fn covering_synthesizes_the_keys_own_dims_onto_the_closer() {
        // The synthesis rules: a covering closer arrives at the fold wearing the KEY's dims, the
        // key's identity as its block, and no content id. A holder_reset is a PROCESS
        // boundary: global over group_idx and tier regardless of what the record carries.
        let mut store = ev(Kind::Store, Some("p"), None, 10.0);
        store.dp_rank = Some(3);
        store.group_idx = Some(7);
        store.tier = Some("GPU".to_string());
        let clear = ev(Kind::Clear, None, None, 20.0);
        let mut reset = ev(Kind::HolderReset, None, None, 21.0);
        reset.dp_rank = Some(3);
        reset.group_idx = Some(999); // a reset's own group is IGNORED: the process took every group
        reset.tier = Some("CPU".to_string()); // and every tier

        let keys = group_by_interval_key(std::slice::from_ref(&store));
        assert_eq!(keys.len(), 1);
        let closers = ScopeClosers::of(&[store, clear, reset]);
        let synthesized = closers.covering(&keys[0].0);
        assert_eq!(synthesized.len(), 2, "the clear and the reset both cover");
        for c in &synthesized {
            assert_eq!(c.dp_rank, Some(3));
            assert_eq!(c.group_idx, Some(7));
            assert_eq!(c.tier.as_deref(), Some("GPU"));
            assert_eq!(c.block_id.as_deref(), Some("p"));
            assert_eq!(c.content_id, None);
        }
    }
}

// ===========================================================================================
// THE LIFECYCLE SLICE — a second event family, and a second loader.
//
// Cache facts carry the ENGINE's clock; lifecycle facts carry the tap process's wall clock. They
// are two clocks on one host, compared only under `delta_skew_ms` and never joined in a
// query — which is exactly why they are two types here rather than one with a nullable half. A
// single `Event` covering both would make a cross-domain comparison a field access away.
// ===========================================================================================

/// One per-endpoint sub-stat from a heartbeat's `endpoints` array.
///
/// **`dropped` is per SOURCE and cumulative**; `oversized` is agent-level and lives on the record
/// rather than here, so a span's oversized delta is replicated onto every source — the
/// conservative direction, and the reading the `ARRAY JOIN` this replaces produced by construction.
#[derive(Debug, Clone, PartialEq)]
pub struct Endpoint {
    /// The instance this endpoint feeds (`endpoints[].source`): the run's declared coverage,
    /// which spec 5.1 expands a run-scoped loss over.
    pub source: String,
    /// Messages dropped from this source, cumulative over the run; absent reads as zero. Its
    /// rise between two heartbeats makes that span a drop window.
    pub dropped: i64,
    /// The wall-domain stamp of the last message this source delivered; `None` where the
    /// heartbeat carried none.
    pub last_msg_at_ms: Option<f64>,
    /// The channel name the transport supplied, relayed by the producer verbatim and
    /// unparsed (spec 2.4). Recognizing a naming convention is this reader's business —
    /// see the pipeline's `topic` module.
    pub topic: Option<String>,
}

/// The lifecycle families: liveness, delivery accounting, and the declarations that gate what may
/// be reported at all.
#[derive(Debug, Clone, PartialEq)]
pub struct LifecycleEvent {
    /// `agent_start` | `heartbeat` | `agent_stop`, and the delivery families beside them.
    /// **A string rather than a closed enum, deliberately**, mirroring the reader's own rule that
    /// an unknown kind lands whole and counted rather than being fatal: this family grows with the
    /// spec, and a reader that refused an unrecognised lifecycle kind would fail on a stream a
    /// later producer version is entitled to emit.
    pub kind: String,
    /// **Wall domain.** Never compared against a cache event's `at_ms` except through the
    /// `delta_skew_ms` allowance.
    pub at_ms: f64,
    /// The instance label the record is scoped to; `""` on an agent-scoped record (storage's
    /// sentinel), whose instances ride `endpoints` instead.
    pub instance: String,
    /// The producer RUN this record belongs to — the unit a declaration, a sequence, and a
    /// staleness verdict are all scoped to. `""` where the segment had no header.
    pub incarnation: String,
    /// Agent-level cumulative counters. `None` where the record does not carry them.
    pub dropped: Option<i64>,
    /// Messages the producer dropped as oversized, agent-level and cumulative like `dropped`;
    /// its rise is replicated onto every source's span — the conservative direction.
    pub oversized: Option<i64>,
    /// The declared liveness bounds (spec 5.8) — the ENTIRE basis on which a reader may call a
    /// producer stale.
    pub heartbeat_secs: Option<u64>,
    /// The second declared liveness bound (spec 5.8): the longest a segment stays open before
    /// it seals. The staleness line is the larger of the two, widened by the platform grace.
    pub max_segment_secs: Option<u64>,
    /// Which key material this run holds. Exactly one: a rotation is a declared discontinuity.
    pub canary: Option<String>,
    /// What a `store` MEANS on this producer's stream (spec 2.3). Carried because it gates
    /// whether a figure may be presented AT ALL rather than decorating one that may: under
    /// `unlabelled` an unqualified count of stores is not an insertion count.
    pub reuse_reporting: Option<String>,
    /// The heartbeat's per-source sub-stats — the run's observed coverage, one entry per
    /// instance it fed. Empty on every kind that carries none.
    pub endpoints: Vec<Endpoint>,
    /// Delivery identity: this segment's place in its incarnation's sequence. A hole in the
    /// observed sequence is spec 5.4's structural loss.
    pub segment_seq: Option<u64>,
    /// The range a `segments_dropped` declaration reclaimed — the loss the producer NAMES.
    pub first_seq: Option<u64>,
    /// The reclaimed range's last segment sequence, inclusive, beside `first_seq`.
    pub last_seq: Option<u64>,
    /// Spec 2.4's declared capacity loss and the ENGINE-clock window it covers. The only cause with a
    /// mapped extent, because the declaration is already in the engine domain — the record's own
    /// producer-clock `at_ms` never enters.
    pub refused: Option<u64>,
    /// The refusal window's start, ENGINE-clock milliseconds — the declaration's own extent.
    pub window_start_ms: Option<f64>,
    /// The refusal window's end, ENGINE-clock milliseconds; with the start, the mapped extent.
    pub window_end_ms: Option<f64>,
    /// The holder a refusal is scoped to. Absent dimensions read as the single-worker zero here,
    /// matching the query's `coalesce`.
    pub dp_rank: Option<u32>,
    /// The refused holder's KV cache group, beside `dp_rank`; absent reads as zero here too.
    pub group_idx: Option<u32>,
    /// `segment_recovered` (spec 2.5): whether the recovered segment's own identity was
    /// readable. `None` on every other kind — never defaulted, because the two cases carry
    /// opposite continuity claims (spec 4.3).
    pub attributed: Option<bool>,
    /// `segment_recovered`: trailing bytes the recovery discarded as incomplete. The producer's
    /// own in-band truncation declaration (spec 5.5) — the loss no sequence hole prices,
    /// because an attributed recovery participates in continuity normally.
    pub dropped_bytes: Option<i64>,
    /// The producer's own version string, from `agent_start` (snapshot provenance). The wire
    /// carries it as an unrecognised field, so it rides the shaped row's `extra` map — a
    /// dependency on the tap's exact spelling, pinned by the corpus-driven test against the
    /// tap's lifecycle fixture so a tap-side rename breaks a test instead of silently emptying
    /// a provenance field.
    pub agent_version: Option<String>,
    /// The model the deployment DECLARED, where a `--provenance` map carried a `model` key —
    /// stated once on the segment header, riding the `extra` map like `agent_version`. The
    /// declared half of the model dimension's two sources; a recognized topic outranks it, and
    /// absence stays absent — never bridged, never defaulted.
    pub declared_model: Option<String>,
    /// As `declared_model`, for a `pod_identity` key: the chart-relayed pod name (C4's
    /// decision — identity stays the stable instance label, the pod rides beside it).
    pub declared_pod: Option<String>,
}

// ===============================================================================================
// THE SOURCE SEAM
// ===============================================================================================
/// Where the Ordered Event IR comes from — the seam a partner implements.
///
/// **Three methods, because scope is a property of each input and there are three scopes.** Cache
/// facts carry the engine's clock and are window-bounded by it. Lifecycle facts carry the tap
/// process's wall clock and are clipped by a wall margin. `segment_open` records are scoped to the
/// retention horizon, because a declared drop's bracket may sit arbitrarily far from the window. A
/// fourth method would mean a derivation reached past the boundary rather than through it.
///
/// **This trait is the whole store-agnostic claim, stated as a type.** Until a SECOND
/// implementation exists the claim is asserted rather than proven — one implementation makes any
/// interface look sufficient, because it cannot disagree with anything. The proof is two sources
/// over one corpus.
///
/// **Ordering is the implementation's obligation, and it is already discharged for free**: the
/// events must arrive as [`sort`] leaves them. That function is this crate's property rather than
/// any query's, so a source reading sealed segments does not have to reproduce a store's
/// `ORDER BY` — it sorts with the same code the engine-backed source does.
///
/// **What an implementation must NOT do is filter.** No semantic rule belongs here — not the
/// own-copy rule, not `block_id IS NOT NULL`, not the window's half-open edge. Those belong to
/// the derivations, which legitimately disagree about them. A source that pre-filtered would pick
/// one derivation's reading and silently starve another's.
pub trait EventSource {
    /// The window's cache facts, ordered.
    ///
    /// The bounded streaming read the keyed reader uses is `pipeline::source::FillSource`,
    /// an extension of this trait; this crate needs only the whole-window load.
    ///
    /// **Shared, not owned, and that is a memory bound rather than a style**: the cache stream is the one load that scales with RETENTION,
    /// and returning it owned meant every memoized read held two complete copies of the
    /// retained span — the map's and the caller's. Behind `Arc`, a memo hit is a pointer
    /// bump and a miss holds ONE copy. A consumer that genuinely needs ownership clones
    /// explicitly, so the cost is visible at the site that pays it.
    ///
    /// # Errors
    /// Transport failures, or bytes that do not decode as the declared shape.
    fn cache_events(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<std::sync::Arc<Vec<Event>>, crate::Error>;

    /// The lifecycle families over an already-clipped WALL range.
    ///
    /// The caller supplies the clip because the margin differs by consumer — banked evidence and
    /// a live snapshot do not want the same one — and neither margin is this seam's to choose.
    ///
    /// # Errors
    /// Transport failures, or a row that does not parse as declared.
    fn lifecycle_events(
        &self,
        fleet_id: &str,
        lo_ms: f64,
        hi_ms: f64,
    ) -> Result<Vec<LifecycleEvent>, crate::Error>;

    /// Every `segment_open` still held, ordered by `(incarnation, segment_seq)`.
    ///
    /// **"Retention horizon", not "unbounded".** Records older than retention do not exist, so
    /// "everything held" IS the true bound. Sparse by construction — one record per SEGMENT, not
    /// per event — which is what makes the wide scope affordable where the cache stream must stay
    /// clipped for volume.
    ///
    /// # Errors
    /// Transport failures, or a row that does not parse as declared.
    fn segment_opens_at_retention_horizon(
        &self,
        fleet_id: &str,
    ) -> Result<Vec<LifecycleEvent>, crate::Error>;

    /// The lifecycle slice a run-constant DECLARATION is read from (spec 2.3): the liveness
    /// kinds at the retention horizon — the same scope family as the opens, because a
    /// declaration is a property of a producer run, not of a window.
    ///
    /// Defaulted to the plain lifecycle load over an unbounded range (which every source clamps
    /// to its own retention), so a second implementation carries no extra obligation. A source
    /// that can read the declaration columns without decoding the rest of the horizon — the
    /// production source projects them alone — overrides this as a cost lever, never a filter:
    /// the returned events still carry no semantic rule.
    ///
    /// # Errors
    /// Transport failures, or a row that does not parse as declared.
    fn lifecycle_declarations(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<Vec<LifecycleEvent>, crate::Error> {
        // Clipped to the caller's span plus the context margin, never the whole horizon:
        // the roster's currency argument is about FRESHNESS, not width. It must be as fresh
        // as the derivation, and its own span plus margin is exactly that.
        self.lifecycle_events(
            fleet_id,
            w.start_ms - crate::completeness::EVIDENCE_CLIP_MARGIN_MS,
            w.end_ms + crate::completeness::EVIDENCE_CLIP_MARGIN_MS,
        )
    }
}
