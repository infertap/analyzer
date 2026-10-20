// SPDX-License-Identifier: AGPL-3.0-only

//! Residency intervals (join-semantics) — the substrate, as a windowed query.
//!
//! Obligation 1, in full: a `store` opens an interval (a store over an open interval is
//! a **re-admit** — the interval stays open, `readmit_count` increments); an `evict`
//! closes it; an evict with no open interval is a **left-open interval** `(−∞, t)` —
//! legitimate, never an error; a `clear` terminates every open interval of its instance
//! **within its declared scope** at that timestamp (the design's correctness
//! requirement) — and a clear record carrying no `dp_rank` or `group_idx` declared no
//! such scope, so it closes across ALL holders of that instance, never just holder zero
//! (the producer emits exactly that shape when the wire named no scope, and reading its
//! absence as zero would leave every other holder's residency open past a clear that
//! emptied it — overstated durations, the confident-wrong direction); intervals still
//! open at window end are right-open, durations censored there.
//!
//! Identity is `(instance, dp_rank, group_idx, key_epoch, p, tier)`. The epoch is in the key
//! so joins never cross a rotation — implicitly or otherwise: there is NO cross-epoch mapping,
//! because a rotation is a declared discontinuity (spec 3.3).
//! `dp_rank` and `group_idx` are in it for the same reason a
//! different instance is: they name a physically different cache. Two data-parallel workers
//! holding one prefix are two blocks, and a key without them reads that as one block
//! re-admitted — reporting duplication as zero rather than as a number.
//!
//! **`tier` is in the interval key and deliberately NOT in [`Holder`].** A block offloaded to
//! CPU is announced and released on each tier separately, so each tier's store must pair with
//! that tier's own evict; collapsing them let a GPU evict close an interval a CPU copy was
//! still holding, and the next GPU store then read as a recompute when it was a promotion off
//! the local CPU tier — counting a device-to-host copy as a recomputed prefill, which
//! overstates waste on
//! the number a customer checks first. But a tier is a *location within* a cache, not a cache:
//! one block on GPU and CPU in one worker is still ONE holder, so admitting tier to `Holder`
//! would invent duplication just as surely. In the key, out of the holder.
//!
//! A NULL tier is a real tier here, matching the producer's own bridge ("`None` is a real tier:
//! the wire declared no `medium`, consistently, and those stores and removes must pair up like
//! any other's"). Every deployment without offloading is entirely NULL or entirely `GPU`, so it
//! has exactly one tier and this key is the previous one.
//!
//! Mechanics of the query: per `(instance, dp_rank, group_idx, block, epoch)` the events are ordered by
//! `(t, closes-first)`; each event's group number is the count of closes strictly before
//! it, so interval `k` is exactly "the stores with `k` closes before them, closed by the
//! `(k+1)`-th close if it exists". Same-timestamp ties resolve closes-before-stores: a
//! store at a clear's own instant survives it (opens the next interval) — the
//! conservative reading for post-clear detection. A clear with no open interval is a
//! no-op (only an EVICT with no open interval makes a left-open interval, per the spec).

use crate::events::IntervalKey;

/// Why an interval closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// An `evict` of the block: closes the open interval, or makes a left-open one where none
    /// was open — positive evidence of residency until that instant.
    Evict,
    /// A `clear` of the instance within its declared scope: every open interval there closes
    /// at that instant; with nothing open it is a no-op.
    Clear,
    /// The holder's engine process restarted (spec 2.3 `holder_reset`). Not a `Clear`:
    /// the engine announced nothing, and the boundary also moves the engine-id space —
    /// a `block_id` MUST NOT be matched across it.
    Reset,
    /// The holder went unobserved: its producer's heartbeats stopped past the liveness bound.
    /// Not evidence the block left; evidence nothing more is known of it.
    Silent,
}

impl CloseReason {
    /// The storage spelling — what the last-close index banks.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Evict => "evict",
            Self::Clear => "clear",
            Self::Reset => "reset",
            Self::Silent => "silent",
        }
    }

    /// The inverse of [`CloseReason::as_str`], for reading a banked reason back. `None`
    /// on an unknown token — refused, never guessed.
    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "evict" => Some(Self::Evict),
            "clear" => Some(Self::Clear),
            "reset" => Some(Self::Reset),
            "silent" => Some(Self::Silent),
            _ => None,
        }
    }
}

/// One residency interval for `(instance, dp_rank, group_idx, block, key_epoch, tier)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    /// The identity the residency was built under: the holder, the block, its epoch and its
    /// tier. A join never crosses one.
    pub key: IntervalKey,
    /// `None` = left-open `(−∞, close)`: predates observation; excluded from duration
    /// statistics, included in existence statistics.
    pub open_ms: Option<f64>,
    /// `None` = right-open: still open at window end; duration censored there.
    pub close_ms: Option<f64>,
    /// Why it closed; `None` exactly when `close_ms` is — still open at window end.
    pub close_reason: Option<CloseReason>,
    /// Stores over this interval while open, beyond the opening one (class 0 of the classification feeds
    /// from this count).
    pub readmits: u64,
    /// What the producer that ADMITTED this block said its traffic was for — the class the
    /// OPENING store carried. `None` = the producer declared nothing, which every emitter
    /// renders `undeclared`; it is a bucket, never a null (spec 2.7: "absent means the
    /// producer said nothing, not that it serves no workload").
    ///
    /// **Taken from the opener, and this is a decision rather than a convenience.** The class is
    /// declared once per `segment_open` and stamped onto every record behind it, so an interval
    /// spanning a seal can hold events that disagree — and it need not be exotic to get there:
    /// `IntervalKey` carries no incarnation, and a restart without a key rotation preserves the
    /// identity space (spec 3.3). So an operator relabelling a tap and restarting it
    /// leaves blocks admitted under the old class and evicted under the new one.
    ///
    /// The residency is attributed to what ADMITTED it, by the rule every declaration follows:
    /// "an event emitted in March carries March's class because that is what the tap was
    /// configured with in March."
    /// The store and the evict are both true about their own emission; the store is what opened
    /// this. It is also the rule a classified store has always followed, so intervals are not
    /// the one exception.
    ///
    /// The rejected alternative was attributing a straddling interval to NEITHER class. It reads
    /// as the conservative choice and is not one: coverage measures observation completeness in
    /// time, not attribution, so the interval would leave both class rows with nothing recording
    /// that it had — a silent understatement, which is the half of I3 that says the reader must
    /// be told why.
    pub workload_class: Option<String>,
}

/// The cache a block sits in. Two data-parallel workers on one instance are two caches, so
/// this — not `instance` — is what "held by N holders" must count.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Holder {
    /// The instance the cache belongs to — the producer's `instance_id`.
    pub instance: String,
    /// Data-parallel rank: an independent cache on the same instance.
    pub dp_rank: u32,
    /// KV cache group: hashed independently within a worker.
    pub group_idx: u32,
}

impl Interval {
    /// The cache this interval sits in: its holder dimensions with the key's `tier` dropped,
    /// since a tier is a location within a cache.
    #[must_use]
    pub fn holder(&self) -> Holder {
        self.key.holder()
    }
}

/// One interval group under construction: the stores carrying `k` closes before them, terminated
/// by the `(k+1)`-th close if one exists.
#[derive(Default)]
struct Group {
    store_count: u64,
    t_open: Option<f64>,
    close: Option<(f64, CloseReason)>,
    /// What the OPENING store declared. See [`Interval::workload_class`].
    declared: Option<String>,
}

impl Group {
    fn is_empty(&self) -> bool {
        self.store_count == 0 && self.close.is_none()
    }
}

/// The banked residency state one interval key resumes from — the slice seeding.
/// A seed is the SAME state a whole-window fold
/// would have reached at the slice boundary, so folding a slice from it is folding the
/// window in two sittings.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IntervalSeed {
    /// An open residency at the boundary: its open instant, the stores it had absorbed
    /// beyond the opener, and the opener's declaration. `None` = not open.
    pub open: Option<FoldSeed>,
    /// The most recent close at the boundary, for the prior-close scan. Becomes a
    /// left-open closed interval: duration-excluded (its open is not restated), close
    /// visible — exactly what a whole-window fold yields for a close whose open predates
    /// the read.
    pub last_close: Option<(f64, CloseReason)>,
}

/// The open half of an [`IntervalSeed`].
#[derive(Debug, Clone, PartialEq)]
pub struct FoldSeed {
    /// `None` = left-open: the residency predates even the seeding derivation's read.
    pub open_since_ms: Option<f64>,
    /// Stores absorbed beyond the opener as of the boundary; a slice store counts on from here.
    pub readmits: u64,
    /// The opener's declaration, carried so a slice store onto the seed never re-owns the interval.
    pub workload_class: Option<String>,
}

/// Build residency intervals for one key by folding its time-ordered events.
///
/// **The same mechanics the SQL states, in the form the SQL states them.** Per key the events are
/// ordered by `(t, closes-first)`; each event's group number is the count of closes strictly
/// before it, so interval `k` is exactly "the stores with `k` closes before them, closed by the
/// `(k+1)`-th close if it exists". Because the order is fixed and a group is terminated by its own
/// close, that numbering falls out of a single pass — a close ends the group it belongs to, and
/// the next event opens the next one.
///
/// Two rules ride the fold rather than a filter afterwards, because both are about what a group
/// MEANS:
///
/// - **A clear with no open interval is a no-op**, not a left-open interval. Only an EVICT with no
///   store before it makes `(−∞, t)`, per the spec's own asymmetry — a clear that emptied a cache
///   holding nothing observed says nothing about residency, while an evict is positive evidence
///   that something was resident until that instant. This is the SQL's
///   `HAVING NOT (store_count = 0 AND close_kind = 'clear')`.
/// - **Same-instant stores beyond the first are re-admits**, counted into `readmits`, because the
///   opening store is the first at that instant and the classifier must agree with this builder
///   about which one it was.
fn fold_key<'a>(
    key: &crate::events::IntervalKey,
    evs: impl Iterator<Item = &'a crate::events::Event>,
) -> Vec<Interval> {
    fold_key_seeded(key, evs, None)
}

fn fold_key_seeded<'a>(
    key: &crate::events::IntervalKey,
    evs: impl Iterator<Item = &'a crate::events::Event>,
    seed: Option<&FoldSeed>,
) -> Vec<Interval> {
    let mut groups: Vec<Group> = Vec::new();
    let mut cur = Group::default();
    if let Some(seed) = seed {
        // Resume the open residency: `store_count` reproduces `readmits + the opener`, so a
        // slice store increments from where the whole-window count stood, and an untouched
        // seed round-trips as a right-open interval with the same figures.
        cur.t_open = seed.open_since_ms;
        cur.store_count = seed.readmits + 1;
        cur.declared.clone_from(&seed.workload_class);
    }

    for ev in evs {
        if ev.kind.closes() {
            let reason = match ev.kind {
                crate::events::Kind::Evict => CloseReason::Evict,
                crate::events::Kind::HolderReset => CloseReason::Reset,
                crate::events::Kind::Silence => CloseReason::Silent,
                // `Store` cannot reach here: `closes()` is false for it.
                _ => CloseReason::Clear,
            };
            // The close terminates the group it sits in — every later event has one more close
            // before it and therefore belongs to the next group.
            cur.close = Some((ev.at_ms, reason));
            groups.push(std::mem::take(&mut cur));
        } else {
            cur.store_count += 1;
            // Occupancy, not `t_open`, decides the opener: unseeded the two are equivalent
            // (a group's first store sets both), and a SEEDED group is occupied from the
            // boundary — a slice store onto it is a re-admit, never a new opener.
            if cur.store_count == 1 {
                cur.t_open = Some(ev.at_ms);
                // The opener's declaration, taken here and never revisited: later stores over
                // this interval are re-admits and do not own it. Same rule, same instant, as
                // `classify.rs`'s `opener_class`.
                cur.declared.clone_from(&ev.workload_class);
            }
        }
    }
    if !cur.is_empty() {
        groups.push(cur); // stores still open at window end: right-open, censored there
    }

    groups
        .into_iter()
        .filter(|g| {
            // A scope-level close with no open interval is a no-op, for a clear and a reset
            // alike: the spec has the reader close what it is TRACKING, and here it tracked
            // nothing. An evict stays — only an evict testifies to a specific block, so a
            // store-less evict is a left-open interval rather than silence.
            !(g.store_count == 0
                && matches!(
                    g.close,
                    Some((
                        _,
                        CloseReason::Clear | CloseReason::Reset | CloseReason::Silent
                    ))
                ))
        })
        .map(|g| Interval {
            key: key.clone(),
            open_ms: g.t_open,
            close_ms: g.close.map(|(t, _)| t),
            close_reason: g.close.map(|(_, r)| r),
            readmits: g.store_count.saturating_sub(1),
            workload_class: g.declared.clone(),
        })
        .collect()
}

/// Residency intervals folded from the Ordered Event IR.
///
/// It consumes the key-grouped stream [`crate::events::group_by_interval_key`] produces: one
/// identity's events in order, which is the shape the fold needs.
///
/// One ordering note, not semantic: nullable key
/// columns sort NULLS LAST in the engine and `None`-first in Rust, so callers that care compare as
/// multisets. Nothing downstream does — every consumer groups these into maps before reading them.
#[must_use]
pub fn residency_intervals_from_events(events: &[crate::events::Event]) -> Vec<Interval> {
    let closers = crate::events::ScopeClosers::of(events);
    residency_intervals_for_keys(&crate::events::group_by_interval_key(events), &closers)
}

/// The per-group slice of [`residency_intervals_from_events`]: fold an already-grouped
/// set of key streams — one identity group's, in the grouped-stream pass — through the
/// same per-key fold the flat entry uses, each key's stream MERGED with the closers
/// covering it (S2: the closers are held once and synthesized per key, never cloned
/// into the groups). One fold, two drivers; a second fold body would be a drift pair
/// inside this crate.
#[must_use]
pub(crate) fn residency_intervals_for_keys(
    keys: &[(crate::events::IntervalKey, Vec<crate::events::Event>)],
    closers: &crate::events::ScopeClosers,
) -> Vec<Interval> {
    keys.iter()
        .flat_map(|(key, evs)| {
            let synthesized = closers.covering(key);
            fold_key(key, merged(evs, &synthesized))
        })
        .collect()
}

/// `residency_intervals_for_keys` resumed from banked state — the slice fold.
///
/// **The union matters**: a seeded key with no slice events must still produce its
/// intervals — an untouched open residency round-trips (the next frontier must not lose
/// it), and another holder's open residency is what RE consults even when that holder was
/// silent this slice. Each seed's `last_close` is appended as a left-open closed interval,
/// which is byte-for-byte what a whole-window fold yields for a close whose open predates
/// the read.
#[must_use]
pub fn residency_intervals_for_keys_seeded(
    keys: &[(crate::events::IntervalKey, Vec<crate::events::Event>)],
    closers: &crate::events::ScopeClosers,
    seeds: &std::collections::BTreeMap<crate::events::IntervalKey, IntervalSeed>,
) -> Vec<Interval> {
    let with_events: std::collections::BTreeSet<&crate::events::IntervalKey> =
        keys.iter().map(|(key, _)| key).collect();
    let mut out: Vec<Interval> = keys
        .iter()
        .flat_map(|(key, evs)| {
            let synthesized = closers.covering(key);
            fold_key_seeded(
                key,
                merged(evs, &synthesized),
                seeds.get(key).and_then(|s| s.open.as_ref()),
            )
        })
        .collect();
    for (key, seed) in seeds {
        if !with_events.contains(key) {
            if let Some(open) = &seed.open {
                // Over the key's SCOPE CLOSERS, never over nothing: a clear or reset in
                // the slice closes a seeded residency whose key had no events of its own —
                // the dormant-identity hole a whole-window fold never has, because there
                // the opener event is in the stream and the key exists. Found by C's
                // design review; pinned by `a_clear_after_the_seam_closes_a_dormant_seed`.
                let synthesized = closers.covering(key);
                out.extend(fold_key_seeded(key, synthesized.iter(), Some(open)));
            }
        }
        if let Some((close, reason)) = seed.last_close {
            out.push(Interval {
                key: key.clone(),
                open_ms: None,
                close_ms: Some(close),
                close_reason: Some(reason),
                readmits: 0,
                workload_class: None,
            });
        }
    }
    out
}

/// Two-pointer merge of a key's own stream with its synthesized closers, under the IR's
/// one total order — the sequence the expansion-era `sort` produced, without the clones.
/// Equivalence is exact rather than approximate: the two sides cannot tie under the
/// total order, because a key's own stream holds only identity-bearing events and a
/// closer never carries one — see [`crate::events::ScopeClosers`].
fn merged<'a>(
    own: &'a [crate::events::Event],
    closers: &'a [crate::events::Event],
) -> impl Iterator<Item = &'a crate::events::Event> {
    let mut a = own.iter().peekable();
    let mut b = closers.iter().peekable();
    std::iter::from_fn(move || match (a.peek(), b.peek()) {
        (Some(x), Some(y)) => {
            if crate::events::ordering(x, y).is_le() {
                a.next()
            } else {
                b.next()
            }
        }
        (Some(_), None) => a.next(),
        (None, _) => b.next(),
    })
}
