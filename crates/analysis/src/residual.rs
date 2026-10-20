// SPDX-License-Identifier: AGPL-3.0-only

//! Duplication as residency arithmetic: per-holder span lists of one identity, and from them
//! the co-residency token-time per holder pair, the copies histogram and its minute cells, and
//! the copy bands and locality words the served duplication tables carry
//! (`docs/classification-semantics.md` §8). Every measure here is an integral of tokens held over
//! time, so it sums across spans, days, pairs and identities.
//!
//! `δ_skew` discipline: each residency interval is shrunk by `δ_skew / 2` at each
//! edge before overlap tests, so co-residency claims survive only when robust to the
//! allowed clock misalignment — skew weakens positives, never manufactures them.

use crate::intervals::Holder;
use std::collections::BTreeMap;

/// Sorted union of spans: overlapping and touching spans coalesce.
fn merge_spans(spans: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut sorted = spans.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut out: Vec<(f64, f64)> = Vec::new();
    for (s, e) in sorted {
        match out.last_mut() {
            Some((_, ce)) if s <= *ce => *ce = ce.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

/// One identity's residency on one holder under one workload class, as δ-shrunk merged
/// spans — the row grain the scratch table holds, and the reassembly unit for
/// every overlap test. `class` is the interval's OWN class (the opener's, mapped
/// through the `undeclared` bucket), so a class-scoped report selects rows by
/// equality and the unscoped report reads them all.
///
/// **Spans are merged per `(class, holder)` BY CONSTRUCTION** ([`HolderSpans::merged`]), so
/// one holder holding a block twice is one span list rather than two holders. A value that
/// skipped the merge would over-count concurrent holders in [`replication_histogram`].
#[derive(Debug, Clone, PartialEq)]
pub struct HolderSpans {
    /// The interval's OWN workload class — the opener's; `undeclared` where none was declared.
    pub class: String,
    /// The cache holding the block: `(instance, dp_rank, group_idx)`, tier excluded ([`Holder`]).
    pub holder: Holder,
    /// `(open, close)` in engine-domain ms, δ-shrunk and merged per holder; a censored edge
    /// reads `−∞` or `∞`.
    pub spans: Vec<(f64, f64)>,
}

impl HolderSpans {
    /// The one construction path for a caller outside this module: the spans go through
    /// `merge_spans`, so the merged-by-construction guarantee above is enforced rather than
    /// trusted.
    #[must_use]
    pub fn merged(class: String, holder: Holder, spans: &[(f64, f64)]) -> Self {
        Self {
            class,
            holder,
            spans: merge_spans(spans),
        }
    }
}

/// One (class, holder-pair) cell of co-residency token-time: the integral, over a
/// window, of one identity's tokens held on BOTH holders at once.
///
/// **This measure SUMS** — across spans, days, pairs and identities — which is the
/// whole reason it exists beside the order-statistic mass and also why it must never
/// be added to it: the wire carries `never_summed_with` for exactly that boundary.
/// `token_ms` is milliseconds × tokens; the glass renders token-hours.
#[derive(Debug, Clone, PartialEq)]
pub struct TokenTimeCell {
    /// The substrate's own bucket word, verbatim: `undeclared` where the producer
    /// declared no workload class (spec 2.7 — a declaration absence, not a mapping
    /// miss). the wire carries this word as-is; nothing renames
    /// testimony between here and the glass.
    pub class: String,
    /// Canonical: `a < b` under `Holder`'s derived order, so one pair is one key
    /// whichever order the span rows arrive in.
    pub a: Holder,
    /// The pair's other holder — the greater under that same order, so `a < b` always.
    pub b: Holder,
    /// Token-time on both holders at once, clipped to the window: milliseconds × tokens, summed
    /// across the pair's span overlaps.
    pub token_ms: f64,
}

/// Fold ONE identity's span rows into its co-residency token-time cells.
///
/// Pairs are taken within a class only — the same equality scoping the class-scoped
/// report applies to its spans. Co-residency ACROSS classes is real and is deliberately
/// not here: it is the cross-workload cut, a different question served separately, and
/// folding it into per-class cells would double-attribute one overlap to two rows.
///
/// `window` clips every span, which is also what makes censored edges finite: a span
/// open at −∞ or ∞ contributes exactly its observed intersection with the window and
/// claims nothing beyond it.
///
/// **Cost is O(span pairs), and it is maximised exactly where the register's value
/// is maximised**: a universally shared prefix held by k holders costs k(k−1)/2 pair
/// tests per fold. That is the declared price of the headline case, not a tail; the
/// accumulator this feeds rides its own byte budget for the same reason.
#[must_use]
pub fn token_time_cells(
    n_tokens: i64,
    spans: &[HolderSpans],
    window: (f64, f64),
) -> Vec<TokenTimeCell> {
    // An identity with no tokens states nothing: a duration without mass is not a
    // figure, and emitting a zero cell would render as a measured zero.
    if n_tokens <= 0 {
        return Vec::new();
    }
    let (win_lo, win_hi) = window;
    let mut cells: BTreeMap<(String, Holder, Holder), f64> = BTreeMap::new();
    for (i, x) in spans.iter().enumerate() {
        for y in &spans[i + 1..] {
            if x.class != y.class || x.holder == y.holder {
                continue;
            }
            let mut overlap_ms = 0.0;
            for &(a_open, a_close) in &x.spans {
                for &(b_open, b_close) in &y.spans {
                    let lo = a_open.max(b_open).max(win_lo);
                    let hi = a_close.min(b_close).min(win_hi);
                    if hi > lo {
                        overlap_ms += hi - lo;
                    }
                }
            }
            if overlap_ms > 0.0 {
                let (a, b) = if x.holder <= y.holder {
                    (x.holder.clone(), y.holder.clone())
                } else {
                    (y.holder.clone(), x.holder.clone())
                };
                #[allow(clippy::cast_precision_loss)] // block token counts are small integers
                let mass = n_tokens as f64;
                *cells.entry((x.class.clone(), a, b)).or_insert(0.0) += overlap_ms * mass;
            }
        }
    }
    cells
        .into_iter()
        .map(|((class, a, b), token_ms)| TokenTimeCell {
            class,
            a,
            b,
            token_ms,
        })
        .collect()
}

/// One bucket of the replication histogram: token-time spent at a given concurrent
/// copy count, and across how many engines those copies were spread.
///
/// **Two dimensions, because one would erase the remedy split.** Three copies on three
/// engines is the gateway's routing affinity; three copies on two engines is the
/// balancer's rank placement. Bucketing on `holders` alone collapses those into one
/// number at exactly the axis change meant to make routing actionable.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationCell {
    /// The class the sweep counted holders within — the substrate's bucket word verbatim, or `""`
    /// for the fleet sweep over [`fleet_flattened`] spans.
    pub class: String,
    /// Concurrent holders — the replication factor itself. **`1` is a real bucket**, not
    /// an absence: see [`replication_histogram`].
    pub holders: u32,
    /// Distinct instances among those holders. Always `<= holders`; equal when every
    /// copy sits on a different engine.
    pub instances: u32,
    /// Distinct (instance, `dp_rank`) pairs among those holders — physical caches.
    /// Always `instances <= ranks <= holders`; `holders - ranks` is the number of copies
    /// that differ only by KV cache group, which is architecture rather than placement
    /// (see [`locality`]).
    pub ranks: u32,
    /// Token-time at exactly this `(holders, instances, ranks)`: milliseconds × tokens, summed
    /// over the window's segments.
    pub token_ms: f64,
}

/// Fold ONE identity's span rows into token-time by concurrent copy count.
///
/// **This is the distribution the pair matrix decomposes, and the axis an operator can
/// act on.** Routing policy controls how many copies of a prefix exist at once; pairs
/// are a lossy quadratic view of that same fact (k copies read as k(k−1)/2 cells), so a
/// reader has to invert a binomial to recover the number they actually wanted.
///
/// **`holders == 1` IS EMITTED, and omitting it would be a lie in the flattering
/// direction.** A mean replication factor has to divide by ALL cached token-time, not
/// only the duplicated part: a fleet that is 99 % unique with 1 % at two copies would
/// otherwise report "2.0x" — alarming for a near-perfect fleet, and understating a bad
/// one from the other side. With the singleton bucket present, the mean means "the copy
/// count experienced by an average token-millisecond of cached content", which is a
/// sentence someone can act on.
///
/// **Output is sparse and needs no cap.** It is O(classes × distinct observed (k,
/// instances) pairs) rather than the pair fold's O(identities × k²), so a 300-instance
/// fleet emits a few hundred rows a day in total. The max-simultaneous-holder order
/// statistic falls out as the highest non-empty `holders` bucket, so the two cannot
/// disagree.
///
/// **PER-CLASS CELLS DO NOT SUM TO A FLEET FIGURE, and adding them lies in the
/// flattering direction.** The sweep counts holders within one class, so an identity held
/// under `agentic` on one holder and under `batch` on another at the same instant is
/// physically TWO copies but reads as two separate singletons. Summing the classes would
/// report a genuinely duplicated fleet at an average replication factor of 1.0× — the
/// worst available combination: wrong, reassuring, and on the headline number.
///
/// The fleet figure is a SEPARATE CALL, not a rollup: sweep the same spans with `class`
/// flattened to one scope, exactly as `residual_by_workload` banks a fleet row at
/// `scope = ""` beside its per-class rows. The per-class output is a cut for attribution;
/// it is never the denominator of anything.
#[must_use]
pub fn replication_histogram(
    n_tokens: i64,
    spans: &[HolderSpans],
    window: (f64, f64),
) -> Vec<ReplicationCell> {
    // A duration without mass is not a figure — same rule the pair fold applies.
    if n_tokens <= 0 {
        return Vec::new();
    }
    #[allow(clippy::cast_precision_loss)] // block token counts are small integers
    let mass = n_tokens as f64;
    let mut cells: BTreeMap<(String, u32, u32, u32), f64> = BTreeMap::new();
    sweep_replication(spans, window, &mut |s: Stretch<'_>| {
        *cells
            .entry((s.class.to_string(), s.holders, s.instances, s.ranks))
            .or_insert(0.0) += (s.hi - s.lo) * mass;
    });
    cells
        .into_iter()
        .map(
            |((class, holders, instances, ranks), token_ms)| ReplicationCell {
                class,
                holders,
                instances,
                ranks,
                token_ms,
            },
        )
        .collect()
}

/// One maximal stretch of a sweep during which the concurrent holder count is constant and
/// positive, clipped to the window.
struct Stretch<'a> {
    /// The class the holders were counted within.
    class: &'a str,
    /// Concurrent holders.
    holders: u32,
    /// Distinct instances among them.
    instances: u32,
    /// Distinct (instance, rank) pairs among them.
    ranks: u32,
    /// The one instance every holder shares, where there is one.
    sole_instance: Option<&'a str>,
    /// The stretch's start, engine-domain milliseconds.
    lo: f64,
    /// The stretch's end, exclusive.
    hi: f64,
}

/// The replication sweep's CORE: walk one identity's span rows and emit every maximal
/// [`Stretch`] during which the concurrent holder count is constant and positive, clipped to
/// `window`.
///
/// One derivation, two drivers — [`replication_histogram`] integrates segments into
/// per-`(k, instances)` cells, and [`replication_minute_cells`] splits the same segments
/// at minute boundaries for the streamed producer's serving grain. A second sweep body
/// would be a drift pair on the figure the console headlines.
fn sweep_replication(
    spans: &[HolderSpans],
    window: (f64, f64),
    emit: &mut impl FnMut(Stretch<'_>),
) {
    let (win_lo, win_hi) = window;

    // Grouped by class: a class-scoped read selects rows by equality, exactly as the
    // pair fold and the flat report do.
    let mut by_class: BTreeMap<&str, Vec<(&Holder, f64, f64)>> = BTreeMap::new();
    for row in spans {
        for &(open, close) in &row.spans {
            let lo = open.max(win_lo);
            let hi = close.min(win_hi);
            if hi > lo {
                by_class
                    .entry(row.class.as_str())
                    .or_default()
                    .push((&row.holder, lo, hi));
            }
        }
    }

    for (class, intervals) in by_class {
        // A sweep line over the endpoints: +1 where a holder opens, −1 where it closes.
        // Linear in spans, where the pair fold is quadratic in holders — the same walk
        // answering the question the reader actually asked.
        let mut events: Vec<(f64, i32, &Holder)> = Vec::with_capacity(intervals.len() * 2);
        for (holder, lo, hi) in intervals {
            events.push((lo, 1, holder));
            events.push((hi, -1, holder));
        }
        // Closes before opens at one instant: a span that ends exactly where another
        // begins is a handoff, not an overlap — the same half-open convention the pair
        // fold applies when it refuses touching intervals.
        events.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Held per INSTANCE and per RANK, so `instances` is one map's length, `ranks` is
        // the other's, and `holders` is the sum: two ranks of one engine are two holders
        // on one instance, and two cache GROUPS of one rank are two holders on one rank.
        // Three counts because the three relationships have three different causes — see
        // [`locality`].
        let mut held: BTreeMap<&str, u32> = BTreeMap::new();
        let mut held_rank: BTreeMap<(&str, u32), u32> = BTreeMap::new();
        let mut holders: u32 = 0;
        let mut cursor = f64::NEG_INFINITY;
        for (at, delta, holder) in events {
            if holders > 0 && at > cursor && cursor.is_finite() {
                #[allow(clippy::cast_possible_truncation)]
                // instance and rank counts fit u32 by construction
                let (instances, ranks) = (held.len() as u32, held_rank.len() as u32);
                // The node this duplication happened INSIDE, where there is one. Copies
                // spanning engines belong to no single instance — attributing them to each
                // would double the mass on any sum across instances — so `None` is the
                // honest answer there and the fleet figure keeps that half.
                let sole = (instances == 1)
                    .then(|| held.keys().next().copied())
                    .flatten();
                emit(Stretch {
                    class,
                    holders,
                    instances,
                    ranks,
                    sole_instance: sole,
                    lo: cursor,
                    hi: at,
                });
            }
            let rank_key = (holder.instance.as_str(), holder.dp_rank);
            if delta > 0 {
                *held.entry(holder.instance.as_str()).or_insert(0) += 1;
                *held_rank.entry(rank_key).or_insert(0) += 1;
                holders += 1;
            } else {
                if let std::collections::btree_map::Entry::Occupied(mut slot) =
                    held.entry(holder.instance.as_str())
                {
                    *slot.get_mut() -= 1;
                    if *slot.get() == 0 {
                        slot.remove();
                    }
                }
                if let std::collections::btree_map::Entry::Occupied(mut slot) =
                    held_rank.entry(rank_key)
                {
                    *slot.get_mut() -= 1;
                    if *slot.get() == 0 {
                        slot.remove();
                    }
                }
                holders = holders.saturating_sub(1);
            }
            cursor = at;
        }
    }
}

/// A minute cell's key: [`MinuteCell`] without its measure.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MinuteKey {
    minute_ms: i64,
    class: String,
    holders: u32,
    instances: u32,
    ranks: u32,
    sole_instance: Option<String>,
}

/// One minute-bucket cell of the replication sweep — the streamed producer's serving
/// grain. `minute_ms` is the bucket's UTC start; a segment crossing a minute
/// boundary is split there, so a bucket's `token_ms` never carries a neighbour's time
/// and summing buckets over a day reproduces [`replication_histogram`] exactly (the
/// unit pin below holds the two to it).
#[derive(Debug, Clone, PartialEq)]
pub struct MinuteCell {
    /// The bucket's UTC start: engine-domain ms on a whole-minute boundary.
    pub minute_ms: i64,
    /// The class the sweep counted holders within — see [`ReplicationCell::class`].
    pub class: String,
    /// Concurrent holders — see [`ReplicationCell::holders`]; `1` is a real bucket here too.
    pub holders: u32,
    /// Distinct instances among those holders — see [`ReplicationCell::instances`].
    pub instances: u32,
    /// Distinct (instance, `dp_rank`) pairs — see [`ReplicationCell::ranks`].
    pub ranks: u32,
    /// The node this duplication happened INSIDE, where every copy shares one instance.
    /// `None` where the copies span engines: that mass belongs to no single node, and
    /// attributing it to each participant would double it on any sum across instances.
    pub sole_instance: Option<String>,
    /// Token-time inside this minute at this copy count: milliseconds × tokens, the segment
    /// split at the bucket's edges.
    pub token_ms: f64,
}

/// One minute of milliseconds — the serving bucket width.
const MINUTE_MS: f64 = 60_000.0;

/// Fold ONE identity's span rows into minute-bucketed token-time by concurrent copy
/// count: [`replication_histogram`]'s segments, split at minute boundaries. Everything
/// the histogram documents holds here — band 1 emitted, per-class cells never summed to
/// a fleet figure (flatten with [`fleet_flattened`] and sweep again), sparse output.
///
/// # Panics
/// On a non-finite window edge: an infinite segment is infinitely many buckets, so a
/// caller holding a censored span must clip to its emission frontier first.
#[must_use]
pub fn replication_minute_cells(
    n_tokens: i64,
    spans: &[HolderSpans],
    window: (f64, f64),
) -> Vec<MinuteCell> {
    if n_tokens <= 0 {
        return Vec::new();
    }
    // The histogram tolerates an unbounded window (a censored span reads as unbounded
    // for overlap); a MINUTE grain cannot — an infinite segment is infinitely many
    // buckets, so a caller holding a censored edge must clip to its emission frontier
    // first. This is the producer's contract, stated where it breaks.
    assert!(
        window.0.is_finite() && window.1.is_finite(),
        "replication_minute_cells needs a finite window; clip censored spans first"
    );
    #[allow(clippy::cast_precision_loss)] // block token counts are small integers
    let mass = n_tokens as f64;
    let mut cells: BTreeMap<MinuteKey, f64> = BTreeMap::new();
    sweep_replication(spans, window, &mut |s: Stretch<'_>| {
        let mut cursor = s.lo;
        while cursor < s.hi {
            #[allow(clippy::cast_possible_truncation)] // in-window instants fit i64 ms
            let bucket = (cursor / MINUTE_MS).floor() as i64;
            #[allow(clippy::cast_precision_loss)] // minute indices are small integers
            let edge = ((bucket + 1) as f64) * MINUTE_MS;
            let end = edge.min(s.hi);
            let key = MinuteKey {
                minute_ms: bucket * 60_000,
                class: s.class.to_string(),
                holders: s.holders,
                instances: s.instances,
                ranks: s.ranks,
                sole_instance: s.sole_instance.map(str::to_string),
            };
            *cells.entry(key).or_insert(0.0) += (end - cursor) * mass;
            cursor = end;
        }
    });
    cells
        .into_iter()
        .map(|(k, token_ms)| MinuteCell {
            minute_ms: k.minute_ms,
            class: k.class,
            holders: k.holders,
            instances: k.instances,
            ranks: k.ranks,
            sole_instance: k.sole_instance,
            token_ms,
        })
        .collect()
}

/// The FLEET view of one identity's span rows: every class flattened to the unscoped
/// spelling (`""`), spans re-merged per holder so the merged-by-construction guarantee
/// [`HolderSpans`] documents still holds. The fleet replication figure is a SEPARATE
/// SWEEP over this — never a sum of the per-class cells, which counts a genuinely
/// duplicated identity as two singletons (see [`replication_histogram`]).
#[must_use]
pub fn fleet_flattened(spans: &[HolderSpans]) -> Vec<HolderSpans> {
    let mut per_holder: BTreeMap<&Holder, Vec<(f64, f64)>> = BTreeMap::new();
    for row in spans {
        per_holder
            .entry(&row.holder)
            .or_default()
            .extend(row.spans.iter().copied());
    }
    per_holder
        .into_iter()
        .map(|(holder, spans)| HolderSpans {
            class: String::new(),
            holder: holder.clone(),
            spans: merge_spans(&spans),
        })
        .collect()
}

/// Band a concurrent holder count. Total on every input: `0` cannot reach it from the
/// sweep (segments exist only while `holders > 0`), and maps to `"1"` defensively.
#[must_use]
pub fn copy_band(holders: u32) -> &'static str {
    match holders {
        0 | 1 => "1",
        2 => "2",
        3 => "3",
        4 => "4",
        5..=8 => "5-8",
        9..=16 => "9-16",
        _ => "17+",
    }
}

/// The locality split — THREE causes, because they have three different remedies.
///
/// - `cross` — copies spanning engines. The fleet gateway's half: routing policy.
/// - `within` — copies inside one engine, on different data-parallel ranks. The
///   deployment balancer's half, what vLLM's cache-aware DP routing would buy.
/// - `group` — copies on ONE rank, differing only by KV cache group. **This is not
///   waste and no policy can remove it.** A hybrid-attention model partitions its
///   layers into groups that cache independently (full attention beside sliding
///   window, or Mamba state), and the engine keys its cache by hash AND group, so one
///   prefix is legitimately resident in each. Counting it as `within` told the operator
///   their engine was placing redundantly when the model's architecture requires both.
///
/// - `none` — ONE copy. There is no second copy, so there is no relationship to
///   classify, and asserting one would be a small lie on a row every remedy query
///   filters out anyway (`copies_band != '1'`). The binary split called this `within`,
///   which claimed the engine had placed a duplicate that does not exist; unnoticed
///   because nothing reads it, wrong all the same.
///
/// **The label is the COARSEST relationship present, not a decomposition.** A set of
/// copies spanning two instances is `cross` even where some of those copies also
/// multiply across ranks or groups inside an instance — the same simplification the
/// binary split always made, now with one more level. The exact decomposition is
/// available to a caller that wants it, since the cell carries `holders`, `instances`
/// and `ranks`: `instances - 1` copies are the router's, `ranks - instances` are the
/// engine's placement, and `holders - ranks` are architectural.
#[must_use]
pub fn locality(holders: u32, instances: u32, ranks: u32) -> &'static str {
    if holders <= 1 {
        "none"
    } else if instances > 1 {
        "cross"
    } else if ranks > 1 {
        "within"
    } else {
        "group"
    }
}

/// What a window's stores establish about a cross-instance zero (spec 5.7): a positive
/// result carries its own evidence, and a zero does not. Counted over the window's days from
/// the stored-mass denominator, own copies only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ZeroBasis {
    /// Instances that stored into their own cache in the window.
    pub instances: u64,
    /// Their own stores.
    pub stores: u64,
    /// Of those, the stores carrying a portable identity (`content_id`, spec 3.1).
    pub comparable_stores: u64,
    /// Distinct key spaces among the runs that made those stores (spec 3.4). The runs that
    /// declared no canary count together as one unkeyed space: nothing relates their
    /// pseudonyms to a keyed run's.
    pub key_spaces: u64,
}

/// What a cross-instance figure of zero means, from [`ZeroBasis::meaning`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ZeroMeaning {
    /// Not zero: the matches are their own evidence.
    Positive,
    /// Fewer than two instances stored: cross-instance duplication could not have been
    /// observed at any value.
    SingleHolder,
    /// No store carried a portable identity, so no two instances' identities could match:
    /// the zero carries no information about duplication.
    NotComparable,
    /// The stores span more than one key space. Pseudonyms under different keys cannot match
    /// at any overlap, so the zero describes the provisioning, not the traffic. A rotation in
    /// flight lands here too, deliberately: a rotation is a declared discontinuity (spec 3.3).
    SplitKeySpace {
        /// The key spaces observed.
        spaces: u64,
    },
    /// Some stores carry only the engine's id: the zero covers the comparable share and is
    /// unverifiable over the rest.
    PartlyComparable {
        /// The comparable share of the own stores, in `(0, 1)`.
        comparable: f64,
    },
    /// Two or more instances, one key space, every store comparable: a measured absence.
    Measured,
}

impl ZeroMeaning {
    /// The wire's word for it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Positive => "positive",
            Self::SingleHolder => "single_holder",
            Self::NotComparable => "not_comparable",
            Self::SplitKeySpace { .. } => "split_key_space",
            Self::PartlyComparable { .. } => "partly_comparable",
            Self::Measured => "measured",
        }
    }
}

impl ZeroBasis {
    /// The meaning of a cross-instance figure of `cross_token_ms` over this basis. Each
    /// condition the spec names is checked in turn, and the first that fails names the zero.
    #[must_use]
    pub fn meaning(&self, cross_token_ms: f64) -> ZeroMeaning {
        if cross_token_ms > 0.0 {
            return ZeroMeaning::Positive;
        }
        if self.instances < 2 {
            return ZeroMeaning::SingleHolder;
        }
        if self.comparable_stores == 0 {
            return ZeroMeaning::NotComparable;
        }
        if self.key_spaces > 1 {
            return ZeroMeaning::SplitKeySpace {
                spaces: self.key_spaces,
            };
        }
        if self.comparable_stores < self.stores {
            #[allow(clippy::cast_precision_loss)] // store counts are far below 2^53
            let comparable = self.comparable_stores as f64 / self.stores as f64;
            return ZeroMeaning::PartlyComparable { comparable };
        }
        ZeroMeaning::Measured
    }
}

#[cfg(test)]
mod zero_basis_tests {
    use super::{ZeroBasis, ZeroMeaning};

    fn basis(instances: u64, stores: u64, comparable_stores: u64, key_spaces: u64) -> ZeroBasis {
        ZeroBasis {
            instances,
            stores,
            comparable_stores,
            key_spaces,
        }
    }

    #[test]
    fn each_condition_names_its_zero_in_the_spec_order() {
        let full = basis(2, 4, 4, 1);
        assert_eq!(full.meaning(0.0), ZeroMeaning::Measured);
        assert_eq!(full.meaning(1.0), ZeroMeaning::Positive);
        assert_eq!(basis(1, 4, 4, 1).meaning(0.0), ZeroMeaning::SingleHolder);
        assert_eq!(basis(2, 4, 0, 1).meaning(0.0), ZeroMeaning::NotComparable);
        assert_eq!(
            basis(2, 4, 4, 2).meaning(0.0),
            ZeroMeaning::SplitKeySpace { spaces: 2 }
        );
        assert_eq!(
            basis(2, 4, 1, 1).meaning(0.0),
            ZeroMeaning::PartlyComparable { comparable: 0.25 }
        );
    }

    #[test]
    fn a_positive_figure_needs_no_basis() {
        assert_eq!(ZeroBasis::default().meaning(5.0), ZeroMeaning::Positive);
        assert_eq!(ZeroBasis::default().meaning(0.0), ZeroMeaning::SingleHolder);
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)] // every asserted value is an exact product of exactly-representable inputs
mod token_time_tests {
    use super::{token_time_cells, HolderSpans, TokenTimeCell};
    use crate::intervals::Holder;

    const DAY: (f64, f64) = (0.0, 86_400_000.0);

    fn holder(instance: &str, dp_rank: u32) -> Holder {
        Holder {
            instance: instance.to_string(),
            dp_rank,
            group_idx: 0,
        }
    }

    fn row(class: &str, h: Holder, spans: &[(f64, f64)]) -> HolderSpans {
        HolderSpans {
            class: class.to_string(),
            holder: h,
            spans: spans.to_vec(),
        }
    }

    /// The universally shared prefix: k holders, one span, every pair pays. This is
    /// the HEADLINE case, not a tail — cost k(k−1)/2 is the declared price of the
    /// content the register exists to illuminate.
    #[test]
    fn shared_prefix_pays_every_pair_and_every_pair_is_one_cell() {
        let spans: Vec<HolderSpans> = (0..5)
            .map(|i| row("chat", holder(&format!("e{i}"), 0), &[(0.0, 1_000.0)]))
            .collect();
        let cells = token_time_cells(16, &spans, DAY);
        assert_eq!(cells.len(), 10, "C(5,2) pairs");
        for cell in &cells {
            assert_eq!(cell.token_ms, 16_000.0, "1000 ms × 16 tokens");
            assert!(cell.a < cell.b, "canonical order");
            assert_ne!(cell.a.instance, cell.b.instance, "all cross-engine here");
        }
    }

    /// Censored edges arrive as ±∞ after the δ-shrink; the window is what makes them
    /// finite, and nothing past it is claimed.
    #[test]
    fn censored_edges_contribute_their_window_intersection_and_nothing_more() {
        let spans = [
            row("chat", holder("a", 0), &[(f64::NEG_INFINITY, 500.0)]),
            row("chat", holder("b", 0), &[(200.0, f64::INFINITY)]),
        ];
        let cells = token_time_cells(10, &spans, (0.0, 1_000.0));
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].token_ms, 3_000.0, "[200,500) × 10 tokens");
    }

    /// Pairs are class-equality scoped. Cross-class co-residency is the cross-workload
    /// cut, served elsewhere — folding it here would double-attribute one overlap.
    #[test]
    fn classes_pair_by_equality_never_across() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 1_000.0)]),
            row("batch", holder("b", 0), &[(0.0, 1_000.0)]),
        ];
        assert!(token_time_cells(8, &spans, DAY).is_empty());
    }

    /// One pair is one key whichever order the rows arrive in.
    #[test]
    fn a_pair_has_one_key_whichever_order_the_rows_arrive() {
        let forward = [
            row("chat", holder("a", 0), &[(0.0, 100.0)]),
            row("chat", holder("b", 0), &[(0.0, 100.0)]),
        ];
        let reversed = [forward[1].clone(), forward[0].clone()];
        assert_eq!(
            token_time_cells(4, &forward, DAY),
            token_time_cells(4, &reversed, DAY)
        );
    }

    /// Disjoint spans sum; the gaps between them contribute nothing.
    #[test]
    fn disjoint_spans_sum_and_gaps_contribute_nothing() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 100.0), (200.0, 300.0)]),
            row("chat", holder("b", 0), &[(50.0, 250.0)]),
        ];
        let cells = token_time_cells(2, &spans, DAY);
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].token_ms, 200.0, "(50+50) ms × 2 tokens");
    }

    /// Two ranks on one engine are two holders; their cell is the intra-engine half
    /// and the caller splits it from cross-engine by instance equality.
    #[test]
    fn within_engine_pairs_carry_the_intra_half() {
        let spans = [
            row("chat", holder("e0", 0), &[(0.0, 500.0)]),
            row("chat", holder("e0", 1), &[(0.0, 500.0)]),
        ];
        let cells = token_time_cells(6, &spans, DAY);
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].a.instance, cells[0].b.instance);
        assert_eq!(cells[0].token_ms, 3_000.0);
    }

    /// A duration without mass is not a figure: no tokens, no cells — an emitted
    /// zero would render as a measured zero.
    #[test]
    fn an_identity_with_no_tokens_states_nothing() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 1_000.0)]),
            row("chat", holder("b", 0), &[(0.0, 1_000.0)]),
        ];
        assert!(token_time_cells(0, &spans, DAY).is_empty());
    }

    /// Empty input, window-disjoint spans, and shrunk-away overlaps all state nothing
    /// rather than zero.
    #[test]
    fn no_overlap_in_window_states_nothing() {
        assert!(token_time_cells(5, &[], DAY).is_empty());
        let outside = [
            row("chat", holder("a", 0), &[(-2_000.0, -1_000.0)]),
            row("chat", holder("b", 0), &[(-2_000.0, -1_000.0)]),
        ];
        assert!(token_time_cells(5, &outside, DAY).is_empty());
        let touching = [
            row("chat", holder("a", 0), &[(0.0, 100.0)]),
            row("chat", holder("b", 0), &[(100.0, 200.0)]),
        ];
        assert!(
            token_time_cells(5, &touching, DAY).is_empty(),
            "half-open adjacency is not co-residency"
        );
    }

    /// The singleton bucket is EMITTED, and the mean is meaningless without it.
    ///
    /// A fleet mostly holding unique content, with a little duplication, must not read as
    /// though everything were duplicated. This is the pitch-integrity case: omitting k=1
    /// would report 2.0x for a near-perfect fleet.
    #[test]
    fn a_single_holder_is_a_bucket_not_an_absence() {
        let spans = [row("chat", holder("a", 0), &[(0.0, 1_000.0)])];
        let cells = super::replication_histogram(10, &spans, DAY);
        assert_eq!(cells.len(), 1, "one bucket: {cells:?}");
        assert_eq!(cells[0].holders, 1);
        assert_eq!(cells[0].instances, 1);
        assert_eq!(cells[0].token_ms, 10_000.0);
    }

    /// Holders and instances are DIFFERENT dimensions, and collapsing them would erase
    /// the remedy split: three copies across three engines is the gateway's problem,
    /// three copies inside two engines is the balancer's.
    #[test]
    fn holders_and_instances_are_counted_separately() {
        let spans = [
            row("chat", holder("e0", 0), &[(0.0, 100.0)]),
            row("chat", holder("e0", 1), &[(0.0, 100.0)]),
            row("chat", holder("e1", 0), &[(0.0, 100.0)]),
        ];
        let cells = super::replication_histogram(1, &spans, DAY);
        assert_eq!(cells.len(), 1, "one instant-shape: {cells:?}");
        assert_eq!(cells[0].holders, 3, "three copies");
        assert_eq!(cells[0].instances, 2, "across two engines");
        assert_eq!(cells[0].ranks, 3, "on three distinct ranks");
    }

    /// **A hybrid-attention model must not read as engine placement**.
    ///
    /// vLLM partitions a hybrid model's layers into KV cache groups that cache
    /// independently, and keys its cache by hash AND group — so one prefix is
    /// legitimately resident in each group of one rank. Those copies are the
    /// architecture, not the balancer, and no routing or placement change removes them.
    /// Before this pin they counted as `within` and told the operator their engine was
    /// placing redundantly.
    #[test]
    fn copies_that_differ_only_by_cache_group_are_architecture_not_placement() {
        let one_rank_two_groups = |g: u32| Holder {
            instance: "e0".to_string(),
            dp_rank: 0,
            group_idx: g,
        };
        let spans = [
            row("chat", one_rank_two_groups(0), &[(0.0, 100.0)]),
            row("chat", one_rank_two_groups(1), &[(0.0, 100.0)]),
        ];
        let cells = super::replication_histogram(1, &spans, DAY);
        assert_eq!(cells.len(), 1, "one instant-shape: {cells:?}");
        assert_eq!(
            cells[0].holders, 2,
            "the identity is resident in both groups"
        );
        assert_eq!(cells[0].instances, 1);
        assert_eq!(
            cells[0].ranks, 1,
            "ONE physical cache, two groups within it"
        );
        assert_eq!(
            super::locality(cells[0].holders, cells[0].instances, cells[0].ranks),
            "group",
            "architecture, never the engine's placement"
        );

        // The contrast that gives the pin its teeth: the SAME two copies, moved to two
        // ranks, are placement — so the split turns on the rank and nothing else.
        let two_ranks = [
            row("chat", holder("e0", 0), &[(0.0, 100.0)]),
            row("chat", holder("e0", 1), &[(0.0, 100.0)]),
        ];
        let placed = super::replication_histogram(1, &two_ranks, DAY);
        assert_eq!(placed[0].ranks, 2);
        assert_eq!(
            super::locality(placed[0].holders, placed[0].instances, placed[0].ranks),
            "within"
        );
    }

    /// The sweep tracks k as spans open and close, so one identity contributes to
    /// SEVERAL buckets over its life.
    #[test]
    fn the_sweep_splits_time_across_the_counts_it_passed_through() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 300.0)]),
            row("chat", holder("b", 0), &[(100.0, 200.0)]),
        ];
        let cells = super::replication_histogram(1, &spans, DAY);
        let at = |k: u32| {
            cells
                .iter()
                .find(|c| c.holders == k)
                .map_or(0.0, |c| c.token_ms)
        };
        assert_eq!(at(1), 200.0, "[0,100) and [200,300) alone");
        assert_eq!(at(2), 100.0, "[100,200) together");
    }

    /// **The histogram and the order statistic cannot disagree**: the max concurrent
    /// holder count IS the highest non-empty bucket, so the existing residual max and
    /// this distribution are two readings of one sweep.
    #[test]
    fn the_highest_bucket_is_the_max_concurrent_holder_count() {
        let spans: Vec<HolderSpans> = (0..4)
            .map(|i| row("chat", holder(&format!("e{i}"), 0), &[(0.0, 50.0)]))
            .collect();
        let cells = super::replication_histogram(2, &spans, DAY);
        let peak = cells.iter().map(|c| c.holders).max().expect("a bucket");
        assert_eq!(peak, 4, "four concurrent holders: {cells:?}");
    }

    /// Total token-time is conserved: the histogram redistributes the identity's
    /// residency across buckets, it does not mint or lose any.
    #[test]
    fn the_histogram_conserves_total_residency_time() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 100.0)]),
            row("chat", holder("b", 0), &[(50.0, 150.0)]),
        ];
        let cells = super::replication_histogram(1, &spans, DAY);
        // Wall time under at least one holder is [0,150) = 150 ms.
        let covered: f64 = cells.iter().map(|c| c.token_ms).sum();
        assert_eq!(covered, 150.0, "no time invented or lost: {cells:?}");
    }

    /// **Per-class cells do not sum to the fleet figure, and this pins the gap.**
    ///
    /// Two holders of the SAME content at the same instant under DIFFERENT classes are
    /// physically two copies, but each per-class sweep sees a singleton. Summing the
    /// classes reports 1.0× for a fleet that is genuinely duplicating — wrong in the
    /// flattering direction, on the headline number. The fleet figure is a separate sweep
    /// over one flattened scope, and this test asserts the two readings DIFFER so nobody
    /// "simplifies" the second call away.
    #[test]
    fn per_class_cells_do_not_sum_to_the_fleet_figure() {
        let per_class = [
            row("agentic", holder("a", 0), &[(0.0, 100.0)]),
            row("batch", holder("b", 0), &[(0.0, 100.0)]),
        ];
        let split = super::replication_histogram(1, &per_class, DAY);
        assert!(
            split.iter().all(|c| c.holders == 1),
            "each class sees a singleton: {split:?}"
        );

        // The same spans swept as ONE scope — what the fleet row actually is.
        let flattened: Vec<HolderSpans> = per_class
            .iter()
            .map(|r| row("", r.holder.clone(), &r.spans))
            .collect();
        let fleet = super::replication_histogram(1, &flattened, DAY);
        assert_eq!(fleet.len(), 1, "one bucket: {fleet:?}");
        assert_eq!(
            fleet[0].holders, 2,
            "physically two copies, which the per-class cut cannot show: {fleet:?}"
        );
    }

    /// Half-open adjacency is a handoff, not an overlap — the same convention the pair
    /// fold applies, so the two kernels cannot disagree about what co-residency means.
    #[test]
    fn a_handoff_never_reads_as_an_overlap() {
        let spans = [
            row("chat", holder("a", 0), &[(0.0, 100.0)]),
            row("chat", holder("b", 0), &[(100.0, 200.0)]),
        ];
        let cells = super::replication_histogram(1, &spans, DAY);
        assert!(
            cells.iter().all(|c| c.holders == 1),
            "never two at once: {cells:?}"
        );
    }

    /// The cells of one identity sum across pairs — the property the ledger's window
    /// figure rests on, asserted at the kernel so the rollup inherits it.
    #[test]
    fn cells_sum_because_token_time_is_an_integral() {
        let spans: Vec<HolderSpans> = (0..3)
            .map(|i| row("chat", holder(&format!("e{i}"), 0), &[(0.0, 100.0)]))
            .collect();
        let cells = token_time_cells(7, &spans, DAY);
        let total: f64 = cells.iter().map(|c: &TokenTimeCell| c.token_ms).sum();
        assert_eq!(total, 3.0 * 700.0, "C(3,2) pairs × 100 ms × 7 tokens");
    }

    /// **Minute cells summed over the window ARE the histogram** — the streamed
    /// placement's serving grain and the day figure are one sweep read two ways,
    /// and this pin is what makes the minute-splitting arithmetic incapable of drifting
    /// from the day figure it feeds.
    #[test]
    fn minute_cells_summed_reproduce_the_histogram_exactly() {
        // Spans deliberately straddling minute boundaries, classes, and instance shapes.
        let spans = [
            row("chat", holder("e0", 0), &[(10_000.0, 130_000.0)]),
            row("chat", holder("e0", 1), &[(55_000.0, 61_000.0)]),
            row("chat", holder("e1", 0), &[(59_999.0, 180_500.0)]),
            row("batch", holder("e1", 0), &[(0.0, 90_000.0)]),
        ];
        let window = (0.0, 86_400_000.0);
        let minutes = super::replication_minute_cells(7, &spans, window);
        let mut summed: std::collections::BTreeMap<(String, u32, u32), f64> =
            std::collections::BTreeMap::new();
        for c in &minutes {
            *summed
                .entry((c.class.clone(), c.holders, c.instances))
                .or_insert(0.0) += c.token_ms;
        }
        for h in super::replication_histogram(7, &spans, window) {
            let got = summed
                .remove(&(h.class.clone(), h.holders, h.instances))
                .unwrap_or_else(|| panic!("missing cell {h:?}"));
            let diff = (got - h.token_ms).abs();
            assert!(diff < 1e-6, "{h:?}: minutes summed to {got}");
        }
        assert!(
            summed.is_empty(),
            "minute cells the histogram lacks: {summed:?}"
        );
    }

    /// A segment crossing a minute boundary lands in both buckets with exact shares,
    /// and every bucket instant belongs to the minute that starts it.
    #[test]
    fn a_boundary_crossing_segment_splits_at_the_minute() {
        let spans = [
            row("chat", holder("a", 0), &[(50_000.0, 70_000.0)]),
            row("chat", holder("b", 0), &[(50_000.0, 70_000.0)]),
        ];
        let cells = super::replication_minute_cells(1, &spans, (0.0, 86_400_000.0));
        assert_eq!(cells.len(), 2, "two buckets: {cells:?}");
        assert_eq!((cells[0].minute_ms, cells[0].token_ms), (0, 10_000.0));
        assert_eq!((cells[1].minute_ms, cells[1].token_ms), (60_000, 10_000.0));
        assert!(cells.iter().all(|c| c.holders == 2));
    }

    /// The band vocabulary is a partition: every holder count lands in exactly one band,
    /// the edges sit where the match says they do, and band "1" is a real member — the
    /// denominator read sums ALL bands.
    #[test]
    fn copy_bands_partition_the_holder_counts() {
        for (k, want) in [
            (1u32, "1"),
            (2, "2"),
            (3, "3"),
            (4, "4"),
            (5, "5-8"),
            (8, "5-8"),
            (9, "9-16"),
            (16, "9-16"),
            (17, "17+"),
            (400, "17+"),
        ] {
            let band = super::copy_band(k);
            assert_eq!(band, want, "k={k}");
            assert!(["1", "2", "3", "4", "5-8", "9-16", "17+"].contains(&band));
        }
        assert_eq!(super::copy_band(0), "1", "defensive total mapping");
        // The three causes, each placed at its own boundary.
        // One instance, one rank: the copies differ only by cache group, which is the
        // model's architecture and no policy removes it.
        // One copy: no relationship exists, and none is claimed.
        assert_eq!(super::locality(1, 1, 1), "none");
        // Two copies on one rank: they differ only by cache group.
        assert_eq!(super::locality(2, 1, 1), "group");
        // One instance, two ranks: the engine's own placement across dp ranks.
        assert_eq!(super::locality(2, 1, 2), "within");
        // Two instances: the fleet gateway's routing, whatever happens inside them.
        assert_eq!(super::locality(2, 2, 2), "cross");
        assert_eq!(
            super::locality(6, 2, 4),
            "cross",
            "coarsest relationship wins"
        );
    }

    /// Flattening to the fleet scope RE-MERGES per holder: one holder holding the same
    /// content under two classes is ONE holder, or the fleet sweep would double it —
    /// the merged-by-construction guarantee [`HolderSpans`] documents, preserved by the
    /// helper every fleet sweep goes through.
    #[test]
    fn fleet_flattening_remerges_a_holder_split_across_classes() {
        let spans = [
            row("agentic", holder("a", 0), &[(0.0, 100.0)]),
            row("batch", holder("a", 0), &[(50.0, 150.0)]),
        ];
        let flat = super::fleet_flattened(&spans);
        assert_eq!(flat.len(), 1, "one holder row: {flat:?}");
        assert_eq!(flat[0].class, "");
        assert_eq!(flat[0].spans, vec![(0.0, 150.0)], "spans coalesced");
        let cells = super::replication_histogram(1, &flat, DAY);
        assert!(
            cells.iter().all(|c| c.holders == 1),
            "one engine holding once is never duplication: {cells:?}"
        );
    }
}
