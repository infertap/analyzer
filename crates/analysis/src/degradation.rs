// SPDX-License-Identifier: AGPL-3.0-only

//! The degradation ledger: the entries a reader holds
//! against itself — where its own knowledge is impaired, for whom, and how hard the
//! figures over that scope may be read.
//!
//! Every entry answers four questions and nothing else: WHO is impaired (a producer
//! run expanded to the instances it covered, an instance, a holder), over WHAT extent
//! (domain-tagged: producer-clock evidence brackets here, engine-clock spans where a
//! declaration provides them), from WHAT cause, and in which REGISTER — the spec's own
//! three-word vocabulary (`uncovered`, `indeterminate`, `lower bound`).
//! The register is derived from the cause, never chosen by a consumer: a consumer that
//! could pick its register would pick the flattering one.
//!
//! Six causes:
//! - a HOLE in an incarnation's observed segment sequence, computed engine-side
//!   so the query returns gaps and never the sequence inventory (at fleet scale the
//!   inventory is millions of rows per raw window; the ledger is incident-shaped,
//!   never GPU-shaped);
//! - a `segments_dropped` declaration — the loss the producer names;
//! - an INCARNATION BOUNDARY: per instance, from its departing run's
//!   `agent_stop` (or last observation where none was received) to its next run's
//!   `agent_start` (or first observation) — a producer observes nothing while it is
//!   not running, so the interval is UNCOVERED, the ledger's strongest register.
//!   Overlapping runs produce no boundary: there was no moment nobody watched;
//! - an IDENTITY REFUSAL: the producer's declared capacity loss, scoped to
//!   exactly the named holder over exactly the declared engine-clock window — the one
//!   cause with a MAPPED extent, because the declaration is already in the engine
//!   domain (the record's own producer-clock `at_ms` never enters, per spec 2.6). Figures
//!   over that holder in that window are LOWER BOUNDS; every other holder stands,
//!   which is the point of a precisely scoped signal;
//! - a DROP WINDOW: a positive counter delta between two heartbeats — inbound
//!   loss the completeness clock detects and this ledger cites, so figures consult
//!   ONE surface for every detected-gap class rather than two;
//! - a RECOVERED TRUNCATION: an attributed recovery's `dropped_bytes` —
//!   the one loss continuity accounting cannot see, because the recovered segment keeps
//!   its identity and leaves no sequence hole. The producer names it; the ledger
//!   consumes it, and the recovered segment's span reads indeterminate.
//!
//! A crash-looping fleet mints boundary entries without limit, so the ledger holds its
//! incident-shaped bound by HULL COMPACTION: past [`HULL_COMPACTION_BOUND`] same-cause
//! entries on one scope, they coalesce to their hull — each cause variant defines its
//! own (a boundary hull runs from the first departure to the last arrival) — and the
//! compacted entry carries the count it summarizes, because a summarized ledger must
//! not present as a quiet one. Sound by the ledger's monotonicity: degradation may
//! widen and never narrow, so a hull costs precision, never honesty.
//!
//! Scope expansion is spec 5.1's rule: a producer-level loss degrades EVERY instance the
//! incarnation covered, resolved as the union of the incarnation's declared coverage
//! (`endpoints[].source` on its lifecycle records) and the instances its surviving
//! cache records mention — the declared set catches instances whose records were lost
//! whole, the surviving set catches runs whose lifecycle segments were lost. Both
//! expansion queries run only when an incident exists; a healthy fleet's ledger costs
//! two gap scans over sparse, kind-pruned families and nothing else.
//!
//! Clock discipline: gap evidence is producer-domain (`segment_open` stamps) and
//! is carried as evidence, not compared against engine windows — an entry with no
//! engine extent degrades every window it is fetched for, the strictly-conservative
//! reading (relevance is bounded by the fetch clip). Sharpening producer brackets onto
//! engine windows via the clock mapping is deliberate future work, and it can only ever
//! NARROW degradation, never widen it.

use crate::completeness::ranges_intersect;
use crate::intervals::Holder;
use crate::Window;
use std::collections::{BTreeMap, BTreeSet};

/// How hard a figure over a degraded scope may be read — the spec's vocabulary.
///
/// Declaration order is ascending severity, and worst-wins composition relies on the
/// derived `Ord`; pinned by `worst_wins_is_the_declaration_order`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Register {
    /// The figure stands, as `≥` and only as `≥`.
    LowerBound,
    /// Facts are present but the window cannot be read as complete.
    Indeterminate,
    /// Nothing was observed; a figure carried across reports an absence nobody
    /// measured.
    Uncovered,
}

impl Register {
    /// The persisted/reported vocabulary — one mapping, total, shared by the snapshot
    /// and the reader corpus alike.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LowerBound => "lower_bound",
            Self::Indeterminate => "indeterminate",
            Self::Uncovered => "uncovered",
        }
    }
}

/// Who an entry impairs. `Ord` because compaction groups by (scope, cause class).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    /// A producer run, expanded to every instance it covered. The expansion is
    /// part of the SCOPE, not a consumer concern: the instances whose records were
    /// lost whole are exactly the ones a record-driven consumer would forget.
    Run {
        /// The producer run whose loss this is — the incident's own scope before expansion.
        incarnation: String,
        /// Every instance the run covered: its declared `endpoints[].source` set, unioned with
        /// the instances its surviving cache records mention (spec 5.1).
        instances: Vec<String>,
    },
    /// One instance, all of its holders.
    Instance(String),
    /// One cache: `(instance, dp_rank, group_idx)`.
    Holder(Holder),
}

impl Scope {
    /// The scope as a reader sees it: the run with the instances it expanded to, the instance,
    /// or the holder as `instance/rank/group`.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Run {
                incarnation,
                instances,
            } => format!("run {incarnation} ({})", instances.join(", ")),
            Self::Instance(instance) => format!("instance {instance}"),
            Self::Holder(h) => format!("holder {}/{}/{}", h.instance, h.dp_rank, h.group_idx),
        }
    }

    fn covers(&self, h: &Holder) -> bool {
        match self {
            Self::Run { instances, .. } => instances.contains(&h.instance),
            Self::Instance(i) => *i == h.instance,
            Self::Holder(own) => own == h,
        }
    }
}

/// What the evidence was. The register is a consequence of the cause — see
/// [`Cause::register`] — so an entry cannot carry a flattering register beside a
/// damning cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// A hole inside an incarnation's observed segment sequence: segments
    /// `first_missing..=last_missing` never arrived and were never declared.
    SequenceGap {
        /// The first segment sequence that never arrived, inclusive.
        first_missing: u64,
        /// The last segment sequence that never arrived, inclusive.
        last_missing: u64,
    },
    /// A `segments_dropped` declaration: the producer names
    /// `first_seq..=last_seq` as reclaimed, and a declared loss the reader ignores is
    /// worse than an undetected one.
    DeclaredDrop {
        /// The first segment sequence the producer reclaimed, inclusive.
        first_seq: u64,
        /// The last segment sequence the producer reclaimed, inclusive.
        last_seq: u64,
    },
    /// The interval between one producer run's last observation and the next run's
    /// first: a producer observes nothing while it is not running.
    IncarnationBoundary {
        /// The departing run: its `agent_stop`, or last observation, opens the bracket.
        from: String,
        /// The arriving run: its `agent_start`, or first observation, closes the bracket.
        to: String,
    },
    /// A declared capacity loss: `refused` stores were denied an identity, so
    /// the named holder's evicts over the declared window cannot resolve and figures
    /// over them read `≥`.
    IdentityRefusal {
        /// How many stores the producer denied an identity over the declared window.
        refused: u64,
    },
    /// A drop window: a positive counter delta between two heartbeats, so the
    /// span lost inbound messages and the loss cannot be localized inside it. The
    /// completeness clock is the detector; the ledger cites it.
    DropWindow {
        /// The `dropped` counter's rise across the span — messages this source lost inbound.
        dropped: u64,
        /// The agent-level `oversized` counter's rise across the span, replicated onto every
        /// source's span (the conservative direction).
        oversized: u64,
    },
    /// An attributed recovery's declared truncation: the segment kept its
    /// identity and participates in continuity normally, so NO sequence hole prices the
    /// records its torn tail lost — `dropped_bytes` is the only place the loss is named,
    /// and a reader that ignored it would read the recovered span as complete. Facts are
    /// present but the span cannot be read as complete: indeterminate, never uncovered.
    RecoveredTruncation {
        /// The bytes the recovery discarded from the segment's torn tail, as the producer
        /// declared them (spec 5.5).
        dropped_bytes: u64,
    },
}

impl Cause {
    /// The register this cause forces.
    #[must_use]
    pub fn register(&self) -> Register {
        match self {
            Self::SequenceGap { .. }
            | Self::DeclaredDrop { .. }
            | Self::DropWindow { .. }
            | Self::RecoveredTruncation { .. } => Register::Indeterminate,
            Self::IncarnationBoundary { .. } => Register::Uncovered,
            Self::IdentityRefusal { .. } => Register::LowerBound,
        }
    }

    /// The cause class, for compaction keys and report grouping.
    #[must_use]
    pub fn class(&self) -> &'static str {
        match self {
            Self::SequenceGap { .. } => "sequence_gap",
            Self::DeclaredDrop { .. } => "declared_drop",
            Self::IncarnationBoundary { .. } => "incarnation_boundary",
            Self::IdentityRefusal { .. } => "identity_refused",
            Self::DropWindow { .. } => "drop_window",
            Self::RecoveredTruncation { .. } => "recovered_truncation",
        }
    }

    /// The hull of two same-class causes: each variant's natural widening — a range
    /// union for sequence causes, first-departure-to-last-arrival for boundaries.
    /// Only ever called on same-class pairs (compaction groups by class).
    fn hull(self, other: &Self) -> Self {
        match (self, other) {
            (
                Self::SequenceGap {
                    first_missing: a,
                    last_missing: b,
                },
                Self::SequenceGap {
                    first_missing: c,
                    last_missing: d,
                },
            ) => Self::SequenceGap {
                first_missing: a.min(*c),
                last_missing: b.max(*d),
            },
            (
                Self::DeclaredDrop {
                    first_seq: a,
                    last_seq: b,
                },
                Self::DeclaredDrop {
                    first_seq: c,
                    last_seq: d,
                },
            ) => Self::DeclaredDrop {
                first_seq: a.min(*c),
                last_seq: b.max(*d),
            },
            (Self::IncarnationBoundary { from, .. }, Self::IncarnationBoundary { to, .. }) => {
                Self::IncarnationBoundary {
                    from,
                    to: to.clone(),
                }
            }
            (Self::IdentityRefusal { refused: a }, Self::IdentityRefusal { refused: b }) => {
                // the natural hull of two declared losses is their total
                Self::IdentityRefusal { refused: a + b }
            }
            (
                Self::DropWindow {
                    dropped: a,
                    oversized: b,
                },
                Self::DropWindow {
                    dropped: c,
                    oversized: d,
                },
            ) => Self::DropWindow {
                dropped: a + c,
                oversized: b + d,
            },
            (
                Self::RecoveredTruncation { dropped_bytes: a },
                Self::RecoveredTruncation { dropped_bytes: b },
            ) => {
                // the natural hull of two declared truncations is their total
                Self::RecoveredTruncation {
                    dropped_bytes: a + b,
                }
            }
            (a, b) => unreachable!("hull over mixed cause classes: {a:?} vs {b:?}"),
        }
    }
}

/// A producer-clock evidence bracket. Either side may be unobservable — a drop of an
/// incarnation's oldest segments has no present predecessor — and an absent side is
/// reported as absent, never invented.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProducerBracket {
    /// The bracket's opening stamp, producer-clock milliseconds; `None` where no present
    /// predecessor could be observed.
    pub start_ms: Option<f64>,
    /// The bracket's closing stamp, producer-clock milliseconds; `None` where no present
    /// successor could be observed.
    pub end_ms: Option<f64>,
}

/// One entry: who, from what evidence, over what extent.
#[derive(Debug, Clone, PartialEq)]
pub struct Degradation {
    /// WHO is impaired: a run expanded to its instances, an instance, or one holder.
    pub scope: Scope,
    /// WHAT the evidence was; the register follows from it and is never chosen beside it.
    pub cause: Cause,
    /// Producer-domain evidence bracket (spec 2.6: never compared against engine windows;
    /// carried for the report and for future clock-mapping sharpening).
    pub producer_bracket: ProducerBracket,
    /// Engine-domain extent, where a declaration provides one. `None` means unmapped,
    /// and an unmapped entry degrades EVERY window it is fetched for — the
    /// strictly-conservative reading.
    pub engine_extent: Option<(f64, f64)>,
    /// How many incidents this entry stands for: 1 for a single observation, more
    /// when hull-compacted past [`HULL_COMPACTION_BOUND`] — carried so a summarized
    /// ledger cannot present as a quiet one.
    pub incidents: u64,
}

impl Degradation {
    /// The register this entry's cause forces.
    #[must_use]
    pub fn register(&self) -> Register {
        self.cause.register()
    }

    fn touches(&self, w: &Window) -> bool {
        self.engine_extent
            .is_none_or(|(s, e)| ranges_intersect(s, e, w.start_ms, w.end_ms))
    }

    fn applies(&self, holders: &[Holder], w: &Window) -> bool {
        self.touches(w) && holders.iter().any(|h| self.scope.covers(h))
    }
}

/// The register of an ABSENCE claim over a span (spec 5.6): exact only within an
/// observed, counted sequence; a lower bound everywhere else — before a producer
/// started, during a gap, or outside any window observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsenceRegister {
    /// The producer's counters establish complete observation over the span: nothing
    /// arrived unrecorded, so absence is a measurement.
    Exact,
    /// Outside counted observation, absence is `≥` and only `≥` — a reader stating
    /// more reports an absence it did not measure.
    LowerBound,
}

impl AbsenceRegister {
    /// The persisted/reported vocabulary, shared with the reader corpus.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::LowerBound => "lower_bound",
        }
    }
}

/// Past this many same-class entries on one scope, they coalesce to their hull.
///
/// The bound exists for one pathology: a crash-looping producer minting boundary
/// entries without limit. A labeled analytic constant (`docs/conformance.md`);
/// re-derived when a real crash-loop drill measures actual ledger sizes. Below it,
/// every incident stays individually visible for the report.
pub const HULL_COMPACTION_BOUND: usize = 32;

/// The assembled ledger for one fleet and window.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Ledger {
    /// The assembled entries, hull-compacted past [`HULL_COMPACTION_BOUND`] per scope and cause
    /// class. Consulted through [`Ledger::register_for`] and [`Ledger::in_window`].
    pub entries: Vec<Degradation>,
}

impl Ledger {
    /// Worst applicable register for a figure over these holders and this
    /// engine-domain window; `None` when no entry touches it. Worst-wins by
    /// [`Register`]'s ordering, never consumer-chosen.
    #[must_use]
    pub fn register_for(&self, holders: &[Holder], w: &Window) -> Option<Register> {
        self.entries
            .iter()
            .filter(|d| d.applies(holders, w))
            .map(Degradation::register)
            .max()
    }

    /// Every entry whose extent touches this engine window, regardless of scope — the
    /// FLEET-figure consultation: an aggregate over every holder is impaired by any
    /// entry at all, so scope filtering would understate it.
    #[must_use]
    pub fn in_window(&self, w: &Window) -> Vec<&Degradation> {
        self.entries.iter().filter(|d| d.touches(w)).collect()
    }
}

/// A run-scoped incident awaiting spec 5.1 expansion.
///
/// `pub` because the expansion that consumes it lives in the store-facing layer: resolving an
/// incarnation to the instances it covered is a QUERY, while deciding what counts as an incident
/// is a fold.
#[derive(Debug, Clone)]
pub struct Incident {
    /// The producer run the loss is scoped to, awaiting expansion to the instances it covered.
    pub incarnation: String,
    /// The loss itself; the expanded entry carries it unchanged.
    pub cause: Cause,
    /// The producer-clock evidence bracket the expanded entry will carry; its engine extent
    /// stays unmapped.
    pub bracket: ProducerBracket,
}

/// Hull-compact same-class entries on one scope past [`HULL_COMPACTION_BOUND`]: the
/// causes fold to their variant's hull, the brackets to theirs (an unknown side stays
/// unknown — a hull may not invent a stamp), and the entry carries the incident count.
/// `pub` for the same reason as `parse`: hull compaction runs after assembly, in the pipeline.
///
/// # Panics
/// If a same-cause group is empty, which the grouping above cannot produce.
#[must_use]
pub fn compact(entries: Vec<Degradation>) -> Vec<Degradation> {
    let mut groups: BTreeMap<(Scope, &'static str), Vec<Degradation>> = BTreeMap::new();
    for e in entries {
        groups
            .entry((e.scope.clone(), e.cause.class()))
            .or_default()
            .push(e);
    }
    groups
        .into_values()
        .flat_map(|group| {
            if group.len() <= HULL_COMPACTION_BOUND {
                return group;
            }
            let incidents = group.len() as u64;
            let hulled = group
                .into_iter()
                .reduce(|acc, next| Degradation {
                    scope: acc.scope,
                    cause: acc.cause.hull(&next.cause),
                    producer_bracket: ProducerBracket {
                        start_ms: hull_side(
                            acc.producer_bracket.start_ms,
                            next.producer_bracket.start_ms,
                            f64::min,
                        ),
                        end_ms: hull_side(
                            acc.producer_bracket.end_ms,
                            next.producer_bracket.end_ms,
                            f64::max,
                        ),
                    },
                    engine_extent: match (acc.engine_extent, next.engine_extent) {
                        (Some((a, b)), Some((c, d))) => Some((a.min(c), b.max(d))),
                        _ => None, // an unmapped part makes the hull unmapped: wider, never narrower
                    },
                    incidents: 1, // overwritten below
                })
                .expect("group is non-empty");
            vec![Degradation {
                incidents,
                ..hulled
            }]
        })
        .collect()
}

/// One side of a bracket hull: known only when BOTH sides are known — an absent stamp
/// stays absent, because a hull may lose precision but never invent evidence.
fn hull_side(a: Option<f64>, b: Option<f64>, pick: fn(f64, f64) -> f64) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(pick(x, y)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(instance: &str) -> Holder {
        Holder {
            instance: instance.to_string(),
            dp_rank: 0,
            group_idx: 0,
        }
    }

    fn entry(scope: Scope, engine_extent: Option<(f64, f64)>) -> Degradation {
        Degradation {
            scope,
            cause: Cause::SequenceGap {
                first_missing: 2,
                last_missing: 2,
            },
            producer_bracket: ProducerBracket {
                start_ms: Some(0.0),
                end_ms: Some(1.0),
            },
            engine_extent,
            incidents: 1,
        }
    }

    #[test]
    fn worst_wins_is_the_declaration_order() {
        assert!(Register::Uncovered > Register::Indeterminate);
        assert!(Register::Indeterminate > Register::LowerBound);
    }

    #[test]
    fn scope_matching_covers_run_instance_and_holder() {
        let run = Scope::Run {
            incarnation: "r".into(),
            instances: vec!["i0".into(), "i1".into()],
        };
        assert!(run.covers(&holder("i1")));
        assert!(!run.covers(&holder("i2")));
        assert!(Scope::Instance("i0".into()).covers(&holder("i0")));
        assert!(!Scope::Instance("i0".into()).covers(&holder("i1")));
        let h = Scope::Holder(holder("i0"));
        assert!(h.covers(&holder("i0")));
        assert!(!h.covers(&Holder {
            dp_rank: 1,
            ..holder("i0")
        }));
    }

    /// An entry with no engine extent degrades every window it is fetched for — the
    /// conservative reading of unmapped producer-domain evidence — while a mapped
    /// extent confines it.
    #[test]
    fn an_unmapped_extent_applies_everywhere_a_mapped_one_is_confined() {
        let w = Window {
            start_ms: 100.0,
            end_ms: 200.0,
        };
        let everywhere = entry(Scope::Instance("i0".into()), None);
        let outside = entry(Scope::Instance("i0".into()), Some((300.0, 400.0)));
        let ledger = Ledger {
            entries: vec![everywhere, outside],
        };
        assert_eq!(
            ledger.register_for(&[holder("i0")], &w),
            Some(Register::Indeterminate)
        );
        let only_outside = Ledger {
            entries: vec![entry(Scope::Instance("i0".into()), Some((300.0, 400.0)))],
        };
        assert_eq!(only_outside.register_for(&[holder("i0")], &w), None);
    }

    #[test]
    fn no_applicable_entry_is_no_register_not_a_default() {
        let ledger = Ledger {
            entries: vec![entry(Scope::Instance("i1".into()), None)],
        };
        let w = Window {
            start_ms: 0.0,
            end_ms: 1.0,
        };
        assert_eq!(ledger.register_for(&[holder("i0")], &w), None);
    }

    /// Each cause variant hulls in its own natural direction, and a bracket side that
    /// any constituent left unknown stays unknown in the hull.
    #[test]
    fn hulls_widen_and_never_invent() {
        let a = Cause::SequenceGap {
            first_missing: 2,
            last_missing: 3,
        };
        let b = Cause::SequenceGap {
            first_missing: 7,
            last_missing: 9,
        };
        assert_eq!(
            a.hull(&b),
            Cause::SequenceGap {
                first_missing: 2,
                last_missing: 9
            }
        );
        let x = Cause::IncarnationBoundary {
            from: "run-1".into(),
            to: "run-2".into(),
        };
        let y = Cause::IncarnationBoundary {
            from: "run-2".into(),
            to: "run-3".into(),
        };
        assert_eq!(
            x.hull(&y),
            Cause::IncarnationBoundary {
                from: "run-1".into(),
                to: "run-3".into()
            },
            "a boundary hull runs first departure to last arrival"
        );
        assert_eq!(hull_side(Some(1.0), Some(3.0), f64::min), Some(1.0));
        assert_eq!(hull_side(None, Some(3.0), f64::min), None);
    }
}

// ===========================================================================================
// THE STORE-FREE PATH. Folds over the Ordered Event IR, one per cause.
//
// **Scope is a property of each INPUT, not one global clip, and this module is where that becomes
// visible.** The cache event stream is window-clipped for volume. The lifecycle stream is clipped
// to `LIFECYCLE_CLIP_MARGIN_MS` because that is the wall-vs-engine slack a boundary needs. And the
// `segment_open` stream is scoped to the RETENTION HORIZON, because a declared drop's bracket may
// sit arbitrarily far from the window. Nothing is unbounded — records past retention do not
// exist, so "everything retained" IS the bound, and naming it that way is the same discipline
// applied everywhere else. Incarnation boundaries fold from the ordinary clipped stream.
// ===========================================================================================

/// Spec 2.4 declared capacity losses, folded from the lifecycle IR.
///
/// **Both sides of the bound are engine-domain** — the declaration's own window against the
/// analysis window — and the record's producer-clock `at_ms` never enters the predicate.
/// That is what makes this the one cause arriving with a mapped extent.
#[must_use]
pub fn refusal_entries_from_events(
    ir: &[crate::events::LifecycleEvent],
    w: &Window,
) -> Vec<Degradation> {
    let mut entries: Vec<Degradation> = ir
        .iter()
        .filter(|e| e.kind == "identity_refused")
        .filter_map(|e| {
            let (refused, start, end) = (e.refused?, e.window_start_ms?, e.window_end_ms?);
            // The declared window must touch the analysis window; closed on the low side and
            // half-open on the high, matching the query.
            (end >= w.start_ms && start < w.end_ms).then(|| Degradation {
                scope: Scope::Holder(Holder {
                    instance: e.instance.clone(),
                    dp_rank: e.dp_rank.unwrap_or(0),
                    group_idx: e.group_idx.unwrap_or(0),
                }),
                cause: Cause::IdentityRefusal { refused },
                producer_bracket: ProducerBracket {
                    start_ms: None,
                    end_ms: None,
                },
                engine_extent: Some((start, end)),
                incidents: 1,
            })
        })
        .collect();
    entries.sort_by(|a, b| {
        let key = |d: &Degradation| match &d.scope {
            Scope::Holder(h) => h.instance.clone(),
            Scope::Instance(i) => i.clone(),
            Scope::Run { incarnation, .. } => incarnation.clone(),
        };
        key(a)
            .cmp(&key(b))
            .then_with(|| match (a.engine_extent, b.engine_extent) {
                (Some(x), Some(y)) => x.0.total_cmp(&y.0),
                _ => std::cmp::Ordering::Equal,
            })
    });
    entries
}

/// One hole in an incarnation's observed segment sequence: the missing range and its
/// producer-clock bracket.
#[derive(Debug, Clone, PartialEq)]
pub struct SequenceHole {
    /// The producer run whose observed sequence has the hole.
    pub incarnation: String,
    /// The first missing segment sequence, inclusive: one past the previous observed open.
    pub missing_from: u64,
    /// The last missing segment sequence, inclusive: one before the next observed open.
    pub missing_to: u64,
    /// The bracket the loss sits inside: the previous observed open, and this one.
    pub bracket_start_ms: f64,
    /// The next observed open's stamp, producer-clock milliseconds — the bracket's close.
    pub bracket_end_ms: f64,
}

/// Spec 5.4 structural loss, folded from the lifecycle IR.
///
/// **The inventory never exists, which is a strict improvement on the query it replaces.** That
/// one computes gaps engine-side specifically so "the sequence inventory never leaves the
/// database" — at fleet scale it is millions of rows per raw window. The fold does better: it
/// keys on `(incarnation, segment_seq)`, so what it holds is bounded by the SEGMENT count rather
/// than the record count, and segments are orders of magnitude fewer than the events inside them.
///
/// **The first observed sequence number reports no gap.** Sequence missing BELOW observation is
/// the window's retention edge — spec 5.6's lower-bound territory, not spec 5.4's detected loss — and
/// claiming it as a hole would manufacture an incident out of the clip.
#[must_use]
pub fn sequence_holes_from_events(ir: &[crate::events::LifecycleEvent]) -> Vec<SequenceHole> {
    // Deduplicated as it goes: `(incarnation, seq) -> open stamp`. A repeated open for one seq is
    // one segment observed twice, not two segments.
    let mut opens: BTreeMap<(&str, u64), f64> = BTreeMap::new();
    for ev in ir.iter().filter(|e| e.kind == "segment_open") {
        if let Some(seq) = ev.segment_seq {
            opens
                .entry((ev.incarnation.as_str(), seq))
                .and_modify(|at| *at = at.min(ev.at_ms))
                .or_insert(ev.at_ms);
        }
    }

    let mut by_run: BTreeMap<&str, Vec<(u64, f64)>> = BTreeMap::new();
    for ((run, seq), at) in opens {
        by_run.entry(run).or_default().push((seq, at));
    }

    let mut holes = Vec::new();
    for (run, seqs) in by_run {
        // Already ordered by seq: the map is keyed on it.
        for pair in seqs.windows(2) {
            let ((prev_seq, prev_at), (seq, at)) = (pair[0], pair[1]);
            if seq > prev_seq + 1 {
                holes.push(SequenceHole {
                    incarnation: run.to_string(),
                    missing_from: prev_seq + 1,
                    missing_to: seq - 1,
                    bracket_start_ms: prev_at,
                    bracket_end_ms: at,
                });
            }
        }
    }
    holes
}

/// Spec 5.4 drop windows, from the uncertainty model — which already has an IR path, so this cause
/// crosses for free once the model does.
#[must_use]
pub fn drop_window_entries_from_model(
    model: &crate::completeness::UncertaintyModel,
) -> Vec<Degradation> {
    model
        .drop_windows()
        .map(|span| Degradation {
            scope: Scope::Instance(span.source.clone()),
            cause: Cause::DropWindow {
                dropped: span.dropped_delta.max(0).unsigned_abs(),
                oversized: span.oversized_delta.max(0).unsigned_abs(),
            },
            producer_bracket: ProducerBracket {
                start_ms: Some(span.start_at_ms),
                end_ms: Some(span.end_at_ms),
            },
            engine_extent: None,
            incidents: 1,
        })
        .collect()
}

/// Spec 5.3 blind spots, folded from the clipped lifecycle IR.
///
/// Per instance: from the departing run's `agent_stop` — or its last observation where none was
/// received — to the next run's `agent_start`, or its first observation. All producer-domain.
///
/// **Overlapping runs produce no boundary**, which is the `from_ms < to_ms` filter: if the
/// incoming run entered before the outgoing one left, there was no moment nobody watched, and
/// emitting a negative bracket would invent a blind spot out of a healthy rolling deploy.
///
/// **Coverage comes from `heartbeat` and `agent_stop` only, never `agent_start`** — matching the
/// query's own CTE. A start record's endpoint list describes what the run intends to cover; the
/// heartbeats describe what it did.
#[must_use]
pub fn incarnation_boundaries_from_events(
    ir: &[crate::events::LifecycleEvent],
) -> Vec<Degradation> {
    struct Run {
        entry_at: f64,
        exit_at: f64,
        first_at: f64,
        saw_start: bool,
        saw_stop: bool,
    }

    let mut runs: BTreeMap<&str, Run> = BTreeMap::new();
    for ev in ir
        .iter()
        .filter(|e| matches!(e.kind.as_str(), "agent_start" | "heartbeat" | "agent_stop"))
    {
        let r = runs.entry(ev.incarnation.as_str()).or_insert(Run {
            entry_at: ev.at_ms,
            exit_at: ev.at_ms,
            first_at: ev.at_ms,
            saw_start: false,
            saw_stop: false,
        });
        r.first_at = r.first_at.min(ev.at_ms);
        match ev.kind.as_str() {
            // A declared entry outranks an inferred one, and the earliest of them wins.
            "agent_start" => {
                r.entry_at = if r.saw_start {
                    r.entry_at.min(ev.at_ms)
                } else {
                    ev.at_ms
                };
                r.saw_start = true;
            }
            "agent_stop" => {
                r.exit_at = if r.saw_stop {
                    r.exit_at.max(ev.at_ms)
                } else {
                    ev.at_ms
                };
                r.saw_stop = true;
            }
            _ => {}
        }
        if !r.saw_start {
            r.entry_at = r.entry_at.min(ev.at_ms);
        }
        if !r.saw_stop {
            r.exit_at = r.exit_at.max(ev.at_ms);
        }
    }

    // Which instances each run was observed covering.
    let mut covers: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for ev in ir
        .iter()
        .filter(|e| matches!(e.kind.as_str(), "heartbeat" | "agent_stop"))
    {
        for ep in &ev.endpoints {
            covers
                .entry(ep.source.as_str())
                .or_default()
                .insert(ev.incarnation.as_str());
        }
    }

    let mut entries = Vec::new();
    for (src, incarnations) in covers {
        let mut ordered: Vec<(&str, &Run)> = incarnations
            .iter()
            .filter_map(|inc| runs.get(inc).map(|r| (*inc, r)))
            .collect();
        // Total, where the query's `ORDER BY r.first_at` is not.
        ordered.sort_by(|a, b| {
            a.1.first_at
                .total_cmp(&b.1.first_at)
                .then_with(|| a.0.cmp(b.0))
        });

        for pair in ordered.windows(2) {
            let ((prev_run, prev), (run, cur)) = (pair[0], pair[1]);
            if prev.exit_at < cur.entry_at {
                entries.push(Degradation {
                    scope: Scope::Instance(src.to_string()),
                    cause: Cause::IncarnationBoundary {
                        from: prev_run.to_string(),
                        to: run.to_string(),
                    },
                    producer_bracket: ProducerBracket {
                        start_ms: Some(prev.exit_at),
                        end_ms: Some(cur.entry_at),
                    },
                    engine_extent: None,
                    incidents: 1,
                });
            }
        }
    }
    entries
}

/// Spec 5.5's declared truncation: an ATTRIBUTED `segment_recovered` with `dropped_bytes > 0`,
/// as run-scoped incidents awaiting spec 5.1 expansion.
///
/// **This is the loss no other cause prices.** An attributed recovery keeps its segment's
/// identity and its `segment_seq`, so continuity accounting sees a complete sequence — the
/// records that died in the torn tail leave no hole. `dropped_bytes` is the producer's only
/// in-band declaration of them, and a store whose deciding close died in that tail would
/// otherwise read as cold rather than indeterminate: the flattering direction.
///
/// The bracket is the recovered segment's own span, from the retention-scoped opens: the
/// nearest observed open of the same incarnation at or before the recovery stamp is the
/// recovered segment's own header (the recovery record rides inside that segment), and the
/// recovery stamp closes the bracket. An absent open leaves the start absent — reported as
/// absent, never invented — and the entry's engine extent stays unmapped, which degrades
/// every window it is fetched for: the strictly-conservative reading.
///
/// The UNATTRIBUTED case never lands here, by contract. Since 1.11 (spec 4.3) it is the
/// recovering incarnation's declaration that a headerless file was discarded, written second
/// in that incarnation's first segment with the whole file's size: the span it covered
/// precedes the incarnation's start and is already uncovered under spec 5.3, and the
/// declaration neither narrows nor widens that window. The record is banked as a lifecycle
/// row (`attributed`, `dropped_bytes`), which is its whole surface. The pre-1.11 form, a
/// marker-first `orphan-<ms>` file with no incarnation, is admitted under 4.1's tolerance and
/// lands in `orphan_batches`.
#[must_use]
pub fn recovered_truncation_incidents_from_events(
    lifecycle_ir: &[crate::events::LifecycleEvent],
    opens_at_retention: &[crate::events::LifecycleEvent],
) -> Vec<Incident> {
    let mut out: Vec<Incident> = lifecycle_ir
        .iter()
        .filter(|e| e.kind == "segment_recovered")
        .filter(|e| e.attributed == Some(true) && !e.incarnation.is_empty())
        .filter_map(|e| {
            let dropped_bytes = u64::try_from(e.dropped_bytes?).ok().filter(|&b| b > 0)?;
            let own_open = opens_at_retention
                .iter()
                .filter(|o| {
                    o.kind == "segment_open" && o.incarnation == e.incarnation && o.at_ms <= e.at_ms
                })
                .map(|o| o.at_ms)
                .reduce(f64::max);
            Some(Incident {
                incarnation: e.incarnation.clone(),
                cause: Cause::RecoveredTruncation { dropped_bytes },
                bracket: ProducerBracket {
                    start_ms: own_open,
                    end_ms: Some(e.at_ms),
                },
            })
        })
        .collect();
    out.sort_by(|a, b| {
        a.incarnation.cmp(&b.incarnation).then_with(|| {
            a.bracket
                .end_ms
                .unwrap_or(f64::NAN)
                .total_cmp(&b.bracket.end_ms.unwrap_or(f64::NAN))
        })
    });
    out
}

/// One `segments_dropped` declaration with the bracket its PRESENT neighbours give it.
#[derive(Debug, Clone, PartialEq)]
pub struct DeclaredDropIncident {
    /// The producer run that declared the drop.
    pub incarnation: String,
    /// The first reclaimed segment sequence, inclusive, as declared.
    pub first_seq: u64,
    /// The last reclaimed segment sequence, inclusive, as declared.
    pub last_seq: u64,
    /// `None` on a side with no observed neighbour — an absent bracket, never a zero wearing
    /// real-value clothes.
    pub bracket_start_ms: Option<f64>,
    /// The stamp of the nearest observed open past `last_seq`, producer-clock milliseconds.
    pub bracket_end_ms: Option<f64>,
}

/// Spec 5.4's declared loss — the one the producer NAMES — bracketed from the retention-scoped opens.
///
/// **Two inputs at two scopes, deliberately.** The declarations come from the window-clipped
/// lifecycle stream, because a declaration is an event that either happened near this window or
/// did not. The bracketing opens come from the source's retention-horizon load
/// (`EventSource::segment_opens_at_retention_horizon`), because a producer may declare a
/// loss long after it happened and the neighbouring segments may be far outside the window. Using
/// the clipped stream for both would silently narrow the bracket — a shorter degraded span than
/// reality, which is the flattering direction and would pass every fixture that does not straddle
/// the clip.
#[must_use]
pub fn declared_drop_incidents_from_events(
    lifecycle_ir: &[crate::events::LifecycleEvent],
    opens_at_retention: &[crate::events::LifecycleEvent],
) -> Vec<DeclaredDropIncident> {
    let mut opens: BTreeMap<&str, Vec<(u64, f64)>> = BTreeMap::new();
    for ev in opens_at_retention
        .iter()
        .filter(|e| e.kind == "segment_open")
    {
        if let Some(seq) = ev.segment_seq {
            opens
                .entry(ev.incarnation.as_str())
                .or_default()
                .push((seq, ev.at_ms));
        }
    }

    let mut out = Vec::new();
    for ev in lifecycle_ir.iter().filter(|e| e.kind == "segments_dropped") {
        let (Some(first_seq), Some(last_seq)) = (ev.first_seq, ev.last_seq) else {
            continue;
        };
        let neighbours = opens.get(ev.incarnation.as_str());
        // `maxIf(at_ms, seq < first)` and `minIf(at_ms, seq > last)`: the closest observed segment
        // on each side, by STAMP, over the segments that qualify.
        let bracket_start_ms = neighbours.and_then(|ns| {
            ns.iter()
                .filter(|(seq, _)| *seq < first_seq)
                .map(|(_, at)| *at)
                .reduce(f64::max)
        });
        let bracket_end_ms = neighbours.and_then(|ns| {
            ns.iter()
                .filter(|(seq, _)| *seq > last_seq)
                .map(|(_, at)| *at)
                .reduce(f64::min)
        });
        out.push(DeclaredDropIncident {
            incarnation: ev.incarnation.clone(),
            first_seq,
            last_seq,
            bracket_start_ms,
            bracket_end_ms,
        });
    }
    out.sort_by(|a, b| {
        a.incarnation
            .cmp(&b.incarnation)
            .then_with(|| a.first_seq.cmp(&b.first_seq))
    });
    out
}
