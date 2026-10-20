// SPDX-License-Identifier: AGPL-3.0-only

//! What a `store` record may be counted into (spec 5.9).
//!
//! Some engines announce a block **reused** from cache with the same event they use for one
//! newly inserted, in a shape a consumer cannot tell apart. The reference producer is one: it
//! builds a prefix-cache hit and a fresh insertion through a single shared builder,
//! deliberately, so both "emit identical event shapes for downstream consumers". A second
//! engine's radix cache emits only for the newly inserted tail, its matched prefix reaching no
//! wire at all. Two engines, two answers — which is why the producer declares rather than the
//! reader guesses.
//!
//! Summing `n_tokens` over `store` records on an unlabelled stream counts a block once per
//! announcement, so the figure rises with how **effective** the cache is — highest on the fleet
//! with the least waste. Every input correct and the measurement inverted, which is why spec 5.9
//! makes it a MUST NOT rather than a caveat.
//!
//! **Facts layer, deliberately.** These verdicts are built on the residency substrate
//! (/`intervals`) and never on [`crate::classify`]. Spec 5.9 is a rule about what the records
//! mean, which sits below the classification boundary; keying it on a class would put
//! a `semantics_version` concept underneath a `contract_version` obligation.
//!
//! The three figures and why they differ, all from one fact — a reuse announcement describes a
//! block the engine found **resident**:
//!
//! - **unqualified** — every store. Refused under `unlabelled`, since each announcement counts
//!   its block again.
//! - **recompute** — stores following an OBSERVED close of the same block. Safe under any
//!   declaration: residency ended at that close, so a reuse announcement could only qualify if
//!   the residency reopened unobserved — a lost record, and therefore already a detected gap
//!   under spec 5.4. Either the store is a genuine recomputation or the window is degraded anyway.
//! - **first insertion** — stores with NO observed prior residency. Refused under `unlabelled`,
//!   because that is exactly where a reuse and a first insertion are indistinguishable: the
//!   block may have been resident since before observation began.
//!
//! The last one is the trap. It looks like it should expire once the cache has turned over, but
//! a block that is never evicted is never re-announced, and the hottest shared prefix in a fleet
//! is the block least likely to be evicted — so the miscount concentrates on exactly the traffic
//! that matters most, and reports a cache as *less* effective than it is.

use crate::intervals::Interval;

/// What a `store` means on a stream (spec 2.3). Ordered weakest-last: a window takes the
/// maximum it contains, because a reader that mixed declarations would apply one producer's
/// `none` to another producer's records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Declaration {
    /// The engine never announces reuse as a store.
    None_,
    /// It does, and the producer labels them (`reused: true`).
    Labelled,
    /// It does, and nothing distinguishes them. Also the reading for an ABSENT declaration and
    /// for a value this version does not define — silence has not told a reader its stores are
    /// insertions, and a value added by a later version describes a case this reader cannot
    /// model, so the one thing known about it is that the reader does not know.
    Unlabelled,
}

impl Declaration {
    fn parse(raw: &str) -> Self {
        match raw {
            "none" => Self::None_,
            "labelled" => Self::Labelled,
            _ => Self::Unlabelled,
        }
    }

    /// May an unqualified count of `store` records be presented as insertions?
    #[must_use]
    pub const fn permits_unqualified(self) -> bool {
        !matches!(self, Self::Unlabelled)
    }
}

/// One spec 5.9 figure: whether a reader may present it, and its value where it may.
///
/// `records`/`tokens` are `None` when `countable` is false — not zero. A refused figure is a
/// figure that does not exist, and rendering it as zero would be the same lie one layer down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Figure {
    /// Whether the window's declaration lets a reader present this figure at all.
    pub countable: bool,
    /// The `store` records counted; `Some` only when `countable`.
    pub records: Option<u64>,
    /// Their `n_tokens` summed; `Some` only when `countable`.
    pub tokens: Option<i64>,
}

impl Figure {
    const fn refused() -> Self {
        Self {
            countable: false,
            records: None,
            tokens: None,
        }
    }

    const fn counted(records: u64, tokens: i64) -> Self {
        Self {
            countable: true,
            records: Some(records),
            tokens: Some(tokens),
        }
    }
}

/// The scope a block identity is an identity within, plus the identity: the key spec 5.9 asks its
/// questions per. Named once so the three maps below and the store rows agree by construction.
type BlockKey = (String, u32, u32, String, Option<u32>);

fn key_of(iv: &Interval) -> BlockKey {
    (
        iv.key.instance.clone(),
        iv.key.dp_rank,
        iv.key.group_idx,
        iv.key.identity.clone(),
        iv.key.key_epoch,
    )
}

/// The window's declaration, folded from the lifecycle IR — the store-free path.
///
/// **The caller supplies the scope, and for this input the correct one is the retention horizon.**
/// The query this replaces has no time predicate at all: a declaration is a property of a producer
/// RUN, not of a window, and a run whose lifecycle records all predate the window still declares
/// what its stores mean. Passing a narrowly-clipped stream here would read a fleet as `unlabelled`
/// that had declared otherwise — which is the SAFE direction (spec 5.9 refuses figures under
/// `unlabelled`) but is still a figure withheld on the strength of a clip rather than the stream.
///
/// **The weakest declaration in the window wins**, because a reader that mixed them would apply
/// one producer's `none` to another producer's records.
#[must_use]
pub fn declaration_from_events(ir: &[crate::events::LifecycleEvent]) -> Declaration {
    ir.iter()
        .filter(|e| matches!(e.kind.as_str(), "agent_start" | "heartbeat" | "agent_stop"))
        .filter_map(|e| e.reuse_reporting.as_deref())
        .map(Declaration::parse)
        .max()
        // No declaration observed at all reads as Unlabelled, per spec 5.9: absence is not "none".
        .unwrap_or(Declaration::Unlabelled)
}

/// The three spec 5.9 figures, folded from the Ordered Event IR — the store-free path.
///
/// **Facts layer, deliberately**: built on the residency substrate and never on the classifier.
/// Spec 5.9 is a rule about what the records MEAN, which sits below the classification boundary, so
/// keying it on a class would put a `semantics_version` concept underneath a `contract_version`
/// obligation.
#[must_use]
pub fn figures_from_events(
    cache_ir: &[crate::events::Event],
    lifecycle_ir: &[crate::events::LifecycleEvent],
) -> (Figure, Figure, Figure) {
    let decl = declaration_from_events(lifecycle_ir);
    let intervals = crate::intervals::residency_intervals_from_events(cache_ir);

    // Tier-agnostic on purpose: spec 5.9 asks about the BLOCK on this holder, not about where it sat.
    // A block demoted to CPU and promoted back has observed prior residency either way.
    let mut closes: std::collections::HashMap<BlockKey, Vec<f64>> =
        std::collections::HashMap::new();
    let mut opens: std::collections::HashMap<BlockKey, Vec<f64>> = std::collections::HashMap::new();
    for iv in &intervals {
        if let Some(c) = iv.close_ms {
            closes.entry(key_of(iv)).or_default().push(c);
        }
        if let Some(o) = iv.open_ms {
            opens.entry(key_of(iv)).or_default().push(o);
        }
    }

    let (mut u_rec, mut u_tok) = (0u64, 0i64);
    let (mut r_rec, mut r_tok) = (0u64, 0i64);
    let (mut f_rec, mut f_tok) = (0u64, 0i64);
    for ev in cache_ir.iter().filter(|e| {
        e.kind == crate::events::Kind::Store
            && e.block_id.is_some()
            && matches!(e.locality.as_deref(), None | Some("LOCAL"))
    }) {
        let Some(identity) = ev.identity() else {
            continue;
        };
        let (dp, grp) = ev.holder_dims();
        let key: BlockKey = (
            ev.instance.clone(),
            dp,
            grp,
            identity.to_string(),
            ev.key_epoch,
        );

        // Unqualified: every store, less the ones a labelled producer marked as reuse.
        if !ev.reused {
            u_rec += 1;
            u_tok += ev.n_tokens;
        }
        let after_close = closes
            .get(&key)
            .is_some_and(|cs| cs.iter().any(|c| *c <= ev.at_ms));
        // Nothing observed about this block before this store: no close, and no residency opened
        // strictly earlier. The interval this store itself opens has `open_ms == at_ms`, so it
        // cannot disqualify its own opener.
        let observed_before = after_close
            || opens
                .get(&key)
                .is_some_and(|os| os.iter().any(|o| *o < ev.at_ms));
        if after_close {
            r_rec += 1;
            r_tok += ev.n_tokens;
        }
        if !observed_before {
            f_rec += 1;
            f_tok += ev.n_tokens;
        }
    }

    (
        if decl.permits_unqualified() {
            Figure::counted(u_rec, u_tok)
        } else {
            Figure::refused()
        },
        // Permitted under EVERY declaration — the qualification is what makes it safe, not the
        // producer's ability to label.
        Figure::counted(r_rec, r_tok),
        // The same gate as `unqualified`, and for the sharper reason: a first-insertion count is
        // refused exactly where a reuse and a first insertion are indistinguishable.
        if decl.permits_unqualified() {
            Figure::counted(f_rec, f_tok)
        } else {
            Figure::refused()
        },
    )
}
