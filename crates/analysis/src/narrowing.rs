// SPDX-License-Identifier: AGPL-3.0-only

//! The narrowing a `seq` permits (spec 5.4).
//!
//! A `dropped` delta localizes a loss no better than the interval between two heartbeats, so
//! one lost message degrades every fact in it. With the transport's sequence on the records the
//! discontinuity sits between two observed numbers, and the degraded span shrinks to that
//! bracket.
//!
//! **Narrowing is the only weakening spec 5.4 permits, so its conditions are the whole of it.** A
//! reader that narrows on an unsound bracket exonerates a span nothing observed — the one
//! direction in which this is unsafe, and the reason two of the three fixtures behind this
//! module are cases that must NOT narrow.
//!
//! Three conditions, each closing a different way of being wrong:
//!
//! - **Both records carry `seq`.** Without a number on each side there is no bracket, only a
//!   guess about where the loss fell.
//! - **They share `(instance_id, incarnation)`.** A sequence is monotonic only within an
//!   incarnation (the reference producer builds its counter per publisher, so a restart begins
//!   again), and a bracket spanning a restart compares numbers drawn from two different
//!   sequences.
//! - **They are ADJACENT in the record order.** An unnumbered record between two numbered ones
//!   cannot be placed on either side of the discontinuity, so the span it occupies cannot be
//!   excluded from the loss. Narrowing to the nearest numbered pair across it would choose a
//!   span the stream never delimited.
//!
//! The finest resolution available is the bracket, never the message: every record decoded from
//! one message shares its `seq`, so the stream cannot say where inside a bracket the loss fell.

/// Whether a degraded span may be narrowed, and to what.
///
/// `start_ms`/`end_ms` are the ENGINE clock, like the cache records they come from, and are
/// `None` when `narrowed` is false — a span that could not be narrowed has no endpoints of its
/// own, and inventing them would be the narrowing this refused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GapSpan {
    /// Whether a sound bracket was found: both records numbered, one incarnation, adjacent, and
    /// a strict forward jump.
    pub narrowed: bool,
    /// `at_ms` of the last record before the discontinuity, engine clock; `None` unless narrowed.
    pub start_ms: Option<f64>,
    /// `at_ms` of the first record after it, engine clock; `None` unless narrowed.
    pub end_ms: Option<f64>,
}

impl GapSpan {
    const fn refused() -> Self {
        Self {
            narrowed: false,
            start_ms: None,
            end_ms: None,
        }
    }
}

/// The narrowed span, folded from the Ordered Event IR — the store-free path.
///
/// **The three conditions are the whole of it, and each closes a different way of being wrong.**
/// Narrowing is the only weakening spec 5.4 permits, so an unsound bracket exonerates a span nothing
/// observed:
///
/// - **Both records carry `seq`.** Without a number on each side there is no bracket, only a guess.
/// - **They share `(instance_id, incarnation)`.** A sequence is monotonic only within an
///   incarnation, so a bracket spanning a restart compares numbers from two different sequences.
/// - **They are ADJACENT in the record order.** This falls out of walking consecutive rows: an
///   unnumbered record between two numbered ones becomes `prev` and refuses the pair, because the
///   span it occupies cannot be excluded from the loss.
///
/// **A strict forward jump only.** An equal or lower number is a duplicate or a restart, and
/// neither is a loss.
#[must_use]
pub fn gap_span_from_events(ir: &[crate::events::Event]) -> GapSpan {
    // The continuity scope spec 5.1 defines, so "adjacent" means adjacent in ONE producer's sequence
    // rather than in the merged stream. Not the IR's own total order, which is time-major.
    let mut ordered: Vec<&crate::events::Event> = ir.iter().collect();
    ordered.sort_by(|a, b| {
        a.instance
            .cmp(&b.instance)
            .then_with(|| a.incarnation.cmp(&b.incarnation))
            .then_with(|| a.at_ms.total_cmp(&b.at_ms))
            .then_with(|| a.seq.cmp(&b.seq))
    });

    let mut prev: Option<&crate::events::Event> = None;
    for ev in ordered {
        if let Some(p) = prev {
            if let (Some(pseq), Some(seq)) = (p.seq, ev.seq) {
                if p.instance == ev.instance && p.incarnation == ev.incarnation && seq > pseq + 1 {
                    return GapSpan {
                        narrowed: true,
                        start_ms: Some(p.at_ms),
                        end_ms: Some(ev.at_ms),
                    };
                }
            }
        }
        prev = Some(ev);
    }
    GapSpan::refused()
}
