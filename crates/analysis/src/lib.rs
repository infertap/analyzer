// SPDX-License-Identifier: AGPL-3.0-only

//! Analysis: the correctness core — the executable form of the classification semantics,
//! `docs/classification-semantics.md` (`SEMANTICS_VERSION`). Implementations conform to
//! `fixtures/joins/` and the tap's conformance corpus, never the reverse.
//!
//! The crate has no dependencies and performs no I/O: every function here is a pure
//! derivation over events and parameters it is handed.
//!
//! Clock domains: cache facts carry engine-domain `at_ms`; lifecycle facts carry the
//! producer's wall-domain `at_ms`. A drop window's engine-domain extent is its producer-time
//! bracket widened by `δ_skew` (see [`completeness`]), so classification and coverage are
//! functions of the bytes, identical for every reader of them.
//!
//! # Where to start
//!
//! This crate is the fold: the derivation from typed events to figures, and the heart of the
//! analyzer as a library. It depends on nothing, and the dependency allowlist pins that.
//!
//! - The surface a partner links is the `pub use` block below: the Ordered Event IR
//!   ([`events::Event`], [`events::EventSource`], [`events::IdentityGroup`]) in, one identity
//!   group's [`classify::Classification`] out, with the [`completeness::Coverage`] that must
//!   travel beside every figure. [`Window`], [`Params`] and [`Discriminator`] are the
//!   derivation's inputs and its identity.
//! - The fold, in reading order: [`intervals`] (residency intervals from the events),
//!   [`classify`] (the six-class taxonomy of every store), [`completeness`] (coverage and the
//!   completeness clock), [`residual`] (per-holder spans, co-residency token-time and the
//!   copies histogram), [`staleness`], [`tree`].
//! - The contracts: [`emitted`] names what a pass produces, once; [`differential`] is the
//!   standing audit that holds the stream equal to a whole re-derivation; [`degradation`] is
//!   the ledger a reader holds against the figures.
//!
//! The law every module obeys is the fence (`ARCHITECTURE.md`): an event fact is a pure
//! function of one identity's own event prefix. The fold therefore takes one identity group
//! at a time and shares nothing between groups.

pub mod block_mass;
pub mod class_band;
pub mod classify;
pub mod completeness;
pub mod coverage_sums;
pub mod degradation;
pub mod differential;
pub mod dup_cells;
pub mod emitted;
pub mod error;
pub mod events;
pub mod generations;
pub mod instance_spans;
pub mod intervals;
pub mod module;
pub mod narrowing;
pub mod observed_coverage;
pub mod residual;
pub mod restated;
pub mod reuse;
pub mod roster;
pub mod staleness;
pub mod tags;
pub mod tree;
pub mod whale_days;

// **The fold's surface, in one place.** What a partner links to run the derivation over
// its own events: the Ordered Event IR
// in, one identity group's classification out, and the coverage that must travel with it.
// Every name is also reachable at its module; this block is the statement of what is
// supported, not a second API.
pub use classify::{classify_identity_group, Classification, GroupClassification};
pub use completeness::{Coverage, UncertaintyModel};
pub use error::Error;
pub use events::{Event, EventSource, IdentityGroup, Kind};

/// One UTC day in milliseconds.
///
/// **One definition, because three things have to agree on it.** A day settles at its end, the
/// rollup attributes a classification to its store-event's day (`toDate(at_ms / 1000, 'UTC')`),
/// and the coverage cells slice their uncertainty spans at the same boundaries. A second copy of
/// this number in one of those three would be a day grain that drifts from the other two
/// silently.
pub const DAY_MS: f64 = 86_400_000.0;

/// One hour in milliseconds, the grain the dated key layout and the hour tier share.
pub const HOUR_MS: f64 = 3_600_000.0;

/// **The examined-history horizon** — how far back classification looks before a store: an hour.
/// See [`Params::examined_history_ms`] for what it means and why it is an hour.
pub const EXAMINED_HISTORY_MS: f64 = HOUR_MS;

/// The examined history in whole days, rounded up: the unit the things DERIVED from it count in,
/// the input horizon's floor and the lifecycle evidence clip. An integer of days so neither has
/// to round a float; the assertion below keeps it covering [`EXAMINED_HISTORY_MS`].
pub const EXAMINED_HISTORY_DAYS: u32 = 1;

const _: () = assert!(EXAMINED_HISTORY_MS <= EXAMINED_HISTORY_DAYS as f64 * DAY_MS);

/// **How late a record may arrive and still be corrected, in whole days.** A day stays open to
/// correction until every operator's edge is this far past it; a record of a day closed before it
/// arrives is counted `too_late`. Not the examined history: how far back a store looks is not how
/// late its evidence may land, and a shipper backlog runs hours.
pub const CORRECTION_HORIZON_DAYS: u32 = 1;

/// [`CORRECTION_HORIZON_DAYS`] in milliseconds.
pub const CORRECTION_HORIZON_MS: f64 = CORRECTION_HORIZON_DAYS as f64 * DAY_MS;

/// **The labeled margin around the examined history, in whole days** — and it is the single
/// primitive two other constants are built from, which is what keeps them consistent by
/// construction rather than by coincidence.
///
/// It appears twice:
///
/// 1. `LEDGER_RETENTION_FLOOR_DAYS` = `CORRECTION_HORIZON_DAYS + EXAMINED_HISTORY_DAYS +
///    CONTEXT_MARGIN_DAYS`: the least input a deployment may keep, so a re-derived day — a late
///    segment can reopen one up to the correction horizon back — still finds its whole examined
///    history behind it.
/// 2. [`completeness::EVIDENCE_CLIP_MARGIN_MS`], the backward reach of the coverage path's
///    lifecycle fetch PAST the analysis window it is already given.
///
/// **The two together are the property that matters**: the evidence clip reaches
/// `EXAMINED_HISTORY_DAYS + CONTEXT_MARGIN_DAYS` before an emitted day, which is exactly the
/// least input a deployment keeps. Everything the coverage path can see is therefore still there
/// on a re-derivation, which is the last step of making an emitted day a function of
/// `(day, parameters)`.
///
/// One day is judgment, not measurement, and it is small deliberately: every day of margin is a
/// day of retention the daemon may no longer spend under pressure, and a day is enough to
/// absorb a boundary that lands mid-day at either end.
pub const CONTEXT_MARGIN_DAYS: u32 = 1;

/// The analysis window `W` (engine-domain milliseconds, half-open `[start, end)`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Window {
    /// The window's first instant, engine-domain milliseconds, inclusive.
    pub start_ms: f64,
    /// The window's end, engine-domain milliseconds, exclusive.
    pub end_ms: f64,
}

impl Window {
    /// Construct a finite, ordered half-open interval. Empty intervals are valid and emit nothing.
    /// Fractional milliseconds are preserved.
    ///
    /// # Errors
    /// Non-finite, reversed, or out-of-range bounds.
    pub fn new(start_ms: f64, end_ms: f64) -> Result<Self, Error> {
        let window = Self { start_ms, end_ms };
        window.validate()?;
        Ok(window)
    }

    /// Validate a caller-supplied range before execution or integer conversion.
    ///
    /// # Errors
    /// Non-finite, reversed, or bounds whose floor/ceiling cannot fit signed milliseconds.
    #[allow(clippy::cast_precision_loss)] // 2^63 is exact in f64 and is the exclusive upper bound
    pub fn validate(&self) -> Result<(), Error> {
        if !self.start_ms.is_finite() || !self.end_ms.is_finite() {
            return Err(Error::InvalidWindow("bounds must be finite"));
        }
        if self.start_ms > self.end_ms {
            return Err(Error::InvalidWindow("start exceeds end"));
        }
        let lower = i64::MIN as f64;
        if self.start_ms.floor() < lower || self.end_ms.ceil() >= -lower {
            return Err(Error::InvalidWindow(
                "millisecond conversion would overflow",
            ));
        }
        Ok(())
    }

    /// Whether the half-open interval contains no instants. Exact equality is intentional.
    #[must_use]
    #[allow(clippy::float_cmp)] // emptiness is exact, never an epsilon-sized interval
    pub fn is_empty(&self) -> bool {
        self.start_ms == self.end_ms
    }

    /// The window's length in milliseconds.
    #[must_use]
    pub fn length_ms(&self) -> f64 {
        (self.end_ms - self.start_ms).max(0.0)
    }

    /// The whole UTC days from `first` through `last`, inclusive.
    ///
    /// # Errors
    /// Reversed days or overflow in the day-to-millisecond conversion.
    pub fn whole_days(first: i64, last: i64) -> Result<Self, Error> {
        let overflow = || Error::InvalidWindow("day conversion would overflow");
        if first > last {
            return Err(Error::InvalidWindow("first day exceeds last"));
        }
        let lo = first.checked_mul(86_400_000).ok_or_else(overflow)?;
        let hi = last
            .checked_add(1)
            .and_then(|d| d.checked_mul(86_400_000))
            .ok_or_else(overflow)?;
        #[allow(clippy::cast_precision_loss)] // validated again against the exclusive i64 boundary
        Self::new(lo as f64, hi as f64)
    }
}

/// Analysis parameters — defaults per join-semantics, stated once there.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Params {
    /// Cross-instance clock allowance. Default 1000 ms; the documented 50 ms
    /// preset is "NTP-disciplined deployments, operator-attested".
    ///
    /// **The middle shape in design ownership rule**, and it stays exactly as it is:
    /// the platform defines the values, and a customer-attested fact about their
    /// infrastructure selects the sharper preset. They attest what their world IS; they do
    /// not pick a number.
    pub delta_skew_ms: f64,
    /// **The examined-history horizon: how far back classification looks before a store.**
    ///
    /// A COVERAGE parameter, not a tuning knob. Wider sees more and costs
    /// linearly more; narrower is cheaper and reads more of the fleet as `cold`. The report states
    /// it in the telescope form the recovery-curve reframe established: *"examined history: 1 h —
    /// `cold` means no prior close observed within it."*
    ///
    /// **It bounds two things:** the prior-close lookback that decides ETR/post-clear versus cold
    /// (a close counts while it is less than the history back), and the taint lookback, which
    /// bounds which telemetry gaps can poison an absence decision. It does not bound an open
    /// interval: a residency is visible for as long as it stays open.
    ///
    /// **Every error it can make understates the waste figures**, which is what makes a finite
    /// horizon admissible at all: a close that fell off the back of the horizon reads as `cold`
    /// rather than as ETR. Nothing it does inflates a number.
    ///
    /// **It does not leave the waste classes untouched.** ETR and post-clear rest on closes within
    /// THIS horizon, so shortening it undercounts them directly: the horizon is the reach of the
    /// reader's own line, not a free coverage knob (`docs/classification-semantics.md` §4).
    ///
    /// **An hour, exact, and no longer** ([`EXAMINED_HISTORY_MS`], refused past it by
    /// [`Params::validate`]). The frontier holds every (holder, block) closed within the horizon,
    /// so its width is the analyzer's memory: at 500 GPUs a day of closes was hundreds of
    /// gigabytes. A GPU's KV cache turns over in minutes, and every band an operator acts on
    /// ([`emitted::Band`]) ends within the hour; a block back after it informs an offload tier,
    /// not the cache, and reads as `cold`, which understates. The bands end at the hour, so a
    /// longer history would state gaps no band names.
    pub examined_history_ms: f64,
}

/// **The human-legible half of the meaning pair**, and it lives here beside [`Discriminator`]
/// because the two answer one question in two halves: what the numbers MEAN.
///
/// A property of the DERIVATION, not of any write path: it is stamped onto every keyed-history
/// row so two generations coexist rather than overwrite, and it lives here so a reader needs no
/// edge to a writer to name the generation it reads.
///
/// `discriminator` is a token over the classification-affecting PARAMETERS; this is our
/// join-semantics. Either moving is a meaning change. Never sum across either.
pub const SEMANTICS_VERSION: &str = "js-7";

/// The classification-affecting parameters, as one comparable token.
///
/// **The defect this closes.** `SEMANTICS_VERSION` is a compile-time constant tracking OUR
/// join-semantics; it says nothing about the parameters in force. `class_rollup_daily` keys on
/// it and replaces on that key, so two derivations under different classification parameters
/// would collide and the later one OVERWRITE the earlier — and since a changed parameter derives
/// the whole log afresh, it would silently rewrite prior days under the new value.
///
/// **Readable rather than hashed, deliberately.** Two parameters fit in a short token, and a
/// discriminator you can read in a query result is worth more during an incident than one you
/// have to look up. It also keeps this crate at zero external dependencies. If the set grows
/// past readability the token becomes a hash and the exhaustive destructure below is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Discriminator(String);

impl Discriminator {
    /// Derive the discriminator from the parameters a derivation runs under.
    ///
    /// **The exhaustive-destructure obligation.** The `let Params { .. }` below names
    /// every field with no `..` rest pattern, so **adding a field to `Params` is a compile error
    /// here until somebody classifies it** as discriminating or not. That is the whole
    /// mechanism: the question gets asked at the moment the field is introduced, rather than
    /// discovered later as a trend that changed shape retroactively.
    #[must_use]
    pub fn of(params: &Params) -> Self {
        // **THE CRITERION, written here so it travels with the guard.** A parameter is
        // DISCRIMINATING if:
        //
        //   two passes over an identical record stream, under different values of it, can
        //   produce different values in KEYED HISTORY
        //
        // — where "values" includes coverage, because coverage is a figure riding every
        // number. Not "does it change class labels": that test is too narrow.
        //
        // The exhaustive destructure (no `..` rest pattern) makes the question mandatory:
        // **adding a field to `Params` is a compile error here until somebody applies the
        // criterion to it.** That is the whole mechanism — the question gets asked when the
        // field is introduced, not discovered later as a trend that changed shape retroactively.
        let Params {
            // DISCRIMINATING — the cross-instance clock allowance decides which events on
            // different holders count as simultaneous, and therefore what joins at all.
            delta_skew_ms,
            // DISCRIMINATING — the horizon the classifier examines decides what `cold` MEANS
            // ("no prior close observed within the examined history"), so two derivations over
            // an identical record stream under different horizons produce different class
            // counts. Naming it is what makes an emitted day a function of (day, parameters)
            // and nothing else, rather than of whatever retention happened to hold.
            examined_history_ms,
        } = params;

        // Milliseconds as integers: two runs at the same parameters must produce byte-identical
        // tokens, and float formatting is not a stable contract to lean on for a storage key.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Self(format!(
            "s{}x{}",
            *delta_skew_ms as u64, *examined_history_ms as u64
        ))
    }

    /// The token spelled back out as the values it encodes.
    ///
    /// **The token is a LOSSLESS encoding of the discriminating set, by construction**, so a
    /// reader needs no stored copy of the parameters a generation ran under — the key already
    /// carries them. That is what lets a generation seam state its own cause while staying a
    /// function of the rows: `refresh_generation_markers` decodes this rather than remembering
    /// what some earlier pass was configured with.
    ///
    /// It lives beside [`Discriminator::of`] because the two are one grammar. A decoder in
    /// another crate is a second definition of the token, and it drifts the first time a
    /// parameter joins the discriminating set.
    ///
    /// A token this grammar does not recognise comes back verbatim. It is a real case rather
    /// than a defensive one — a marker written by a build whose grammar differs is exactly what
    /// [`Discriminator::observed`] exists for — and printing the raw token beats inventing values
    /// for it.
    #[must_use]
    pub fn describe(&self) -> String {
        let decoded = self.0.strip_prefix('s').and_then(|rest| {
            let (skew, history) = rest.split_once('x')?;
            let (skew, history) = (skew.parse::<u64>().ok()?, history.parse::<u64>().ok()?);
            Some(format!(
                "delta_skew_ms={skew} examined_history_ms={history}"
            ))
        });
        decoded.unwrap_or_else(|| self.0.clone())
    }

    /// A discriminator as it was RECORDED, rather than derived from parameters in hand.
    ///
    /// **The provenance distinction is the whole reason this is a second constructor rather than
    /// a `From<String>`.** [`Discriminator::of`] derives a token from parameters this process
    /// holds and can therefore explain. This one accepts a token that came out of storage — a
    /// `markers` row's `from_value`, naming a generation whose parameters are no longer in hand
    /// and may never have been this binary's.
    ///
    /// A caller reading history across a parameter seam needs exactly that and cannot get it from
    /// `of`: the `Params` the value was derived under are gone. What it must NOT get is the
    /// ability to fabricate a generation — so this is the only other way to make one, it is
    /// named for where the value came from, and nothing constructs it from user input.
    ///
    /// **The token is opaque here and is not validated**, deliberately. Its grammar is `of`'s
    /// business; a parser here would be a second place that knows the format, and it would reject tokens written by a future version
    /// that this one is meant to read past rather than misread.
    #[must_use]
    pub fn observed(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    #[must_use]
    /// The discriminator as the store spells it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Params {
    /// Validate finite nonnegative semantic durations before state identity or execution.
    ///
    /// # Errors
    /// Non-finite, negative or overflowing duration.
    pub fn validate(&self) -> Result<(), Error> {
        for value in [self.delta_skew_ms, self.examined_history_ms] {
            if value < 0.0 || Window::new(0.0, value).is_err() {
                return Err(Error::InvalidParameters(
                    "durations must be finite nonnegative milliseconds within the signed range",
                ));
            }
        }
        if self.examined_history_ms > EXAMINED_HISTORY_MS {
            return Err(Error::InvalidParameters(
                "the examined history is at most an hour: the bands state gaps up to it",
            ));
        }
        Ok(())
    }
}

impl Params {
    /// These parameters with `delta_skew_ms` set. `Params` is `#[non_exhaustive]`: start from
    /// `Params::default()` and set what differs.
    #[must_use]
    pub fn with_delta_skew_ms(mut self, delta_skew_ms: f64) -> Self {
        self.delta_skew_ms = delta_skew_ms;
        self
    }

    /// These parameters with `examined_history_ms` set.
    #[must_use]
    pub fn with_examined_history_ms(mut self, examined_history_ms: f64) -> Self {
        self.examined_history_ms = examined_history_ms;
        self
    }
}

impl Default for Params {
    fn default() -> Self {
        Self {
            delta_skew_ms: 1000.0,
            // An hour — see the field's own note.
            examined_history_ms: EXAMINED_HISTORY_MS,
        }
    }
}

#[cfg(test)]
mod discriminator_tests {
    use super::{Discriminator, Params};

    #[test]
    fn the_discriminator_separates_parameter_generations_and_nothing_else() {
        // The defect it closes: rows computed under different classification parameters
        // collided on one rollup key and the later pass overwrote the earlier. Different
        // parameters must produce different tokens, or history silently rewrites itself.
        let base = Params::default();
        assert_eq!(Discriminator::of(&base), Discriminator::of(&base));

        let skew_changed = Params {
            delta_skew_ms: 50.0,
            ..Params::default()
        };
        assert_ne!(
            Discriminator::of(&base),
            Discriminator::of(&skew_changed),
            "the clock allowance decides what joins at all"
        );

        // The examined-history horizon decides what `cold` MEANS — "no prior close observed
        // within it" — so two passes over identical records under different horizons produce
        // different class counts in keyed history. It also replaces the thing that was never a
        // parameter: under `full_span` the horizon was whatever retention happened to hold, so a
        // day re-passed later saw less context and could reclassify under the SAME token.
        let horizon_changed = Params {
            examined_history_ms: 2.0 * 86_400_000.0,
            ..Params::default()
        };
        assert_ne!(
            Discriminator::of(&base),
            Discriminator::of(&horizon_changed),
            "the examined history decides what cold means, and cold is keyed history"
        );
    }

    #[test]
    fn the_token_is_stable_and_readable() {
        // Stability: a storage key computed from float formatting would be a contract nobody
        // stated. Readability: during an incident the discriminator is visible in a query
        // result rather than something to look up.
        let d = Discriminator::of(&Params::default());
        assert_eq!(d.as_str(), "s1000x3600000");
        assert_eq!(
            d,
            Discriminator::of(&Params::default()),
            "same parameters, byte-identical token"
        );
    }
}
