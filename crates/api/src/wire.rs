// SPDX-License-Identifier: AGPL-3.0-only

//! The wire types — one `#[derive(Serialize)]` struct per response family.
//!
//! **Why these exist.** The contract is derived from these types (`docs/read-api.md` section 10),
//! and section 2.2's source ratchet counts their fields; a handler building `serde_json::Value`
//! would give neither anything to read.
//!
//! **[`Figure`] is the load-bearing type.** A field that holds a measured mass, a fraction of
//! stored mass or a coverage fraction is typed `Figure` and nothing else — so `tokens: f64` does
//! not compile into a response, and `static_audit::api_wire_figures_are_typed` fails the build if
//! a vocabulary field appears with a bare numeric type.
//!
//! **It is NOT a second serializer.** `covered_json` is the one
//! place a coverage-carrying figure becomes JSON in this workspace, and a `#[derive(Serialize)]`
//! on four fields here would have been a second one — the duplication read-api section 2.2 exists to prevent,
//! reintroduced by the very change meant to enforce it. So `Figure` delegates: the type system
//! gets something to check, and the bytes still come from one function.

use analyzer_serving_clickhouse::Covered;
use serde::{Serialize, Serializer};
use utoipa::ToSchema;

/// A coverage-carrying figure on the wire.
///
/// Newtype rather than an alias, so the ratchet has a name to look for and a bare `f64` cannot be
/// substituted by accident.
#[derive(Debug, Clone)]
pub struct Figure(pub Covered<f64>);

impl From<&Covered<f64>> for Figure {
    fn from(c: &Covered<f64>) -> Self {
        Self(c.clone())
    }
}

/// The four field names a figure always carries, in the order `covered_json` emits them.
///
/// **The ONE hand-written thing in the generated spec, and therefore the one place the document
/// could disagree with the bytes** — every other schema is derived from a struct the serializer
/// walks. `figure_schema_matches_what_covered_json_emits` pins it against actual output, so the
/// disagreement is a test failure rather than a client generated against a shape that never
/// ships.
pub const FIGURE_FIELDS: [&str; 4] = ["value", "register", "coverage", "coverage_kind"];

impl utoipa::PartialSchema for Figure {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, Type};
        ObjectBuilder::new()
            .description(Some(
                "A value that cannot exist without the coverage it was measured at. All four \
                 fields are always present: a naked number is unrepresentable here.",
            ))
            .property(
                "value",
                ObjectBuilder::new().schema_type(Type::Number).build(),
            )
            .property(
                "register",
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .description(Some(
                        "The register the value is stated in, always a lower bound.",
                    ))
                    .build(),
            )
            .property(
                "coverage",
                ObjectBuilder::new()
                    .schema_type(Type::Number)
                    .minimum(Some(0.0))
                    .maximum(Some(1.0))
                    .build(),
            )
            .property(
                "coverage_kind",
                ObjectBuilder::new()
                    .schema_type(Type::String)
                    .description(Some(
                        "How that fraction was computed. Two figures at 80% on different bases \
                         are not comparable.",
                    ))
                    .build(),
            )
            .required("value")
            .required("register")
            .required("coverage")
            .required("coverage_kind")
            .into()
    }
}

impl utoipa::ToSchema for Figure {}

/// A coverage-carrying figure, as every consumer of this product sees it.
///
/// **Four fields always, and a naked number is unrepresentable on the wire** because there is no
/// path to JSON that omits them — the same law `Covered` holds in the type system, held once more
/// at the boundary where the type stops travelling.
///
/// It lives here because a serializer belongs to whatever renders, and this API is the only
/// renderer; the read layer below it serializes nothing.
#[must_use]
pub(crate) fn covered_json(c: &Covered<f64>) -> serde_json::Value {
    serde_json::json!({
        "value": c.value,
        "register": c.register,
        "coverage": c.coverage,
        "coverage_kind": c.coverage_kind,
    })
}

impl Serialize for Figure {
    /// **Delegated, deliberately.** See the module header: the shape has exactly one definition,
    /// `covered_json`, so every figure on the wire is the same object rather than two that
    /// agree today.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        covered_json(&self.0).serialize(serializer)
    }
}

/// A figure derived from another's confidence — a fraction from its numerator, a count from the
/// mass it counts. **Never coverage 1.0 by default**: a count rendered as fully observed is a
/// claim nobody measured.
#[must_use]
pub fn derived(value: f64, from: &Covered<f64>) -> Figure {
    Figure(Covered {
        value,
        register: from.register,
        coverage: from.coverage,
        coverage_kind: from.coverage_kind,
    })
}

/// The two-key envelope. Every 200 is one of these.
#[derive(Serialize)]
pub struct Envelope<T> {
    /// The freshness stamp, as `envelope::Stamp::to_json` renders it: fleet, window, pass,
    /// meaning pair, raw horizon and analyzer version. Identical across responses at steady
    /// state, which is what lets a console declare it once and render only the deviations.
    pub stamp: serde_json::Value,
    /// The response family's payload, one wire struct per route. A bounded refusal rides
    /// INSIDE it, in the data position, never in place of it.
    pub data: T,
}

// ===========================================================================================
// Shared fragments.
// ===========================================================================================

impl WindowJson {
    /// A window in both forms, from one place. `routes` and `sse` both needed this and were
    /// building it two ways — one through `envelope::window_json`, one inline in a `json!`.
    #[must_use]
    pub fn of(w: &analyzer_analysis::Window) -> Self {
        let v = crate::envelope::window_json(w);
        Self {
            start_ms: w.start_ms,
            end_ms: w.end_ms,
            start_day: v["start_day"].as_str().unwrap_or_default().to_string(),
            end_day: v["end_day"].as_str().unwrap_or_default().to_string(),
        }
    }
}

#[derive(Serialize, ToSchema)]
pub(crate) struct WindowJson {
    pub start_ms: f64,
    pub end_ms: f64,
    pub start_day: String,
    /// The last day COVERED, not the exclusive bound.
    pub end_day: String,
}

/// An in-band refusal — a bounded answer rather than an error.
///
/// **The same closed cause set as [`crate::error::ApiError`], and a `bound` for the same reason.**
/// `docs/read-api.md` makes this the PRIMARY refusal path, and its rule is that a refusal which
/// does not state its bound is the vague 404 the section exists to prevent. A free-string cause
/// here would make a console narrow it against a transcribed copy of the enum and read the bound
/// out of whatever the surrounding response happens to carry; one typed field does that work.
#[derive(Serialize, ToSchema)]
pub(crate) struct Refusal {
    pub cause: crate::error::Cause,
    pub message: String,
    /// The facts that make the refusal actionable — the horizon, the marker, the generation.
    /// **Absent only where the cause is self-describing**, never because nobody supplied it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound: Option<serde_json::Value>,
    /// What was asked for, echoed beside the bound. The 4xx body carries the same pair, and
    /// `error.rs` promises the two paths read identically — this field is what keeps that true
    /// on the in-band one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested: Option<serde_json::Value>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HolderId {
    pub instance: String,
    pub dp_rank: u32,
    pub group_idx: u32,
}

/// A coverage statement beside a comparison, rather than inside it.
#[derive(Serialize, ToSchema)]
pub(crate) struct CoverageStatement {
    pub value: f64,
    pub coverage_kind: &'static str,
}

// ===========================================================================================
// `/v1/stamp`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct StampData {
    pub ready: bool,
}

// ===========================================================================================
// `/v1/figures` — shape 1, segmented.
// ===========================================================================================

/// Shape 10 — where the duplication sits, as co-residency token-time.
///
/// **This measure SUMS and shape 6's does not.** `day_rows_sum_to_window` states it on the wire
/// so a consumer asserts the aggregation law rather than guessing it — the failure when it
/// guesses wrong is silent.
#[derive(Serialize, ToSchema)]
pub struct PlacementData {
    /// Always `placement`: the co-residency register these figures are stated in, named on
    /// the wire so a consumer never sums it into the two in `never_summed_with`.
    pub register: &'static str,
    /// Both siblings, because placement is neither the order-statistic mass nor recompute.
    pub never_summed_with: Vec<&'static str>,
    /// Token-milliseconds. The glass renders token-hours; the wire keeps the base unit.
    pub unit: &'static str,
    /// True: day rows sum to the window figure.
    pub day_rows_sum_to_window: bool,
    /// Where the rows came from — the producer relation they were read out of.
    pub bound: PlacementBound,
    /// The quantity `rows` and `within_engine` DISJOINTLY partition — a pair's instances are
    /// equal or they are not, so the two arrays already sum to this and are never summed again.
    /// Its coverage is the WEAKEST cell in the response, never an average.
    pub window_total: Figure,
    /// What `limit` left out, absent when nothing was.
    ///
    /// **Absent means complete because the count was taken**, not because nobody looked. Pairs
    /// grow with the SQUARE of holders, so this response is bounded by construction and a bound
    /// nobody states reads as a census.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<TruncationJson>,
    /// Cross-engine pairs: the fleet gateway's routing affinity moves these.
    pub rows: Vec<PlacementRow>,
    /// Same-engine rank pairs: data-parallel rank placement moves these.
    pub within_engine: Vec<WithinEngineRow>,
}

/// The pairs a `limit` excluded, and the mass they carried.
///
/// **The excluded MASS is the field that matters.** Fifty dropped pairs holding a rounding error
/// are noise; three holding 40 % of the window mean the reader is looking at the wrong picture.
/// A count alone cannot tell those apart, so both ride together and `window_total` stays the
/// denominator over EVERY pair, returned or not.
#[derive(Serialize, ToSchema)]
pub struct TruncationJson {
    /// How many holder pairs `rows` and `within_engine` carry between them, each whole.
    pub pairs_returned: usize,
    /// How many holder pairs had co-residency mass in the window, returned or not.
    pub pairs_total: usize,
    /// Σ token-milliseconds over the pairs `limit` dropped, at the weakest coverage among
    /// them — the mass that says whether the cut matters.
    pub token_ms_excluded: Figure,
}

/// Where these rows come from, stated rather than left to be inferred.
#[derive(Serialize, ToSchema)]
pub struct PlacementBound {
    /// Which relation produced these rows: the producer's per-day pair table, whose
    /// pair basis is the per-instance observed spans (the grain ladder's pair basis).
    pub source: &'static str,
}

/// One workload × engine-pair × day cell.
#[derive(Serialize, ToSchema)]
pub struct PlacementRow {
    /// Declared on the segment (the spec's workload clause); `undeclared` where the producer declared none.
    pub workload: String,
    /// The UTC day this cell covers, `YYYY-MM-DD` as the engine rendered it.
    pub day: String,
    /// The first member's instance — the operator-named slot its tap was started with.
    /// Pairs are canonical: `a` sorts before `b` under the holder order.
    pub engine_a: String,
    /// The first member's data-parallel rank within `engine_a`.
    pub rank_a: u32,
    /// The first member's KV cache group index within that rank.
    pub group_a: u32,
    /// The second member's instance. Never `engine_a`: same-engine pairs ride
    /// `within_engine`.
    pub engine_b: String,
    /// The second member's data-parallel rank within `engine_b`.
    pub rank_b: u32,
    /// The second member's KV cache group index within that rank.
    pub group_b: u32,
    /// Coverage is the pair's own observed intersection, at instance grain.
    pub token_ms: Figure,
}

/// One workload × rank-pair × day cell inside a single engine.
#[derive(Serialize, ToSchema)]
pub struct WithinEngineRow {
    /// Declared on the segment (the spec's workload clause); `undeclared` where the producer
    /// declared none.
    pub workload: String,
    /// The UTC day this cell covers, `YYYY-MM-DD` as the engine rendered it.
    pub day: String,
    /// The one instance both members belong to.
    pub engine: String,
    /// The first member's data-parallel rank; `a` sorts before `b` under the holder order.
    pub rank_a: u32,
    /// The first member's KV cache group index within that rank.
    pub group_a: u32,
    /// The second member's data-parallel rank.
    pub rank_b: u32,
    /// The second member's KV cache group index within that rank.
    pub group_b: u32,
    /// Token-milliseconds of co-residency; coverage is the pair's own observed intersection.
    pub token_ms: Figure,
}

/// The thrash lens — banded recompute mass (`bands_by`, the thrash
/// panel). The four observed gap bands ride `rows`; the GAP-UNOBSERVED mass — no
/// observed prior close, first-seen and gone-past-lookback indistinguishable by
/// construction — is ITS OWN figure beside them, never a fifth band in the stack.
#[derive(Serialize, ToSchema)]
pub(crate) struct BandsData {
    pub register: &'static str,
    pub never_summed_with: &'static str,
    pub grain: &'static str,
    /// The served bucket WIDTH in milliseconds (3,600,000 at hour grain, 86,400,000 at
    /// day) — the window-echo block `ReplicationData` carries, on this surface too.
    pub bucket_ms: i64,
    pub start_ms: f64,
    pub end_ms: f64,
    /// For a custom range: why this grain (`range × retention`); absent on the stops.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derived: Option<String>,
    /// The holder cut's POPULATION: how many distinct holders had banded mass in the
    /// window, before the top-N bound cut them — "top 8 of 468" needs its denominator.
    /// Absent when the request carried no holder dim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub population: Option<u64>,
    /// How many holders the bound kept (worst first by window mass, each whole or
    /// absent). Absent with `population`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holders_returned: Option<u64>,
    /// Whether the data behind this grain still reaches the window's start — hour grain
    /// serves from the event horizon and says so.
    pub reach: ReachJson,
    /// The observed bands only (`<10s`, `10s-1m`, `1-10m`, `10-60m`).
    pub rows: Vec<BandRowJson>,
    /// The gap-unobserved mass over the window, beside the bands by ruling.
    pub gap_unobserved: Figure,
    pub gap_unobserved_events: u64,
}

/// One banded bucket row of the recompute register. A `holder` dim value is the
/// slash-joined triple `instance/dp_rank/group_idx` (e.g. `i03/0/0`).
#[derive(Serialize, ToSchema)]
pub(crate) struct BandRowJson {
    pub bucket: String,
    pub band: String,
    pub dims: Vec<DimValue>,
    pub events: u64,
    pub tokens: Figure,
}

/// The copies histogram — the replication panel's read.
///
/// Duplicated token-time per bucket per FIXED band (`k2`/`k34`/`k58`/`k9` — copies 2,
/// 3–4, 5–8, 9+), the members table, and the anchor's fleet scalars, in one response.
/// No coverage figure by statement: `emitted_to_ms` is the honest edge.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReplicationData {
    pub register: &'static str,
    pub never_summed_with: Vec<&'static str>,
    /// Token-milliseconds. The glass renders token-hours; the wire keeps the base unit.
    pub unit: &'static str,
    /// The served grain, in the one vocabulary every bucketed read speaks: `minute`, `hour`,
    /// `day`. An explicit ask is honored verbatim; `auto` ladders by span against this
    /// table's measured reach, and the consumer renders the buckets it got.
    pub grain: &'static str,
    /// The served bucket WIDTH in milliseconds (60,000 / 3,600,000 / 86,400,000).
    pub bucket_ms: i64,
    pub start_ms: f64,
    pub end_ms: f64,
    /// For a custom range: why this grain (`range × retention`); absent on the stops.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derived: Option<String>,
    /// The cut this response's `rows` state; absent = fleet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
    /// The selected cut's Σ duplicated token-ms over the window.
    pub window_dup_token_ms: f64,
    pub rows: Vec<CopiesBucketJson>,
    /// One row per workload with duplicated mass, plus the fleet row (key `fleet`).
    pub members: Vec<CopiesMemberJson>,
    /// Fleet scalars whatever the cut: the remedy split (a disjoint partition of the
    /// fleet's duplicated mass) and the mean-copies pair — mean = `copies_token_ms` /
    /// `window_held_token_ms`, exact because the numerator was summed from exact copy
    /// counts before banding.
    pub cross_engine_token_ms: f64,
    pub within_engine_token_ms: f64,
    /// The third remedy bucket: duplicated mass that exists only because a
    /// hybrid-attention model splits its layers into separate KV cache groups, so one
    /// prefix is resident in each group of ONE rank. **Architecture, never waste** — no
    /// routing or placement change removes it, and it must not be rendered as the engine
    /// placing redundantly. Zero on every single-group model, which is most of them.
    pub cache_group_token_ms: f64,
    pub copies_token_ms: f64,
    pub window_held_token_ms: f64,
    /// The producer's emission frontier; absent before its first applied tick.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emitted_to_ms: Option<f64>,
    /// What `cross_engine_token_ms` means when it is zero (spec 5.7).
    pub zero_basis: ZeroBasisJson,
}

/// What a cross-instance figure of zero means over the window's UTC days (spec 5.7). A
/// positive figure is its own evidence; a zero is a measured absence only when two or more
/// instances stored, every store carried a portable identity, and one key space covered them.
#[derive(Serialize, ToSchema)]
pub(crate) struct ZeroBasisJson {
    /// `positive`, `measured`, or the first condition that failed: `single_holder`,
    /// `not_comparable`, `split_key_space`, `partly_comparable`.
    pub meaning: &'static str,
    /// Instances that stored into their own cache.
    pub instances: u64,
    /// Their own stores.
    pub stores: u64,
    /// Of those, the stores carrying a `content_id`.
    pub comparable_stores: u64,
    /// Distinct key spaces among the runs that made the stores; the runs that declared no
    /// canary count as one.
    pub key_spaces: u64,
}

/// One bucket of the histogram, band keys in the console's own vocabulary.
#[derive(Serialize, ToSchema)]
pub(crate) struct CopiesBucketJson {
    /// The bucket's START instant, engine-domain ms (the width is the response-level
    /// `bucket_ms` — one field names a width, this one names an instant, and both say
    /// so rather than sharing a bare name).
    pub bucket_ms: i64,
    pub k2: f64,
    pub k34: f64,
    pub k58: f64,
    pub k9: f64,
}

/// One member row: share of the fleet's duplicated mass, and the band profile of the
/// member's own (four fractions summing to 1, all zero when it has none).
#[derive(Serialize, ToSchema)]
pub(crate) struct CopiesMemberJson {
    pub key: String,
    pub label: String,
    pub share: f64,
    pub profile: [f64; 4],
}

/// The whale panel — the heaviest duplicated identities.
///
/// **No coverage figure, by statement rather than omission**: the streamed producer's
/// integrals are exact for what its provable frontier has emitted, so the honest
/// uncertainty is `emitted_to_ms` — figures are complete through that instant and
/// unstated beyond it, and the glass renders that edge as the unproven edge.
#[derive(Serialize, ToSchema)]
pub(crate) struct WhalesData {
    pub register: &'static str,
    /// Both siblings: whale mass is the placement family's integral, cut by identity.
    pub never_summed_with: Vec<&'static str>,
    /// Token-milliseconds. The glass renders token-hours; the wire keeps the base unit.
    pub unit: &'static str,
    /// Which derivation produced these rows.
    pub source: &'static str,
    /// The window's days the register has stated. A day is stated once it settles, so the
    /// window's open days are not in these figures.
    pub days_stated: u32,
    /// Each stated day's cut: the identities a day keeps, heaviest by duplicated time first.
    pub cut: u32,
    /// Σ over the stated days of the identities with duplicated time that day. An identity
    /// duplicated on two days counts on each.
    pub duplicated_identity_days: u64,
    /// Σ duplicated token-milliseconds over EVERY identity on the stated days, returned or not.
    pub window_dup_token_ms: f64,
    /// Σ held token-milliseconds over every identity on the stated days — the share's
    /// denominator.
    pub window_held_token_ms: f64,
    pub rows: Vec<WhaleJson>,
}

/// One identity's window totals, fleet scope, ranked by `dup_token_ms`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WhaleJson {
    pub identity: String,
    /// The identity's token mass — max across its stores, the tree's node figure.
    pub n_tokens: i64,
    /// Token-milliseconds held at two or more simultaneous copies, over the stated days it was
    /// in the day's cut: exact for those days.
    pub dup_token_ms: f64,
    /// The most `dup_token_ms` falls short of the window's total: each stated day it was not in
    /// the cut adds that day's floor, the most an identity outside the cut duplicated. Zero
    /// when the identity was in every day's cut.
    pub dup_token_ms_bound: f64,
    /// Token-milliseconds held at any copy count, over the stated days it was in the cut.
    pub held_token_ms: f64,
    /// The stated days it was in the cut.
    pub days_in_cut: u32,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct FiguresData {
    pub register: &'static str,
    /// The grain the response was SERVED at (server-chosen granularity):
    /// `"hour"` or `"day"`. When the request omitted `grain` (or said `auto`), the
    /// server chose from the range's length and the raw span it actually holds — the
    /// consumer renders the buckets it got and never re-derives the tier from a
    /// client-side constant.
    pub grain: &'static str,
    /// The register this one may never be added to. On the response because the consumer is a
    /// separate repository and cannot read the register rule in `docs/read-api.md`.
    pub never_summed_with: &'static str,
    /// **Always an array, never a blended series**, and there is deliberately no `total` field
    /// spanning it — never-sum expressed as an absence.
    pub segments: Vec<Segment>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Segment {
    pub discriminator: String,
    pub semantics_version: String,
    pub window: WindowJson,
    /// `None` where this process cannot state figures for the generation — never an empty
    /// array, which would read as "looked and found nothing".
    pub figures: Option<Vec<Bucket>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

/// One time bucket of a segment's figures: the recompute register over that bucket,
/// grouped by the request's `dims`.
#[derive(Serialize, ToSchema)]
pub struct Bucket {
    /// The bucket's start as the engine rendered it: a UTC `YYYY-MM-DD` at day grain, a UTC
    /// `YYYY-MM-DD hh:mm:ss` at hour and minute grain.
    pub bucket: String,
    /// One entry per requested dimension, in the order `Dim` sorts; empty when none was
    /// asked for.
    pub dims: Vec<DimValue>,
    /// Store events in the bucket for this group — the count `tokens` was summed over.
    pub events: u64,
    /// Recompute mass in tokens, with the token-weighted coverage it was measured at.
    pub tokens: Figure,
}

/// One grouping dimension and the value this row holds on it.
#[derive(Serialize, ToSchema)]
pub struct DimValue {
    /// The dimension's name, from the closed set: `class`, `holder`, `instance`, `workload`,
    /// `model`, `tier`.
    pub dim: &'static str,
    /// The value on that dimension as the engine delivered it; a `holder` is the triple
    /// `instance`, `dp_rank`, `group_idx` joined by a tab — one value, not three.
    pub value: String,
}

// ===========================================================================================
// `/v1/holders`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct HoldersData {
    pub register: &'static str,
    /// The grain the response was SERVED at (server-chosen granularity):
    /// `"hour"` or `"day"`. When the request omitted `grain` (or said `auto`), the
    /// server chose from the range's length and the raw span it actually holds — the
    /// consumer renders the buckets it got and never re-derives the tier from a
    /// client-side constant.
    /// `None` only on a refusal answered before any read resolved one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grain: Option<&'static str>,
    /// `None` where the page could not be read — **never an empty array**, which a consumer that
    /// forgot to check `refused` renders as "found nothing". `Segment.figures` states the rule and
    /// this applies it: in a generated client the type is `T[] | null`, so forgetting to narrow is
    /// a compile error rather than a silently wrong panel.
    pub rows: Option<Vec<HolderRow>>,
    /// The denominator the shares were taken against, from the SAME read as the rows — so it goes
    /// absent with them. A zero here would be the naked number `Covered` exists to prevent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fleet_total: Option<Figure>,
    pub page: Page,
    /// **A bounded refusal, in the data position** (the read API's refusal rule). Present exactly when the rows above are
    /// absent — the two are one fact stated once each way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HolderRow {
    pub holder: HolderId,
    /// The rank's tap-process state, judged at READ time against real now — the roster's
    /// own `active`/`inactive` judgment denormalized per row.
    /// **State qualifies the rank now; `tokens.coverage` qualifies this row's figures over
    /// the window; neither implies the other** — a console must never synthesize this from
    /// coverage, so the row carries it.
    pub state: &'static str,
    /// Which kind of not-running, exactly as the roster states it — present when `state`
    /// is `inactive` (`stopped`, `silent`, `ungraded`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// Declared on the segment (the spec's workload clause). Absent reads as `undeclared` — the producer
    /// declared nothing, which is a different absence from a config-mapping miss.
    pub workload: Option<String>,
    /// **Observed, never declared.** Absent reads as `unobserved`, a different claim.
    pub model: Option<String>,
    pub events: u64,
    pub tokens: Figure,
    pub share_of_fleet_total: Figure,
}

/// One page's own accounting.
///
/// **`next_cursor` is `None` on the last page and only there.** At the v1 envelope it is almost
/// always `None` on the FIRST page too, because one page holds the table — which is exactly why
/// the goldens page at `limit = 1`: a path nobody exercises is a path that breaks silently when
/// the envelope lifts.
#[derive(Serialize, ToSchema)]
pub struct Page {
    /// The page size the request asked for, or the default — the bound the rows were cut at.
    pub limit: u32,
    /// How many rows this page carries. Zero on a refusal, where nothing was read.
    pub returned: usize,
    /// How many rows the filtered surface holds in total, where the surface can know cheaply.
    /// `None` where counting would cost a second read the page did not need.
    pub total_rows: Option<usize>,
    /// Opaque, and **bound to the stamp that minted it**. Presented against a newer tick it is
    /// refused with `cursor_stale` rather than silently answering about different rows.
    pub next_cursor: Option<String>,
}

// ===========================================================================================
// `/v1/coverage`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct CoverageData {
    pub rows: Vec<CoverageRow>,
}

/// One UTC day's observed coverage, for the fleet or for one instance (shape 3).
#[derive(Serialize, ToSchema)]
pub struct CoverageRow {
    /// The UTC day, `YYYY-MM-DD` as the engine rendered it.
    pub day: String,
    /// The instance this row is for; absent when the row is the fleet's figure for that day.
    pub instance: Option<String>,
    /// Observed milliseconds over the day, summed across the row's instances.
    pub covered_ms: f64,
    /// The denominator: the part of the day stated, through the covered edge while the day is
    /// open and the whole day once it settles, per instance, summed across the row's instances.
    pub window_ms: f64,
    /// `covered_ms / window_ms`, instance-time weighted — never a mean of per-instance
    /// fractions. Zero where nothing was observed, which carries no claim.
    pub fraction: f64,
    /// How many instances the row aggregates — the denominator a fleet figure is read
    /// against: 94 % over two instances is a different statement than over eighty.
    pub instances: u64,
    /// The basis, on the row: two figures at 80 % on different bases are not comparable.
    pub coverage_kind: &'static str,
    /// The SECOND basis: at what coverage this row's classified mass
    /// was observed — with both sums behind the fraction (`token_wsum` / `token_mass`)
    /// so a consumer merging days merges honestly; fractions do not average, the sums
    /// do. All three absent where the row has no classified mass: absent beats zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_wsum: Option<f64>,
    /// Σ tokens of the row's classified mass — the other half of the second basis. Absent
    /// with `token_wsum`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_mass: Option<f64>,
    /// `token_wsum / token_mass`. Absent where the row has no classified mass: absent beats
    /// zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_weighted_fraction: Option<f64>,
}

// ===========================================================================================
// `/v1/delivery`
// ===========================================================================================

/// The delivery ledger: of what the taps produced, what arrived — since install, never
/// windowed. The spec makes `segment_seq` zero-based and gap-free, so the three segment parts
/// partition `expected` exactly, and the tail past each tap's last word is outside the claim.
#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryData {
    pub fleet: DeliveryTotals,
    /// One row per producer run. A run whose every segment was reclaimed-and-declared still
    /// appears: "everything it produced was dropped, and it said so" is a statement.
    pub rows: Vec<DeliveryRow>,
    /// Arrivals with no sequence claim — coverage-indeterminate, counted so the rows above
    /// are legibly not the whole story when this is nonzero.
    pub orphan_batches: u64,
    /// The knowledge horizon, stated: segments past each tap's last word are invisible here.
    pub cannot_tell_you: &'static str,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryTotals {
    pub segments_expected: u64,
    pub segments_captured: u64,
    pub segments_declared_dropped: u64,
    pub segments_missing_unexplained: u64,
    /// `captured / expected`. Absent while nothing is expected — never a default to 1.0,
    /// which would render an empty install as perfect delivery.
    pub fraction: Option<f64>,
    pub records_seen: u64,
    pub records_dropped_at_source: u64,
    pub records_oversized: u64,
    /// Newest producer instant across the rows (wall domain). Absent before any arrival.
    pub as_of_ms: Option<f64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryRow {
    pub incarnation: String,
    /// Every instance this run's segments named, sorted; empty when none did.
    pub instances: Vec<String>,
    /// Wall domain. Absent for a run known only through a drop declaration.
    pub first_seen_ms: Option<f64>,
    pub last_seen_ms: Option<f64>,
    pub segments_expected: u64,
    pub segments_captured: u64,
    pub segments_declared_dropped: u64,
    pub segments_missing_unexplained: u64,
    pub records_seen: u64,
    pub records_dropped_at_source: u64,
    pub records_oversized: u64,
}

// ===========================================================================================
// `/v1/analyze`
// ===========================================================================================

/// The analyze-now acknowledgement. In-band honesty rather than a new cause: a process
/// that runs no loop says so here, and a 200 with `accepted: false` is a complete answer.
#[derive(Serialize, ToSchema)]
pub(crate) struct AnalyzeData {
    pub accepted: bool,
    /// What happens next, in a sentence — or why nothing will.
    pub note: &'static str,
}

// ===========================================================================================
// `/v1/annotations`
// ===========================================================================================

/// The declaration's acknowledgement. The declaration is its bucket object; the marker
/// appears when the next ingest cycle records it, and the note says so.
#[derive(Serialize, ToSchema)]
pub(crate) struct AnnotationData {
    pub recorded: bool,
    /// Whether the declaration reached its durable form — the bucket object replay
    /// recovers. Always `true` in an acknowledgement: a process with no bucket refuses the
    /// declaration instead.
    pub durable: bool,
    pub note: &'static str,
}

// ===========================================================================================
// `/v1/roster`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct RosterData {
    pub scope: ClipScope,
    pub cannot_tell_you: &'static str,
    /// `None` where the page could not be read — **never an empty array**, which a consumer that
    /// forgot to check `refused` renders as "found nothing". `Segment.figures` states the rule and
    /// this applies it: in a generated client the type is `T[] | null`, so forgetting to narrow is
    /// a compile error rather than a silently wrong panel.
    pub rows: Option<Vec<RunRow>>,
    /// Totals to the RUN count, never the holder count — and across a paged roster it is THIS
    /// page's, which is why [`RosterData::page`] is not optional. Absent with the rows:
    /// `producer_runs: 0` on a refusal is a claim about the FLEET, not about this page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_counts: Option<std::collections::BTreeMap<String, usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_runs: Option<usize>,
    pub page: Page,
    /// **A bounded refusal, in the data position** (the read API's refusal rule). Present exactly when the rows above are
    /// absent — the two are one fact stated once each way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ClipScope {
    pub kind: &'static str,
    pub margin_ms: f64,
    pub note: &'static str,
}

/// One node's INTERNAL duplication — the mass it held more than once inside itself.
///
/// **A different question from the fleet's copies histogram, not a slice of it.** Copies
/// spanning engines belong to no single node, so they are absent here by construction; the
/// fleet figure keeps that half. Summing this across instances therefore yields internal
/// duplication and nothing else, with no double count to correct for.
#[derive(Serialize, ToSchema)]
pub(crate) struct InstanceDuplicationData {
    pub register: &'static str,
    pub never_summed_with: &'static str,
    pub instance: String,
    /// The grain the rows were served at — `minute` from the minute cut inside the minute
    /// knob's reach, `hour` otherwise (a day ask reads hour buckets). Stated, so a 3 h
    /// panel knows its bars are minute-wide.
    pub grain: &'static str,
    /// The bucket width in ms, the grain's own.
    pub bucket_ms: i64,
    pub start_ms: f64,
    pub end_ms: f64,
    /// Duplicated token-time across this node's DATA-PARALLEL RANKS. **The half with a
    /// lever**: two ranks are two independent caches on one machine, so the same prefix in
    /// both was prefilled twice and stored twice. vLLM's own DP balancer scores engines by
    /// load with no prefix term, so this happens by default; a gateway that sets
    /// `data_parallel_rank` by prefix affinity is what removes it.
    pub within_ranks_token_ms: f64,
    /// Duplicated token-time across one rank's KV CACHE GROUPS. **Architecture, not waste**
    /// — a hybrid-attention model partitions its layers and each group caches the prefix
    /// independently. No routing or placement change removes it, and it is reported beside
    /// the other so a hybrid node does not appear to duplicate less than it does.
    pub cache_groups_token_ms: f64,
    /// Σ copies × token-time over this node's internal duplication.
    pub copies_token_ms: f64,
    /// **The denominator**: all token-time this node held over the window, held-once
    /// included. `within_ranks_token_ms / held_token_ms` is the share of what this node
    /// holds that it holds twice inside itself — the figure an operator can weigh. Without
    /// it the two measures above are numerators, and their ratio to each other states the
    /// duplication's character while saying nothing about its size.
    pub held_token_ms: f64,
    /// Per `(bucket, band)` rows — the shape a sparkline reads. Empty is a MEASURED zero:
    /// the read looked at this node and found no internal duplication.
    pub rows: Vec<InstanceDupRowJson>,
    pub cannot_tell_you: &'static str,
}

/// One bucket of one node's internal duplication.
#[derive(Serialize, ToSchema)]
pub(crate) struct InstanceDupRowJson {
    /// The bucket's START instant; the width is the document's `bucket_ms`.
    pub bucket_ms: f64,
    /// A `COPY_BANDS` value. Band `1` is held-once mass and never appears here.
    pub copies_band: String,
    /// `within` (across ranks) or `group` (across cache groups). Never `cross`.
    pub locality: String,
    pub token_ms: f64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct RunRow {
    pub instance: String,
    pub incarnation: String,
    pub first_seen_ms: f64,
    pub last_seen_ms: f64,
    pub stop_observed: bool,
    pub model: Option<String>,
    pub engine_version: Option<String>,
    /// **The slot's current occupant.** `instance` names a slot the OPERATOR chose — the
    /// tap takes it as a command-line label, so it is config and never discovered, and a
    /// reader may infer nothing about topology from it. This is the chart-relayed pod
    /// name filling that slot now (observed over declared, the dashboard's slot rule). The two have different
    /// lifetimes: a slot outlives its pods, and a pod name that changes every few minutes
    /// is a naming choice the operator can see and fix. Absent reads as unobserved — a
    /// deployment relaying no pod identity has said nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod: Option<String>,
    /// The server's judgment, made at read time against real now: `active` or `inactive`.
    /// The console paints from this field alone — the enum is contract-closed, and every
    /// threshold behind it is server-side.
    pub state: &'static str,
    /// Which kind of not-running — present exactly when `state` is `inactive`. `stopped`:
    /// an announced departure (a rolling deploy looks like this). `silent`: past the
    /// threshold with no stop, the alarming one. `ungraded`: no declaration observed, a
    /// coverage gap rather than a health claim. How alarming each looks is the console's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// Evidence for the hover, present when `reason` is `silent`: real now minus the last
    /// record heard — the sentence "silent 42m (threshold 30m)" in numbers, never prose.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silent_for_ms: Option<f64>,
    /// The line this run was judged by: the larger declared bound widened by the platform
    /// grace (30 min). Absent when nothing was declared (`reason` = `ungraded`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold_ms: Option<f64>,
}

// ===========================================================================================
// `/v1/markers`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct MarkersData {
    pub markers: Vec<MarkerJson>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct MarkerJson {
    pub id: String,
    pub at_ms: f64,
    pub kind: String,
    pub provenance: String,
    pub label: String,
    pub from_value: Option<String>,
    pub to_value: Option<String>,
    pub source: String,
    /// Decided server-side: annotation or seam. A consumer getting this wrong would blend two
    /// generations or refuse to compare across a deploy.
    pub breaks: bool,
    /// **A baseline names a generation's START, not a transition** — an origin rather than a
    /// seam. Carried explicitly so the console renders it without inferring from a null.
    pub origin: bool,
}

// ===========================================================================================
// `/v1/days/{day}`
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct DayData {
    pub day: String,
    pub by_class: Vec<ClassMass>,
    /// `None` where the holders page could not be read. **Scoped to this SECTION**, which is why
    /// the refusal below is named for it: the day's class breakdown and coverage are unpaged and
    /// were read perfectly well, so a root-level refusal would blank content that is fine.
    pub holders: Option<Vec<DayHolder>>,
    /// **The day's holder list is PAGED, and says so**: the total and the cursor mark the cut.
    /// A silent cut is the failure the whole `Covered` discipline exists to prevent one axis
    /// over: a number that does not state its own basis.
    pub holders_page: Page,
    /// The holders section's own bounded refusal — named for its scope, not for the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holders_refused: Option<Refusal>,
    pub coverage: Option<Figure>,
    /// Carried rather than inferred from an empty drill: a retention bound is not an absence of
    /// activity.
    pub evidence_available: bool,
    pub evidence_bound: ReachJson,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ClassMass {
    pub class: String,
    pub tokens: Figure,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DayHolder {
    pub holder: HolderId,
    pub tokens: Figure,
    pub share_of_fleet_total: Figure,
}

// ===========================================================================================
// `/v1/evidence` — level 2.
// ===========================================================================================

#[derive(Serialize, ToSchema)]
pub(crate) struct EvidenceData {
    pub reach: ReachJson,
    /// Where the rows came from: `ring` (the holder's newest stores, as repeat compute
    /// classified them) or `log` (the block re-derived from the log partitioned by block).
    pub read_from: &'static str,
    /// `None` where the subject could not be read; empty only when the horizon covers it and
    /// there was genuinely nothing. The type carries the distinction, so no prose has to.
    pub records: Option<Vec<EvidenceRecord>>,
    /// More records exist behind this page. **`page.next_cursor` is how to reach them** — a
    /// truncation flag without a continuation turns a bound into a wall.
    pub truncated: bool,
    pub page: Page,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EvidenceRecord {
    pub at_ms: f64,
    pub block_id: String,
    pub class: String,
    pub n_tokens: u64,
    /// ms since this identity's most recent OBSERVED prior close on this holder, per event.
    /// **Serialized as `null`, never skipped**:
    /// "no observed prior close" is a claim every row states, not a field that goes missing.
    pub gap_ms: Option<f64>,
    /// The epoch scoping the block's identity; `null` where the producer declared none.
    pub key_epoch: Option<u32>,
    pub coverage: f64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ReachJson {
    pub covers: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_from_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_from_day: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_days: Option<u32>,
    /// Which of the two readings this is: older than the oldest record, or no records at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

// ===========================================================================================
// `/v1/health`
// ===========================================================================================

/// `/v1/health`'s payload — the METER's health: ingest accounting, coverage per instance,
/// the roster's populations and the degradation ledger over the window.
#[derive(Serialize, ToSchema)]
pub struct HealthData {
    /// What the ingest ledger says right now, with the loop's own hints beside it.
    pub ingest: IngestJson,
    /// Observed coverage per UTC day per instance over the window — the meter's trust figure.
    pub coverage_by_instance: Vec<CoverageRow>,
    /// How many producer runs the roster holds over the window; `run_states` totals to this,
    /// never to the holder count.
    pub producer_runs: usize,
    /// Runs per staleness key: `active`, or the inactive reason (`stopped`, `silent`,
    /// `ungraded`) — the finest wire grain, so a client sums and never interprets.
    pub run_states: std::collections::BTreeMap<String, usize>,
    /// The roster's runs split on whether their model was observed.
    pub models: ModelCounts,
    /// Every tap version observed on this fleet's arrivals, distinct and sorted. A fleet
    /// fact, deliberately not window-scoped.
    pub agent_versions: Vec<String>,
    /// The degradation ledger's entries whose extent touches the window, each with its three
    /// clauses.
    pub degradations: Vec<DegradationJson>,
    /// Every installed derivation module and what the executor records about its output in
    /// the window. A figure a disabled or rebuilding module feeds is not a zero: read it
    /// against this.
    pub modules: Vec<ModuleJson>,
}

/// One public derivation module's standing. The core registers (repeat compute, placement,
/// coverage, the roster, the tag cells) always run and are not listed.
#[derive(Serialize, ToSchema)]
pub struct ModuleJson {
    /// The module's name, as its definition declares it.
    pub name: String,
    /// Whether the deployment's configuration enables it.
    pub enabled: bool,
    /// The revision the standing was read at, or null when a concurrent write moved it.
    pub revision: Option<u64>,
    /// The first settled day, UTC `YYYY-MM-DD`, its output is stated for: the first day that
    /// settled while it was enabled. Before it the module stated nothing, which a reader must
    /// not take for zero. Null until a day settles with it enabled, and for a disabled module.
    pub stated_from_day: Option<String>,
    /// The last settled day, UTC `YYYY-MM-DD`, through which its output is stated; null with
    /// `stated_from_day`.
    pub stated_through_day: Option<String>,
}

/// The roster's runs, split on whether the served model is known.
#[derive(Serialize, ToSchema)]
pub struct ModelCounts {
    /// Runs whose model was observed on the wire.
    pub observed: u64,
    /// Runs with no model observed. Reads as `undeclared`, a different claim from
    /// `unattributed`.
    pub undeclared: u64,
}

/// The ingest ledger's current state — the strip's freshness source — with the loop's
/// daemon-native hints beside it.
#[derive(Serialize, ToSchema)]
pub struct IngestJson {
    /// The segment file the newest batch came from; absent when nothing has ever been
    /// ingested.
    pub last_segment: Option<String>,
    /// When the newest batch was inserted, OUR wall clock — never the producer's. Absent
    /// with `last_segment`.
    pub last_segment_at_ms: Option<f64>,
    /// Records the newest batch carried. Absent with `last_segment`.
    pub last_segment_records: Option<u64>,
    /// The engine-domain extent of the newest batch's CACHE facts; absent where it carried
    /// none — the ordinary case for a lifecycle-only segment.
    pub last_segment_span: Option<SpanJson>,
    /// Wall domain on both sides: our insert stamp against our own clock.
    pub lag_ms: Option<f64>,
    /// Batches ingested since install. The ledger is forever-sized.
    pub batches_total: u64,
    /// The odometer: records received since install.
    pub events_since_install: u64,
    /// Records since install whose `kind` the reader did not recognize — a rising count is
    /// the shape a producer-side change takes, and it is invisible in the totals.
    pub unknown_kinds_total: u64,
    /// Daemon-native. **Absent rather than faked** when the loop has published nothing.
    pub next_tick_at_ms: Option<f64>,
    /// Files listed and not yet ingested. Absent when nothing measured it — which is always,
    /// today: the shell publishes `None` by design, absent over invented.
    pub backlog_files: Option<u64>,
    /// Their bytes. Absent with `backlog_files`.
    pub backlog_bytes: Option<u64>,
    /// Whether the disk-pressure ladder is refusing intake right now. Absent before the
    /// loop's first publish.
    pub refusing: Option<bool>,
}

/// An engine-domain span, with the clock domain stated on it.
#[derive(Serialize, ToSchema)]
pub struct SpanJson {
    /// The span's first instant, engine-domain ms.
    pub start_ms: f64,
    /// The span's last instant, engine-domain ms.
    pub end_ms: f64,
    /// Cache facts only — a span across lifecycle and cache `at_ms` would straddle two clocks.
    pub domain: &'static str,
}

/// A degradation notice, with all three clauses the degradation contract requires — scope,
/// unaffected, remedy. **An entry missing `unaffected` or `remedy`
/// is a schema violation** rather than a sparse response.
#[derive(Serialize, ToSchema)]
pub struct DegradationJson {
    /// What the entry covers, in words: `run <incarnation> (<instances>)`, `instance <id>`, or
    /// `holder <instance>/<rank>/<group>`.
    pub scope: String,
    /// The cause class token: `sequence_gap`, `declared_drop`, `incarnation_boundary`,
    /// `identity_refused`, `drop_window` or `recovered_truncation`.
    pub class: &'static str,
    /// The register the cause forces on figures it touches: `lower_bound`, `indeterminate`
    /// or `uncovered`.
    pub register: &'static str,
    /// How many incidents the entry stands for — 1 for one observation, more once
    /// hull-compacted — so a summarized ledger cannot present as a quiet one.
    pub incidents: u64,
    /// The first clause: what is degraded, as a sentence carrying the cause's own numbers.
    pub degraded: String,
    /// The second clause: what this entry does NOT impair, so a reader discounts nothing
    /// else.
    pub unaffected: &'static str,
    /// The third clause: what an operator does about it, or why no action closes it.
    pub remedy: &'static str,
}

// ===========================================================================================
// `/v1/blocks`, `/v1/blocks/{block_id}/holders`, `/v1/holders/{i}/{d}/{g}/timeline`,
// `/v1/markers/{id}` — shapes 4, 5 and 7's detail.
// ===========================================================================================

/// The blocks by stored mass, per UTC day: each day's heaviest blocks from a summary whose memory
/// does not grow with the day, every figure a bound with its error stated.
#[derive(Serialize, ToSchema)]
pub(crate) struct BlocksData {
    /// `stored`: the tokens stores wrote. Never placement's held mass, and never joined with
    /// it.
    pub register: &'static str,
    /// The window's days, latest first; a day with no stores is absent.
    pub days: Vec<BlockDayJson>,
}

/// One UTC day's ranking and the summary it is stated under.
#[derive(Serialize, ToSchema)]
pub(crate) struct BlockDayJson {
    /// The day, `YYYY-MM-DD`.
    pub day: String,
    /// Tokens every store wrote on the day, at the day's token-weighted coverage.
    pub tokens: Figure,
    /// The blocks the summary monitors at once.
    pub capacity: u32,
    /// The blocks the day ranks; beyond them the ranking says nothing.
    pub cut: u32,
    /// What any block's figure may overstate by: the day's tokens over the capacity. With
    /// fewer distinct blocks than the capacity, every figure is exact.
    pub error_bound: f64,
    /// The ranked blocks, heaviest upper bound first, at most the request's limit.
    pub blocks: Vec<BlockRow>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BlockRow {
    pub block_id: String,
    /// What the block's stored tokens are at least.
    pub tokens_at_least: i64,
    /// What they are at most.
    pub tokens_at_most: i64,
    /// Whether the block is certainly in the day's true top `cut`.
    pub certain: bool,
    /// **Holders that STORED it, never holders it was resident on at once**, counted since the
    /// summary began monitoring it: a lower bound.
    pub holders_at_least: u32,
}

// ===========================================================================================
// `/v1/tags`
// ===========================================================================================

/// The declared tags over the window, grouped byte for byte and never interpreted, each with its
/// exposure and its two figures, and the window's coverage beside them.
#[derive(Serialize, ToSchema)]
pub(crate) struct TagsData {
    /// Where exposure stops, epoch milliseconds: the window's end, or the classified edge where
    /// that is earlier. Absent when nothing has been classified, and then `tags` is empty.
    pub through_ms: Option<f64>,
    /// The fleet's observed coverage over the window's days, instance-time weighted: every
    /// figure below is over what was observed, never extrapolated.
    pub coverage: TagCoverageJson,
    /// Each tag declared over the window, in the labels' byte order.
    pub tags: Vec<TagRowJson>,
}

/// Coverage over the window's days, with both sums behind the fraction.
#[derive(Serialize, ToSchema)]
pub(crate) struct TagCoverageJson {
    pub covered_ms: f64,
    pub window_ms: f64,
    /// `covered_ms / window_ms`; absent where nothing was observed.
    pub fraction: Option<f64>,
}

/// One tag over the window.
#[derive(Serialize, ToSchema)]
pub(crate) struct TagRowJson {
    /// The label, as declared.
    pub tag: String,
    /// Identities that carried the tag inside the window: the sampling unit of both intervals.
    pub identities: usize,
    /// Their exposure: the minutes each carried the tag, summed.
    pub identity_minutes: f64,
    /// Stores attributed to the tag: their instants, widened by the skew allowance, lie wholly
    /// inside a declared interval.
    pub stores: i64,
    /// Those stores' tokens.
    pub prefill_tokens: i64,
    /// The tokens of the recompute register among them (`ETR` and `post_clear`; `RE` is not a
    /// refetch).
    pub refetch_tokens: i64,
    /// The tokens of indeterminate stores among them, in neither figure, so taint is visible.
    pub indeterminate_tokens: i64,
    /// Stores at an interval's edge, attributed to no tag.
    pub ambiguous_stores: i64,
    /// The primary figure: refetch tokens per identity-minute of exposure. Exposure does not
    /// move with the policy, so neither does the denominator.
    pub refetch_per_identity_minute: RatioJson,
    /// The secondary figure: refetch tokens per stored token. Its bias is upward under a policy
    /// that cuts refetch, since a treated identity stores less, so it understates a gain; under
    /// `reuse_reporting: unlabelled` stored tokens also count reuse announcements.
    pub refetch_per_stored_token: RatioJson,
}

/// A ratio of sums over identities, with its 90 % interval: the ratio estimator, its variance
/// linearized over identities.
#[derive(Serialize, ToSchema)]
pub(crate) struct RatioJson {
    /// Absent where the denominator is zero.
    pub value: Option<f64>,
    /// The interval's lower end; absent with fewer than two identities.
    pub low_90: Option<f64>,
    /// The interval's upper end; absent with fewer than two identities.
    pub high_90: Option<f64>,
}

/// Which holders stored one block, and when. **Event grain, so rows carry `n_tokens` beside their
/// own stamped `coverage`** rather than a computed figure — the pattern `EvidenceRecord` set: a
/// single row's mass is a column the classification stated, not an aggregate this layer derived.
#[derive(Serialize, ToSchema)]
pub(crate) struct BlockHoldersData {
    pub block_id: String,
    pub bound: ReachJson,
    /// Where the rows came from: `ring` (the holder's newest stores, as repeat compute
    /// classified them) or `log` (the block re-derived from the log partitioned by block).
    pub read_from: &'static str,
    /// `None` where the page could not be read — **never an empty array**, which a consumer that
    /// forgot to check `refused` renders as "found nothing". `Segment.figures` states the rule and
    /// this applies it: in a generated client the type is `T[] | null`, so forgetting to narrow is
    /// a compile error rather than a silently wrong panel.
    pub rows: Option<Vec<BlockHolderRow>>,
    pub page: Page,
    /// **A bounded refusal, in the data position** (the read API's refusal rule). Present exactly when the rows above are
    /// absent — the two are one fact stated once each way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct BlockHolderRow {
    pub holder: HolderId,
    pub at_ms: f64,
    pub class: String,
    pub n_tokens: u64,
}

/// One holder's store timeline — shape 5, the "lifecycle of the bleed".
#[derive(Serialize, ToSchema)]
pub(crate) struct TimelineData {
    pub holder: HolderId,
    pub bound: ReachJson,
    /// Where the rows came from: `ring` (the holder's newest stores, as repeat compute
    /// classified them) or `log` (the block re-derived from the log partitioned by block).
    pub read_from: &'static str,
    /// `None` where the page could not be read — **never an empty array**, which a consumer that
    /// forgot to check `refused` renders as "found nothing". `Segment.figures` states the rule and
    /// this applies it: in a generated client the type is `T[] | null`, so forgetting to narrow is
    /// a compile error rather than a silently wrong panel.
    pub rows: Option<Vec<TimelineRow>>,
    pub page: Page,
    /// **A bounded refusal, in the data position** (the read API's refusal rule). Present exactly when the rows above are
    /// absent — the two are one fact stated once each way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refused: Option<Refusal>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TimelineRow {
    pub at_ms: f64,
    pub block_id: String,
    pub class: String,
    pub n_tokens: u64,
    /// ms since this identity's most recent observed prior close on this holder — the same
    /// per-event value `/v1/evidence` serves. `null` = no observed prior close.
    pub gap_ms: Option<f64>,
    /// The epoch scoping the block's identity; `null` where the producer declared none.
    pub key_epoch: Option<u32>,
}

/// One discontinuity marker, reached by clicking its dagger.
#[derive(Serialize, ToSchema)]
pub(crate) struct MarkerData {
    pub marker: MarkerJson,
}

// ===========================================================================================
// `GET /v1/events` — the SSE payloads.
//
// **These are types rather than inline `json!` for the reason the response bodies are.** Absent
// from the document, the console would hand-write these three shapes from read-api section 5's
// prose — making the stream the ONE part of the seam a regenerated client cannot check, and
// therefore the one part a generation canary is blind to. Rename a field on `tick_applied` and
// the slide-forward breaks silently.
//
// The framing (`id:`/`event:`/`data:`, the 20-second heartbeat, `Last-Event-ID`) stays prose in
// read-api section 5 — OpenAPI can say `text/event-stream` but not describe a frame grammar. What it CAN carry is
// the payload of each `data:` line, which is the part that has fields to rename.
// ===========================================================================================

/// A tick was applied: the classified day and the meaning it was computed under.
#[derive(Serialize, ToSchema)]
pub(crate) struct TickAppliedEvent {
    /// The UTC day of the classified edge. **What tells a console its figures are stale and
    /// which window to refetch.**
    pub classified_day: String,
    pub window: WindowJson,
    /// **Carried so a parameter change arriving mid-session is visible as a seam** rather than as
    /// figures that quietly moved.
    pub discriminator: String,
    pub semantics_version: String,
    pub at_ms: f64,
}

/// A batch was ingested. **Wall-clock facts, and they stay wall-clock facts**: these
/// feed the dark status strip, and nothing here carries coverage because none of it is a
/// measurement of the fleet.
#[derive(Serialize, ToSchema)]
pub(crate) struct IngestBatchEvent {
    pub segment: String,
    pub records: u64,
    pub span: SpanJson,
    pub at_ms: f64,
}

/// The stream cannot replay the gap. **A resync is an EVENT, not an error** — the client stays
/// connected and refetches, because disconnecting would turn a bounded buffer gap into an outage
/// on the surface whose whole job is saying when something changed.
#[derive(Serialize, ToSchema)]
pub(crate) struct ResyncEvent {
    pub reason: String,
    /// The stream's current epoch, minted per process. A client that sees a new one knows the
    /// analyzer restarted and its sessions died at the same instant.
    pub epoch: String,
}
/// Session lifecycle is intentionally outside the data stamp envelope.
#[derive(Serialize, ToSchema)]
pub(crate) struct SessionData {
    pub session: SessionState,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SessionState {
    Created,
    Destroyed,
}
