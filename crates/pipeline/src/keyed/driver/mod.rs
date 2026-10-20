// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed operators over the input log: one [`Source`], one [`Intake`], repeat compute and
//! placement fed from one read of each batch per tick. Repeat compute classifies under the
//! window's context (the completeness model from the segments' arrivals, the run-scoped model
//! relay); placement folds up to the watermark and emits behind
//! it. What waits for the watermark is a batch reference in the intake, never its records.

use super::held::{Batch, Held};
use super::intake::Intake;
use super::placement::{PlacementCompute, PlacementTick};
use super::repeat::{RepeatCompute, RepeatContext};
use super::source::{Position, Source};
use super::state::KeyedState;
use super::store::{Seeds, Store};
use crate::Error;
use analyzer_analysis::class_band::ClassBandCells;
use analyzer_analysis::coverage_sums::CoverageSums;
use analyzer_analysis::dup_cells::{DupCells, IdentityState};
use analyzer_analysis::emitted::{Cells, Meaning};
use analyzer_analysis::instance_spans::InstanceSpans;
use analyzer_analysis::module::{DayPartitioned, Persisted, Row, Table};
use analyzer_analysis::restated::{Family, Outcome};
use analyzer_analysis::{Discriminator, Params, Window};

mod correction;

/// The blocks a day's summary of the blocks by stored mass monitors, unless set: exact while a
/// day stores fewer distinct blocks, each figure within the day's mass over it beyond.
pub const BLOCK_CAPACITY: usize = 65_536;
/// The blocks a day publishes, unless set.
pub const BLOCK_CUT: usize = 1_000;

/// The instant the log's records are complete through.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Watermark {
    /// The source's provable watermark: every live run's segments delivered without a gap.
    Provable,
    /// A stated instant: a settled log, whose every record has been committed.
    At(f64),
    /// The source's provable watermark where any run has delivered a sequenced segment, else
    /// the stated instant: a log whose carriers claim no sequence (a corpus, a harness, an
    /// orphan-only fleet) is settled as committed, through its newest record.
    ProvableOr(f64),
}

/// Records that arrived behind an operator's edge, and what its correction path did with them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Late {
    /// Records that arrived behind the edge.
    pub records: u64,
    /// Identities re-derived from the log.
    pub corrected: u64,
    /// Scope closers among them: a closer reaches every block its holder held, so no one
    /// identity's re-derivation corrects it. Counted, the span it touches uncorrected.
    pub closers: u64,
    /// Records of days no longer open to change: the cells of those days are no longer held,
    /// so a correction there would publish a difference as a whole value. Counted, and left
    /// uncorrected.
    pub too_late: u64,
}

/// A correction path: records behind an operator's edge in, the identities it re-derived and
/// the scope closers it counted out.
type Correct<D> = fn(
    &mut D,
    &analyzer_lake::Lake,
    &Params,
    Vec<analyzer_analysis::events::Event>,
) -> Result<(u64, u64), Error>;

/// What one tick produced.
#[derive(Debug)]
pub struct TickRun {
    /// The class and band cells of the stores classified.
    pub cells: ClassBandCells,
    /// What placement folded and emitted.
    pub placement: PlacementTick,
    /// Records that arrived behind the classified edge, and what repeat compute's correction
    /// path did with them.
    pub late: Late,
    /// Records that arrived behind placement's data edge, and what its correction path did.
    pub placement_late: Late,
    /// Sources whose arrivals reached behind the stated coverage edge, a new source's first
    /// day included: their stretch stated again from their heartbeats.
    pub coverage_restated: u64,
    /// Days this tick settled, and settled days a correction restated: what the settled-day
    /// hook states its day-grain outputs for.
    pub settled_days: Vec<i64>,
    /// The edge classified through, if anything has been.
    pub classified_to_ms: Option<f64>,
    /// The sampled audit of the stores classified.
    pub audit: super::audit::Audit,
    /// The evict audit of the tick's records on the sampled engine ids.
    pub evicts: analyzer_analysis::differential::AuditOutcome,
    /// Delivered batches with records still to fold.
    pub pending: usize,
}

/// The keyed operators driven by a source: repeat compute, placement, the intake they share,
/// and the cells they hold.
pub struct Driver<P> {
    repeat: RepeatCompute,
    placement: PlacementCompute<P>,
    intake: Intake,
    classes: Held<Cells>,
    placed: Held<DupCells>,
    covered: Held<CoverageSums>,
    roster: super::roster::RosterFeed,
    /// The sources coverage states, on their own clock.
    coverage: super::coverage::CoverageFeed,
    blocks: analyzer_analysis::block_mass::BlockMass,
    /// The whale register: each settled day's cut and totals.
    whales: analyzer_analysis::whale_days::WhaleDays,
    ring: super::ring::HolderRing,
    tags: super::tags::TagFeed,
    tag_cells: Held<analyzer_analysis::tags::TagCells>,
    /// What the correction paths restated, per hour and family.
    restated: Held<analyzer_analysis::restated::Restated>,
    /// The pair basis: each instance's observed spans per day, stated with coverage.
    spans: Held<analyzer_analysis::instance_spans::InstanceSpans>,
    /// The rings that changed since the last save, as they stood when drained.
    rings: Vec<(
        analyzer_analysis::intervals::Holder,
        Vec<analyzer_analysis::emitted::ClassifiedFact>,
    )>,
    /// The days of the blocks by stored mass drained since the last save, their summaries' bytes.
    block_days: Vec<(i64, Vec<u8>)>,
    /// The whale days drained since the last save, their bytes.
    whale_days: Vec<(i64, Vec<u8>)>,
    layout: super::spine::SpineLayout,
    /// The log objects written since the last save: the manifest names them at the save.
    written: super::spine::Written,
    /// The roster's changed runs, drained with the batch and written at the next save.
    roster_runs: Vec<(String, analyzer_analysis::roster::Run)>,
    /// Through where coverage is stated: settled, every source's tail attested past it.
    covered_to_ms: Option<f64>,
    /// Where the sampled audit stands.
    audit: super::audit::AuditEdge,
    /// The evict audit over the sampled engine ids: the latest store on each, in memory only,
    /// so a start grades the evicts of earlier stores `unjoined`.
    evicts: analyzer_analysis::differential::EvictAudit,
    /// One engine id in this many is in the evict audit's sample.
    evict_sample: u64,
    /// The meaning the ticks classify under, from the first tick's parameters.
    meaning: Option<Meaning>,
    /// How long past a day's end every edge must be before the day is settled.
    settle_delay_ms: f64,
    /// The last day settled.
    settled_through_day: Option<i64>,
    /// The days settled or restated since the last drain, with their coverage.
    due: Vec<super::SettledDay>,
    /// Every meaning's settled days, this one's included.
    generations: Vec<super::store::Generation>,
    /// Each enabled public module's first settled day.
    module_spans: std::collections::BTreeMap<String, i64>,
    /// Settled days a correction restated since the last tick's report.
    restated_days: std::collections::BTreeSet<i64>,
    /// The first day still open to change: the held days before it retire at the next save.
    open_from_day: Option<i64>,
    /// The input position the classified stores were derived at, for the tick in progress:
    /// what a correction re-derives the old side under. Transient.
    model_through: Option<u64>,
    /// The identities a correction path re-derived in the tick in progress: the taint pass
    /// leaves the late-record path's to it, and the audit samples none of them. Transient.
    corrected_identities: std::collections::BTreeSet<String>,
    /// The identities placement's correction path re-derived in the tick in progress, which
    /// re-taint leaves to it. Transient.
    placement_corrected: std::collections::BTreeSet<String>,
    /// Where this tick's batches changed the model behind the edge, per identity: re-taint's
    /// reach, which a correction of the same identity starts no later than. Transient.
    reach: Retainted,
}

/// Each identity re-taint reaches, from the instant its re-derivation starts: by its classes,
/// a changed drop window's stores or a moved silence's blocks, and by its placement, a moved
/// silence's blocks.
#[derive(Debug, Default)]
struct Retainted {
    classes: std::collections::BTreeMap<String, f64>,
    placement: std::collections::BTreeMap<String, f64>,
}

impl<P> Driver<P>
where
    P: KeyedState<String, IdentityState> + Send,
{
    /// A driver over `repeat` and `placement`, from the start of the log.
    #[must_use]
    pub fn new(repeat: RepeatCompute, placement: PlacementCompute<P>) -> Self {
        Self {
            repeat,
            placement,
            intake: Intake::default(),
            classes: Held::default(),
            placed: Held::default(),
            covered: Held::default(),
            roster: super::roster::RosterFeed::default(),
            coverage: super::coverage::CoverageFeed::default(),
            blocks: analyzer_analysis::block_mass::BlockMass::new(BLOCK_CAPACITY, BLOCK_CUT),
            whales: analyzer_analysis::whale_days::WhaleDays::default(),
            whale_days: Vec::new(),
            ring: super::ring::HolderRing::new(super::ring::RING_CAPACITY),
            rings: Vec::new(),
            tags: super::tags::TagFeed::default(),
            tag_cells: Held::default(),
            restated: Held::default(),
            spans: Held::default(),
            block_days: Vec::new(),
            layout: super::spine::SpineLayout::default(),
            written: super::spine::Written::default(),
            roster_runs: Vec::new(),
            covered_to_ms: None,
            audit: super::audit::AuditEdge::default(),
            evicts: analyzer_analysis::differential::EvictAudit::default(),
            evict_sample: super::audit::EVICT_SAMPLE_MODULUS,
            open_from_day: None,
            model_through: None,
            corrected_identities: std::collections::BTreeSet::new(),
            placement_corrected: std::collections::BTreeSet::new(),
            reach: Retainted::default(),
            meaning: None,
            settle_delay_ms: analyzer_analysis::staleness::STALENESS_GRACE_MS,
            settled_through_day: None,
            due: Vec::new(),
            generations: Vec::new(),
            module_spans: std::collections::BTreeMap::new(),
            restated_days: std::collections::BTreeSet::new(),
        }
    }

    /// How long past a day's end every edge must be before the day settles: the staleness
    /// grace unless set, so a run silent inside the grace can still land its records first.
    #[must_use]
    pub fn with_settle_delay(mut self, delay_ms: f64) -> Self {
        self.settle_delay_ms = delay_ms;
        self
    }

    /// The last day settled.
    #[must_use]
    pub fn settled_through_day(&self) -> Option<i64> {
        self.settled_through_day
    }

    /// Every meaning's settled days.
    #[must_use]
    pub fn generations(&self) -> &[super::store::Generation] {
        &self.generations
    }

    /// Record which public modules state `settled`, the days this tick settled: an enabled
    /// module states from the first of them on, and a module not `enabled` loses its span, so
    /// enabling it again starts a new one. Its output before that day was never stated.
    pub fn state_modules<'n>(
        &mut self,
        enabled: impl IntoIterator<Item = &'n str>,
        settled: &[i64],
    ) {
        let enabled: std::collections::BTreeSet<&str> = enabled.into_iter().collect();
        self.module_spans
            .retain(|name, _| enabled.contains(name.as_str()));
        if let Some(&first) = settled.iter().min() {
            for name in enabled {
                self.module_spans.entry(name.to_string()).or_insert(first);
            }
        }
    }

    /// Each enabled public module's first settled day.
    #[must_use]
    pub fn module_spans(&self) -> &std::collections::BTreeMap<String, i64> {
        &self.module_spans
    }

    /// The blocks by stored mass: a summary of `capacity` blocks a day, publishing the top `cut`.
    /// [`BLOCK_CAPACITY`] and [`BLOCK_CUT`] unless set. Days a store restored keep their own
    /// capacity.
    #[must_use]
    pub fn with_block_mass(mut self, capacity: usize, cut: usize) -> Self {
        self.blocks = std::mem::take(&mut self.blocks).reconfigured(capacity, cut);
        self
    }

    /// The blocks by stored mass.
    #[must_use]
    pub fn blocks(&self) -> &analyzer_analysis::block_mass::BlockMass {
        &self.blocks
    }

    /// The held pair basis: each instance's observed spans per day.
    #[must_use]
    pub fn pair_basis(&self) -> &analyzer_analysis::instance_spans::InstanceSpans {
        self.spans.cells()
    }

    /// The held restatement cells: what the correction paths restated, per hour and family.
    #[must_use]
    pub fn restated(&self) -> &analyzer_analysis::restated::Restated {
        self.restated.cells()
    }

    /// The held tag cells.
    #[must_use]
    pub fn tag_cells(&self) -> &analyzer_analysis::tags::TagCells {
        self.tag_cells.cells()
    }

    /// The declared tag intervals as they stand.
    #[must_use]
    pub fn tags(&self) -> &super::tags::TagFeed {
        &self.tags
    }

    /// The newest stores per holder.
    #[must_use]
    pub fn ring(&self) -> &super::ring::HolderRing {
        &self.ring
    }

    /// The log's partitioning and retention: [`super::spine::SpineLayout::default`] unless set.
    #[must_use]
    pub fn with_layout(mut self, layout: super::spine::SpineLayout) -> Self {
        self.layout = layout;
        self
    }

    /// Where the source stands.
    #[must_use]
    pub fn position(&self) -> &Position {
        self.intake.position()
    }

    /// Repeat compute.
    #[must_use]
    pub fn repeat(&self) -> &RepeatCompute {
        &self.repeat
    }

    /// Placement.
    #[must_use]
    pub fn placement(&self) -> &PlacementCompute<P> {
        &self.placement
    }

    /// The intake: where the source stands, and the batches still owed.
    #[must_use]
    pub fn intake(&self) -> &Intake {
        &self.intake
    }

    /// The held class and band cells.
    #[must_use]
    pub fn classes(&self) -> &Held<Cells> {
        &self.classes
    }

    /// The held placement cells.
    #[must_use]
    pub fn placed(&self) -> &Held<DupCells> {
        &self.placed
    }

    /// The held coverage cells.
    #[must_use]
    pub fn covered(&self) -> &Held<CoverageSums> {
        &self.covered
    }

    /// Through where coverage is stated.
    #[must_use]
    pub fn covered_to_ms(&self) -> Option<f64> {
        self.covered_to_ms
    }

    /// Sample one engine id in `modulus` for the evict audit (`1`: every id).
    #[must_use]
    pub fn with_evict_sample(mut self, modulus: u64) -> Self {
        self.evict_sample = modulus.max(1);
        self
    }

    /// Where the sampled audit stands.
    #[must_use]
    pub fn audit(&self) -> super::audit::AuditEdge {
        self.audit
    }

    /// The roster.
    #[must_use]
    pub fn roster(&self) -> &super::roster::RosterFeed {
        &self.roster
    }

    /// The cells that moved since the last call, every family: the batch to publish.
    pub fn drain(&mut self) -> Batch {
        let roster = self.roster.drain();
        self.roster_runs.extend(roster.runs);
        for (holder, ring) in self.ring.drain() {
            self.rings.retain(|(h, _)| *h != holder);
            self.rings.push((holder, ring));
        }
        let (block_rows, block_days) = self.blocks.drain();
        for day in block_days {
            if let (Some(summary), Some(published)) =
                (self.blocks.days().get(&day), self.blocks.published(day))
            {
                self.block_days.retain(|(d, _)| *d != day);
                self.block_days
                    .push((day, super::blocks::encode(summary, published)));
            }
        }
        let (whale_rows, whale_days) = self.whales.drain();
        for day in whale_days {
            if let (Some(stated), Some(published)) =
                (self.whales.days().get(&day), self.whales.published(day))
            {
                self.whale_days.retain(|(d, _)| *d != day);
                self.whale_days
                    .push((day, super::whales::encode(stated, published)));
            }
        }
        Batch {
            blocks: block_rows,
            whales: whale_rows,
            tags: self.tag_cells.drain(),
            restated: self.restated.drain(),
            spans: self.spans.drain(),
            declarations: self.tags.drain_rows(),
            classes: self.classes.drain(),
            placement: self.placed.drain(),
            coverage: self.covered.drain(),
            roster: roster.rows,
            days: std::mem::take(&mut self.due),
        }
    }

    /// Take what `source` delivers after the driver's position, then fold what `watermark`
    /// allows, one read of each batch for both operators: repeat compute the records between
    /// its classified edge and its bound, a fresh batch's records behind that edge counted
    /// late; placement the records up to and at the watermark.
    ///
    /// # Errors
    /// The input log, a batch or the model relay could not be read.
    pub fn tick(
        &mut self,
        lake: &analyzer_lake::Lake,
        source: &dyn Source,
        params: &Params,
        watermark: Watermark,
    ) -> Result<TickRun, Error> {
        // The roster reads every commit on its own clock, whatever the watermark allows.
        self.roster.tick(lake)?;
        // Declarations too, before anything classifies: a store is attributed against every
        // statement committed by then, and a statement changing an interval behind the edge
        // restates the stores it reaches.
        let restated = self.tags.tick(lake)?;
        self.retag(lake, params, &restated)?;
        let (through_before, classified_before) = self.take(source)?;
        // Coverage starts where the observed records start, the first tick that has any, and
        // days settle from that day on.
        let first_seen = self.intake.first_at_ms();
        if self.settled_through_day.is_none() {
            self.settled_through_day =
                first_seen.map(|first| analyzer_analysis::emitted::day_index(first) - 1);
        }
        let watermark_ms = self.resolve(watermark, source);
        let mut behind = Vec::new();
        let mut placement_behind = Vec::new();
        let mut placement = PlacementTick::default();
        if let (Some(bound), Some(w)) = (self.repeat.bound(watermark_ms), watermark_ms) {
            let classified = self.repeat.classified_to_ms().unwrap_or(f64::NEG_INFINITY);
            let placed = self.placement.data_to_ms().unwrap_or(f64::NEG_INFINITY);
            // Placement folds the records at the watermark too: the data edge is just past it.
            let data_to = w + 1.0;
            let mut reads = self
                .intake
                .read_windows(source, &[(classified, bound), (placed, data_to)])?;
            let (for_placement, for_repeat) = (
                reads.pop().unwrap_or_default(),
                reads.pop().unwrap_or_default(),
            );
            behind = for_repeat.behind;
            placement_behind = for_placement.behind;
            let accepted_late = self.repeat.accept(for_repeat.records);
            debug_assert_eq!(accepted_late, 0, "the read starts at the classified edge");
            self.placement.accept(for_placement.records);
        }
        // One model for both operators, reaching placement's data edge.
        let model = self.model(lake, params, watermark_ms.map(|w| w + 1.0))?;
        if let Some(w) = watermark_ms {
            placement = self.place(w + 1.0, params, &model);
        }
        // The first-read records go to the log now, so no tick's records wait in memory.
        let fresh = self.intake.take_fresh();
        let modulus = self.evict_sample;
        self.evicts.observe(&fresh, &|b| {
            crate::spine::identity_hash(b).is_multiple_of(modulus)
        });
        // A store still open waits for its evict as long as the log is kept; a closed one, as
        // long as the examined history, for a second evict of it.
        if let Some(newest) = fresh.iter().map(|e| e.at_ms).reduce(f64::max) {
            let kept_ms = f64::from(self.layout.retention_days) * analyzer_analysis::DAY_MS;
            self.evicts
                .forget_before(newest - kept_ms, newest - params.examined_history_ms);
        }
        self.written
            .absorb(super::spine::write_tick(lake, self.layout, &fresh)?);
        let meaning = Meaning::current(&Discriminator::of(params));
        let models = self.roster.cells().models();
        let tick = self.classify(
            lake,
            watermark_ms,
            &RepeatContext {
                params,
                model: &model,
                meaning: &meaning,
                models: &models,
                coverage: 1.0,
            },
        )?;
        self.absorb(&tick.cells, &placement, params);
        let (repeat_late, placement_late) = self.restate(
            (lake, source, params),
            (behind, placement_behind),
            (through_before, classified_before),
        )?;
        let audit = self.audit_tick(lake, params, &tick.cells.facts, classified_before)?;
        let coverage_restated = match watermark_ms {
            Some(end) => self.cover(lake, params, end)?,
            None => 0,
        };
        // Every edge has moved: the days behind all of them settle.
        let meaning = Meaning::current(&Discriminator::of(params));
        let edges = (
            self.repeat.classified_to_ms(),
            self.placement.emitted_to_ms(),
        );
        let settled_days = self.settle(&meaning, edges);
        self.state_whale_days(&settled_days);
        // Each day's coverage, captured before the held cells retire it.
        for &day in &settled_days {
            self.due.retain(|d| d.day != day);
            self.due
                .push(super::SettledDay::of(day, self.covered.cells()));
        }
        self.meaning = Some(meaning);
        self.retire_closed_days();
        Ok(TickRun {
            cells: tick.cells,
            placement,
            late: repeat_late,
            placement_late,
            coverage_restated,
            settled_days,
            classified_to_ms: tick.classified_to_ms,
            audit,
            evicts: self.evicts.take(),
            pending: self.intake.pending(),
        })
    }
}

impl<P> Driver<P>
where
    P: KeyedState<String, IdentityState> + Send,
{
    /// State the whale register's `days`, each one settled or restated: every identity's day is
    /// exact once the day settles.
    fn state_whale_days(&mut self, days: &[i64]) {
        for &day in days {
            self.whales.set_day(
                day,
                analyzer_analysis::whale_days::Day::of(
                    self.placement.whale_totals(day),
                    analyzer_analysis::whale_days::WHALE_CUT,
                ),
            );
        }
    }

    /// Retire the held days no record can reach any more: a day stays open to change while a
    /// record in the examined history before the slowest edge can still reach it.
    fn retire_closed_days(&mut self) {
        if let (Some(classified), Some(emitted), Some(covered)) = (
            self.repeat.classified_to_ms(),
            self.placement.emitted_to_ms(),
            self.covered_to_ms,
        ) {
            let open_from =
                classified.min(emitted).min(covered) - analyzer_analysis::CORRECTION_HORIZON_MS;
            let day = analyzer_analysis::emitted::day_index(open_from);
            self.classes.retire_before(day);
            self.placed.retire_before(day);
            self.blocks.retire_before(day);
            self.tag_cells.retire_before(day);
            self.restated.retire_before(day);
            self.spans.retire_before(day);
            self.covered.retire_before(day);
            if self.open_from_day.is_some_and(|before| before < day) {
                self.placement.retire_whales_before(day);
                self.whales.retire_before(day);
            }
            self.open_from_day = Some(day);
        }
    }
}

impl<P> Driver<P> {
    /// The instant `watermark` names at the intake's position.
    fn resolve(&self, watermark: Watermark, source: &dyn Source) -> Option<f64> {
        match watermark {
            Watermark::Provable => self.intake.watermark(source),
            Watermark::At(at) => Some(at),
            Watermark::ProvableOr(at) => self.intake.watermark(source).or(Some(at)),
        }
    }

    /// Take a tick's output into what the driver holds: the classified stores into the blocks
    /// by stored mass, the holders' rings and the tag cells, and every family's partial cells
    /// into the held cells.
    fn absorb(&mut self, classified: &ClassBandCells, placement: &PlacementTick, params: &Params) {
        for fact in &classified.facts {
            self.blocks.add(
                analyzer_analysis::emitted::day_index(fact.at_ms),
                &fact.block,
                fact.n_tokens,
                &fact.holder,
            );
        }
        self.ring.add(&classified.facts);
        let mut tagged = analyzer_analysis::tags::TagCells::default();
        tagged.apply(
            &classified.facts,
            self.tags.index(),
            params.delta_skew_ms,
            1,
        );
        self.tag_cells.add(tagged);
        self.classes.add(classified.cells.clone());
        self.placed.add(placement.cells.clone());
    }

    /// Every day the driver still holds open, whole, for each day-held family: its tables, its
    /// days and their rows. What a tick publishes by replacing those days when the working
    /// state rolled back past ticks serving already holds, so a key only a lost tick published
    /// leaves with the replace.
    pub(crate) fn open_days_whole(&self) -> Vec<(&'static [Table], Vec<i64>, Vec<Row>)> {
        fn whole<P: Persisted + DayPartitioned>(
            cells: &P,
        ) -> (&'static [Table], Vec<i64>, Vec<Row>) {
            let days: Vec<i64> = cells.days().into_iter().collect();
            let rows = days.iter().flat_map(|day| cells.render_day(*day)).collect();
            (P::TABLES, days, rows)
        }
        let blocks: Vec<i64> = self.blocks.days().keys().copied().collect();
        let whale_days: Vec<i64> = self.whales.days().keys().copied().collect();
        vec![
            whole(self.classes.cells()),
            whole(self.placed.cells()),
            whole(self.covered.cells()),
            whole(self.tag_cells.cells()),
            whole(self.restated.cells()),
            whole(self.spans.cells()),
            (
                analyzer_analysis::block_mass::TABLES,
                blocks.clone(),
                blocks
                    .iter()
                    .flat_map(|day| self.blocks.day_rows(*day))
                    .collect(),
            ),
            (
                analyzer_analysis::whale_days::TABLES,
                whale_days.clone(),
                whale_days
                    .iter()
                    .flat_map(|day| self.whales.day_rows(*day))
                    .collect(),
            ),
        ]
    }

    /// Remember the settled days a correction over `window` restated.
    fn note_restated(&mut self, window: Window) {
        if let Some(through) = self.settled_through_day {
            let first = analyzer_analysis::emitted::day_index(window.start_ms);
            let last = analyzer_analysis::emitted::day_index(window.end_ms).min(through);
            self.restated_days.extend(first..=last);
        }
    }

    /// Settle the days every edge has passed by the settle delay, extend this meaning's
    /// generation over them, and return them with the settled days corrections restated.
    fn settle(
        &mut self,
        meaning: &Meaning,
        (classified_to_ms, emitted_to_ms): (Option<f64>, Option<f64>),
    ) -> Vec<i64> {
        let mut days: std::collections::BTreeSet<i64> = std::mem::take(&mut self.restated_days);
        let edges = [classified_to_ms, emitted_to_ms, self.covered_to_ms];
        if let [Some(a), Some(b), Some(c)] = edges {
            // The last whole day before the edge less the delay.
            let through =
                analyzer_analysis::emitted::day_index(a.min(b).min(c) - self.settle_delay_ms) - 1;
            let from = self.settled_through_day.map_or(through, |d| d + 1);
            if through >= from {
                days.extend(from..=through);
                self.settled_through_day = Some(through);
                match self.generations.iter_mut().find(|g| {
                    g.semantics_version == meaning.semantics_version
                        && g.discriminator == meaning.discriminator
                }) {
                    Some(g) => {
                        g.first_day = g.first_day.min(from);
                        g.last_day = g.last_day.max(through);
                    }
                    None => self.generations.push(super::store::Generation {
                        semantics_version: meaning.semantics_version.to_string(),
                        discriminator: meaning.discriminator.clone(),
                        first_day: from,
                        last_day: through,
                    }),
                }
            }
        }
        days.into_iter().collect()
    }

    /// State coverage through where it is settled at the watermark `end`. First the sources
    /// whose arrivals reached behind the stated edge are stated again over the open days, each
    /// cell that changed replaced; then every source is stated from the edge on, from the start
    /// of its first evidence's day. The model of the segments' arrivals reaches back over the
    /// examined history, so each source is judged against its whole span history. Returns the
    /// sources restated.
    fn cover(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        end: f64,
    ) -> Result<u64, Error> {
        let arrivals = self.coverage.read(lake)?;
        let behind = self.coverage.take(&arrivals, self.covered_to_ms);
        let Some(from) = self.covered_to_ms.or_else(|| {
            self.coverage
                .sources()
                .values()
                .map(|s| s.stated_from_ms)
                .reduce(f64::min)
        }) else {
            return Ok(0);
        };
        #[allow(clippy::cast_precision_loss)] // day indexes are small integers
        let open_from = self
            .open_from_day
            .map_or(f64::NEG_INFINITY, |d| d as f64 * analyzer_analysis::DAY_MS);
        let earliest = behind
            .iter()
            .map(|(_, stated_from)| stated_from.max(open_from))
            .fold(from, f64::min);
        let window = Window {
            start_ms: earliest - params.examined_history_ms,
            end_ms: end,
        };
        let Some(model) = crate::arrivals::window_model_from_arrivals(lake, &window, params)?
        else {
            return Ok(0);
        };
        let mut restated_arrivals = analyzer_analysis::restated::Restated::default();
        for (_, first, _) in arrivals.iter().filter(|a| a.1 < from) {
            let outcome = if *first < open_from {
                Outcome::TooLate
            } else {
                Outcome::Corrected
            };
            restated_arrivals.count(Family::Coverage, *first, outcome);
        }
        self.restated.add(restated_arrivals);
        for (source, stated_from) in &behind {
            let restated = Window {
                start_ms: stated_from.max(open_from),
                end_ms: from,
            };
            if restated.start_ms < restated.end_ms {
                let mut days = self.covered.restate(
                    source,
                    restated,
                    &model.source_coverage_sums(source, restated),
                );
                days.extend(self.spans.restate(
                    source,
                    restated,
                    &InstanceSpans::of_model(&model, restated, Some(source)),
                ));
                for day in days {
                    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
                    self.note_restated(Window {
                        start_ms: day as f64 * analyzer_analysis::DAY_MS,
                        end_ms: day as f64 * analyzer_analysis::DAY_MS,
                    });
                }
            }
        }
        let to = model
            .coverage_settled_to(analyzer_analysis::staleness::STALENESS_GRACE_MS)
            .min(end);
        if to > from {
            for (source, stated) in self.coverage.sources() {
                let stretch = Window {
                    start_ms: from.max(stated.stated_from_ms),
                    end_ms: to,
                };
                if stretch.start_ms < stretch.end_ms {
                    self.covered
                        .add(model.source_coverage_sums(source, stretch));
                    self.spans
                        .add(InstanceSpans::of_model(&model, stretch, Some(source)));
                }
            }
            self.covered_to_ms = Some(to);
            self.coverage
                .forget_before(to - analyzer_analysis::CORRECTION_HORIZON_MS);
        }
        Ok(behind.len() as u64)
    }
}

impl Driver<Seeds> {
    /// Maintain the log partitioned by block, between ticks and inside its own unit of work:
    /// compact every hour both operators have passed that still holds tick objects, and
    /// retire the hours older than the layout's retention. Writes are versioned at the store's
    /// last tick, so the next save's manifest entries win. An hour holding objects this driver
    /// has not saved yet is left alone: compaction deletes what no manifest names. Returns the
    /// hours compacted.
    ///
    /// # Errors
    /// The working state or the store failed, or an object does not decode.
    pub fn maintain(&self, lake: &analyzer_lake::Lake, store: &Store) -> Result<Vec<i64>, Error> {
        let (Some(classified), Some(emitted)) = (
            self.repeat.classified_to_ms(),
            self.placement.emitted_to_ms(),
        ) else {
            return Ok(Vec::new());
        };
        #[allow(clippy::cast_precision_loss)] // ticks stay far below 2^52
        let version = store.tick() as f64;
        let passed = super::spine::hour_of(classified.min(emitted));
        let mut compacted = Vec::new();
        for hour in super::spine::hours_with_ticks(lake)? {
            if hour < passed && !self.written.holds(hour) {
                super::spine::compact(lake, hour, version)?;
                compacted.push(hour);
            }
        }
        super::spine::collect_held_garbage(lake, version)?;
        let kept_ms = f64::from(self.layout.retention_days) * analyzer_analysis::DAY_MS;
        super::spine::retire_before(lake, super::spine::hour_of(classified - kept_ms), version)?;
        Ok(compacted)
    }

    /// A driver resumed from `lake`'s stored state under `params`' meaning, with fresh operators
    /// configured as the ones that wrote it; from the start of the log when the store was never
    /// written.
    ///
    /// # Errors
    /// [`Error::IncompatibleState`] when the store was written under another meaning: the
    /// caller starts a fresh state ([`Store::reset`]). Otherwise the working state failed, or a
    /// record does not decode.
    pub fn open(
        lake: &analyzer_lake::Lake,
        params: &Params,
        repeat: RepeatCompute,
        placement: PlacementCompute<Seeds>,
    ) -> Result<(Self, Store), Error> {
        let meaning = Meaning::current(&Discriminator::of(params));
        let (store, restored) = Store::restore(lake, &meaning, repeat, placement)?;
        Ok((
            Self {
                repeat: restored.repeat,
                placement: restored.placement,
                intake: restored.intake,
                classes: Held::restore(lake)?,
                placed: Held::restore(lake)?,
                covered: Held::restore(lake)?,
                roster: restored.roster,
                coverage: restored.coverage,
                blocks: {
                    let mut blocks =
                        analyzer_analysis::block_mass::BlockMass::new(BLOCK_CAPACITY, BLOCK_CUT);
                    for (day, bytes) in &restored.blocks {
                        let (summary, published) = super::blocks::decode(bytes)?;
                        blocks.restore_day(*day, summary, published);
                    }
                    blocks
                },
                whales: {
                    let mut whales = analyzer_analysis::whale_days::WhaleDays::default();
                    for (day, bytes) in &restored.whales {
                        let (stated, published) = super::whales::decode(bytes)?;
                        whales.restore_day(*day, stated, published);
                    }
                    whales
                },
                whale_days: Vec::new(),
                block_days: Vec::new(),
                ring: super::ring::HolderRing::restored(super::ring::RING_CAPACITY, restored.rings),
                rings: Vec::new(),
                tags: restored.tags,
                tag_cells: Held::restore(lake)?,
                restated: Held::restore(lake)?,
                spans: Held::restore(lake)?,
                layout: super::spine::SpineLayout::default(),
                written: super::spine::Written::default(),
                roster_runs: Vec::new(),
                covered_to_ms: restored.covered_to_ms,
                audit: restored.audit,
                evicts: analyzer_analysis::differential::EvictAudit::default(),
                evict_sample: super::audit::EVICT_SAMPLE_MODULUS,
                open_from_day: restored.open_from_day,
                model_through: None,
                corrected_identities: std::collections::BTreeSet::new(),
                placement_corrected: std::collections::BTreeSet::new(),
                reach: Retainted::default(),
                meaning: Some(meaning),
                settle_delay_ms: analyzer_analysis::staleness::STALENESS_GRACE_MS,
                settled_through_day: restored.settled_through_day,
                due: Vec::new(),
                generations: restored.generations,
                module_spans: restored.module_spans,
                restated_days: std::collections::BTreeSet::new(),
            },
            store,
        ))
    }

    /// Write what the ticks since the last save changed to `store`: the operators' state, the
    /// cells that moved, and the head naming the intake and `publication`, the revision the
    /// batch goes out under. Returns the batch, to publish once the caller's unit of work
    /// around the ticks and this has ended.
    ///
    /// # Errors
    /// The working state failed.
    pub fn save(
        &mut self,
        lake: &analyzer_lake::Lake,
        store: &mut Store,
        publication: u64,
    ) -> Result<Batch, Error> {
        let batch = self.drain();
        let runs = std::mem::take(&mut self.roster_runs);
        let block_days = std::mem::take(&mut self.block_days);
        let whale_days = std::mem::take(&mut self.whale_days);
        let rings = std::mem::take(&mut self.rings);
        let declarations = self.tags.take_unsaved();
        let written = std::mem::take(&mut self.written);
        store.write(
            lake,
            (&mut self.repeat, &mut self.placement),
            &super::store::Save {
                intake: &self.intake,
                batch: &batch,
                publication,
                retire_before: self.open_from_day,
                covered_to_ms: self.covered_to_ms,
                audit: self.audit,
                roster: (&runs, self.roster.position()),
                coverage: &self.coverage,
                blocks: &block_days,
                whales: &whale_days,
                rings: &rings,
                tags: (&declarations, self.tags.position()),
                meaning: self.meaning.as_ref().ok_or_else(|| {
                    Error::Derivation("a keyed driver saved before its first tick or open".into())
                })?,
                settled_through_day: self.settled_through_day,
                generations: &self.generations,
                module_spans: &self.module_spans,
                spine: &written,
            },
        )?;
        Ok(batch)
    }
}
