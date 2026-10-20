// SPDX-License-Identifier: AGPL-3.0-only

//! Namespace-bound execution shared by standalone callers and the daemon: one keyed tick under
//! the journal's intent (`docs/pipeline.md`).

use crate::keyed::{
    Batch, Driver, Emission, FillReader, Hook, Late, PlacementCompute, RamState, RepeatCompute,
    Seeds, SegmentSource, Store, Watermark,
};
use crate::{
    core::Core, execution::Namespace, modules::ModuleSet, publish::Serving, spine::Spine, Error,
};
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::{Discriminator, Params};
use std::sync::{Arc, Mutex, PoisonError};

/// The keyed driver and its store, lent across ticks by the process that runs them: the
/// operators' state stays in memory between ticks, and a tick that fails drops it, so the next
/// one opens it again from the store.
#[derive(Default)]
pub struct Lent {
    held: Option<Held>,
}

/// The driver and the store it was opened from.
type Held = (Driver<Seeds>, Store);

/// One execution configuration over one lineage: the spine it derives in and the serving store
/// it publishes to.
///
/// Each operation acquires local writer exclusion at its mutation boundary. Holding this value
/// does not acquire a lease or authorize concurrent distributed writers.
///
/// **Every operation blocks.** The object store and a serving store each drive their own
/// runtime with `block_on`, so an operation MUST NOT be called from a thread that is running
/// an async runtime: tokio panics on the nested `block_on`. From async code, call it through
/// `tokio::task::spawn_blocking` (as `analyzer_api` does for every read).
pub struct Executor<'a> {
    spine: Arc<Spine>,
    serving: Arc<dyn Serving>,
    params: Params,
    modules: ModuleSet,
    lanes: usize,
    lent: Option<&'a Mutex<Lent>>,
}

/// What one tick did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Advance {
    /// The publication the tick's batch went out under.
    pub publication: u64,
    /// Whether an earlier tick's unapplied batch was published again first.
    pub republished: bool,
    /// Stores classified.
    pub stores: u64,
    /// Records behind repeat compute's edge, and what its correction path did.
    pub late: Late,
    /// Records behind placement's edge, and what its correction path did.
    pub placement_late: Late,
    /// Sources whose coverage was stated again from their heartbeats.
    pub coverage_restated: u64,
    /// Days the settled-day hook stated.
    pub settled_days: Vec<i64>,
    /// Repeat compute's classified edge.
    pub classified_to_ms: Option<f64>,
    /// Placement's emitted edge.
    pub emitted_to_ms: Option<f64>,
    /// Coverage's stated edge.
    pub covered_to_ms: Option<f64>,
    /// The tick's sampled audit.
    pub audit: crate::keyed::Audit,
    /// Where the sampled audit stands after it.
    pub audit_edge: crate::keyed::AuditEdge,
    /// The evict audit of the tick's records on the sampled engine ids.
    pub evicts: analyzer_analysis::differential::AuditOutcome,
    /// Delivered batches with records still to fold.
    pub pending: usize,
}

/// A public module's standing: through which settled day its output is stated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleAvailability {
    /// The module's declarations.
    pub definition: analyzer_analysis::module::ModuleSpec,
    /// Whether this configuration runs it.
    pub enabled: bool,
    /// The first settled day the module's output is stated for: the first day that settled
    /// while it was enabled. Before it, the module stated nothing. `None` until a day settles
    /// with it enabled, and for a disabled module.
    pub stated_from_day: Option<i64>,
    /// The last settled day, through which its output is stated; `None` with
    /// `stated_from_day`.
    pub stated_through_day: Option<i64>,
    /// The revision the standing was read at; `None` when a concurrent write moved it.
    pub revision: Option<u64>,
}

impl<'a> Executor<'a> {
    /// An executor over `spine`'s lineage, publishing to `serving`, under `params`. The serving
    /// store must be bound to the spine's namespace: one store serves one namespace.
    ///
    /// # Errors
    /// Invalid parameters, a store bound to no namespace or another one, or the store's
    /// failure.
    pub fn open(
        spine: Arc<Spine>,
        params: &Params,
        serving: Arc<dyn Serving>,
    ) -> Result<Self, Error> {
        params.validate()?;
        match serving.bound()? {
            Some(bound) if bound == *spine.namespace() => {}
            Some(_) => return Err(Error::IncompatibleState),
            None => {
                return Err(Error::Derivation(
                    "the serving store must be bound to a namespace before execution".into(),
                ))
            }
        }
        Ok(Self {
            spine,
            serving,
            params: params.clone(),
            modules: ModuleSet::all(),
            lanes: LANES,
            lent: None,
        })
    }

    fn core(&self) -> Core<'_> {
        Core {
            spine: &self.spine,
            serving: &self.serving,
        }
    }

    /// Run `modules`, the public modules this configuration enables, over each settled day.
    #[must_use]
    pub fn with_modules(mut self, modules: ModuleSet) -> Self {
        self.modules = modules;
        self
    }

    /// Run each keyed operator on `lanes` lanes (at least one), each on a thread of its own. A
    /// restore routes every identity to its lane afresh, so the count may change between
    /// starts.
    #[must_use]
    pub fn with_lanes(mut self, lanes: usize) -> Self {
        self.lanes = lanes.max(1);
        self
    }

    /// Keep the keyed driver in `lent` between ticks rather than opening it from the store for
    /// each.
    #[must_use]
    pub fn with_lent(mut self, lent: &'a Mutex<Lent>) -> Self {
        self.lent = Some(lent);
        self
    }

    /// The lineage's lake.
    #[must_use]
    pub fn lake(&self) -> analyzer_lake::Lake {
        self.spine.lake()
    }

    /// The spine this executor derives in.
    #[must_use]
    pub fn spine(&self) -> &Arc<Spine> {
        &self.spine
    }

    /// The logical owner, independent of this worker or its local caches.
    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        self.spine.namespace()
    }

    /// Publish the served input tables (batches, segments, drops, and the rest) through the
    /// input log committed since the last publication: what readers of what arrived read.
    /// Returns the batches published.
    ///
    /// # Errors
    /// The journal or the input log could not be read, or the serving store refused.
    pub fn publish_inputs(&self) -> Result<u64, Error> {
        let core = self.core();
        let _writer = core.writer()?;
        crate::inputs::publish(&core.journal()?, &core.published()?, core.fleet())
    }

    /// Keep the input log to its retention: drop the days older than `retention_days`, and
    /// fold into one manifest each day older than `horizon_days`, the raw horizon past which no
    /// source reads an object.
    ///
    /// # Errors
    /// The input log could not be read, or another instance holds the log's lease.
    pub fn maintain_input_log(
        &self,
        retention_days: u32,
        horizon_days: u32,
    ) -> Result<crate::inputs::Maintained, Error> {
        crate::inputs::maintain(&self.lake(), retention_days, horizon_days)
    }

    /// Retire the published state of every day older than `retention_days`: what serving
    /// rebuilds from must outlive serving's longest horizon, and nothing past it does. Returns
    /// the days retired.
    ///
    /// # Errors
    /// The store failed, or another instance holds the lineage's lease.
    pub fn retire_published(&self, retention_days: u32) -> Result<Vec<i64>, Error> {
        let core = self.core();
        let _writer = core.writer()?;
        #[allow(clippy::cast_precision_loss)] // epoch milliseconds are exact in f64
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Derivation(format!("retirement clock: {e}")))?
            .as_millis() as f64;
        let first_kept = analyzer_analysis::emitted::day_index(now_ms) - i64::from(retention_days);
        Ok(analyzer_lake::state::StateLogs::at(&self.lake()).retire_before(first_kept)?)
    }

    /// One keyed tick. An earlier tick's batch the journal holds unapplied is published again
    /// first, under its own publication, and applied. Then: record the intent under a new
    /// publication; tick and save in one unit of the working state; publish the batch, each
    /// family through the lake's state log and then serving; state the day-grain outputs of the
    /// days it settled; maintain the log partitioned by block in a unit of its own; and apply
    /// the intent, stating the edges readers may now see.
    ///
    /// # Errors
    /// The journal, the working state, the source, the store or a module failed. The intent
    /// stays unapplied, and the next tick publishes its batch again.
    pub fn advance(&self, source: &dyn crate::source::FillSource) -> Result<Advance, Error> {
        let core = self.core();
        let _writer = core.writer()?;
        let mut own = Lent::default();
        let mut guard = self
            .lent
            .map(|m| m.lock().unwrap_or_else(PoisonError::into_inner));
        let lent: &mut Lent = guard.as_deref_mut().unwrap_or(&mut own);
        // Taken out and put back only when the tick succeeds: a tick that fails leaves the
        // operators in memory ahead of the store, so the next one opens them again.
        let held = lent.held.take();
        let (advance, held) = self.tick(source, held)?;
        lent.held = Some(held);
        Ok(advance)
    }

    fn tick(
        &self,
        source: &dyn crate::source::FillSource,
        held: Option<Held>,
    ) -> Result<(Advance, Held), Error> {
        let core = self.core();
        let lake = core.lake();
        let journal = core.journal()?;
        let working = lake.working()?;
        let (mut driver, mut store) = match held {
            Some(held) => held,
            None => self.open_driver(&lake)?,
        };
        let mut advance = Advance::default();
        if let Some(tick) = journal.read()?.keyed.filter(|t| !t.applied) {
            // The store names the publication it last wrote; an intent it never reached wrote
            // nothing, and is superseded by this tick's.
            if store.publication() == tick.publication {
                // The cells go out again under their publication, exactly once by key. The
                // day-grain outputs are stated whole again under a fresh revision, so their
                // replace retires whatever the failed attempt wrote, even a key a module's
                // reconstruction no longer produces.
                let restated = crate::execution::advance(&journal, "keyed")?;
                self.publish(
                    &store.outbox(&lake)?,
                    (tick.publication, restated),
                    source,
                    &driver,
                )?;
                advance.republished = true;
            }
            self.apply(&journal, &driver, tick.publication)?;
        }
        // The newest publication serving holds as applied: a store that names an older one was
        // rolled back past ticks whose cells are out, and replaying them cannot retract a key
        // only they published.
        let applied = journal
            .read()?
            .keyed
            .filter(|t| t.applied)
            .map(|t| t.publication);
        let publication = crate::execution::advance(&journal, "keyed")?;
        journal.commit(|h| {
            h.keyed = Some(crate::journal::KeyedTick {
                publication,
                applied: false,
            });
        })?;
        if working.begin(&lake)? || !store.current(&lake)? {
            (driver, store) = self.open_driver(&lake)?;
        }
        let reader = FillReader::new(source, core.fleet());
        let segments = SegmentSource::new(&lake, &reader);
        let watermark = match crate::facts::full_span(&lake)? {
            Some(span) => Watermark::ProvableOr(span.end_ms),
            None => Watermark::Provable,
        };
        let settled_before = driver.settled_through_day();
        let run = driver.tick(&lake, &segments, &self.params, watermark)?;
        // The days this tick settled for the first time: a correction's restated days are older,
        // and a module enabled now never stated the days between.
        let newly: Vec<i64> = run
            .settled_days
            .iter()
            .copied()
            .filter(|d| settled_before.is_none_or(|b| *d > b))
            .collect();
        driver.state_modules(self.modules.enabled().map(|r| r.spec.name), &newly);
        let behind = applied.is_some_and(|p| store.publication() < p);
        let batch = driver.save(&lake, &mut store, publication)?;
        lake.end_unit(working.position())?;
        self.publish(&batch, (publication, publication), source, &driver)?;
        if behind {
            self.restate_open_days(&driver, publication)?;
        }
        working.begin(&lake)?;
        driver.maintain(&lake, &store)?;
        lake.end_unit(working.position())?;
        self.apply(&journal, &driver, publication)?;
        advance.publication = publication;
        advance.stores = run.cells.facts.len() as u64;
        advance.late = run.late;
        advance.placement_late = run.placement_late;
        advance.coverage_restated = run.coverage_restated;
        advance.settled_days = batch.days.iter().map(|d| d.day).collect();
        advance.classified_to_ms = driver.repeat().classified_to_ms();
        advance.emitted_to_ms = driver.placement().emitted_to_ms();
        advance.covered_to_ms = driver.covered_to_ms();
        advance.audit = run.audit;
        advance.audit_edge = driver.audit();
        advance.evicts = run.evicts;
        advance.pending = run.pending;
        Ok((advance, (driver, store)))
    }

    /// The keyed driver resumed from the store. A store written under another meaning (the
    /// parameters changed) is reset, its generations carried, and the new meaning derives from
    /// the log afresh.
    fn open_driver(&self, lake: &analyzer_lake::Lake) -> Result<Held, Error> {
        let open = || {
            let skew = self.params.delta_skew_ms;
            Driver::open(
                lake,
                &self.params,
                RepeatCompute::new(self.lanes, Emission::Watermarked { delay_ms: skew }),
                // A closer just past an instant can shorten the residency before it by half the
                // skew, so placement's cells settle that far behind its data edge.
                PlacementCompute::new(
                    self.lanes,
                    Emission::Watermarked {
                        delay_ms: skew / 2.0,
                    },
                    RamState::default,
                ),
            )
        };
        match open() {
            Err(Error::IncompatibleState) => {
                let working = lake.working()?;
                working.begin(lake)?;
                Store::reset(lake)?;
                lake.end_unit(working.position())?;
                open()
            }
            opened => opened,
        }
    }

    /// Publish `batch`, every family, under `publication`, then state the day-grain outputs of
    /// its days under `stated`.
    fn publish(
        &self,
        batch: &Batch,
        (publication, stated): (u64, u64),
        source: &dyn crate::source::FillSource,
        driver: &Driver<Seeds>,
    ) -> Result<(), Error> {
        let core = self.core();
        let published = core.published()?;
        let meaning = Meaning::current(&Discriminator::of(&self.params));
        let head = crate::publish::Head {
            fleet_id: core.fleet(),
            meaning: &meaning,
        };
        for (tables, rows) in batch.families() {
            if !rows.is_empty() {
                crate::publish::Publish::apply(&published, &head, tables, publication, rows)?;
            }
        }
        Hook {
            lake: &core.lake(),
            source,
            fleet: core.fleet(),
            params: &self.params,
            modules: &self.modules,
            publish: &published,
        }
        .state(&batch.days, driver.generations(), stated)
    }

    /// Replace every open day of every day-held family with the driver's whole cells, under
    /// `publication`: after a rollback, serving holds cells of ticks the replay re-derived, and
    /// a key the replay no longer produces leaves only with a replace of its day.
    fn restate_open_days(&self, driver: &Driver<Seeds>, publication: u64) -> Result<(), Error> {
        let core = self.core();
        let published = core.published()?;
        let meaning = Meaning::current(&Discriminator::of(&self.params));
        for (tables, days, rows) in driver.open_days_whole() {
            if days.is_empty() {
                continue;
            }
            crate::publish::Publish::replace(
                &published,
                &crate::publish::Scope {
                    head: crate::publish::Head {
                        fleet_id: core.fleet(),
                        meaning: &meaning,
                    },
                    tables,
                    days: &days,
                },
                publication,
                &rows,
            )?;
        }
        Ok(())
    }

    /// Apply the intent under `publication`: state the driver's edges, then mark the intent
    /// applied, so the journal never vouches for an edge readers were not told.
    fn apply(
        &self,
        journal: &crate::journal::Journal,
        driver: &Driver<Seeds>,
        publication: u64,
    ) -> Result<(), Error> {
        use crate::publish::Status;
        let core = self.core();
        let published = core.published()?;
        let meaning = Meaning::current(&Discriminator::of(&self.params));
        let head = crate::publish::Head {
            fleet_id: core.fleet(),
            meaning: &meaning,
        };
        #[allow(clippy::cast_precision_loss)] // epoch milliseconds are exact in f64
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Derivation(format!("tick clock: {e}")))?
            .as_millis() as f64;
        let facts = [
            driver
                .repeat()
                .classified_to_ms()
                .map(Status::ClassifiedToMs),
            driver.placement().emitted_to_ms().map(Status::EmittedToMs),
            driver.covered_to_ms().map(Status::CoveredToMs),
            driver.audit().audited_to_ms.map(Status::AuditedToMs),
            driver
                .audit()
                .disagreed_at_ms
                .map(Status::AuditDisagreedAtMs),
            driver.settled_through_day().map(Status::SettledThroughDay),
            Some(Status::LastTickAtMs(now_ms)),
        ];
        for fact in facts.into_iter().flatten() {
            crate::publish::status(&published, &head, publication, fact)?;
        }
        journal.commit(|h| {
            if let Some(tick) = h.keyed.as_mut().filter(|t| t.publication == publication) {
                tick.applied = true;
            }
        })?;
        Ok(())
    }

    /// Each public module's standing: through which settled day its output is stated.
    ///
    /// # Errors
    /// The journal, the working state or the serving store failed.
    pub fn availability(&self) -> Result<Vec<ModuleAvailability>, Error> {
        let before = self.revision()?;
        let lake = self.lake();
        let settled = Store::settled_through_day(&lake)?;
        let spans = Store::module_spans(&lake)?;
        let mut modules: Vec<ModuleAvailability> = self
            .modules
            .registered()
            .map(|r| {
                let enabled = self.modules.has(r.spec.name);
                let from = spans.get(r.spec.name).copied().filter(|_| enabled);
                ModuleAvailability {
                    definition: r.spec,
                    enabled,
                    stated_from_day: from,
                    stated_through_day: settled.filter(|_| from.is_some()),
                    revision: before,
                }
            })
            .collect();
        if before != self.revision()? {
            for module in &mut modules {
                module.revision = None;
            }
        }
        Ok(modules)
    }

    /// Current durable consistency revision; an unapplied tick or an unsettled store withholds
    /// it. Validate both sides of a read against the same revision.
    ///
    /// # Errors
    /// Store failure or malformed durable standing.
    pub fn revision(&self) -> Result<Option<u64>, Error> {
        crate::execution::revision(&*self.serving, &self.spine)
    }
}

/// Lanes per keyed operator, unless set.
const LANES: usize = 8;
